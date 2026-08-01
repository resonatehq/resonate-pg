# Modeling Brief — resonate-pg

Specula Phase 1 output. Handoff to Spec Generation.
Target commit: `54fe651`. All `file:line` references are to `resonate.sql`.

## 1. System Overview

- **resonate-pg** — a Resonate durable-execution server implemented entirely in
  PL/pgSQL. Core logic is one file, `resonate.sql`, 1334 lines.
- **Category A (distributed / message-passing).** The database is the server; SDK
  workers are remote processes that reach it through one RPC entry point
  (`resonate_rpc`, :1055) and drain an addressed outbox (`dequeue_execute`,
  `dequeue_unblock`, :1235-1265). Failure model is worker crash + lease expiry,
  not intra-process data races.
- **Protocol**: the Resonate durable-promise protocol — promise actions P-01..P-06
  (:384-523), task actions T-01..T-11 (:531-822), schedule actions S-01..S-04
  (:830-881).
- **Key architectural choice**: Postgres has no timers, so *every* timeout is a
  row with a deadline, and pg_cron polls `process_timeouts()` every 5s (:1196-1228).
  This makes "the deadline has passed" and "the deadline has been *processed*" two
  distinct facts, and the code carries a read-time projection
  (`_promise_json`, :179-196) that reports a pending-but-expired promise as
  settled while the row still says `pending`. **That projection/row split is the
  single most bug-prone construct in the file** and organizes Scenario 1 and 2 below.
- **Concurrency model**: one transaction per action; `pg_advisory_xact_lock` on
  every id touched (`_lock`, :381) plus `SELECT ... FOR UPDATE` on every row read.
  Actions touching overlapping ids are therefore serialized, and interleaving
  happens strictly *between* actions.

## 2. Scenarios

### Scenario 1: Guard-set drift across the entry points that consult a promise's liveness

**Mechanism**: "is this promise still alive?" is re-derived independently at
eleven call sites, and the derivations disagree. Three distinct predicates are in
use for what is nominally one question:

| predicate | sites |
|---|---|
| `p.state = 'pending' AND p.timeout_at > now` | :461, :613, :640, :709, :759, :778 |
| `_promise_timed(p)` = `state='pending' AND external` | :204-207, :966 |
| *(no check at all)* | :583-594 (task.create claim), :803-819 (task.continue), :515 (register_listener) |

**Evidence**:
- Historical: `34ebe99` "Enforce timeouts on external promises" — a promise whose
  timeout was written but never enforced; "the row even read as
  `rejected_timedout` through `_promise_json`'s projection while never actually
  settling." Fixed for the *external* case by introducing the `external` column.
- Historical: `9187493` "Reject unawaitable and malformed await requests" — closed
  the same strand for the two **callback** writers (P-04 :488, T-06 :722) by
  returning 422 when the awaited is internal. It did not touch the **listener**
  writer (P-05 :501-520), which registers against any promise.
- Code analysis: :613 (`task_acquire` rejects a dead promise with 409) versus
  :583-594 (`task_create`'s claim branch consults neither promise state nor
  timeout) — two entry points to the same "take this task" operation with
  different answers.
- Code analysis: :815 (`task_continue` re-emits an execute with no promise check).

**Affected code paths**: `promise_register_listener`, `task_create`,
`task_continue`, `_promise_timed`, `process_promise_timeouts`.

**Suggested modeling approach**:
- Variables: `promises[i].external`, `promises[i].timeoutAt`, plus a global `now`
  that advances independently of the driver, so the model can sit in the window
  where a deadline has passed but `process_timeouts` has not run.
- Actions: model the driver (`OnPromiseTimeout`) as a *separate* action gated on
  `external`, never folded into `Tick`. Model `TaskClaim` (T-02's claim branch)
  distinctly from `TaskAcquire` (T-03) so their guard sets can diverge.
- Granularity: one action per RPC — each is one transaction.

**Priority**: High.
**Rationale**: the two previous bug-fix commits in this file are both instances of
this mechanism; the residual sites are the ones neither commit swept.

### Scenario 2: Read-time projection versus row state

**Mechanism**: `_promise_json` (:179-196) reports `rejected_timedout`/`resolved`
for a row that is still `pending`. Every observer that goes through the projection
sees a settled promise; every mechanism keyed off the row (`_cascade_settle` :269,
`gc` :1271, the `state <> 'pending'` indexes) sees a live one. Where the row is
never actually settled, the two never reconcile.

**Evidence**:
- Code analysis: :183-186 projection; :206 eligibility restricted to `external`;
  :1275 `gc` filters on `p.state <> 'pending'`, so a promise that only *projects*
  as settled is never reclaimed, and :1276-1277 additionally pins every promise
  sharing its `origin_id`.
- Code analysis: :596-597 `task_create` returns `_promise_json_raw(p)` — the
  projection with `now = -1`, i.e. deliberately *un*projected — so T-02 hands the
  worker `pending` for a promise that P-01 reports as `rejected_timedout`.

**Affected code paths**: `_promise_json`, `_promise_json_raw`, `gc`,
`process_promise_timeouts`.

**Suggested modeling approach**: model the projection as an operator
`Proj(i)` over `(promises, now)` distinct from `promises[i].state`, and state
observer-facing invariants in terms of `Proj`, not the row.

**Priority**: High.
**Rationale**: this is what makes Scenario 1's residual sites *observable* rather
than merely untidy.

### Scenario 3: Empty string treated as a present target

**Mechanism**: `resonate:target` is tested two different ways — `IS NOT NULL` at
the creation site, `IS NOT NULL AND <> ''` at every redelivery site.

**Evidence**: :415 `IF tgt IS NOT NULL` (creates the task, emits the execute)
versus :783, :815, :916, :934 `IF p.target IS NOT NULL AND p.target <> ''`
(emit nothing). Also :259 in `_enqueue_resume` uses the `<> ''` form.

**Affected code paths**: `promise_create`, `task_release`, `task_continue`,
`_on_task_retry_timeout`, `_on_task_lease_timeout`, `_enqueue_resume`.

**Priority**: Medium. **Rationale**: narrow trigger (client must send `""`), but
the resulting task is permanently undeliverable and permanently retrying.

### Scenario 4: Lock-order inversion between the cascade and the suspend path

**Mechanism**: `_cascade_settle` locks the settling promise, then locks awaiter ids
in `(awaited, awaiter)` order (:290-291). `task_suspend` locks the task id and all
awaited ids in sorted-id order (:697-702). The two orders are not the same
relation, so two transactions can take the same pair of advisory locks in opposite
order.

**Evidence**: :290-291 versus :697-702. Mitigated — not prevented — by the
50-attempt `deadlock_detected` retry loops at :1001-1011 and :1069-1164.

**Priority**: Low. **Rationale**: the retry loops make this a latency/throughput
concern rather than a correctness one; TLA+ is the wrong tool (the model treats
each transaction as atomic, which is exactly what the locks buy).

## 3. Modeling Recommendations

### 3.1 Model

- **The projection/row split** (Scenario 2) — state every observer-facing property
  over `Proj(i)`. Without this the model cannot express "observer A sees settled,
  observer B never hears".
- **`now` decoupled from the driver** (Scenario 1) — a `Tick` action that advances
  time without running timeouts is what creates the window all of Scenario 1 lives in.
- **`task_create`'s claim branch as its own action** (Scenario 1) — the point is
  precisely that it does not share `task_acquire`'s guards.
- **Promise kind as a creation-time choice** over `{target, timer, ext, plain}` —
  `external` is a generated column (:38-41) and drives timeout eligibility.
- **Listener and callback registration as separate actions** — the whole finding is
  that they have different guard sets.

### 3.2 Do Not Model

- **Cron expression parsing** (`_cron_field`, `_next_cron`, :301-368) — a pure
  function over strings; unit tests are the right tool. (Reviewed by hand: `*/n`,
  `a-b/n`, `a/n` and the dom/dow OR-semantics at :351-355 all match Vixie cron.
  Day-of-week `7` and month/day *names* are unsupported and surface as a 400 from
  `schedule_create` :856-857 — a documentation gap, not a safety bug.)
- **HTTP push** (`_notify_outbox`, :137-167) — external I/O, and failures are
  already caught and warned.
- **The advisory-lock discipline itself** — modelling each action as atomic
  *assumes* it, which is the right division of labour; Scenario 4 is a
  code-review finding.
- **`param`/`value` payloads** — opaque blobs, never inspected by the server.
- **Schedules** (S-01..S-04) — self-contained, and their only coupling to the rest
  is a plain `promise_create` call (:950).

## 4. Proposed Extensions

| Extension | Variables | Purpose | Scenario |
|---|---|---|---|
| Read-time projection | `Proj(i)` over `promises`, `now` | separate what observers read from what the row says | 2 |
| Decoupled clock | `now`, `Tick` | open the deadline-passed / not-yet-processed window | 1 |
| Driver as explicit action | `OnPromiseTimeout` gated on `external` | make timeout *eligibility* a modelled choice | 1 |
| Dispatch ledger | `badDispatch` (history) | record any lease/dispatch granted against a dead promise | 1 |
| Observation ledger | `obs` (history) | record the first settled state each promise was ever read as | 2 |

## 5. Proposed Invariants

| Invariant | Type | Description | Targets |
|---|---|---|---|
| `Stickiness` | Safety | once `promise.get` reports a promise settled, it never reports anything else | Scenario 2 |
| `NoStrandedListener` | Safety (at quiescence) | no listener row survives on a promise that observers read as settled | Scenario 1, 2 |
| `NoStrandedTask` | Safety (at quiescence) | a suspended task is still parked on a genuinely pending promise | Scenario 1 |
| `NoDeadDispatch` | Safety | no worker is granted a lease on, or dispatched against, an already-dead promise | Scenario 1 |
| `TaskPromiseCoherence` | Safety (driver idle) | a settled promise's task is fulfilled | Scenario 2 |
| `CallbacksAreExternal` | Safety | every callback's awaited is external — the property `9187493` set out to establish | Scenario 1 |

## 6. Findings Pending Verification

### 6.1 Model-Checkable

| ID | Description | Expected violation | Scenario |
|---|---|---|---|
| MC-1 | P-05 has no `external` guard while P-04/T-06 do. Can an observer be left permanently waiting on a promise the server reports as settled? | `NoStrandedListener` | 1, 2 |
| MC-2 | T-02's claim branch and T-10 omit the promise-liveness guard T-03 applies. Can a worker be dispatched against a promise that is already dead? | `NoDeadDispatch` | 1 |
| MC-3 | `_on_task_lease_timeout` (:921-937) does not bump `version`, so the fence token survives the lease. Can two workers both act on one task generation? | `Stickiness`, `TaskPromiseCoherence` | 1 |
| MC-4 | `_enqueue_resume` (:245-266) deletes all of a task's resume rows when it wakes it, and `task_suspend` deletes them again (:732). Can a wakeup be dropped such that a suspended task is never resumed? | `NoStrandedTask` | 1 |

### 6.2 Test-Verifiable

| ID | Description | Suggested test |
|---|---|---|
| T-1 | `resonate:target = ''` creates a task nothing can redeliver | SQL: create, drain outbox, tick the retry timer 3×, assert outbox empty and task still pending |
| T-2 | cron day-of-week `7` and month/day names are rejected with 400 | `schedule.create` with `0 0 * * 7` and `0 0 * JAN *` |

### 6.3 Code-Review-Only

| ID | Description | Suggested action |
|---|---|---|
| CR-1 | `task_heartbeat` checks `t.pid = p_pid` (:667); `task_fulfill`, `task_release`, `task_suspend`, `task_fence` do not. Version is the fence token, so this is likely deliberate — but the asymmetry is undocumented. | confirm intent, add a comment |
| CR-2 | Lock-order inversion, Scenario 4 (:290-291 vs :697-702) | document; the retry loops already contain it |
| CR-3 | `callbacks.awaiter_id` has no FK (:70-74) while `awaited_id` does, so `gc` can leave orphan awaiter rows | harmless today (`_enqueue_resume` returns on a missing task), but worth a note |
| CR-4 | An internal promise past its timeout is never reclaimable by `gc` and pins its whole `origin_id` group (:1275-1277) | second-order consequence of MC-1; fix follows from it |

## 7. Reference Pointers

- `resonate.sql` — schema :22-109, helpers :175-373, promises :384-523,
  tasks :531-822, schedules :830-881, timeouts :890-1015, RPC :1024-1189,
  dispatch :1235-1265, gc :1271-1290.
- `test/conformance.py` — HTTP shim for the resonate-conformance harness; provides
  `debug.reset` / `debug.tick` / `debug.snap`, which is how the model's `Tick` and
  driver actions map onto a live database.
- Prior fixes in this mechanism: `34ebe99`, `9187493`.
- Spec artifacts: `.specula-output/spec/`, reproductions: `.specula-output/repro/`.
