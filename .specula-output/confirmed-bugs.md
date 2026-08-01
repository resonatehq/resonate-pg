# Confirmed Bugs — resonate-pg

Specula Phase 4 output. Target `resonate.sql` @ `54fe651`.
Every finding below was reproduced against a live Postgres 16 instance with
`resonate.sql` applied unmodified; the reproduction script is
`.specula-output/repro/repro.sql` and its recorded output is
`.specula-output/repro/repro-output.txt`.

All five share one mechanism: **"is this promise still alive?" is answered by
three different predicates at different call sites** (modeling brief, Scenario 1).

Findings 1, 2, 4 and 5 are also **divergences from the reference specification**
(`resonatehq/resonate-specification`, branch `claude/close-the-square`, which is
14 commits ahead of `main` and carries two commits that tighten exactly these
rules). See `spec-comparison.md` for the three-way comparison; the normative
wording for each fix is quoted there.

---

## BUG-1 — a listener on an internal promise is never notified, while
## `promise.get` reports that same promise as settled

**Tier A.** Found by model checking (`NoStrandedListener`, 6-state
counterexample), reproduced on a live database.

**Where**: `promise_register_listener`, `resonate.sql:501-520`.

**What happens**

`promise.register_listener` (P-05) registers against any promise that exists and
has not yet expired. It has no `external` guard. But `process_promise_timeouts`
only ever enforces a timeout for an *external* promise — `_promise_timed` is
`state = 'pending' AND external` (:204-207), and that is the driver's entire
`WHERE` clause (:966).

So for a promise with no `resonate:target`, no `resonate:timer` and no
`resonate:external` tag — a plain `promise.create`, which is a public protocol
operation — the deadline is accepted, stored, and never enforced:

| observer | what it sees after `timeout_at` passes |
|---|---|
| `promise.get` | `rejected_timedout` (projection, :183-186) |
| the row | `pending`, forever |
| a registered listener | nothing, forever — no `unblock` is ever emitted |
| `gc` | nothing to collect: filters on `state <> 'pending'` (:1275) |

**Reproduction** (from `repro.sql`, live database, all times explicit):

```
create plain promise "latent", timeoutAt = 2000              -> 200
register_listener("latent", "poll://any@worker1")            -> 200
register_callback(awaited="latent", awaiter=<targeted>)      -> 422   <-- the asymmetry
process_timeouts(9000)                                       -> latent not selected
  row state ................ pending
  promise.get reports ...... rejected_timedout
  listeners still registered 1
  unblock messages emitted . 0
  gc(now) collected ........ 0
```

**Why this is a bug and not a design choice**

Commit `9187493` closed exactly this hole for the two *callback* writers, and its
message states the reasoning: "An internal promise's timeout is not enforced —
nothing outside this database ever settles it — so an awaiter parked on one could
wait forever. Both callback writers now refuse it." `promise_register_callback`
(:488) and `task_suspend` (:722) return 422. `promise_register_listener` is the
third writer of a wait registration and was not swept — it still returns 200.

Commit `34ebe99` describes the same failure mode as a bug in its own right: "The
row even read as `rejected_timedout` through `_promise_json`'s projection while
never actually settling, so the timeout looked real and did nothing."

**Second consequence**: because the row never leaves `pending`, `gc` can never
reclaim it (:1275). And if such a promise is itself named as a workflow's
`resonate:origin`, `gc`'s origin clause (:1276-1277) retains every settled promise
in that group as well, since the clause requires the origin not to be pending —
so one stuck promise can pin a whole workflow's history indefinitely.

**The reference spec agrees.** `resonate-specification` commit `6ddfab7`
("external-only waiters everywhere") added exactly this guard, for exactly this
reason: "without the guard the machine accepted an obligation its transition
relation cannot discharge (an internal promise that dies by deadline is settled
by projection only -- no tau ever emits the unblock)." Its `state.lean` docstring
now says internal promises must not have awaiters -- "ENFORCED: both registration
paths (`register_callback`, `register_listener`) refuse internal promises with
`422`".

**Suggested fix**: mirror :488 — after the 404 check in
`promise_register_listener`, `IF NOT pa.external THEN RETURN 422`. Alternatively,
enforce timeouts for all promises rather than only external ones, which would
close the gap at the source for the listener path, `gc`, and any future waiter.
The first is the minimal change consistent with the two prior commits.

---

## BUG-2 — `task.create` and `task.continue` dispatch workers against a promise
## that `task.acquire` refuses as dead

**Tier A.** Found by model checking (`NoDeadDispatch`, 4-state counterexample),
reproduced on a live database.

**Where**: `task_create` claim branch, `resonate.sql:580-594`;
`task_continue`, `resonate.sql:803-819`.

**What happens**

`task_acquire` (T-03) refuses to hand out a lease when the promise is gone:

```sql
IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN 409;  -- :613
```

`task_create`'s claim branch checks `resonate:target` (422), that a task row
exists (409), and `t.state` — and nothing at all about the promise. Between a
promise's `timeout_at` and the pg_cron tick that processes it — a window up to the
5s poll interval (:1211) — the two entry points disagree:

```
create workflow "wf", timeoutAt = 2000, target set   -> task pending v0
at now = 9000 (past timeout, driver has not ticked):
  task.acquire(wf, v0)  -> 409     correct
  task.create (wf)      -> 200     lease granted: state=acquired, version=1, pid=w2
  promise.get(wf)       -> rejected_timedout
  task.fulfill(wf, v1)  -> 409     the run's result is thrown away
```

The worker runs the workflow body — side effects and all — and is then told its
result is invalid. For a durable-execution engine whose contract is "nothing runs
twice", dispatching a run that is guaranteed to be discarded is the wrong side of
that contract.

Compounding it, `task_create` returns the promise through `_promise_json_raw`
(:596-597), which is the projection with `now = -1` — deliberately unprojected. So
T-02 hands the worker a promise that reads `pending` while P-01 reads
`rejected_timedout` for the same row at the same instant.

**BUG-2b**, same mechanism: `task_continue` (T-10) has no promise check either and
*emits an execute* (:816). Reproduced: halt a task, let its promise expire, call
`task.continue` → `200` and one execute message queued for an already-dead
workflow.

**The reference spec agrees.** Its T-02 claim branch is gated
(`if p.state == .pending ∧ p.timeoutAt > now`) with the comment "No lease is ever
armed on a logically dead task", and its T-10 returns 409. The unprojected
response is a separate known defect there too
(branch `fix/task-create-serve-projected-record`): serving the raw record "made
settled-promise stability falsifiable on the wire: GET could answer
rejectedTimedout and a later task.create could answer pending for the same
promise" — which is what `repro-output.txt` shows.

**Suggested fix**: apply T-03's guard at both sites. In `task_create`'s claim
branch, after the task row is located, and in `task_continue` after the promise is
loaded:

```sql
IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
```

Note `task_continue` already loads `p` (:811) and uses it only for the target.

---

## BUG-3 — `resonate:target = ''` creates a task that is dispatched once and can
## never be redelivered

**Tier B.** Found by code analysis, reproduced on a live database. Not
model-checked (the model does not carry address values).

**Where**: `promise_create:415` versus `:783`, `:815`, `:916`, `:934`, `:259`.

**What happens**

The creation site tests presence one way and every redelivery site tests it
another:

| site | test | with `target = ''` |
|---|---|---|
| `promise_create:415` | `tgt IS NOT NULL` | true → creates the task, emits execute |
| `task_release:783` | `p.target IS NOT NULL AND p.target <> ''` | false → no emit |
| `task_continue:815` | same | false → no emit |
| `_on_task_retry_timeout:916` | same | false → no emit |
| `_on_task_lease_timeout:934` | same | false → no emit |
| `_enqueue_resume:259` | `tgt IS NOT NULL AND tgt <> ''` | false → no emit |

Reproduced:

```
create "empty", tags = {"resonate:target": ""}   -> 200
  task ..... pending v0, timeout_at 6000
  outbox ... one execute row, address <>          (the empty address)
drain the outbox, then tick the retry timer 3x:
  task ..... still pending
  outbox ... empty after all three retries
```

The task is left permanently `pending`, its retry timer rescheduling every 5s
forever and emitting nothing. The one message it did produce went to the empty
address, where `pg_notify` fires on `resonate_q_` + md5('').

**Suggested fix**: normalize once at the boundary. Either reject an empty
`resonate:target` at `promise_create` (400), or treat `''` as absent there —
`IF NULLIF(tgt, '') IS NOT NULL` — so the creation site agrees with the five
sites that already use the `<> ''` form. `resonate.invoke` (:1182) already applies
`NULLIF(target, '')`, so the boundary convention exists; `promise_create` just
does not follow it.

---

---

## BUG-4 — `task.halt` succeeds on a task `task.get` already reports `fulfilled`

**Tier A.** Found by comparison with the reference spec, reproduced on a live
database, then confirmed by model checking (`NoHaltOnDead`).

**Where**: `task_halt`, `resonate.sql:789-801` — it never loads the promise.

`task_get` (:540-545) projects a task as `fulfilled` as soon as its promise is no
longer effectively pending. `task_halt` branches only on the *stored* task state.
So at one instant, for one task:

```
task.get  -> state "fulfilled"        (and halt-on-fulfilled is 409, :796)
task.halt -> 200, task row becomes 'halted'
```

The reference spec's T-09 returns 409 here and names the principle: "Branching on
the raw stored task here would make the stored-vs-projected divergence
observable — the one thing the projection discipline forbids."

The state self-heals — the promise timeout later cascades the task to
`fulfilled` — but the two responses contradict each other on the wire, which is
the property `Stickiness` protects for promises and nothing protects for tasks.

**Suggested fix**: load the promise and return 409 unless
`p.state = 'pending' AND p.timeout_at > p_now`, ahead of the `t.state` checks.

---

## BUG-5 — the task timeout handlers redispatch a logically dead workflow

**Tier C (latent).** Found by comparison with the reference spec, reproduced —
but only when the handlers are reached outside the shipped driver.

**Where**: `_on_task_retry_timeout` :904-919, `_on_task_lease_timeout` :921-937.
Both read the promise solely for `p.target` and never consult its state.

```
process_task_timeouts(9000) on a lease-expired task whose promise died at 3000
  -> task re-pended, one execute emitted   (a dispatch that can never be fulfilled)
```

**Why it is Tier C and not Tier A**: through `process_timeouts` (:997-1012) it
cannot happen. That function drains `process_promise_timeouts` before
`process_task_timeouts`, and every task's promise carries a target and is
therefore external, so the promise loop settles it and the cascade fulfils the
task before the task loop runs. Verified both ways in `repro.sql`.

So resonate.sql is correct here **by sequencing, not by guard** — and nothing
states or tests that dependency. `process_task_timeouts` is a callable function in
its own right (it is not granted to `resonate_worker`, so this is an
operator-reachable path, not a client-reachable one).

The reference spec closed it with a guard in `3e8a1d6` ("no new work for the
dead"), where it had to: its timeout transitions are independent τ rules with no
ordering between them, so there the same gap was "the sole source of the
doomed-dispatch message surplus".

**Suggested fix**: gate both handlers on the promise, matching T-08 `task_release`
(:778), which already does exactly this for the client-driven version of the same
decision. Cheap, and it removes the unstated ordering dependency.

---

## Dispositions

| ID | Tier | Status | Mechanism |
|---|---|---|---|
| BUG-1 | A | Reproduced, live DB + 6-state counterexample | missing `external` guard on the third wait-registration writer |
| BUG-2 | A | Reproduced, live DB + 4-state counterexample | promise-liveness guard missing at 2 of 8 task entry points |
| BUG-2b | A | Reproduced, live DB | same |
| BUG-3 | B | Reproduced, live DB | `''` vs `NULL` disagreement across 6 sites |
| BUG-4 | A | Reproduced, live DB + `NoHaltOnDead` counterexample | stored-vs-projected divergence made observable |
| BUG-5 | C | Reproduced via `process_task_timeouts`; masked by driver ordering | same guard gap, contained by sequencing |

## Refuted during this run

- **Dropped wakeup for a suspended task** (`NoStrandedTask`) — refuted
  exhaustively across 3.8M states. `task_suspend`'s 300 path (:723-726) re-reads
  every awaited under lock and refuses to park when any has already settled.
- **Stale fence token after a lease timeout** (`_on_task_lease_timeout` does not
  bump `version`, :930) — no violation. The expired generation is fenced by
  `t.state <> 'acquired'` at every action that could act on it.
- **Settlement stickiness** (`Stickiness`) — holds exhaustively. No interleaving of
  settle / fulfill / timeout / claim lets a client read a promise as settled and
  later read it as anything else.
- **`CallbacksAreExternal`** — holds exhaustively; `9187493` fully achieved its
  goal for the callback path.

## Not model-checked, recorded from code review

- **CR-1** `task_heartbeat` checks `t.pid = p_pid` (:667); `task_fulfill`,
  `task_release`, `task_suspend` and `task_fence` do not. The version is the fence
  token, so this is likely deliberate — but the asymmetry is undocumented.
- **CR-2** Lock-order inversion: `_cascade_settle` takes `lock(awaited)` then
  `lock(awaiter)` (:290-291); `task_suspend` takes all its locks in sorted-id
  order (:697-702). Contained by the 50-attempt `deadlock_detected` retry loops
  (:1001-1011, :1069-1164), so it costs retries under contention rather than
  correctness.
- **CR-3** `callbacks.awaiter_id` has no foreign key while `awaited_id` does
  (:70-74), so `gc` can leave orphan awaiter rows. Harmless today —
  `_enqueue_resume` returns on a missing task (:250).
- **CR-4** Cron day-of-week `7` and month/day *names* (`JAN`, `MON`) are
  unsupported; both surface as a 400 from `schedule_create` (:856-857) rather than
  a documented error. `_cron_field`'s `*/n`, `a-b/n`, `a/n` handling and the
  dom/dow OR-semantics (:351-355) were checked by hand against Vixie cron and are
  correct.
