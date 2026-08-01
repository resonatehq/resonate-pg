# Instrumentation Spec — resonate-pg

Specula Phase 2 output. Maps every `base.tla` action to the point in
`resonate.sql` where the harness emits its trace event. Consumed by Phase 2.5
(`../harness/`) and Phase 3A (`Trace.tla`).

Target: `resonate.sql` @ `54fe651` (the shipped code, which is what `base.tla`
models — the guard switches are all `FALSE` during trace validation).

## Category

**Category A — distributed / message-passing.** One append-only trace table
stands in for the mutex-protected NDJSON writer. Every action is one Postgres
transaction holding advisory locks on the ids it touches, so the trace table's
identity column is a faithful total order over committed actions. No probe
effect worth modelling: the emit is one INSERT inside a transaction that already
does several.

## Emit discipline

Each emit sits **inside the real handler body, after the last statement that
mutates state**, so the recorded snapshot is that action's post-state. Emits are
inserted by `../harness/instrument.py`, which fails loudly if any anchor does not
match exactly once. No handler logic is copied, moved or reimplemented.

## Action → code mapping

| `base.tla` action | Event name | `resonate.sql` function | Emit point |
|---|---|---|---|
| `PromiseCreate` | `PromiseCreate` | `promise_create` :393 | after the pending insert (+ task/execute), :422 |
| `PromiseCreate` | `PromiseCreate` | `promise_create` :393 | after the already-expired insert, :433 |
| `PromiseSettle` | `PromiseSettle` | `promise_settle` :442 | after `_cascade_settle`, :466 |
| `RegisterCallback` | `RegisterCallback` | `promise_register_callback` :471 | after the callback insert, :493 |
| `RegisterListener` | `RegisterListener` | `promise_register_listener` :501 | after the listener insert, :516 |
| `TaskClaim` | `TaskClaim` | `task_create` claim branch :588 | after the acquiring UPDATE, :590 |
| `TaskAcquire` | `TaskAcquire` | `task_acquire` :600 | after the acquiring UPDATE, :617 |
| `TaskSuspend` (200) | `TaskSuspend` | `task_suspend` :673 | after the suspending UPDATE, :733 |
| `TaskSuspend` (300) | `TaskSuspend300` | `task_suspend` :673 | after the resume-row delete, :724 |
| `TaskFulfill` | `TaskFulfill` | `task_fulfill` :739 | after `_cascade_settle`, :763 |
| `TaskRelease` | `TaskRelease` | `task_release` :767 | after the re-pend + emit, :785 |
| `TaskHalt` | `TaskHalt` | `task_halt` :789 | after the halting UPDATE, :799 |
| `TaskContinue` | `TaskContinue` | `task_continue` :803 | after the re-pend + emit, :817 |
| `OnPromiseTimeout` | `OnPromiseTimeout` | `_on_promise_timeout` :890 | after `_cascade_settle`, :901 |
| `OnTaskRetryTimeout` | `OnTaskRetryTimeout` | `_on_task_retry_timeout` :904 | after the re-arm + emit, :918 |
| `OnTaskLeaseTimeout` | `OnTaskLeaseTimeout` | `_on_task_lease_timeout` :921 | after the re-pend + emit, :936 |

Two `base.tla` actions are deliberately not instrumented:

| Action | Why |
|---|---|
| `Tick` | Time is a caller-supplied argument (`resonate:debug_time`, :1063), not a transition. Modelled as a silent action in `Trace.tla`, constrained to fire only while the spec clock is behind the next event's. |
| `Dequeue` | `dequeue_execute` / `dequeue_unblock` (:1235-1265) are worker-side reads. The scenarios never drain the outbox, so the recorded outbox and the spec's agree without it. |

## State capture level

**Full** at every point. The snapshot (`harness/src/trace.sql`, `_trace_state`)
records every `base.tla` state variable:

| Trace field | Spec variable | Source |
|---|---|---|
| `promises[id].{state,timeoutAt,external,isTimer,hasTarget}` | `promises` | `resonate.promises`, incl. the generated `external` column |
| `tasks[id].{state,version,timeoutAt}` | `tasks` | `resonate.tasks`; `NULL` timeout maps to `0`, matching `NoTask` |
| `callbacks` | `callbacks` | `resonate.callbacks` |
| `listeners` | `listeners` | `resonate.listeners` |
| `resumes` | `resumes` | `resonate.task_resumes` |
| `execs` | `outbox` (execute) | `resonate.outbox WHERE kind='execute'` |
| `unblocks` | `outbox` (unblock) | `resonate.outbox WHERE kind='unblock'` |

`Trace.tla`'s `ValidatePostState` checks all seven — there is no weak-validation
variant and no field is captured but unchecked.

Not captured, because `base.tla` does not model them: promise `param`/`value`
payloads, `settled_at`/`created_at`, task `pid`/`ttl`, schedules, and the
outbox's `seq`/`created_at`.

## Harness tuning

| Setting | Value | Reason |
|---|---|---|
| `_retry_timeout()` | `1` instead of `5000` | puts the scenarios in the model's time domain (`Retry = 1`) |
| `ttl` argument | `1` | same, for `Ttl = 1` |
| ids | `a`, `b` | match `Ids` |
| listener address | `poll://any@L` | P-05 validates the address form (:507), so a bare `L` is a 400 |
| clock | small integers via `resonate:debug_time` | makes `now` directly comparable to the spec's |

Only the first is a change to the system; it is a timing constant, not protocol
logic, and it is declared in each trace's `config` line.
