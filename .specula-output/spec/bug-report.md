# Bug Report — TLC model checking of `base.tla`

Specula Phase 3 output. Spec: `.specula-output/spec/base.tla` (model of
`resonate.sql` @ `54fe651`). Model: `MC.tla` / `MC.cfg`, hunt configs
`MC_hunt_<Invariant>.cfg`, raw TLC output in `output/`.

## Model configuration

| Constant | Value | Note |
|---|---|---|
| `Ids` | `{a, b}` | two promise/task ids |
| `Addrs` | `{"L"}` | one listener address |
| `MaxTime` | 3 | time horizon |
| `MaxVersion` | 2 | bound on `task.version` |
| `Retry` | 1 | `_retry_timeout()` |
| `Ttl` | 1 | lease ttl |

BFS, 4 workers, exhaustive within the constraint. Complete state graph:
**3,830,244 distinct states, depth 22**.

## Results

| Invariant | Verdict | States generated | Distinct |
|---|---|---|---|
| `Stickiness` | ✅ holds (exhaustive) | 25,699,032 | 3,830,244 |
| `TaskPromiseCoherence` | ✅ holds (exhaustive) | 25,699,032 | 3,830,244 |
| `CallbacksAreExternal` | ✅ holds (exhaustive) | 25,699,032 | 3,830,244 |
| `NoStrandedTask` | ✅ holds (exhaustive) | 25,699,032 | 3,830,244 |
| `NoStrandedListener` | ❌ **violated** (6-state trace, depth 6) | 100,311 | 39,648 |
| `NoDeadDispatch` | ❌ **violated** (4-state trace, depth 8 search) | 2,965 | 1,830 |

Four of the six properties hold across the whole reachable state space. Two are
violated, both by short traces, both instances of Scenario 1 in the modeling brief.

## Case C-1 — `NoStrandedListener` violated

**Counterexample** (6 states, `output/NoStrandedListener.out`):

| # | Action | Resulting state |
|---|---|---|
| 1 | *initial* | empty |
| 2 | `PromiseCreate(b, toat=1, "plain")` | `b` pending, `external = FALSE`, `timeoutAt = 1` |
| 3 | `RegisterListener(b, "L")` | `listeners = {<<b, "L">>}` |
| 4 | `Tick` | `now = 1` |
| 5 | `Tick` | `now = 2` |
| 6 | `Tick` | `now = 3` — quiesced, invariant fails |

In the final state `now = 3`, the driver has nothing to do (`b` is not `external`,
so `process_promise_timeouts`' `WHERE` clause never selects it — :966, :204-207),
`b`'s row still reads `pending`, `Proj(b) = "rejected_timedout"`, and
`<<b, "L">>` is still in `listeners`. The unblock message was never emitted.

**Cross-reference with code**: `promise_register_listener` (:501-520) checks only
that the awaited exists and is not yet expired. It has no `external` guard. The two
callback writers do — `promise_register_callback` :488 and `task_suspend` :722,
both added by `9187493` for exactly this reason ("an awaiter could be stranded
forever"). The listener writer was not swept by that commit.

**Case classification**: **Case C** — real bug, not a spec fidelity issue. The
guard the model says is missing is textually absent at :501-520.

## Case C-2 — `NoDeadDispatch` violated

**Counterexample** (4 states, `output/NoDeadDispatch.out`):

| # | Action | Resulting state |
|---|---|---|
| 1 | *initial* | empty |
| 2 | `PromiseCreate(a, toat=1, "target")` | `a` pending, task `a` pending v0, execute emitted |
| 3 | `Tick` | `now = 1` — `Proj(a) = "rejected_timedout"`, row still `pending` |
| 4 | `TaskClaim(a)` | task `a` **acquired v1**, `badDispatch = {a}` |

`TaskAcquire(a)` is *disabled* in state 3 (its guard `promises[a].timeoutAt > now`
fails — :613). `TaskClaim(a)`, which models `task_create`'s claim branch, is
enabled, because that branch consults neither the promise state nor its timeout
(:580-594). The model grants a lease that the sibling entry point refuses.

**Cross-reference with code**: confirmed textually. `task_create` :580-594 checks
`p.tags ? 'resonate:target'` (422), that a task row exists (409), and
`t.state` — and nothing about the promise. `task_continue` :803-819 is the same
shape and additionally emits an execute (:816).

**Case classification**: **Case C** — real bug.

## Notes on the properties that held

- `Stickiness` holding is a meaningful positive result: across 3.8M states, no
  interleaving of settle / fulfill / timeout / claim lets a client read a promise
  as settled and later read it as anything else. The `state`+`timeout_at` guard at
  :461 and :759 and the sticky-terminal echo are doing their job.
- `NoStrandedTask` holding means MC-4 in the brief (dropped wakeup) is **refuted**
  within this model: the `_enqueue_resume` / `task_suspend` resume-row deletions do
  not strand a suspended task. The 300-response path at :723-726 is what saves it —
  `task_suspend` re-reads every awaited under lock and refuses to park when any is
  already settled.
- `CallbacksAreExternal` holding confirms `9187493` achieved what it set out to for
  the callback path — which is precisely what makes the listener path's omission a
  gap rather than a design choice.
- MC-3 (fence token survives the lease, `_on_task_lease_timeout` not bumping
  `version`) produced no violation: an expired lease leaves the task `pending`, and
  every action a stale worker could take requires `state = 'acquired'`, so the
  stale generation is fenced by state rather than by version.

## Limitations

- Two ids, one address, horizon 3. Bugs needing three interacting promises or a
  longer causal chain are out of reach.
- Schedules (S-01..S-04) and cron parsing are not modelled (see brief § 3.2).
- Each action is atomic, which models the advisory-lock discipline rather than
  verifying it. Scenario 4 (lock-order inversion) is therefore invisible to this
  model by construction and is reported as a code-review finding.
