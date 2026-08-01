# resonate-pg vs. the reference specification

A three-way comparison: **`resonatehq/resonate-specification`** (the normative
Lean 4 machine) against **`resonate.sql`** @ `54fe651` and against the TLA+ model
in `.specula-output/spec/base.tla`.

## Which spec version is normative

| ref | date | note |
|---|---|---|
| `main` @ `434e40f` | 2026-07-30 | "The abstract twins: projected and materialized on the coalesced state (#5)" |
| **`claude/close-the-square`** | **2026-08-01** | **14 ahead of `main`, 0 behind — the newest state, and the one carrying the new validation rules** |
| `tla+` | 2026-07-15 | an older alternate encoding in TLA+; predates both fixes and predates `resonate:external` |
| `fix/task-create-serve-projected-record` | 2026-07-26 | superseded — its fix is subsumed by `close-the-square` |

Two commits on `claude/close-the-square` change request validation:

- **`6ddfab7` "external-only waiters everywhere; the square, closed"** (Jul 30) —
  `promise.register_listener` refuses internal promises with 422, in all four
  machines.
- **`3e8a1d6` "no new work for the dead: lease and retry decide on the projected
  state"** (Jul 30) — the lease-expiry and retry transitions consult the
  projected promise state before re-pending or redispatching.

`resonate.sql` predates both.

## The rule, as the spec now states it

`spec/03-concrete/state.lean` on `close-the-square`:

```lean
/-- External promises — explicitly tagged `resonate:external = "true"`,
    targeted, or timers — may have awaiters and carry an armed (durable)
    timeout; the timeout transition guarantees their awaiters are never
    stranded. Internal promises must not have awaiters — ENFORCED: both
    registration paths (`register_callback`, `register_listener`) refuse
    internal promises with `422` — their deadlines are projection-only,
    so an obligation recorded on one could never be discharged. -/
```

The word `ENFORCED` and the parenthetical naming both registration paths are new
in `6ddfab7`. Before it, the same docstring said only "Internal promises must not
have awaiters", stated as a convention rather than a checked guard — which is
exactly the state `resonate.sql` is in today.

And the second rule, from `3e8a1d6`'s handler comments:

> TIMEOUT ALWAYS WINS, extended to reassignment and redispatch. Cleanup of a
> dead task belongs to the promise-timeout transition alone.

## Divergences

Every row below was reproduced against a live Postgres 16 —
see `repro/repro.sql`, output in `repro/repro-output.txt`.

| # | Action | Reference spec (newest) | `resonate.sql` | Reproduced |
|---|---|---|---|---|
| 1 | **P-05** `promise.register_listener` | `if !pAwaited.external then return 422` | no `external` guard (:501-520) | BUG-1 |
| 2 | **T-02** `task.create`, claim branch | `if p.state == .pending ∧ p.timeoutAt > now` gates the claim | no promise check at all (:580-594) | BUG-2 |
| 3 | **T-02** `task.create`, response | serves `(p.project now).toRecord` | serves `_promise_json_raw(p)` — deliberately unprojected (:596-597) | BUG-2 |
| 4 | **T-10** `task.continue` | `if p.state != .pending ‖ p.timeoutAt ≤ now then 409` | no promise check (:803-819) | BUG-2b |
| 5 | **T-09** `task.halt` | `if !(p.state == .pending ∧ p.timeoutAt > now) then 409` | never loads the promise (:789-801) | BUG-4 |
| 6 | `onTaskRetryTimeout` / `onTaskLeaseTimeout` | decision goes through the projected state | reads the promise only for its target (:904-937) | BUG-5 (masked) |

### 1 — P-05, the listener guard (BUG-1)

The spec's reasoning is the same one I derived from the counterexample, in their
vocabulary:

> without the guard the machine accepted an obligation its transition relation
> cannot discharge (an internal promise that dies by deadline is settled by
> projection only — no tau ever emits the unblock)

That is the 6-state TLC trace in `spec/bug-report.md`, restated. The fix is the
one I recommended — 422, positioned after the 404 and before the state check,
mirroring P-04.

### 2, 3 — T-02, both halves (BUG-2)

The claim branch on `close-the-square` carries the gate *and* an explicit comment:

```lean
-- Re-acquisition is gated on the PROJECTED promise: a logically
-- settled promise serves the projected pair — fulfilled task,
-- settled record — exactly as the expired fresh-create path does.
-- No lease is ever armed on a logically dead task.
```

And the dead branch returns `(p.project now).toRecord`, closing the unprojected
response. The earlier `fix/task-create-serve-projected-record` branch states the
consequence of *not* doing so exactly as I observed it empirically:

> made settled-promise stability falsifiable on the wire: GET could answer
> rejectedTimedout and a later task.create could answer pending for the same
> promise, on a valid trace.

That is `promise.get → rejected_timedout` beside `task.create → pending` in
`repro-output.txt`.

### 5 — T-09 `task.halt` (BUG-4, new)

I did not flag this in the first pass; the spec's T-09 does, and it names the
principle:

> Branching on the raw stored task here would make the stored-vs-projected
> divergence observable — the one thing the projection discipline forbids.

`resonate.sql` makes it observable: at one instant `task.get` reports the task
`fulfilled` (:540-545, the projection) while `task.halt` returns `200` and halts
it — even though halt-on-`fulfilled` is `409` (:796). Two endpoints, one task, one
instant, contradictory answers.

### 6 — the task timeout handlers (BUG-5, new, latent)

`_on_task_retry_timeout` and `_on_task_lease_timeout` load the promise purely for
`p.target` and never consult its state. Calling `process_task_timeouts(9000)`
directly on a lease-expired task whose promise died at 3000 re-pends the task and
emits an `execute` — a dispatch for a workflow that cannot be fulfilled.

**But through the shipped driver this is masked.** `process_timeouts` (:997-1012)
drains `process_promise_timeouts` *before* `process_task_timeouts`, and every task
belongs to a targeted — hence external — promise, so the promise loop settles it
and the cascade fulfils the task before the task loop looks. Verified both ways in
`repro.sql`. So resonate.sql is correct here **by sequencing, not by guard**:
correct today, and silently dependent on an ordering that nothing states or tests.
The spec, whose timeout transitions are independent τ rules with no such ordering,
had to close it with a guard — which is why it shows up there as a real defect
("the sole source of the doomed-dispatch message surplus") and here as a latent one.

## What this changes about the TLA+ model

The model now carries three switches (`base.tla`) so it can run as either server:

| constant | `FALSE` / shipped | `TRUE` / spec |
|---|---|---|
| `ListenerExternalGuard` | P-05 accepts any awaited | P-05 requires `external` |
| `PromiseLivenessGuard` | T-02 claim, T-09, T-10 and the timeout handlers ungated | all gated on `Proj(i) = "pending"` |
| `SequencedDriver` | task-timeout handlers may run with a promise timeout still due (models `process_task_timeouts` alone) | — |

`MC.cfg` runs the shipped server (`SequencedDriver = TRUE`, matching
`process_timeouts`); `MC_unsequenced.cfg` drops the sequencing;
`MC_fixed.cfg` turns every spec guard on.

Two honest corrections to the first pass:

1. **The model could not have found BUG-2's response half (row 3).** `obs` records
   `Proj(i)`, not what each handler actually returned, so a handler serving an
   unprojected record is invisible to it. `Stickiness` held for that reason, not
   because the property is true on the wire. Modelling response payloads is what
   the spec's own stability theorem does, and is the right next extension.
2. **BUG-4 and BUG-5 were reachable in the first model and I did not instrument
   for them.** The actions existed; no invariant named their bad outcome.
   `NoHaltOnDead` and the extended `badDispatch` now do, and both violate
   immediately.

## Invariant vocabulary, side by side

The `tla+` branch's `ServerInv.cfg` carries 28 invariants, nearly all *structural
well-formedness* (`PromiseWithTargetHasTask`, `LeaseTimeoutOnlyForAcquiredTask`,
`TaskHasAtMostOneTimeout`). Mine are *observer-facing safety* properties. The two
sets are complementary and overlap at three points:

| reference invariant | mine |
|---|---|
| `SuspendedTaskHasCallback` | `NoStrandedTask` |
| `SettledPromiseHasFulfilledTask` | `TaskPromiseCoherence` |
| `SettledPromiseHasNoCallbacks` / `CallbackAwaiterIsPending` | `CallbacksAreExternal` |

Its `NonExternalPromiseHasNoTimeout` is worth adopting: the reference never *arms*
a timeout for an internal promise, so the obligation cannot exist. `resonate.sql`
stores a `timeout_at` for every promise (`NOT NULL`, :42) and merely declines to
enforce it — the structural root of BUG-1. Its scope, incidentally, is close to
mine (`PromiseIds = {"p1","p2"}`, `MaxTime = 3`), which is some independent
validation of the bounds.

## Bottom line

Five of the six divergences are `resonate.sql` lagging validation rules the spec
now states explicitly; the sixth (row 6) is latent, masked by driver ordering.
Nothing in the newest spec contradicts a finding from the first pass — it
confirms three of them, supplies the normative wording for the fixes, and adds
two I had missed.
