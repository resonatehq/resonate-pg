# Specula run on resonate-pg

[Specula](https://github.com/specula-org/Specula) finds deep bugs in concurrent
and distributed system code by writing TLA+ specs of the target, model-checking
them, and reproducing violations at the code level. This directory is the output
of one full pass over `resonate.sql` @ `54fe651`.

## Findings

Five bugs, all reproduced against a live Postgres 16 with `resonate.sql` applied
unmodified. Details and suggested fixes in [`confirmed-bugs.md`](confirmed-bugs.md);
the comparison against the reference specification is in
[`spec-comparison.md`](spec-comparison.md).

| | Finding | Found by |
|---|---|---|
| BUG-1 | A listener on an internal promise is never notified, while `promise.get` reports that promise as `rejected_timedout`. `promise.register_listener` has no `external` guard; the two callback writers do. | model checking |
| BUG-2 | `task.create` and `task.continue` grant an execution lease / dispatch a worker against a promise that `task.acquire` refuses with 409; `task.create` also serves an unprojected promise record. | model checking |
| BUG-3 | `resonate:target = ''` creates a task that is dispatched exactly once and can never be redelivered — the creation site tests `IS NOT NULL`, all five redelivery sites test `<> ''`. | code analysis |
| BUG-4 | `task.halt` returns 200 on a task `task.get` already reports `fulfilled` (for which halt is 409). | spec comparison, then model checking |
| BUG-5 | The task retry/lease timeout handlers redispatch a logically dead workflow. Latent — masked by `process_timeouts`' internal ordering, not by a guard. | spec comparison |

Four properties were **verified** exhaustively over the whole reachable state space
(4.7M distinct states): settlement stickiness, promise/task coherence, callback
externality, and no-stranded-suspended-task.

And with the reference specification's guards switched on, **all seven invariants
hold exhaustively** (`MC_fixed.cfg`) — the recommended fixes are verified, not just
argued.

## Layout

```
modeling-brief.md         Phase 1  code analysis -> spec design
spec-comparison.md        resonate.sql vs. resonatehq/resonate-specification
spec/base.tla             Phase 2  TLA+ model of resonate.sql, annotated with file:line
spec/MC.tla, MC.cfg       Phase 2  model configuration
spec/MC_hunt_*.cfg        Phase 2  one config per invariant
spec/MC_fixed.cfg         Phase 3  every reference-spec guard applied
spec/MC_timeout_*.cfg     Phase 3  isolates BUG-5 and its masking
spec/bug-report.md        Phase 3  TLC results and counterexample traces
spec/output/              Phase 3  raw TLC output
repro/repro.sql           Phase 4  executable reproductions
repro/repro-output.txt    Phase 4  recorded output of the above
confirmed-bugs.md         Phase 4  confirmation report, with suggested fixes
```

## Reproducing

Model checking (needs Java 21+ and
[`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases) v1.8.0):

```bash
cd .specula-output/spec
java -XX:+UseParallelGC -cp /path/to/tla2tools.jar tlc2.TLC \
     -workers 4 -config MC_hunt_NoDeadDispatch.cfg MC.tla
```

Each `MC_hunt_<Invariant>.cfg` checks one property. `MC.cfg` checks all seven at
once, in the shipped configuration; `MC_fixed.cfg` turns on the reference spec's
guards and checks that all seven then hold. The violated properties produce
counterexamples in 4 to 6 states; each exhaustive run takes roughly 90-150s on
4 cores.

Bug reproduction (needs Postgres 16+; pg_cron is not required — the script calls
`resonate.process_timeouts(now)` directly, as `test/conformance.py` does):

```bash
psql -d yourdb -f resonate.sql
psql -d yourdb -f .specula-output/repro/repro.sql
```

Each test prints `BUG-n REPRODUCED` or `BUG-n not reproduced`.

## Scope

Two promise ids, one listener address, time horizon 3, versions bounded at 2.
Schedules (S-01..S-04) and cron parsing are not modelled — see
`modeling-brief.md` § 3.2 for what was excluded and why. Each action is modelled
as atomic, which reflects the advisory-lock discipline (`_lock`, `resonate.sql:381`)
rather than verifying it.
