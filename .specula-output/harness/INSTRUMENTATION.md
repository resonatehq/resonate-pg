# Harness — how to adjust the instrumentation

Specula Phase 2.5 output for resonate-pg. Everything here exists to produce the
NDJSON traces in `../traces/` that `../spec/Trace.tla` replays.

## Layout

```
instrument.py                  inserts the emit calls into resonate.sql
patches/instrumentation.patch  the resulting diff, regenerated on every run
src/trace.sql                  trace table, state snapshot, emit, harness tuning
src/scenarios.sql              five workloads driving the real wire entrypoint
run.sh                         instrument -> load -> run scenarios -> export NDJSON
build/                         generated: the instrumented resonate.sql
```

## Running

```bash
PGURL="postgresql://user@host/db" ./run.sh
```

Defaults to the local socket the Specula run used (`-h /tmp -p 5433 -U postgres`).
Postgres 16+; pg_cron is not needed — the scenarios call `process_timeouts()`
directly, as `test/conformance.py` does. Each run drops and recreates the
`resonate` schema.

## How the instrumentation works

`instrument.py` holds one anchor per emit point. Each anchor is a verbatim slice
of `resonate.sql` containing a `<<EMIT>>` marker on its own line; the marker is
replaced by `PERFORM _trace_emit('<Event>', p_now);`. The script asserts every
anchor matches **exactly once** and exits non-zero otherwise, so a refactor in
`resonate.sql` produces a loud failure rather than a silently truncated trace.

Emits sit inside the real handler bodies, after the last state-mutating
statement, so the snapshot is the action's post-state. Nothing is reimplemented.

## Adjusting

**Moving an emit**: edit the anchor in `instrument.py`. Keep `<<EMIT>>` after the
last statement that writes state, and before any `RETURN`.

**Adding an action**: add an anchor + event name to `POINTS`, add a matching
wrapper to `../spec/Trace.tla`, and list it in `../spec/instrumentation-spec.md`.

**Adding a field to the snapshot**: extend `_trace_state()` in `src/trace.sql`
*and* the corresponding check in `Trace.tla`'s `ValidatePostState`. Do not add a
field to one without the other — a captured-but-unchecked field is dead weight,
and a checked-but-uncaptured field makes the conditional vacuously true.

**Anchor no longer matches** (after editing `resonate.sql`): `run.sh` fails with
`anchor for <Event> matched 0 times`. Re-copy the surrounding lines from the
current `resonate.sql` into the anchor.

## Scenarios

| Scenario | Covers |
|---|---|
| `suspend_resume` | the durable-execution loop: acquire, suspend on a child, child fulfils, root resumed, reacquired, fulfilled |
| `timeouts` | every internal transition: lease expiry, retry redelivery, promise timeout cascading into a resume |
| `claim_halt_continue` | `task.create`'s claim branch, release, halt, continue |
| `listener_callback` | both wait-registration paths, an unblock on settlement, and the suspend-300 path |
| `external_settle` | `promise.settle` driven from outside, cascading to a listener and a suspended awaiter |

Between them the five cover all 16 instrumented emit points.

## Constraints the scenarios must respect

- **Ids are `a` and `b` only**, and the listener address is `poll://any@L` —
  these are `Trace.cfg`'s `Ids` and `Addrs`. New ids need the constant widened.
- **Times are small integers** passed as `resonate:debug_time`, under
  `Trace.cfg`'s `MaxTime`.
- **`task.create` is only used to claim an existing pending task.** `base.tla`
  models T-02's claim branch (`TaskClaim`) but not its create-a-new-promise
  branch, so a `task.create` on an unknown id has no wrapper to match it.
- Versions must be tracked by hand across a scenario; a wrong version yields a
  409 and simply emits nothing, which shows up as a short trace.

## Known capture notes

- `tasks.timeout_at` is `NULL` on a task created already-fulfilled; the snapshot
  maps `NULL` to `0` to match `base.tla`'s `NoTask`.
- `_retry_timeout()` is redefined to `1` (from `5000`) so the scenarios run in
  the model's time domain. This is a timing constant, not protocol logic, and is
  declared in each trace's `config` line.
- The outbox is never drained, so `execs`/`unblocks` accumulate exactly as the
  spec's `outbox` does.
