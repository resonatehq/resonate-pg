<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./assets/resonate-banner.png">
  <img alt="Resonate" src="./assets/resonate-banner-light.png">
</picture>

# One table

`resonate-single.sql` collapses five tables into one. `promises`, `tasks`,
`task_resumes`, `callbacks` and `listeners` become a single `promises` table;
`outbox` and `schedules` stay where they are. Seven tables become three.

The argument for it is not that joins are expensive. It is that the
specification's abstract machine is already shaped this way:

```lean
structure PromiseObject where
  ...
  callbacks : List String := []
  listeners : List String := []

structure TaskObject where
  ...
  resumes   : List String   := []
```

`spec/02-abstract/state.lean` carries obligations on the promise and resumes on
the task, and a task exists exactly for the promises tagged `resonate:target`.
The two-table layout took that state apart across five relations; this one puts
it back. Everything below follows from that.

## What changed in the schema

| two-table | single-table |
|---|---|
| `tasks` row joined on a shared id | `task_state`, `task_version`, `ttl`, `pid` columns, NULL when the promise has no `resonate:target` |
| `callbacks(awaited_id, awaiter_id)` | `awaiters TEXT[]` on the awaited promise |
| `listeners(awaited_id, address)` | `listeners TEXT[]` on the awaited promise |
| `task_resumes(task_id, awaited_id)` | `resumes TEXT[]` |
| `tasks.timeout_at` (retry **and** lease) | `retry_at` **and** `expires_at` |

Two of those deserve their own note.

**`timeout_at` had to split.** `TaskObject` has both `retryAt` and `expiresAt`,
and four catalogue entries quantify over them separately —
`well_formed_task_acquired_iff_has_expires_at`,
`well_formed_task_pending_iff_has_retry_at`, and the two `*_is_cleared`
entries. One column cannot state any of them. Left unstated, the two-table
implementation also leaves a stale lease instant behind on every suspended,
halted and fulfilled task: `task_suspend` and `task_halt` clear `pid` and `ttl`
but not `timeout_at`, and nothing reads it again because the index that scans
it is partial on `state IN ('pending','acquired')`. Dead data, not a live bug —
but the constraint is what makes it impossible rather than merely unread.

**`resumes` stayed a list.** The obvious move is a boolean: nothing in
`resonate.sql` reads which promise resumed a task, only how many. But the wire
field is a count, and it is a count in the specification too —
`TaskObject.toRecord` publishes `resumes := t.resumes.length`. Narrowing the
column to a boolean narrows the wire contract with it, and
`well_formed_task_resumes_unique` and `well_formed_task_suspended_has_no_resumes`
both quantify over the list. Since the array is empty or one element in the
ordinary case, it costs nothing to keep it honest.

**The awaiter fan-in is not the risk it looks like.** `callbacks` keys on the
*awaited* promise, and a parent awaiting 500 children is 500 rows with 500
different `awaited_id`s — each child's array holds one element. Fan-out spreads
across rows; only fan-in lengthens an array, and that is the rare shape.

## The one behavioural divergence

`_cascade_settle` in the two-table layout eagerly deletes the settling
promise's *own* pending registrations — "I am settled, stop waiting on
anything" — using `idx_callbacks_awaiter_id` for the reverse lookup. An array
column has no reverse index, so the merged layout drops that sweep and leaves
the settled awaiter as a tombstone.

It is unobservable. `_enqueue_resume` loads the awaiter's task and matches
`suspended` or `pending/acquired/halted`; a fulfilled task matches neither and
the call is a no-op. Nothing on the wire exposes the awaiter set. The cost of a
tombstone is one extra advisory lock and row read when the awaited promise
eventually settles.

`test/model.py` canonicalises it away — on both sides, so the comparison is
about behaviour rather than about the sweep — and says so where it does.

## Settlement became one statement

Settling a promise and fulfilling its task used to be two UPDATEs against two
tables. In one row, two statements leave a settled promise beside an acquired
task between them, and `consistent_settled_promise_has_fulfilled_task` forbids
exactly that. A CHECK constraint, unlike a foreign key, cannot be deferred past
an intermediate state, so the constraint forces the two writes together —
`_settle_row` does the settle, the fulfilment and the obligation discharge in
one statement, and `_cascade_settle` walks the obligations from the caller's
pre-settle copy.

This is the merge paying for itself in a way that has nothing to do with
performance: the constraint rejected a shape the old code could reach, and the
fix is strictly better code.

## The catalogue as constraints

`constraints.sql` states **38 of the 45** `.state` entries of the conformance
catalogue as `CHECK`, `UNIQUE` and `FOREIGN KEY` constraints, named after the
properties, so a violation reports the same string the Lean catalogue and the
trace checker use. The two-table layout can state **28**.

Run `python3 test/coverage.py` for the full table. The ten the merge unlocks:

| property | why the split could not state it |
|---|---|
| `consistent_task_iff_targeted_promise` | a biconditional across two tables |
| `consistent_settled_promise_has_fulfilled_task` | relates `promises.state` to `tasks.state` |
| `consistent_settled_task_promise_settled` | the converse, same problem |
| `well_formed_task_acquired_iff_has_expires_at` | needs `expires_at` as its own column |
| `well_formed_task_pending_iff_has_retry_at` | needs `retry_at` as its own column |
| `well_formed_task_suspended_is_cleared` | needs both |
| `well_formed_task_halted_is_cleared` | needs both |
| `well_formed_task_fulfilled_is_cleared` | its `resumes` clause joins `task_resumes` |
| `well_formed_task_suspended_has_no_resumes` | joins `task_resumes` |
| `well_formed_promise_obligations_require_external` | joins `callbacks`/`listeners` to `promises.external`, and a foreign key cannot reference a partial unique index |

Nothing is lost. `consistent_outbox_execute_names_existing_task` is a plain
foreign key on the split (`outbox.task_id REFERENCES tasks(id)`) and has no
task-only key to point at once merged — recovered with a generated column that
is the id exactly when the row is a task, so `UNIQUE` tolerates the NULLs of
the rows that are not and the foreign key matches only the rest.

The seven that stay out, and why:

- **clock-relative** (2) — `created_at ≤ now`, `settled_at ≤ now`. `now` is a
  request parameter, not a column, and a CHECK expression must be IMMUTABLE.
- **cross-row** (5) — an awaiter naming another promise, an outbox row compared
  against a task's version or a promise's target tag, a suspended task holding
  a rung somewhere else. A CHECK sees one row, and Postgres has no
  array-element foreign key. Merging does not help these; only triggers or the
  trace checker do.

The 50 `.trans` entries are out of reach by construction: no CHECK sees the
previous version of the row.

### The three known gaps

`constraints-gaps.sql` is separate and off by default. The specification
records three constraints as true of the protocol and false of the machine —
`ttl > 0`, a non-empty `resonate:target`, a `resonate:delay` before the
deadline — and asks implementations to enforce them at their doors anyway. The
randomised run reaches all three on the stock server in both layouts, so they
are reachable here too. Turning them on changes behaviour, and a CHECK raise
surfaces as a 500 rather than the 400 a door check would give, so enforce them
at the doors first and keep these as the backstop.

## Two conformance bugs, found on the way

Both predate this work and both are fixed in **both** layouts, so the
comparison stays apples-to-apples.

**`promise.register_listener` had no `external` guard.** `register_callback`
refuses a non-external awaited with 422 — its timeout is not enforced, so an
awaiter could wait forever — and the spec's `promiseRegisterListener` refuses
it identically. `resonate.sql` omitted the check, so a listener could be stored
on an internal promise, violating
`well_formed_promise_obligations_require_external`.

**Three doors accepted `Tags.timerTargeted`.** `resonate:timer` and
`resonate:target` together are a 400 in the spec at `promise.create`,
`task.create` and `schedule.create`. All three accepted it, producing states
that violate `well_formed_promise_timer_not_targeted` and
`well_formed_schedule_promise_tags_not_timer_targeted`.

## Testing

```bash
psql -d res_two -f resonate.sql
psql -d res_one -f resonate-single.sql -f constraints.sql

python3 test/differential.py --programs 50 --steps 250 --gc-every 25
python3 test/coverage.py
python3 test/bench.py --n 2000
```

`test/differential.py` drives both layouts with one randomised request stream
on one explicit clock — `resonate:debug_time` makes every timestamp
reproducible — and after every request asserts that the responses are
identical, that the canonical `ServerState` projections are equal, and that
neither store violates a catalogue property. It reaches every RPC kind, both
timeout sweeps, and `gc`.

`test/model.py` holds the projection and a transcription of all 45 `.state`
entries, so the properties are checked against **both** stores, not just the
new one.
