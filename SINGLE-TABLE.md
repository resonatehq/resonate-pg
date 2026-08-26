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

## The id carries the origin

`origin_id` is derived from the id, not from the `resonate:origin` tag:

```sql
origin_id TEXT GENERATED ALWAYS AS (split_part(id, ':', 1)) STORED
```

The convention is `foo.1` for a top-level promise, which is its own origin, and
`foo.1:1` for a child of it — at most one colon, everything before it the
origin. One expression covers both shapes, because `split_part` on a
colon-free id returns the whole id.

This is strictly better than checking the tag against the id. A constraint can
only reject a disagreement that a client already sent; a derived column makes
the disagreement unrepresentable. The tag is then redundant, and where a
request still carries one, `promise_create` and `task_create` reject a
mismatch with a **400** — at the door, where a malformed request belongs,
rather than as the 500 a CHECK violation would surface as.

Two consequences worth stating.

**The origin index stops being partial.** `split_part` of a non-null primary
key is never null, so `WHERE origin_id IS NOT NULL` now matches every row and
keeping it would only have misled. (The column carries no `NOT NULL` of its
own: it would be redundant with the expression, and stating it invites the
reader to think it is load-bearing.) A predicate that does exclude the roots — `origin_id <> id` — is not
usable, because the planner cannot prove it from an `origin_id = $1 AND
id <> $1` lookup and the index would go unread. So it indexes every row, and
the cost lands exactly on the workloads that set no origin tag today:

| workload | tag-derived, partial | id-derived, full |
|---|---|---|
| fanout (tags everything) | 32K | 32K |
| lifecycle (tags everything) | 16K | 16K |
| listeners (tags nothing) | 8K | 16K |
| heartbeat (tags nothing) | 8K | 16K |

Where the tag was already being set the index is unchanged; where it was not,
it doubles from a near-empty base. That is the honest trade for an origin that
cannot be wrong.

**A schedule may not carry an origin tag.** A schedule expands a new promise
id on every firing, so a fixed `resonate:origin` could agree with at most one
of them and every later firing would be refused at `promise_create`'s door —
the schedule would silently produce nothing, one firing at a time.
`schedule_create` refuses the tag up front with a 400 instead. Schedules
created before this check may still carry one; they need clearing at the same
time as the id rewrite below.

**It is only correct under the colon convention.** An id from an older
convention — `foo.1.2` for a child, say — computes its own id as its origin,
silently, and `gc` and `_preload` both read `origin_id`. Any store holding
pre-convention ids needs them rewritten before this column is introduced, not
after.

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

## Measurements

`test/bench.py` runs identical workloads against three variants — the two-table
baseline, the merged schema bare, and the merged schema with `constraints.sql`
— and `test/report.py` cuts the result globally, by RPC kind and by response
status. Repeats round-robin across variants and the figure is the median of
seven, with the min/max spread printed beside it.

**Read the write counters, not the clock.** This container's p50 varies ±10-45%
between repeats, so only latency gaps wider than the printed band mean
anything. WAL bytes, rows written and index size are deterministic and say the
same thing more precisely.

### Global — merged vs the two-table baseline (constraints off)

| workload | p50 | WAL | rows written | index size |
|---|---|---|---|---|
| fanout | **−35%** | **−16%** | −33% | **−44%** |
| reads | **−25%** | −12% | −33% | −24% |
| lifecycle | **−12%** | −9% | −33% | −23% |
| rejects | ±0% | ±0% | — | −24% |
| listeners | +20% | **−10%** | −33% | −28% |
| heartbeat | +23% | **+35%** | −3% | −17% |

Merged writes fewer rows in every workload and less WAL in five of six. It is
smaller on disk in all six, and the index saving is the large one — the split
carries a primary key per obligation table, and those disappear.

### Where it wins, by request kind

| request | Δ p50 | why |
|---|---|---|
| `promise.settle` (fanout) | **−45%** | `_cascade_settle` reads the obligations out of the row it just settled instead of scanning two tables and issuing three DELETEs |
| `task.acquire` (fanout) | **−45%** | one row lookup and one UPDATE, not two of each |
| `task.get` | **−27%** | the `resumes` count was a correlated subquery per serialisation; now a column read |
| `promise.get` | −12% | |

The fanout gain is the interesting one, because fan-out is the shape the
obligation tables were there to serve: one parent awaiting eight children is
eight callback registrations and then eight resume records. It is where the
split should look best.

What changes is not that the same writes got cheaper but that most of them stop
being writes at all. Per-table counters, 200 parents x 8 children:

| | split | merged |
|---|---|---|
| `promises` | 1800 ins, 1600 upd, 0 HOT | 1800 ins, 5200 upd, **2627 HOT** |
| `tasks` | 1800 ins, 2200 upd, 0 HOT | — |
| `callbacks` | 1600 ins | — |
| `task_resumes` | 1600 ins | — |
| total row-writes | 12 800 | **9 000** |

The merged layout does *more* updates — an obligation that was an INSERT into a
skinny table is now an UPDATE of the promise row — and far fewer inserts,
because the task, callback and resume rows stop existing.

Half of those updates are HOT. A normal update writes a new entry into **every**
index on the table, not just the ones whose columns changed; a Heap-Only Tuple
update writes none, provided no indexed column moved and the new version fits on
the same page. `awaiters` and `resumes` appear in no index, so an append
qualifies. The split cannot match this on the same work for a reason that has
nothing to do with its indexes: it records obligations with INSERTs, and an
insert is never HOT.

Page room is the other condition, and it is why the ratio is 50.5% rather than
100%. `fillfactor = 70` on `promises` takes it to 57.7% and the index from 352K
to 320K — a real knob, trading table size for index size, worth measuring on
real data rather than adopting on principle.

### Where it loses, by request kind

| request | Δ p50 | Δ WAL | why |
|---|---|---|---|
| `task.heartbeat` | **+24%** | **+35%** | the wide row, exactly as predicted |
| `promise.register_listener` | +21% | −10% | appending to an array is an UPDATE of a wide row; the split did an INSERT into a two-column table |

**The heartbeat regression is not the payload.** The obvious mitigation — a
lower `toast_tuple_target` to push `param_data`/`value_data` out of line — moved
WAL by nothing at all, because a repeating payload compresses away and was
never inline. The cost is the row's width in columns and indexes, not its
bytes, and `expires_at` is indexed so a heartbeat can never HOT-update. The
escape hatch, if this workload dominates, is a skinny side table for the lease
— a five-into-two merge rather than five-into-one — at the price of the four
`well_formed_task_*` constraints that need those columns in the row.

### What the constraints cost

Read paths pay nothing: `promise.get`, `task.get` and the 409 guard path are
within noise of the unconstrained schema, because a CHECK runs on row
modification and those modify nothing. Write paths pay. Bisected on lifecycle
(five interleaved repeats, ±4-18%):

| variant | p50 | WAL | what it adds |
|---|---|---|---|
| merged, bare | 634µs | 1.70MB | — |
| + 31 plain CHECKs | 798µs | 1.70MB | +26%, no extra writes |
| + 4 function CHECKs | 849µs | 1.70MB | +8% |
| + the outbox foreign key | 869µs | **1.98MB** | +11% latency, **+16% WAL** |

Bisected again on `listeners`, which puts six addresses on every promise:

| variant | p50 | what it adds |
|---|---|---|
| merged, bare | 538µs | — |
| + 31 plain CHECKs | 721µs | +34% |
| + the outbox foreign key | 764µs | +6% |
| + 4 function CHECKs | 923µs | **+28%** |

The function CHECKs cost 8% on lifecycle and 28% here, and the difference is
the point: the cardinality guard in front of `_arr_uniq` and `_addrs_valid`
makes them free for an array of fewer than two elements, which is the ordinary
case, and does nothing once a promise really does carry six listeners. That
cost scales with obligation fan-in, so it is bounded by the same thing that
bounds the array length — read it as "uniqueness costs where there is
something to compare", not as a flat tax.

Three things are worth knowing before deciding.

**The expression shape mattered more than the claims.** The first cut of
`constraints.sql` cost +75% on lifecycle. Three uniqueness CHECKs called a SQL
function whose body is a subquery — not inlinable, so it ran on every row
modification — and two re-ran a jsonb key lookup that a STORED generated column
already held. Guarding the calls behind a cardinality test and reading `target`
and `is_timer` instead took it to +18% without dropping a single property.

**The foreign key is the one structural cost.** It is the only constraint that
adds an index, and `promises_task_key_unique` takes a new entry on every
non-HOT update of a promise — hence the WAL, not just the CPU. It buys exactly
one catalogue entry, `consistent_outbox_execute_names_existing_task`. If the
constraint budget has to come down, drop that one first: it is 11% of the
latency and all of the extra WAL, for 1 of 38 properties.

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
