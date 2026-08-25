-- =============================================================================
-- The conformance catalogue, as table constraints
-- =============================================================================
-- Every `.state` entry of resonatehq/resonate-specification's property
-- catalogue (spec/02-abstract/properties.lean) that the single-table layout can
-- state declaratively. A `.state` property is "a bad row or a bad join" — which
-- is exactly what a CHECK constraint rejects, provided the join it needs is
-- inside one row.
--
-- That proviso is the whole reason the merge pays here. In the two-table
-- layout a promise and its task are two rows, so every property relating them
-- is a cross-table claim and no CHECK can see it. Merged, they are one row and
-- the claim is intra-row.
--
-- Constraint names are the property names, verbatim: a violation reports the
-- same string the Lean catalogue, the Go checker and the trace checker use.
--
-- Apply AFTER resonate-single.sql:
--   psql -d yourdb -f resonate-single.sql -f constraints.sql
--
-- Not covered here, and why:
--   * `.trans` entries (50 of the 95) — claims about a PAIR of states. No
--     CHECK sees the previous row version; these need triggers or the trace
--     checker.
--   * clock-relative entries (created_at ≤ now, settled_at ≤ now) — `now` is a
--     request parameter, not a column, and a CHECK expression must be
--     IMMUTABLE.
--   * cross-ROW joins (an awaiter naming another promise, outbox rows naming
--     a task) — a CHECK sees one row. Postgres has no array-element foreign
--     key, so these stay procedural or need a trigger.
--   * the three `gaps` entries — they change behaviour. See constraints-gaps.sql.
-- =============================================================================

-- A CHECK runs on every insert and every update of the row, so the SHAPE of the
-- expression matters as much as the claim. Two rules throughout: read a STORED
-- generated column (`target`, `is_timer`, `external`) rather than re-running the
-- jsonb key lookup, and guard a function call behind the cheap test that makes
-- it unnecessary in the ordinary case.

SET search_path TO resonate, public;

-- --- promises: well-formedness ----------------------------------------------

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_created_at_lte_timeout_at
  CHECK (created_at <= timeout_at);

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_pending_created_before_deadline
  CHECK (state <> 'pending' OR created_at < timeout_at);

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_settled_at_lte_timeout_at
  CHECK (settled_at IS NULL OR settled_at <= timeout_at);

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_created_at_lte_settled_at
  CHECK (settled_at IS NULL OR created_at <= settled_at);

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_settled_at_iff_not_pending
  CHECK ((state <> 'pending') = (settled_at IS NOT NULL));

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_pending_has_no_value
  CHECK (state <> 'pending' OR (value_data IS NULL AND value_headers = '{}'::jsonb));

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_deadline_verdict_matches_timer_tag
  CHECK (settled_at IS DISTINCT FROM timeout_at
         OR state = CASE WHEN is_timer THEN 'resolved' ELSE 'rejected_timedout' END);

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_deadline_settlement_has_no_value
  CHECK (settled_at IS DISTINCT FROM timeout_at
         OR (value_data IS NULL AND value_headers = '{}'::jsonb));

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_timer_not_targeted
  CHECK (NOT (is_timer AND target IS NOT NULL));

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_timedout_is_server_owned
  CHECK (state <> 'rejected_timedout' OR settled_at = timeout_at);

-- The awaiter set was a table with a two-column primary key; as an array
-- column its uniqueness is a predicate on the row.
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_callbacks_unique
  CHECK (cardinality(awaiters) < 2 OR _arr_uniq(awaiters));

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_listeners_unique
  CHECK (cardinality(listeners) < 2 OR _arr_uniq(listeners));

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_obligations_require_external
  CHECK (external OR (awaiters = '{}' AND listeners = '{}'));

ALTER TABLE promises ADD CONSTRAINT well_formed_promise_awaiter_is_not_self
  CHECK (NOT (id = ANY (awaiters)));

-- --- tasks: well-formedness -------------------------------------------------
-- All four of these need `retry_at` and `expires_at` to be separate columns.
-- The two-table layout had one `timeout_at` doing both jobs, so it could not
-- state them at all — and, left unstated, it also left a stale lease instant on
-- suspended, halted and fulfilled tasks.

ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_iff_has_pid
  CHECK (task_state IS NULL OR (task_state = 'acquired') = (pid IS NOT NULL));

ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_iff_has_ttl
  CHECK (task_state IS NULL OR (task_state = 'acquired') = (ttl IS NOT NULL));

ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_iff_has_expires_at
  CHECK (task_state IS NULL OR (task_state = 'acquired') = (expires_at IS NOT NULL));

ALTER TABLE promises ADD CONSTRAINT well_formed_task_pending_iff_has_retry_at
  CHECK (task_state IS NULL OR (task_state = 'pending') = (retry_at IS NOT NULL));

ALTER TABLE promises ADD CONSTRAINT well_formed_task_fulfilled_is_cleared
  CHECK (task_state IS DISTINCT FROM 'fulfilled'
         OR (pid IS NULL AND ttl IS NULL AND expires_at IS NULL
             AND retry_at IS NULL AND resumes = '{}'));

ALTER TABLE promises ADD CONSTRAINT well_formed_task_suspended_is_cleared
  CHECK (task_state IS DISTINCT FROM 'suspended'
         OR (pid IS NULL AND ttl IS NULL AND expires_at IS NULL AND retry_at IS NULL));

ALTER TABLE promises ADD CONSTRAINT well_formed_task_halted_is_cleared
  CHECK (task_state IS DISTINCT FROM 'halted'
         OR (pid IS NULL AND ttl IS NULL AND expires_at IS NULL AND retry_at IS NULL));

ALTER TABLE promises ADD CONSTRAINT well_formed_task_suspended_has_no_resumes
  CHECK (task_state IS DISTINCT FROM 'suspended' OR resumes = '{}');

ALTER TABLE promises ADD CONSTRAINT well_formed_task_resumes_unique
  CHECK (cardinality(resumes) < 2 OR _arr_uniq(resumes));

ALTER TABLE promises ADD CONSTRAINT well_formed_task_acquired_version_positive
  CHECK (task_state IS DISTINCT FROM 'acquired' OR task_version >= 1);

-- --- promise ⊕ task: the entries the two-table layout could not state --------

-- A task exists exactly for the targeted promises — a biconditional across two
-- tables, and a single NULL test in one.
ALTER TABLE promises ADD CONSTRAINT consistent_task_iff_targeted_promise
  CHECK ((task_state IS NOT NULL) = (target IS NOT NULL));

-- A settled promise's task is fulfilled.
ALTER TABLE promises ADD CONSTRAINT consistent_settled_promise_has_fulfilled_task
  CHECK (state = 'pending' OR task_state IS NULL OR task_state = 'fulfilled');

-- ...and the converse: a fulfilled task's promise is settled.
ALTER TABLE promises ADD CONSTRAINT consistent_settled_task_promise_settled
  CHECK (task_state IS DISTINCT FROM 'fulfilled' OR state <> 'pending');

-- --- obligations ------------------------------------------------------------

ALTER TABLE promises ADD CONSTRAINT consistent_listener_addresses_deliverable
  CHECK (listeners = '{}' OR _addrs_valid(listeners));

-- --- outbox → task ----------------------------------------------------------
-- `consistent_outbox_execute_names_existing_task` is a foreign key on the
-- two-table layout (outbox.task_id REFERENCES tasks(id)) and the one catalogue
-- entry the split states more directly: merged, there is no task-only key to
-- point at. A generated column recovers it — `task_key` is the id exactly when
-- the row is a task, UNIQUE tolerates the NULLs of the rows that are not, and
-- the foreign key matches only the non-NULL values.
ALTER TABLE promises ADD COLUMN IF NOT EXISTS task_key TEXT
  GENERATED ALWAYS AS (CASE WHEN tags ? 'resonate:target' THEN id END) STORED;

ALTER TABLE promises ADD CONSTRAINT promises_task_key_unique UNIQUE (task_key);

ALTER TABLE outbox ADD CONSTRAINT consistent_outbox_execute_names_existing_task
  FOREIGN KEY (task_id) REFERENCES promises (task_key) ON DELETE CASCADE;

-- --- schedules --------------------------------------------------------------

ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_promise_tags_not_timer_targeted
  CHECK (NOT (COALESCE(promise_tags->>'resonate:timer','') = 'true'
              AND promise_tags ? 'resonate:target'));

ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_created_at_lte_next_run_at
  CHECK (created_at <= next_run_at);

ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_created_at_lte_last_run_at
  CHECK (last_run_at IS NULL OR created_at <= last_run_at);

ALTER TABLE schedules ADD CONSTRAINT well_formed_schedule_last_run_at_lt_next_run_at
  CHECK (last_run_at IS NULL OR last_run_at < next_run_at);

-- --- outbox -----------------------------------------------------------------

ALTER TABLE outbox ADD CONSTRAINT consistent_outbox_unblock_address_deliverable
  CHECK (kind <> 'unblock' OR _addr_valid(address));

-- An unblock message carries a settled promise record. The other half of the
-- entry — that the promise it names is settled IN THE STORE — is a cross-row
-- join and stays outside.
ALTER TABLE outbox ADD CONSTRAINT consistent_outbox_unblock_names_settled_promise
  CHECK (kind <> 'unblock' OR promise->>'state' <> 'pending');

-- `execute` names a task, `unblock` carries a promise: the shape half of
-- consistent_outbox_execute_names_existing_task.
ALTER TABLE outbox ADD CONSTRAINT well_formed_outbox_payload_matches_kind
  CHECK (CASE kind
           WHEN 'execute' THEN task_id IS NOT NULL AND version IS NOT NULL AND promise IS NULL
           WHEN 'unblock' THEN task_id IS NULL AND version IS NULL AND promise IS NOT NULL
         END);
