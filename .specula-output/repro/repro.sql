-- =============================================================================
-- Specula Phase 4 · reproduction tests for the five confirmed findings
-- =============================================================================
--   psql -d yourdb -f resonate.sql
--   psql -d yourdb -f .specula-output/repro/repro.sql
--
-- Each test drives resonate_rpc with an explicit `resonate:debug_time` so the
-- clock is deterministic, and calls resonate.process_timeouts(now) in place of
-- the pg_cron driver.  Every test prints PASS (behaviour as specified) or
-- BUG (finding reproduced).
-- =============================================================================

\set QUIET on
\pset pager off
SET client_min_messages TO WARNING;

CREATE OR REPLACE FUNCTION pg_temp.reset() RETURNS void LANGUAGE sql AS $$
  TRUNCATE resonate.outbox, resonate.listeners, resonate.callbacks,
           resonate.task_resumes, resonate.tasks, resonate.schedules,
           resonate.promises CASCADE;
$$;

CREATE OR REPLACE FUNCTION pg_temp.rpc(kind text, data jsonb, now bigint)
  RETURNS jsonb LANGUAGE sql AS $$
  SELECT resonate.resonate_rpc(jsonb_build_object(
    'kind', kind,
    'head', jsonb_build_object('resonate:debug_time', now::text),
    'data', data));
$$;

-- =============================================================================
-- BUG-1 · a listener on an internal promise is never notified, although
--         promise.get reports that promise as settled.
--
--   P-05 promise.register_listener (resonate.sql:501-520) has no `external`
--   guard, but process_promise_timeouts only enforces timeouts for external
--   promises (_promise_timed, resonate.sql:204-207).  P-04 and T-06 both
--   return 422 for exactly this situation (resonate.sql:488, 722).
-- =============================================================================
\echo ''
\echo '=== BUG-1: listener stranded on an internal promise ==='
SELECT pg_temp.reset();

-- t=1000: a plain promise -- no target, no timer, no resonate:external tag.
SELECT pg_temp.rpc('promise.create',
       '{"id":"latent","timeoutAt":2000}'::jsonb, 1000)->'head'->>'status'
       AS create_status;

-- Someone subscribes to it.  Accepted with 200.
SELECT pg_temp.rpc('promise.register_listener',
       '{"awaited":"latent","address":"poll://any@worker1"}'::jsonb, 1000)
       ->'head'->>'status' AS listener_status;

-- A callback on the same promise is refused with 422 -- the asymmetry.
SELECT pg_temp.rpc('promise.create',
       '{"id":"awaiter","timeoutAt":900000,"tags":{"resonate:target":"poll://any@w"}}'::jsonb,
       1000)->'head'->>'status' AS awaiter_created;
SELECT pg_temp.rpc('promise.register_callback',
       '{"awaited":"latent","awaiter":"awaiter"}'::jsonb, 1000)
       ->'head'->>'status' AS callback_status_expect_422;

-- t=9000: long past timeoutAt.  Run the driver to a fixpoint.
-- (the count below is the *awaiter* task's retry timer firing; the latent
--  promise itself is never selected, which is the whole point)
SELECT resonate.process_timeouts(9000) AS driver_processed;

SELECT
  (SELECT state FROM resonate.promises WHERE id = 'latent')            AS row_state,
  pg_temp.rpc('promise.get','{"id":"latent"}'::jsonb, 9000)
    ->'data'->'promise'->>'state'                                      AS promise_get_reports,
  (SELECT count(*) FROM resonate.listeners WHERE awaited_id = 'latent') AS listeners_still_registered,
  (SELECT count(*) FROM resonate.outbox WHERE kind = 'unblock')         AS unblock_messages_emitted;

SELECT CASE WHEN
    (SELECT state FROM resonate.promises WHERE id = 'latent') = 'pending'
    AND pg_temp.rpc('promise.get','{"id":"latent"}'::jsonb, 9000)
          ->'data'->'promise'->>'state' = 'rejected_timedout'
    AND (SELECT count(*) FROM resonate.listeners WHERE awaited_id = 'latent') = 1
    AND (SELECT count(*) FROM resonate.outbox WHERE kind = 'unblock') = 0
  THEN 'BUG-1 REPRODUCED: promise.get says rejected_timedout, listener never notified'
  ELSE 'BUG-1 not reproduced' END AS result;

-- Second consequence: the row never leaves 'pending', so gc can never
-- reclaim it, nor anything else in its origin group.
SELECT resonate.gc(9999999999) AS gc_collected_expect_0;

-- =============================================================================
-- BUG-2 · task.create hands out an execution lease on a promise that
--         task.acquire refuses as dead.
--
--   T-03 task_acquire checks the promise (resonate.sql:613).  T-02 task_create's
--   claim branch (resonate.sql:580-594) checks neither promise state nor
--   promise timeout.  T-10 task_continue (resonate.sql:803-819) is the same.
-- =============================================================================
\echo ''
\echo '=== BUG-2: task.create claims a lease on an already-dead promise ==='
SELECT pg_temp.reset();

-- t=1000: a workflow whose promise expires at t=2000.  Task is 'pending'.
SELECT pg_temp.rpc('promise.create',
       '{"id":"wf","timeoutAt":2000,"tags":{"resonate:target":"poll://any@w1"}}'::jsonb,
       1000)->'head'->>'status' AS create_status;
DELETE FROM resonate.outbox;

-- t=9000: promise timed out at 2000, driver has not ticked yet (pg_cron polls
-- every 5s, so this window is ~5s wide in production).
SELECT pg_temp.rpc('task.acquire',
       '{"id":"wf","version":0,"pid":"w1","ttl":1000}'::jsonb, 9000)
       ->'head'->>'status' AS acquire_status_expect_409;

SELECT pg_temp.rpc('task.create',
       ('{"pid":"w2","ttl":1000,"action":{"kind":"promise.create","data":'
        || '{"id":"wf","timeoutAt":2000,"tags":{"resonate:target":"poll://any@w1"}}}}')::jsonb,
       9000)->'head'->>'status' AS create_status_expect_409;

SELECT state, version, pid FROM resonate.tasks WHERE id = 'wf';

-- The lease is real, and the promise handed back to the worker reads 'pending'
-- although promise.get projects it as rejected_timedout.
SELECT
  (SELECT state FROM resonate.tasks WHERE id = 'wf')                   AS task_state,
  pg_temp.rpc('promise.get','{"id":"wf"}'::jsonb, 9000)
    ->'data'->'promise'->>'state'                                      AS promise_get_reports,
  pg_temp.rpc('task.fulfill',
      '{"id":"wf","version":1,"action":{"kind":"promise.settle","data":{"state":"resolved"}}}'::jsonb,
      9000)->'head'->>'status'                                         AS fulfill_status;

SELECT CASE WHEN
    (SELECT state FROM resonate.tasks WHERE id = 'wf') = 'acquired'
  THEN 'BUG-2 REPRODUCED: task.acquire 409 but task.create granted the lease; '
       || 'the run''s result is then rejected at task.fulfill'
  ELSE 'BUG-2 not reproduced' END AS result;

-- Same guard gap on task.continue (T-10).
SELECT pg_temp.reset();
SELECT pg_temp.rpc('promise.create',
       '{"id":"wf2","timeoutAt":2000,"tags":{"resonate:target":"poll://any@w1"}}'::jsonb,
       1000)->'head'->>'status' AS create_status;
SELECT pg_temp.rpc('task.halt','{"id":"wf2"}'::jsonb, 1000)->'head'->>'status' AS halt_status;
DELETE FROM resonate.outbox;
SELECT pg_temp.rpc('task.continue','{"id":"wf2"}'::jsonb, 9000)
       ->'head'->>'status' AS continue_status_expect_409;
SELECT CASE WHEN (SELECT count(*) FROM resonate.outbox WHERE kind = 'execute') = 1
  THEN 'BUG-2b REPRODUCED: task.continue re-dispatched an already-dead workflow'
  ELSE 'BUG-2b not reproduced' END AS result;

-- =============================================================================
-- BUG-3 · resonate:target = '' produces a task that is dispatched exactly
--         once and can never be redelivered.
--
--   promise_create tests `tgt IS NOT NULL` (resonate.sql:415) so '' counts as a
--   target: a task row is created and an execute is emitted to the empty
--   address.  Every redelivery path instead tests `target <> ''`
--   (resonate.sql:783, 815, 916, 934) and emits nothing.
-- =============================================================================
\echo ''
\echo '=== BUG-3: empty resonate:target creates an undeliverable task ==='
SELECT pg_temp.reset();

SELECT pg_temp.rpc('promise.create',
       '{"id":"empty","timeoutAt":900000,"tags":{"resonate:target":""}}'::jsonb, 1000)
       ->'head'->>'status' AS create_status;

SELECT id, state, version, timeout_at FROM resonate.tasks WHERE id = 'empty';
SELECT key, kind, '<' || address || '>' AS address FROM resonate.outbox;

-- Consume that one message, then let the retry timer fire repeatedly.
DELETE FROM resonate.outbox;
SELECT resonate.process_timeouts(20000) AS tick1;
SELECT resonate.process_timeouts(40000) AS tick2;
SELECT resonate.process_timeouts(60000) AS tick3;

SELECT
  (SELECT state FROM resonate.tasks WHERE id = 'empty')      AS task_state,
  (SELECT count(*) FROM resonate.outbox)                     AS outbox_after_3_retries;

SELECT CASE WHEN
    (SELECT state FROM resonate.tasks WHERE id = 'empty') = 'pending'
    AND (SELECT count(*) FROM resonate.outbox) = 0
  THEN 'BUG-3 REPRODUCED: task pending forever, retry timer emits nothing'
  ELSE 'BUG-3 not reproduced' END AS result;

\echo ''
-- =============================================================================
-- BUG-4 · task.halt succeeds on a task that task.get already reports fulfilled.
--
--   The reference spec's T-09 returns 409 once the promise is logically
--   settled, with the reasoning stated in the handler: "Branching on the raw
--   stored task here would make the stored-vs-projected divergence observable
--   — the one thing the projection discipline forbids."  resonate.sql's
--   task_halt (:789-801) never loads the promise at all.
-- =============================================================================
\echo ''
\echo '=== BUG-4: task.halt on a task task.get calls fulfilled ==='
SELECT pg_temp.reset();

SELECT pg_temp.rpc('promise.create',
       '{"id":"h1","timeoutAt":2000,"tags":{"resonate:target":"poll://any@w1"}}'::jsonb,
       1000)->'head'->>'status' AS create_status;
DELETE FROM resonate.outbox;

-- t=9000: promise logically dead, driver has not ticked.
SELECT
  pg_temp.rpc('task.get','{"id":"h1"}'::jsonb, 9000)
    ->'data'->'task'->>'state'                              AS task_get_reports,
  pg_temp.rpc('task.halt','{"id":"h1"}'::jsonb, 9000)
    ->'head'->>'status'                                     AS halt_status,
  (SELECT state FROM resonate.tasks WHERE id = 'h1')        AS task_row_after;

SELECT CASE WHEN
    (SELECT state FROM resonate.tasks WHERE id = 'h1') = 'halted'
  THEN 'BUG-4 REPRODUCED: task.get says fulfilled (halt-on-fulfilled is 409), '
       || 'yet task.halt returned 200 and halted it'
  ELSE 'BUG-4 not reproduced' END AS result;

-- =============================================================================
-- BUG-5 · the task timeout handlers redispatch a logically dead workflow.
--
--   _on_task_retry_timeout (:904-919) and _on_task_lease_timeout (:921-937)
--   read the promise only for its target and never consult its state or
--   timeout.  The reference spec added that gate in 3e8a1d6, "no new work for
--   the dead".  In resonate.sql the gap is MASKED inside process_timeouts
--   (:997-1012), which drains promise timeouts before task timeouts; it is
--   exposed by calling process_task_timeouts() on its own.
-- =============================================================================
\echo ''
\echo '=== BUG-5: task timeout handlers redispatch a dead workflow ==='
SELECT pg_temp.reset();

SELECT pg_temp.rpc('promise.create',
       '{"id":"h2","timeoutAt":3000,"tags":{"resonate:target":"poll://any@w1"}}'::jsonb,
       1000)->'head'->>'status' AS create_status;
SELECT pg_temp.rpc('task.acquire',
       '{"id":"h2","version":0,"pid":"w1","ttl":500}'::jsonb, 1000)
       ->'head'->>'status' AS acquire_status;
DELETE FROM resonate.outbox;

-- lease expired at 1500, promise expired at 3000, now = 9000.
SELECT resonate.process_task_timeouts(9000) AS task_loop_processed;
SELECT
  (SELECT count(*) FROM resonate.outbox WHERE kind = 'execute') AS execute_msgs,
  (SELECT state FROM resonate.tasks WHERE id = 'h2')            AS task_state,
  (SELECT state FROM resonate.promises WHERE id = 'h2')         AS promise_row;

SELECT CASE WHEN (SELECT count(*) FROM resonate.outbox WHERE kind = 'execute') = 1
  THEN 'BUG-5 REPRODUCED (via process_task_timeouts): dead workflow redispatched'
  ELSE 'BUG-5 not reproduced' END AS result;

-- The shipped driver masks it: promise timeouts drain first.
SELECT pg_temp.reset();
SELECT pg_temp.rpc('promise.create',
       '{"id":"h3","timeoutAt":3000,"tags":{"resonate:target":"poll://any@w1"}}'::jsonb,
       1000)->'head'->>'status' AS create_status;
SELECT pg_temp.rpc('task.acquire',
       '{"id":"h3","version":0,"pid":"w1","ttl":500}'::jsonb, 1000)
       ->'head'->>'status' AS acquire_status;
DELETE FROM resonate.outbox;
SELECT resonate.process_timeouts(9000) AS full_driver_processed;
SELECT CASE WHEN (SELECT count(*) FROM resonate.outbox WHERE kind = 'execute') = 0
  THEN 'BUG-5 MASKED via process_timeouts: ordering, not a guard, is what saves it'
  ELSE 'BUG-5 also reproduces through the full driver' END AS result;

\echo ''
