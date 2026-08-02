-- =============================================================================
-- Specula trace scenarios for resonate-pg  (Phase 2.5)
-- =============================================================================
-- Each scenario drives the REAL wire entrypoint, resonate.resonate_rpc, and the
-- REAL timeout driver, resonate.process_timeouts. Nothing here reimplements
-- protocol logic; the instrumented handlers emit the trace as they run.
--
-- Ids are 'a' and 'b' and the listener address is 'L', matching base.tla's
-- CONSTANTS, and time is passed explicitly as small integers via
-- `resonate:debug_time` so the trace lives in the model's time domain
-- (Retry = 1, Ttl = 1 -- see trace.sql).
--
-- Usage:  psql -v scenario=suspend_resume -f scenarios.sql
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off
SET client_min_messages TO WARNING;

CREATE OR REPLACE FUNCTION pg_temp.reset() RETURNS void LANGUAGE sql AS $$
  TRUNCATE resonate.outbox, resonate.listeners, resonate.callbacks,
           resonate.task_resumes, resonate.tasks, resonate.schedules,
           resonate.promises CASCADE;
  SELECT resonate._trace_reset();
$$;

CREATE OR REPLACE FUNCTION pg_temp.rpc(kind text, data jsonb, now bigint)
  RETURNS int LANGUAGE sql AS $$
  SELECT (resonate.resonate_rpc(jsonb_build_object(
    'kind', kind,
    'head', jsonb_build_object('resonate:debug_time', now::text),
    'data', data))->'head'->>'status')::int;
$$;

SELECT pg_temp.reset();

-- =============================================================================
-- Scenario: suspend_resume
--   the core durable-execution loop -- a root workflow parks on a child, the
--   child settles, the root is resumed, reacquired and fulfilled.
-- =============================================================================
SELECT CASE WHEN :'scenario' = 'suspend_resume' THEN 1 ELSE 0 END AS run \gset run_
\if :run_run

SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":9,"tags":{"resonate:target":"L"}}', 0) AS create_a;
SELECT pg_temp.rpc('promise.create',
  '{"id":"b","timeoutAt":8,"tags":{"resonate:target":"L"}}', 0) AS create_b;
SELECT pg_temp.rpc('task.acquire',
  '{"id":"a","version":0,"pid":"w1","ttl":1}', 0) AS acquire_a;
SELECT pg_temp.rpc('task.suspend',
  '{"id":"a","version":1,"actions":[{"data":{"awaited":"b"}}]}', 0) AS suspend_a;
SELECT pg_temp.rpc('task.acquire',
  '{"id":"b","version":0,"pid":"w2","ttl":1}', 1) AS acquire_b;
SELECT pg_temp.rpc('task.fulfill',
  '{"id":"b","version":1,"action":{"kind":"promise.settle","data":{"state":"resolved"}}}', 1)
  AS fulfill_b;
SELECT pg_temp.rpc('task.acquire',
  '{"id":"a","version":1,"pid":"w1","ttl":1}', 2) AS reacquire_a;
SELECT pg_temp.rpc('task.fulfill',
  '{"id":"a","version":2,"action":{"kind":"promise.settle","data":{"state":"resolved"}}}', 2)
  AS fulfill_a;

\endif

-- =============================================================================
-- Scenario: timeouts
--   every internal transition: lease expiry, retry redelivery, promise timeout
--   with a cascade to a suspended awaiter.
-- =============================================================================
SELECT CASE WHEN :'scenario' = 'timeouts' THEN 1 ELSE 0 END AS run \gset to_
\if :to_run

SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":9,"tags":{"resonate:target":"L"}}', 0) AS create_a;
SELECT pg_temp.rpc('promise.create',
  '{"id":"b","timeoutAt":3,"tags":{"resonate:target":"L"}}', 0) AS create_b;
-- a acquires a lease that will expire
SELECT pg_temp.rpc('task.acquire',
  '{"id":"a","version":0,"pid":"w1","ttl":1}', 0) AS acquire_a;
-- park a on b so b's timeout cascades into a resume
SELECT pg_temp.rpc('task.suspend',
  '{"id":"a","version":1,"actions":[{"data":{"awaited":"b"}}]}', 0) AS suspend_a;
-- b's lease expires, then retries
SELECT pg_temp.rpc('task.acquire',
  '{"id":"b","version":0,"pid":"w2","ttl":1}', 0) AS acquire_b;
SELECT resonate.process_timeouts(1) AS tick_1;   -- b lease expiry
SELECT resonate.process_timeouts(2) AS tick_2;   -- b retry redelivery
SELECT resonate.process_timeouts(3) AS tick_3;   -- b promise timeout -> cascade -> a resumed
SELECT resonate.process_timeouts(4) AS tick_4;   -- a retry redelivery

\endif

-- =============================================================================
-- Scenario: claim_halt_continue
--   the task entry points that are not task.acquire, plus release.
-- =============================================================================
SELECT CASE WHEN :'scenario' = 'claim_halt_continue' THEN 1 ELSE 0 END AS run \gset ch_
\if :ch_run

SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":9,"tags":{"resonate:target":"L"}}', 0) AS create_a;
-- task.create claims the existing pending task
SELECT pg_temp.rpc('task.create',
  '{"pid":"w1","ttl":1,"action":{"kind":"promise.create","data":{"id":"a","timeoutAt":9,"tags":{"resonate:target":"L"}}}}',
  0) AS claim_a;
SELECT pg_temp.rpc('task.release', '{"id":"a","version":1}', 0) AS release_a;
SELECT pg_temp.rpc('task.halt', '{"id":"a"}', 0) AS halt_a;
SELECT pg_temp.rpc('task.continue', '{"id":"a"}', 1) AS continue_a;
SELECT pg_temp.rpc('task.acquire',
  '{"id":"a","version":1,"pid":"w1","ttl":1}', 1) AS acquire_a;
SELECT pg_temp.rpc('task.fulfill',
  '{"id":"a","version":2,"action":{"kind":"promise.settle","data":{"state":"rejected"}}}', 1)
  AS fulfill_a;

\endif

-- =============================================================================
-- Scenario: listener_callback
--   both wait-registration paths, and both ways an awaited can settle.
-- =============================================================================
SELECT CASE WHEN :'scenario' = 'listener_callback' THEN 1 ELSE 0 END AS run \gset lc_
\if :lc_run

SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":9,"tags":{"resonate:target":"L"}}', 0) AS create_a;
SELECT pg_temp.rpc('promise.create',
  '{"id":"b","timeoutAt":2,"tags":{"resonate:timer":"true"}}', 0) AS create_b_timer;
-- a listener on the timer (P-05 requires a routable address), and a callback
-- from a onto the timer (P-04)
SELECT pg_temp.rpc('promise.register_listener',
  '{"awaited":"b","address":"poll://any@L"}', 0) AS listen_b;
SELECT pg_temp.rpc('promise.register_callback',
  '{"awaited":"b","awaiter":"a"}', 0) AS callback_b_a;
-- a parks on the timer as well
SELECT pg_temp.rpc('task.acquire',
  '{"id":"a","version":0,"pid":"w1","ttl":1}', 0) AS acquire_a;
SELECT pg_temp.rpc('task.suspend',
  '{"id":"a","version":1,"actions":[{"data":{"awaited":"b"}}]}', 0) AS suspend_a;
-- the timer fires: resolves, notifies the listener, resumes the awaiter
SELECT resonate.process_timeouts(2) AS tick_2;
-- re-acquiring and parking on an already-settled awaited is the 300 path
SELECT pg_temp.rpc('task.acquire',
  '{"id":"a","version":1,"pid":"w1","ttl":1}', 2) AS reacquire_a;
SELECT pg_temp.rpc('task.suspend',
  '{"id":"a","version":2,"actions":[{"data":{"awaited":"b"}}]}', 2) AS suspend_a_300;

\endif

-- =============================================================================
-- Scenario: external_settle
--   P-03 promise.settle driven from outside, cascading to a listener and to a
--   suspended awaiter.
-- =============================================================================
SELECT CASE WHEN :'scenario' = 'external_settle' THEN 1 ELSE 0 END AS run \gset es_
\if :es_run

SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":9,"tags":{"resonate:target":"L"}}', 0) AS create_a;
SELECT pg_temp.rpc('promise.create',
  '{"id":"b","timeoutAt":9,"tags":{"resonate:external":"true"}}', 0) AS create_b_external;
SELECT pg_temp.rpc('promise.register_listener',
  '{"awaited":"b","address":"poll://any@L"}', 0) AS listen_b;
SELECT pg_temp.rpc('task.acquire',
  '{"id":"a","version":0,"pid":"w1","ttl":1}', 0) AS acquire_a;
SELECT pg_temp.rpc('task.suspend',
  '{"id":"a","version":1,"actions":[{"data":{"awaited":"b"}}]}', 0) AS suspend_a;
-- someone outside the database resolves the latent promise
SELECT pg_temp.rpc('promise.settle',
  '{"id":"b","state":"resolved"}', 1) AS settle_b;

\endif

\echo 'scenario complete'

-- =============================================================================
-- BUG-CONDITION SCENARIOS
-- =============================================================================
-- Generated by the unified specification's mutation counterexamples
-- (tla/scenarios/*.md) and scripted here against the real server. Unlike the
-- scenarios above, these deliberately let a promise pass its deadline before
-- acting, which is the condition every known defect in this family needs.
-- A conformant server cannot produce these traces.
-- =============================================================================

-- --- stranded-listener: a waiter attached to an INTERNAL promise -------------
SELECT CASE WHEN :'scenario' = 'bug_stranded_listener' THEN 1 ELSE 0 END AS run \gset bsl_
\if :bsl_run

SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":9,"tags":{"resonate:target":"L"}}', 0) AS create_a;
-- b carries no tags at all: internal, so its deadline is never enforced
SELECT pg_temp.rpc('promise.create',
  '{"id":"b","timeoutAt":2}', 0) AS create_b_internal;
-- the call a conformant server must refuse with 422
SELECT pg_temp.rpc('promise.register_listener',
  '{"awaited":"b","address":"poll://any@L"}', 0) AS listen_internal;
-- and the deadline passes with nothing discharging the obligation
SELECT resonate.process_timeouts(3) AS tick_3;

\endif

-- --- dead-dispatch-claim: task.create leases a logically dead promise --------
SELECT CASE WHEN :'scenario' = 'bug_dead_claim' THEN 1 ELSE 0 END AS run \gset bdc_
\if :bdc_run

SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":1,"tags":{"resonate:target":"L"}}', 0) AS create_a;
-- now = 1: a is logically dead, the driver has not run
SELECT pg_temp.rpc('task.create',
  '{"pid":"w1","ttl":1,"action":{"kind":"promise.create","data":{"id":"a","timeoutAt":1,"tags":{"resonate:target":"L"}}}}',
  1) AS claim_dead;

\endif

-- --- dead-redispatch: the retry timer redelivers a dead workflow -------------
SELECT CASE WHEN :'scenario' = 'bug_dead_redispatch' THEN 1 ELSE 0 END AS run \gset bdr_
\if :bdr_run

SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":1,"tags":{"resonate:target":"L"}}', 0) AS create_a;
DELETE FROM resonate.outbox;
-- the task loop on its own, so process_timeouts' ordering cannot mask it
SELECT resonate.process_task_timeouts(1) AS task_loop;

\endif

-- --- halt-on-dead: task.halt on a task task.get already calls fulfilled ------
SELECT CASE WHEN :'scenario' = 'bug_halt_on_dead' THEN 1 ELSE 0 END AS run \gset bhd_
\if :bhd_run

SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":1,"tags":{"resonate:target":"L"}}', 0) AS create_a;
-- now = 1: a is logically dead; halt must be 409
SELECT pg_temp.rpc('task.halt', '{"id":"a"}', 1) AS halt_dead;

\endif

-- --- resume-dead-awaiter: the cascade wakes a logically dead awaiter ---------
SELECT CASE WHEN :'scenario' = 'bug_resume_dead_awaiter' THEN 1 ELSE 0 END AS run \gset brd_
\if :brd_run

-- a dies at 1; b is driven from outside and stays alive
SELECT pg_temp.rpc('promise.create',
  '{"id":"a","timeoutAt":1,"tags":{"resonate:target":"L"}}', 0) AS create_a;
SELECT pg_temp.rpc('promise.create',
  '{"id":"b","timeoutAt":9,"tags":{"resonate:external":"true"}}', 0) AS create_b;
SELECT pg_temp.rpc('task.acquire',
  '{"id":"a","version":0,"pid":"w1","ttl":1}', 0) AS acquire_a;
SELECT pg_temp.rpc('task.suspend',
  '{"id":"a","version":1,"actions":[{"data":{"awaited":"b"}}]}', 0) AS suspend_a;
-- now = 5: a is long dead. Settling b cascades a resume onto it anyway.
SELECT pg_temp.rpc('promise.settle',
  '{"id":"b","state":"resolved"}', 5) AS settle_b;

\endif
