-- =============================================================================
-- Regression tests for the guard fixes (issues #8-#12)
-- =============================================================================
--   psql -d yourdb -f resonate.sql
--   psql -d yourdb -f test/regression.sql
--
-- Time is passed explicitly through `resonate:debug_time`, and the pg_cron
-- driver is stood in for by calling resonate.process_timeouts(now), the same
-- way test/conformance.py does. Any failed assertion raises, and ON_ERROR_STOP
-- makes psql exit non-zero.
-- =============================================================================

\set ON_ERROR_STOP on
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

CREATE OR REPLACE FUNCTION pg_temp.status(kind text, data jsonb, now bigint)
  RETURNS int LANGUAGE sql AS $$
  SELECT (pg_temp.rpc(kind, data, now)->'head'->>'status')::int;
$$;

CREATE OR REPLACE FUNCTION pg_temp.ok(cond boolean, name text) RETURNS void
  LANGUAGE plpgsql AS $$
BEGIN
  IF cond THEN
    RAISE NOTICE 'ok   %', name;
  ELSE
    RAISE EXCEPTION 'FAIL %', name;
  END IF;
END $$;

CREATE OR REPLACE FUNCTION pg_temp.eq(got anyelement, want anyelement, name text)
  RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF got IS NOT DISTINCT FROM want THEN
    RAISE NOTICE 'ok   % (%)', name, got;
  ELSE
    RAISE EXCEPTION 'FAIL %: got %, want %', name, got, want;
  END IF;
END $$;

SET client_min_messages TO NOTICE;

-- =============================================================================
-- #8 · a listener may only attach to an external promise
-- =============================================================================
\echo ''
\echo '--- #8 promise.register_listener requires an external awaited'
DO $$
DECLARE unblocks int; listeners_left int;
BEGIN
  PERFORM pg_temp.reset();

  -- internal promise: no target, no timer, no resonate:external
  PERFORM pg_temp.eq(pg_temp.status('promise.create',
      '{"id":"internal","timeoutAt":2000}', 1000), 200, 'internal promise created');
  PERFORM pg_temp.eq(pg_temp.status('promise.register_listener',
      '{"awaited":"internal","address":"poll://any@w1"}', 1000), 422,
      '#8 listener on internal awaited is 422');

  -- the callback path answers the same way, as it did before
  PERFORM pg_temp.status('promise.create',
      '{"id":"awaiter","timeoutAt":900000,"tags":{"resonate:target":"poll://any@w1"}}', 1000);
  PERFORM pg_temp.eq(pg_temp.status('promise.register_callback',
      '{"awaited":"internal","awaiter":"awaiter"}', 1000), 422,
      'callback on internal awaited is still 422');

  -- each flavour of external promise still accepts a listener ...
  PERFORM pg_temp.status('promise.create',
      '{"id":"ext_target","timeoutAt":900000,"tags":{"resonate:target":"poll://any@w1"}}', 1000);
  PERFORM pg_temp.status('promise.create',
      '{"id":"ext_timer","timeoutAt":900000,"tags":{"resonate:timer":"true"}}', 1000);
  PERFORM pg_temp.status('promise.create',
      '{"id":"ext_tag","timeoutAt":900000,"tags":{"resonate:external":"true"}}', 1000);
  PERFORM pg_temp.eq(pg_temp.status('promise.register_listener',
      '{"awaited":"ext_target","address":"poll://any@w1"}', 1000), 200, 'listener on targeted');
  PERFORM pg_temp.eq(pg_temp.status('promise.register_listener',
      '{"awaited":"ext_timer","address":"poll://any@w1"}', 1000), 200, 'listener on timer');
  PERFORM pg_temp.eq(pg_temp.status('promise.register_listener',
      '{"awaited":"ext_tag","address":"poll://any@w1"}', 1000), 200, 'listener on resonate:external');

  -- ... and is notified when the awaited settles
  DELETE FROM resonate.outbox;
  PERFORM pg_temp.status('promise.settle',
      '{"id":"ext_tag","state":"resolved"}', 2000);
  SELECT count(*) INTO unblocks FROM resonate.outbox WHERE kind = 'unblock';
  PERFORM pg_temp.eq(unblocks, 1, 'settling an awaited emits the unblock');
  SELECT count(*) INTO listeners_left FROM resonate.listeners WHERE awaited_id = 'ext_tag';
  PERFORM pg_temp.eq(listeners_left, 0, 'the listener row is consumed');

  -- ... and is notified when it dies by deadline, via the driver
  DELETE FROM resonate.outbox;
  PERFORM resonate.process_timeouts(900001);
  SELECT count(*) INTO unblocks FROM resonate.outbox WHERE kind = 'unblock';
  PERFORM pg_temp.ok(unblocks >= 1, 'a timed-out awaited emits the unblock');
END $$;

-- =============================================================================
-- #9 · task.create and task.continue do not arm a lease on a dead promise
-- =============================================================================
\echo ''
\echo '--- #9 no lease is armed on a logically dead promise'
DO $$
DECLARE r jsonb; execs int;
BEGIN
  PERFORM pg_temp.reset();
  PERFORM pg_temp.status('promise.create',
      '{"id":"wf","timeoutAt":2000,"tags":{"resonate:target":"poll://any@w1"}}', 1000);
  DELETE FROM resonate.outbox;

  -- now = 9000: promise died at 2000, the driver has not ticked
  PERFORM pg_temp.eq(pg_temp.status('task.acquire',
      '{"id":"wf","version":0,"pid":"w1","ttl":1000}', 9000), 409,
      'task.acquire refuses the dead promise');

  r := pg_temp.rpc('task.create',
      ('{"pid":"w2","ttl":1000,"action":{"kind":"promise.create","data":' ||
       '{"id":"wf","timeoutAt":2000,"tags":{"resonate:target":"poll://any@w1"}}}}')::jsonb, 9000);
  PERFORM pg_temp.eq((r->'head'->>'status')::int, 200, 'task.create answers 200');
  PERFORM pg_temp.eq(r->'data'->'task'->>'state', 'fulfilled',
      '#9 task.create serves the projected task, not a lease');
  PERFORM pg_temp.eq(r->'data'->'promise'->>'state', 'rejected_timedout',
      '#9 task.create serves the projected promise, not the raw row');
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 'wf'), 'pending',
      '#9 no lease was written');

  -- task.continue on a dead promise
  PERFORM pg_temp.reset();
  PERFORM pg_temp.status('promise.create',
      '{"id":"wf2","timeoutAt":2000,"tags":{"resonate:target":"poll://any@w1"}}', 1000);
  PERFORM pg_temp.eq(pg_temp.status('task.halt', '{"id":"wf2"}', 1000), 200, 'halt while live');
  DELETE FROM resonate.outbox;
  PERFORM pg_temp.eq(pg_temp.status('task.continue', '{"id":"wf2"}', 9000), 409,
      '#9 task.continue refuses the dead promise');
  SELECT count(*) INTO execs FROM resonate.outbox WHERE kind = 'execute';
  PERFORM pg_temp.eq(execs, 0, '#9 no execute was emitted for a dead workflow');
END $$;

-- =============================================================================
-- #10 · resonate:target = '' behaves like any other unrouted target
-- =============================================================================
\echo ''
\echo '--- #10 an empty target is a present, unrouted address everywhere'
DO $$
DECLARE execs int;
BEGIN
  PERFORM pg_temp.reset();
  PERFORM pg_temp.eq(pg_temp.status('promise.create',
      '{"id":"empty","timeoutAt":900000,"tags":{"resonate:target":""}}', 1000), 200,
      'promise with empty target created');
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 'empty'), 'pending',
      'a task exists for it');

  -- the retry timer keeps redelivering, as it does for any live task
  DELETE FROM resonate.outbox;
  PERFORM resonate.process_timeouts(20000);
  SELECT count(*) INTO execs FROM resonate.outbox WHERE kind = 'execute';
  PERFORM pg_temp.eq(execs, 1, '#10 the retry timer redelivers an empty-target task');

  DELETE FROM resonate.outbox;
  PERFORM resonate.process_timeouts(40000);
  SELECT count(*) INTO execs FROM resonate.outbox WHERE kind = 'execute';
  PERFORM pg_temp.eq(execs, 1, '#10 and keeps redelivering');
END $$;

-- =============================================================================
-- #11 · task.halt refuses a task whose promise is logically settled
-- =============================================================================
\echo ''
\echo '--- #11 task.halt agrees with task.get'
DO $$
DECLARE projected text;
BEGIN
  PERFORM pg_temp.reset();
  PERFORM pg_temp.status('promise.create',
      '{"id":"h1","timeoutAt":2000,"tags":{"resonate:target":"poll://any@w1"}}', 1000);

  projected := pg_temp.rpc('task.get', '{"id":"h1"}', 9000)->'data'->'task'->>'state';
  PERFORM pg_temp.eq(projected, 'fulfilled', 'task.get projects the task fulfilled');
  PERFORM pg_temp.eq(pg_temp.status('task.halt', '{"id":"h1"}', 9000), 409,
      '#11 task.halt answers 409, agreeing with task.get');
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 'h1'), 'pending',
      '#11 the task was not halted');

  -- halting a live task still works, and is still idempotent
  PERFORM pg_temp.eq(pg_temp.status('task.halt', '{"id":"h1"}', 1500), 200, 'halt while live');
  PERFORM pg_temp.eq(pg_temp.status('task.halt', '{"id":"h1"}', 1500), 200, 'halt is idempotent');
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 'h1'), 'halted', 'task halted');
END $$;

-- =============================================================================
-- #12 · the task timeout handlers create no work for a dead task
-- =============================================================================
\echo ''
\echo '--- #12 lease and retry timeouts decide on the projected promise'
DO $$
DECLARE execs int;
BEGIN
  PERFORM pg_temp.reset();
  PERFORM pg_temp.status('promise.create',
      '{"id":"t1","timeoutAt":3000,"tags":{"resonate:target":"poll://any@w1"}}', 1000);
  PERFORM pg_temp.eq(pg_temp.status('task.acquire',
      '{"id":"t1","version":0,"pid":"w1","ttl":500}', 1000), 200, 'lease acquired');
  DELETE FROM resonate.outbox;

  -- lease dead since 1500, promise dead since 3000; the task loop on its own
  PERFORM resonate.process_task_timeouts(9000);
  SELECT count(*) INTO execs FROM resonate.outbox WHERE kind = 'execute';
  PERFORM pg_temp.eq(execs, 0, '#12 no redispatch for a dead task');
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 't1'), 'acquired',
      '#12 the dead lease is not returned to circulation');

  -- and the promise-timeout transition still owns the cleanup
  PERFORM resonate.process_timeouts(9000);
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 't1'), 'fulfilled',
      '#12 cleanup belongs to the promise timeout');
END $$;

-- =============================================================================
-- Happy paths — the behaviour the guards must not disturb
-- =============================================================================
\echo ''
\echo '--- happy paths'
DO $$
DECLARE r jsonb; execs int; ver int;
BEGIN
  -- a live task still times out its lease and is redispatched
  PERFORM pg_temp.reset();
  PERFORM pg_temp.status('promise.create',
      '{"id":"live","timeoutAt":900000,"tags":{"resonate:target":"poll://any@w1"}}', 1000);
  PERFORM pg_temp.status('task.acquire', '{"id":"live","version":0,"pid":"w1","ttl":500}', 1000);
  DELETE FROM resonate.outbox;
  PERFORM resonate.process_timeouts(2000);
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 'live'), 'pending',
      'an expired lease on a LIVE task is returned to circulation');
  SELECT count(*) INTO execs FROM resonate.outbox WHERE kind = 'execute';
  PERFORM pg_temp.eq(execs, 1, 'and is redispatched');

  -- the retry timer keeps redelivering a live pending task
  DELETE FROM resonate.outbox;
  PERFORM resonate.process_timeouts(20000);
  SELECT count(*) INTO execs FROM resonate.outbox WHERE kind = 'execute';
  PERFORM pg_temp.eq(execs, 1, 'the retry timer redelivers a live task');

  -- claim, halt, continue on a live task
  PERFORM pg_temp.reset();
  PERFORM pg_temp.status('promise.create',
      '{"id":"c1","timeoutAt":900000,"tags":{"resonate:target":"poll://any@w1"}}', 1000);
  r := pg_temp.rpc('task.create',
      ('{"pid":"w2","ttl":1000,"action":{"kind":"promise.create","data":' ||
       '{"id":"c1","timeoutAt":900000,"tags":{"resonate:target":"poll://any@w1"}}}}')::jsonb, 1000);
  PERFORM pg_temp.eq(r->'data'->'task'->>'state', 'acquired',
      'task.create still claims a LIVE pending task');
  PERFORM pg_temp.eq(r->'data'->'promise'->>'state', 'pending',
      'and serves its promise as pending');
  PERFORM pg_temp.eq(pg_temp.status('task.halt', '{"id":"c1"}', 1000), 200, 'halt a live task');
  DELETE FROM resonate.outbox;
  PERFORM pg_temp.eq(pg_temp.status('task.continue', '{"id":"c1"}', 1000), 200,
      'continue a live task');
  SELECT count(*) INTO execs FROM resonate.outbox WHERE kind = 'execute';
  PERFORM pg_temp.eq(execs, 1, 'continue redispatches');

  -- suspend / resume: the core durable-execution loop
  PERFORM pg_temp.reset();
  PERFORM pg_temp.status('promise.create',
      '{"id":"root","timeoutAt":900000,"tags":{"resonate:target":"poll://any@w1"}}', 1000);
  PERFORM pg_temp.status('promise.create',
      '{"id":"child","timeoutAt":900000,"tags":{"resonate:target":"poll://any@w2"}}', 1000);
  r := pg_temp.rpc('task.acquire', '{"id":"root","version":0,"pid":"w1","ttl":5000}', 1000);
  ver := (r->'data'->'task'->>'version')::int;
  PERFORM pg_temp.eq(pg_temp.status('task.suspend',
      ('{"id":"root","version":' || ver || ',"actions":[{"data":{"awaited":"child"}}]}')::jsonb,
      1000), 200, 'root suspends on child');
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 'root'), 'suspended',
      'root is suspended');

  DELETE FROM resonate.outbox;
  PERFORM pg_temp.eq(pg_temp.status('promise.settle',
      '{"id":"child","state":"resolved"}', 2000), 200, 'child settles');
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 'root'), 'pending',
      'root is resumed');
  SELECT count(*) INTO execs FROM resonate.outbox WHERE kind = 'execute' AND task_id = 'root';
  PERFORM pg_temp.eq(execs, 1, 'and redispatched');
  PERFORM pg_temp.eq((SELECT count(*)::int FROM resonate.task_resumes WHERE task_id = 'root'), 1,
      'with the resume recorded');

  -- and the resumed root can complete
  r := pg_temp.rpc('task.acquire', '{"id":"root","version":1,"pid":"w1","ttl":5000}', 2000);
  PERFORM pg_temp.eq((r->'head'->>'status')::int, 200, 'root reacquired');
  PERFORM pg_temp.eq(pg_temp.status('task.fulfill',
      ('{"id":"root","version":' || (r->'data'->'task'->>'version') ||
       ',"action":{"kind":"promise.settle","data":{"state":"resolved"}}}')::jsonb, 2000),
      200, 'root fulfilled');
  PERFORM pg_temp.eq((SELECT state FROM resonate.promises WHERE id = 'root'), 'resolved',
      'root promise resolved');
  PERFORM pg_temp.eq((SELECT state FROM resonate.tasks WHERE id = 'root'), 'fulfilled',
      'root task fulfilled');

  -- timer promises still resolve on their deadline
  PERFORM pg_temp.reset();
  PERFORM pg_temp.status('promise.create',
      '{"id":"tmr","timeoutAt":5000,"tags":{"resonate:timer":"true"}}', 1000);
  PERFORM resonate.process_timeouts(6000);
  PERFORM pg_temp.eq((SELECT state FROM resonate.promises WHERE id = 'tmr'), 'resolved',
      'a timer promise resolves at its deadline');

  -- an external promise still times out into rejected_timedout
  PERFORM pg_temp.status('promise.create',
      '{"id":"ext","timeoutAt":5000,"tags":{"resonate:external":"true"}}', 1000);
  PERFORM resonate.process_timeouts(6000);
  PERFORM pg_temp.eq((SELECT state FROM resonate.promises WHERE id = 'ext'), 'rejected_timedout',
      'an external promise times out');

  -- settlement is still sticky
  PERFORM pg_temp.eq(pg_temp.status('promise.settle', '{"id":"ext","state":"resolved"}', 6000),
      200, 'settling a settled promise echoes 200');
  PERFORM pg_temp.eq((SELECT state FROM resonate.promises WHERE id = 'ext'), 'rejected_timedout',
      'and does not change it');
END $$;

\echo ''
\echo 'ALL REGRESSION TESTS PASSED'
