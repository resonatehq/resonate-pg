-- =============================================================================
-- A Resonate Server in a single Postgres file
-- =============================================================================
-- Apply this file to a Postgres 16+ database:
-- psql -d yourdb -f resonate.sql
-- =============================================================================

-- =============================================================================
-- SECTION 1 · SCHEMA
-- =============================================================================
-- Conventions: time is ms since epoch, passed as explicit `now` to every action
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS resonate;

SET search_path TO resonate, public;

-- --- promises ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS promises (
  id            TEXT PRIMARY KEY,
  state         TEXT NOT NULL DEFAULT 'pending'
                  CHECK (state IN ('pending','resolved','rejected',
                                   'rejected_canceled','rejected_timedout')),
  param_headers JSONB NOT NULL DEFAULT '{}',
  param_data    TEXT,
  value_headers JSONB NOT NULL DEFAULT '{}',
  value_data    TEXT,
  tags          JSONB NOT NULL DEFAULT '{}',
  -- lineage + routing tags popped into first-class columns
  target        TEXT GENERATED ALWAYS AS (tags->>'resonate:target') STORED,
  origin_id     TEXT GENERATED ALWAYS AS (tags->>'resonate:origin') STORED,
  parent_id     TEXT GENERATED ALWAYS AS (tags->>'resonate:parent') STORED,
  branch_id     TEXT GENERATED ALWAYS AS (tags->>'resonate:branch') STORED,
  is_timer      BOOLEAN NOT NULL
                  GENERATED ALWAYS AS (COALESCE(tags->>'resonate:timer','') = 'true') STORED,
  -- external (settled by a worker or the server timer) iff it has a target or is a timer
  kind          TEXT GENERATED ALWAYS AS (
                  CASE WHEN tags->>'resonate:target' IS NOT NULL
                         OR COALESCE(tags->>'resonate:timer','') = 'true'
                       THEN 'external' ELSE 'internal' END) STORED,
  timeout_at    BIGINT NOT NULL,   -- promise's own timer (armed iff pending & external)
  created_at    BIGINT NOT NULL,
  settled_at    BIGINT
);
CREATE INDEX IF NOT EXISTS idx_promises_timeout_at ON promises (timeout_at) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS idx_promises_origin_id ON promises (origin_id) WHERE origin_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_promises_branch_id ON promises (branch_id) WHERE branch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_promises_settled_at ON promises (settled_at) WHERE state <> 'pending';  -- gc sweep, § 11

-- --- tasks ------------------------------------------------------------------
-- Spec TaskObject (state, version, ttl, pid); `resumes` lives in task_resumes.
-- One inline timer: pending => retry, acquired => lease. No disarm -- timeout_at
-- is left stale on suspend/halt/fulfill; the sweep filters on state.
CREATE TABLE IF NOT EXISTS tasks (
  id         TEXT PRIMARY KEY REFERENCES promises(id) ON DELETE CASCADE,
  state      TEXT NOT NULL DEFAULT 'pending'
               CHECK (state IN ('pending','acquired','suspended','halted','fulfilled')),
  version    INT  NOT NULL DEFAULT 0,
  ttl        BIGINT,
  pid        TEXT,
  timeout_at BIGINT
);
CREATE INDEX IF NOT EXISTS idx_tasks_timeout_at ON tasks (timeout_at) WHERE state IN ('pending','acquired');

CREATE TABLE IF NOT EXISTS task_resumes (
  task_id    TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  awaited_id TEXT NOT NULL,
  PRIMARY KEY (task_id, awaited_id)
);

-- --- registrations ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS callbacks (
  awaited_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  awaiter_id TEXT NOT NULL,
  PRIMARY KEY (awaited_id, awaiter_id)
);
CREATE INDEX IF NOT EXISTS idx_callbacks_awaiter_id ON callbacks (awaiter_id);

CREATE TABLE IF NOT EXISTS listeners (
  awaited_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  address    TEXT NOT NULL,
  PRIMARY KEY (awaited_id, address)
);

-- --- schedules --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schedules (
  id             TEXT PRIMARY KEY,
  cron           TEXT NOT NULL,
  promise_id     TEXT NOT NULL,
  promise_timeout BIGINT NOT NULL,
  promise_param_headers JSONB NOT NULL DEFAULT '{}',
  promise_param_data    TEXT,
  promise_tags   JSONB NOT NULL DEFAULT '{}',
  created_at     BIGINT NOT NULL,
  next_run_at    BIGINT NOT NULL,
  last_run_at    BIGINT
);
CREATE INDEX IF NOT EXISTS idx_schedules_next_run_at ON schedules (next_run_at);

-- --- outbox (messages) ------------------------------------------------------
-- Dedup key (spec OutboxEntry.key): execute -> taskId; unblock -> "<pid>:notify:<addr>".
CREATE TABLE IF NOT EXISTS outbox (
  key       TEXT PRIMARY KEY,
  kind      TEXT NOT NULL CHECK (kind IN ('execute','unblock')),
  address   TEXT NOT NULL,
  task_id   TEXT,            -- execute
  version   INT,             -- execute
  promise   JSONB,           -- unblock: full promise record
  seq       BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_outbox_dispatch ON outbox (kind, address, seq);  -- dequeue_* ordered walk, § 10

-- =============================================================================
-- NOTIFY plumbing
-- =============================================================================
-- Per-address NOTIFY channel; one definition shared by server + client. md5
-- because LISTEN channels are 63-byte identifiers and addresses aren't.
CREATE OR REPLACE FUNCTION outbox_channel(p_address text) RETURNS text
  LANGUAGE sql IMMUTABLE AS $$ SELECT 'resonate_q_' || md5(p_address) $$;

-- pg_net present? Guard so installs without it still work (http targets undeliverable).
CREATE OR REPLACE FUNCTION _http_available() RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname = 'net' AND p.proname = 'http_post');
$$;

-- Message body POSTed to an http target. The empty `head` is required: the SDK
-- wire Message type demands a MessageHead (no serverUrl -- workers talk back to
-- this Postgres server directly).
CREATE OR REPLACE FUNCTION _outbox_http_body(o outbox) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN o.kind = 'execute'
    THEN jsonb_build_object('kind', 'execute', 'head', '{}'::jsonb,
           'data', jsonb_build_object('task',
                     jsonb_build_object('id', o.task_id, 'version', o.version)))
    ELSE jsonb_build_object('kind', 'unblock', 'head', '{}'::jsonb,
           'data', jsonb_build_object('promise', o.promise)) END;
$$;

-- Delivery by address scheme:
--   http(s) -> PUSH: net.http_post after commit, then delete (lost push -> retry
--     timer re-emits; no pg_net -> row left undelivered).
--   else -> PULL: NOTIFY the address's channel (a missed NOTIFY is harmless).
CREATE OR REPLACE FUNCTION _notify_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.address LIKE 'http://%' OR NEW.address LIKE 'https://%' THEN
    IF _http_available() THEN
      -- best-effort: a bad target must not fail the action; leave the row on error
      BEGIN
        PERFORM net.http_post(url := NEW.address, body := _outbox_http_body(NEW));
        DELETE FROM outbox WHERE key = NEW.key;
      EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'resonate: http push to % failed: %', NEW.address, SQLERRM;
      END;
    END IF;
  ELSE
    PERFORM pg_notify(outbox_channel(NEW.address), NEW.kind);
  END IF;
  RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER trg_outbox_notify
  AFTER INSERT OR UPDATE ON outbox
  FOR EACH ROW EXECUTE FUNCTION _notify_outbox();

-- (No timer NOTIFY: timers are driven solely by pg_cron polling process_timeouts,
-- section 9. The outbox NOTIFY above is delivery -- waking pull workers -- not timers.)


-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 2 · HELPERS · projection, values, resume, settlement cascade, cron
-- ═══════════════════════════════════════════════════════════════════════════
SET search_path TO resonate, public;

-- Spec ServerConfig.retryTimeout (ms).
CREATE OR REPLACE FUNCTION _retry_timeout() RETURNS bigint
  LANGUAGE sql IMMUTABLE AS $$ SELECT 5000::bigint $$;

-- --- JSON serialization ------------------------------------------------------
-- Projection: a pending promise past timeoutAt is reported settled (resolved if
-- timer, else rejected_timedout, settledAt = timeoutAt), though the row is untouched.
CREATE OR REPLACE FUNCTION _promise_json(p promises, now bigint) RETURNS jsonb
  LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE st text := p.state; sa bigint := p.settled_at;
BEGIN
  IF p.state = 'pending' AND p.timeout_at <= now THEN
    st := CASE WHEN p.is_timer THEN 'resolved' ELSE 'rejected_timedout' END;
    sa := p.timeout_at;
  END IF;
  RETURN jsonb_build_object(
    'id',        p.id,
    'state',     st,
    'param',     jsonb_build_object('headers', p.param_headers, 'data', p.param_data),
    'value',     jsonb_build_object('headers', p.value_headers, 'data', p.value_data),
    'tags',      p.tags,
    'timeoutAt', p.timeout_at,
    'createdAt', p.created_at,
    'settledAt', sa);
END $$;

-- Stored record verbatim, NO timeout projection (spec p.toRecord; task.create uses it).
CREATE OR REPLACE FUNCTION _promise_json_raw(p promises) RETURNS jsonb
  LANGUAGE sql IMMUTABLE AS $$ SELECT _promise_json(p, -1) $$;

-- A pending promise is timed iff it's a timer promise OR backs a task (a
-- task-backed promise must time out so its task can be fulfilled). Promise-timer sweep set.
CREATE OR REPLACE FUNCTION _promise_timed(p promises) RETURNS boolean
  LANGUAGE sql STABLE AS $$
  SELECT p.state = 'pending'
     AND (p.is_timer OR EXISTS (SELECT 1 FROM tasks WHERE tasks.id = p.id));
$$;

CREATE OR REPLACE FUNCTION _task_json(t tasks) RETURNS jsonb
  LANGUAGE sql STABLE AS $$
  SELECT jsonb_build_object(
    'id',      t.id,
    'state',   t.state,
    'version', t.version,
    'resumes', (SELECT count(*) FROM task_resumes r WHERE r.task_id = t.id),
    'ttl',     t.ttl,
    'pid',     t.pid);
$$;

CREATE OR REPLACE FUNCTION _schedule_json(s schedules) RETURNS jsonb
  LANGUAGE sql IMMUTABLE AS $$
  SELECT jsonb_build_object(
    'id',             s.id,
    'cron',           s.cron,
    'promiseId',      s.promise_id,
    'promiseTimeout', s.promise_timeout,
    'promiseParam',   jsonb_build_object('headers', s.promise_param_headers,
                                         'data',    s.promise_param_data),
    'promiseTags',    s.promise_tags,
    'nextRunAt',      s.next_run_at,
    'lastRunAt',      s.last_run_at,
    'createdAt',      s.created_at);
$$;

-- --- outbox emitters (spec setMessage; dedup by OutboxEntry.key) -------------
CREATE OR REPLACE FUNCTION _emit_execute(addr text, tid text, ver int) RETURNS void
  LANGUAGE sql AS $$
  INSERT INTO outbox (key, kind, address, task_id, version, promise)
  VALUES (tid, 'execute', addr, tid, ver, NULL)
  ON CONFLICT (key) DO UPDATE
    SET kind = 'execute', address = EXCLUDED.address,
        task_id = EXCLUDED.task_id, version = EXCLUDED.version, promise = NULL;
$$;

-- Resume (spec 00-resume.enqueueResume): a settled awaited nudges an awaiter task.
--   suspended -> pending + re-emit execute (same version) + fresh retry timer
--   pending/acquired/halted -> buffer the trigger id (deduped); fulfilled -> nothing
CREATE OR REPLACE FUNCTION _enqueue_resume(p_awaited text, p_awaiter text, now bigint)
  RETURNS void LANGUAGE plpgsql AS $$
DECLARE t tasks; tgt text;
BEGIN
  SELECT * INTO t FROM tasks WHERE id = p_awaiter FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;

  IF t.state = 'suspended' THEN
    UPDATE tasks SET state = 'pending', pid = pid, ttl = ttl,
                     timeout_at = now + _retry_timeout() WHERE id = t.id;
    DELETE FROM task_resumes WHERE task_id = t.id;
    INSERT INTO task_resumes (task_id, awaited_id) VALUES (t.id, p_awaited)
      ON CONFLICT DO NOTHING;
    SELECT target INTO tgt FROM promises WHERE id = p_awaiter;
    IF tgt IS NOT NULL AND tgt <> '' THEN
      PERFORM _emit_execute(tgt, t.id, t.version);
    END IF;
  ELSIF t.state IN ('pending', 'acquired', 'halted') THEN
    INSERT INTO task_resumes (task_id, awaited_id) VALUES (t.id, p_awaited)
      ON CONFLICT DO NOTHING;
  END IF;  -- fulfilled: no-op
END $$;

-- --- settlement cascade ------------------------------------------------------
-- Precondition: `p` is ALREADY updated to its settled form. Fulfills its task,
-- unblocks listeners, resumes awaiters, tears down registrations.
CREATE OR REPLACE FUNCTION _cascade_settle(p promises, now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE awaiter text;
BEGIN
  UPDATE tasks SET state = 'fulfilled', pid = NULL, ttl = NULL WHERE id = p.id;
  DELETE FROM task_resumes WHERE task_id = p.id;
  -- p's task is fulfilled, so its execute message is dead -- drop it (a delivered
  -- stale execute is harmless anyway: acquire would 409). Unblock rows (task_id
  -- NULL, including the ones emitted just below) are untouched.
  DELETE FROM outbox WHERE task_id = p.id;

  -- unblock listeners, ordered by (awaited, address)
  INSERT INTO outbox (key, kind, address, task_id, version, promise)
  SELECT p.id || ':notify:' || l.address, 'unblock', l.address, NULL, NULL,
         _promise_json(p, now)
  FROM listeners l WHERE l.awaited_id = p.id
  ORDER BY l.awaited_id, l.address
  ON CONFLICT (key) DO UPDATE
    SET kind = 'unblock', address = EXCLUDED.address,
        task_id = NULL, version = NULL, promise = EXCLUDED.promise;

  -- scrub: a settled promise can never resume, so drop it as an awaiter
  DELETE FROM callbacks
    WHERE awaiter_id = p.id
      AND awaited_id IN (SELECT id FROM promises WHERE state = 'pending');

  -- Lock awaiters up front in sorted order so concurrent settles sharing awaiters
  -- can't deadlock. Resume per-row, ordered by (awaited, awaiter): each resume is
  -- one spec transition and realistic fan-out is 1-2 awaiters -- kept 1:1 with the spec.
  PERFORM _lock(awaiter_id) FROM callbacks
    WHERE awaited_id = p.id ORDER BY awaited_id, awaiter_id;
  FOR awaiter IN SELECT awaiter_id FROM callbacks WHERE awaited_id = p.id ORDER BY awaited_id, awaiter_id LOOP
    PERFORM _enqueue_resume(p.id, awaiter, now);
  END LOOP;

  DELETE FROM callbacks WHERE awaited_id = p.id;
  DELETE FROM listeners WHERE awaited_id = p.id;
END $$;

-- --- cron --------------------------------------------------------------------
-- Parse one cron field to the allowed ints in [lo,hi]. Supports * a a-b */s a-b/s
-- and comma lists.
CREATE OR REPLACE FUNCTION _cron_field(spec text, lo int, hi int) RETURNS int[]
  LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE part text; rng text; step text; a int; b int; s int; acc int[] := '{}'; i int;
BEGIN
  FOREACH part IN ARRAY string_to_array(spec, ',') LOOP
    step := '1'; rng := part;
    IF position('/' in part) > 0 THEN
      rng  := split_part(part, '/', 1);
      step := split_part(part, '/', 2);
    END IF;
    s := step::int;
    IF rng = '*' THEN
      a := lo; b := hi;
    ELSIF position('-' in rng) > 0 THEN
      a := split_part(rng, '-', 1)::int; b := split_part(rng, '-', 2)::int;
    ELSE
      a := rng::int; b := CASE WHEN position('/' in part) > 0 THEN hi ELSE rng::int END;
    END IF;
    i := a;
    WHILE i <= b LOOP acc := acc || i; i := i + s; END LOOP;
  END LOOP;
  RETURN acc;
END $$;

-- Next cron fire strictly after `after` (ms epoch). 5-field; dom/dow OR when both restricted.
CREATE OR REPLACE FUNCTION _next_cron(cron text, after bigint) RETURNS bigint
  LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  f text[] := regexp_split_to_array(trim(cron), '\s+');
  mins int[]; hrs int[]; doms int[]; mons int[]; dows int[];
  dom_star bool; dow_star bool;
  ts timestamptz; cutoff timestamptz; ok bool; dow int;
BEGIN
  IF array_length(f, 1) <> 5 THEN
    RAISE EXCEPTION 'unsupported cron (need 5 fields): %', cron;
  END IF;
  mins := _cron_field(f[1], 0, 59);
  hrs  := _cron_field(f[2], 0, 23);
  doms := _cron_field(f[3], 1, 31);
  mons := _cron_field(f[4], 1, 12);
  dows := _cron_field(f[5], 0, 6);
  dom_star := (f[3] = '*'); dow_star := (f[5] = '*');

  ts := date_trunc('minute', to_timestamp(after / 1000.0) AT TIME ZONE 'UTC') + interval '1 minute';
  cutoff := ts + interval '366 days' + interval '1 day';
  WHILE ts < cutoff LOOP
    dow := extract(dow from ts)::int;  -- 0=Sun..6=Sat
    IF extract(month  from ts)::int = ANY(mons)
       AND extract(hour   from ts)::int = ANY(hrs)
       AND extract(minute from ts)::int = ANY(mins) THEN
      IF dom_star AND dow_star THEN ok := true;
      ELSIF dom_star THEN ok := dow = ANY(dows);
      ELSIF dow_star THEN ok := extract(day from ts)::int = ANY(doms);
      ELSE ok := extract(day from ts)::int = ANY(doms) OR dow = ANY(dows);
      END IF;
      IF ok THEN
        RETURN (extract(epoch from ts) * 1000)::bigint;
      END IF;
    END IF;
    ts := ts + interval '1 minute';
  END LOOP;
  RAISE EXCEPTION 'no cron match within horizon: %', cron;
END $$;

-- Expand a schedule's promise-id template ({{.timestamp}}, {{.id}}).
CREATE OR REPLACE FUNCTION _expand(template text, sid text, ts bigint) RETURNS text
  LANGUAGE sql IMMUTABLE AS $$
  SELECT replace(replace(template, '{{.timestamp}}', ts::text), '{{.id}}', sid);
$$;


-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 3 · PROMISE ACTIONS · P-01 .. P-06
-- ═══════════════════════════════════════════════════════════════════════════

SET search_path TO resonate, public;

CREATE OR REPLACE FUNCTION _lock(id text) RETURNS void
  LANGUAGE sql AS $$ SELECT pg_advisory_xact_lock(hashtextextended(id, 0)); $$;

-- P-01 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION promise_get(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE p promises;
BEGIN
  SELECT * INTO p FROM promises WHERE id = p_id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
END $$;

-- P-02 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION promise_create(
    p_id text, p_timeout_at bigint, p_param jsonb, p_tags jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  p promises; tgt text; delay bigint; st text;
  ph jsonb := COALESCE(p_param->'headers', '{}'::jsonb);
  pd text  := p_param->>'data';
  tags jsonb := COALESCE(p_tags, '{}'::jsonb);
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO p FROM promises WHERE id = p_id FOR UPDATE;
  IF FOUND THEN
    RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
  END IF;

  IF p_timeout_at > p_now THEN
    INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at)
    VALUES (p_id, 'pending', ph, pd, tags, p_timeout_at, p_now)
    RETURNING * INTO p;

    tgt := tags->>'resonate:target';
    IF tgt IS NOT NULL THEN
      -- dispatch now unless a future resonate:delay holds it (then arm at the delay)
      IF tags ? 'resonate:delay' AND (tags->>'resonate:delay')::bigint > p_now THEN
        delay := (tags->>'resonate:delay')::bigint;
      ELSE
        delay := p_now + _retry_timeout();
        PERFORM _emit_execute(tgt, p.id, 0);
      END IF;
      INSERT INTO tasks (id, state, version, timeout_at) VALUES (p.id, 'pending', 0, delay);
    END IF;
    RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
  ELSE
    -- created already past its deadline: settle immediately
    st := CASE WHEN COALESCE(tags->>'resonate:timer','') = 'true'
               THEN 'resolved' ELSE 'rejected_timedout' END;
    INSERT INTO promises (id, state, param_headers, param_data, tags,
                          timeout_at, created_at, settled_at)
    VALUES (p_id, st, ph, pd, tags, p_timeout_at, p_timeout_at, p_timeout_at)
    RETURNING * INTO p;
    IF tags ? 'resonate:target' THEN
      INSERT INTO tasks (id, state, version) VALUES (p.id, 'fulfilled', 0);
    END IF;
    RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
  END IF;
EXCEPTION WHEN unique_violation THEN
  -- lost a create race; the winner's row is authoritative
  SELECT * INTO p FROM promises WHERE id = p_id;
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
END $$;

-- P-03 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION promise_settle(
    p_id text, p_state text, p_value jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  p promises;
  vh jsonb := COALESCE(p_value->'headers', '{}'::jsonb);
  vd text  := p_value->>'data';
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO p FROM promises WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;

  IF p.state = 'pending' AND p.timeout_at > p_now THEN
    UPDATE promises
       SET state = p_state, value_headers = vh, value_data = vd, settled_at = p_now
     WHERE id = p.id
     RETURNING * INTO p;
    PERFORM _cascade_settle(p, p_now);
  END IF;
  -- projection covers pending&expired and already-settled
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
END $$;

-- P-04 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION promise_register_callback(
    p_awaited text, p_awaiter text, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE pa promises; pw promises;
BEGIN
  PERFORM _lock(LEAST(p_awaited, p_awaiter));
  PERFORM _lock(GREATEST(p_awaited, p_awaiter));

  SELECT * INTO pa FROM promises WHERE id = p_awaited FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO pw FROM promises WHERE id = p_awaiter;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 422); END IF;
  IF NOT (pw.tags ? 'resonate:target') THEN RETURN jsonb_build_object('status', 422); END IF;

  IF pa.state = 'pending' THEN
    IF pa.timeout_at > p_now THEN
      IF pw.state = 'pending' AND pw.timeout_at > p_now THEN
        INSERT INTO callbacks (awaited_id, awaiter_id) VALUES (p_awaited, p_awaiter)
          ON CONFLICT DO NOTHING;
      END IF;
    END IF;
  END IF;
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(pa, p_now));
END $$;

-- P-05 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION promise_register_listener(
    p_awaited text, p_address text, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE pa promises;
BEGIN
  PERFORM _lock(p_awaited);
  SELECT * INTO pa FROM promises WHERE id = p_awaited FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;

  IF pa.state = 'pending' AND pa.timeout_at > p_now THEN
    INSERT INTO listeners (awaited_id, address) VALUES (p_awaited, p_address)
      ON CONFLICT DO NOTHING;
  END IF;
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(pa, p_now));
END $$;

-- P-06 (unspecified) ---------------------------------------------------------
CREATE OR REPLACE FUNCTION promise_search(p_req jsonb, p_now bigint) RETURNS jsonb
  LANGUAGE sql AS $$ SELECT jsonb_build_object('status', 501); $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 4 · TASK ACTIONS · T-01 .. T-11
-- ═══════════════════════════════════════════════════════════════════════════
SET search_path TO resonate, public;

-- T-01 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_get(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  SELECT * INTO t FROM tasks WHERE id = p_id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;

  IF p.state = 'pending' AND p.timeout_at > p_now THEN
    RETURN jsonb_build_object('status', 200, 'task', _task_json(t));
  END IF;
  -- otherwise the task is reported as fulfilled (projection)
  RETURN jsonb_build_object('status', 200, 'task', jsonb_build_object(
    'id', t.id, 'state', 'fulfilled', 'version', t.version,
    'resumes', 0, 'ttl', NULL, 'pid', NULL));
END $$;

-- T-02 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_create(
    p_pid text, p_ttl bigint, p_action jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  a_id  text   := p_action->>'id';
  a_to  bigint := (p_action->>'timeoutAt')::bigint;
  a_ph  jsonb  := COALESCE(p_action#>'{param,headers}', '{}'::jsonb);
  a_pd  text   := p_action#>>'{param,data}';
  a_tags jsonb := COALESCE(p_action->'tags', '{}'::jsonb);
  p promises; t tasks; st text;
BEGIN
  PERFORM _lock(a_id);
  SELECT * INTO p FROM promises WHERE id = a_id FOR UPDATE;

  IF NOT FOUND THEN
    IF a_to > p_now THEN
      INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at)
      VALUES (a_id, 'pending', a_ph, a_pd, a_tags, a_to, p_now) RETURNING * INTO p;
      INSERT INTO tasks (id, state, version, ttl, pid, timeout_at)
      VALUES (p.id, 'acquired', 1, p_ttl, p_pid, p_now + p_ttl) RETURNING * INTO t;
      RETURN jsonb_build_object('status', 200, 'task', _task_json(t),
                                'promise', _promise_json_raw(p));
    ELSE
      st := CASE WHEN COALESCE(a_tags->>'resonate:timer','') = 'true'
                 THEN 'resolved' ELSE 'rejected_timedout' END;
      INSERT INTO promises (id, state, param_headers, param_data, tags,
                            timeout_at, created_at, settled_at)
      VALUES (a_id, st, a_ph, a_pd, a_tags, a_to, a_to, a_to) RETURNING * INTO p;
      INSERT INTO tasks (id, state, version) VALUES (p.id, 'fulfilled', 0) RETURNING * INTO t;
      RETURN jsonb_build_object('status', 200, 'task', _task_json(t),
                                'promise', _promise_json_raw(p));
    END IF;
  END IF;

  -- promise already exists: (re)claim its task
  IF NOT (p.tags ? 'resonate:target') THEN RETURN jsonb_build_object('status', 422); END IF;
  SELECT * INTO t FROM tasks WHERE id = p.id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state = 'fulfilled' THEN
    RETURN jsonb_build_object('status', 200, 'task', _task_json(t),
                              'promise', _promise_json_raw(p));
  ELSIF t.state = 'pending' THEN
    DELETE FROM task_resumes WHERE task_id = t.id;
    UPDATE tasks SET state = 'acquired', version = version + 1, ttl = p_ttl, pid = p_pid,
                     timeout_at = p_now + p_ttl
      WHERE id = t.id RETURNING * INTO t;
    RETURN jsonb_build_object('status', 200, 'task', _task_json(t),
                              'promise', _promise_json_raw(p));
  ELSE
    RETURN jsonb_build_object('status', 409);
  END IF;
END $$;

-- T-03 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_acquire(
    p_id text, p_version int, p_pid text, p_ttl bigint, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state <> 'pending' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version <> p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  DELETE FROM task_resumes WHERE task_id = t.id;
  UPDATE tasks SET state = 'acquired', version = version + 1, ttl = p_ttl, pid = p_pid,
                   timeout_at = p_now + p_ttl
    WHERE id = t.id RETURNING * INTO t;
  RETURN jsonb_build_object('status', 200, 'task', _task_json(t),
                            'promise', _promise_json(p, p_now));
END $$;

-- T-04 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_fence(
    p_id text, p_version int, p_action jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises; r jsonb; kind text := p_action->>'kind'; req jsonb := p_action->'req';
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state <> 'acquired' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version <> p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  IF kind = 'create' THEN
    r := promise_create(req->>'id', (req->>'timeoutAt')::bigint,
                        req->'param', COALESCE(req->'tags','{}'::jsonb), p_now);
    RETURN jsonb_build_object('status', 200, 'action', jsonb_build_object('create', r));
  ELSIF kind = 'settle' THEN
    r := promise_settle(req->>'id', req->>'state', req->'value', p_now);
    RETURN jsonb_build_object('status', 200, 'action', jsonb_build_object('settle', r));
  ELSE
    RETURN jsonb_build_object('status', 422);
  END IF;
END $$;

-- T-05 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_heartbeat(p_pid text, p_tasks jsonb, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
BEGIN
  -- advisory locks in sorted order (the array arrives in client order)
  PERFORM _lock(lid) FROM (
    SELECT DISTINCT ref->>'id' AS lid
    FROM jsonb_array_elements(COALESCE(p_tasks, '[]'::jsonb)) ref
  ) s ORDER BY lid;

  -- extend the lease of every still-matching ref
  UPDATE tasks t SET timeout_at = p_now + COALESCE(t.ttl, 0)
  FROM (SELECT ref->>'id' AS id, (ref->>'version')::int AS version
        FROM jsonb_array_elements(COALESCE(p_tasks, '[]'::jsonb)) ref) r
  JOIN promises p ON p.id = r.id
  WHERE t.id = r.id AND t.state = 'acquired' AND t.version = r.version AND t.pid = p_pid
    AND p.state = 'pending' AND p.timeout_at > p_now;

  RETURN jsonb_build_object('status', 200);
END $$;

-- T-06 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_suspend(
    p_id text, p_version int, p_actions jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE t tasks; tp promises; missing int; settled bool;
BEGIN
  -- Lock the task AND every awaited promise in sorted order. The awaited lock is
  -- essential: without it a concurrent settle can slip between our state-check and
  -- our callback-register, firing no callback and stranding the task suspended.
  PERFORM _lock(lid) FROM (
    SELECT p_id AS lid
    UNION
    SELECT act->>'awaited'
      FROM jsonb_array_elements(COALESCE(p_actions, '[]'::jsonb)) AS act
  ) s ORDER BY lid;
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO tp FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state <> 'acquired' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF tp.state <> 'pending' OR tp.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version <> p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  -- validate awaiteds: any missing -> 422; any already settled/expired -> 300
  SELECT count(*) FILTER (WHERE pa.id IS NULL),
         COALESCE(bool_or(pa.state <> 'pending' OR pa.timeout_at <= p_now), false)
    INTO missing, settled
  FROM jsonb_array_elements(COALESCE(p_actions, '[]'::jsonb)) act
  LEFT JOIN promises pa ON pa.id = act->>'awaited';

  IF missing > 0 THEN RETURN jsonb_build_object('status', 422); END IF;
  IF settled THEN
    DELETE FROM task_resumes WHERE task_id = t.id;
    RETURN jsonb_build_object('status', 300);
  END IF;

  -- register callbacks in action order
  INSERT INTO callbacks (awaited_id, awaiter_id)
  SELECT act->>'awaited', t.id
  FROM jsonb_array_elements(COALESCE(p_actions, '[]'::jsonb)) act
  ON CONFLICT DO NOTHING;
  DELETE FROM task_resumes WHERE task_id = t.id;
  UPDATE tasks SET state = 'suspended', pid = NULL, ttl = NULL WHERE id = t.id;
  RETURN jsonb_build_object('status', 200);
END $$;

-- T-07 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_fulfill(
    p_id text, p_version int, p_action jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  t tasks; p promises;
  vh jsonb := COALESCE(p_action#>'{value,headers}', '{}'::jsonb);
  vd text  := p_action#>>'{value,data}';
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state <> 'acquired' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version <> p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  UPDATE promises SET state = p_action->>'state', value_headers = vh, value_data = vd,
                      settled_at = p_now
    WHERE id = p.id RETURNING * INTO p;
  PERFORM _cascade_settle(p, p_now);
  RETURN jsonb_build_object('status', 200, 'promise', _promise_json(p, p_now));
END $$;

-- T-08 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_release(p_id text, p_version int, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 409); END IF;

  IF t.state <> 'acquired' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF p.state <> 'pending' OR p.timeout_at <= p_now THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.version <> p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  UPDATE tasks SET state = 'pending', pid = NULL, ttl = NULL,
                   timeout_at = p_now + _retry_timeout() WHERE id = t.id;
  PERFORM _emit_execute(COALESCE(p.target, ''), t.id, t.version);
  RETURN jsonb_build_object('status', 200);
END $$;

-- T-09 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_halt(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE t tasks;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  IF t.state = 'fulfilled' THEN RETURN jsonb_build_object('status', 409); END IF;
  IF t.state = 'halted' THEN RETURN jsonb_build_object('status', 200); END IF;

  UPDATE tasks SET state = 'halted', pid = NULL, ttl = NULL WHERE id = t.id;
  RETURN jsonb_build_object('status', 200);
END $$;

-- T-10 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION task_continue(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  IF t.state <> 'halted' THEN RETURN jsonb_build_object('status', 409); END IF;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;

  UPDATE tasks SET state = 'pending', timeout_at = p_now + _retry_timeout() WHERE id = t.id;
  PERFORM _emit_execute(COALESCE(p.target, ''), t.id, t.version);
  RETURN jsonb_build_object('status', 200);
END $$;

-- T-11 (unspecified) ---------------------------------------------------------
CREATE OR REPLACE FUNCTION task_search(p_req jsonb, p_now bigint) RETURNS jsonb
  LANGUAGE sql AS $$ SELECT jsonb_build_object('status', 501); $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 5 · SCHEDULE ACTIONS · S-01 .. S-04
-- ═══════════════════════════════════════════════════════════════════════════
SET search_path TO resonate, public;

-- S-01 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION schedule_get(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE s schedules;
BEGIN
  SELECT * INTO s FROM schedules WHERE id = p_id;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  RETURN jsonb_build_object('status', 200, 'schedule', _schedule_json(s));
END $$;

-- S-02 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION schedule_create(
    p_id text, p_cron text, p_promise_id text, p_promise_timeout bigint,
    p_promise_param jsonb, p_promise_tags jsonb, p_now bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE s schedules; nxt bigint;
BEGIN
  PERFORM _lock('sched:' || p_id);
  SELECT * INTO s FROM schedules WHERE id = p_id FOR UPDATE;
  IF FOUND THEN
    RETURN jsonb_build_object('status', 200, 'schedule', _schedule_json(s));
  END IF;

  nxt := _next_cron(p_cron, p_now);
  INSERT INTO schedules (id, cron, promise_id, promise_timeout,
                         promise_param_headers, promise_param_data, promise_tags,
                         created_at, next_run_at, last_run_at)
  VALUES (p_id, p_cron, p_promise_id, p_promise_timeout,
          COALESCE(p_promise_param->'headers','{}'::jsonb), p_promise_param->>'data',
          COALESCE(p_promise_tags,'{}'::jsonb), p_now, nxt, NULL)
  RETURNING * INTO s;
  RETURN jsonb_build_object('status', 200, 'schedule', _schedule_json(s));
END $$;

-- S-03 -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION schedule_delete(p_id text, p_now bigint) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE s schedules;
BEGIN
  PERFORM _lock('sched:' || p_id);
  SELECT * INTO s FROM schedules WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('status', 404); END IF;
  DELETE FROM schedules WHERE id = p_id;
  RETURN jsonb_build_object('status', 200);
END $$;

-- S-04 (unspecified) ---------------------------------------------------------
CREATE OR REPLACE FUNCTION schedule_search(p_req jsonb, p_now bigint) RETURNS jsonb
  LANGUAGE sql AS $$ SELECT jsonb_build_object('status', 501); $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 6 · INTERNAL TRANSITIONS · timeouts + process_timeouts driver
-- ═══════════════════════════════════════════════════════════════════════════
-- Postgres has no timers -- timeouts are rows. Each on_* is the exact transition
-- from spec 02-timeouts; pg_cron drives process_timeouts (section 9).
SET search_path TO resonate, public;

-- promise timeout: a pending promise reaches timeoutAt -----------------------
CREATE OR REPLACE FUNCTION _on_promise_timeout(p_id text, p_now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE p promises; st text;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO p FROM promises WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;
  IF p.state <> 'pending' THEN RETURN; END IF;

  st := CASE WHEN p.is_timer THEN 'resolved' ELSE 'rejected_timedout' END;
  UPDATE promises SET state = st, settled_at = p.timeout_at WHERE id = p.id RETURNING * INTO p;
  PERFORM _cascade_settle(p, p_now);
END $$;

-- task retry timeout (kind 0): re-dispatch a still-pending task --------------
CREATE OR REPLACE FUNCTION _on_task_retry_timeout(p_id text, p_now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;
  IF t.state <> 'pending' THEN RETURN; END IF;

  UPDATE tasks SET timeout_at = p_now + _retry_timeout() WHERE id = t.id;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN; END IF;
  PERFORM _emit_execute(COALESCE(p.target, ''), t.id, t.version);
END $$;

-- task lease timeout (kind 1): reclaim an abandoned acquired task ------------
CREATE OR REPLACE FUNCTION _on_task_lease_timeout(p_id text, p_now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE t tasks; p promises;
BEGIN
  PERFORM _lock(p_id);
  SELECT * INTO t FROM tasks WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;
  IF t.state <> 'acquired' THEN RETURN; END IF;

  UPDATE tasks SET state = 'pending', pid = NULL, ttl = NULL,
                   timeout_at = p_now + _retry_timeout() WHERE id = t.id;
  SELECT * INTO p FROM promises WHERE id = t.id;
  IF NOT FOUND THEN RETURN; END IF;
  PERFORM _emit_execute(COALESCE(p.target, ''), t.id, t.version);
END $$;

-- schedule timeout: fire (and catch up) a due schedule ----------------------
CREATE OR REPLACE FUNCTION _on_schedule_timeout(p_id text, p_now bigint) RETURNS void
  LANGUAGE plpgsql AS $$
DECLARE s schedules; cron_time bigint; pid text;
BEGIN
  PERFORM _lock('sched:' || p_id);
  SELECT * INTO s FROM schedules WHERE id = p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN; END IF;

  -- catchUp: create one promise per missed occurrence up to now
  WHILE s.next_run_at <= p_now LOOP
    cron_time := s.next_run_at;
    pid := _expand(s.promise_id, s.id, cron_time);
    PERFORM promise_create(
      pid, cron_time + s.promise_timeout,
      jsonb_build_object('headers', s.promise_param_headers, 'data', s.promise_param_data),
      s.promise_tags, cron_time);
    s.last_run_at := cron_time;
    s.next_run_at := _next_cron(s.cron, cron_time);
  END LOOP;

  UPDATE schedules SET last_run_at = s.last_run_at, next_run_at = s.next_run_at WHERE id = s.id;
END $$;

-- Per-kind timeout sweeps: scan one class's due rows, apply the transition,
-- return the count. Broken out so a driver can pace classes differently. Each is
-- idempotent -- every transition rearms/deletes its timer strictly beyond p_now.
CREATE OR REPLACE FUNCTION process_promise_timeouts(p_now bigint) RETURNS int
  LANGUAGE plpgsql AS $$
DECLARE r record; cnt int := 0;
BEGIN
  FOR r IN SELECT id FROM promises
           WHERE _promise_timed(promises) AND timeout_at <= p_now
           ORDER BY timeout_at, id LOOP
    PERFORM _on_promise_timeout(r.id, p_now); cnt := cnt + 1;
  END LOOP;
  RETURN cnt;
END $$;

CREATE OR REPLACE FUNCTION process_task_timeouts(p_now bigint) RETURNS int
  LANGUAGE plpgsql AS $$
DECLARE r record; cnt int := 0;
BEGIN
  FOR r IN SELECT id, state FROM tasks
           WHERE state IN ('pending','acquired') AND timeout_at <= p_now
           ORDER BY timeout_at, id LOOP
    IF r.state = 'pending' THEN PERFORM _on_task_retry_timeout(r.id, p_now);
    ELSE                        PERFORM _on_task_lease_timeout(r.id, p_now); END IF;
    cnt := cnt + 1;
  END LOOP;
  RETURN cnt;
END $$;

CREATE OR REPLACE FUNCTION process_schedule_timeouts(p_now bigint) RETURNS int
  LANGUAGE plpgsql AS $$
DECLARE r record; cnt int := 0;
BEGIN
  FOR r IN SELECT id FROM schedules WHERE next_run_at <= p_now ORDER BY next_run_at, id LOOP
    PERFORM _on_schedule_timeout(r.id, p_now); cnt := cnt + 1;
  END LOOP;
  RETURN cnt;
END $$;

-- Apply all timeouts due at p_now. Order promises -> tasks -> schedules is
-- load-bearing (a promise cascade may fulfill a task before its timer is seen).
CREATE OR REPLACE FUNCTION process_timeouts(p_now bigint) RETURNS int
  LANGUAGE plpgsql AS $$
DECLARE cnt int; _attempt int;
BEGIN
  -- retry on deadlock: the sweep locks many ids; the victim rolls back (releasing
  -- locks) and re-runs, and the sweeps are idempotent
  FOR _attempt IN 1..50 LOOP
    BEGIN
      cnt := 0;
      cnt := cnt + process_promise_timeouts(p_now);
      cnt := cnt + process_task_timeouts(p_now);
      cnt := cnt + process_schedule_timeouts(p_now);
      RETURN cnt;
    EXCEPTION WHEN deadlock_detected THEN
      IF _attempt >= 50 THEN RAISE; END IF;
    END;
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION process_timeouts() RETURNS int
  LANGUAGE sql AS $$ SELECT process_timeouts((extract(epoch from clock_timestamp()) * 1000)::bigint); $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 7 · WIRE DISPATCHER · resonate_rpc(jsonb) — resonate-sdk-ts protocol
-- ═══════════════════════════════════════════════════════════════════════════
-- The one entrypoint the SDK network layer calls. Request:
--   { kind, head:{corrId, version, "resonate:debug_time"?}, data:{...} }
-- Response: { kind, head:{corrId, status, version}, data:{...} | "<error>" }.
SET search_path TO resonate, public;

CREATE OR REPLACE FUNCTION _status_text(status int) RETURNS text
  LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE status
    WHEN 400 THEN 'bad request'    WHEN 404 THEN 'not found'
    WHEN 409 THEN 'conflict'       WHEN 422 THEN 'unprocessable entity'
    WHEN 501 THEN 'not implemented' ELSE 'error' END;
$$;

-- Shape a response's `data` for the kind + raw action result. 2xx/3xx -> object; else string.
CREATE OR REPLACE FUNCTION _rpc_data(kind text, status int, r jsonb) RETURNS jsonb
  LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF status NOT IN (200, 300) THEN
    RETURN to_jsonb(_status_text(status));
  END IF;
  RETURN CASE kind
    WHEN 'promise.get'               THEN jsonb_build_object('promise', r->'promise')
    WHEN 'promise.create'            THEN jsonb_build_object('promise', r->'promise')
    WHEN 'promise.settle'            THEN jsonb_build_object('promise', r->'promise')
    WHEN 'promise.register_callback' THEN jsonb_build_object('promise', r->'promise')
    WHEN 'promise.register_listener' THEN jsonb_build_object('promise', r->'promise')
    WHEN 'task.get'                  THEN jsonb_build_object('task', r->'task')
    WHEN 'task.create'               THEN jsonb_build_object('task', r->'task', 'promise', r->'promise', 'preload', '[]'::jsonb)
    WHEN 'task.acquire'              THEN jsonb_build_object('task', r->'task', 'promise', r->'promise', 'preload', '[]'::jsonb)
    WHEN 'task.fulfill'              THEN jsonb_build_object('promise', r->'promise')
    WHEN 'task.suspend'              THEN CASE WHEN status = 300 THEN jsonb_build_object('preload', '[]'::jsonb) ELSE '{}'::jsonb END
    WHEN 'schedule.get'              THEN jsonb_build_object('schedule', r->'schedule')
    WHEN 'schedule.create'           THEN jsonb_build_object('schedule', r->'schedule')
    -- task.release / task.halt / task.continue / task.heartbeat / schedule.delete
    ELSE '{}'::jsonb
  END;
END $$;

CREATE OR REPLACE FUNCTION resonate_rpc(req jsonb) RETURNS jsonb
  LANGUAGE plpgsql AS $$
DECLARE
  kind text := req->>'kind';
  head jsonb := COALESCE(req->'head', '{}'::jsonb);
  corr text := head->>'corrId';
  ver  text := COALESCE(head->>'version', '1');
  d    jsonb := COALESCE(req->'data', '{}'::jsonb);
  now  bigint := COALESCE((head->>'resonate:debug_time')::bigint,
                          (extract(epoch from clock_timestamp()) * 1000)::bigint);
  r jsonb; status int; data jsonb;
  ik text; inner_raw jsonb; inner_kind text; inner_status int; inner_res jsonb;
  _attempt int;
BEGIN
  -- retry on deadlock: multi-id ops take per-id advisory locks; the victim rolls
  -- back, releasing locks, and retries -- transparent to the client
  FOR _attempt IN 1..50 LOOP
   BEGIN
  -- debug.* route to the optional debug module (test/debug/debug.sql, not deployed
  -- in prod); absent -> undefined_function -> 501.
  IF kind LIKE 'debug.%' THEN
   BEGIN
     EXECUTE 'SELECT resonate._debug($1, $2, $3)' INTO r USING kind, d, now;
   EXCEPTION WHEN undefined_function THEN
     r := jsonb_build_object('status', 501);
   END;
  ELSE
  CASE kind
    WHEN 'promise.get' THEN
      r := promise_get(d->>'id', now);
    WHEN 'promise.create' THEN
      r := promise_create(d->>'id', (d->>'timeoutAt')::bigint, d->'param', COALESCE(d->'tags','{}'::jsonb), now);
    WHEN 'promise.settle' THEN
      r := promise_settle(d->>'id', d->>'state', d->'value', now);
    WHEN 'promise.register_callback' THEN
      r := promise_register_callback(d->>'awaited', d->>'awaiter', now);
    WHEN 'promise.register_listener' THEN
      r := promise_register_listener(d->>'awaited', d->>'address', now);
    WHEN 'promise.search' THEN
      r := promise_search(d, now);

    WHEN 'task.get' THEN
      r := task_get(d->>'id', now);
    WHEN 'task.create' THEN
      -- action is a full PromiseCreateReq; the procedure wants its .data
      r := task_create(d->>'pid', (d->>'ttl')::bigint, (d->'action')->'data', now);
    WHEN 'task.acquire' THEN
      r := task_acquire(d->>'id', (d->>'version')::int, d->>'pid', (d->>'ttl')::bigint, now);
    WHEN 'task.release' THEN
      r := task_release(d->>'id', (d->>'version')::int, now);
    WHEN 'task.heartbeat' THEN
      r := task_heartbeat(d->>'pid', COALESCE(d->'tasks','[]'::jsonb), now);
    WHEN 'task.suspend' THEN
      -- actions are PromiseRegisterCallbackReq[]; unwrap each to its .data {awaited,awaiter}
      r := task_suspend(d->>'id', (d->>'version')::int,
             COALESCE((SELECT jsonb_agg(e->'data') FROM jsonb_array_elements(d->'actions') e), '[]'::jsonb),
             now);
    WHEN 'task.fulfill' THEN
      r := task_fulfill(d->>'id', (d->>'version')::int, (d->'action')->'data', now);
    WHEN 'task.halt' THEN
      r := task_halt(d->>'id', now);
    WHEN 'task.continue' THEN
      r := task_continue(d->>'id', now);
    WHEN 'task.fence' THEN
      inner_kind := CASE WHEN (d->'action')->>'kind' = 'promise.create' THEN 'create' ELSE 'settle' END;
      r := task_fence(d->>'id', (d->>'version')::int,
                      jsonb_build_object('kind', inner_kind, 'req', (d->'action')->'data'), now);
    WHEN 'task.search' THEN
      r := task_search(d, now);

    WHEN 'schedule.get' THEN
      r := schedule_get(d->>'id', now);
    WHEN 'schedule.create' THEN
      r := schedule_create(d->>'id', d->>'cron', d->>'promiseId', (d->>'promiseTimeout')::bigint,
                           d->'promiseParam', COALESCE(d->'promiseTags','{}'::jsonb), now);
    WHEN 'schedule.delete' THEN
      r := schedule_delete(d->>'id', now);
    WHEN 'schedule.search' THEN
      r := schedule_search(d, now);

    ELSE
      r := jsonb_build_object('status', 501);
  END CASE;
  END IF;

  status := (r->>'status')::int;

  IF kind LIKE 'debug.%' THEN
    data := COALESCE(r->'data', '{}'::jsonb);
  ELSIF kind = 'task.fence' AND status = 200 THEN
    -- wrap the inner promise.create/settle result as its own nested Response
    inner_raw    := COALESCE(r->'action'->'create', r->'action'->'settle');
    inner_kind   := CASE WHEN r->'action'->'create' IS NOT NULL THEN 'promise.create' ELSE 'promise.settle' END;
    inner_status := (inner_raw->>'status')::int;
    inner_res := jsonb_build_object(
      'kind', inner_kind,
      'head', jsonb_build_object('corrId', corr, 'status', inner_status, 'version', ver),
      'data', _rpc_data(inner_kind, inner_status, inner_raw));
    data := jsonb_build_object('action', inner_res, 'preload', '[]'::jsonb);
  ELSE
    data := _rpc_data(kind, status, r);
  END IF;

  RETURN jsonb_build_object(
    'kind', kind,
    'head', jsonb_build_object('corrId', corr, 'status', status, 'version', ver),
    'data', data);
   EXCEPTION WHEN deadlock_detected THEN
     IF _attempt >= 50 THEN RAISE; END IF;   -- exhausted retries: surface it
   END;
  END LOOP;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 8 · TEST/DEBUG SUPPORT — in test/debug/debug.sql (NOT deployed)
-- ═══════════════════════════════════════════════════════════════════════════
-- resonate_reset/apply/snapshot are dev/test-only; load test/debug/debug.sql on
-- top in dev/CI. debug.* actions route through resonate_rpc, else 501 (§ 7).
SET search_path TO resonate, public;


-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 9 · pg_cron TIMER DRIVER
-- ═══════════════════════════════════════════════════════════════════════════
-- pg_cron is the sole timer driver: it polls process_timeouts() every 5s so
-- promise/task/schedule timers advance with nothing external. Wrapped so a
-- missing/unschedulable pg_cron WARNs instead of failing the install -- but then
-- timers don't fire. Sub-minute cadence needs pg_cron >= 1.5, else once-a-minute.
DO $cron$
DECLARE
  v_db text := current_database();
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_cron') THEN
    RAISE WARNING 'resonate: pg_cron not available -- timers will NOT fire until it is installed and scheduled (section 9)';
    RETURN;
  END IF;

  BEGIN
    -- create only if absent: on Supabase pg_cron is pre-installed and re-running
    -- CREATE EXTENSION (even IF NOT EXISTS) errors and would abort the block
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
      CREATE EXTENSION pg_cron;
    END IF;

    -- schedule in THIS db via schedule_in_database (pg_cron >= 1.4); fall back to
    -- plain cron.schedule, then to once-a-minute on pre-1.5
    BEGIN
      PERFORM cron.schedule_in_database('resonate_process_timeouts', '5 seconds',
                'SELECT resonate.process_timeouts()', v_db);
    EXCEPTION WHEN OTHERS THEN
      BEGIN
        PERFORM cron.schedule('resonate_process_timeouts', '5 seconds',
                  'SELECT resonate.process_timeouts()');
      EXCEPTION WHEN OTHERS THEN
        PERFORM cron.schedule('resonate_process_timeouts', '* * * * *',
                  'SELECT resonate.process_timeouts()');
      END;
    END;

    RAISE NOTICE 'resonate: pg_cron enabled; process_timeouts scheduled (database %)', v_db;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'resonate: pg_cron present but could not be scheduled (%) -- timers will NOT fire until scheduled', SQLERRM;
  END;
END
$cron$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 10 · DISPATCH · addressed outbox drain (execute + unblock)
-- ═══════════════════════════════════════════════════════════════════════════
-- Each dequeue is a destructive, addressed, ORDER BY seq, FOR UPDATE SKIP LOCKED
-- read (idx_outbox_dispatch); a lost execute is re-emitted by the retry timer.

CREATE OR REPLACE FUNCTION dequeue_execute(p_target text, p_limit int DEFAULT 100)
  RETURNS TABLE(task_id text, version int)
  LANGUAGE sql AS $$
  WITH d AS (
    DELETE FROM outbox
    WHERE ctid IN (
      SELECT ctid FROM outbox
      -- pull targets only: http(s) targets are push-delivered, never dequeued
      WHERE kind = 'execute' AND address = p_target
        AND address NOT LIKE 'http://%' AND address NOT LIKE 'https://%'
      ORDER BY seq
      FOR UPDATE SKIP LOCKED
      LIMIT p_limit)
    RETURNING task_id, version, seq)
  SELECT task_id, version FROM d ORDER BY seq;
$$;

CREATE OR REPLACE FUNCTION dequeue_unblock(p_address text, p_limit int DEFAULT 100)
  RETURNS TABLE(promise jsonb)
  LANGUAGE sql AS $$
  WITH d AS (
    DELETE FROM outbox
    WHERE ctid IN (
      SELECT ctid FROM outbox
      -- pull targets only: http(s) targets are push-delivered, never dequeued
      WHERE kind = 'unblock' AND address = p_address
        AND address NOT LIKE 'http://%' AND address NOT LIKE 'https://%'
      ORDER BY seq
      FOR UPDATE SKIP LOCKED
      LIMIT p_limit)
    RETURNING promise, seq)
  SELECT promise FROM d ORDER BY seq;
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 11 · RETENTION · gc
-- ═══════════════════════════════════════════════════════════════════════════
-- Terminal rows are marked, never auto-deleted. gc(settled_before, limit) deletes
-- settled promises at/<= cutoff, oldest first; ON DELETE CASCADE takes tasks,
-- resumes, callbacks, listeners. Cutoff must exceed the id-replay window (ids are
-- idempotent). Admin-only; batched. Each collected promise takes its undelivered
-- unblock with it -- deleted ONLY for promises collected in THIS batch (by id),
-- never by cutoff alone, or it would reap the unblocks of retained promises.

CREATE OR REPLACE FUNCTION gc(p_settled_before bigint, p_limit int DEFAULT 10000)
  RETURNS bigint LANGUAGE sql AS $$
  WITH doomed AS (
    SELECT ctid FROM promises
    WHERE state <> 'pending' AND settled_at <= p_settled_before
    ORDER BY settled_at
    LIMIT p_limit),
  del AS (
    DELETE FROM promises WHERE ctid IN (SELECT ctid FROM doomed) RETURNING id),
  unb AS (
    DELETE FROM outbox
    WHERE kind = 'unblock' AND promise->>'id' IN (SELECT id FROM del)
    RETURNING 1)
  SELECT count(*) FROM del;
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- ▐  SECTION 12 · AUTHZ · least-privilege resonate_worker role
-- ═══════════════════════════════════════════════════════════════════════════
-- Lock EXECUTE (default PUBLIC) to one worker role. The wire entrypoints are
-- SECURITY DEFINER (run as owner) so the procedures are the sole path to state;
-- everything else (DDL, gc, ticker) stays with the owner. resonate_worker is NOLOGIN.

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'resonate_worker') THEN
    CREATE ROLE resonate_worker;
  END IF;
END $$;

REVOKE ALL     ON ALL TABLES    IN SCHEMA resonate FROM PUBLIC;
REVOKE ALL     ON ALL SEQUENCES IN SCHEMA resonate FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA resonate FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA resonate REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

GRANT USAGE ON SCHEMA resonate TO resonate_worker;

ALTER FUNCTION resonate_rpc(jsonb)        SECURITY DEFINER;
ALTER FUNCTION dequeue_execute(text, int) SECURITY DEFINER;
ALTER FUNCTION dequeue_unblock(text, int) SECURITY DEFINER;

-- Pin search_path on EVERY function so resolution is deterministic and self-
-- contained -- no reliance on ambient session/database search_path. Re-applied
-- each install; covers SECURITY DEFINER functions too.
DO $pin$
DECLARE f regprocedure;
BEGIN
  FOR f IN
    SELECT p.oid::regprocedure
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'resonate'
  LOOP
    EXECUTE format('ALTER FUNCTION %s SET search_path = resonate, pg_temp', f);
  END LOOP;
END $pin$;

-- The worker surface: send (resonate_rpc), drain (dequeue_*), pump helpers. NOT
-- process_timeouts -- driving timers is privileged; run it from an admin/ticker.
GRANT EXECUTE ON FUNCTION
  resonate_rpc(jsonb),
  dequeue_execute(text, int),
  dequeue_unblock(text, int),
  outbox_channel(text)
  TO resonate_worker;
