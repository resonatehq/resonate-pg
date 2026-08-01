-- =============================================================================
-- Specula trace module for resonate-pg  (Phase 2.5)
-- =============================================================================
-- Loaded AFTER the instrumented resonate.sql. Provides the emit call that the
-- instrumentation patch inserts into the real handler bodies, plus the state
-- snapshot it records.
--
-- Category A (distributed / message-passing): one append-only table stands in
-- for the single mutex-protected NDJSON writer. Every action runs in its own
-- transaction and takes advisory locks on the ids it touches, so the `seq`
-- column is a faithful total order over committed actions.
-- =============================================================================

SET search_path TO resonate, public;

CREATE TABLE IF NOT EXISTS resonate.trace (
  seq   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event TEXT   NOT NULL,
  now   BIGINT NOT NULL,
  state JSONB  NOT NULL
);

-- Post-state snapshot, in the vocabulary of base.tla's variables.
-- Read straight off the real tables inside the emitting transaction.
CREATE OR REPLACE FUNCTION resonate._trace_state() RETURNS jsonb
  LANGUAGE sql AS $$
  SELECT jsonb_build_object(
    'promises', COALESCE((
      SELECT jsonb_object_agg(p.id, jsonb_build_object(
               'state',     p.state,
               'timeoutAt', p.timeout_at,
               'external',  p.external,
               'isTimer',   p.is_timer,
               'hasTarget', (p.tags ? 'resonate:target')))
      FROM resonate.promises p), '{}'::jsonb),
    'tasks', COALESCE((
      SELECT jsonb_object_agg(t.id, jsonb_build_object(
               'state',     t.state,
               'version',   t.version,
               -- base.tla carries 0 where the row carries NULL
               'timeoutAt', COALESCE(t.timeout_at, 0)))
      FROM resonate.tasks t), '{}'::jsonb),
    'callbacks', COALESCE((
      SELECT jsonb_agg(jsonb_build_array(c.awaited_id, c.awaiter_id)
                       ORDER BY c.awaited_id, c.awaiter_id)
      FROM resonate.callbacks c), '[]'::jsonb),
    'listeners', COALESCE((
      SELECT jsonb_agg(jsonb_build_array(l.awaited_id, l.address)
                       ORDER BY l.awaited_id, l.address)
      FROM resonate.listeners l), '[]'::jsonb),
    'resumes', COALESCE((
      SELECT jsonb_agg(jsonb_build_array(r.task_id, r.awaited_id)
                       ORDER BY r.task_id, r.awaited_id)
      FROM resonate.task_resumes r), '[]'::jsonb),
    'execs', COALESCE((
      SELECT jsonb_agg(jsonb_build_array(o.task_id, o.version) ORDER BY o.task_id)
      FROM resonate.outbox o WHERE o.kind = 'execute'), '[]'::jsonb),
    'unblocks', COALESCE((
      SELECT jsonb_agg(jsonb_build_array(o.promise->>'id', o.address)
                       ORDER BY o.promise->>'id', o.address)
      FROM resonate.outbox o WHERE o.kind = 'unblock'), '[]'::jsonb));
$$;

CREATE OR REPLACE FUNCTION resonate._trace_emit(ev text, n bigint) RETURNS void
  LANGUAGE sql AS $$
  INSERT INTO resonate.trace (event, now, state)
  SELECT ev, n, resonate._trace_state();
$$;

CREATE OR REPLACE FUNCTION resonate._trace_reset() RETURNS void
  LANGUAGE sql AS $$ DELETE FROM resonate.trace; $$;

-- ---------------------------------------------------------------------------
-- Harness tuning: base.tla is checked with Retry = 1 and Ttl = 1, so the
-- scenarios run in the same time domain as the model. This changes a timing
-- constant only -- no protocol logic -- the same way a Raft harness shortens
-- the election timeout. Recorded in the trace's config line.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION resonate._retry_timeout() RETURNS bigint
  LANGUAGE sql IMMUTABLE AS $$ SELECT 1::bigint $$;

ALTER FUNCTION resonate._trace_state()        SET search_path = resonate, pg_temp;
ALTER FUNCTION resonate._trace_emit(text, bigint) SET search_path = resonate, pg_temp;
ALTER FUNCTION resonate._trace_reset()        SET search_path = resonate, pg_temp;
ALTER FUNCTION resonate._retry_timeout()      SET search_path = resonate, pg_temp;
