-- =============================================================================
-- The specification's three KNOWN GAPS, as table constraints
-- =============================================================================
-- These are not in the property catalogue. The specification records them as
-- constraints that are true of the PROTOCOL and false of the machine — a
-- reachable state violates each, so putting them in the catalogue would turn
-- the sweep red. Its note on them:
--
--   "They are here because an implementation should enforce them at its doors
--    even though the specification does not."
--
-- The randomised differential run reaches all three on the stock server, in
-- both layouts, so they are reachable here too, not just in the model.
--
-- Enabling this file CHANGES BEHAVIOUR. A request that would produce one of
-- these states currently succeeds; with these constraints it raises, and
-- `resonate_rpc`'s exception arm turns the raise into a 500 rather than the
-- 400/422 a door check would give. Enforce them properly at the doors before
-- relying on these; the constraints are the backstop, not the interface.
--
-- Apply AFTER resonate-single.sql and constraints.sql:
--   psql -d yourdb -f resonate-single.sql -f constraints.sql -f constraints-gaps.sql
-- =============================================================================

SET search_path TO resonate, public;

-- A lease of length zero expires at the instant it is granted. task.create and
-- task.acquire accept ttl = 0 and there is no lower bound anywhere.
ALTER TABLE promises ADD CONSTRAINT well_formed_task_ttl_positive
  CHECK (task_state IS DISTINCT FROM 'acquired' OR ttl > 0);

-- `tags ? 'resonate:target'` is true for the empty string, so the task is
-- created and its `execute` is enqueued to an address nothing can receive.
-- A target names a worker group, not a URL, so non-emptiness is the whole
-- claim — `_addr_valid` would be the wrong predicate here.
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_target_is_nonempty
  CHECK (target IS NULL OR target <> '');

-- A first dispatch scheduled at or after the promise's own deadline can never
-- fire: by the time it comes due the promise has timed out and the dispatch
-- guard refuses. `_parse_nat` is total, matching the machine, so a malformed
-- delay becomes a garbage instant rather than an error — with the same effect,
-- which is what the constraint rejects.
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_delay_before_deadline
  CHECK (NOT (tags ? 'resonate:delay')
         OR _parse_nat(tags->>'resonate:delay') < timeout_at);
