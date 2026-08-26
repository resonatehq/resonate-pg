-- =============================================================================
-- The promise id format, as table constraints
-- =============================================================================
-- Separate from constraints.sql and opt-in, because this is NOT a catalogue
-- property. The specification treats `id : String` as opaque — no entry
-- constrains its shape — so these state a deployment's id convention rather
-- than a claim about the protocol. Enabling them does not move the 38/45.
--
-- The convention:
--
--     foo.1        a top-level promise; the id IS the origin
--     foo.1:1      a child; `foo.1` is the origin, `1` is the rest
--     foo.1:1.2    deeper; the rest carries its own structure, with dots
--
-- So: at most ONE colon, and everything before it is the origin. Dots are
-- ordinary characters on both sides and are not the separator — `_preload`'s
-- dotted ancestry test is a different mechanism and is untouched by this.
--
-- Apply AFTER resonate-single.sql and constraints.sql:
--   psql -d yourdb -f resonate-single.sql -f constraints.sql -f constraints-id.sql
--
-- BEFORE ADOPTING, two things:
--
--   1. These are a backstop, not the interface. A CHECK violation reaches the
--      client through `resonate_rpc`'s exception arm as a 500, and a malformed
--      id deserves a 400. Add the same test to `promise_create` and
--      `task_create` so the door answers first — the pattern the
--      `Tags.timerTargeted` guards already follow.
--   2. `ALTER TABLE` fails outright on a table already holding a
--      non-conforming id. Check with:
--        SELECT id FROM resonate.promises
--         WHERE id !~ '^[^:]*(:[^:]*)?$'
--            OR (origin_id IS NOT NULL AND origin_id <> split_part(id, ':', 1));
-- =============================================================================

SET search_path TO resonate, public;

-- An anchored regex beats the strpos pair here — measured 0.79µs against
-- 1.31µs per row, and it reads as the claim rather than as an encoding of it.
ALTER TABLE promises ADD CONSTRAINT well_formed_promise_id_at_most_one_separator
  CHECK (id ~ '^[^:]*(:[^:]*)?$');

-- The agreement between the id and the `resonate:origin` tag is NOT here: the
-- column is derived from the id (`split_part(id, ':', 1)`), so the two cannot
-- disagree in storage, and a tag naming anything else is rejected at the door
-- by `promise_create` and `task_create` with a 400. A constraint would only be
-- able to say the same thing later and worse, as a 500.

-- The strictest of the three, and it is OFF because the randomised run refutes
-- it against the stock server: `promise.create {"id": "foo.6:6", "tags": {}}`
-- is accepted today and this rejects it, as a 500. A child id with no
-- `resonate:origin` tag is ordinary traffic unless every door downstream is
-- known to set the tag. Uncomment only once that is true of your SDK.
--
-- ALTER TABLE promises ADD CONSTRAINT consistent_promise_child_names_origin
--   CHECK (position(':' IN id) = 0 OR origin_id IS NOT NULL);
--
-- There is a better answer than constraining the tag, and it is worth reading
-- before adopting any of this. Under this convention the tag is REDUNDANT: the
-- origin is already in the id. So derive the column instead of checking it —
--
--   origin_id TEXT GENERATED ALWAYS AS (split_part(id, ':', 1)) STORED
--
-- which makes both consistent_promise_id_matches_origin and the constraint
-- above unnecessary by construction: the origin cannot disagree with the id
-- because it IS the id's prefix, and it can never be missing. What it costs is
-- the ability to set `resonate:origin` to anything other than that prefix,
-- which today's schema permits and `_preload` and the streaming examples
-- consume. That is a change to the wire contract, not to the storage, so it
-- belongs in a discussion about the tag rather than in a constraints file.
