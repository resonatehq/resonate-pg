#!/usr/bin/env bash
# =============================================================================
# Specula trace harness for resonate-pg  (Phase 2.5)
# =============================================================================
# One command: instrument resonate.sql, load it into a database, run every
# scenario, export one NDJSON trace per scenario.
#
#   PGURL="postgresql://user@host/db" .specula-output/harness/run.sh
#
# Defaults to the local socket used by the Specula run. Requires psql and
# python3; pg_cron is not needed -- the scenarios call process_timeouts()
# directly, the same way test/conformance.py does.
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="$HERE/../traces"
BUILD="$HERE/build"

PSQL_ARGS=${PGURL:+-d "$PGURL"}
PSQL_ARGS=${PSQL_ARGS:--h /tmp -p 5433 -U postgres}
# shellcheck disable=SC2086
psql() { command psql $PSQL_ARGS -v ON_ERROR_STOP=1 "$@"; }

SCENARIOS=(suspend_resume timeouts claim_halt_continue listener_callback external_settle
           bug_stranded_listener bug_dead_claim bug_dead_redispatch
           bug_halt_on_dead bug_resume_dead_awaiter)

mkdir -p "$OUT" "$BUILD"

echo "==> instrumenting resonate.sql"
python3 "$HERE/instrument.py" "$ROOT/resonate.sql" "$BUILD/resonate-instrumented.sql"
diff -u "$ROOT/resonate.sql" "$BUILD/resonate-instrumented.sql" \
     > "$HERE/patches/instrumentation.patch" || true

echo "==> loading instrumented schema"
psql -q -c "DROP SCHEMA IF EXISTS resonate CASCADE" >/dev/null
psql -q -f "$BUILD/resonate-instrumented.sql" 2>&1 | grep -v "pg_cron" || true
psql -q -f "$HERE/src/trace.sql"

for s in "${SCENARIOS[@]}"; do
  echo "==> scenario $s"
  psql -q -v scenario="$s" -f "$HERE/src/scenarios.sql" >/dev/null

  # NDJSON: a config line, then one line per emitted event.
  {
    psql -tAq -c "SELECT jsonb_build_object(
        'tag','config',
        'config', jsonb_build_object(
          'scenario', '$s', 'ids', jsonb_build_array('a','b'),
          'addrs', jsonb_build_array('L'), 'retry', 1, 'ttl', 1))::text"
    psql -tAq -c "SELECT jsonb_build_object(
        'tag','trace',
        'ts', seq,
        'event', jsonb_build_object('name', event, 'now', now, 'state', state))::text
      FROM resonate.trace ORDER BY seq"
  } | grep -v '^$' > "$OUT/$s.ndjson"

  echo "    $(wc -l < "$OUT/$s.ndjson") lines -> traces/$s.ndjson"
done

echo "==> done"
