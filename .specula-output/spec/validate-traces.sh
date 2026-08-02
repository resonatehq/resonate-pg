#!/usr/bin/env bash
# =============================================================================
# Specula Phase 3A · trace validation
# =============================================================================
# Drives TLC through every recorded trace, checking that base.tla reproduces
# each observed transition and that the spec's state matches the state the
# database actually held after every action.
#
#   TLA_LIB=/path/to/jars .specula-output/spec/validate-traces.sh
#
# Needs tla2tools.jar and CommunityModules-deps.jar (for the Json module).
# =============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRACES="$HERE/../traces"
OUT="$HERE/output"
LIB="${TLA_LIB:-$HERE/../../.tla-lib}"

mkdir -p "$OUT"

fail=0
for t in "$TRACES"/*.ndjson; do
  name="$(basename "$t" .ndjson)"

  cat > "$HERE/Trace.cfg" <<CFG
SPECIFICATION TraceSpec

CONSTANTS
    Ids        = {"a", "b"}
    Addrs      = {"poll://any@L"}
    MaxTime    = 12
    MaxVersion = 8
    Retry      = 1
    Ttl        = 1
    ListenerExternalGuard = FALSE
    PromiseLivenessGuard  = FALSE
    TimeoutLivenessGuard  = FALSE
    SequencedDriver       = FALSE
    TraceFile  = "$t"

CHECK_DEADLOCK FALSE

PROPERTIES TraceMatched

INVARIANT
    TypeOK
    Stickiness
    TaskPromiseCoherence
    CallbacksAreExternal
CFG

  java -XX:+UseParallelGC \
       -cp "$LIB/tla2tools.jar:$LIB/CommunityModules-deps.jar" \
       tlc2.TLC -config Trace.cfg -workers 1 "$HERE/Trace.tla" \
       > "$OUT/trace-$name.out" 2>&1

  events=$(grep -c '"tag": *"trace"' "$t" 2>/dev/null || \
           python3 -c "import json,sys;print(sum(1 for l in open('$t') if json.loads(l)['tag']=='trace'))")

  if grep -q "Model checking completed. No error has been found" "$OUT/trace-$name.out"; then
    depth=$(grep -oE "depth of the complete state graph search is [0-9]+" "$OUT/trace-$name.out" | grep -oE "[0-9]+$")
    printf "  %-22s VALID    %2s events, depth %s\n" "$name" "$events" "$depth"
  else
    printf "  %-22s FAILED   %2s events -- see output/trace-%s.out\n" "$name" "$events" "$name"
    grep -E "^Error:" "$OUT/trace-$name.out" | head -2 | sed 's/^/      /'
    fail=1
  fi
done

exit $fail
