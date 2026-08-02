#!/usr/bin/env bash
# Name the exact call the specification could not produce.
#
#   ./refusal-point.sh traces/pg-bug_halt_on_dead.ndjson
#   ./refusal-point.sh                      # every trace in traces/
#
# validate-traces.sh answers conformant / not conformant. When a trace is
# refused this says WHERE: it replays growing prefixes and reports the first
# event the model cannot take. That event is the bug report.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB="${TLA_LIB:-$HERE/.tla-lib}"

targets=("$@")
[ ${#targets[@]} -eq 0 ] && targets=("$HERE"/traces/*.ndjson)

for t in "${targets[@]}"; do
  printf "%-30s" "$(basename "$t" .ndjson)"
  python3 - "$t" "$HERE" "$LIB" <<'PYX'
import json, subprocess, sys, os, tempfile

T, HERE, LIB = sys.argv[1], sys.argv[2], sys.argv[3]
lines = [l for l in open(T) if l.strip()]
parsed = [json.loads(l) for l in lines]
cfg_line = lines[0]
c = parsed[0]["config"]
trace = [l for l, d in zip(lines, parsed) if d.get("tag") == "trace"]
events = [json.loads(l)["event"] for l in trace]
n = len(trace)

def st(xs):
    return "{" + ",".join('"%s"' % x for x in xs) + "}"

def replays(k):
    """Does the k-event prefix replay all the way to the end?"""
    fd, path = tempfile.mkstemp(suffix=".ndjson", dir=os.path.join(HERE, "traces"))
    with os.fdopen(fd, "w") as f:
        f.write(cfg_line)
        f.writelines(trace[:k])
    cfgp = os.path.join(HERE, ".probe-%d.cfg" % k)
    with open(cfgp, "w") as f:
        f.write(f'''SPECIFICATION TraceSpec
CONSTANTS
    Ids     = {st(c["ids"])}
    Addrs   = {st(c["addrs"])}
    Workers = {{"w1"}}
    MaxTime    = 64
    MaxVersion = 16
    Retry = {c["retry"]}
    Ttl   = {c["ttl"]}
    TTLs  = {{{c["ttl"]}}}
    FaultsOn = TRUE
    TraceFile = "{path}"
CHECK_DEADLOCK FALSE
INVARIANT NotReplayed
''')
    r = subprocess.run(
        ["java", "-XX:+UseParallelGC", "-cp",
         f"{LIB}/tla2tools.jar:{LIB}/CommunityModules-deps.jar",
         "tlc2.TLC", "-config", cfgp, "-workers", "1",
         os.path.join(HERE, "UTrace.tla")],
        capture_output=True, text=True)
    os.unlink(path); os.unlink(cfgp)
    return "Invariant NotReplayed is violated" in r.stdout

ok = 0
for k in range(1, n + 1):
    if replays(k):
        ok = k
    else:
        break

if ok == n:
    print("CONFORMANT   all %d events replay" % n)
else:
    e = events[ok]
    print("REFUSED      at event %d/%d: %s (now=%s) -- %d before it replay"
          % (ok + 1, n, e.get("raw", e["name"]), e["now"], ok))
PYX
done
