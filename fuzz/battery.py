#!/usr/bin/env python3
"""The injected-bug battery, and the feedback comparison that runs on it.

A fuzzer's feedback is a design choice, and "does this signal help?" is a
question with a number attached. This builds a set of stores that each differ
from `resonate-single.sql` by exactly one removed guard, then measures, for each
feedback configuration, how many executions the fuzzer needs to notice.

Each bug is a DELETION, never a rewrite: the store still answers every request,
it just stops refusing something it should refuse. That is the shape a real
regression takes when someone "simplifies" a guard away.

  python3 fuzz/battery.py --build     build the stores
  python3 fuzz/battery.py --run       run the comparison
"""
import argparse, json, re, subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PG = ["-h", "/tmp", "-p", "5433", "-U", "postgres"]

# name -> the exact text removed from resonate-single.sql, and what it guarded.
BUGS = {
    "timer_targeted": (
        """  IF COALESCE(tags->>'resonate:timer','') = 'true' AND tags ? 'resonate:target' THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
""",
        "promise.create stops refusing timer+target (well_formed_promise_timer_not_targeted)",
    ),
    "listener_external": (
        """  -- An INTERNAL awaited must not carry obligations: its timeout is not
  -- enforced, so a listener on it could wait forever. Same rule, and same
  -- 422, as promise.register_callback (spec: promiseRegisterListener).
  IF NOT pa.external THEN RETURN jsonb_build_object('status', 422); END IF;
""",
        "register_listener stops refusing an internal awaited "
        "(well_formed_promise_obligations_require_external)",
    ),
    "callback_self": (
        """  IF p_awaited = p_awaiter THEN RETURN jsonb_build_object('status', 400); END IF;
""",
        "register_callback stops refusing a self-await "
        "(well_formed_promise_awaiter_is_not_self)",
    ),
    "suspend_empty": (
        """  IF jsonb_array_length(COALESCE(p_actions, '[]'::jsonb)) = 0 THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
""",
        "task.suspend stops refusing an empty action list (spec T-06 guard 1)",
    ),
    "suspend_dup": (
        """  IF (SELECT count(act->>'awaited') <> count(DISTINCT act->>'awaited')
      FROM jsonb_array_elements(p_actions) act) THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
""",
        "task.suspend stops refusing duplicate awaited ids (spec T-06 guard 3)",
    ),
    "acquire_version": (
        """  IF t.task_version IS DISTINCT FROM p_version THEN RETURN jsonb_build_object('status', 409); END IF;

  UPDATE promises SET task_state = 'acquired', task_version = task_version + 1,""",
        "task.acquire stops fencing on version — the lease is no longer safe",
    ),
    "suspend_settled": (
        """  IF settled THEN
    UPDATE promises SET resumes = '{}' WHERE id = t.id;
    RETURN jsonb_build_object('status', 300);
  END IF;
""",
        "task.suspend parks on an already-settled awaited instead of returning 300 "
        "— the task can never be woken",
    ),
    "origin_door": (
        """  IF tags ? 'resonate:origin'
     AND tags->>'resonate:origin' IS DISTINCT FROM split_part(p_id, ':', 1) THEN
    RETURN jsonb_build_object('status', 400);
  END IF;
""",
        "promise.create stops refusing an origin tag that disagrees with the id",
    ),
}

REPLACEMENTS = {
    # acquire_version deletes a guard but must keep the statement after it
    "acquire_version": """  UPDATE promises SET task_state = 'acquired', task_version = task_version + 1,""",
}

FEEDBACKS = ["points", "edges", "preconds", "shapes"]


def psql(db, *args):
    return subprocess.run(["psql", *PG, "-d", db, *args],
                          capture_output=True, text=True)


def build():
    base = (ROOT / "resonate-single.sql").read_text()
    constraints = ROOT / "constraints-all.sql"

    # the control store: the two-table baseline
    subprocess.run(["psql", *PG, "-c", "DROP DATABASE IF EXISTS res_fuzz_two",
                    "-c", "CREATE DATABASE res_fuzz_two"], capture_output=True)
    r = psql("res_fuzz_two", "-v", "ON_ERROR_STOP=1", "-q", "-f", str(ROOT / "resonate.sql"))
    assert "ERROR" not in r.stderr, r.stderr[:400]
    print("built res_fuzz_two (two-table control)")

    # the correct merged store, and one store per injected bug
    variants = {"none": base}
    for name, (text, _) in BUGS.items():
        assert base.count(text) == 1, f"{name}: anchor matched {base.count(text)} times"
        variants[name] = base.replace(text, REPLACEMENTS.get(name, ""))

    for name, sql in variants.items():
        db = "res_fuzz_one" if name == "none" else f"res_fuzz_{name}"
        path = Path("/tmp") / f"{db}.sql"
        path.write_text(sql)
        subprocess.run(["psql", *PG, "-c", f"DROP DATABASE IF EXISTS {db}",
                        "-c", f"CREATE DATABASE {db}"], capture_output=True)
        r = psql(db, "-v", "ON_ERROR_STOP=1", "-q", "-f", str(path),
                 "-f", str(constraints))
        if "ERROR" in r.stderr:
            print(f"  !! {db}: {r.stderr.strip()[:200]}")
        else:
            print(f"built {db}")


def run(max_execs, repeats):
    binary = ROOT / "fuzz" / "target" / "release" / "resonate-pg-fuzz"
    assert binary.exists(), "cargo build --release first"
    results = {}
    for bug in ["none"] + list(BUGS):
        db = "res_fuzz_one" if bug == "none" else f"res_fuzz_{bug}"
        for fb in FEEDBACKS:
            runs = []
            for rep in range(repeats):
                env = {"FUZZ_ONE": f"host=/tmp port=5433 user=postgres dbname={db}",
                       "FUZZ_FEEDBACK": fb, "FUZZ_MAX_EXECS": str(max_execs),
                       "FUZZ_QUIET": "1", "PATH": "/usr/bin:/bin"}
                t0 = time.time()
                p = subprocess.run([str(binary)], capture_output=True, text=True,
                                   env=env, cwd=str(ROOT / "fuzz"), timeout=1800)
                m = re.search(r"RESULT found=(\d) execs=(\d+)", p.stdout)
                if not m:
                    runs.append(None)
                    continue
                runs.append(int(m.group(2)) if m.group(1) == "1" else None)
                del t0
            results[(bug, fb)] = runs
            got = [r for r in runs if r is not None]
            shown = f"{sum(got)//len(got)}" if got else f">{max_execs}"
            print(f"  {bug:18s} {fb:9s} execs-to-detect: {shown}"
                  f"  ({len(got)}/{repeats} detected)", flush=True)
    return results


def report(results, max_execs):
    print(f"\n{'=' * 78}\nEXECUTIONS TO FIRST DETECTION (mean of detecting runs; "
          f"cap {max_execs})\n{'=' * 78}")
    w = max(len(b) for b in ["none"] + list(BUGS)) + 2
    print(f"{'injected bug':<{w}}" + "".join(f"{f:>12}" for f in FEEDBACKS))
    print("-" * (w + 12 * len(FEEDBACKS)))
    for bug in list(BUGS) + ["none"]:
        row = f"{bug:<{w}}"
        for fb in FEEDBACKS:
            got = [r for r in results.get((bug, fb), []) if r is not None]
            row += f"{(str(sum(got) // len(got)) if got else '—'):>12}"
        print(row)
    print(f"\n'—' means no detection within {max_execs} executions.")
    print("The `none` row is the control: it MUST be '—' in every column, or the")
    print("fuzzer is reporting divergence between two stores that agree.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--max-execs", type=int, default=600)
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--out", default="/tmp/battery.json")
    a = ap.parse_args()
    if a.build:
        build()
    if a.run:
        res = run(a.max_execs, a.repeats)
        json.dump({f"{b}|{f}": v for (b, f), v in res.items()}, open(a.out, "w"))
        report(res, a.max_execs)
    if not a.build and not a.run:
        ap.print_help()
