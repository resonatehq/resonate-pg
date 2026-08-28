"""Differential: the ported reference engine vs a resonate-pg database.

Drives `test/oracle.py` and a live database with the same randomised request
stream on the same explicit clock, and reports where they disagree — on the
response, on the canonical state, or on the outbox.

Unlike `differential.py`, which compares two implementations of the SAME
protocol version and expects zero divergence, this one compares resonate-pg
against a model taken from a LATER branch of the Rust server. Divergence is the
output, not a failure: the run classifies it so protocol drift can be told
apart from a defect.

Usage:
  python3 test/oracle_diff.py                      # resonate-pg reading
  python3 test/oracle_diff.py --strict             # oracle.rs as written
  python3 test/oracle_diff.py --db res_two         # against the two-table store
"""
import argparse, json, random, sys
from collections import Counter

import psycopg

import differential as gen_mod
import model
import oracle as oracle_mod


def db_rpc(conn, kind, data, now, corr):
    req = {"kind": kind, "head": {"corrId": corr, "version": "1",
                                  "resonate:debug_time": str(now)}, "data": data}
    return conn.execute("SELECT resonate.resonate_rpc(%s::jsonb)",
                        (json.dumps(req),)).fetchone()[0]


def canon_response(r):
    """Compare status and the parts of the body both sides agree to carry.

    The model returns prose in `data` for an error; resonate-pg returns its own
    phrase. The status is the contract; the message is not.
    """
    head = r.get("head", {})
    status = head.get("status")
    data = r.get("data")
    if status != 200 and status != 300:
        return {"status": status}
    if not isinstance(data, dict):
        return {"status": status, "data": data}
    out = {"status": status}
    if "promise" in data:
        out["promise"] = data["promise"]
    if "task" in data:
        out["task"] = data["task"]
    if "schedule" in data:
        out["schedule"] = data["schedule"]
    return out


def classify(kind, orc, db):
    """Name the divergence, so a run reports classes rather than instances."""
    so, sd = orc.get("status"), db.get("status")
    if so != sd:
        if sd == 501 and so == 200:
            return "search-not-implemented"
        if so == 400 and sd == 200:
            return "model-rejects-at-door"
        if so == 200 and sd == 400:
            return "pg-rejects-at-door"
        if {so, sd} == {404, 422} or {so, sd} == {404, 409}:
            return f"guard-order:{so}-vs-{sd}"
        return f"status:{so}-vs-{sd}"
    if orc.get("promise") != db.get("promise"):
        po, pd = orc.get("promise") or {}, db.get("promise") or {}
        diff = sorted(k for k in set(po) | set(pd) if po.get(k) != pd.get(k))
        return "promise-field:" + ",".join(diff)
    if orc.get("task") != db.get("task"):
        to, td = orc.get("task") or {}, db.get("task") or {}
        diff = sorted(k for k in set(to) | set(td) if to.get(k) != td.get(k))
        return "task-field:" + ",".join(diff)
    if orc.get("schedule") != db.get("schedule"):
        return "schedule-field"
    return "body"


def run(dsn, layout, seed, programs, steps, strict, gc_every, verbose):
    compat = oracle_mod.Compat.strict() if strict else oracle_mod.Compat()
    classes, examples, total = Counter(), {}, 0
    model_violations = Counter()
    state_classes, state_examples = Counter(), {}

    with psycopg.connect(dsn, autocommit=True) as db:
        for prog in range(programs):
            db.execute(gen_mod.RESET[layout])
            orc = oracle_mod.Oracle(compat)
            g = gen_mod.Gen(random.Random(seed * 100003 + prog), "resonate")
            for i in range(steps):
                kind, data = g.step()
                now, corr = g.now, f"{prog}:{i}"
                total += 1

                try:
                    ro = orc.apply({"kind": kind,
                                    "head": {"corrId": corr, "version": "1",
                                             "resonate:debug_time": str(now)},
                                    "data": data})
                except Exception as e:
                    ro = {"kind": kind, "head": {"corrId": corr, "status": 500,
                                                 "version": "1"},
                          "data": f"model raised: {type(e).__name__}: {e}"}
                rd = db_rpc(db, kind, data, now, corr)

                co, cd = canon_response(ro), canon_response(rd)
                if co != cd:
                    c = classify(kind, co, cd)
                    key = f"{kind} → {c}"
                    classes[key] += 1
                    examples.setdefault(key, (data, co, cd))

                if g.rnd.random() < 0.12:
                    now += g.rnd.choice([1, 6_000, 30_000])
                    g.now = now
                    orc.tick(now)
                    db.execute("SELECT resonate.process_timeouts(%s)", (now,))

                if gc_every and i % gc_every == gc_every - 1:
                    db.execute("SELECT resonate.gc(%s, 64)", (now,))

            # Does the MODEL satisfy the specification's own catalogue? A
            # difference from resonate-pg is an opinion; a catalogue violation
            # is a finding, and it is the same 45 `.state` entries both stores
            # are already held to.
            for name in model.check(g.now, orc.snapshot()):
                if name not in model.GAPS:
                    model_violations[name] += 1

            # one state comparison per program, at the end
            so = orc.snapshot()
            sd = model.normalize(model.snapshot(db, layout))
            for part in ("promises", "tasks", "outbox"):
                # `rank` is dispatch order: a sequence in the database, a dict's
                # insertion order in the model. Not comparable, and not part of
                # what an outbox row says.
                def strip(x):
                    y = {k: v for k, v in x.items() if k != "rank"}
                    return json.dumps(y, sort_keys=True)
                a = {strip(x) for x in so[part]}
                b = {strip(x) for x in sd[part]}
                if a != b:
                    state_classes[part] += 1
                    state_examples.setdefault(part, (sorted(a - b)[:2],
                                                     sorted(b - a)[:2]))
            if verbose:
                print(f"  program {prog} done", file=sys.stderr)

    return total, classes, examples, state_classes, state_examples, model_violations


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="res_one")
    ap.add_argument("--layout", default=None, help="two|one (default: from --db)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--programs", type=int, default=20)
    ap.add_argument("--steps", type=int, default=150)
    ap.add_argument("--strict", action="store_true",
                    help="run the model as oracle.rs has it, not as resonate-pg reads it")
    ap.add_argument("--gc-every", type=int, default=0)
    ap.add_argument("-v", action="store_true")
    a = ap.parse_args()
    layout = a.layout or ("two" if a.db.endswith("two") or "two" in a.db else "one")
    dsn = f"host=/tmp port=5433 user=postgres dbname={a.db}"

    total, classes, examples, sclasses, sexamples, mviol = run(
        dsn, layout, a.seed, a.programs, a.steps, a.strict, a.gc_every, a.v)

    mode = "strict (oracle.rs as written)" if a.strict else "resonate-pg reading"
    print(f"\n{'=' * 74}\nORACLE DIFFERENTIAL — {a.db} ({layout}-table), {mode}")
    print(f"{'=' * 74}")
    print(f"{total} requests, {a.programs} programs x {a.steps} steps, seed {a.seed}")
    diverged = sum(classes.values())
    print(f"response divergence: {diverged} ({diverged / total * 100:.1f}%)\n")
    if classes:
        w = max(len(k) for k in classes)
        for k, n in classes.most_common():
            print(f"  {k:<{w}}  {n:6d}")
        print("\nfirst example of each class:")
        for k, _ in classes.most_common():
            d, co, cd = examples[k]
            print(f"\n  {k}")
            print(f"    request : {json.dumps(d)[:200]}")
            print(f"    model   : {json.dumps(co)[:260]}")
            print(f"    pg      : {json.dumps(cd)[:260]}")
    else:
        print("  none")

    print(f"\ncatalogue properties the MODEL violates "
          f"(checked at the end of each of {a.programs} programs):")
    if mviol:
        for k, n in mviol.most_common():
            print(f"  {k:<52} {n}")
    else:
        print("  none")

    print(f"\nstate divergence, by part (programs out of {a.programs}):")
    if sclasses:
        for k, n in sclasses.most_common():
            print(f"  {k:<12} {n}")
            mo, md = sexamples[k]
            for x in mo:
                print(f"    model only: {x[:200]}")
            for x in md:
                print(f"    pg only   : {x[:200]}")
    else:
        print("  none")
    return 0


if __name__ == "__main__":
    sys.exit(main())
