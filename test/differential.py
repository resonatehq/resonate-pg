"""Differential test: two-table store vs single-table store.

Drives both layouts with the SAME randomised request stream on the SAME
explicit clock, and after every request asserts

  1. the RPC responses are byte-identical,
  2. the canonical `ServerState` projections are equal,
  3. neither store violates any of the `.state` properties of the
     specification's conformance catalogue.

Usage:  python3 test/differential.py [--seed N] [--programs N] [--steps N]
"""
import argparse, json, random, sys
import psycopg
import model

TARGETS = ["g1", "g2"]
ADDRS = ["http://a/1", "https://b/2", "poll://c@d", "not-a-url"]
SETTLE_STATES = ["resolved", "rejected", "rejected_canceled",
                 "rejected_timedout", "pending", None]


class Gen:
    """Random request stream over a small, deliberately colliding id space."""

    def __init__(self, rnd):
        self.rnd = rnd
        self.now = 1_000_000
        self.ids = []
        self.sched = []
        self.n = 0

    def _id(self):
        r = self.rnd
        if self.ids and r.random() < 0.75:
            return r.choice(self.ids)
        self.n += 1
        # a dotted id space so `_preload`-style ancestry and origin tags mean
        # something, and so ids collide across requests
        pid = f"p{self.n % 17}" + ("." + str(self.n % 5) if r.random() < 0.4 else "")
        self.ids.append(pid)
        return pid

    def _tags(self):
        r = self.rnd
        t = {}
        if r.random() < 0.55:
            t["resonate:target"] = r.choice(TARGETS + [""])
        if r.random() < 0.25:
            # deliberately allowed to coincide with a target: Tags.timerTargeted
            # is a shape the doors must refuse, so the stream has to produce it
            t["resonate:timer"] = "true"
        if r.random() < 0.2:
            t["resonate:external"] = "true"
        if r.random() < 0.3:
            t["resonate:origin"] = r.choice(self.ids) if self.ids else "p0"
        if r.random() < 0.15:
            t["resonate:delay"] = str(self.now + r.choice([-5000, 5000, 10 ** 15]))
        return t

    def _to(self):
        r = self.rnd
        return self.now + r.choice([-10_000, -1, 0, 1, 5_000, 60_000, 10 ** 9])

    def step(self):
        r = self.rnd
        self.now += r.choice([0, 1, 10, 250, 3_000, 7_000])
        k = r.random()

        if k < 0.16:
            return ("promise.create",
                    {"id": self._id(), "timeoutAt": self._to(),
                     "param": {"headers": {}, "data": r.choice([None, "x"])},
                     "tags": self._tags()})
        if k < 0.24:
            return ("promise.settle",
                    {"id": self._id(), "state": r.choice(SETTLE_STATES),
                     "value": {"headers": {}, "data": "v"}})
        if k < 0.28:
            return ("promise.register_callback",
                    {"awaited": self._id(), "awaiter": self._id()})
        if k < 0.32:
            return ("promise.register_listener",
                    {"awaited": self._id(), "address": r.choice(ADDRS)})
        if k < 0.36:
            return ("promise.get", {"id": self._id()})
        if k < 0.44:
            return ("task.create",
                    {"pid": f"w{r.randint(1, 3)}", "ttl": r.choice([1, 5_000, 0]),
                     "action": {"data": {"id": self._id(), "timeoutAt": self._to(),
                                         "param": {"headers": {}, "data": None},
                                         "tags": self._tags()}}})
        if k < 0.54:
            return ("task.acquire",
                    {"id": self._id(), "version": r.randint(0, 3),
                     "pid": f"w{r.randint(1, 3)}", "ttl": r.choice([1, 5_000])})
        if k < 0.60:
            return ("task.get", {"id": self._id()})
        if k < 0.68:
            n = r.randint(0, 3)
            return ("task.suspend",
                    {"id": self._id(), "version": r.randint(0, 3),
                     "actions": [{"data": {"awaited": self._id()}} for _ in range(n)]})
        if k < 0.76:
            return ("task.fulfill",
                    {"id": self._id(), "version": r.randint(0, 3),
                     "action": {"data": {"state": r.choice(SETTLE_STATES),
                                         "value": {"headers": {}, "data": "r"}}}})
        if k < 0.80:
            return ("task.release", {"id": self._id(), "version": r.randint(0, 3)})
        if k < 0.83:
            return ("task.heartbeat",
                    {"pid": f"w{r.randint(1, 3)}",
                     "tasks": [{"id": self._id(), "version": r.randint(0, 3)}]})
        if k < 0.86:
            return ("task.halt", {"id": self._id()})
        if k < 0.89:
            return ("task.continue", {"id": self._id()})
        if k < 0.92:
            inner = r.choice(["promise.create", "promise.settle"])
            data = ({"id": self._id(), "timeoutAt": self._to(),
                     "param": {"headers": {}, "data": None}, "tags": self._tags()}
                    if inner == "promise.create" else
                    {"id": self._id(), "state": r.choice(SETTLE_STATES),
                     "value": {"headers": {}, "data": "f"}})
            return ("task.fence",
                    {"id": self._id(), "version": r.randint(0, 3),
                     "action": {"kind": inner, "data": data}})
        if k < 0.95:
            sid = f"s{r.randint(1, 3)}"
            if sid not in self.sched:
                self.sched.append(sid)
            return ("schedule.create",
                    {"id": sid, "cron": r.choice(["* * * * *", "0 * * * *", "bogus"]),
                     "promiseId": "sp.{{.id}}.{{.timestamp}}",
                     "promiseTimeout": 60_000,
                     "promiseParam": {"headers": {}, "data": None},
                     "promiseTags": self._tags()})
        if k < 0.97:
            return ("schedule.delete", {"id": f"s{r.randint(1, 3)}"})
        return ("schedule.get", {"id": f"s{r.randint(1, 3)}"})


def rpc(conn, kind, data, now, corr):
    req = {"kind": kind, "head": {"corrId": corr, "version": "1",
                                  "resonate:debug_time": str(now)},
           "data": data}
    return conn.execute("SELECT resonate.resonate_rpc(%s::jsonb)",
                        (json.dumps(req),)).fetchone()[0]


def tick(conn, now):
    conn.execute("SELECT resonate.process_timeouts(%s)", (now,))


RESET = {
    "two": "TRUNCATE resonate.outbox, resonate.listeners, resonate.callbacks, "
           "resonate.task_resumes, resonate.tasks, resonate.schedules, "
           "resonate.promises CASCADE",
    "one": "TRUNCATE resonate.outbox, resonate.schedules, resonate.promises CASCADE",
}


def run(dsn_two, dsn_one, seed, programs, steps, verbose=False):
    fails, gaps_hit = [], set()
    with psycopg.connect(dsn_two, autocommit=True) as a, \
         psycopg.connect(dsn_one, autocommit=True) as b:
        for prog in range(programs):
            a.execute(RESET["two"])
            b.execute(RESET["one"])
            gen = Gen(random.Random(seed * 100003 + prog))
            for i in range(steps):
                kind, data = gen.step()
                now = gen.now
                corr = f"{prog}:{i}"

                ra = rpc(a, kind, data, now, corr)
                rb = rpc(b, kind, data, now, corr)
                if ra != rb:
                    fails.append(("response", prog, i, kind, data, ra, rb))
                    break

                if gen.rnd.random() < 0.12:
                    now += gen.rnd.choice([1, 6_000, 30_000])
                    gen.now = now
                    tick(a, now)
                    tick(b, now)

                sa = model.normalize(model.snapshot(a, "two"))
                sb = model.normalize(model.snapshot(b, "one"))
                if sa != sb:
                    fails.append(("state", prog, i, kind, data,
                                  _diff(sa, sb), None))
                    break

                va = model.check(now, sa)
                vb = model.check(now, sb)
                if set(va) != set(vb):
                    fails.append(("property-divergence", prog, i, kind, data, va, vb))
                    break
                cat = [n for n in va if n not in model.GAPS]
                if cat:
                    fails.append(("property", prog, i, kind, data, cat, cat))
                    break
                gaps_hit.update(n for n in va if n in model.GAPS)
            if verbose:
                print(f"  program {prog}: {steps} steps ok", file=sys.stderr)
    return fails, gaps_hit


def _diff(a, b):
    out = {}
    for k in a:
        if a[k] != b[k]:
            xs = {json.dumps(x, sort_keys=True) for x in a[k]}
            ys = {json.dumps(x, sort_keys=True) for x in b[k]}
            out[k] = {"two_only": sorted(xs - ys)[:3], "one_only": sorted(ys - xs)[:3]}
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--programs", type=int, default=40)
    ap.add_argument("--steps", type=int, default=120)
    ap.add_argument("--two", default="host=/tmp port=5433 user=postgres dbname=res_two")
    ap.add_argument("--one", default="host=/tmp port=5433 user=postgres dbname=res_one")
    ap.add_argument("-v", action="store_true")
    args = ap.parse_args()

    f, gaps = run(args.two, args.one, args.seed, args.programs, args.steps, args.v)
    if gaps:
        print(f"note: both stores reached {len(gaps)} of the spec's "
              f"{model.GAP_COUNT} known gaps identically: {', '.join(sorted(gaps))}")
    if not f:
        print(f"ok — {args.programs} programs x {args.steps} steps, "
              f"responses + state + {model.COUNT} catalogue properties agree")
        sys.exit(0)
    for kind, prog, i, k, d, x, y in f[:5]:
        print(f"\nFAIL[{kind}] program {prog} step {i}: {k} {json.dumps(d)}")
        print("  two:", json.dumps(x)[:1400])
        if y is not None:
            print("  one:", json.dumps(y)[:1400])
    print(f"\n{len(f)} failing program(s)")
    sys.exit(1)
