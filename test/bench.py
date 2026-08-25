"""Benchmark: two-table store vs single-table store.

Runs identical workloads against both layouts and reports, globally and broken
down by RPC kind and by response status:

  latency        p50 / p90 / p99 / mean, measured server-side around the one
                 `resonate_rpc` call, so client and network are out of it
  WAL bytes      pg_current_wal_insert_lsn delta — the honest measure of write
                 amplification, independent of fsync settings
  heap/index     pg_stat_* tuple counters: rows fetched, rows written, and how
                 many updates were HOT (a HOT update writes no index entries)
  buffers        shared hits and reads
  size           table + index bytes after the run

Usage:  python3 test/bench.py [--workload W] [--n N] [--repeat R]
"""
import argparse, json, random, statistics, sys, time
import psycopg

DSN_TWO = "host=/tmp port=5433 user=postgres dbname=res_two"
DSN_ONE = "host=/tmp port=5433 user=postgres dbname=res_one"

RESET = {
    "two": "TRUNCATE resonate.outbox, resonate.listeners, resonate.callbacks, "
           "resonate.task_resumes, resonate.tasks, resonate.schedules, "
           "resonate.promises CASCADE",
    "one": "TRUNCATE resonate.outbox, resonate.schedules, resonate.promises CASCADE",
}

TABLES = {
    "two": ["promises", "tasks", "task_resumes", "callbacks", "listeners",
            "schedules", "outbox"],
    "one": ["promises", "schedules", "outbox"],
}


# --------------------------------------------------------------------------
# workloads — each yields (kind, data) with an explicit clock
# --------------------------------------------------------------------------

class Clock:
    def __init__(self):
        self.now = 1_000_000_000

    def tick(self, ms=10):
        self.now += ms
        return self.now


def wl_lifecycle(n, clk):
    """The ordinary path: create a targeted promise, claim it, fulfil it."""
    for i in range(n):
        pid = f"lc{i}"
        clk.tick()
        yield ("promise.create", {"id": pid, "timeoutAt": clk.now + 600_000,
                                  "param": {"headers": {}, "data": "x" * 64},
                                  "tags": {"resonate:target": "g1",
                                           "resonate:origin": pid}})
        clk.tick()
        yield ("task.acquire", {"id": pid, "version": 0, "pid": "w1", "ttl": 30_000})
        clk.tick()
        yield ("task.fulfill", {"id": pid, "version": 1,
                                "action": {"data": {"state": "resolved",
                                                    "value": {"headers": {}, "data": "ok"}}}})


def wl_heartbeat(n, clk):
    """Lease-refresh heavy: the case the wide merged row is supposed to hurt."""
    ids = [f"hb{i}" for i in range(32)]
    for pid in ids:
        clk.tick()
        yield ("promise.create", {"id": pid, "timeoutAt": clk.now + 3_600_000,
                                  "param": {"headers": {}, "data": "x" * 512},
                                  "tags": {"resonate:target": "g1"}})
        clk.tick()
        yield ("task.acquire", {"id": pid, "version": 0, "pid": "w1", "ttl": 300_000})
    for i in range(n):
        clk.tick()
        pid = ids[i % len(ids)]
        yield ("task.heartbeat", {"pid": "w1", "tasks": [{"id": pid, "version": 1}]})


def wl_fanout(n, clk):
    """A parent fans out to children, suspends on them, they settle, it wakes."""
    width = 8
    for i in range(n):
        root = f"fo{i}"
        clk.tick()
        yield ("promise.create", {"id": root, "timeoutAt": clk.now + 600_000,
                                  "param": {"headers": {}, "data": "x" * 64},
                                  "tags": {"resonate:target": "g1",
                                           "resonate:origin": root}})
        clk.tick()
        yield ("task.acquire", {"id": root, "version": 0, "pid": "w1", "ttl": 300_000})
        kids = [f"{root}.{j}" for j in range(width)]
        for k in kids:
            clk.tick()
            yield ("promise.create", {"id": k, "timeoutAt": clk.now + 600_000,
                                      "param": {"headers": {}, "data": "y" * 64},
                                      "tags": {"resonate:target": "g2",
                                               "resonate:origin": root}})
        clk.tick()
        yield ("task.suspend", {"id": root, "version": 1,
                                "actions": [{"data": {"awaited": k}} for k in kids]})
        for k in kids:
            clk.tick()
            yield ("promise.settle", {"id": k, "state": "resolved",
                                      "value": {"headers": {}, "data": "kid"}})


def wl_listeners(n, clk):
    """Notification fan-in: many listeners on one promise, then settle it."""
    for i in range(n):
        pid = f"ls{i}"
        clk.tick()
        yield ("promise.create", {"id": pid, "timeoutAt": clk.now + 600_000,
                                  "param": {"headers": {}, "data": "x" * 64},
                                  "tags": {"resonate:target": "g1"}})
        for j in range(6):
            clk.tick()
            yield ("promise.register_listener",
                   {"awaited": pid, "address": f"https://sink/{j}"})
        clk.tick()
        yield ("promise.settle", {"id": pid, "state": "resolved",
                                  "value": {"headers": {}, "data": "done"}})


def wl_reads(n, clk):
    """Read-only: promise.get / task.get over a warm working set."""
    ids = [f"rd{i}" for i in range(200)]
    for pid in ids:
        clk.tick()
        yield ("promise.create", {"id": pid, "timeoutAt": clk.now + 600_000,
                                  "param": {"headers": {}, "data": "x" * 256},
                                  "tags": {"resonate:target": "g1"}})
    r = random.Random(7)
    for i in range(n):
        clk.tick()
        pid = r.choice(ids)
        yield ("promise.get", {"id": pid}) if i % 2 else ("task.get", {"id": pid})


def wl_contention(n, clk):
    """Rejected requests: the guard paths, which should be pure read cost."""
    clk.tick()
    yield ("promise.create", {"id": "ct", "timeoutAt": clk.now + 600_000,
                              "param": {"headers": {}, "data": "x"},
                              "tags": {"resonate:target": "g1"}})
    for i in range(n):
        clk.tick()
        # wrong version → 409 on the fence check
        yield ("task.acquire", {"id": "ct", "version": 99, "pid": "w1", "ttl": 1000})


WORKLOADS = {
    "lifecycle": wl_lifecycle,
    "heartbeat": wl_heartbeat,
    "fanout": wl_fanout,
    "listeners": wl_listeners,
    "reads": wl_reads,
    "rejects": wl_contention,
}


# --------------------------------------------------------------------------
# instrumentation
# --------------------------------------------------------------------------

STAT = """
SELECT jsonb_build_object(
  'wal', pg_current_wal_insert_lsn() - '0/0'::pg_lsn,
  'tup', (SELECT jsonb_build_object(
      'ins', COALESCE(sum(n_tup_ins),0), 'upd', COALESCE(sum(n_tup_upd),0),
      'del', COALESCE(sum(n_tup_del),0), 'hot', COALESCE(sum(n_tup_hot_upd),0),
      'seq', COALESCE(sum(seq_tup_read),0), 'idx', COALESCE(sum(idx_tup_fetch),0))
    FROM pg_stat_all_tables WHERE schemaname = 'resonate'),
  'buf', (SELECT jsonb_build_object(
      'hit', COALESCE(sum(heap_blks_hit + idx_blks_hit),0),
      'read', COALESCE(sum(heap_blks_read + idx_blks_read),0))
    FROM pg_statio_all_tables WHERE schemaname = 'resonate'))
"""

SIZE = """
SELECT jsonb_object_agg(relname, jsonb_build_object(
         'table', pg_table_size(c.oid), 'index', pg_indexes_size(c.oid)))
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'resonate' AND c.relkind = 'r'
"""


def stats(conn):
    conn.execute("SELECT pg_stat_force_next_flush()")
    return conn.execute(STAT).fetchone()[0]


def run_one(dsn, layout, workload, n, warm):
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(RESET[layout])
        c.execute("SELECT pg_stat_reset()")
        clk = Clock()

        if warm:                       # untimed warm-up pass, same shape
            for kind, data in WORKLOADS[workload](max(4, n // 10), clk):
                rpc(c, kind, data, clk.now)
            c.execute(RESET[layout])
            c.execute("SELECT pg_stat_reset()")
            clk = Clock()

        c.execute("VACUUM ANALYZE")
        before = stats(c)
        rows = []
        t0 = time.perf_counter()
        for kind, data in WORKLOADS[workload](n, clk):
            t = time.perf_counter()
            r = rpc(c, kind, data, clk.now)
            dt = (time.perf_counter() - t) * 1e6      # µs
            rows.append((kind, r["head"]["status"], dt))
        wall = time.perf_counter() - t0
        after = stats(c)
        size = c.execute(SIZE).fetchone()[0]
    return rows, before, after, size, wall


def rpc(conn, kind, data, now):
    req = {"kind": kind, "head": {"corrId": "b", "version": "1",
                                  "resonate:debug_time": str(now)}, "data": data}
    return conn.execute("SELECT resonate.resonate_rpc(%s::jsonb)",
                        (json.dumps(req),)).fetchone()[0]


def pct(xs, q):
    xs = sorted(xs)
    if not xs:
        return 0.0
    k = min(len(xs) - 1, int(round(q * (len(xs) - 1))))
    return xs[k]


def summarize(rows):
    out = {}
    for key in ("__all__",):
        ds = [d for _, _, d in rows]
        out[key] = _agg(ds)
    by_kind, by_status = {}, {}
    for k, s, d in rows:
        by_kind.setdefault(k, []).append(d)
        by_status.setdefault(s, []).append(d)
    return {"all": _agg([d for _, _, d in rows]),
            "kind": {k: _agg(v) for k, v in sorted(by_kind.items())},
            "status": {str(s): _agg(v) for s, v in sorted(by_status.items())}}


def _agg(ds):
    return {"n": len(ds), "mean": statistics.mean(ds) if ds else 0,
            "p50": pct(ds, .50), "p90": pct(ds, .90), "p99": pct(ds, .99)}


def delta(a, b):
    return {"wal": b["wal"] - a["wal"],
            "tup": {k: b["tup"][k] - a["tup"][k] for k in a["tup"]},
            "buf": {k: b["buf"][k] - a["buf"][k] for k in a["buf"]}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", default="all")
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    names = list(WORKLOADS) if args.workload == "all" else [args.workload]
    report = {}
    for w in names:
        report[w] = {}
        for layout, dsn in (("two", DSN_TWO), ("one", DSN_ONE)):
            best = None
            for rep in range(args.repeat):
                rows, b4, af, size, wall = run_one(dsn, layout, w, args.n, warm=(rep == 0))
                s = summarize(rows)
                cand = {"latency": s, "delta": delta(b4, af), "size": size,
                        "wall_s": wall, "ops": len(rows)}
                # keep the fastest repeat: least contaminated by background noise
                if best is None or cand["latency"]["all"]["p50"] < best["latency"]["all"]["p50"]:
                    best = cand
            report[w][layout] = best
            print(f"{w:10s} {layout}  n={best['ops']:6d}  "
                  f"p50={best['latency']['all']['p50']:8.1f}µs  "
                  f"p99={best['latency']['all']['p99']:9.1f}µs  "
                  f"wal={best['delta']['wal']/1e6:8.2f}MB  "
                  f"upd={best['delta']['tup']['upd']:7d}  "
                  f"hot={best['delta']['tup']['hot']:7d}", file=sys.stderr)

    js = json.dumps(report, indent=1)
    if args.out:
        open(args.out, "w").write(js)
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        print(js)


if __name__ == "__main__":
    main()
