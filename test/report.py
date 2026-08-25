"""Render bench.py's JSON as comparison tables.

Global, then by RPC kind, then by response status — the three cuts that say
where a layout wins and where it loses, rather than one aggregate that hides
both.

Usage:  python3 test/bench.py --out b.json && python3 test/report.py b.json
"""
import json, sys

BASE = "two"


def rel(new, old):
    if not old:
        return "  —   "
    d = (new - old) / old * 100
    return f"{d:+6.1f}%"


def table(title, rows, cols):
    print(f"\n### {title}")
    w = max([len(r[0]) for r in rows] + [len(title)])
    print(f"{'':<{w}}  " + "  ".join(f"{c:>12}" for c in cols))
    for r in rows:
        print(f"{r[0]:<{w}}  " + "  ".join(f"{v:>12}" for v in r[1:]))


def main(path):
    rep = json.load(open(path))
    variants = list(next(iter(rep.values())).keys())

    print("=" * 78)
    print("GLOBAL — per-request latency (µs) and write volume, by workload")
    print("=" * 78)
    for wl, per in rep.items():
        rows = []
        for v in variants:
            d = per[v]
            lat, dl = d["latency"]["all"], d["delta"]
            base = per[BASE]
            med = d.get("spread", {}).get("p50_med", lat["p50"])
            bmed = base.get("spread", {}).get("p50_med", base["latency"]["all"]["p50"])
            sp = d.get("spread")
            noise = (f"±{(sp['p50_max']-sp['p50_min'])/sp['p50_med']*100:.0f}%"
                     if sp and sp["p50_med"] else "")
            rows.append([
                v,
                f"{med:.0f}",
                noise,
                f"{lat['p99']:.0f}",
                rel(med, bmed),
                f"{dl['wal']/1e6:.2f}",
                rel(dl["wal"], base["delta"]["wal"]),
                f"{dl['tup']['ins']+dl['tup']['upd']+dl['tup']['del']}",
                f"{dl['buf']['hit']}",
            ])
        table(f"{wl}  (n={per[variants[0]]['ops']} requests)", rows,
              ["p50", "noise", "p99", "Δp50", "WAL MB", "ΔWAL",
               "rows written", "buf hits"])

    print()
    print("=" * 78)
    print("BY REQUEST TYPE — p50 µs")
    print("=" * 78)
    kinds = sorted({k for per in rep.values() for v in per.values()
                    for k in v["latency"]["kind"]})
    for wl, per in rep.items():
        present = [k for k in kinds if k in per[variants[0]]["latency"]["kind"]]
        if not present:
            continue
        rows = []
        for k in present:
            base = per[BASE]["latency"]["kind"][k]
            row = [k, str(base["n"])]
            for v in variants:
                d = per[v]["latency"]["kind"].get(k)
                row.append(f"{d['p50']:.0f}" if d else "-")
            row.append(rel(per[variants[-1]]["latency"]["kind"][k]["p50"], base["p50"]))
            rows.append(row)
        table(wl, rows, ["n"] + variants + [f"Δ{variants[-1]}"])

    print()
    print("=" * 78)
    print("BY RESPONSE STATUS — p50 µs")
    print("=" * 78)
    for wl, per in rep.items():
        sts = sorted(per[variants[0]]["latency"]["status"])
        rows = []
        for s in sts:
            base = per[BASE]["latency"]["status"][s]
            row = [s, str(base["n"])]
            for v in variants:
                d = per[v]["latency"]["status"].get(s)
                row.append(f"{d['p50']:.0f}" if d else "-")
            row.append(rel(per[variants[-1]]["latency"]["status"][s]["p50"], base["p50"]))
            rows.append(row)
        table(wl, rows, ["n"] + variants + [f"Δ{variants[-1]}"])

    print()
    print("=" * 78)
    print("STORAGE — bytes after the run")
    print("=" * 78)
    for wl, per in rep.items():
        rows = []
        for v in variants:
            sz = per[v]["size"] or {}
            t = sum(x["table"] for x in sz.values())
            i = sum(x["index"] for x in sz.values())
            rows.append([v, f"{t/1024:.0f}K", f"{i/1024:.0f}K", f"{(t+i)/1024:.0f}K",
                         str(len(sz))])
        table(wl, rows, ["table", "index", "total", "relations"])


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "bench.json")
