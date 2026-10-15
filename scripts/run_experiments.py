"""Raft experiments: leader-election time vs timeout randomisation, commit latency vs message loss,
randomised chaos testing, and bug injection to show the checks catch real violations."""
import json, sys, time
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from raft import Cluster, check_safety  # noqa
from raft.chaos import chaos_run  # noqa
from raft.node import RaftNode  # noqa

OUT, FIG = ROOT / "results", ROOT / "results" / "figures"
FIG.mkdir(parents=True, exist_ok=True)
R = {}

# 1) Time to elect a new leader after the leader crashes (cf. paper Figure 16)
el = []
dist = {}
for lo, hi in ((150, 151), (150, 155), (150, 175), (150, 200), (150, 300)):
    times, no_first, no_second = [], 0, 0
    for seed in range(300):
        c = Cluster(5, seed=seed, election_timeout=(lo, hi))
        if c.first_leader_time(t_max=20000) is None:
            no_first += 1
            continue
        c.run_until(c.now + 400)
        crash_at = c.now
        c.crash(c.leader())
        t = c.first_leader_time(t_max=crash_at + 20000)
        if t is not None:
            times.append(t - crash_at)
        else:
            no_second += 1
    a = np.array(times)
    dist[f"{lo}-{hi}"] = a
    el.append({"election_timeout_ms": f"{lo}-{hi}", "no_initial_leader_in_20s": f"{no_first}/300",
               "no_leader_20s_after_crash": no_second, "measured": len(a), "median_ms": round(float(np.median(a)), 1),
               "p95_ms": round(float(np.percentile(a, 95)), 1), "max_ms": round(float(a.max()), 1)})
R["election"] = el

# 2) Write latency and availability vs message loss (steady cluster, 500 writes over 10 s)
lat = []
for drop in (0.0, 0.05, 0.10, 0.20, 0.30):
    c = Cluster(5, seed=11, drop=drop)
    c.first_leader_time()
    start = c.now
    for k in range(500):
        c.submit(f"d{k}", ("set", f"k{k % 50}", k), at=start + k * 20)
    c.run_until(start + 10000 + 8000)
    L = np.array(c.latency)
    lat.append({"drop_rate": drop, "acked_%": round(100 * len(c.acked) / 500, 1), "p50_latency_ms": round(float(np.median(L)), 1),
                "p99_latency_ms": round(float(np.percentile(L, 99)), 1), "messages_per_write": round(c.msgs_sent / 500, 1),
                "safety_errors": len(check_safety(c))})
R["loss"] = lat

# 3) Chaos testing of the correct implementation
t0 = time.time()
n_runs, viol, acked = 300, 0, 0
for seed in range(n_runs):
    c, errs = chaos_run(1000 + seed)
    viol += bool(errs); acked += len(c.acked)
R["chaos"] = {"runs": n_runs, "runs_with_violations": viol, "acknowledged_writes_total": acked, "seconds": round(time.time() - t0, 1)}

# 4) Bug injection: the same chaos harness must catch broken variants
bugs = []
for name, attr in (("vote ignores log up-to-dateness", "BUG_IGNORE_LOG_IN_VOTE"), ("commit by counting old-term entries", "BUG_COMMIT_OLD_TERM")):
    setattr(RaftNode, attr, True)
    caught, first, kinds = 0, None, set()
    for seed in range(n_runs):
        c, errs = chaos_run(1000 + seed)
        if errs:
            caught += 1; first = seed if first is None else first
            for e in errs:
                kinds.add("election safety" if e.startswith("two leaders") else "log matching" if e.startswith("log matching")
                          else "state-machine safety" if "applied differently" in e else "lost acknowledged write")
    setattr(RaftNode, attr, False)
    bugs.append({"injected_bug": name, "runs_with_violations": f"{caught}/{n_runs}", "first_failing_run": first, "violation_types": sorted(kinds)})
R["bug_injection"] = bugs

fig, ax = plt.subplots(figsize=(6.5, 4))
for k, a in dist.items():
    s = np.sort(a); ax.plot(s, np.arange(1, len(s) + 1) / len(s), label=f"timeout {k} ms")
ax.set_xscale("log"); ax.set_xlabel("time without a leader after crash (ms)"); ax.set_ylabel("cumulative fraction"); ax.legend(fontsize=8)
ax.set_title("Randomised election timeouts prevent split votes"); fig.tight_layout(); fig.savefig(FIG / "election_cdf.png", dpi=130); plt.close(fig)

(OUT / "results.json").write_text(json.dumps(R, indent=2))
def tbl(rows):
    cols = list(rows[0]); return "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n" + "".join("| " + " | ".join(str(r[c]) for c in cols) + " |\n" for r in rows)
md = "# Results\n\n## 1. Leader election after a leader crash (5 nodes, 5-20 ms message delay)\n\n" + tbl(el)
md += "\n## 2. Writes under message loss (500 writes, one every 20 ms)\n\n" + tbl(lat)
md += f"\n## 3. Chaos testing\n\n{R['chaos']['runs']} randomised runs (crashes, restarts, partitions, 5% loss, 300 writes each): " \
      f"**{R['chaos']['runs_with_violations']} runs with safety violations**, {R['chaos']['acknowledged_writes_total']} acknowledged writes checked ({R['chaos']['seconds']} s).\n"
md += "\n## 4. Bug injection (same 300 chaos runs)\n\n" + tbl(bugs)
(OUT / "summary.md").write_text(md)
print(md)
