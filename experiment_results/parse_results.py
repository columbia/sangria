#!/usr/bin/env python3
import json
import glob
from pathlib import Path

results_dir = Path(__file__).parent / "cascading_aborts_invaluable_rook"
result_files = glob.glob(str(results_dir / "*/result.json"))

data = []
for file in sorted(result_files):
    with open(file) as f:
        result = json.load(f)
        data.append({
            "abort_rate": result["config"]["abort_rate"],
            "iteration": result["config"]["iteration"],
            "throughput": result["throughput"],
            "total_transactions": result["total_transactions"],
            "avg_latency": result["avg_latency"],
            "p50_latency": result["p50_latency"],
            "p95_latency": result["p95_latency"],
            "p99_latency": result["p99_latency"],
        })

# Sort by abort rate and iteration
data.sort(key=lambda x: (x["abort_rate"], x["iteration"]))

print("=== CASCADING ABORT EXPERIMENT RESULTS ===\n")
print(f"{'Abort Rate':<12} {'Iteration':<10} {'Throughput':<15} {'Total Txns':<12} {'Avg Latency':<15}")
print("-" * 75)
for d in data:
    print(f"{d['abort_rate']:<12.2f} {d['iteration']:<10} {d['throughput']:<15.2f} {d['total_transactions']:<12.0f} {d['avg_latency']:<15.6f}")

print("\n=== SUMMARY BY ABORT RATE ===\n")
# Group by abort rate
from collections import defaultdict
import statistics

by_abort_rate = defaultdict(list)
for d in data:
    by_abort_rate[d["abort_rate"]].append(d)

print(f"{'Abort Rate':<12} {'Mean Throughput':<20} {'Std Dev':<15} {'Mean Total Txns':<15}")
print("-" * 70)
for abort_rate in sorted(by_abort_rate.keys()):
    trials = by_abort_rate[abort_rate]
    throughputs = [t["throughput"] for t in trials]
    total_txns = [t["total_transactions"] for t in trials]
    mean_throughput = statistics.mean(throughputs)
    std_throughput = statistics.stdev(throughputs) if len(throughputs) > 1 else 0
    mean_txns = statistics.mean(total_txns)
    print(f"{abort_rate:<12.2f} {mean_throughput:<20.2f} {std_throughput:<15.2f} {mean_txns:<15.1f}")
