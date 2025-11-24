#!/usr/bin/env python3
"""
Plot cascading abort experiment results showing throughput degradation
"""
import json
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from collections import defaultdict
import statistics

# Set style for better-looking plots
plt.style.use('seaborn-v0_8-darkgrid')

# Load results
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

# Create DataFrame
df = pd.DataFrame(data)
df = df.sort_values(by=["abort_rate", "iteration"])

# Save raw data to CSV
output_dir = Path(__file__).parent
csv_filename = output_dir / "cascading_aborts_results.csv"
df.to_csv(csv_filename, index=False)
print(f"Saved raw data to: {csv_filename}")

# Calculate statistics by abort rate
stats_by_abort_rate = defaultdict(lambda: {
    "throughputs": [],
    "total_txns": [],
})

for _, row in df.iterrows():
    abort_rate = row["abort_rate"]
    stats_by_abort_rate[abort_rate]["throughputs"].append(row["throughput"])
    stats_by_abort_rate[abort_rate]["total_txns"].append(row["total_transactions"])

# Prepare data for plotting
abort_rates = sorted(stats_by_abort_rate.keys())
mean_throughputs = []
std_throughputs = []
mean_total_txns = []

for abort_rate in abort_rates:
    throughputs = stats_by_abort_rate[abort_rate]["throughputs"]
    total_txns = stats_by_abort_rate[abort_rate]["total_txns"]

    mean_throughputs.append(statistics.mean(throughputs))
    std_throughputs.append(statistics.stdev(throughputs) if len(throughputs) > 1 else 0)
    mean_total_txns.append(statistics.mean(total_txns))

# Create summary DataFrame
summary_df = pd.DataFrame({
    "abort_rate": abort_rates,
    "mean_throughput": mean_throughputs,
    "std_throughput": std_throughputs,
    "mean_total_transactions": mean_total_txns
})

summary_csv_filename = output_dir / "cascading_aborts_summary.csv"
summary_df.to_csv(summary_csv_filename, index=False)
print(f"Saved summary data to: {summary_csv_filename}")

# Convert abort rates to percentages for display
abort_rates_pct = [rate * 100 for rate in abort_rates]

# Create figure with multiple subplots
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Cascading Abort Experiment Results\n(Pipelined 2PC, 2500 queries, 50 keys, zipf=0.9)',
             fontsize=14, fontweight='bold')

# Plot 1: Throughput vs Abort Rate with error bars
ax1 = axes[0, 0]
ax1.errorbar(abort_rates_pct, mean_throughputs, yerr=std_throughputs,
             marker='o', linewidth=2, markersize=8, capsize=5,
             color='#2E86AB', ecolor='#A23B72', capthick=2)
ax1.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax1.set_ylabel('Throughput (txn/s)', fontsize=11, fontweight='bold')
ax1.set_title('Committed Transaction Throughput', fontsize=12, fontweight='bold')
ax1.grid(True, alpha=0.3)
ax1.set_xticks(abort_rates_pct)

# Add value labels on points
for i, (x, y) in enumerate(zip(abort_rates_pct, mean_throughputs)):
    ax1.annotate(f'{y:.1f}', (x, y), textcoords="offset points",
                xytext=(0,10), ha='center', fontsize=9)

# Plot 2: Total Committed Transactions vs Abort Rate
ax2 = axes[0, 1]
ax2.plot(abort_rates_pct, mean_total_txns, marker='s', linewidth=2,
         markersize=8, color='#F18F01')
ax2.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax2.set_ylabel('Total Committed Transactions', fontsize=11, fontweight='bold')
ax2.set_title('Total Committed Transactions', fontsize=12, fontweight='bold')
ax2.grid(True, alpha=0.3)
ax2.set_xticks(abort_rates_pct)

# Add value labels
for x, y in zip(abort_rates_pct, mean_total_txns):
    ax2.annotate(f'{y:.1f}', (x, y), textcoords="offset points",
                xytext=(0,10), ha='center', fontsize=9)

# Plot 3: Individual trial points + mean line
ax3 = axes[1, 0]
for abort_rate in abort_rates:
    rate_pct = abort_rate * 100
    throughputs = stats_by_abort_rate[abort_rate]["throughputs"]
    # Plot individual points
    x_points = [rate_pct] * len(throughputs)
    ax3.scatter(x_points, throughputs, alpha=0.6, s=100, color='#C73E1D')

# Plot mean line
ax3.plot(abort_rates_pct, mean_throughputs, marker='o', linewidth=2,
         markersize=10, color='#2E86AB', label='Mean', zorder=5)
ax3.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax3.set_ylabel('Throughput (txn/s)', fontsize=11, fontweight='bold')
ax3.set_title('Individual Trials + Mean', fontsize=12, fontweight='bold')
ax3.grid(True, alpha=0.3)
ax3.set_xticks(abort_rates_pct)
ax3.legend()

# Plot 4: Throughput degradation percentage
baseline_throughput = mean_throughputs[0]  # 0% abort rate as baseline
degradation_pct = [(baseline_throughput - tp) / baseline_throughput * 100
                   for tp in mean_throughputs]

ax4 = axes[1, 1]
bars = ax4.bar(abort_rates_pct, degradation_pct,
               color=['green' if x <= 0 else 'purple' for x in degradation_pct],
               alpha=0.7, edgecolor='black', linewidth=1.5)
ax4.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax4.set_ylabel('Throughput Degradation (%)', fontsize=11, fontweight='bold')
ax4.set_title('Throughput Degradation from Baseline (0%)', fontsize=12, fontweight='bold')
ax4.grid(True, alpha=0.3, axis='y')
ax4.set_xticks(abort_rates_pct)

# Add value labels on bars
for i, (bar, val) in enumerate(zip(bars, degradation_pct)):
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height,
            f'{val:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()

# Save plot
plot_filename = output_dir / "cascading_aborts_plots.png"
plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
print(f"Saved plots to: {plot_filename}")

# Also save as PDF for publication quality
plot_pdf_filename = output_dir / "cascading_aborts_plots.pdf"
plt.savefig(plot_pdf_filename, bbox_inches='tight')
print(f"Saved PDF plots to: {plot_pdf_filename}")

plt.show()

# Print summary statistics
print("\n" + "="*70)
print("CASCADING ABORT EXPERIMENT SUMMARY")
print("="*70)
print(f"\nExperiment: Pipelined 2PC with varying abort rates")
print(f"Workload: 2500 queries, 50 keys, max_concurrency=50, zipf=0.9")
print(f"Iterations per configuration: 3")
print("\n" + "-"*70)
print(f"{'Abort Rate':<15} {'Mean Throughput':<20} {'Std Dev':<15} {'Mean Txns':<15}")
print("-"*70)
for abort_rate, mean_tp, std_tp, mean_txn in zip(abort_rates, mean_throughputs, std_throughputs, mean_total_txns):
    print(f"{abort_rate*100:>6.0f}%         {mean_tp:>10.2f} txn/s      {std_tp:>10.2f}      {mean_txn:>10.1f}")
print("-"*70)
print(f"\nThroughput degradation from 0% baseline:")
for abort_rate, deg in zip(abort_rates[1:], degradation_pct[1:]):
    print(f"  {abort_rate*100:.0f}% abort rate: {deg:.1f}% degradation")
print("="*70)
