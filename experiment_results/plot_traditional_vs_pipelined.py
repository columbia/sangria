#!/usr/bin/env python3
"""
Compare Traditional 2PC vs Pipelined 2PC under cascading aborts
"""
import json
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from collections import defaultdict
import statistics

# Set style for publication-quality plots
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.dpi'] = 300
plt.rcParams['font.size'] = 11

# Load Pipelined 2PC (cascading aborts) results
pipelined_dir = Path(__file__).parent / "cascading_aborts_acoustic_boar"
pipelined_files = glob.glob(str(pipelined_dir / "*/result.json"))

pipelined_data = []
for file in sorted(pipelined_files):
    with open(file) as f:
        result = json.load(f)
        pipelined_data.append({
            "abort_rate": result["config"]["abort_rate"],
            "iteration": result["config"]["iteration"],
            "throughput": result["throughput"],
            "total_transactions": result["total_transactions"],
        })

# Load Traditional 2PC results
traditional_csv = Path(__file__).parent / "traditional_abort_summary.csv"
traditional_df = pd.read_csv(traditional_csv)

# Create DataFrames
pipelined_df = pd.DataFrame(pipelined_data)
pipelined_df = pipelined_df.sort_values(by=["abort_rate", "iteration"])

# Calculate statistics for Pipelined 2PC
pipelined_stats = defaultdict(lambda: {"throughputs": [], "total_txns": []})
for _, row in pipelined_df.iterrows():
    abort_rate = row["abort_rate"]
    pipelined_stats[abort_rate]["throughputs"].append(row["throughput"])
    pipelined_stats[abort_rate]["total_txns"].append(row["total_transactions"])

pipelined_abort_rates = sorted(pipelined_stats.keys())
pipelined_mean_throughputs = []
pipelined_std_throughputs = []
pipelined_mean_total_txns = []

for abort_rate in pipelined_abort_rates:
    throughputs = pipelined_stats[abort_rate]["throughputs"]
    total_txns = pipelined_stats[abort_rate]["total_txns"]

    pipelined_mean_throughputs.append(statistics.mean(throughputs))
    pipelined_std_throughputs.append(statistics.stdev(throughputs) if len(throughputs) > 1 else 0)
    pipelined_mean_total_txns.append(statistics.mean(total_txns))

# Convert to percentages
pipelined_abort_rates_pct = [rate * 100 for rate in pipelined_abort_rates]
traditional_abort_rates_pct = traditional_df["abort_rate_pct"].tolist()

# Create comprehensive comparison figure
fig = plt.figure(figsize=(16, 10))
gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

# Plot 1: Throughput Comparison (main plot)
ax1 = fig.add_subplot(gs[0, :])
ax1.errorbar(traditional_abort_rates_pct, traditional_df["mean_throughput"],
             yerr=traditional_df["std_throughput"],
             marker='o', linewidth=2.5, markersize=10, capsize=5,
             color='#2E86AB', ecolor='#666666', capthick=2,
             label='Traditional 2PC (Locks held until commit)', zorder=3)

ax1.errorbar(pipelined_abort_rates_pct, pipelined_mean_throughputs,
             yerr=pipelined_std_throughputs,
             marker='s', linewidth=2.5, markersize=10, capsize=5,
             color='#C73E1D', ecolor='#666666', capthick=2,
             label='Pipelined 2PC (Cascading aborts)', zorder=2)

ax1.set_xlabel('Abort Rate (%)', fontsize=13, fontweight='bold')
ax1.set_ylabel('Goodput (committed txn/s)', fontsize=13, fontweight='bold')
ax1.set_title('Traditional 2PC vs Pipelined 2PC: Throughput Under Artificial Aborts\n(2500 queries, 50 keys, max_concurrency=50, zipf=0.9)',
             fontsize=14, fontweight='bold')
ax1.grid(True, alpha=0.3)
ax1.legend(fontsize=12, loc='upper right')
ax1.set_xticks([0, 10, 20, 30])

# Add value labels
for x, y in zip(traditional_abort_rates_pct, traditional_df["mean_throughput"]):
    ax1.annotate(f'{y:.0f}', (x, y), textcoords="offset points",
                xytext=(0,10), ha='center', fontsize=9, color='#2E86AB', fontweight='bold')
for x, y in zip(pipelined_abort_rates_pct, pipelined_mean_throughputs):
    if y > 0:
        ax1.annotate(f'{y:.0f}', (x, y), textcoords="offset points",
                    xytext=(0,-15), ha='center', fontsize=9, color='#C73E1D', fontweight='bold')

# Plot 2: Throughput Degradation Percentage
ax2 = fig.add_subplot(gs[1, 0])
traditional_baseline = traditional_df.iloc[0]["mean_throughput"]
pipelined_baseline = pipelined_mean_throughputs[0]

traditional_degradation = [(traditional_baseline - tp) / traditional_baseline * 100
                          for tp in traditional_df["mean_throughput"]]
pipelined_degradation = [(pipelined_baseline - tp) / pipelined_baseline * 100
                        for tp in pipelined_mean_throughputs]

x = np.arange(len(traditional_abort_rates_pct))
width = 0.35

bars1 = ax2.bar(x - width/2, traditional_degradation, width,
                label='Traditional 2PC', color='#2E86AB', alpha=0.7, edgecolor='black')
bars2 = ax2.bar(x + width/2, pipelined_degradation, width,
                label='Pipelined 2PC', color='#C73E1D', alpha=0.7, edgecolor='black')

ax2.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax2.set_ylabel('Throughput Degradation (%)', fontsize=11, fontweight='bold')
ax2.set_title('Throughput Degradation from Baseline (0%)', fontsize=12, fontweight='bold')
ax2.set_xticks(x)
ax2.set_xticklabels([int(r) for r in traditional_abort_rates_pct])
ax2.legend()
ax2.grid(True, alpha=0.3, axis='y')

# Add value labels on bars
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}%', ha='center', va='bottom', fontsize=9, fontweight='bold')

# Plot 3: Total Committed Transactions
ax3 = fig.add_subplot(gs[1, 1])
ax3.plot(traditional_abort_rates_pct, traditional_df["mean_total_transactions"],
         marker='o', linewidth=2.5, markersize=10, color='#2E86AB',
         label='Traditional 2PC')
ax3.plot(pipelined_abort_rates_pct, pipelined_mean_total_txns,
         marker='s', linewidth=2.5, markersize=10, color='#C73E1D',
         label='Pipelined 2PC')

ax3.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax3.set_ylabel('Total Committed Transactions', fontsize=11, fontweight='bold')
ax3.set_title('Total Committed Transactions', fontsize=12, fontweight='bold')
ax3.grid(True, alpha=0.3)
ax3.legend()
ax3.set_xticks([0, 10, 20, 30])

# Plot 4: Efficiency Ratio (Traditional / Pipelined)
ax4 = fig.add_subplot(gs[2, 0])
efficiency_ratios = []
for trad_tp, pipe_tp in zip(traditional_df["mean_throughput"], pipelined_mean_throughputs):
    if pipe_tp > 0:
        efficiency_ratios.append(trad_tp / pipe_tp)
    else:
        efficiency_ratios.append(float('inf'))  # Pipelined collapsed

bars = ax4.bar(traditional_abort_rates_pct, efficiency_ratios,
               color=['green' if r < 2 else 'orange' if r < 5 else 'red' for r in efficiency_ratios],
               alpha=0.7, edgecolor='black', linewidth=1.5)

ax4.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax4.set_ylabel('Efficiency Ratio (Trad / Pipe)', fontsize=11, fontweight='bold')
ax4.set_title('Traditional Advantage Over Pipelined', fontsize=12, fontweight='bold')
ax4.set_xticks([0, 10, 20, 30])
ax4.grid(True, alpha=0.3, axis='y')
ax4.set_ylim(0, 10)

# Add value labels
for i, (bar, val) in enumerate(zip(bars, efficiency_ratios)):
    height = bar.get_height()
    if val == float('inf'):
        label = '∞'
        actual_height = 10
    else:
        label = f'{val:.1f}x'
        actual_height = min(height, 10)
    ax4.text(bar.get_x() + bar.get_width()/2., actual_height,
            label, ha='center', va='bottom', fontsize=10, fontweight='bold')

# Plot 5: Individual trial scatter plot
ax5 = fig.add_subplot(gs[2, 1])

# Traditional trials
for abort_rate_pct in traditional_abort_rates_pct:
    # We don't have individual trial data for traditional, just plot the mean
    idx = traditional_abort_rates_pct.index(abort_rate_pct)
    ax5.scatter([abort_rate_pct], [traditional_df.iloc[idx]["mean_throughput"]],
               alpha=0.6, s=150, color='#2E86AB', marker='o')

# Pipelined trials
for abort_rate in pipelined_abort_rates:
    rate_pct = abort_rate * 100
    throughputs = pipelined_stats[abort_rate]["throughputs"]
    x_points = [rate_pct] * len(throughputs)
    ax5.scatter(x_points, throughputs, alpha=0.6, s=150, color='#C73E1D', marker='s')

# Plot mean lines
ax5.plot(traditional_abort_rates_pct, traditional_df["mean_throughput"],
         linewidth=2, color='#2E86AB', label='Traditional Mean', zorder=5)
ax5.plot(pipelined_abort_rates_pct, pipelined_mean_throughputs,
         linewidth=2, color='#C73E1D', label='Pipelined Mean', zorder=5)

ax5.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax5.set_ylabel('Throughput (txn/s)', fontsize=11, fontweight='bold')
ax5.set_title('Individual Trials + Mean', fontsize=12, fontweight='bold')
ax5.grid(True, alpha=0.3)
ax5.set_xticks([0, 10, 20, 30])
ax5.legend()

plt.tight_layout()

# Save plots
output_dir = Path(__file__).parent
plot_filename = output_dir / "traditional_vs_pipelined_comparison.png"
plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
print(f"Saved comparison plots to: {plot_filename}")

plot_pdf_filename = output_dir / "traditional_vs_pipelined_comparison.pdf"
plt.savefig(plot_pdf_filename, bbox_inches='tight')
print(f"Saved PDF plots to: {plot_pdf_filename}")

# Save combined summary CSV
combined_summary = pd.DataFrame({
    "abort_rate_pct": traditional_abort_rates_pct,
    "traditional_mean_throughput": traditional_df["mean_throughput"],
    "traditional_std_throughput": traditional_df["std_throughput"],
    "pipelined_mean_throughput": pipelined_mean_throughputs,
    "pipelined_std_throughput": pipelined_std_throughputs,
    "traditional_mean_txns": traditional_df["mean_total_transactions"],
    "pipelined_mean_txns": pipelined_mean_total_txns,
    "traditional_degradation_pct": traditional_degradation,
    "pipelined_degradation_pct": pipelined_degradation,
    "efficiency_ratio": efficiency_ratios
})

combined_csv = output_dir / "traditional_vs_pipelined_summary.csv"
combined_summary.to_csv(combined_csv, index=False)
print(f"Saved combined summary to: {combined_csv}")

# Print summary statistics
print("\n" + "="*80)
print("TRADITIONAL 2PC vs PIPELINED 2PC COMPARISON")
print("="*80)
print(f"\nExperiment: Both with 2500 queries, 50 keys, max_concurrency=50, zipf=0.9")
print(f"Artificial abort rates: 0%, 10%, 20%, 30%")
print(f"Iterations per configuration: 3")
print("\n" + "-"*80)
print(f"{'Abort Rate':<12} {'Traditional':<20} {'Pipelined':<20} {'Efficiency Ratio':<20}")
print(f"{'':12} {'(txn/s)':<20} {'(txn/s)':<20} {'(Trad / Pipe)':<20}")
print("-"*80)

for i, abort_rate_pct in enumerate(traditional_abort_rates_pct):
    trad_tp = traditional_df.iloc[i]["mean_throughput"]
    pipe_tp = pipelined_mean_throughputs[i]
    ratio = efficiency_ratios[i]
    ratio_str = f"{ratio:.2f}x" if ratio != float('inf') else "∞ (collapse)"
    print(f"{abort_rate_pct:>6.0f}%      {trad_tp:>10.2f} ± {traditional_df.iloc[i]['std_throughput']:.2f}    "
          f"{pipe_tp:>10.2f} ± {pipelined_std_throughputs[i]:.2f}    {ratio_str:>15}")

print("-"*80)
print(f"\nKey Findings:")
print(f"  • Traditional 2PC at 0% aborts: {traditional_df.iloc[0]['mean_throughput']:.2f} txn/s")
print(f"  • Pipelined 2PC at 0% aborts: {pipelined_mean_throughputs[0]:.2f} txn/s")
print(f"  • Pipelined advantage at 0% aborts: {pipelined_mean_throughputs[0]/traditional_df.iloc[0]['mean_throughput']:.2f}x")
print(f"\n  • Pipelined 2PC suffers COMPLETE COLLAPSE at 10%+ abort rates")
print(f"  • Traditional 2PC gracefully degrades: {traditional_df.iloc[1]['mean_throughput']:.2f} txn/s at 10%")
print(f"  • Traditional 2PC still achieves {traditional_df.iloc[3]['mean_throughput']:.2f} txn/s at 30% aborts")
print("="*80)

# plt.show()  # Commented out for non-interactive execution
