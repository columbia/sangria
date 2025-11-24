#!/usr/bin/env python3
"""
Plot Traditional 2PC abort rate experiment results.

This experiment tests Traditional 2PC under varying artificial abort rates
to measure goodput (committed transactions/sec). Unlike Pipelined 2PC,
Traditional 2PC should NOT experience cascading aborts because it holds
locks until commit.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Read the data
df = pd.read_csv('traditional_abort_results.csv')

# Group by abort_rate and calculate statistics
summary = df.groupby('config/abort_rate').agg({
    'throughput': ['mean', 'std'],
    'total_transactions': 'mean'
}).reset_index()

summary.columns = ['abort_rate', 'mean_throughput', 'std_throughput', 'mean_total_transactions']

# Convert abort_rate to percentage for display
summary['abort_rate_pct'] = summary['abort_rate'] * 100

print("\n=== Traditional 2PC Abort Experiment Summary ===")
print(summary)
print()

# Calculate throughput degradation from baseline (0% abort rate)
baseline_throughput = summary[summary['abort_rate'] == 0.0]['mean_throughput'].values[0]
summary['throughput_degradation_pct'] = ((baseline_throughput - summary['mean_throughput']) / baseline_throughput) * 100

# Save summary
summary.to_csv('traditional_abort_summary.csv', index=False)
print("Summary saved to traditional_abort_summary.csv")

# Create figure with subplots
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Traditional 2PC Abort Experiment Results\n(2500 queries, 50 keys, zipf=0.9)',
             fontsize=14, fontweight='bold')

# Plot 1: Committed Transaction Throughput
ax1 = axes[0, 0]
ax1.errorbar(summary['abort_rate_pct'], summary['mean_throughput'],
             yerr=summary['std_throughput'], marker='o', linestyle='-',
             capsize=5, capthick=2, label='Mean', color='steelblue', linewidth=2, markersize=8)

# Plot individual trials
for abort_rate in df['config/abort_rate'].unique():
    trials = df[df['config/abort_rate'] == abort_rate]
    ax1.scatter([abort_rate * 100] * len(trials), trials['throughput'],
               alpha=0.4, s=80, color='coral', label='Individual Trials' if abort_rate == 0.0 else '')

# Add value labels
for _, row in summary.iterrows():
    ax1.annotate(f'{row["mean_throughput"]:.1f}',
                xy=(row['abort_rate_pct'], row['mean_throughput']),
                xytext=(0, 10), textcoords='offset points',
                ha='center', fontsize=9, fontweight='bold')

ax1.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax1.set_ylabel('Throughput (txn/s)', fontsize=11, fontweight='bold')
ax1.set_title('Committed Transaction Throughput', fontsize=12, fontweight='bold')
ax1.grid(True, alpha=0.3, linestyle='--')
ax1.legend()
ax1.set_ylim(bottom=0)

# Plot 2: Total Committed Transactions
ax2 = axes[0, 1]
ax2.plot(summary['abort_rate_pct'], summary['mean_total_transactions'],
         marker='o', linestyle='-', color='darkorange', linewidth=2, markersize=8)

# Add value labels
for _, row in summary.iterrows():
    ax2.annotate(f'{row["mean_total_transactions"]:.0f}',
                xy=(row['abort_rate_pct'], row['mean_total_transactions']),
                xytext=(0, 10), textcoords='offset points',
                ha='center', fontsize=9, fontweight='bold')

ax2.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax2.set_ylabel('Total Committed Transactions', fontsize=11, fontweight='bold')
ax2.set_title('Total Committed Transactions', fontsize=12, fontweight='bold')
ax2.grid(True, alpha=0.3, linestyle='--')
ax2.set_ylim(bottom=0)

# Plot 3: Individual Trials + Mean
ax3 = axes[1, 0]
for abort_rate in df['config/abort_rate'].unique():
    trials = df[df['config/abort_rate'] == abort_rate]
    ax3.scatter([abort_rate * 100] * len(trials), trials['throughput'],
               alpha=0.6, s=100, label=f'{abort_rate*100:.0f}%')

# Add mean line
ax3.plot(summary['abort_rate_pct'], summary['mean_throughput'],
         marker='D', linestyle='--', color='steelblue', linewidth=2,
         markersize=10, label='Mean', zorder=10)

ax3.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax3.set_ylabel('Throughput (txn/s)', fontsize=11, fontweight='bold')
ax3.set_title('Individual Trials + Mean', fontsize=12, fontweight='bold')
ax3.legend(title='Abort Rate')
ax3.grid(True, alpha=0.3, linestyle='--')
ax3.set_ylim(bottom=0)

# Plot 4: Throughput Degradation from Baseline
ax4 = axes[1, 1]
bars = ax4.bar(summary['abort_rate_pct'], summary['throughput_degradation_pct'],
               color=['green' if x <= 0 else 'purple' for x in summary['throughput_degradation_pct']],
               alpha=0.7, edgecolor='black', linewidth=1.5)

# Add value labels
for i, (_, row) in enumerate(summary.iterrows()):
    ax4.text(row['abort_rate_pct'], row['throughput_degradation_pct'] + 1.5,
            f'{row["throughput_degradation_pct"]:.1f}%',
            ha='center', va='bottom' if row['throughput_degradation_pct'] >= 0 else 'top',
            fontsize=10, fontweight='bold')

ax4.axhline(y=0, color='black', linestyle='-', linewidth=1)
ax4.set_xlabel('Abort Rate (%)', fontsize=11, fontweight='bold')
ax4.set_ylabel('Throughput Degradation (%)', fontsize=11, fontweight='bold')
ax4.set_title('Throughput Degradation from Baseline (0%)', fontsize=12, fontweight='bold')
ax4.grid(True, alpha=0.3, linestyle='--', axis='y')

plt.tight_layout()
plt.savefig('traditional_abort_plots.png', dpi=300, bbox_inches='tight')
print("Plot saved to traditional_abort_plots.png")
plt.close()

print("\n=== Key Findings ===")
print(f"Baseline throughput (0% abort): {baseline_throughput:.2f} txn/s")
for _, row in summary.iterrows():
    if row['abort_rate'] > 0:
        print(f"{row['abort_rate_pct']:.0f}% abort rate: {row['mean_throughput']:.2f} txn/s "
              f"({row['throughput_degradation_pct']:.1f}% change)")

print("\nNote: Traditional 2PC does NOT have cascading aborts because locks are held until commit.")
print("Throughput variation is due to retry overhead and statistical variance, NOT cascading aborts.")
