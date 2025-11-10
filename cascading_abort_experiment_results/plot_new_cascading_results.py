#!/usr/bin/env python3
"""
Custom plotter for cascading abort experiment results with new metrics.

NOTE: This experiment ran with the old parser, so it only has:
- throughput (total attempts, not just commits)
- total_transactions

To get the new metrics (commit_throughput, abort_rate, etc.), we need to:
1. Update ray_task.py on the server with the new parse_metrics patterns
2. Re-run the experiment
"""

import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

# Find all the CSV files from the latest experiment
csv_files = glob.glob("smoky_dugong_cascading_abort_Pipelined_*pct_results.csv")

if not csv_files:
    print("No CSV files found!")
    exit(1)

print(f"Found {len(csv_files)} CSV files")

# Parse abort rate from filename and load data
all_data = []
for csv_file in csv_files:
    df = pd.read_csv(csv_file)

    # Extract abort rate from filename (e.g., "10pct" -> 10)
    abort_pct = int(csv_file.split('_')[-2].replace('pct', ''))

    df['abort_rate_pct'] = abort_pct
    all_data.append(df)

# Combine all data
combined_df = pd.concat(all_data, ignore_index=True)

# Group by abort rate and calculate statistics
summary = combined_df.groupby('abort_rate_pct').agg({
    'throughput': ['mean', 'std', 'min', 'max'],
    'total_transactions': 'mean'
}).reset_index()

# Flatten column names
summary.columns = ['abort_rate_pct', 'throughput_mean', 'throughput_std',
                   'throughput_min', 'throughput_max', 'total_transactions']

print("\n" + "="*80)
print("CASCADING ABORT EXPERIMENT RESULTS")
print("="*80)
print(summary.to_string(index=False))
print("\nNOTE: 'throughput' measures ALL transaction attempts (commits + aborts)")
print("      This is the OLD metric. New metrics require re-running with updated parser.")
print("="*80 + "\n")

# Create plot
fig, ax = plt.subplots(figsize=(10, 6))

# Plot with error bars
ax.errorbar(summary['abort_rate_pct'], summary['throughput_mean'],
            yerr=summary['throughput_std'],
            marker='o', linestyle='-', linewidth=2, markersize=8,
            capsize=5, capthick=2, label='Total Throughput (attempts/s)')

ax.set_xlabel('Artificial Abort Rate (%)', fontsize=12, fontweight='bold')
ax.set_ylabel('Throughput (transactions/second)', fontsize=12, fontweight='bold')
ax.set_title('Cascading Abort Performance (Pipelined 2PC)\nNOTE: Throughput = ALL attempts (commits + aborts)',
             fontsize=14, fontweight='bold')
ax.grid(True, alpha=0.3)
ax.legend(fontsize=10)

# Add value labels on points
for _, row in summary.iterrows():
    ax.annotate(f"{row['throughput_mean']:.1f}",
                (row['abort_rate_pct'], row['throughput_mean']),
                textcoords="offset points", xytext=(0,10),
                ha='center', fontsize=9)

plt.tight_layout()
plt.savefig('smoky_dugong_cascading_abort_OLD_METRIC.png', dpi=300, bbox_inches='tight')
print(f"✓ Plot saved as 'smoky_dugong_cascading_abort_OLD_METRIC.png'")
print("\nTo get new metrics (commit_throughput, abort_rate):")
print("  1. Commit and push the ray_task.py changes")
print("  2. SSH to server and pull changes")
print("  3. Re-run experiment: python3 run_experiments.py")
plt.show()
