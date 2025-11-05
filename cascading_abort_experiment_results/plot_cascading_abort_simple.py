#!/usr/bin/env python3
"""Plot cascading abort experiment results using matplotlib"""

import pandas as pd
import matplotlib.pyplot as plt
import glob

# Load all the CSV results
csv_files = sorted(glob.glob("spectacular_ant_cascading_abort_Pipelined_*pct_results.csv"))

# Read and combine all CSVs
dfs = []
for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    # Extract abort rate from filename (e.g., "10pct" -> 0.1)
    abort_rate_str = csv_file.split("_")[-2]  # e.g., "10pct"
    abort_rate = int(abort_rate_str.replace("pct", "")) / 100.0
    df['artificial_abort_rate'] = abort_rate
    dfs.append(df)

combined_df = pd.concat(dfs, ignore_index=True)

# Group by abort rate and calculate mean throughput
summary = combined_df.groupby('artificial_abort_rate')['throughput'].agg(['mean', 'std']).reset_index()
summary['abort_rate_pct'] = summary['artificial_abort_rate'] * 100

print("\n" + "="*60)
print("CASCADING ABORT RESULTS SUMMARY")
print("="*60)
for _, row in summary.iterrows():
    print(f"Abort Rate: {row['abort_rate_pct']:.0f}%  →  "
          f"Throughput: {row['mean']:.2f} ± {row['std']:.2f} txn/s")
print("="*60 + "\n")

# Create the plot
plt.figure(figsize=(10, 6))
plt.plot(summary['abort_rate_pct'], summary['mean'],
         marker='o', linewidth=2, markersize=8, color='red', label='Pipelined')
plt.fill_between(summary['abort_rate_pct'],
                 summary['mean'] - summary['std'],
                 summary['mean'] + summary['std'],
                 alpha=0.2, color='red')

plt.xlabel('Artificial Abort Rate (%)', fontsize=14)
plt.ylabel('Throughput (txn/s)', fontsize=14)
plt.title('Cascading Abort Performance\n(3000 transactions, 50 concurrency)',
          fontsize=16, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.legend(fontsize=12)
plt.tight_layout()
plt.savefig('cascading_abort_results.png', dpi=300)
print("✓ Plot saved to cascading_abort_results.png")
plt.close()
