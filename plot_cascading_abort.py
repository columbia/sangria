#!/usr/bin/env python3
"""Plot cascading abort experiment results"""

import pandas as pd
import glob
import sys
sys.path.insert(0, 'workload-generator/scripts')
from plotter import line, make_plots

# Load all the CSV results
csv_files = glob.glob("spectacular_ant_cascading_abort_Pipelined_*pct_results.csv")

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
summary = combined_df.groupby('artificial_abort_rate')['throughput'].mean().reset_index()
summary['abort_rate_pct'] = summary['artificial_abort_rate'] * 100

print("\n=== Cascading Abort Results Summary ===")
print(summary)
print()

# Create a simple plot
# Add a dummy 'key' column for the line function
summary['baseline'] = 'Pipelined'

# Create the plot
figs = [[
    (line, {
        'df': summary,
        'x': 'abort_rate_pct',
        'y': 'throughput',
        'key': 'baseline',
        'showlegend': False,
        'x_axis_title': 'Artificial Abort Rate (%)',
        'y_axis_title': 'Throughput (txn/s)',
        'title': 'Cascading Abort Performance',
        'x_range': [0, 55],
        'y_range': [0, max(summary['throughput']) * 1.1],
    })
]]

fig = make_plots(
    figs,
    rows=1,
    cols=1,
    axis_title_font_size={'x': 18, 'y': 18},
    axis_tick_font_size={'x': 14, 'y': 14},
    output_path='cascading_abort_results',
    height=500,
    width=800,
    showlegend=False,
    title='<b>Throughput vs Artificial Abort Rate</b>',
)

print("✓ Plot saved to cascading_abort_results.png")
