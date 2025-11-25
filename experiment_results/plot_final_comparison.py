#!/usr/bin/env python3
"""
Generate publication-quality comparison plot: Traditional 2PC vs Pipelined 2PC
with complete 0-50% abort rate data.
"""

import json
import os
import glob
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# Data directories
TRADITIONAL_DIR = "/Users/ifesionubogu/Desktop/shared_sangria_2/sangria/experiment_results/traditional_aborts_provocative_woodlouse"
PIPELINED_DIR = "/Users/ifesionubogu/Desktop/shared_sangria_2/sangria/experiment_results/cascading_aborts_proficient_nuthatch"

def load_results(results_dir):
    """Load all result.json files from experiment directory."""
    results = []
    for trial_dir in glob.glob(os.path.join(results_dir, "run_workload_*")):
        result_file = os.path.join(trial_dir, "result.json")
        if os.path.exists(result_file):
            with open(result_file) as f:
                data = json.load(f)
                results.append({
                    'abort_rate': data['config']['abort_rate'],
                    'throughput': data['throughput'],
                    'total_transactions': data['total_transactions'],
                    'baseline': data['config']['baseline']
                })
    return results

def aggregate_results(results):
    """Aggregate results by abort rate."""
    from collections import defaultdict
    aggregated = defaultdict(lambda: {'throughputs': [], 'transactions': []})

    for r in results:
        abort_rate = r['abort_rate']
        aggregated[abort_rate]['throughputs'].append(r['throughput'])
        aggregated[abort_rate]['transactions'].append(r['total_transactions'])

    summary = {}
    for abort_rate, data in sorted(aggregated.items()):
        summary[abort_rate] = {
            'mean_throughput': np.mean(data['throughputs']),
            'std_throughput': np.std(data['throughputs']),
            'mean_transactions': np.mean(data['transactions']),
            'std_transactions': np.std(data['transactions']),
            'all_throughputs': data['throughputs']
        }
    return summary

# Load and process data
print("Loading Traditional 2PC results...")
traditional_results = load_results(TRADITIONAL_DIR)
traditional_summary = aggregate_results(traditional_results)

print("Loading Pipelined 2PC results...")
pipelined_results = load_results(PIPELINED_DIR)
pipelined_summary = aggregate_results(pipelined_results)

# Print summaries
print("\n=== Traditional 2PC Summary ===")
for abort_rate, data in traditional_summary.items():
    print(f"  {int(abort_rate*100)}%: {data['mean_throughput']:.1f} ± {data['std_throughput']:.1f} txn/s")

print("\n=== Pipelined 2PC Summary ===")
for abort_rate, data in pipelined_summary.items():
    print(f"  {int(abort_rate*100)}%: {data['mean_throughput']:.1f} ± {data['std_throughput']:.1f} txn/s")

# Prepare data for plotting
abort_rates = sorted(set(traditional_summary.keys()) | set(pipelined_summary.keys()))
abort_percentages = [int(r * 100) for r in abort_rates]

traditional_means = [traditional_summary.get(r, {'mean_throughput': 0})['mean_throughput'] for r in abort_rates]
traditional_stds = [traditional_summary.get(r, {'std_throughput': 0})['std_throughput'] for r in abort_rates]

pipelined_means = [pipelined_summary.get(r, {'mean_throughput': 0})['mean_throughput'] for r in abort_rates]
pipelined_stds = [pipelined_summary.get(r, {'std_throughput': 0})['std_throughput'] for r in abort_rates]

# Create figure with subplots
fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=(
        '<b>Throughput Comparison</b>',
        '<b>Throughput Degradation (%)</b>',
        '<b>Committed Transactions</b>',
        '<b>Performance Summary</b>'
    ),
    specs=[[{"type": "scatter"}, {"type": "scatter"}],
           [{"type": "scatter"}, {"type": "table"}]],
    vertical_spacing=0.15,
    horizontal_spacing=0.1
)

# Color scheme
TRADITIONAL_COLOR = '#2E86AB'  # Blue
PIPELINED_COLOR = '#E94F37'    # Red

# Plot 1: Throughput Comparison with error bars
fig.add_trace(
    go.Scatter(
        x=abort_percentages,
        y=traditional_means,
        mode='lines+markers',
        name='Traditional 2PC',
        line=dict(color=TRADITIONAL_COLOR, width=3),
        marker=dict(size=10, symbol='circle'),
        error_y=dict(type='data', array=traditional_stds, visible=True, color=TRADITIONAL_COLOR)
    ),
    row=1, col=1
)

fig.add_trace(
    go.Scatter(
        x=abort_percentages,
        y=pipelined_means,
        mode='lines+markers',
        name='Pipelined 2PC',
        line=dict(color=PIPELINED_COLOR, width=3),
        marker=dict(size=10, symbol='diamond'),
        error_y=dict(type='data', array=pipelined_stds, visible=True, color=PIPELINED_COLOR)
    ),
    row=1, col=1
)

# Plot 2: Throughput degradation relative to 0% abort rate
trad_baseline = traditional_means[0] if traditional_means[0] > 0 else 1
pipe_baseline = pipelined_means[0] if pipelined_means[0] > 0 else 1

trad_degradation = [(1 - t/trad_baseline) * 100 for t in traditional_means]
pipe_degradation = [(1 - p/pipe_baseline) * 100 for p in pipelined_means]

fig.add_trace(
    go.Scatter(
        x=abort_percentages,
        y=trad_degradation,
        mode='lines+markers',
        name='Traditional 2PC',
        line=dict(color=TRADITIONAL_COLOR, width=3),
        marker=dict(size=10, symbol='circle'),
        showlegend=False
    ),
    row=1, col=2
)

fig.add_trace(
    go.Scatter(
        x=abort_percentages,
        y=pipe_degradation,
        mode='lines+markers',
        name='Pipelined 2PC',
        line=dict(color=PIPELINED_COLOR, width=3),
        marker=dict(size=10, symbol='diamond'),
        showlegend=False
    ),
    row=1, col=2
)

# Plot 3: Committed Transactions
trad_txns = [traditional_summary.get(r, {'mean_transactions': 0})['mean_transactions'] for r in abort_rates]
pipe_txns = [pipelined_summary.get(r, {'mean_transactions': 0})['mean_transactions'] for r in abort_rates]

fig.add_trace(
    go.Scatter(
        x=abort_percentages,
        y=trad_txns,
        mode='lines+markers',
        name='Traditional 2PC',
        line=dict(color=TRADITIONAL_COLOR, width=3),
        marker=dict(size=10, symbol='circle'),
        showlegend=False
    ),
    row=2, col=1
)

fig.add_trace(
    go.Scatter(
        x=abort_percentages,
        y=pipe_txns,
        mode='lines+markers',
        name='Pipelined 2PC',
        line=dict(color=PIPELINED_COLOR, width=3),
        marker=dict(size=10, symbol='diamond'),
        showlegend=False
    ),
    row=2, col=1
)

# Plot 4: Summary Table
table_data = {
    'Abort Rate': [f'{p}%' for p in abort_percentages],
    'Traditional (txn/s)': [f'{t:.1f}' for t in traditional_means],
    'Pipelined (txn/s)': [f'{p:.1f}' for p in pipelined_means],
    'Difference': [f'{t-p:+.1f}' if p > 0 else f'+{t:.1f}' for t, p in zip(traditional_means, pipelined_means)]
}

fig.add_trace(
    go.Table(
        header=dict(
            values=['<b>Abort Rate</b>', '<b>Traditional</b>', '<b>Pipelined</b>', '<b>Δ (Trad-Pipe)</b>'],
            fill_color='#404040',
            font=dict(color='white', size=12),
            align='center'
        ),
        cells=dict(
            values=[table_data['Abort Rate'], table_data['Traditional (txn/s)'],
                   table_data['Pipelined (txn/s)'], table_data['Difference']],
            fill_color=[['#f8f8f8', '#ffffff'] * 3],
            font=dict(size=11),
            align='center',
            height=25
        )
    ),
    row=2, col=2
)

# Update layout
fig.update_layout(
    title=dict(
        text='<b>Traditional 2PC vs Pipelined 2PC: Impact of Artificial Abort Rates</b>',
        font=dict(size=18),
        x=0.5
    ),
    font=dict(family='Arial', size=12),
    legend=dict(
        orientation='h',
        yanchor='bottom',
        y=1.02,
        xanchor='center',
        x=0.5,
        font=dict(size=12)
    ),
    height=700,
    width=1100,
    template='plotly_white'
)

# Update axes
fig.update_xaxes(title_text='Abort Rate (%)', row=1, col=1, dtick=10)
fig.update_yaxes(title_text='Throughput (txn/s)', row=1, col=1)

fig.update_xaxes(title_text='Abort Rate (%)', row=1, col=2, dtick=10)
fig.update_yaxes(title_text='Degradation (%)', row=1, col=2)

fig.update_xaxes(title_text='Abort Rate (%)', row=2, col=1, dtick=10)
fig.update_yaxes(title_text='Committed Transactions', row=2, col=1)

# Save
output_path = '/Users/ifesionubogu/Desktop/shared_sangria_2/sangria/experiment_results/traditional_vs_pipelined_0_50.html'
fig.write_html(output_path)
print(f"\nPlot saved to: {output_path}")

# Also create a simpler single-plot version for publication
fig2 = go.Figure()

fig2.add_trace(
    go.Scatter(
        x=abort_percentages,
        y=traditional_means,
        mode='lines+markers',
        name='Traditional 2PC',
        line=dict(color=TRADITIONAL_COLOR, width=3),
        marker=dict(size=12, symbol='circle'),
        error_y=dict(type='data', array=traditional_stds, visible=True, color=TRADITIONAL_COLOR, thickness=2)
    )
)

fig2.add_trace(
    go.Scatter(
        x=abort_percentages,
        y=pipelined_means,
        mode='lines+markers',
        name='Pipelined 2PC',
        line=dict(color=PIPELINED_COLOR, width=3),
        marker=dict(size=12, symbol='diamond'),
        error_y=dict(type='data', array=pipelined_stds, visible=True, color=PIPELINED_COLOR, thickness=2)
    )
)

fig2.update_layout(
    title=dict(
        text='<b>Throughput Under Artificial Abort Rates</b>',
        font=dict(size=20),
        x=0.5
    ),
    xaxis_title='Abort Rate (%)',
    yaxis_title='Throughput (txn/s)',
    font=dict(family='Arial', size=14),
    legend=dict(
        orientation='h',
        yanchor='bottom',
        y=1.02,
        xanchor='center',
        x=0.5,
        font=dict(size=14)
    ),
    height=500,
    width=800,
    template='plotly_white'
)

fig2.update_xaxes(dtick=10, range=[-2, 52])
fig2.update_yaxes(range=[0, max(traditional_means + pipelined_means) * 1.1])

output_path2 = '/Users/ifesionubogu/Desktop/shared_sangria_2/sangria/experiment_results/throughput_comparison_simple.html'
fig2.write_html(output_path2)
print(f"Simple plot saved to: {output_path2}")

print("\n=== KEY FINDINGS ===")
print(f"At 0% abort rate:")
print(f"  - Traditional: {traditional_means[0]:.1f} txn/s")
print(f"  - Pipelined: {pipelined_means[0]:.1f} txn/s")
if pipelined_means[0] > 0:
    print(f"  - Pipelined is {pipelined_means[0]/traditional_means[0]:.2f}x faster")

print(f"\nAt 10%+ abort rate:")
print(f"  - Traditional: maintains {traditional_means[1]:.1f} txn/s ({100*traditional_means[1]/traditional_means[0]:.0f}% of baseline)")
print(f"  - Pipelined: collapses to {pipelined_means[1]:.1f} txn/s ({100*pipelined_means[1]/pipelined_means[0]:.0f}% of baseline)")

print(f"\nAt 50% abort rate:")
print(f"  - Traditional: {traditional_means[-1]:.1f} txn/s ({100*traditional_means[-1]/traditional_means[0]:.0f}% of baseline)")
print(f"  - Pipelined: {pipelined_means[-1]:.1f} txn/s")
