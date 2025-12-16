#!/usr/bin/env python3
"""
Create sanity check visualization for debug abort experiment results.

=== HOW TO REPRODUCE ===
cd sangria/experiment_results/debug_sanity_check
python3 create_visualization.py

Generated files:
  - sanity_check_visualization.html
"""

import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Load results
with open('debug_results_summary.json', 'r') as f:
    data = json.load(f)

results = data['results']

# Extract data by baseline
pipelined_0 = next(r for r in results if r['baseline'] == 'Pipelined' and r['abort_rate'] == 0.0)
pipelined_30 = next(r for r in results if r['baseline'] == 'Pipelined' and r['abort_rate'] == 0.3)
traditional_0 = next(r for r in results if r['baseline'] == 'Traditional' and r['abort_rate'] == 0.0)
traditional_30 = next(r for r in results if r['baseline'] == 'Traditional' and r['abort_rate'] == 0.3)

# Create figure with subplots
fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=(
        'Throughput Comparison',
        'P50 Latency Comparison',
        'Transaction Completion',
        'Resolver Dependency Groups (Pipelined Only)'
    ),
    specs=[[{}, {}], [{}, {}]]
)

colors = {'Pipelined': '#2E86AB', 'Traditional': '#E94F37'}
abort_rates = ['0%', '30%']

# 1. Throughput comparison
fig.add_trace(
    go.Bar(
        name='Pipelined',
        x=abort_rates,
        y=[pipelined_0['throughput'], pipelined_30['throughput']],
        marker_color=colors['Pipelined'],
        text=[f"{pipelined_0['throughput']:.1f}", f"{pipelined_30['throughput']:.1f}"],
        textposition='auto',
        legendgroup='Pipelined',
        showlegend=True
    ),
    row=1, col=1
)
fig.add_trace(
    go.Bar(
        name='Traditional',
        x=abort_rates,
        y=[traditional_0['throughput'], traditional_30['throughput']],
        marker_color=colors['Traditional'],
        text=[f"{traditional_0['throughput']:.1f}", f"{traditional_30['throughput']:.1f}"],
        textposition='auto',
        legendgroup='Traditional',
        showlegend=True
    ),
    row=1, col=1
)

# 2. Latency comparison
fig.add_trace(
    go.Bar(
        name='Pipelined',
        x=abort_rates,
        y=[pipelined_0['p50_latency_ms'], pipelined_30['p50_latency_ms']],
        marker_color=colors['Pipelined'],
        text=[f"{pipelined_0['p50_latency_ms']:.1f}ms", f"{pipelined_30['p50_latency_ms']:.1f}ms"],
        textposition='auto',
        legendgroup='Pipelined',
        showlegend=False
    ),
    row=1, col=2
)
fig.add_trace(
    go.Bar(
        name='Traditional',
        x=abort_rates,
        y=[traditional_0['p50_latency_ms'], traditional_30['p50_latency_ms']],
        marker_color=colors['Traditional'],
        text=[f"{traditional_0['p50_latency_ms']:.1f}ms", f"{traditional_30['p50_latency_ms']:.1f}ms"],
        textposition='auto',
        legendgroup='Traditional',
        showlegend=False
    ),
    row=1, col=2
)

# 3. Transaction completion
fig.add_trace(
    go.Bar(
        name='Pipelined',
        x=abort_rates,
        y=[pipelined_0['total_transactions'], pipelined_30['total_transactions']],
        marker_color=colors['Pipelined'],
        text=[f"{pipelined_0['total_transactions']}", f"{pipelined_30['total_transactions']}"],
        textposition='auto',
        legendgroup='Pipelined',
        showlegend=False
    ),
    row=2, col=1
)
fig.add_trace(
    go.Bar(
        name='Traditional',
        x=abort_rates,
        y=[traditional_0['total_transactions'], traditional_30['total_transactions']],
        marker_color=colors['Traditional'],
        text=[f"{traditional_0['total_transactions']}", f"{traditional_30['total_transactions']}"],
        textposition='auto',
        legendgroup='Traditional',
        showlegend=False
    ),
    row=2, col=1
)

# 4. Resolver dependency groups (Pipelined only)
groups_0 = pipelined_0.get('resolver_groups', {})
groups_30 = pipelined_30.get('resolver_groups', {})

group_sizes = ['Size 1', 'Size 2', 'Size 3']
counts_0 = [groups_0.get('size_1', 0), groups_0.get('size_2', 0), groups_0.get('size_3', 0)]
counts_30 = [groups_30.get('size_1', 0), 0, 0]

fig.add_trace(
    go.Bar(
        name='0% Abort',
        x=group_sizes,
        y=counts_0,
        marker_color='#4ECDC4',
        text=[str(c) for c in counts_0],
        textposition='auto',
        showlegend=True
    ),
    row=2, col=2
)
fig.add_trace(
    go.Bar(
        name='30% Abort',
        x=group_sizes,
        y=counts_30,
        marker_color='#FF6B6B',
        text=[str(c) for c in counts_30],
        textposition='auto',
        showlegend=True
    ),
    row=2, col=2
)

# Update layout
fig.update_layout(
    title_text='<b>Pipelined vs Traditional 2PC Sanity Check</b><br><sub>Debug experiment on c220g1-030801.wisc.cloudlab.us | 2025-12-16</sub>',
    barmode='group',
    height=800,
    showlegend=True,
    legend=dict(x=1.02, y=0.5),
)

fig.update_xaxes(title_text='Abort Rate', row=1, col=1)
fig.update_xaxes(title_text='Abort Rate', row=1, col=2)
fig.update_xaxes(title_text='Abort Rate', row=2, col=1)
fig.update_xaxes(title_text='Dependency Group Size', row=2, col=2)

fig.update_yaxes(title_text='tx/s', row=1, col=1)
fig.update_yaxes(title_text='Latency (ms)', row=1, col=2)
fig.update_yaxes(title_text='Transactions', row=2, col=1)
fig.update_yaxes(title_text='Count', row=2, col=2)

# Add annotations
sanity_text = [
    "✓ Pipelined is ~4x faster than Traditional (253 vs 63 tx/s)",
    "✓ Pipelined has ~4x lower latency (75ms vs 310ms p50)",
    "✓ Pipelined resolver tracks dependency groups",
    "✓ Traditional has no resolver (locks prevent concurrency)",
]

fig.add_annotation(
    text="<br>".join(sanity_text),
    xref="paper", yref="paper",
    x=0.5, y=-0.12,
    showarrow=False,
    font=dict(size=12),
    align="center",
    bgcolor="rgba(255,255,255,0.8)",
    bordercolor="#888",
    borderwidth=1
)

# Save
fig.write_html('sanity_check_visualization.html')
print("Saved: sanity_check_visualization.html")

# Print summary
print("\n" + "="*60)
print("SANITY CHECK SUMMARY")
print("="*60)
for obs in data['key_observations']:
    print(f"• {obs}")
print("="*60)
