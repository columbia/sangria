#!/usr/bin/env python3
"""
Plot latency comparison between Pipelined and Traditional 2PC.
Data from latency experiment with Zipf=0.3, abort rates: 0%, 10%, 30%.

=== HOW TO REPRODUCE ===
1. Run the latency experiment on CloudLab:
   cd ~/sangria
   python3 workload-generator/scripts/latency_experiment.py --baseline both

2. Copy results from ~/ray_results/ to experiment_results/latency_results/

3. Update the data dict below with the new results

4. Run this script to generate plots:
   python3 plot_latency.py

Generated files:
  - latency_comparison.html
  - tail_latency_comparison.html
  - latency_speedup.html
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Data from the latency experiment
data = {
    "Pipelined": {
        "abort_rate": [0, 10, 30],
        "throughput": [1085.46, 1273.23, 723.41],
        "avg_latency_ms": [46.007, 31.818, 33.878],
        "p50_latency_ms": [44.667, 16.104, None],  # P50 not shown for 30%
        "p95_latency_ms": [69.539, 168.051, None],
        "p99_latency_ms": [98.381, 197.355, None],
    },
    "Traditional": {
        "abort_rate": [0, 10, 30],
        "throughput": [771.17, 637.18, 401.78],
        "avg_latency_ms": [64.596, 63.105, 60.815],
        "p50_latency_ms": [17.443, 16.883, 16.408],
        "p95_latency_ms": [477.790, 467.153, 451.548],
        "p99_latency_ms": [557.204, 546.224, 532.885],
    }
}

colors = {"Pipelined": "#2ecc71", "Traditional": "#e74c3c"}

# Create figure with 2 subplots side by side
fig = make_subplots(
    rows=1, cols=2,
    subplot_titles=["Average Latency (ms)", "Throughput (tx/s)"],
    horizontal_spacing=0.12
)

# Plot 1: Average Latency
for baseline in ["Pipelined", "Traditional"]:
    fig.add_trace(
        go.Scatter(
            x=data[baseline]["abort_rate"],
            y=data[baseline]["avg_latency_ms"],
            mode="lines+markers",
            name=f"{baseline} 2PC",
            line=dict(color=colors[baseline], width=3),
            marker=dict(size=12),
            legendgroup=baseline,
        ),
        row=1, col=1
    )

# Plot 2: Throughput
for baseline in ["Pipelined", "Traditional"]:
    fig.add_trace(
        go.Scatter(
            x=data[baseline]["abort_rate"],
            y=data[baseline]["throughput"],
            mode="lines+markers",
            name=f"{baseline} 2PC",
            line=dict(color=colors[baseline], width=3),
            marker=dict(size=12),
            legendgroup=baseline,
            showlegend=False,
        ),
        row=1, col=2
    )

fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=1)
fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=2)
fig.update_yaxes(title_text="Latency (ms)", row=1, col=1)
fig.update_yaxes(title_text="Throughput (tx/s)", row=1, col=2)

fig.update_layout(
    title=dict(
        text="<b>Latency Comparison: Pipelined vs Traditional 2PC (Zipf=0.3)</b>",
        font=dict(size=20)
    ),
    template="plotly_white",
    width=1100,
    height=450,
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5
    ),
    font=dict(size=14)
)

fig.write_html("latency_comparison.html")
print("Saved: latency_comparison.html")

# Create a second figure for tail latencies (P95, P99)
fig2 = make_subplots(
    rows=1, cols=2,
    subplot_titles=["P95 Latency (ms)", "P99 Latency (ms)"],
    horizontal_spacing=0.12
)

# P95 Latency
for baseline in ["Pipelined", "Traditional"]:
    # Filter out None values
    abort_rates = []
    p95_values = []
    for i, v in enumerate(data[baseline]["p95_latency_ms"]):
        if v is not None:
            abort_rates.append(data[baseline]["abort_rate"][i])
            p95_values.append(v)

    fig2.add_trace(
        go.Bar(
            x=[f"{ar}%" for ar in abort_rates],
            y=p95_values,
            name=f"{baseline} 2PC",
            marker_color=colors[baseline],
            legendgroup=baseline,
        ),
        row=1, col=1
    )

# P99 Latency
for baseline in ["Pipelined", "Traditional"]:
    abort_rates = []
    p99_values = []
    for i, v in enumerate(data[baseline]["p99_latency_ms"]):
        if v is not None:
            abort_rates.append(data[baseline]["abort_rate"][i])
            p99_values.append(v)

    fig2.add_trace(
        go.Bar(
            x=[f"{ar}%" for ar in abort_rates],
            y=p99_values,
            name=f"{baseline} 2PC",
            marker_color=colors[baseline],
            legendgroup=baseline,
            showlegend=False,
        ),
        row=1, col=2
    )

fig2.update_yaxes(title_text="Latency (ms)", row=1, col=1)
fig2.update_yaxes(title_text="Latency (ms)", row=1, col=2)

fig2.update_layout(
    title=dict(
        text="<b>Tail Latency Comparison: Pipelined vs Traditional 2PC (Zipf=0.3)</b>",
        font=dict(size=20)
    ),
    template="plotly_white",
    width=1100,
    height=450,
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5
    ),
    font=dict(size=14),
    barmode="group"
)

fig2.write_html("tail_latency_comparison.html")
print("Saved: tail_latency_comparison.html")

# Create a combined summary figure
fig3 = go.Figure()

# Speedup ratios
abort_rates = [0, 10, 30]
latency_speedup = [64.596/46.007, 63.105/31.818, 60.815/33.878]
throughput_ratio = [1085.46/771.17, 1273.23/637.18, 723.41/401.78]

fig3.add_trace(go.Bar(
    x=[f"{ar}%" for ar in abort_rates],
    y=latency_speedup,
    name="Latency Speedup (Trad/Pipe)",
    marker_color="#3498db",
))

fig3.add_trace(go.Bar(
    x=[f"{ar}%" for ar in abort_rates],
    y=throughput_ratio,
    name="Throughput Ratio (Pipe/Trad)",
    marker_color="#9b59b6",
))

fig3.add_hline(y=1.0, line_dash="dash", line_color="gray",
              annotation_text="Break-even", annotation_position="right")

fig3.update_layout(
    title=dict(
        text="<b>Pipelined 2PC Advantage Over Traditional (Zipf=0.3)</b>",
        font=dict(size=20)
    ),
    xaxis_title="Abort Rate",
    yaxis_title="Ratio (higher = Pipelined is better)",
    template="plotly_white",
    width=800,
    height=450,
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5
    ),
    font=dict(size=14),
    barmode="group"
)

fig3.write_html("latency_speedup.html")
print("Saved: latency_speedup.html")

print("\nSummary:")
print("="*60)
print(f"{'Abort%':<8} {'Latency Speedup':<18} {'Throughput Ratio'}")
print("-"*60)
for i, ar in enumerate(abort_rates):
    print(f"{ar}%{'':<5} {latency_speedup[i]:.2f}x{'':<14} {throughput_ratio[i]:.2f}x")
print("="*60)
