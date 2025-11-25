#!/usr/bin/env python3
"""
Publication-quality comparison plots using Plotly
Traditional 2PC vs Pipelined 2PC under varying abort rates
"""
import json
import glob
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from pathlib import Path
from collections import defaultdict
import statistics

# Load Pipelined 2PC (cascading aborts) results
def load_pipelined_results(pipelined_dir):
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

    # Calculate statistics
    pipelined_stats = defaultdict(lambda: {"throughputs": [], "total_txns": []})
    for row in pipelined_data:
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

    return {
        "abort_rates_pct": [rate * 100 for rate in pipelined_abort_rates],
        "mean_throughput": pipelined_mean_throughputs,
        "std_throughput": pipelined_std_throughputs,
        "mean_total_txns": pipelined_mean_total_txns,
        "stats": pipelined_stats,
        "abort_rates": pipelined_abort_rates
    }

# Find experiment directory
def find_latest_pipelined_dir(base_dir):
    # Look for directories matching pattern
    pattern = "cascading_aborts_*"
    dirs = list(base_dir.glob(pattern))
    if not dirs:
        return None
    # Return the most recently modified
    return max(dirs, key=lambda p: p.stat().st_mtime)

# Main plotting function
def create_comparison_plots():
    output_dir = Path(__file__).parent

    # Load Traditional 2PC results
    traditional_csv = output_dir / "traditional_abort_summary.csv"
    if not traditional_csv.exists():
        print(f"Error: {traditional_csv} not found")
        return

    traditional_df = pd.read_csv(traditional_csv)

    # Load Pipelined 2PC results
    pipelined_dir = find_latest_pipelined_dir(output_dir)
    if not pipelined_dir:
        print("Error: No pipelined results directory found")
        return

    print(f"Using pipelined results from: {pipelined_dir}")
    pipelined = load_pipelined_results(pipelined_dir)

    # Create figure with subplots
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            'Throughput Comparison',
            'Throughput Degradation',
            'Total Committed Transactions',
            'Efficiency Ratio (Traditional / Pipelined)',
            'Individual Trial Variability',
            'Performance Summary Table'
        ),
        specs=[
            [{"type": "scatter"}, {"type": "bar"}],
            [{"type": "scatter"}, {"type": "bar"}],
            [{"type": "scatter"}, {"type": "table"}]
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.12
    )

    # Plot 1: Main throughput comparison
    fig.add_trace(
        go.Scatter(
            x=traditional_df["abort_rate_pct"],
            y=traditional_df["mean_throughput"],
            error_y=dict(type='data', array=traditional_df["std_throughput"]),
            mode='lines+markers',
            name='Traditional 2PC',
            line=dict(color='#2E86AB', width=3),
            marker=dict(size=10, symbol='circle'),
        ),
        row=1, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=pipelined["abort_rates_pct"],
            y=pipelined["mean_throughput"],
            error_y=dict(type='data', array=pipelined["std_throughput"]),
            mode='lines+markers',
            name='Pipelined 2PC',
            line=dict(color='#C73E1D', width=3),
            marker=dict(size=10, symbol='square'),
        ),
        row=1, col=1
    )

    # Plot 2: Degradation bars
    traditional_baseline = traditional_df.iloc[0]["mean_throughput"]
    pipelined_baseline = pipelined["mean_throughput"][0]

    traditional_degradation = [(traditional_baseline - tp) / traditional_baseline * 100
                              for tp in traditional_df["mean_throughput"]]
    pipelined_degradation = [(pipelined_baseline - tp) / pipelined_baseline * 100
                            for tp in pipelined["mean_throughput"]]

    x_labels = [f"{int(r)}%" for r in traditional_df["abort_rate_pct"]]

    fig.add_trace(
        go.Bar(
            x=x_labels,
            y=traditional_degradation,
            name='Traditional',
            marker_color='#2E86AB',
            opacity=0.7
        ),
        row=1, col=2
    )

    fig.add_trace(
        go.Bar(
            x=x_labels,
            y=pipelined_degradation,
            name='Pipelined',
            marker_color='#C73E1D',
            opacity=0.7
        ),
        row=1, col=2
    )

    # Plot 3: Total transactions
    fig.add_trace(
        go.Scatter(
            x=traditional_df["abort_rate_pct"],
            y=traditional_df["mean_total_transactions"],
            mode='lines+markers',
            name='Traditional',
            line=dict(color='#2E86AB', width=2),
            marker=dict(size=8),
            showlegend=False
        ),
        row=2, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=pipelined["abort_rates_pct"],
            y=pipelined["mean_total_txns"],
            mode='lines+markers',
            name='Pipelined',
            line=dict(color='#C73E1D', width=2),
            marker=dict(size=8),
            showlegend=False
        ),
        row=2, col=1
    )

    # Plot 4: Efficiency ratio
    efficiency_ratios = []
    for trad_tp, pipe_tp in zip(traditional_df["mean_throughput"], pipelined["mean_throughput"]):
        if pipe_tp > 0:
            efficiency_ratios.append(trad_tp / pipe_tp)
        else:
            efficiency_ratios.append(10)  # Cap at 10 for visualization

    colors = ['green' if r < 2 else 'orange' if r < 5 else 'red' for r in efficiency_ratios]

    fig.add_trace(
        go.Bar(
            x=x_labels,
            y=efficiency_ratios,
            marker_color=colors,
            opacity=0.8,
            showlegend=False,
            text=[f'{r:.1f}x' if r < 10 else '∞' for r in efficiency_ratios],
            textposition='outside'
        ),
        row=2, col=2
    )

    # Plot 5: Individual trial scatter
    # Traditional trials (just means since we don't have individual data)
    fig.add_trace(
        go.Scatter(
            x=traditional_df["abort_rate_pct"],
            y=traditional_df["mean_throughput"],
            mode='markers',
            name='Traditional (mean)',
            marker=dict(size=12, color='#2E86AB', opacity=0.6),
            showlegend=False
        ),
        row=3, col=1
    )

    # Pipelined trials (individual points)
    for abort_rate in pipelined["abort_rates"]:
        rate_pct = abort_rate * 100
        throughputs = pipelined["stats"][abort_rate]["throughputs"]
        x_points = [rate_pct] * len(throughputs)
        fig.add_trace(
            go.Scatter(
                x=x_points,
                y=throughputs,
                mode='markers',
                marker=dict(size=12, color='#C73E1D', opacity=0.6, symbol='square'),
                showlegend=False
            ),
            row=3, col=1
        )

    # Plot 6: Summary table
    table_headers = ["Abort Rate", "Traditional<br>(txn/s)", "Pipelined<br>(txn/s)", "Ratio"]
    table_data = []

    for i, abort_rate_pct in enumerate(traditional_df["abort_rate_pct"]):
        trad_tp = traditional_df.iloc[i]["mean_throughput"]
        pipe_tp = pipelined["mean_throughput"][i]
        ratio = efficiency_ratios[i]
        table_data.append([
            f"{int(abort_rate_pct)}%",
            f"{trad_tp:.1f} ± {traditional_df.iloc[i]['std_throughput']:.1f}",
            f"{pipe_tp:.1f} ± {pipelined['std_throughput'][i]:.1f}",
            f"{ratio:.1f}x" if ratio < 10 else "∞"
        ])

    fig.add_trace(
        go.Table(
            header=dict(values=table_headers,
                       fill_color='#2E86AB',
                       font=dict(color='white', size=12),
                       align='center'),
            cells=dict(values=list(zip(*table_data)),
                      fill_color='lavender',
                      align='center',
                      font=dict(size=11))
        ),
        row=3, col=2
    )

    # Update layout
    fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=1)
    fig.update_yaxes(title_text="Goodput (committed txn/s)", row=1, col=1)

    fig.update_xaxes(title_text="Abort Rate", row=1, col=2)
    fig.update_yaxes(title_text="Degradation (%)", row=1, col=2)

    fig.update_xaxes(title_text="Abort Rate (%)", row=2, col=1)
    fig.update_yaxes(title_text="Total Committed Transactions", row=2, col=1)

    fig.update_xaxes(title_text="Abort Rate", row=2, col=2)
    fig.update_yaxes(title_text="Efficiency Ratio", row=2, col=2, range=[0, 10])

    fig.update_xaxes(title_text="Abort Rate (%)", row=3, col=1)
    fig.update_yaxes(title_text="Throughput (txn/s)", row=3, col=1)

    fig.update_layout(
        title_text="<b>Traditional 2PC vs Pipelined 2PC: Performance Under Artificial Aborts</b><br>" +
                   "<sub>2500 queries, 50 keys, max_concurrency=50, zipf=0.9</sub>",
        showlegend=True,
        height=1400,
        width=1600,
        font=dict(family="Arial, sans-serif", size=11),
        plot_bgcolor='white',
        paper_bgcolor='white'
    )

    # Save interactive HTML
    html_file = output_dir / "traditional_vs_pipelined_interactive.html"
    fig.write_html(str(html_file))
    print(f"Saved interactive plot to: {html_file}")

    # Save static image (disabled - requires kaleido package)
    # png_file = output_dir / "traditional_vs_pipelined_plotly.png"
    # fig.write_image(str(png_file), width=1600, height=1400)
    # print(f"Saved static image to: {png_file}")

    # Print summary
    print("\n" + "="*80)
    print("TRADITIONAL 2PC vs PIPELINED 2PC COMPARISON")
    print("="*80)
    print(f"\nExperiment: Both with 2500 queries, 50 keys, max_concurrency=50, zipf=0.9")
    print(f"Abort rates: {', '.join([f'{int(r)}%' for r in traditional_df['abort_rate_pct']])}")
    print("\n" + "-"*80)
    print(f"{'Abort Rate':<12} {'Traditional':<25} {'Pipelined':<25} {'Ratio':<15}")
    print(f"{'':12} {'(txn/s)':<25} {'(txn/s)':<25} {'(Trad/Pipe)':<15}")
    print("-"*80)

    for i, abort_rate_pct in enumerate(traditional_df["abort_rate_pct"]):
        trad_tp = traditional_df.iloc[i]["mean_throughput"]
        trad_std = traditional_df.iloc[i]["std_throughput"]
        pipe_tp = pipelined["mean_throughput"][i]
        pipe_std = pipelined["std_throughput"][i]
        ratio = efficiency_ratios[i]
        ratio_str = f"{ratio:.2f}x" if ratio < 10 else "∞"
        print(f"{abort_rate_pct:>6.0f}%      {trad_tp:>10.2f} ± {trad_std:<8.2f}  "
              f"{pipe_tp:>10.2f} ± {pipe_std:<8.2f}  {ratio_str:>10}")

    print("-"*80)
    print(f"\nKey Findings:")
    print(f"  • Traditional at 0%: {traditional_df.iloc[0]['mean_throughput']:.2f} txn/s")
    print(f"  • Pipelined at 0%: {pipelined['mean_throughput'][0]:.2f} txn/s")
    if pipelined["mean_throughput"][0] > traditional_df.iloc[0]["mean_throughput"]:
        ratio = pipelined["mean_throughput"][0] / traditional_df.iloc[0]["mean_throughput"]
        print(f"  • Pipelined advantage at 0%: {ratio:.2f}x faster")
    else:
        ratio = traditional_df.iloc[0]["mean_throughput"] / pipelined["mean_throughput"][0]
        print(f"  • Traditional advantage at 0%: {ratio:.2f}x faster")

    # Check for collapse
    for i, rate in enumerate(traditional_df["abort_rate_pct"]):
        if rate > 0 and pipelined["mean_throughput"][i] == 0:
            print(f"\n  • Pipelined 2PC COLLAPSES at {int(rate)}%+ abort rates")
            print(f"  • Traditional still achieves {traditional_df.iloc[i]['mean_throughput']:.2f} txn/s at {int(rate)}%")
            break

    print("="*80)

if __name__ == "__main__":
    create_comparison_plots()
