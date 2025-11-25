#!/usr/bin/env python3
"""
Plot comparative analysis of Pipelined 2PC vs Traditional 2PC under varying abort rates.
Uses Plotly for interactive, publication-quality visualizations.
"""

import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import sys

# Experiment directories - update these with your actual experiment names
PIPELINED_DIR = "cascading_aborts_stirring_malamute"
TRADITIONAL_DIR = "traditional_aborts_granite_impala"

def load_experiment_data(base_dir: Path, experiment_name: str) -> pd.DataFrame:
    """Load all result.json files from an experiment directory."""
    exp_dir = base_dir / experiment_name / "driver_artifacts"
    if not exp_dir.exists():
        print(f"Warning: Directory not found: {exp_dir}")
        return pd.DataFrame()

    records = []
    for run_dir in exp_dir.iterdir():
        if run_dir.is_dir() and run_dir.name.startswith("run_workload_"):
            result_file = run_dir / "result.json"
            if result_file.exists():
                with open(result_file) as f:
                    data = json.load(f)
                    records.append({
                        "baseline": data["config"]["baseline"],
                        "abort_rate": data["config"]["abort_rate"],
                        "throughput": data["throughput"],
                        "total_transactions": data["total_transactions"],
                        "avg_latency": data["avg_latency"],
                        "p50_latency": data["p50_latency"],
                        "p95_latency": data["p95_latency"],
                        "p99_latency": data["p99_latency"],
                        "total_duration": data["total_duration"],
                        "iteration": data["config"].get("iteration", 0),
                    })

    return pd.DataFrame(records)


def create_throughput_comparison(df: pd.DataFrame, output_dir: Path):
    """Create throughput comparison plot."""
    # Calculate mean and std for each (baseline, abort_rate) combination
    summary = df.groupby(["baseline", "abort_rate"]).agg({
        "throughput": ["mean", "std"],
        "total_transactions": ["mean", "std"]
    }).reset_index()
    summary.columns = ["baseline", "abort_rate", "throughput_mean", "throughput_std",
                       "total_tx_mean", "total_tx_std"]

    # Create figure with secondary y-axis
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Goodput vs Abort Rate", "Committed Transactions vs Abort Rate"),
        horizontal_spacing=0.12
    )

    colors = {"Pipelined": "#2ecc71", "Traditional": "#e74c3c"}

    for baseline in ["Pipelined", "Traditional"]:
        data = summary[summary["baseline"] == baseline]

        # Throughput plot
        fig.add_trace(
            go.Scatter(
                x=data["abort_rate"] * 100,
                y=data["throughput_mean"],
                error_y=dict(type="data", array=data["throughput_std"]),
                mode="lines+markers",
                name=f"{baseline} 2PC",
                line=dict(color=colors[baseline], width=3),
                marker=dict(size=10),
                legendgroup=baseline,
            ),
            row=1, col=1
        )

        # Total transactions plot
        fig.add_trace(
            go.Scatter(
                x=data["abort_rate"] * 100,
                y=data["total_tx_mean"],
                error_y=dict(type="data", array=data["total_tx_std"]),
                mode="lines+markers",
                name=f"{baseline} 2PC",
                line=dict(color=colors[baseline], width=3),
                marker=dict(size=10),
                legendgroup=baseline,
                showlegend=False,
            ),
            row=1, col=2
        )

    fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=1)
    fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=2)
    fig.update_yaxes(title_text="Throughput (tx/s)", row=1, col=1)
    fig.update_yaxes(title_text="Committed Transactions", row=1, col=2)

    fig.update_layout(
        title=dict(
            text="<b>Pipelined 2PC vs Traditional 2PC: Impact of Abort Rate</b>",
            font=dict(size=20)
        ),
        template="plotly_white",
        width=1200,
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        font=dict(size=14)
    )

    # Save as HTML
    fig.write_html(output_dir / "cascading_aborts_comparison.html")
    print(f"Saved: {output_dir / 'cascading_aborts_comparison.html'}")

    # Save as PNG
    try:
        fig.write_image(output_dir / "cascading_aborts_comparison.png", scale=2)
        print(f"Saved: {output_dir / 'cascading_aborts_comparison.png'}")
    except Exception as e:
        print(f"Could not save PNG (install kaleido): {e}")

    return fig


def create_latency_comparison(df: pd.DataFrame, output_dir: Path):
    """Create latency comparison plot."""
    summary = df.groupby(["baseline", "abort_rate"]).agg({
        "avg_latency": ["mean", "std"],
        "p99_latency": ["mean", "std"]
    }).reset_index()
    summary.columns = ["baseline", "abort_rate", "avg_latency_mean", "avg_latency_std",
                       "p99_latency_mean", "p99_latency_std"]

    # Convert to milliseconds
    for col in ["avg_latency_mean", "avg_latency_std", "p99_latency_mean", "p99_latency_std"]:
        summary[col] = summary[col] * 1000

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Average Latency", "P99 Latency"),
        horizontal_spacing=0.12
    )

    colors = {"Pipelined": "#2ecc71", "Traditional": "#e74c3c"}

    for baseline in ["Pipelined", "Traditional"]:
        data = summary[summary["baseline"] == baseline]

        # Average latency
        fig.add_trace(
            go.Scatter(
                x=data["abort_rate"] * 100,
                y=data["avg_latency_mean"],
                error_y=dict(type="data", array=data["avg_latency_std"]),
                mode="lines+markers",
                name=f"{baseline} 2PC",
                line=dict(color=colors[baseline], width=3),
                marker=dict(size=10),
                legendgroup=baseline,
            ),
            row=1, col=1
        )

        # P99 latency
        fig.add_trace(
            go.Scatter(
                x=data["abort_rate"] * 100,
                y=data["p99_latency_mean"],
                error_y=dict(type="data", array=data["p99_latency_std"]),
                mode="lines+markers",
                name=f"{baseline} 2PC",
                line=dict(color=colors[baseline], width=3),
                marker=dict(size=10),
                legendgroup=baseline,
                showlegend=False,
            ),
            row=1, col=2
        )

    fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=1)
    fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=2)
    fig.update_yaxes(title_text="Latency (ms)", row=1, col=1)
    fig.update_yaxes(title_text="Latency (ms)", row=1, col=2)

    fig.update_layout(
        title=dict(
            text="<b>Transaction Latency: Pipelined vs Traditional 2PC</b>",
            font=dict(size=20)
        ),
        template="plotly_white",
        width=1200,
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        font=dict(size=14)
    )

    fig.write_html(output_dir / "cascading_aborts_latency.html")
    print(f"Saved: {output_dir / 'cascading_aborts_latency.html'}")

    return fig


def create_speedup_plot(df: pd.DataFrame, output_dir: Path):
    """Create a plot showing Pipelined speedup over Traditional."""
    summary = df.groupby(["baseline", "abort_rate"])["throughput"].agg(["mean", "std"]).reset_index()
    summary.columns = ["baseline", "abort_rate", "throughput_mean", "throughput_std"]

    # Pivot to compare
    pivot = summary.pivot(index="abort_rate", columns="baseline", values="throughput_mean").reset_index()
    pivot["speedup"] = pivot["Pipelined"] / pivot["Traditional"]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=pivot["abort_rate"] * 100,
        y=pivot["speedup"],
        marker_color="#3498db",
        text=[f"{x:.2f}x" for x in pivot["speedup"]],
        textposition="outside",
        name="Speedup"
    ))

    fig.add_hline(y=1.0, line_dash="dash", line_color="gray",
                  annotation_text="Break-even", annotation_position="right")

    fig.update_layout(
        title=dict(
            text="<b>Pipelined 2PC Speedup over Traditional 2PC</b>",
            font=dict(size=20)
        ),
        xaxis_title="Abort Rate (%)",
        yaxis_title="Speedup (Pipelined / Traditional)",
        template="plotly_white",
        width=800,
        height=500,
        font=dict(size=14)
    )

    fig.write_html(output_dir / "cascading_aborts_speedup.html")
    print(f"Saved: {output_dir / 'cascading_aborts_speedup.html'}")

    return fig, pivot


def create_summary_table(df: pd.DataFrame, output_dir: Path):
    """Create a summary CSV with statistics."""
    summary = df.groupby(["baseline", "abort_rate"]).agg({
        "throughput": ["mean", "std", "min", "max"],
        "total_transactions": ["mean", "std"],
        "avg_latency": ["mean", "std"],
        "p99_latency": ["mean", "std"]
    }).round(2)

    summary.to_csv(output_dir / "cascading_aborts_summary.csv")
    print(f"Saved: {output_dir / 'cascading_aborts_summary.csv'}")

    return summary


def main():
    # Base directory for experiments
    base_dir = Path(__file__).parent

    # Load Pipelined data
    print(f"\nLoading Pipelined data from: {PIPELINED_DIR}")
    pipelined_df = load_experiment_data(base_dir, PIPELINED_DIR)

    # Load Traditional data
    print(f"Loading Traditional data from: {TRADITIONAL_DIR}")
    traditional_df = load_experiment_data(base_dir, TRADITIONAL_DIR)

    if pipelined_df.empty or traditional_df.empty:
        print("\nError: Could not load experiment data!")
        print("Please update PIPELINED_DIR and TRADITIONAL_DIR at the top of this script.")
        sys.exit(1)

    # Combine data
    df = pd.concat([pipelined_df, traditional_df], ignore_index=True)

    print(f"\nLoaded {len(df)} total runs")
    print(f"  Pipelined: {len(pipelined_df)} runs")
    print(f"  Traditional: {len(traditional_df)} runs")

    # Create output directory
    output_dir = base_dir

    # Generate plots
    print("\nGenerating plots...")

    fig1 = create_throughput_comparison(df, output_dir)
    fig2 = create_latency_comparison(df, output_dir)
    fig3, speedup_data = create_speedup_plot(df, output_dir)
    summary = create_summary_table(df, output_dir)

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY: Pipelined 2PC vs Traditional 2PC")
    print("="*60)

    print("\nSpeedup at each abort rate:")
    for _, row in speedup_data.iterrows():
        print(f"  {int(row['abort_rate']*100)}% abort: {row['speedup']:.2f}x speedup")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
