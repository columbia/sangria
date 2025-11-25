#!/usr/bin/env python3
"""
Plot Pipelined 2PC vs Traditional 2PC comparison across Zipf values (0.1, 0.3, 0.6).
Compares goodput (committed transactions per second) under varying abort rates.
"""

import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# Experiment directories for each Zipf value
EXPERIMENTS = {
    0.1: {
        "Pipelined": "pipelined_zipf_0.1_hungry_octopus",
        "Traditional": "traditional_zipf_0.1_precise_cockatoo",
    },
    0.3: {
        "Pipelined": "pipelined_zipf_0.3_amiable_manatee",
        "Traditional": "traditional_zipf_0.3_singing_salmon",
    },
    0.6: {
        "Pipelined": "pipelined_zipf_0.6_soft_salmon",
        "Traditional": "traditional_zipf_0.6_sticky_skylark",
    },
}


def load_experiment_data(base_dir: Path, experiment_name: str) -> pd.DataFrame:
    """Load all result.json files from an experiment directory."""
    exp_dir = base_dir / experiment_name
    if not exp_dir.exists():
        print(f"Warning: Directory not found: {exp_dir}")
        return pd.DataFrame()

    records = []
    for result_file in exp_dir.rglob("result.json"):
        with open(result_file) as f:
            data = json.load(f)
            records.append({
                "baseline": data["config"]["baseline"],
                "abort_rate": data["config"]["abort_rate"],
                "zipf_exponent": data["config"]["zipf_exponent"],
                "num_keys": data["config"]["num_keys"],
                "throughput": data["throughput"],  # This is goodput (committed tx/s)
                "total_transactions": data["total_transactions"],  # Committed transactions
                "avg_latency": data["avg_latency"],
                "p50_latency": data["p50_latency"],
                "p95_latency": data["p95_latency"],
                "p99_latency": data["p99_latency"],
                "total_duration": data["total_duration"],
                "iteration": data["config"].get("iteration", 0),
            })

    return pd.DataFrame(records)


def create_committed_tx_comparison(df: pd.DataFrame, output_dir: Path):
    """Create committed transactions comparison plot with 3 subplots (one per Zipf value)."""
    zipf_values = sorted(df["zipf_exponent"].unique())

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=[f"Zipf = {z}" for z in zipf_values],
        horizontal_spacing=0.08
    )

    colors = {"Pipelined": "#2ecc71", "Traditional": "#e74c3c"}

    for col_idx, zipf in enumerate(zipf_values, 1):
        zipf_data = df[df["zipf_exponent"] == zipf]

        # Calculate mean and std for each (baseline, abort_rate) combination
        summary = zipf_data.groupby(["baseline", "abort_rate"]).agg({
            "total_transactions": ["mean", "std"]
        }).reset_index()
        summary.columns = ["baseline", "abort_rate", "total_tx_mean", "total_tx_std"]

        for baseline in ["Pipelined", "Traditional"]:
            data = summary[summary["baseline"] == baseline]

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
                    showlegend=(col_idx == 1),
                ),
                row=1, col=col_idx
            )

        fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=col_idx)
        if col_idx == 1:
            fig.update_yaxes(title_text="Committed Transactions", row=1, col=col_idx)

    fig.update_layout(
        title=dict(
            text="<b>Total Committed Transactions under Varying Abort Rates</b>",
            font=dict(size=20)
        ),
        template="plotly_white",
        width=1400,
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

    fig.write_html(output_dir / "zipf_committed_tx_comparison.html")
    print(f"Saved: {output_dir / 'zipf_committed_tx_comparison.html'}")

    return fig


def create_goodput_comparison(df: pd.DataFrame, output_dir: Path):
    """Create goodput (committed tx / duration) comparison plot.

    Goodput = committed_transactions / total_duration
    This measures how fast commits happen on average.
    """
    zipf_values = sorted(df["zipf_exponent"].unique())

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=[f"Zipf = {z}" for z in zipf_values],
        horizontal_spacing=0.08
    )

    colors = {"Pipelined": "#2ecc71", "Traditional": "#e74c3c"}

    for col_idx, zipf in enumerate(zipf_values, 1):
        zipf_data = df[df["zipf_exponent"] == zipf]

        # Calculate mean and std for each (baseline, abort_rate) combination
        # throughput field is already computed as total_transactions / total_duration
        summary = zipf_data.groupby(["baseline", "abort_rate"]).agg({
            "throughput": ["mean", "std"]
        }).reset_index()
        summary.columns = ["baseline", "abort_rate", "goodput_mean", "goodput_std"]

        for baseline in ["Pipelined", "Traditional"]:
            data = summary[summary["baseline"] == baseline]

            fig.add_trace(
                go.Scatter(
                    x=data["abort_rate"] * 100,
                    y=data["goodput_mean"],
                    error_y=dict(type="data", array=data["goodput_std"]),
                    mode="lines+markers",
                    name=f"{baseline} 2PC",
                    line=dict(color=colors[baseline], width=3),
                    marker=dict(size=10),
                    legendgroup=baseline,
                    showlegend=(col_idx == 1),
                ),
                row=1, col=col_idx
            )

        fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=col_idx)
        if col_idx == 1:
            fig.update_yaxes(title_text="Goodput (committed tx/s)", row=1, col=col_idx)

    fig.update_layout(
        title=dict(
            text="<b>Goodput: Committed Transactions per Second</b>",
            font=dict(size=20)
        ),
        template="plotly_white",
        width=1400,
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

    fig.write_html(output_dir / "zipf_goodput_comparison.html")
    print(f"Saved: {output_dir / 'zipf_goodput_comparison.html'}")

    return fig


def create_latency_comparison(df: pd.DataFrame, output_dir: Path):
    """Create latency comparison plot with 3 subplots (one per Zipf value)."""
    zipf_values = sorted(df["zipf_exponent"].unique())

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=[f"Zipf = {z}" for z in zipf_values],
        horizontal_spacing=0.08
    )

    colors = {"Pipelined": "#2ecc71", "Traditional": "#e74c3c"}

    for col_idx, zipf in enumerate(zipf_values, 1):
        zipf_data = df[df["zipf_exponent"] == zipf]

        summary = zipf_data.groupby(["baseline", "abort_rate"]).agg({
            "avg_latency": ["mean", "std"]
        }).reset_index()
        summary.columns = ["baseline", "abort_rate", "avg_latency_mean", "avg_latency_std"]

        # Convert to milliseconds
        summary["avg_latency_mean"] = summary["avg_latency_mean"] * 1000
        summary["avg_latency_std"] = summary["avg_latency_std"] * 1000

        for baseline in ["Pipelined", "Traditional"]:
            data = summary[summary["baseline"] == baseline]

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
                    showlegend=(col_idx == 1),
                ),
                row=1, col=col_idx
            )

        fig.update_xaxes(title_text="Abort Rate (%)", row=1, col=col_idx)
        if col_idx == 1:
            fig.update_yaxes(title_text="Avg Latency (ms)", row=1, col=col_idx)

    fig.update_layout(
        title=dict(
            text="<b>Pipelined 2PC vs Traditional 2PC: Latency under Varying Abort Rates</b>",
            font=dict(size=20)
        ),
        template="plotly_white",
        width=1400,
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

    fig.write_html(output_dir / "zipf_latency_comparison.html")
    print(f"Saved: {output_dir / 'zipf_latency_comparison.html'}")

    return fig


def create_speedup_comparison(df: pd.DataFrame, output_dir: Path):
    """Create comparison plot showing Pipelined vs Traditional committed transactions ratio."""
    zipf_values = sorted(df["zipf_exponent"].unique())

    fig = go.Figure()

    colors = {0.1: "#3498db", 0.3: "#9b59b6", 0.6: "#e67e22"}

    for zipf in zipf_values:
        zipf_data = df[df["zipf_exponent"] == zipf]

        # Use total_transactions instead of throughput
        summary = zipf_data.groupby(["baseline", "abort_rate"])["total_transactions"].mean().reset_index()
        pivot = summary.pivot(index="abort_rate", columns="baseline", values="total_transactions").reset_index()
        pivot["ratio"] = pivot["Pipelined"] / pivot["Traditional"]

        fig.add_trace(go.Scatter(
            x=pivot["abort_rate"] * 100,
            y=pivot["ratio"],
            mode="lines+markers",
            name=f"Zipf = {zipf}",
            line=dict(color=colors[zipf], width=3),
            marker=dict(size=10),
        ))

    fig.add_hline(y=1.0, line_dash="dash", line_color="gray",
                  annotation_text="Break-even", annotation_position="right")

    fig.update_layout(
        title=dict(
            text="<b>Committed Transactions Ratio: Pipelined / Traditional 2PC</b>",
            font=dict(size=20)
        ),
        xaxis_title="Abort Rate (%)",
        yaxis_title="Ratio (Pipelined / Traditional)",
        template="plotly_white",
        width=900,
        height=500,
        font=dict(size=14),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
    )

    fig.write_html(output_dir / "zipf_ratio_comparison.html")
    print(f"Saved: {output_dir / 'zipf_ratio_comparison.html'}")

    return fig


def create_summary_table(df: pd.DataFrame, output_dir: Path):
    """Create a summary CSV with statistics."""
    summary = df.groupby(["zipf_exponent", "baseline", "abort_rate"]).agg({
        "throughput": ["mean", "std"],
        "total_transactions": ["mean", "std"],
        "avg_latency": ["mean", "std"],
    }).round(4)

    summary.to_csv(output_dir / "zipf_comparison_summary.csv")
    print(f"Saved: {output_dir / 'zipf_comparison_summary.csv'}")

    return summary


def main():
    base_dir = Path(__file__).parent

    # Load all experiment data
    all_data = []

    for zipf, baselines in EXPERIMENTS.items():
        for baseline, exp_name in baselines.items():
            print(f"Loading {baseline} Zipf={zipf} from: {exp_name}")
            df = load_experiment_data(base_dir, exp_name)
            if not df.empty:
                all_data.append(df)
                print(f"  Loaded {len(df)} runs")
            else:
                print(f"  WARNING: No data found!")

    if not all_data:
        print("\nError: No experiment data found!")
        return

    df = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal: {len(df)} runs loaded")

    # Verify data
    print("\nData summary:")
    print(df.groupby(["zipf_exponent", "baseline"]).size())

    # Generate plots
    print("\nGenerating plots...")

    fig0 = create_committed_tx_comparison(df, base_dir)
    fig1 = create_goodput_comparison(df, base_dir)
    fig2 = create_latency_comparison(df, base_dir)
    fig3 = create_speedup_comparison(df, base_dir)
    summary = create_summary_table(df, base_dir)

    # Print committed transactions summary
    print("\n" + "="*60)
    print("COMMITTED TRANSACTIONS SUMMARY")
    print("="*60)

    for zipf in sorted(df["zipf_exponent"].unique()):
        print(f"\nZipf = {zipf}:")
        zipf_data = df[df["zipf_exponent"] == zipf]
        summary_df = zipf_data.groupby(["baseline", "abort_rate"])["total_transactions"].mean().reset_index()
        pivot = summary_df.pivot(index="abort_rate", columns="baseline", values="total_transactions")
        pivot["ratio"] = pivot["Pipelined"] / pivot["Traditional"]

        print(f"  {'Abort%':<10} {'Pipelined':<12} {'Traditional':<12} {'Ratio'}")
        print(f"  {'-'*45}")
        for abort_rate in sorted(pivot.index):
            p = pivot.loc[abort_rate, "Pipelined"]
            t = pivot.loc[abort_rate, "Traditional"]
            r = pivot.loc[abort_rate, "ratio"]
            print(f"  {int(abort_rate*100):<10} {p:<12.0f} {t:<12.0f} {r:.2f}")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
