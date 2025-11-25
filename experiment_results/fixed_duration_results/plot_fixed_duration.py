#!/usr/bin/env python3
"""
Plot Pipelined 2PC vs Traditional 2PC comparison using fixed-duration experiment results.
This uses data from 60-second fixed duration runs to ensure fair comparison.
"""

import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# Experiment directories for each Zipf value
EXPERIMENTS = {
    0.1: {
        "Pipelined": "pipelined_zipf_0.1_fixed_therapeutic_giraffe",
        "Traditional": "traditional_zipf_0.1_fixed_successful_husky",
    },
    0.3: {
        "Pipelined": "pipelined_zipf_0.3_fixed_inventive_eagle",
        "Traditional": "traditional_zipf_0.3_fixed_sage_bird",
    },
    0.6: {
        "Pipelined": "pipelined_zipf_0.6_fixed_ruby_chinchilla",
        "Traditional": "traditional_zipf_0.6_fixed_whimsical_quetzal",
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
                "avg_latency": data.get("avg_latency", 0),
                "p50_latency": data.get("p50_latency", 0),
                "p95_latency": data.get("p95_latency", 0),
                "p99_latency": data.get("p99_latency", 0),
                "total_duration": data.get("total_duration", 60),
                "iteration": data["config"].get("iteration", 0),
            })

    return pd.DataFrame(records)


def create_goodput_comparison(df: pd.DataFrame, output_dir: Path):
    """Create goodput (committed tx/s) comparison plot with 3 subplots (one per Zipf value)."""
    zipf_values = sorted(df["zipf_exponent"].unique())

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=[f"Zipf = {z}" for z in zipf_values],
        horizontal_spacing=0.08
    )

    colors = {"Pipelined": "#2ecc71", "Traditional": "#e74c3c"}

    for col_idx, zipf in enumerate(zipf_values, 1):
        zipf_data = df[df["zipf_exponent"] == zipf]

        # Calculate mean for each (baseline, abort_rate) combination
        summary = zipf_data.groupby(["baseline", "abort_rate"]).agg({
            "throughput": ["mean"]
        }).reset_index()
        summary.columns = ["baseline", "abort_rate", "goodput_mean"]

        for baseline in ["Pipelined", "Traditional"]:
            data = summary[summary["baseline"] == baseline]

            fig.add_trace(
                go.Scatter(
                    x=data["abort_rate"] * 100,
                    y=data["goodput_mean"],
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
            text="<b>Goodput: Committed Transactions per Second (Fixed 60s Duration)</b>",
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

    fig.write_html(output_dir / "fixed_duration_goodput_comparison.html")
    print(f"Saved: {output_dir / 'fixed_duration_goodput_comparison.html'}")

    return fig


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

        # Calculate mean for each (baseline, abort_rate) combination
        summary = zipf_data.groupby(["baseline", "abort_rate"]).agg({
            "total_transactions": ["mean"]
        }).reset_index()
        summary.columns = ["baseline", "abort_rate", "total_tx_mean"]

        for baseline in ["Pipelined", "Traditional"]:
            data = summary[summary["baseline"] == baseline]

            fig.add_trace(
                go.Scatter(
                    x=data["abort_rate"] * 100,
                    y=data["total_tx_mean"],
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
            text="<b>Total Committed Transactions in 60 Seconds</b>",
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

    fig.write_html(output_dir / "fixed_duration_committed_tx_comparison.html")
    print(f"Saved: {output_dir / 'fixed_duration_committed_tx_comparison.html'}")

    return fig


def create_speedup_comparison(df: pd.DataFrame, output_dir: Path):
    """Create comparison plot showing Pipelined vs Traditional goodput ratio."""
    zipf_values = sorted(df["zipf_exponent"].unique())

    fig = go.Figure()

    colors = {0.1: "#3498db", 0.3: "#9b59b6", 0.6: "#e67e22"}

    for zipf in zipf_values:
        zipf_data = df[df["zipf_exponent"] == zipf]

        # Use throughput (goodput) instead of total_transactions
        summary = zipf_data.groupby(["baseline", "abort_rate"])["throughput"].mean().reset_index()
        pivot = summary.pivot(index="abort_rate", columns="baseline", values="throughput").reset_index()
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
            text="<b>Goodput Speedup: Pipelined / Traditional 2PC</b>",
            font=dict(size=20)
        ),
        xaxis_title="Abort Rate (%)",
        yaxis_title="Speedup Ratio (Pipelined / Traditional)",
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

    fig.write_html(output_dir / "fixed_duration_speedup_comparison.html")
    print(f"Saved: {output_dir / 'fixed_duration_speedup_comparison.html'}")

    return fig


def create_summary_table(df: pd.DataFrame, output_dir: Path):
    """Create a summary CSV with statistics."""
    summary = df.groupby(["zipf_exponent", "baseline", "abort_rate"]).agg({
        "throughput": ["mean"],
        "total_transactions": ["mean"],
        "avg_latency": ["mean"],
    }).round(4)

    summary.to_csv(output_dir / "fixed_duration_summary.csv")
    print(f"Saved: {output_dir / 'fixed_duration_summary.csv'}")

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

    fig1 = create_goodput_comparison(df, base_dir)
    fig2 = create_committed_tx_comparison(df, base_dir)
    fig3 = create_speedup_comparison(df, base_dir)
    summary = create_summary_table(df, base_dir)

    # Print goodput summary
    print("\n" + "="*80)
    print("GOODPUT SUMMARY (Fixed 60s Duration)")
    print("="*80)

    for zipf in sorted(df["zipf_exponent"].unique()):
        print(f"\nZipf = {zipf}:")
        zipf_data = df[df["zipf_exponent"] == zipf]
        summary_df = zipf_data.groupby(["baseline", "abort_rate"])["throughput"].mean().reset_index()
        pivot = summary_df.pivot(index="abort_rate", columns="baseline", values="throughput")
        pivot["ratio"] = pivot["Pipelined"] / pivot["Traditional"]

        print(f"  {'Abort%':<10} {'Pipelined':<15} {'Traditional':<15} {'Speedup'}")
        print(f"  {'-'*55}")
        for abort_rate in sorted(pivot.index):
            p = pivot.loc[abort_rate, "Pipelined"]
            t = pivot.loc[abort_rate, "Traditional"]
            r = pivot.loc[abort_rate, "ratio"]
            print(f"  {int(abort_rate*100):<10} {p:<15.2f} {t:<15.2f} {r:.2f}x")

    print("\n" + "="*80)


if __name__ == "__main__":
    main()
