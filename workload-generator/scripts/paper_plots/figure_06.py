"""Figure 6: centralization cost versus early-lock-release benefit."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    from .common import (
        CAPACITY_LABELS,
        COLORS,
        PAPER_STYLE,
        aggregate_with_strict_expanded,
        grouped_bar,
        load_results,
        protocol_legend_handles,
        save_figure,
        single_result_cli,
        style_axis,
    )
except ImportError:  # Allow ``python figure_06.py ...``.
    from common import (
        CAPACITY_LABELS,
        COLORS,
        PAPER_STYLE,
        aggregate_with_strict_expanded,
        grouped_bar,
        load_results,
        protocol_legend_handles,
        save_figure,
        single_result_cli,
        style_axis,
    )


LOADS = (0, 100, 1000)
CAPACITY_COLORS = {0: COLORS["Sangria"], 100: "#ff7f0e", 1000: COLORS["Pipelined-2PC"]}
CAPACITY_MARKERS = {0: "s", 100: "^", 1000: "o"}


def _processed_data(summary: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    throughput = summary.pivot(
        index=["resolver_tx_load_concurrency", "max_concurrency"],
        columns="protocol",
        values="throughput_mean",
    ).reset_index()
    required = {"Pipelined-2PC", "Strict-2PC"}
    if not required.issubset(throughput.columns):
        missing = ", ".join(sorted(required - set(throughput.columns)))
        raise ValueError(f"Cannot compute Figure 6 ratio; missing protocols: {missing}")
    ratio = throughput[
        ["resolver_tx_load_concurrency", "max_concurrency"]
    ].copy()
    ratio["throughput_ratio"] = (
        throughput["Pipelined-2PC"] / throughput["Strict-2PC"]
    )
    ratio["panel"] = "throughput_ratio"
    ratio["capacity"] = ratio["resolver_tx_load_concurrency"].map(CAPACITY_LABELS)

    latency = summary[summary["resolver_tx_load_concurrency"] == 1000][
        [
            "resolver_tx_load_concurrency",
            "max_concurrency",
            "protocol",
            "avg_latency_mean",
            "avg_latency_std",
            "samples",
        ]
    ].copy()
    latency["mean_latency_ms"] = latency["avg_latency_mean"] * 1000.0
    latency["latency_std_ms"] = latency["avg_latency_std"] * 1000.0
    latency["panel"] = "mean_latency"
    latency["capacity"] = "Low"

    ratio_export = ratio.rename(columns={"throughput_ratio": "value"})
    ratio_export["protocol"] = "Pipelined-2PC / Strict-2PC"
    ratio_export["error"] = np.nan
    ratio_export["samples"] = np.nan
    latency_export = latency.rename(
        columns={"mean_latency_ms": "value", "latency_std_ms": "error"}
    )
    columns = [
        "panel",
        "capacity",
        "resolver_tx_load_concurrency",
        "max_concurrency",
        "protocol",
        "value",
        "error",
        "samples",
    ]
    processed = pd.concat(
        [ratio_export[columns], latency_export[columns]], ignore_index=True
    )
    return ratio, latency, processed


def plot(result_directory: Path, output_directory: Path):
    results = load_results(result_directory)
    summary = aggregate_with_strict_expanded(
        results,
        x_column="max_concurrency",
        metrics=["throughput", "avg_latency"],
        loads=LOADS,
    )
    ratio, latency, processed = _processed_data(summary)
    concurrency = sorted(summary["max_concurrency"].unique())
    positions = np.arange(len(concurrency), dtype=float)

    with plt.rc_context(PAPER_STYLE):
        fig, axes = plt.subplots(1, 2, figsize=(7.1, 2.5))

        ratio_ax = axes[0]
        for load in LOADS:
            line = ratio[ratio["resolver_tx_load_concurrency"] == load].set_index(
                "max_concurrency"
            )
            values = line["throughput_ratio"].reindex(concurrency)
            ratio_ax.plot(
                positions,
                values,
                color=CAPACITY_COLORS[load],
                marker=CAPACITY_MARKERS[load],
                markersize=3.6,
                linewidth=1.25,
                label=f"{CAPACITY_LABELS[load]} (load {load})",
                zorder=3,
            )
        ratio_ax.axhline(
            1.0,
            color="#333333",
            linestyle="--",
            linewidth=0.9,
            label="Equal throughput",
            zorder=2,
        )
        ratio_ax.set_xticks(positions, [f"{value:g}" for value in concurrency])
        ratio_ax.set_ylabel("Pipelined / Strict throughput")
        ratio_ax.set_title("(a) Early-lock-release benefit", pad=3)
        style_axis(ratio_ax, comma_y=False)
        ratio_ax.legend(
            loc="lower center",
            bbox_to_anchor=(0.5, 1.04),
            ncol=2,
            frameon=False,
            columnspacing=0.9,
            handlelength=1.8,
        )

        latency_ax = axes[1]
        grouped_bar(
            latency_ax,
            latency,
            "max_concurrency",
            "mean_latency_ms",
            error_column="latency_std_ms",
            x_values=concurrency,
        )
        latency_ax.set_ylabel("Mean latency (ms)")
        latency_ax.set_title("(b) Low Resolver capacity", pad=3)
        latency_ax.legend(
            handles=protocol_legend_handles(),
            loc="lower center",
            bbox_to_anchor=(0.5, 1.04),
            ncol=3,
            frameon=False,
            columnspacing=0.75,
            handlelength=1.45,
        )

        fig.supxlabel("Concurrency level", y=0.015, fontsize=8.5)
        fig.subplots_adjust(left=0.09, right=0.995, bottom=0.21, top=0.70, wspace=0.31)
        return save_figure(fig, output_directory, "figure_06", processed)


if __name__ == "__main__":
    single_result_cli(plot, "Generate paper Figure 6 from a Q1 result directory.")

