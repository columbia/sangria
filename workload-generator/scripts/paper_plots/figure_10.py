"""Publication plot for Figure 10: decision-threshold sensitivity."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd

try:
    from .common import COLORS, PAPER_STYLE, load_results, numeric, require_columns, save_figure, style_axis
except ImportError:  # Allow direct execution.
    from common import COLORS, PAPER_STYLE, load_results, numeric, require_columns, save_figure, style_axis


CONTENTION_THRESHOLDS = [180, 100, 50, 20, 0]
RESOLVER_THRESHOLDS = [50, 100, 200, 300, 600]


def _summarize(
    result_directory: Path,
    threshold_column: str,
    threshold_order: list[int],
) -> tuple[pd.DataFrame, dict[str, float]]:
    results = load_results(result_directory)
    require_columns(results, "protocol", "throughput", threshold_column)
    results = results.copy()
    results["throughput"] = numeric(results, "throughput")
    results[threshold_column] = numeric(results, threshold_column)

    adaptive = results[results["protocol"] == "Sangria"].dropna(
        subset=[threshold_column, "throughput"]
    )
    summary = (
        adaptive.groupby(threshold_column, as_index=False)
        .agg(
            throughput=("throughput", "mean"),
            throughput_std=("throughput", "std"),
            samples=("throughput", "count"),
        )
        .set_index(threshold_column)
        .reindex(threshold_order)
        .reset_index()
    )
    missing = summary.loc[summary["throughput"].isna(), threshold_column].tolist()
    if missing:
        raise ValueError(
            f"Missing Sangria measurements for {threshold_column}: {missing}"
        )
    summary["throughput_std"] = summary["throughput_std"].fillna(0.0)

    baselines: dict[str, float] = {}
    for protocol in ("Strict-2PC", "Pipelined-2PC"):
        values = results.loc[results["protocol"] == protocol, "throughput"].dropna()
        if values.empty:
            raise ValueError(f"No throughput measurements found for {protocol}")
        baselines[protocol] = float(values.mean())
    return summary, baselines


def _draw_panel(
    ax,
    summary: pd.DataFrame,
    baselines: dict[str, float],
    thresholds: list[int],
    reference: int,
    xlabel: str,
    title: str,
) -> None:
    positions = np.arange(len(thresholds))
    ax.plot(
        positions,
        summary["throughput"],
        color=COLORS["Sangria"],
        marker="s",
        markersize=4.2,
        markeredgecolor="white",
        markeredgewidth=0.55,
        linewidth=1.8,
        zorder=4,
    )
    ax.axhline(
        baselines["Pipelined-2PC"],
        color=COLORS["Pipelined-2PC"],
        linestyle=":",
        linewidth=1.65,
        zorder=2,
    )
    ax.axhline(
        baselines["Strict-2PC"],
        color=COLORS["Strict-2PC"],
        linestyle="--",
        linewidth=1.35,
        zorder=2,
    )
    reference_position = thresholds.index(reference)
    ax.axvline(
        reference_position,
        color="#666666",
        linestyle="-.",
        linewidth=0.9,
        zorder=1,
    )

    ax.set_xticks(positions, [str(value) for value in thresholds])
    ax.set_xlabel(xlabel)
    ax.set_title(title, pad=4)
    style_axis(ax)
    ax.margins(x=0.04, y=0.12)


def plot(
    contention_directory: Path,
    resolver_directory: Path,
    output_directory: Path,
):
    contention_column = "threshold_overrides/open_clients_low"
    resolver_column = "threshold_overrides/resolver_load_mid"
    contention, contention_baselines = _summarize(
        contention_directory, contention_column, CONTENTION_THRESHOLDS
    )
    resolver, resolver_baselines = _summarize(
        resolver_directory, resolver_column, RESOLVER_THRESHOLDS
    )

    processed_parts = []
    for panel, summary, baselines, threshold_name in (
        ("contention", contention, contention_baselines, "contention_threshold"),
        ("resolver_load", resolver, resolver_baselines, "resolver_load_threshold"),
    ):
        part = summary.rename(columns={summary.columns[0]: "threshold"}).copy()
        part.insert(0, "panel", panel)
        part.insert(2, "threshold_name", threshold_name)
        part["pipelined_throughput"] = baselines["Pipelined-2PC"]
        part["strict_throughput"] = baselines["Strict-2PC"]
        processed_parts.append(part)
    processed = pd.concat(processed_parts, ignore_index=True)

    with plt.rc_context(PAPER_STYLE):
        fig, axes = plt.subplots(1, 2, figsize=(7.15, 2.55))
        _draw_panel(
            axes[0],
            contention,
            contention_baselines,
            CONTENTION_THRESHOLDS,
            50,
            r"Contention threshold $C_L$ (clients)",
            "(a) Contention sensitivity",
        )
        _draw_panel(
            axes[1],
            resolver,
            resolver_baselines,
            RESOLVER_THRESHOLDS,
            200,
            r"Resolver-load threshold $R_M$",
            "(b) Resolver-load sensitivity",
        )
        axes[0].set_ylabel("Throughput (tx/s)")

        legend = [
            Line2D([], [], color=COLORS["Sangria"], marker="s", linewidth=1.8, label="Sangria"),
            Line2D([], [], color=COLORS["Pipelined-2PC"], linestyle=":", linewidth=1.65, label="Pipelined-2PC"),
            Line2D([], [], color=COLORS["Strict-2PC"], linestyle="--", linewidth=1.35, label="Strict-2PC"),
            Line2D([], [], color="#666666", linestyle="-.", linewidth=0.9, label="Reference"),
        ]
        fig.legend(
            handles=legend,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.015),
            ncol=4,
            frameon=False,
            columnspacing=1.25,
            handlelength=2.2,
        )
        fig.tight_layout(rect=(0, 0, 1, 0.89), w_pad=1.35)

        return save_figure(fig, output_directory, "figure_10", processed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contention_directory", type=Path)
    parser.add_argument("resolver_directory", type=Path)
    parser.add_argument("--output-directory", "-o", type=Path)
    args = parser.parse_args()
    output_directory = (
        args.output_directory.expanduser().resolve()
        if args.output_directory
        else args.contention_directory.expanduser().resolve() / "paper_plots"
    )
    for path in plot(
        args.contention_directory.expanduser().resolve(),
        args.resolver_directory.expanduser().resolve(),
        output_directory,
    ):
        print(path)


if __name__ == "__main__":
    main()
