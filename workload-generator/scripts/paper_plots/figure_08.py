#!/usr/bin/env python3
"""Figure 8: throughput while Resolver capacity changes at runtime."""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

try:
    from .common import (
        PAPER_STYLE,
        PROTOCOL_ORDER,
        grouped_bar,
        load_results,
        protocol_legend_handles,
        require_columns,
        save_figure,
        single_result_cli,
    )
except ImportError:
    from common import (
        PAPER_STYLE,
        PROTOCOL_ORDER,
        grouped_bar,
        load_results,
        protocol_legend_handles,
        require_columns,
        save_figure,
        single_result_cli,
    )


CONCURRENCY_ORDER = (5, 50, 500)


def _summarize(results):
    frame = results[["protocol", "max_concurrency", "throughput"]].copy()
    frame["concurrency"] = pd.to_numeric(frame["max_concurrency"], errors="coerce")
    frame["throughput"] = pd.to_numeric(frame["throughput"], errors="coerce")
    frame = frame.dropna(subset=["concurrency", "throughput"])
    frame["concurrency"] = frame["concurrency"].astype(int)
    frame = frame[frame["concurrency"].isin(CONCURRENCY_ORDER)]

    return (
        frame.groupby(["protocol", "concurrency"], as_index=False, sort=False)
        .agg(
            throughput_mean=("throughput", "mean"),
            throughput_std=("throughput", "std"),
            samples=("throughput", "size"),
        )
    )


def plot(result_directory: Path, output_directory: Path):
    results = load_results(result_directory)
    require_columns(results, "protocol", "throughput", "max_concurrency")
    summary = _summarize(results)

    expected = {
        (protocol, concurrency)
        for protocol in PROTOCOL_ORDER
        for concurrency in CONCURRENCY_ORDER
    }
    observed = set(zip(summary["protocol"], summary["concurrency"]))
    missing = sorted(expected - observed)
    if missing:
        raise ValueError(f"Figure 8 is missing protocol/concurrency combinations: {missing}")

    protocol_rank = {protocol: i for i, protocol in enumerate(PROTOCOL_ORDER)}
    concurrency_rank = {value: i for i, value in enumerate(CONCURRENCY_ORDER)}
    summary["_protocol_rank"] = summary["protocol"].map(protocol_rank)
    summary["_concurrency_rank"] = summary["concurrency"].map(concurrency_rank)
    summary = (
        summary.sort_values(["_concurrency_rank", "_protocol_rank"])
        .drop(columns=["_protocol_rank", "_concurrency_rank"])
        .reset_index(drop=True)
    )

    with plt.rc_context(PAPER_STYLE):
        fig, ax = plt.subplots(figsize=(3.45, 2.2))
        grouped_bar(
            ax,
            summary,
            "concurrency",
            "throughput_mean",
            error_column="throughput_std",
            x_values=CONCURRENCY_ORDER,
        )
        ax.set_xlabel("Concurrency level")
        ax.set_ylabel("Throughput (tx/s)")
        ax.set_ylim(bottom=0)
        ax.legend(
            handles=protocol_legend_handles(),
            loc="lower center",
            bbox_to_anchor=(0.5, 1.01),
            ncol=3,
            frameon=False,
            columnspacing=0.9,
            handletextpad=0.4,
        )
        fig.subplots_adjust(left=0.19, right=0.99, bottom=0.23, top=0.80)

        output_directory = Path(output_directory)
        output_directory.mkdir(parents=True, exist_ok=True)
        return save_figure(fig, output_directory, "figure_08", summary)


if __name__ == "__main__":
    single_result_cli(
        plot, "Generate paper Figure 8 from a runtime-resolver result directory."
    )
