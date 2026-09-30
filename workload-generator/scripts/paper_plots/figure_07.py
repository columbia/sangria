#!/usr/bin/env python3
"""Figure 7: throughput while foreground contention changes at runtime."""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

try:
    from .common import (
        CAPACITY_LABELS,
        PAPER_STYLE,
        PROTOCOL_ORDER,
        grouped_bar,
        load_results,
        numeric,
        protocol_legend_handles,
        require_columns,
        save_figure,
        single_result_cli,
    )
except ImportError:
    from common import (
        CAPACITY_LABELS,
        PAPER_STYLE,
        PROTOCOL_ORDER,
        grouped_bar,
        load_results,
        numeric,
        protocol_legend_handles,
        require_columns,
        save_figure,
        single_result_cli,
    )


LOAD_ORDER = (0, 100, 1000)


def _capacity_label(load):
    return CAPACITY_LABELS.get(load, CAPACITY_LABELS.get(str(load), str(load)))


def _summarize(results):
    """Aggregate by capacity, treating Strict-2PC's load as irrelevant."""

    frame = results[["protocol", "resolver_tx_load_concurrency", "throughput"]].copy()
    frame["resolver_tx_load_concurrency"] = numeric(
        frame, "resolver_tx_load_concurrency"
    )
    frame["throughput"] = numeric(frame, "throughput")
    frame = frame.dropna(subset=["resolver_tx_load_concurrency", "throughput"])

    non_strict = frame[
        (frame["protocol"] != "Strict-2PC")
        & frame["resolver_tx_load_concurrency"].isin(LOAD_ORDER)
    ]
    non_strict = (
        non_strict.groupby(
            ["protocol", "resolver_tx_load_concurrency"], as_index=False
        )
        .agg(
            throughput_mean=("throughput", "mean"),
            throughput_std=("throughput", "std"),
            samples=("throughput", "size"),
        )
    )

    strict_values = frame.loc[frame["protocol"] == "Strict-2PC", "throughput"]
    if strict_values.empty:
        strict = pd.DataFrame(columns=non_strict.columns)
    else:
        strict = pd.DataFrame(
            {
                "protocol": ["Strict-2PC"] * len(LOAD_ORDER),
                "resolver_tx_load_concurrency": LOAD_ORDER,
                "throughput_mean": [strict_values.mean()] * len(LOAD_ORDER),
                "throughput_std": [strict_values.std()] * len(LOAD_ORDER),
                "samples": [strict_values.size] * len(LOAD_ORDER),
            }
        )
    summary = pd.concat([non_strict, strict], ignore_index=True)
    summary["throughput_std"] = summary["throughput_std"].fillna(0.0)
    return summary


def plot(result_directory: Path, output_directory: Path):
    results = load_results(result_directory)
    require_columns(
        results, "protocol", "throughput", "resolver_tx_load_concurrency"
    )
    summary = _summarize(results)

    expected = {(protocol, load) for protocol in PROTOCOL_ORDER for load in LOAD_ORDER}
    observed = set(zip(summary["protocol"], summary["resolver_tx_load_concurrency"]))
    missing = sorted(expected - observed)
    if missing:
        raise ValueError(f"Figure 7 is missing protocol/load combinations: {missing}")

    summary["resolver_capacity"] = summary["resolver_tx_load_concurrency"].map(
        _capacity_label
    )
    load_rank = {load: i for i, load in enumerate(LOAD_ORDER)}
    protocol_rank = {protocol: i for i, protocol in enumerate(PROTOCOL_ORDER)}
    summary["_load_rank"] = summary["resolver_tx_load_concurrency"].map(load_rank)
    summary["_protocol_rank"] = summary["protocol"].map(protocol_rank)
    summary = (
        summary.sort_values(["_load_rank", "_protocol_rank"])
        .drop(columns=["_load_rank", "_protocol_rank"])
        .reset_index(drop=True)
    )

    with plt.rc_context(PAPER_STYLE):
        fig, ax = plt.subplots(figsize=(3.45, 2.2))
        grouped_bar(
            ax,
            summary,
            "resolver_tx_load_concurrency",
            "throughput_mean",
            error_column="throughput_std",
            x_values=LOAD_ORDER,
        )
        ax.set_xticks(range(len(LOAD_ORDER)), [_capacity_label(load) for load in LOAD_ORDER])
        ax.set_xlabel("Resolver capacity")
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
        return save_figure(fig, output_directory, "figure_07", summary)


if __name__ == "__main__":
    single_result_cli(
        plot, "Generate paper Figure 7 from a runtime-contention result directory."
    )
