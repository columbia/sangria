"""Figure 4: contention versus Resolver capacity."""

from pathlib import Path

import matplotlib.pyplot as plt

try:
    from .common import (
        CAPACITY_LABELS,
        PAPER_STYLE,
        aggregate_with_strict_expanded,
        grouped_bar,
        load_results,
        protocol_legend_handles,
        save_figure,
        single_result_cli,
    )
except ImportError:  # Allow ``python figure_04.py ...``.
    from common import (
        CAPACITY_LABELS,
        PAPER_STYLE,
        aggregate_with_strict_expanded,
        grouped_bar,
        load_results,
        protocol_legend_handles,
        save_figure,
        single_result_cli,
    )


LOADS = (0, 100, 1000)


def plot(result_directory: Path, output_directory: Path):
    results = load_results(result_directory)
    summary = aggregate_with_strict_expanded(
        results,
        x_column="max_concurrency",
        metrics=["throughput"],
        loads=LOADS,
    )
    concurrency = sorted(summary["max_concurrency"].unique())

    with plt.rc_context(PAPER_STYLE):
        fig, axes = plt.subplots(1, 3, figsize=(7.1, 2.35), sharey=True)
        for index, (ax, load) in enumerate(zip(axes, LOADS)):
            panel = summary[summary["resolver_tx_load_concurrency"] == load]
            grouped_bar(
                ax,
                panel,
                "max_concurrency",
                "throughput_mean",
                error_column="throughput_std",
                x_values=concurrency,
            )
            letter = chr(ord("a") + index)
            ax.set_title(
                f"({letter}) {CAPACITY_LABELS[load]} Resolver capacity\n"
                f"({load} background clients)",
                pad=3,
            )
            if index == 0:
                ax.set_ylabel("Throughput (tx/s)")

        fig.supxlabel("Concurrency level", y=0.015, fontsize=8.5)
        fig.legend(
            handles=protocol_legend_handles(),
            loc="upper center",
            bbox_to_anchor=(0.5, 0.995),
            ncol=3,
            frameon=False,
            columnspacing=1.4,
            handlelength=1.8,
        )
        fig.subplots_adjust(left=0.075, right=0.995, bottom=0.22, top=0.72, wspace=0.16)
        return save_figure(fig, output_directory, "figure_04", summary)


if __name__ == "__main__":
    single_result_cli(plot, "Generate paper Figure 4 from a Q1 result directory.")

