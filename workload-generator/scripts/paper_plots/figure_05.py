"""Figure 5: YCSB skew sweep at three Resolver capacities."""

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
except ImportError:  # Allow ``python figure_05.py ...``.
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
        x_column="zipf_exponent",
        metrics=["throughput"],
        loads=LOADS,
    )
    zipf_values = sorted(summary["zipf_exponent"].unique())

    with plt.rc_context(PAPER_STYLE):
        fig, axes = plt.subplots(3, 1, figsize=(3.45, 5.3), sharex=True, sharey=True)
        for index, (ax, load) in enumerate(zip(axes, LOADS)):
            panel = summary[summary["resolver_tx_load_concurrency"] == load]
            grouped_bar(
                ax,
                panel,
                "zipf_exponent",
                "throughput_mean",
                error_column="throughput_std",
                x_values=zipf_values,
            )
            letter = chr(ord("a") + index)
            ax.set_title(
                f"({letter}) {CAPACITY_LABELS[load]} Resolver capacity "
                f"({load} background clients)",
                loc="left",
                pad=3,
            )

        fig.text(0.015, 0.5, "Throughput (tx/s)", rotation=90, va="center", fontsize=8.5)
        fig.supxlabel("Zipf exponent", y=0.02, fontsize=8.5)
        fig.legend(
            handles=protocol_legend_handles(),
            loc="upper center",
            bbox_to_anchor=(0.5, 0.992),
            ncol=3,
            frameon=False,
            columnspacing=0.8,
            handlelength=1.5,
        )
        fig.subplots_adjust(left=0.19, right=0.985, bottom=0.11, top=0.90, hspace=0.36)
        return save_figure(fig, output_directory, "figure_05", summary)


if __name__ == "__main__":
    single_result_cli(plot, "Generate paper Figure 5 from a YCSB result directory.")

