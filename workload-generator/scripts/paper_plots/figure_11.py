"""Publication plot for Figure 11: Resolver commit-batch CDFs."""

from __future__ import annotations

import ast
import json
from pathlib import Path
import re
import warnings

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import numpy as np
import pandas as pd

try:
    from .common import CAPACITY_LABELS, PAPER_STYLE, load_results, numeric, require_columns, save_figure, single_result_cli
except ImportError:  # Allow direct execution.
    from common import CAPACITY_LABELS, PAPER_STYLE, load_results, numeric, require_columns, save_figure, single_result_cli


GROUP_SIZE_KEY = re.compile(r"^Group size: ([1-9][0-9]*)$")
CAPACITY_COLORS = {0: "#0072B2", 100: "#E69F00", 1000: "#D55E00"}
CAPACITY_STYLES = {0: "-", 100: "--", 1000: ":"}
CONCURRENCIES = [1, 5, 25, 50, 100, 200, 500]


def _parse_stats(value: object) -> dict:
    if isinstance(value, dict):
        return value
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return {}
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        try:
            parsed = ast.literal_eval(value)
        except (ValueError, SyntaxError, TypeError):
            return {}
    return parsed if isinstance(parsed, dict) else {}


def _pooled_histogram(rows: pd.DataFrame) -> dict[int, float]:
    histogram: dict[int, float] = {}
    successful_without_groups = []
    for _, row in rows.iterrows():
        stats = _parse_stats(row.get("resolver_stats"))
        row_has_groups = False
        for key, value in stats.items():
            match = GROUP_SIZE_KEY.fullmatch(str(key))
            if match is None:
                continue
            try:
                count = float(value)
            except (TypeError, ValueError):
                continue
            if not np.isfinite(count) or count <= 0:
                continue
            size = int(match.group(1))
            histogram[size] = histogram.get(size, 0.0) + count
            row_has_groups = True

        throughput = pd.to_numeric(pd.Series([row.get("throughput")]), errors="coerce").iloc[0]
        if not row_has_groups and pd.notna(throughput) and throughput > 0:
            successful_without_groups.append(row)

    # At very low concurrency there may be no dependent commit to record.  Such
    # successful runs consist entirely of singleton commits, as in the legacy
    # instrumentation.  Failed/NaN runs are deliberately not synthesized.
    if not histogram and successful_without_groups:
        singles = 0.0
        for row in successful_without_groups:
            for column in ("total_transactions", "num_queries"):
                value = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
                if pd.notna(value) and value > 0:
                    singles += float(value)
                    break
        if singles > 0:
            histogram[1] = singles
    return histogram


def _cdf(histogram: dict[int, float], maximum: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    sizes = np.arange(1, maximum + 1)
    counts = np.array([histogram.get(int(size), 0.0) for size in sizes])
    total = counts.sum()
    if total <= 0:
        raise ValueError("Cannot compute a CDF from an empty batch histogram")
    return sizes, np.cumsum(counts) / total, counts


def _series_for_panel_a(data: pd.DataFrame):
    series = [("Strict-2PC", {1: 1.0}, None, None)]
    for load in (0, 100, 1000):
        rows = data[
            (data["protocol"] == "Pipelined-2PC")
            & (data["concurrency"] == 500)
            & (data["background_load"] == load)
        ]
        histogram = _pooled_histogram(rows)
        if not histogram:
            warnings.warn(f"No valid batch-size data for concurrency=500, Resolver load={load}")
            continue
        label = f"{CAPACITY_LABELS.get(load, load)} capacity"
        series.append((label, histogram, load, 500))
    return series


def _series_for_panel_b(data: pd.DataFrame):
    series = [("Strict-2PC", {1: 1.0}, 0, None)]
    for concurrency in CONCURRENCIES:
        rows = data[
            (data["protocol"] == "Pipelined-2PC")
            & (data["background_load"] == 0)
            & (data["concurrency"] == concurrency)
        ]
        histogram = _pooled_histogram(rows)
        if not histogram:
            warnings.warn(
                f"No valid batch-size data for Resolver load=0, concurrency={concurrency}"
            )
            continue
        series.append((f"{concurrency} clients", histogram, 0, concurrency))
    return series


def _draw_cdfs(ax, series, panel: str) -> list[dict]:
    maximum = max(max(histogram) for _, histogram, _, _ in series)
    records: list[dict] = []
    concurrency_colors = plt.cm.viridis(np.linspace(0.08, 0.88, len(CONCURRENCIES)))
    concurrency_color = dict(zip(CONCURRENCIES, concurrency_colors))

    for label, histogram, load, concurrency in series:
        sizes, values, counts = _cdf(histogram, maximum)
        if label == "Strict-2PC":
            color, linestyle, width = "#333333", "--", 1.25
        elif panel == "capacity":
            color = CAPACITY_COLORS[int(load)]
            linestyle = CAPACITY_STYLES[int(load)]
            width = 1.55
        else:
            color = concurrency_color[int(concurrency)]
            linestyle, width = "-", 1.4
        ax.step(
            sizes,
            values,
            where="post",
            color=color,
            linestyle=linestyle,
            linewidth=width,
            label=label,
        )
        total = float(counts.sum())
        for size, value, count in zip(sizes, values, counts):
            records.append(
                {
                    "panel": panel,
                    "series": label,
                    "background_load": load,
                    "concurrency": concurrency,
                    "batch_size": int(size),
                    "batch_count": float(count),
                    "total_batches": total,
                    "cdf": float(value),
                }
            )

    ax.set_xlim(1, max(2, maximum))
    ax.set_ylim(0, 1.025)
    ax.set_xlabel("Commit-batch size")
    ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=7))
    ax.yaxis.set_major_locator(MaxNLocator(5))
    ax.set_axisbelow(True)
    ax.grid(color="#dddddd", linewidth=0.5, alpha=0.75)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return records


def plot(result_directory: Path, output_directory: Path):
    data = load_results(result_directory)
    require_columns(
        data,
        "protocol",
        "resolver_stats",
        "max_concurrency",
        "resolver_tx_load_concurrency",
        "throughput",
    )
    data = data.copy()
    data["concurrency"] = numeric(data, "max_concurrency")
    data["background_load"] = numeric(data, "resolver_tx_load_concurrency")

    panel_a = _series_for_panel_a(data)
    panel_b = _series_for_panel_b(data)
    if len(panel_a) == 1 or len(panel_b) == 1:
        raise ValueError("No Pipelined-2PC batch-size measurements were available")

    with plt.rc_context(PAPER_STYLE):
        fig, axes = plt.subplots(1, 2, figsize=(7.15, 2.8), sharey=True)
        records = _draw_cdfs(axes[0], panel_a, "capacity")
        records.extend(_draw_cdfs(axes[1], panel_b, "contention"))
        axes[0].set_ylabel("Cumulative fraction")
        axes[0].set_title("(a) Varying Resolver capacity", pad=4)
        axes[1].set_title("(b) Varying contention", pad=4)
        axes[0].legend(frameon=False, loc="lower right", handlelength=2.3)
        axes[1].legend(
            frameon=False,
            loc="lower right",
            ncol=2,
            columnspacing=0.9,
            handlelength=1.8,
            fontsize=6.8,
        )
        fig.tight_layout(w_pad=1.2)

        processed = pd.DataFrame.from_records(records)
        return save_figure(fig, output_directory, "figure_11", processed)


if __name__ == "__main__":
    single_result_cli(plot, __doc__)
