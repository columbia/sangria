"""Shared data loading and styling for the paper-specific figures."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, Iterable, Sequence

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd


PROTOCOL_ORDER = ["Sangria", "Pipelined-2PC", "Strict-2PC"]
COLORS = {
    "Sangria": "#2ca02c",
    "Pipelined-2PC": "#d62728",
    "Strict-2PC": "#1f77b4",
}
HATCHES = {
    "Sangria": "///",
    "Pipelined-2PC": "xxx",
    "Strict-2PC": "...",
}
CAPACITY_LABELS = {
    0: "High",
    100: "Medium",
    1000: "Low",
}

PAPER_STYLE = {
    "font.family": "sans-serif",
    "font.size": 8,
    "axes.labelsize": 8.5,
    "axes.titlesize": 9,
    "xtick.labelsize": 7.5,
    "ytick.labelsize": 7.5,
    "legend.fontsize": 7.5,
    "axes.linewidth": 0.6,
    "xtick.major.width": 0.6,
    "ytick.major.width": 0.6,
    "xtick.major.size": 3,
    "ytick.major.size": 3,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
}


def _canonical_protocol(value: object) -> str:
    text = str(value).strip()
    key = text.lower().replace("_", "").replace("-", "").replace(" ", "")
    aliases = {
        "adaptive": "Sangria",
        "sangria": "Sangria",
        "pipelined": "Pipelined-2PC",
        "pipelined2pc": "Pipelined-2PC",
        "traditional": "Strict-2PC",
        "strict": "Strict-2PC",
        "strict2pc": "Strict-2PC",
    }
    return aliases.get(key, text)


def load_results(result_directory: Path) -> pd.DataFrame:
    """Read and normalize all Ray ``*_results.csv`` files in a run directory."""

    result_directory = Path(result_directory)
    files = sorted(result_directory.glob("*_results.csv"))
    if not files:
        raise FileNotFoundError(
            f"No *_results.csv files found in {result_directory}"
        )

    frames = []
    for path in files:
        frame = pd.read_csv(path)
        renames = {}
        for column in frame.columns:
            if column.startswith("config/"):
                short_name = column[len("config/") :]
                if short_name not in frame.columns:
                    renames[column] = short_name
        frame = frame.rename(columns=renames)

        fallback_baseline = path.name[: -len("_results.csv")]
        if "baseline" not in frame.columns:
            frame["baseline"] = fallback_baseline
        else:
            frame["baseline"] = frame["baseline"].fillna(fallback_baseline)
        frame["source_file"] = path.name
        frames.append(frame)

    results = pd.concat(frames, ignore_index=True, sort=False)
    results["protocol"] = results["baseline"].map(_canonical_protocol)
    if {"throughput", "total_transactions"}.issubset(results.columns):
        throughput = pd.to_numeric(results["throughput"], errors="coerce")
        transactions = pd.to_numeric(
            results["total_transactions"], errors="coerce"
        )
        invalid = results[(throughput == 0) & transactions.isna()]
        if not invalid.empty:
            trials = ", ".join(
                invalid.get("trial_id", invalid.index.astype(str))
                .astype(str)
                .tolist()
            )
            raise ValueError(
                "Invalid zero-throughput measurements without workload metrics: "
                f"{trials}"
            )
    return results


def require_columns(df: pd.DataFrame, *columns: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError("Missing result columns: " + ", ".join(missing))


def numeric(df: pd.DataFrame, column: str) -> pd.Series:
    """Return a numeric view of a result column, coercing invalid values to NaN."""

    require_columns(df, column)
    return pd.to_numeric(df[column], errors="coerce")


def _aggregate(data: pd.DataFrame, keys: Sequence[str], metrics: Sequence[str]):
    aggregations = {}
    for metric in metrics:
        aggregations[f"{metric}_mean"] = (metric, "mean")
        aggregations[f"{metric}_std"] = (metric, "std")
    aggregations["samples"] = (metrics[0], "count")
    result = data.groupby(list(keys), as_index=False).agg(**aggregations)
    for metric in metrics:
        result[f"{metric}_std"] = result[f"{metric}_std"].fillna(0.0)
    return result


def aggregate_with_strict_expanded(
    df: pd.DataFrame,
    x_column: str,
    metrics: Sequence[str],
    loads: Iterable[int] = (0, 100, 1000),
) -> pd.DataFrame:
    """Aggregate a capacity sweep and repeat Strict-2PC at every capacity.

    Strict-2PC does not use the Resolver. Its repeated grid points are therefore
    treated as repetitions of one measurement and their aggregate is shown in
    every Resolver-capacity panel.
    """

    load_column = "resolver_tx_load_concurrency"
    require_columns(df, "protocol", load_column, x_column, *metrics)
    data = df.copy()
    data[load_column] = numeric(data, load_column)
    data[x_column] = numeric(data, x_column)
    for metric in metrics:
        data[metric] = numeric(data, metric)
    data = data[data["protocol"].isin(PROTOCOL_ORDER)]
    data = data.dropna(subset=[load_column, x_column, *metrics])

    selected_loads = list(loads)
    non_strict = data[
        (data["protocol"] != "Strict-2PC")
        & data[load_column].isin(selected_loads)
    ]
    non_strict_summary = _aggregate(
        non_strict, ["protocol", load_column, x_column], metrics
    )

    strict = data[data["protocol"] == "Strict-2PC"]
    strict_summary = _aggregate(strict, ["protocol", x_column], metrics)
    strict_copies = []
    for load in selected_loads:
        copy = strict_summary.copy()
        copy[load_column] = load
        strict_copies.append(copy)

    pieces = [non_strict_summary, *strict_copies]
    summary = pd.concat(pieces, ignore_index=True, sort=False)
    order = {protocol: index for index, protocol in enumerate(PROTOCOL_ORDER)}
    summary["_protocol_order"] = summary["protocol"].map(order)
    summary = summary.sort_values(
        [load_column, x_column, "_protocol_order"]
    ).drop(columns="_protocol_order")
    return summary.reset_index(drop=True)


def _display_number(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:g}"


def style_axis(ax, *, comma_y: bool = True) -> None:
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#d9d9d9", linewidth=0.55, alpha=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    if comma_y:
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:,.0f}"))
    ax.margins(y=0.08)


def protocol_legend_handles() -> list[Patch]:
    return [
        Patch(
            facecolor=COLORS[protocol],
            edgecolor="#333333",
            linewidth=0.45,
            hatch=HATCHES[protocol],
            label=protocol,
        )
        for protocol in PROTOCOL_ORDER
    ]


def grouped_bar(
    ax,
    data: pd.DataFrame,
    x_column: str,
    y_column: str,
    *,
    error_column: str | None = None,
    x_values: Sequence[object] | None = None,
    legend: bool = False,
) -> None:
    """Draw consistently ordered and styled grouped protocol bars."""

    if x_values is None:
        x_values = sorted(data[x_column].dropna().unique())
    x_values = list(x_values)
    positions = np.arange(len(x_values), dtype=float)
    width = 0.24

    for index, protocol in enumerate(PROTOCOL_ORDER):
        protocol_data = data[data["protocol"] == protocol].set_index(x_column)
        values = protocol_data[y_column].reindex(x_values).to_numpy(dtype=float)
        errors = None
        if error_column is not None and error_column in protocol_data:
            errors = protocol_data[error_column].reindex(x_values).to_numpy(dtype=float)
            errors = np.nan_to_num(errors, nan=0.0)
        offset = (index - (len(PROTOCOL_ORDER) - 1) / 2) * width
        ax.bar(
            positions + offset,
            values,
            width=width,
            yerr=errors,
            capsize=1.5 if errors is not None else 0,
            error_kw={"elinewidth": 0.55, "capthick": 0.55},
            color=COLORS[protocol],
            edgecolor="#333333",
            linewidth=0.45,
            hatch=HATCHES[protocol],
            label=protocol if legend else "_nolegend_",
            zorder=3,
        )

    ax.set_xticks(positions, [_display_number(value) for value in x_values])
    style_axis(ax)
    if legend:
        ax.legend(frameon=False, ncol=3)


def save_figure(
    fig,
    output_directory: Path,
    stem: str,
    processed_df: pd.DataFrame,
) -> list[Path]:
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    pdf_path = output_directory / f"{stem}.pdf"
    png_path = output_directory / f"{stem}.png"
    csv_path = output_directory / f"{stem}.csv"
    fig.savefig(pdf_path, facecolor="white")
    fig.savefig(png_path, dpi=300, facecolor="white")
    processed_df.to_csv(csv_path, index=False)
    plt.close(fig)
    return [pdf_path, png_path, csv_path]


def single_result_cli(
    plot_function: Callable[[Path, Path], object], description: str
) -> None:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "result_directory",
        type=Path,
        help="Ray result directory containing *_results.csv files.",
    )
    parser.add_argument(
        "--output-directory",
        "-o",
        type=Path,
        help="Destination (default: RESULT_DIRECTORY/paper_plots).",
    )
    args = parser.parse_args()
    result_directory = args.result_directory.expanduser().resolve()
    output_directory = (
        args.output_directory.expanduser().resolve()
        if args.output_directory
        else result_directory / "paper_plots"
    )
    outputs = plot_function(result_directory, output_directory)
    if outputs:
        for path in outputs:
            print(path)
