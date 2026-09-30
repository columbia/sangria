"""Generate the publication table for the hotspot dependency stress test."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    from .common import PAPER_STYLE, load_results, numeric, require_columns, save_figure, single_result_cli
except ImportError:  # Allow direct execution.
    from common import PAPER_STYLE, load_results, numeric, require_columns, save_figure, single_result_cli


TABLE_ORDER = ["Strict-2PC", "Pipelined-2PC", "Sangria"]
STATISTICS = (
    "dependency_depth_p50",
    "dependency_depth_p95",
    "dependency_depth_max",
    "max_resolver_queue",
    "max_participant_batch",
)


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


def _finite_stat(stats: dict, name: str) -> float:
    try:
        value = float(stats.get(name, np.nan))
    except (TypeError, ValueError):
        return np.nan
    return value if np.isfinite(value) else np.nan


def _summary(data: pd.DataFrame) -> pd.DataFrame:
    require_columns(data, "protocol", "throughput", "avg_latency", "p99_latency", "resolver_stats")
    data = data.copy()
    for metric in ("throughput", "avg_latency", "p99_latency"):
        data[metric] = numeric(data, metric)
    parsed = data["resolver_stats"].map(_parse_stats)
    for statistic in STATISTICS:
        data[statistic] = parsed.map(lambda value, key=statistic: _finite_stat(value, key))

    records = []
    for protocol in TABLE_ORDER:
        rows = data[data["protocol"] == protocol]
        if rows.empty:
            raise ValueError(f"No measurements found for {protocol}")
        record = {
            "protocol": protocol,
            "throughput": rows["throughput"].mean(),
            "avg_latency_ms": rows["avg_latency"].mean() * 1000,
            "p99_latency_ms": rows["p99_latency"].mean() * 1000,
            "chain_p50": rows["dependency_depth_p50"].mean(),
            "chain_p95": rows["dependency_depth_p95"].mean(),
            "chain_max": rows["dependency_depth_max"].max(),
            "max_resolver_queue": rows["max_resolver_queue"].max(),
            "max_participant_batch": rows["max_participant_batch"].max(),
            "samples": int(rows["throughput"].count()),
        }
        if protocol == "Strict-2PC":
            record["max_resolver_queue"] = np.nan
            record["max_participant_batch"] = np.nan
        records.append(record)
    return pd.DataFrame.from_records(records)


def _integer(value: float) -> str:
    if pd.isna(value):
        return "--"
    return str(int(round(float(value))))


def _display_rows(summary: pd.DataFrame) -> list[list[str]]:
    rows = []
    for _, row in summary.iterrows():
        depth = "/".join(
            _integer(row[column]) for column in ("chain_p50", "chain_p95", "chain_max")
        )
        rows.append(
            [
                row["protocol"],
                f'{row["throughput"]:,.2f}',
                f'{row["avg_latency_ms"]:.2f}',
                f'{row["p99_latency_ms"]:.2f}',
                depth,
                _integer(row["max_resolver_queue"]),
                _integer(row["max_participant_batch"]),
            ]
        )
    return rows


def _write_latex(summary: pd.DataFrame, path: Path) -> None:
    lines = [
        r"\begin{tabular}{lrrrrrr}",
        r"\toprule",
        r"Protocol & Throughput & Mean (ms) & p99 (ms) & Chain depth (p50/p95/max) & Max Resolver queue & Max Resolver batch \\",
        r"\midrule",
    ]
    for row in _display_rows(summary):
        lines.append(" & ".join(row) + r" \\")
    lines.extend([r"\bottomrule", r"\end{tabular}"])
    path.write_text("\n".join(lines) + "\n")


def plot(result_directory: Path, output_directory: Path):
    summary = _summary(load_results(result_directory))
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)

    tex_path = output_directory / "table_04.tex"
    _write_latex(summary, tex_path)

    headers = [
        "Protocol",
        "Throughput\n(tx/s)",
        "Mean\n(ms)",
        "p99\n(ms)",
        "Chain depth\n(p50/p95/max)",
        "Max Resolver\nqueue",
        "Max Resolver\nbatch",
    ]
    with plt.rc_context(PAPER_STYLE):
        fig, ax = plt.subplots(figsize=(7.15, 1.45))
        ax.axis("off")
        table = ax.table(
            cellText=_display_rows(summary),
            colLabels=headers,
            cellLoc="center",
            colLoc="center",
            colWidths=[0.16, 0.13, 0.10, 0.10, 0.23, 0.14, 0.14],
            bbox=(0, 0, 1, 1),
        )
        table.auto_set_font_size(False)
        table.set_fontsize(7.2)
        for (row, column), cell in table.get_celld().items():
            cell.set_edgecolor("#b8b8b8")
            cell.set_linewidth(0.5)
            if row == 0:
                cell.set_facecolor("#e8e8e8")
                cell.set_text_props(weight="bold")
            elif row % 2 == 0:
                cell.set_facecolor("#f7f7f7")
            else:
                cell.set_facecolor("white")
            if column == 0:
                cell.set_text_props(ha="left", weight="bold" if row else "bold")

        outputs = save_figure(fig, output_directory, "table_04", summary)
    return [*outputs, tex_path]


if __name__ == "__main__":
    single_result_cli(plot, __doc__)
