from pathlib import Path
from typing import Dict, List
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotter import (
    make_plots,
    bar,
    cdf,
    scatter,
    bar2,
    entropy_line,
    max_line,
    delta_line,
    delta_line2,
    predictions,
    delta_between_requests,
    avg_entropy,
)
import argparse
import json
from utils import RAY_LOGS_DIR, process_resolver_stats_group_sizes, get_unique_key_values
from itertools import product
import warnings
from datetime import datetime
import ast

warnings.filterwarnings("ignore")

METRICS = [
    "throughput",
    "avg_latency",
    # "p99_latency",
]  # "p50_latency", "p95_latency", "p99_latency"]

RESOLVER_STATS = [
    "requests_per_second",
    "waiting_transactions_std",
    "waiting_transactions_max",
    "waiting_transactions_min",
    "waiting_transactions_avg",
    "waiting_transactions_count",
]

RANGE_SERVER_STATS = [
    "num_waiters",
    "num_pending_commits",
    "request_timestamps",
]

CONFIG_PARAMS = [
    "baseline",
    "num_queries",
    "max_concurrency",
    "zipf_exponent",
    "num_keys",
    "resolver_cores",
    "resolver_tx_load_concurrency",
]


class Plotter:
    def __init__(self, experiment_name: str):
        self.experiments_path = RAY_LOGS_DIR / experiment_name
        self.plots_path = self.experiments_path / "plots"
        if not self.plots_path.exists():
            self.plots_path.mkdir(parents=True, exist_ok=True)

        self.results = pd.concat(
            [pd.read_csv(file) for file in self.experiments_path.glob("*.csv")]
        )
        self.results.columns = self.results.columns.str.replace("config/", "")

    def process_results(self):
        group_params = [param for param in CONFIG_PARAMS if param in self.results]
        results = self.results.groupby(group_params)[METRICS].mean().reset_index()
        return results

    def plot_metrics_vs_x_vs_z(
        self, y: str, x: str, facet_row: str, fixed_params: Dict[str, int]
    ):
        results = self.process_results()
        keys_fixed = list(fixed_params.keys())
        df = results[[y, x, facet_row, "baseline", *keys_fixed]]
        for param, value in fixed_params.items():
            df = df[df[param] == value]
        df = df.sort_values(by=x)
        df[x] = df[x].astype(str)
        df[facet_row] = df[facet_row].astype(str)
        figs = []

        unique_facet_row_values = df[facet_row].unique()
        max_value = df[y].max()
        # print(facet_row, unique_facet_row_values)
        for i, facet_row_value in enumerate(unique_facet_row_values):
            arg = {
                "df": df[df[facet_row] == facet_row_value],
                "x": x,
                "y": y,
                "key": "baseline",
                "title": f"{facet_row}={facet_row_value}",
                "showlegend": True if i == 0 else False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": x,
                "y_axis_title": y,
                "y_range": (0, max_value),
            }
            figs.append([(bar, arg)])

        rows = len(figs)
        cols = len(figs[0])

        out_dir = self.plots_path.joinpath(f"metrics")
        out_dir.mkdir(parents=True, exist_ok=True)
        figs_args = {
            "axis_title_font_size": {"x": 18, "y": 18},
            "axis_tick_font_size": {"x": 14, "y": 14},
            "column_widths": [5],
            "output_path": f"{out_dir.joinpath(f'{y}_{facet_row}_{x}')}",
            "title": f"{', '.join([f'{k}={v}' for k, v in fixed_params.items()])}",
            "height": rows * 400,
            "width": 1500,
        }
        make_plots(figs, rows=rows, cols=cols, **figs_args)

        df.to_csv(out_dir.joinpath(f"{y}_{facet_row}_{x}.csv"), index=False)

    def plot_resolver_group_sizes(self, free_param: str, fixed_params: Dict[str, int]):
        results = self.results.copy()

        keys_fixed = list(fixed_params.keys())
        df = results[["resolver_stats", "baseline", free_param, *keys_fixed]]
        for param, value in fixed_params.items():
            df = df[df[param] == value]

        df["resolver_stats"] = df["resolver_stats"].apply(json.loads)
        df["resolver_stats"] = df["resolver_stats"].apply(
            lambda x: {k: v for k, v in x.items() if k.startswith("Group size:")}
        )
        df["resolver_stats"] = df["resolver_stats"].apply(
            lambda x: ({"Group size: 1": fixed_params["num_queries"]} if x == {} else x)
        )
        figs = []
        cumvalues, group_sizes = process_resolver_stats_group_sizes(df, free_param)
        for i, free_param_value in enumerate(
            sorted(get_unique_key_values(df, free_param))
        ):
            arg = {
                "df": df[df[free_param] == free_param_value],
                "cumvalues": cumvalues[free_param_value],
                "group_sizes": group_sizes,
                "showlegend": True if i == 0 else False,
                "legend_title": "baseline",
                "x_axis_title": "Group Commit Sizes",
                "y_axis_title": f"{free_param}={free_param_value}",
                "y_range": (0, 1.1),
            }
            figs.append([(cdf, arg)])

        rows = len(figs)
        cols = len(figs[0])

        out_dir = self.plots_path.joinpath(f"resolver/group_sizes")
        out_dir.mkdir(parents=True, exist_ok=True)

        figs_args = {
            "axis_title_font_size": {"x": 18, "y": 18},
            "axis_tick_font_size": {"x": 20, "y": 20},
            "column_widths": [5],
            "output_path": f"{out_dir.joinpath(f'{free_param}_{fixed_params}')}",
            "title": f"{', '.join([f'{k}={v}' for k, v in fixed_params.items()])}",
            "height": rows * 300,
            "width": 1500,
        }
        make_plots(figs, rows=rows, cols=cols, **figs_args)
        df.to_csv(out_dir.joinpath(f"{free_param}_{fixed_params}.csv"), index=False)

    def plot_resolver_stats(
        self, y: str, x: str, facet_row: str, fixed_params: Dict[str, int]
    ):

        # Only first iteration of each experiment
        results = self.results[self.results["iteration"] == 1]

        keys_fixed = list(fixed_params.keys())
        df = results[["resolver_stats", x, facet_row, "baseline", *keys_fixed]]
        for param, value in fixed_params.items():
            df = df[df[param] == value]
        df = df.sort_values(by=x)
        df[x] = df[x].astype(str)
        df[facet_row] = df[facet_row].astype(str)

        df["resolver_stats"] = df["resolver_stats"].apply(json.loads)

        y_name = "requests_num" if y == "waiting_transactions_count" else y
        df[y_name] = df["resolver_stats"].apply(lambda x: x.get(y, 0))
        del df["resolver_stats"]

        figs = []

        unique_facet_row_values = df[facet_row].unique()
        max_value = df[y_name].max()
        # print(facet_row, unique_facet_row_values)
        for i, facet_row_value in enumerate(unique_facet_row_values):
            arg = {
                "df": df[df[facet_row] == facet_row_value],
                "x": x,
                "y": y_name,
                "key": "baseline",
                "title": f"{facet_row}={facet_row_value}",
                "showlegend": True if i == 0 else False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": x,
                "y_axis_title": y_name,
                "y_range": (0, max_value),
            }
            figs.append([(bar, arg)])

        rows = len(figs)
        cols = len(figs[0])

        out_dir = self.plots_path.joinpath(f"resolver/stats")
        out_dir.mkdir(parents=True, exist_ok=True)
        figs_args = {
            "axis_title_font_size": {"x": 18, "y": 18},
            "axis_tick_font_size": {"x": 14, "y": 14},
            "column_widths": [5],
            "output_path": f"{out_dir.joinpath(f'{y_name}_{facet_row}_{x}')}",
            "title": f"{', '.join([f'{k}={v}' for k, v in fixed_params.items()])}",
            "height": rows * 300,
            "width": 1500,
        }
        make_plots(figs, rows=rows, cols=cols, **figs_args)

    def plot_range_server_stats(self, fixed_params: Dict[str, int]):

        # Only first iteration of each experiment
        results = self.results[self.results["iteration"] == 1]

        keys_fixed = list(fixed_params.keys())
        df = results[["range_server_stats", "max_concurrency", *keys_fixed]]
        for param, value in fixed_params.items():
            df = df[df[param] == value]

        # List of RangeStatistics - one for each range
        num_ranges = fixed_params["num_keys"]
        df["range_server_stats"] = df["range_server_stats"].apply(json.loads)

        df = df.sort_values(by="max_concurrency")
        max_concurrency = df["max_concurrency"].max()
        df["max_concurrency"] = df["max_concurrency"].astype(str)

        figs = []
        for range_id in range(int(num_ranges)):
            d = df["range_server_stats"].apply(lambda x: x.get(str(range_id)))

            # --------- Request timestamps ---------
            df["request_timestamps"] = d.apply(lambda x: x.get("request_timestamps"))
            df["request_timestamps"] = df["request_timestamps"].apply(
                lambda x: [
                    datetime.strptime(t[:26], "%Y-%m-%d %H:%M:%S.%f").timestamp()
                    for t in x
                ]
            )
            df["request_timestamps"] = df["request_timestamps"].apply(
                lambda x: [float(t - x[0]) for t in x]
            )
            arg1 = {
                "df": df.copy(),
                "x": "request_timestamps",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": True if range_id == 0 else False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "request_timestamps",
                "y_axis_title": "occurence",
                "x_range": [0, 5],
                "y_range": (0, max_concurrency),
            }

            # # --------- Num waiters ---------
            df["num_waiters"] = d.apply(lambda x: x.get("num_waiters"))
            df["num_waiters"] = df["num_waiters"].apply(lambda x: [int(t) for t in x])
            # num_waiters_max = max(df["num_waiters"].apply(lambda x: max(x))) + 10
            arg2 = {
                "df": df.copy(),
                "y": "num_waiters",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "-",
                "y_axis_title": "num_waiters",
                "y_range": [0, 100],
            }
            # # --------- Num pending commits ---------
            df["num_pending_commits"] = d.apply(lambda x: x.get("num_pending_commits"))
            df["num_pending_commits"] = df["num_pending_commits"].apply(
                lambda x: [int(t) for t in x]
            )
            # num_pending_commits_max = max(df["num_pending_commits"].apply(lambda x: max(x))) + 10
            arg3 = {
                "df": df.copy(),
                "y": "num_pending_commits",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "-",
                "y_axis_title": "num_pending_commits",
                "y_range": [0, 100],
            }

            # --------- Num waiter + num pending commits ---------
            df["sum_waiters_pending"] = df["num_waiters"] + df["num_pending_commits"]
            arg4 = {
                "df": df.copy(),
                "y": "sum_waiters_pending",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "-",
                "y_axis_title": "waiters+pending",
                "y_range": [0, 150],
            }

            # --------- Entropy of Num waiter + num pending commits ---------
            arg5 = {
                "df": df.copy(),
                "y": "sum_waiters_pending",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "-",
                "y_axis_title": "waiters+pending entropy",
                "y_range": [0, 5],
            }
            arg6 = {
                "df": df.copy(),
                "y": "sum_waiters_pending",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "-",
                "y_axis_title": "waiters+pending max",
                "y_range": [0, 20],
            }
            # --------- Deltas of request timestamps ---------
            arg7 = {
                "df": df.copy(),
                "y": "request_timestamps",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "request_timestamps",
                "y_axis_title": "delta",
                # "x_range": [0, 5],
                "y_range": (0, 0.05),
            }
            # --------- Smoothed Deltas of request timestamps ---------
            arg8 = {
                "df": df.copy(),
                "y": "request_timestamps",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "request_timestamps",
                "y_axis_title": "smoothed_delta",
                # "x_range": [0, 5],
                "y_range": (0, 0.05),
            }
            # --------- Predictions ---------
            df["predictions"] = d.apply(lambda x: x.get("predictions"))
            df["predictions"] = df["predictions"].apply(lambda x: [int(t) for t in x])
            # predictions_max = max(df["predictions"].apply(lambda x: max(x))) + 10
            arg9 = {
                "df": df.copy(),
                "y": "predictions",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "-",
                "y_axis_title": "predictions",
                "y_range": [0, 150],
            }
            # --------- Avg Deltas between requests ---------
            df["avg_delta_between_requests"] = d.apply(
                lambda x: x.get("avg_delta_between_requests")
            )
            df["avg_delta_between_requests"] = df["avg_delta_between_requests"].apply(
                lambda x: [float(t) for t in x]
            )
            # predictions_max = max(df["predictions"].apply(lambda x: max(x))) + 10
            arg10 = {
                "df": df.copy(),
                "y": "avg_delta_between_requests",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "-",
                "y_axis_title": "avg_delta_between_requests",
                "y_range": [0, 20],
            }

            # --------- Avg Entropies ---------
            df["avg_entropies"] = d.apply(lambda x: x.get("avg_entropies"))
            df["avg_entropies"] = df["avg_entropies"].apply(
                lambda x: [float(t) for t in x]
            )
            # predictions_max = max(df["predictions"].apply(lambda x: max(x))) + 10
            arg11 = {
                "df": df.copy(),
                "y": "avg_entropies",
                "key": "max_concurrency",
                "title": f"key={range_id}",
                "showlegend": True if range_id == 0 else False,
                "legend_title": "baseline",
                "legend_orientation": "h",
                "x_axis_title": "-",
                "y_axis_title": "avg_entropies",
                "y_range": [0, 5],
            }
            figs.append(
                [
                    (scatter, arg1),
                    (delta_line, arg7),
                    (delta_line2, arg8),
                    (bar2, arg4),
                    (entropy_line, arg5),
                    (max_line, arg6),
                    (predictions, arg9),
                    (delta_between_requests, arg10),
                    (avg_entropy, arg11),
                ]
            )  # (bar2, arg2), (bar2, arg3)])

        rows = len(figs)
        cols = len(figs[0])

        out_dir = self.plots_path.joinpath(f"range_server_stats")
        out_dir.mkdir(parents=True, exist_ok=True)
        figs_args = {
            "axis_title_font_size": {"x": 18, "y": 18},
            "axis_tick_font_size": {"x": 14, "y": 14},
            "column_widths": [10] * cols,
            "output_path": f"{out_dir.joinpath(str(fixed_params['baseline']) + '_' + str(fixed_params['resolver_tx_load_concurrency']))}",
            "title": f"{', '.join([f'{k}={v}' for k, v in fixed_params.items()])}",
            "height": rows * 300,
            "width": 6000,
        }
        make_plots(figs, rows=rows, cols=cols, **figs_args)


BASELINE_LABELS = {
    "Adaptive": "Sangria",
    "Pipelined": "Pipelined-2PC",
    "Traditional": "Strict-2PC",
}
BASELINE_COLORS = {
    "Sangria": "#2ca02c",
    "Pipelined-2PC": "#d62728",
    "Strict-2PC": "#1f77b4",
}


def _save_figure(fig: go.Figure, output_path: Path):
    """Always save an interactive plot; save a PNG when Kaleido is available."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_path.with_suffix(".html")), include_plotlyjs="cdn")
    try:
        fig.write_image(str(output_path.with_suffix(".png")))
    except Exception as error:
        message = str(error).strip()
        reason = message.splitlines()[0] if message else type(error).__name__
        print(f"Could not export {output_path.name}.png ({reason}); kept HTML output.")


def _parse_resolver_stats(value):
    if isinstance(value, dict):
        return value
    if pd.isna(value) or not value:
        return {}
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return {}


def _filter_fixed_params(df: pd.DataFrame, fixed_params: Dict):
    filtered = df.copy()
    for param, value in fixed_params.items():
        if param not in filtered:
            continue
        numeric = pd.to_numeric(filtered[param], errors="coerce")
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            filtered = filtered[filtered[param].astype(str) == str(value)]
        else:
            filtered = filtered[numeric == numeric_value]
    return filtered


def _repeat_traditional_across_resolver_loads(df: pd.DataFrame):
    load = "resolver_tx_load_concurrency"
    if load not in df or "baseline" not in df:
        return df
    target_loads = df.loc[df["baseline"] != "Traditional", load].dropna().unique()
    traditional = df[df["baseline"] == "Traditional"]
    if traditional.empty or len(target_loads) == 0:
        return df
    non_traditional = df[df["baseline"] != "Traditional"]
    copies = []
    for value in target_loads:
        copied = traditional.copy()
        copied[load] = value
        copies.append(copied)
    return pd.concat([non_traditional, *copies], ignore_index=True)


def _paper_labels(df: pd.DataFrame):
    labeled = df.copy()
    labeled["protocol"] = labeled["baseline"].map(BASELINE_LABELS).fillna(
        labeled["baseline"]
    )
    return labeled


def plot_generic_throughput(
    plotter: Plotter, free_params: List[str], fixed_params: Dict
):
    df = _filter_fixed_params(plotter.results, fixed_params)
    df = _repeat_traditional_across_resolver_loads(df)
    x, facet = free_params[1], free_params[0]
    if df[x].nunique() == 1 and df[facet].nunique() > 1:
        x, facet = facet, x

    summary = (
        df.groupby(["baseline", x, facet], dropna=False)["throughput"]
        .mean()
        .reset_index()
    )
    summary = _paper_labels(summary)
    summary.to_csv(plotter.plots_path / "throughput_summary.csv", index=False)
    facet_arg = facet if summary[facet].nunique() > 1 else None
    fig = px.bar(
        summary,
        x=x,
        y="throughput",
        color="protocol",
        facet_col=facet_arg,
        barmode="group",
        labels={"throughput": "Transactions / sec", x: x.replace("_", " ")},
        color_discrete_map=BASELINE_COLORS,
    )
    fig.update_layout(template="simple_white", legend_title_text="", height=430)
    fig.update_yaxes(matches="y")
    _save_figure(fig, plotter.plots_path / "throughput")


def plot_q1_tradeoff_details(plotter: Plotter, fixed_params: Dict):
    """Create the two Figure 6 views from the Figure 4 measurements."""
    df = _filter_fixed_params(plotter.results, fixed_params)
    df = _repeat_traditional_across_resolver_loads(df)
    df["concurrency"] = pd.to_numeric(df["max_concurrency"], errors="coerce")
    df["background_load"] = pd.to_numeric(
        df["resolver_tx_load_concurrency"], errors="coerce"
    )
    df = df.dropna(subset=["concurrency", "background_load"])

    throughput = (
        df.groupby(["baseline", "background_load", "concurrency"])["throughput"]
        .mean()
        .reset_index()
    )
    ratio = throughput.pivot(
        index=["background_load", "concurrency"],
        columns="baseline",
        values="throughput",
    ).reset_index()
    if {"Pipelined", "Traditional"}.issubset(ratio.columns):
        ratio["Pipelined / Strict"] = ratio["Pipelined"] / ratio["Traditional"]
        ratio = ratio.sort_values(["background_load", "concurrency"])
        ratio.to_csv(plotter.plots_path / "q1_crossover_ratio.csv", index=False)
        fig = px.line(
            ratio,
            x="concurrency",
            y="Pipelined / Strict",
            color="background_load",
            markers=True,
            labels={
                "concurrency": "Concurrency level",
                "background_load": "Resolver background clients",
            },
        )
        fig.add_hline(y=1, line_dash="dash", line_color="black")
        fig.update_layout(template="simple_white", legend_title_text="")
        _save_figure(fig, plotter.plots_path / "q1_crossover_ratio")

    if df.empty:
        return
    low_capacity_load = df["background_load"].max()
    latency = df[df["background_load"] == low_capacity_load]
    latency = (
        latency.groupby(["baseline", "concurrency"])["avg_latency"]
        .mean()
        .reset_index()
    )
    latency["Mean latency (ms)"] = latency["avg_latency"] * 1000
    latency = _paper_labels(latency).sort_values("concurrency")
    latency.to_csv(
        plotter.plots_path / "q1_low_capacity_latency.csv", index=False
    )
    fig = px.line(
        latency,
        x="concurrency",
        y="Mean latency (ms)",
        color="protocol",
        markers=True,
        labels={"concurrency": "Concurrency level"},
        color_discrete_map=BASELINE_COLORS,
    )
    fig.update_layout(template="simple_white", legend_title_text="")
    _save_figure(fig, plotter.plots_path / "q1_low_capacity_latency")


def plot_single_parameter_throughput(
    plotter: Plotter, free_param: str, fixed_params: Dict
):
    df = _filter_fixed_params(plotter.results, fixed_params)
    if free_param not in df:
        print(f"Skipping plot: result column '{free_param}' was not produced.")
        return
    summary = (
        df.groupby(["baseline", free_param], dropna=False)["throughput"]
        .mean()
        .reset_index()
    )
    summary = _paper_labels(summary)
    summary.to_csv(plotter.plots_path / "throughput_summary.csv", index=False)
    fig = px.line(
        summary,
        x=free_param,
        y="throughput",
        color="protocol",
        markers=True,
        labels={
            "throughput": "Transactions / sec",
            free_param: free_param.replace("_", " "),
        },
        color_discrete_map=BASELINE_COLORS,
    )
    fig.update_layout(template="simple_white", legend_title_text="", height=430)
    _save_figure(fig, plotter.plots_path / "throughput")


def plot_threshold_sensitivity(plotter: Plotter, threshold_column: str):
    is_contention = threshold_column.endswith("open_clients_low")
    adaptive = plotter.results[plotter.results["baseline"] == "Adaptive"].copy()
    adaptive[threshold_column] = pd.to_numeric(
        adaptive[threshold_column], errors="coerce"
    )
    adaptive = adaptive.dropna(subset=[threshold_column])
    summary = (
        adaptive.groupby(threshold_column)["throughput"].mean().reset_index()
    )
    summary = summary.sort_values(threshold_column, ascending=not is_contention)
    summary["profile"] = range(1, len(summary) + 1)

    baseline_means = (
        plotter.results[plotter.results["baseline"].isin(["Pipelined", "Traditional"])]
        .groupby("baseline")["throughput"]
        .mean()
    )
    for baseline, value in baseline_means.items():
        summary[BASELINE_LABELS[baseline]] = value
    summary = summary.rename(columns={threshold_column: "threshold", "throughput": "Sangria"})
    summary.to_csv(
        plotter.plots_path
        / ("fig10_contention.csv" if is_contention else "fig10_resolver.csv"),
        index=False,
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=summary["profile"],
            y=summary["Sangria"],
            mode="lines+markers",
            name="Sangria",
            line=dict(color=BASELINE_COLORS["Sangria"], width=3),
        )
    )
    for protocol, dash in (("Strict-2PC", "dash"), ("Pipelined-2PC", "dot")):
        if protocol in summary:
            fig.add_trace(
                go.Scatter(
                    x=summary["profile"],
                    y=summary[protocol],
                    mode="lines",
                    name=protocol,
                    line=dict(color=BASELINE_COLORS[protocol], dash=dash, width=2),
                )
            )
    reference = 50 if is_contention else 200
    reference_rows = summary.index[summary["threshold"] == reference].tolist()
    if reference_rows:
        fig.add_vline(x=int(summary.loc[reference_rows[0], "profile"]), line_dash="dashdot")
    symbol = "C_L" if is_contention else "R_M"
    fig.update_xaxes(
        tickmode="array",
        tickvals=summary["profile"],
        ticktext=summary["threshold"],
        title=f"Threshold {symbol}",
    )
    fig.update_yaxes(title="Transactions / sec", showgrid=True)
    fig.update_layout(template="simple_white", legend_title_text="", height=430)
    stem = "fig10_contention" if is_contention else "fig10_resolver"
    _save_figure(fig, plotter.plots_path / stem)


def plot_threshold_grid(plotter: Plotter, low_column: str, mid_column: str):
    """Plot the older two-threshold sweep without treating it as Figure 10."""
    df = plotter.results[plotter.results["baseline"] == "Adaptive"].copy()
    for column in (low_column, mid_column):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    summary = (
        df.dropna(subset=[low_column, mid_column])
        .groupby([low_column, mid_column])["throughput"]
        .mean()
        .reset_index()
    )
    summary.to_csv(plotter.plots_path / "threshold_grid.csv", index=False)
    pivot = summary.pivot(index=mid_column, columns=low_column, values="throughput")
    fig = go.Figure(
        go.Heatmap(
            x=pivot.columns,
            y=pivot.index,
            z=pivot.values,
            colorbar_title="tx/s",
        )
    )
    fig.update_xaxes(title="Open-client low threshold")
    fig.update_yaxes(title="Open-client middle threshold")
    fig.update_layout(template="simple_white", height=480)
    _save_figure(fig, plotter.plots_path / "threshold_grid")


def plot_resolver_calibration(plotter: Plotter):
    rows = []
    for _, row in plotter.results.iterrows():
        stats = _parse_resolver_stats(row.get("resolver_stats"))
        concurrency = str(row["resolver_tx_load_concurrency"]).split(":", 1)[0]
        rows.append(
            {
                "background_clients": pd.to_numeric(concurrency, errors="coerce"),
                "Resolver load (mean)": stats.get("resolver_load_signal_avg"),
                "Resolver load (max)": stats.get("resolver_load_signal_max"),
            }
        )
    summary = pd.DataFrame(rows).dropna(subset=["background_clients"])
    value_columns = [column for column in summary if column != "background_clients"]
    if summary.empty or summary[value_columns].notna().sum().sum() == 0:
        print("Skipping calibration plot: no Resolver-load signal was recorded.")
        return
    summary = summary.groupby("background_clients", as_index=False).mean()
    summary.to_csv(plotter.plots_path / "resolver_calibration.csv", index=False)
    melted = summary.melt(
        id_vars="background_clients", var_name="signal", value_name="resolver_load"
    )
    fig = px.line(
        melted,
        x="background_clients",
        y="resolver_load",
        color="signal",
        markers=True,
        labels={
            "background_clients": "Background clients",
            "resolver_load": "Resolver load",
        },
    )
    fig.update_layout(template="simple_white", legend_title_text="", height=430)
    _save_figure(fig, plotter.plots_path / "resolver_calibration")


def plot_dependency_stress(plotter: Plotter):
    records = []
    for baseline, rows in plotter.results.groupby("baseline"):
        parsed_stats = rows["resolver_stats"].apply(_parse_resolver_stats)

        def stat_values(name):
            return pd.Series(
                [stats.get(name, 0) for stats in parsed_stats], dtype=float
            )

        records.append(
            {
                "protocol": BASELINE_LABELS.get(baseline, baseline),
                "throughput": rows["throughput"].mean(),
                "avg_latency_ms": rows["avg_latency"].mean() * 1000,
                "p99_latency_ms": rows["p99_latency"].mean() * 1000,
                "chain_p50": stat_values("dependency_depth_p50").mean(),
                "chain_p95": stat_values("dependency_depth_p95").mean(),
                "chain_max": stat_values("dependency_depth_max").max(),
                "max_resolver_queue": stat_values("max_resolver_queue").max(),
                "max_participant_batch": stat_values("max_participant_batch").max(),
            }
        )
    summary = pd.DataFrame(records)
    order = ["Strict-2PC", "Pipelined-2PC", "Sangria"]
    summary["protocol"] = pd.Categorical(summary["protocol"], order, ordered=True)
    summary = summary.sort_values("protocol")
    summary.to_csv(plotter.plots_path / "table4_summary.csv", index=False)

    fig = make_subplots(rows=1, cols=2, subplot_titles=("Throughput", "Latency"))
    for _, row in summary.iterrows():
        protocol = str(row["protocol"])
        color = BASELINE_COLORS.get(protocol)
        fig.add_trace(
            go.Bar(
                x=[protocol], y=[row["throughput"]], name=protocol,
                marker_color=color, legendgroup=protocol,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Bar(
                x=["Mean", "p99"],
                y=[row["avg_latency_ms"], row["p99_latency_ms"]],
                name=protocol,
                marker_color=color,
                legendgroup=protocol,
                showlegend=False,
            ),
            row=1,
            col=2,
        )
    fig.update_yaxes(title_text="Transactions / sec", row=1, col=1)
    fig.update_yaxes(title_text="Latency (ms)", row=1, col=2)
    fig.update_layout(template="simple_white", barmode="group", height=430)
    _save_figure(fig, plotter.plots_path / "table4_performance")


def main():
    parser = argparse.ArgumentParser(
        description="Plot metrics for a given Ray Tune experiment"
    )
    parser.add_argument(
        "-e",
        "--experiment-name",
        required=True,
        help="The Ray Tune experiment name (e.g. cooperative_giraffe_ba7b1a13)",
    )
    parser.add_argument(
        "-p",
        "--fixed-params",
        required=False,
        type=str,
        help="The fixed parameters to plot",
        default="num_queries=1500,zipf_exponent=0.0,max_concurrency=28",
    )
    parser.add_argument(
        "-f",
        "--free-params",
        required=False,
        type=str,
        help="The free parameters to plot",
        default="resolver_cores,max_concurrency",
    )

    args = parser.parse_args()
    plotter = Plotter(args.experiment_name)

    fixed_params = {}
    for parameter in filter(None, args.fixed_params.split(",")):
        key, value = parameter.split("=", 1)
        try:
            fixed_params[key] = float(value)
        except ValueError:
            fixed_params[key] = value

    free_params = [param for param in args.free_params.split(",") if param]
    if len(free_params) not in (1, 2):
        parser.error("--free-params must contain one or two comma-separated columns")

    low_threshold = "threshold_overrides/open_clients_low"
    mid_threshold = "threshold_overrides/open_clients_mid"
    resolver_threshold = "threshold_overrides/resolver_load_mid"
    columns = set(plotter.results.columns)

    if free_params == ["baseline"]:
        plot_dependency_stress(plotter)
    elif resolver_threshold in columns:
        plot_threshold_sensitivity(plotter, resolver_threshold)
    elif low_threshold in columns and mid_threshold not in columns:
        plot_threshold_sensitivity(plotter, low_threshold)
    elif low_threshold in columns and mid_threshold in columns:
        plot_threshold_grid(plotter, low_threshold, mid_threshold)
    elif free_params == ["resolver_tx_load_concurrency"]:
        plot_resolver_calibration(plotter)
    elif len(free_params) == 1:
        plot_single_parameter_throughput(plotter, free_params[0], fixed_params)
    else:
        plot_generic_throughput(plotter, free_params, fixed_params)

        is_q1_tradeoff = (
            free_params
            == ["resolver_tx_load_concurrency", "max_concurrency"]
            and float(fixed_params.get("num_queries", -1)) == 2500
        )
        if is_q1_tradeoff:
            plot_q1_tradeoff_details(plotter, fixed_params)

        # Retain the legacy batch-size CDF as a best-effort secondary output.
        try:
            plotter.results = _repeat_traditional_across_resolver_loads(
                plotter.results
            )
            subplot_key = free_params[0]
            for subplot_value in get_unique_key_values(plotter.results, subplot_key):
                group_fixed_params = dict(fixed_params)
                group_fixed_params[subplot_key] = subplot_value
                plotter.plot_resolver_group_sizes(
                    free_param=free_params[1],
                    fixed_params=group_fixed_params,
                )
            if is_q1_tradeoff:
                concurrency_values = get_unique_key_values(
                    plotter.results, "max_concurrency"
                )
                high_contention = next(
                    value for value in concurrency_values if str(value) == "500"
                )
                group_fixed_params = dict(fixed_params)
                group_fixed_params["max_concurrency"] = high_contention
                plotter.plot_resolver_group_sizes(
                    free_param="resolver_tx_load_concurrency",
                    fixed_params=group_fixed_params,
                )
        except Exception as error:
            reason = str(error).splitlines()[0]
            print(f"Could not generate legacy batch-size plot: {reason}")


if __name__ == "__main__":
    main()
