from pathlib import Path
from typing import Dict, List
import pandas as pd
import plotly.express as px
from run_experiments import RAY_LOGS_DIR
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
import plotly.io as pio
import argparse
import json
from utils import process_resolver_stats_group_sizes, get_unique_key_values
from itertools import product
import copy
import warnings
from datetime import datetime

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
        self.results = self.results[self.results["iteration"] != 0]
        results = self.results.groupby(CONFIG_PARAMS)[METRICS].mean().reset_index()
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
        # Only first iteration of each experiment
        results = self.results[self.results["iteration"] == 1].copy()

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

    fixed_params = {
        k: float(v)
        for k, v in [param.split("=") for param in args.fixed_params.split(",")]
    }

    free_params = args.free_params.split(",")
    assert len(free_params) <= 2

    # Obtain the df_traditional where baseline is Traditional and concat it with the df for every value of resolver_cores
    df_traditional = plotter.results[plotter.results["baseline"] == "Traditional"]
    df = plotter.results[plotter.results["baseline"] != "Traditional"]

    #  Didn't rerun traditional experiments for each value of resolver_tx_load_concurrency because it's invariant to it
    for resolver_tx_load_concurrency in get_unique_key_values(
        df, "resolver_tx_load_concurrency"
    ):
        df_traditional["resolver_tx_load_concurrency"] = resolver_tx_load_concurrency
        df = pd.concat([df, copy.deepcopy(df_traditional)])
    plotter.results = df

    # --------Plot Metrics vs X vs Z--------
    facet_row, x_axis = free_params
    for metric in METRICS:
        plotter.plot_metrics_vs_x_vs_z(
            y=metric,
            x=x_axis,
            facet_row=facet_row,
            fixed_params=fixed_params,
        )

    #     # --------Plot Resolver Stats--------
    #     facet_row, x_axis = free_params
    #     for stat in RESOLVER_STATS:
    #         plotter.plot_resolver_stats(
    #             y=stat,
    #             x=x_axis,
    #             facet_row=facet_row,
    #             fixed_params=fixed_params,
    #         )

    # # --------Plot Range Server Stats--------
    # unique_resolver_loads = get_unique_key_values(df, "resolver_tx_load_concurrency")
    # unique_baselines = get_unique_key_values(df, "baseline")

    # for unique_baseline in unique_baselines:
    #     for unique_resolver_load in unique_resolver_loads:
    #         fixed_params["resolver_tx_load_concurrency"] = unique_resolver_load
    #         fixed_params["baseline"] = unique_baseline
    #         plotter.plot_range_server_stats(
    #             fixed_params=fixed_params,
    #         )

    # --------Plot Resolver Group Sizes--------
    subplot_key = free_params[0]
    unique_values_of_subplot_key = get_unique_key_values(plotter.results, subplot_key)
    for subplot_key_value in unique_values_of_subplot_key:
        fixed_params[subplot_key] = subplot_key_value
        plotter.plot_resolver_group_sizes(
            free_param=free_params[1],
            fixed_params=fixed_params,
        )


if __name__ == "__main__":
    main()
