from pathlib import Path
from typing import Dict, List
import pandas as pd
import plotly.express as px
from run_experiments import RAY_LOGS_DIR
from plotter import make_plots, line, bar, cdf
import plotly.io as pio
import argparse
import json
from utils import process_resolver_stats, get_unique_key_values
from itertools import product
import copy
import warnings

warnings.filterwarnings("ignore")

METRICS = [
    "throughput",
    "avg_latency",
    # "p99_latency",
]  # "p50_latency", "p95_latency", "p99_latency"]

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

        figs_args = {
            "axis_title_font_size": {"x": 18, "y": 18},
            "axis_tick_font_size": {"x": 14, "y": 14},
            "column_widths": [5],
            "output_path": f"{self.plots_path.joinpath(f'{y}_{facet_row}_{x}.png')}",
            "title": f"{', '.join([f'{k}={v}' for k, v in fixed_params.items()])}",
            "height": rows * 300,
            "width": 1500,
        }
        make_plots(figs, rows=rows, cols=cols, **figs_args)

    def plot_resolver_stats(self, free_param: str, fixed_params: Dict[str, int]):
        keys_fixed = list(fixed_params.keys())
        df = self.results[["resolver_stats", "baseline", free_param, *keys_fixed]]
        for param, value in fixed_params.items():
            df = df[df[param] == value]

        df["resolver_stats"] = df["resolver_stats"].apply(
            lambda x: (
                json.dumps({"Group size: 1": fixed_params["num_queries"]})
                if x == "{}"
                else x
            )
        )

        figs = []
        cumvalues, group_sizes = process_resolver_stats(df, free_param)
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

        figs_args = {
            "axis_title_font_size": {"x": 18, "y": 18},
            "axis_tick_font_size": {"x": 20, "y": 20},
            "column_widths": [5],
            "output_path": f"{self.plots_path.joinpath(f'resolver_stats_{free_param}_{fixed_params}.png')}",
            "title": f"{', '.join([f'{k}={v}' for k, v in fixed_params.items()])}",
            "height": rows * 300,
            "width": 1500,
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

    #  Append resolver_cores to each baseline if baseline not traditional
    # plotter.results["baseline"] = plotter.results.apply(
    #     lambda row: (
    #         str(row["resolver_cores"]) + "_RC_" + row["baseline"]
    #         if row["baseline"] != "Traditional"
    #         else row["baseline"]
    #     ),
    #     axis=1,
    # )

    # Obtain the df_traditional where baseline is Traditional and concat it with the df for every value of resolver_cores
    df_traditional = plotter.results[plotter.results["baseline"] == "Traditional"]
    df = plotter.results[plotter.results["baseline"] != "Traditional"]

    #  Didn't rerun traditional experiments for each value of resolver_tx_load_concurrency because it's invariant to it
    for resolver_tx_load_concurrency in get_unique_key_values(df, "resolver_tx_load_concurrency"):
        df_traditional["resolver_tx_load_concurrency"] = resolver_tx_load_concurrency
        df = pd.concat([df, copy.deepcopy(df_traditional)])
    plotter.results = df

    facet_row, x_axis = free_params
    for metric in METRICS:
        plotter.plot_metrics_vs_x_vs_z(
            y=metric,
            x=x_axis,
            facet_row=facet_row,
            fixed_params=fixed_params,
        )

    subplot_key = free_params[0]
    unique_values_of_subplot_key = get_unique_key_values(plotter.results, subplot_key)
    for subplot_key_value in unique_values_of_subplot_key:
        fixed_params[subplot_key] = subplot_key_value
        plotter.plot_resolver_stats(
            free_param=free_params[1],
            fixed_params=fixed_params,
        )


if __name__ == "__main__":
    main()
