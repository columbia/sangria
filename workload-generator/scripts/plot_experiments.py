from pathlib import Path
from typing import Dict, List
import pandas as pd
import plotly.express as px
from run_experiments import RAY_LOGS_DIR
from plotter import make_plots, line
import plotly.io as pio


class Plotter:
    def __init__(self, experiment_name: str):
        self.experiments_path = RAY_LOGS_DIR / experiment_name
        self.plots_path = self.experiments_path / "plots"
        if not self.plots_path.exists():
            self.plots_path.mkdir(parents=True, exist_ok=True)
        self.results = pd.read_csv(self.experiments_path / "results.csv")
        self.results.columns = self.results.columns.str.replace("config/", "")

    def plot_metrics_vs_x_vs_z(
        self, metrics: List[str], x: str, z: str, fixed_params: Dict[str, int]
    ):
        keys_fixed = list(fixed_params.keys())
        df = self.results[[*metrics, x, z, *keys_fixed]]
        for param, value in fixed_params.items():
            df = df[df[param] == value]
        df = df.dropna()
        df[z] = df[z].astype(str)
        figs = []
        args = []
        unique_keys = df[z].unique()
        color_map = {key: color for key, color in zip(unique_keys, ['red', 'blue', 'green', 'orange', 'purple', 'brown', 'pink', 'gray'])}
        marker_map = {key: marker for key, marker in zip(unique_keys, ['circle', 'square', 'diamond', 'triangle-up', 'triangle-down', 'star', 'x', 'circle-open'])}
        for i, metric in enumerate(metrics):
            arg = {
                "df": df,
                "x": x,
                "y": metric,
                "key": z,
                "color_map": color_map,
                "marker_map": marker_map,
                "showlegend": True if i == 0 else False,
                "legend_title": z,
                "legend_orientation": "h",
                "x_axis_title": x,
                "y_axis_title": metric,
            }
            figs.append([(line, arg)])

        rows = len(figs)
        cols = len(figs[0])

        figs_args = {
            "axis_title_font_size": {"x": 18, "y": 18},
            "axis_tick_font_size": {"x": 14, "y": 14},
            "column_widths": [5],
            "output_path": f"{self.plots_path.joinpath(f'metrics_vs_{x}_vs_{z}.png')}",
            "height": rows * 300,
            "width": 1500,
        }
        make_plots(figs, rows=rows, cols=cols, **figs_args)

    def plot_y_vs_x_vs_z(self, y: str, x: str, z: str, fixed_params: Dict[str, int]):
        keys_fixed = list(fixed_params.keys())
        df = self.results[[y, x, z, *keys_fixed]]
        for param, value in fixed_params.items():
            df = df[df[param] == value]
        df = df.dropna()
        fig = px.line(df, x=x, y=y, color=z)
        pio.write_image(
            fig, f"{self.plots_path.joinpath(f'{y}_vs_{x}_vs_{z}.png')}", format="png"
        )


if __name__ == "__main__":
    experiment_name = "small_pogona_405f59c4"
    plotter = Plotter(experiment_name)
    plotter.plot_metrics_vs_x_vs_z(
        ["throughput", "avg_latency", "p99_latency"],
        "max-concurrency",
        "num-keys",
        {"num-queries": 100, "zipf-exponent": 0},
    )

    plotter.plot_metrics_vs_x_vs_z(
        ["throughput", "avg_latency", "p99_latency"],
        "zipf-exponent",
        "num-keys",
        {"num-queries": 100, "max-concurrency": 10},
    )