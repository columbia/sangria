import json
import warnings
from typing import List

import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings("ignore", category=DeprecationWarning)

pio.kaleido.scope.mathjax = None

colors = [
            "red",  # "rgba(255, 0, 0, 0.4)",
            "blue",  # "rgba(0, 0, 255, 0.4)",
            "green",  # "rgba(0, 128, 0, 0.4)",
            "orange",  # "rgba(255, 165, 0, 0.4)",
            "purple",  # "rgba(128, 0, 128, 0.4)",
            "brown",  # "rgba(165, 42, 42, 0.4)",
            "pink",  # "rgba(255, 192, 203, 0.4)",
            "gray",  # "rgba(128, 128, 128, 0.4)"
        ]
markers = [
            "circle",
            "square",
            "diamond",
            "triangle-up",
            "triangle-down",
            "star",
            "x",
            "circle-open",
        ]


def make_plots(
    figs,
    rows,
    cols,
    axis_title_font_size,
    axis_tick_font_size,
    output_path,
    height=None,
    width=None,
    titles=None,
    column_widths=None,
    showlegend=True,
    legend_title=None,
    title=None,
) -> go.Figure:
    """Make plots from the given figures"""

    fig: go.Figure = make_subplots(
        rows=rows, cols=cols, subplot_titles=titles, column_widths=column_widths
    )
    # Add the figures to the subplots
    for i, row in enumerate(figs):
        for j, (func, args) in enumerate(row):
            trace = func(**args)
            if isinstance(trace, list):
                for t in trace:
                    fig.add_trace(t, row=i + 1, col=j + 1)
            else:
                fig.add_trace(trace, row=i + 1, col=j + 1)
            fig.update_xaxes(
                title=args.get("x_axis_title"),
                tickfont={"size": axis_tick_font_size.get("x")},
                title_font={"size": axis_title_font_size.get("x")},
                showgrid=True,
                range=args.get("x_range"),
                row=i + 1,
                col=j + 1,
            )
            fig.update_yaxes(
                title=args.get("y_axis_title"),
                title_standoff=0,
                tickfont={"size": axis_tick_font_size.get("y")},
                title_font={"size": axis_title_font_size.get("y")},
                showgrid=True,
                range=args.get("y_range"),
                row=i + 1,
                col=j + 1,
            )

    fig.update_layout(
        legend={
            # "title": None,
            # "font": {"size": 20},
            # **legend,
        },
        template="simple_white",
        showlegend=showlegend,
        legend_font_size=20,
        height=height,
        width=width,
        barmode="group",
        title=title,
        # boxmode="group",
        # margin=dict(t=0, b=0),
    )
    # fig.show()
    fig.write_image(f"{output_path}.png", engine="kaleido")
    return fig


def get_unique_key_values(df, key):
    """Get the unique values of the key"""
    return df[key].unique()


def line(
    df, x, y, key=None, showlegend=True, **kwargs
) -> List[go.Scatter]:
    """Create a line plot"""

    unique_keys = get_unique_key_values(df, key)
    unique_keys = sorted(unique_keys)

    color_map = {key: color for key, color in zip(unique_keys, colors)}
    marker_map = {key: marker for key, marker in zip(unique_keys, markers)}

    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        trace = go.Scatter(
            x=df_key[x],
            y=df_key[y],
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            mode="lines+markers",
            marker_color=color_map[unique_key],
            marker_symbol=marker_map[unique_key],
            marker_size=10,
        )
        traces.append(trace)
    return traces


def bar(
    df,
    x,
    y,
    key=None,
    showlegend=True,
    order=None,
    **kwargs,
) -> List[go.Bar]:
    """Create a bar plot"""
    unique_keys = get_unique_key_values(df, key)
    unique_keys = sorted(unique_keys)

    color_map = {key: color for key, color in zip(unique_keys, colors)}

    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        trace = go.Bar(
            x=df_key[x],
            y=df_key[y],
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            marker_color=color_map[unique_key],
            # marker_symbol=marker_map[unique_key],
        )
        traces.append(trace)
    return traces

def cdf(df, showlegend=True, **kwargs):
    """Create a CDF plot"""

    baselines = get_unique_key_values(df, "baseline")
    baselines = sorted(baselines)
    color_map = {key: color for key, color in zip(baselines, colors)}

    traces = []
    
    group_sizes = set()
    # Collect group_sizes per baseline
    resolver_stats_per_baseline = {}
    for baseline in baselines:
        df_key = df[df["baseline"] == baseline]
        resolver_stats = {}
        for _, row in df_key.iterrows():
            resolver_stats.update(json.loads(row["resolver_stats"]))
        resolver_stats = {int(k.split(":")[1]): v for k, v in resolver_stats.items()}
        group_sizes.update(resolver_stats.keys())
        resolver_stats_per_baseline[baseline] = resolver_stats

    for i, baseline in enumerate(baselines):
        resolver_stats = resolver_stats_per_baseline[baseline]
        for group_size in group_sizes:
            if group_size not in resolver_stats:
                resolver_stats[group_size] = 0


        sum_resolver_stats = sum(resolver_stats.values())
        resolver_stats = {k: v / sum_resolver_stats for k, v in resolver_stats.items()}
        resolver_stats = dict(sorted(resolver_stats.items()))
        cumulative_values = [sum(list(resolver_stats.values())[:i+1]) for i in range(len(resolver_stats))]

        traces.append(
            go.Scatter(
                x=list(resolver_stats.keys()),
                y=cumulative_values,
                name=baseline,
                legendgroup=baseline,
                legendgrouptitle=dict(text="baseline") if i == 0 else None,
                showlegend=showlegend,
                marker_color=color_map[baseline],
                mode="lines",
            )
        )
    return traces