import json
import warnings
from typing import List

import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings("ignore", category=DeprecationWarning)

pio.kaleido.scope.mathjax = None


def create_color_map(df, key):
    unique_keys = get_unique_key_values(df, key)
    unique_keys = sorted(unique_keys)

    colors = {
        "Traditional": "blue",
        "Pipelined": [
            # "rgba(255, 0, 0, 0.2)",
            # "rgba(255, 0, 0, 0.4)",
            # "rgba(255, 0, 0, 0.6)",
            # "rgba(255, 0, 0, 0.8)",
            "red",
        ],
        "Adaptive": [
            # "rgba(0, 128, 0, 0.2)",
            # "rgba(0, 128, 0, 0.4)",
            # "rgba(0, 128, 0, 0.6)",
            # "rgba(0, 128, 0, 0.8)",
            "green",
        ],
    }

    color_map = {}
    for key in unique_keys:
        if key.endswith("Pipelined"):
            color_map[key] = colors["Pipelined"].pop(0)
        elif key.endswith("Adaptive"):
            color_map[key] = colors["Adaptive"].pop(0)
        else:
            color_map[key] = colors["Traditional"]
    return color_map


# markers = [
#     "circle",
#     "square",
#     "diamond",
#     "triangle-up",
#     "triangle-down",
#     "star",
#     "x",
#     "circle-open",
# ]


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
            fig.add_annotation(
                text=f"<b>{args.get('title')}</b>" if args.get("title") else "",
                xref="paper",
                yref="paper",
                x=0,
                y=args.get("y_range")[1],
                showarrow=False,
                font=dict(size=20),
                # align="center",
                xanchor="left",
                yanchor="top",
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


def line(df, x, y, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    """Create a line plot"""

    unique_keys = get_unique_key_values(df, key)
    unique_keys = sorted(unique_keys)

    color_map = create_color_map(df, key)
    # marker_map = {key: marker for key, marker in zip(unique_keys, markers)}

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
            # marker_symbol=marker_map[unique_key],
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

    color_map = create_color_map(df, key)

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


def bar_with_error_bars(
    df, x, y, key=None, showlegend=True, order=None, **kwargs
) -> List[go.Bar]:
    """Create a bar plot with error bars showing min/max values"""
    unique_keys = get_unique_key_values(df, key)
    unique_keys = sorted(unique_keys)

    color_map = create_color_map(df, key)

    # box plot with error bars
    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        trace = go.Box(
            y=df_key[y],
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            marker_color=color_map[unique_key],
            boxpoints="all",
            jitter=0.3,
            pointpos=-1.8,
            whiskerwidth=0.2,
            fillcolor=color_map[unique_key],
            opacity=0.6,
            line=dict(color="black", width=1),
        )
        traces.append(trace)
    return traces


def cdf(df, cumvalues, group_sizes, showlegend=True, **kwargs):
    """Create a CDF plot"""

    baselines = get_unique_key_values(df, "baseline")
    baselines = sorted(baselines)
    color_map = create_color_map(df, "baseline")
    traces = []
    for i, baseline in enumerate(baselines):
        traces.append(
            go.Scatter(
                x=list(group_sizes),
                y=cumvalues[baseline],
                name=baseline,
                legendgroup=baseline,
                legendgrouptitle=dict(text="baseline") if i == 0 else None,
                showlegend=showlegend,
                marker_color=color_map[baseline],
                mode="lines",
            )
        )
    return traces
