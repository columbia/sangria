import json
import warnings
from typing import List
from collections import Counter
import math
import numpy as np
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)

pio.kaleido.scope.mathjax = None

colors_list = ["red", "green", "blue", "purple", "orange", "brown", "pink", "gray", "black"]
markers_list = ["circle", "square", "diamond", "triangle-up", "triangle-down", "triangle-left", "triangle-right", "triangle-top", "triangle-bottom", "triangle-topleft", "triangle-topright", "triangle-bottomleft", "triangle-bottomright"]

def create_color_map(df, key):
    if key == "max_concurrency":
        unique_keys = get_unique_key_values(df, key)
        color_map = {}
        for i, key in enumerate(unique_keys):
            color_map[key] = colors_list[i]
        return color_map
    

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


def create_marker_map(df, key):
    if key == "max_concurrency":
        unique_keys = get_unique_key_values(df, key)
        marker_map = {}
        for i, key in enumerate(unique_keys):
            marker_map[key] = markers_list[i]
        return marker_map

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




# ------------------------------------------------------------
def scatter(df, x, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    """Create a scatter plot"""

    unique_keys = get_unique_key_values(df, key)
    color_map = create_color_map(df, key)

    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        x_list = df_key[x].values.tolist()[0]
        y = [int(unique_key)] * len(x_list)
        trace = go.Scatter(
            x=x_list,
            y=y,
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            mode="markers",
            marker_color=color_map[unique_key],
            # marker_symbol=marker_map[unique_key],
            marker_size=5,
        )
        traces.append(trace)
    return traces


def bar2(df, y, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    unique_keys = get_unique_key_values(df, key)
    color_map = create_color_map(df, key)
    
    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        y_list = df_key[y].values.tolist()[0]
        x = []
        y_values = []
        for value in sorted(list(set(y_list))):
            x.append(value)
            y_values.append(y_list.count(value))
        trace = go.Bar(
            x=x,
            y=y_values,
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            marker_color=color_map[unique_key],
        )
        traces.append(trace)
    return traces


def entropy(samples):
    # THRESHOLD = 50
    # if len(samples) > THRESHOLD:
    #     samples = samples[THRESHOLD:]
    total = len(samples)
    if total == 0:
        return 0.0
    freqs = Counter(samples)
    ent = 0.0
    for count in freqs.values():
        p = count / total
        ent -= p * math.log2(p)
    return ent

data = {}
def entropy_line(df, y, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    unique_keys = get_unique_key_values(df, key)
    color_map = create_color_map(df, key)
    
    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        y_list = df_key[y].values.tolist()[0]


        if unique_key not in data:
            data[unique_key] = []
        
        # # counter of ylist and normalize it
        # counter = Counter(y_list)
        # total = sum(counter.values())
        # normalized_counter = {k: v / total for k, v in counter.items()}
        # data[unique_key].append(normalized_counter)
        # if all(len(data[key]) == 50 for key in data):
        #     # Flatten the data structure to create proper DataFrame
        #     flattened_data = {}
        #     for unique_key, counters_list in data.items():
        #         for counter in counters_list:
        #             row_data = {'max_concurrency': unique_key}
        #             counter = {int(k): v for k, v in counter.items()}
        #             sorted_keys = sorted(counter.keys())
        #             for key in sorted_keys:
        #                 row_data[key] = counter[key]
        #             flattened_data[f"{unique_key}_{len(flattened_data)}"] = row_data
        #     x
        #     df = pd.DataFrame.from_dict(flattened_data, orient='index')
        #     df.to_csv(f"data.csv")

        entropy_list = [entropy(y_list[:i+1]) for i in range(len(y_list))]
        # take the avg of each prefix
        entropy_list = [sum(entropy_list[:i+1]) / (i+1) for i in range(len(entropy_list))]

        trace = go.Scatter(
            x=list(range(len(entropy_list))),
            y=entropy_list,
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            marker_color=color_map[unique_key],
            mode="lines",
        )
        traces.append(trace)
    return traces


def max_line(df, y, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    unique_keys = get_unique_key_values(df, key)
    color_map = create_color_map(df, key)
    traces = []

    def percentile(y_list):
        if len(y_list) == 0:
            return 0
        return np.percentile(y_list, 75)
    
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        y_list = df_key[y].values.tolist()[0]
        max_list = [percentile(y_list[:i+1]) for i in range(len(y_list))]

        # # take the avg of each prefix
        # max_list = [sum(max_list[:i+1]) / (i+1) for i in range(len(max_list))]

        trace = go.Scatter(
            x=list(range(len(max_list))),
            y=max_list,
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            marker_color=color_map[unique_key],
            mode="lines",
        )
        traces.append(trace)
    return traces

def delta_line(df, y, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    unique_keys = get_unique_key_values(df, key)
    color_map = create_color_map(df, key)
    
    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        timestamp_stream = df_key[y].values.tolist()[0]

        delta_list = [timestamp_stream[i+1] - timestamp_stream[i] for i in range(len(timestamp_stream)-1)]
        delta_avg = [sum(delta_list[:i+1]) / (i+1) for i in range(len(delta_list))]

        trace = go.Scatter(
            x=list(range(len(delta_avg))),
            y=delta_avg,
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            marker_color=color_map[unique_key],
            mode="lines",
        )
        traces.append(trace)
    return traces


def delta_line2(df, y, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    unique_keys = get_unique_key_values(df, key)
    color_map = create_color_map(df, key)
    
    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        timestamp_stream = df_key[y].values.tolist()[0]

        tracker = RobustPrefixGapScore()
        scores = []
        for ts in timestamp_stream:
            tracker.update(ts)
            scores.append(tracker.score())
        # print(scores)
        # delta_list = [y_list[i+1] - y_list[i] for i in range(len(y_list)-1)]
        # delta_avg = [sum(delta_list[:i+1]) / (i+1) for i in range(len(delta_list))]

        trace = go.Scatter(
            x=list(range(len(scores))),
            y=scores,
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            marker_color=color_map[unique_key],
            mode="lines",
        )
        traces.append(trace)
    return traces


def predictions(df, y, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    """Create a scatter plot"""

    unique_keys = get_unique_key_values(df, key)
    color_map = create_color_map(df, key)
    # marker_map = create_marker_map(df, key)

    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        y_list = df_key[y].values.tolist()[0]
        trace = go.Scatter(
            x=list(range(len(y_list))),
            y=y_list,
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            mode="lines",
            marker_color=color_map[unique_key],
            # marker_symbol=marker_map[unique_key],
            # marker_line_width=1,
            # marker_size=5,
        )
        traces.append(trace)
    return traces

def delta_between_requests(df, y, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    unique_keys = get_unique_key_values(df, key)
    color_map = create_color_map(df, key)
    
    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        avg_delta_between_requests = df_key[y].values.tolist()[0]

        trace = go.Scatter(
            x=list(range(len(avg_delta_between_requests))),
            y=avg_delta_between_requests,
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            marker_color=color_map[unique_key],
            mode="lines",
        )
        traces.append(trace)
    return traces

def avg_entropy(df, y, key=None, showlegend=True, **kwargs) -> List[go.Scatter]:
    """Create a scatter plot"""

    unique_keys = get_unique_key_values(df, key)
    color_map = create_color_map(df, key)
    # marker_map = create_marker_map(df, key)

    traces = []
    for i, unique_key in enumerate(unique_keys):
        df_key = df[df[key] == unique_key]
        y_list = df_key[y].values.tolist()[0]
        trace = go.Scatter(
            x=list(range(len(y_list))),
            y=y_list,
            name=unique_key,
            legendgroup=unique_key,
            showlegend=showlegend,
            legendgrouptitle=dict(text=key) if i == 0 else None,
            mode="lines",
            marker_color=color_map[unique_key],
            # marker_symbol=marker_map[unique_key],
            # marker_line_width=1,
            # marker_size=5,
        )
        traces.append(trace)
    return traces
class RobustPrefixGapScore:
    def __init__(self, burst_window=3, multiplier=1, patience=2):
        self.history_size = 1000
        self.effective_gaps = []
        self.total_weighted_gap = 0.0
        self.n = 0
        self.prev_ts = None
        self.burst_window = burst_window
        self.multiplier = multiplier
        self.patience = patience
        self.consec_big_gaps = 0
        self.recent_burst_deltas = []

    def current_cap(self):
        if not self.recent_burst_deltas:
            return float('inf')  # Avoid clipping early on
        # median_gap = np.median(self.recent_burst_deltas)
        # return self.multiplier * median_gap
        min_gap = np.min(self.recent_burst_deltas)
        return self.multiplier * min_gap
    
    def update(self, timestamp):
        if self.prev_ts is None:
            self.prev_ts = timestamp
            return

        delta = timestamp - self.prev_ts
        self.prev_ts = timestamp
        cap_threshold = self.current_cap()

        # Check if this delta qualifies as part of the burst
        if delta <= cap_threshold:
            effective_gap = delta
            self.consec_big_gaps = 0
            self.recent_burst_deltas.append(delta)
            if len(self.recent_burst_deltas) > self.burst_window:
                self.recent_burst_deltas.pop(0)
        else:
            self.consec_big_gaps += 1
            if self.consec_big_gaps < self.patience:
                # Discount the large gap
                effective_gap = cap_threshold
            else:
                # Now accept the big gaps as real
                effective_gap = delta

        self.effective_gaps.append(effective_gap)
        if len(self.effective_gaps) > self.history_size:
            self.effective_gaps.pop(0)

        # self.total_weighted_gap += effective_gap
        # self.n += 1

    def score(self):
        # if self.n == 0:
        #     return 0.0
        if len(self.effective_gaps) == 0:
            return 0.0
        # avg_gap = self.total_weighted_gap / self.n
        # return avg_gap #1 / avg_gap  # Higher score = denser burst
        return np.mean(self.effective_gaps)