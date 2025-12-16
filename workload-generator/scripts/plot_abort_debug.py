#!/usr/bin/env python3
"""
Visualize abort patterns from debug_abort_experiment.py results.

This script creates visualizations showing:
1. Committed vs Aborted transaction counts per baseline
2. Cascading abort events (should only appear in Pipelined)
3. Sanity check that Traditional has regular aborts, Pipelined has cascading aborts

=== HOW TO REPRODUCE ===
1. First run the debug experiment:
   python3 workload-generator/scripts/debug_abort_experiment.py --baseline both

2. Copy results from ~/ray_results/debug_* to experiment_results/

3. Run this script:
   python3 plot_abort_debug.py <experiment_dir>

Generated files:
  - abort_sanity_check.html - Bar chart comparing committed/aborted/cascading
  - abort_summary.json - JSON summary of results
"""

import json
import sys
import os
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def load_experiment_results(experiment_dir):
    """Load results from a debug experiment directory."""
    results = []

    experiment_path = Path(experiment_dir)
    if not experiment_path.exists():
        print(f"Error: Directory {experiment_dir} does not exist")
        return results

    # Find all result.json files
    for result_file in experiment_path.rglob("result.json"):
        try:
            with open(result_file, 'r') as f:
                data = json.load(f)
                results.append({
                    "file": str(result_file),
                    "baseline": data.get("config", {}).get("baseline", "Unknown"),
                    "abort_rate": data.get("config", {}).get("abort_rate", 0),
                    "throughput": data.get("throughput", 0),
                    "total_transactions": data.get("total_transactions", 0),
                    "transaction_logs": data.get("transaction_logs", "{}"),
                })
        except Exception as e:
            print(f"Error loading {result_file}: {e}")

    return results


def parse_transaction_logs(tx_logs_str):
    """Parse transaction logs JSON string."""
    try:
        return json.loads(tx_logs_str)
    except:
        return {}


def create_sanity_check_plot(results, output_dir):
    """Create visualization comparing abort patterns between baselines."""

    # Group results by baseline and abort rate
    data = {}
    for r in results:
        baseline = r["baseline"]
        abort_rate = r["abort_rate"]
        key = (baseline, abort_rate)

        tx_logs = parse_transaction_logs(r["transaction_logs"])

        if key not in data:
            data[key] = {
                "baseline": baseline,
                "abort_rate": abort_rate,
                "committed": tx_logs.get("num_committed", 0),
                "aborted": tx_logs.get("num_aborted", 0),
                "cascading": tx_logs.get("num_cascading", 0),
                "throughput": r["throughput"],
            }

    if not data:
        print("No data to plot")
        return

    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Committed Transactions",
            "Aborted Transactions",
            "Cascading Abort Events",
            "Throughput Comparison"
        ),
        specs=[[{}, {}], [{}, {}]]
    )

    colors = {"Pipelined": "#2E86AB", "Traditional": "#E94F37"}

    # Sort data by baseline and abort rate
    sorted_keys = sorted(data.keys(), key=lambda x: (x[0], x[1]))

    for baseline in ["Pipelined", "Traditional"]:
        baseline_data = [(k, v) for k, v in data.items() if v["baseline"] == baseline]
        if not baseline_data:
            continue

        baseline_data.sort(key=lambda x: x[0][1])  # Sort by abort rate

        x_labels = [f"{d['abort_rate']*100:.0f}%" for _, d in baseline_data]
        committed = [d["committed"] for _, d in baseline_data]
        aborted = [d["aborted"] for _, d in baseline_data]
        cascading = [d["cascading"] for _, d in baseline_data]
        throughput = [d["throughput"] for _, d in baseline_data]

        color = colors.get(baseline, "#888888")

        # Committed transactions
        fig.add_trace(
            go.Bar(name=baseline, x=x_labels, y=committed, marker_color=color,
                   legendgroup=baseline, showlegend=True),
            row=1, col=1
        )

        # Aborted transactions
        fig.add_trace(
            go.Bar(name=baseline, x=x_labels, y=aborted, marker_color=color,
                   legendgroup=baseline, showlegend=False),
            row=1, col=2
        )

        # Cascading abort events
        fig.add_trace(
            go.Bar(name=baseline, x=x_labels, y=cascading, marker_color=color,
                   legendgroup=baseline, showlegend=False),
            row=2, col=1
        )

        # Throughput
        fig.add_trace(
            go.Bar(name=baseline, x=x_labels, y=throughput, marker_color=color,
                   legendgroup=baseline, showlegend=False),
            row=2, col=2
        )

    fig.update_layout(
        title_text="Abort Pattern Sanity Check: Pipelined vs Traditional",
        barmode='group',
        height=800,
        showlegend=True,
    )

    fig.update_xaxes(title_text="Abort Rate", row=1, col=1)
    fig.update_xaxes(title_text="Abort Rate", row=1, col=2)
    fig.update_xaxes(title_text="Abort Rate", row=2, col=1)
    fig.update_xaxes(title_text="Abort Rate", row=2, col=2)

    fig.update_yaxes(title_text="Count", row=1, col=1)
    fig.update_yaxes(title_text="Count", row=1, col=2)
    fig.update_yaxes(title_text="Events", row=2, col=1)
    fig.update_yaxes(title_text="tx/s", row=2, col=2)

    # Add annotations for sanity check
    annotations = []

    # Check if cascading aborts are as expected
    pipelined_cascading = sum(d["cascading"] for k, d in data.items()
                               if d["baseline"] == "Pipelined" and d["abort_rate"] > 0)
    traditional_cascading = sum(d["cascading"] for k, d in data.items()
                                 if d["baseline"] == "Traditional" and d["abort_rate"] > 0)

    sanity_text = []
    if pipelined_cascading > 0:
        sanity_text.append("✓ Pipelined has cascading aborts (expected)")
    else:
        sanity_text.append("⚠ Pipelined should have cascading aborts")

    if traditional_cascading == 0:
        sanity_text.append("✓ Traditional has no cascading aborts (expected)")
    else:
        sanity_text.append("⚠ Traditional should NOT have cascading aborts")

    fig.add_annotation(
        text="<br>".join(sanity_text),
        xref="paper", yref="paper",
        x=0.5, y=-0.1,
        showarrow=False,
        font=dict(size=14),
        align="center"
    )

    output_path = Path(output_dir) / "abort_sanity_check.html"
    fig.write_html(str(output_path))
    print(f"Saved: {output_path}")

    return data


def create_summary_json(data, output_dir):
    """Create JSON summary of results."""
    summary = {
        "sanity_check": {
            "pipelined_has_cascading": False,
            "traditional_no_cascading": True,
        },
        "results": []
    }

    for (baseline, abort_rate), d in data.items():
        entry = {
            "baseline": baseline,
            "abort_rate": abort_rate,
            "committed": d["committed"],
            "aborted": d["aborted"],
            "cascading_events": d["cascading"],
            "throughput": d["throughput"],
        }
        summary["results"].append(entry)

        if baseline == "Pipelined" and d["cascading"] > 0:
            summary["sanity_check"]["pipelined_has_cascading"] = True
        if baseline == "Traditional" and d["cascading"] > 0:
            summary["sanity_check"]["traditional_no_cascading"] = False

    output_path = Path(output_dir) / "abort_summary.json"
    with open(output_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"Saved: {output_path}")

    # Print summary
    print("\n" + "="*60)
    print("SANITY CHECK RESULTS")
    print("="*60)
    if summary["sanity_check"]["pipelined_has_cascading"]:
        print("✓ Pipelined 2PC has cascading aborts (EXPECTED)")
    else:
        print("⚠ Pipelined 2PC should have cascading aborts but none detected")

    if summary["sanity_check"]["traditional_no_cascading"]:
        print("✓ Traditional 2PC has no cascading aborts (EXPECTED)")
    else:
        print("⚠ Traditional 2PC should NOT have cascading aborts but some detected")
    print("="*60)


def main():
    if len(sys.argv) < 2:
        # Try to find most recent debug experiment
        ray_results = Path.home() / "ray_results"
        if ray_results.exists():
            debug_dirs = list(ray_results.glob("debug_*"))
            if debug_dirs:
                experiment_dir = max(debug_dirs, key=os.path.getmtime)
                print(f"Using most recent debug experiment: {experiment_dir}")
            else:
                print("Usage: python plot_abort_debug.py <experiment_dir>")
                print("No debug experiments found in ~/ray_results/")
                sys.exit(1)
        else:
            print("Usage: python plot_abort_debug.py <experiment_dir>")
            sys.exit(1)
    else:
        experiment_dir = sys.argv[1]

    output_dir = experiment_dir

    print(f"Loading results from: {experiment_dir}")
    results = load_experiment_results(experiment_dir)

    if not results:
        print("No results found!")
        sys.exit(1)

    print(f"Found {len(results)} result files")

    data = create_sanity_check_plot(results, output_dir)
    if data:
        create_summary_json(data, output_dir)


if __name__ == "__main__":
    main()
