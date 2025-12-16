#!/usr/bin/env python3
"""
Experiment to measure goodput under varying abort rates with FIXED DURATION.
Each run executes for 5 minutes (300 seconds) to ensure fair comparison.

This fixes the measurement issue where experiments with higher abort rates
finished faster (aborts are quick), artificially inflating throughput numbers.
"""

import psutil
from math import prod
import subprocess
from pathlib import Path
import ray
from ray import tune
from coolname import generate_slug
import argparse
from ray.tune.schedulers import FIFOScheduler
from ray_task import run_workload
from grid_searcher import GridSearcherInOrder
from atomix_setup import atomix_setup
from utils import *

# Fixed duration for all runs: 1 minute = 60 seconds
DURATION_SECONDS = 60


def run_experiment(ray_logs_dir, baseline_type, zipf_exponent):
    """
    Run experiment for a specific baseline and zipf exponent.
    Uses fixed 2-minute duration instead of fixed transaction count.
    """
    BASELINES = [PIPELINED if baseline_type == "pipelined" else TRADITIONAL]
    ABORT_RATES = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50]
    NUM_ITERATIONS = 1  # Run only 1 iteration per config to avoid server reuse issues
    NUM_QUERIES = [None]  # None = run indefinitely until SIGUSR1
    NUM_KEYS = [50]  # 50 keys for consistency
    MAX_CONCURRENCY = ["50"]
    ZIPFIAN_CONSTANT = [zipf_exponent]
    WORKLOAD_TYPE = ["custom"]

    # No background resolver load
    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "0",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        },
    ]

    RESOLVER_CAPACITY = [
        {
            "cpu_percentage": 1,
            "background_runtime_core_ids": [1],
        },
    ]

    namespace, name = generate_slug(2).split("-")
    experiment_name = f"{baseline_type}_zipf_{zipf_exponent}_fixed_{namespace}_{name}"

    config = {
        "baseline": BASELINES,
        "abort_rate": ABORT_RATES,
        "num_keys": NUM_KEYS,
        "max_concurrency": MAX_CONCURRENCY,
        "resolver_capacity": RESOLVER_CAPACITY,
        "resolver_tx_load": RESOLVER_TX_LOAD,
        "num_queries": NUM_QUERIES,
        "zipf_exponent": ZIPFIAN_CONSTANT,
        "namespace": [namespace],
        "name": [name],
        "background_runtime_core_ids": [list(range(3, 32))],
        "workload_type": WORKLOAD_TYPE,
        "duration_seconds": [DURATION_SECONDS],  # Fixed 5 minute duration
        "collect_dependency_tree": [True],  # Enable dependency tree collection for Pipelined
    }

    reporter = tune.CLIReporter(
        metric_columns=["throughput", "total_transactions"],
        parameter_columns=[
            "baseline",
            "abort_rate",
            "zipf_exponent",
            "iteration",
        ],
        max_report_frequency=20,
    )

    print(f"\n{'='*60}")
    print(f"Starting {baseline_type.upper()} experiment: {experiment_name}")
    print(f"Zipf exponent: {zipf_exponent}")
    print(f"Duration: {DURATION_SECONDS} seconds per trial")
    print(f"Total runs: {len(ABORT_RATES)} abort rates = {len(ABORT_RATES)} runs")
    print(f"Abort rates: {ABORT_RATES}")
    print(f"{'='*60}\n")

    analysis = tune.run(
        tune.with_parameters(run_workload),
        config={},
        num_samples=prod([len(v) for v in list(config.values())]) * NUM_ITERATIONS,
        resources_per_trial={"cpu": psutil.cpu_count()},
        storage_path=ray_logs_dir,
        name=experiment_name,
        search_alg=GridSearcherInOrder(
            atomix_setup, NUM_ITERATIONS, config, experiment_name, ray_logs_dir
        ),
        reuse_actors=True,
        max_concurrent_trials=1,
        scheduler=FIFOScheduler(),
        verbose=1,
        progress_reporter=reporter,
    )

    print(f"\nExperiment completed: {experiment_name}")
    print(f"Results saved to: {ray_logs_dir}/{experiment_name}")

    # Print summary
    df = analysis.dataframe()
    if not df.empty:
        print("\n=== SUMMARY ===")
        summary = df[["config/abort_rate", "throughput", "total_transactions"]].groupby("config/abort_rate").agg(['mean', 'std'])
        print(summary)

    return experiment_name


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run fixed-duration abort rate experiments")
    parser.add_argument(
        "--ray-logs-dir",
        type=str,
        default=str(Path.home() / "ray_results"),
        help="Directory to store ray logs",
    )
    parser.add_argument(
        "--baseline",
        type=str,
        choices=["pipelined", "traditional", "both"],
        default="both",
        help="Which baseline to test",
    )
    parser.add_argument(
        "--zipf",
        type=float,
        choices=[0.1, 0.3, 0.6],
        default=None,
        help="Zipf exponent (if not specified, runs all: 0.1, 0.3, 0.6)",
    )
    args = parser.parse_args()

    ray_logs_dir = Path(args.ray_logs_dir)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    # Initialize ray
    ray.init(ignore_reinit_error=True)

    # Build servers before starting experiments
    print("Skipping build - assuming servers already built")
    atomix_setup.dump_servers_config()

    # Determine which zipf values to run
    zipf_values = [args.zipf] if args.zipf else [0.1, 0.3, 0.6]
    baselines = ["pipelined", "traditional"] if args.baseline == "both" else [args.baseline]

    experiment_names = []
    for zipf in zipf_values:
        for baseline in baselines:
            exp_name = run_experiment(ray_logs_dir, baseline, zipf)
            experiment_names.append(exp_name)

    print("\n" + "="*60)
    print("ALL EXPERIMENTS COMPLETED")
    print("="*60)
    for name in experiment_names:
        print(f"  - {name}")
    print("="*60)

    ray.shutdown()
