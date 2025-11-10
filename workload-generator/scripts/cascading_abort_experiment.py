#!/usr/bin/env python3
"""
Experiment to measure throughput under varying abort rates.
Tests abort rates from 5% to 50% in increments of 5%.
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

def abort_rate_experiment(ray_logs_dir):
    """
    Measure committed transactions per second under varying abort rates.
    Abort rates: 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50
    """
    BASELINES = [PIPELINED]  # Only test pipelined (cascading aborts only make sense with pipelined 2PC)
    ABORT_RATES = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]
    NUM_ITERATIONS = 3  # Run each configuration 3 times for statistical significance
    NUM_QUERIES = [2500]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["50"]  # High concurrency to create dependencies
    ZIPFIAN_CONSTANT = [0.9]  # High contention to create dependencies
    WORKLOAD_TYPE = ["custom"]

    # No background resolver load for simplicity
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
    experiment_name = f"cascading_aborts_{namespace}_{name}"

    config = {
        "baseline": BASELINES,
        "abort_rate": ABORT_RATES,  # New parameter
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
    }

    reporter = tune.CLIReporter(
        metric_columns=["throughput"],
        parameter_columns=[
            "baseline",
            "abort_rate",
            "num_keys",
            "max_concurrency",
            "iteration",
        ],
        max_report_frequency=20,
    )

    print(f"\\nStarting cascading abort experiment: {experiment_name}")
    print(f"Configurations to test: {len(ABORT_RATES)} abort rates x {NUM_ITERATIONS} iterations = {len(ABORT_RATES) * NUM_ITERATIONS} runs")
    print(f"Abort rates: {ABORT_RATES}")
    print()

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

    print(f"\\nExperiment completed: {experiment_name}")
    print(f"Results saved to: {ray_logs_dir}/{experiment_name}")

    # Print summary
    df = analysis.dataframe()
    if not df.empty:
        print("\\n=== SUMMARY ===")
        print(df[["config/abort_rate", "throughput", "total_transactions"]].groupby("config/abort_rate").agg(['mean', 'std']))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run cascading abort rate experiment")
    parser.add_argument(
        "--ray-logs-dir",
        type=str,
        default=str(Path.home() / "ray_results"),
        help="Directory to store ray logs",
    )
    args = parser.parse_args()

    ray_logs_dir = Path(args.ray_logs_dir)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    # Initialize ray
    ray.init(ignore_reinit_error=True)

    # Build servers before starting experiments (skip if already built)
    print("Skipping build - assuming servers already built")
    # atomix_setup.build_servers()
    atomix_setup.dump_servers_config()

    # Run the experiment
    abort_rate_experiment(ray_logs_dir)

    ray.shutdown()
