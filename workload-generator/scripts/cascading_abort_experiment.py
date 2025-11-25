#!/usr/bin/env python3
"""
Experiment to measure throughput under varying abort rates.
Quick test with abort rates: 5%, 15%, 30%.
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

def pipelined_abort_experiment(ray_logs_dir):
    """
    Measure Pipelined 2PC throughput under varying abort rates.
    Uses dependency workload to create read-write conflicts that lead to cascading aborts.
    """
    BASELINES = [PIPELINED]  # Test pipelined to see cascading aborts
    ABORT_RATES = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50]
    NUM_ITERATIONS = 3  # Run each configuration 3 times
    NUM_QUERIES = [2500]
    NUM_KEYS = [5]  # Small key space to maximize conflicts
    MAX_CONCURRENCY = ["50"]  # High concurrency to create dependencies
    ZIPFIAN_CONSTANT = [0.9]  # High skew to concentrate writes on hot keys
    WORKLOAD_TYPE = ["dependency"]  # Use new dependency workload type!

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

    print(f"\\nStarting Pipelined 2PC abort experiment: {experiment_name}")
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


def traditional_abort_experiment(ray_logs_dir):
    """
    Measure Traditional 2PC throughput under varying abort rates.
    Traditional 2PC holds locks until commit, so no cascading aborts occur.
    Uses dependency workload for fair comparison with Pipelined 2PC.
    """
    BASELINES = [TRADITIONAL]  # Test Traditional 2PC (no cascading aborts)
    ABORT_RATES = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50]
    NUM_ITERATIONS = 3  # Run each configuration 3 times
    NUM_QUERIES = [2500]
    NUM_KEYS = [5]  # Small key space to maximize conflicts
    MAX_CONCURRENCY = ["50"]  # High concurrency
    ZIPFIAN_CONSTANT = [0.9]  # High skew to concentrate writes on hot keys
    WORKLOAD_TYPE = ["dependency"]  # Use dependency workload for fair comparison

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
    experiment_name = f"traditional_aborts_{namespace}_{name}"

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

    print(f"\\nStarting Traditional 2PC abort experiment: {experiment_name}")
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
    parser = argparse.ArgumentParser(description="Run abort rate experiments")
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
        default="pipelined",
        help="Which baseline to test: pipelined, traditional, or both",
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

    # Run the experiment(s)
    if args.baseline in ["pipelined", "both"]:
        pipelined_abort_experiment(ray_logs_dir)
    if args.baseline in ["traditional", "both"]:
        traditional_abort_experiment(ray_logs_dir)

    ray.shutdown()
