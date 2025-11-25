#!/usr/bin/env python3
"""
Latency experiment to measure average latency of committed transactions
under varying abort rates. Fixed Zipf=0.3, abort rates: 0%, 10%, 30%.

This experiment verifies that Pipelined maintains lower latency than Traditional
even when dealing with cascading aborts.
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

# Fixed duration for all runs: 60 seconds
DURATION_SECONDS = 60


def run_experiment(ray_logs_dir, baseline_type):
    """
    Run latency experiment for a specific baseline.
    Fixed Zipf=0.3, abort rates: 0%, 10%, 30%
    """
    BASELINES = [PIPELINED if baseline_type == "pipelined" else TRADITIONAL]
    ABORT_RATES = [0.0, 0.10, 0.30]  # Just 3 abort rates for quick comparison
    NUM_ITERATIONS = 1
    NUM_QUERIES = [None]  # Run until SIGUSR1
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["50"]
    ZIPFIAN_CONSTANT = [0.3]  # Fixed Zipf
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
    experiment_name = f"latency_{baseline_type}_{namespace}_{name}"

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
        "duration_seconds": [DURATION_SECONDS],
    }

    reporter = tune.CLIReporter(
        metric_columns=["throughput", "total_transactions", "avg_latency", "p50_latency", "p95_latency", "p99_latency"],
        parameter_columns=[
            "baseline",
            "abort_rate",
        ],
        max_report_frequency=20,
    )

    print(f"\n{'='*60}")
    print(f"Starting LATENCY experiment: {experiment_name}")
    print(f"Baseline: {baseline_type.upper()}")
    print(f"Zipf: 0.3 (fixed)")
    print(f"Abort rates: {ABORT_RATES}")
    print(f"Duration: {DURATION_SECONDS} seconds per trial")
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

    # Print detailed latency summary
    df = analysis.dataframe()
    if not df.empty:
        print("\n" + "="*70)
        print(f"LATENCY SUMMARY - {baseline_type.upper()}")
        print("="*70)
        print(f"{'Abort%':<10} {'Throughput':<12} {'Avg Latency':<15} {'P50':<12} {'P95':<12} {'P99':<12}")
        print("-"*70)
        for _, row in df.iterrows():
            abort = row.get("config/abort_rate", 0) * 100
            tput = row.get("throughput", 0)
            avg_lat = row.get("avg_latency", 0) * 1000  # Convert to ms
            p50 = row.get("p50_latency", 0) * 1000
            p95 = row.get("p95_latency", 0) * 1000
            p99 = row.get("p99_latency", 0) * 1000
            print(f"{int(abort):<10} {tput:<12.2f} {avg_lat:<15.3f}ms {p50:<12.3f}ms {p95:<12.3f}ms {p99:<12.3f}ms")
        print("="*70)

    return experiment_name, df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run latency experiments")
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
    args = parser.parse_args()

    ray_logs_dir = Path(args.ray_logs_dir)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    # Initialize ray
    ray.init(ignore_reinit_error=True)

    # Build servers before starting experiments
    print("Skipping build - assuming servers already built")
    atomix_setup.dump_servers_config()

    baselines = ["pipelined", "traditional"] if args.baseline == "both" else [args.baseline]

    all_results = {}
    experiment_names = []

    for baseline in baselines:
        exp_name, df = run_experiment(ray_logs_dir, baseline)
        experiment_names.append(exp_name)
        all_results[baseline] = df

    # Print comparison if both baselines were run
    if len(all_results) == 2:
        print("\n" + "="*80)
        print("LATENCY COMPARISON: PIPELINED vs TRADITIONAL (Zipf=0.3)")
        print("="*80)

        pipe_df = all_results["pipelined"]
        trad_df = all_results["traditional"]

        print(f"{'Abort%':<8} {'Pipe Avg':<12} {'Trad Avg':<12} {'Speedup':<10} {'Pipe Tput':<12} {'Trad Tput':<12} {'Tput Ratio'}")
        print("-"*80)

        for abort_rate in [0.0, 0.10, 0.30]:
            pipe_row = pipe_df[pipe_df["config/abort_rate"] == abort_rate].iloc[0] if not pipe_df.empty else None
            trad_row = trad_df[trad_df["config/abort_rate"] == abort_rate].iloc[0] if not trad_df.empty else None

            if pipe_row is not None and trad_row is not None:
                pipe_lat = pipe_row.get("avg_latency", 0) * 1000
                trad_lat = trad_row.get("avg_latency", 0) * 1000
                lat_speedup = trad_lat / pipe_lat if pipe_lat > 0 else 0

                pipe_tput = pipe_row.get("throughput", 0)
                trad_tput = trad_row.get("throughput", 0)
                tput_ratio = pipe_tput / trad_tput if trad_tput > 0 else 0

                print(f"{int(abort_rate*100):<8} {pipe_lat:<12.3f}ms {trad_lat:<12.3f}ms {lat_speedup:<10.2f}x {pipe_tput:<12.2f} {trad_tput:<12.2f} {tput_ratio:.2f}x")

        print("="*80)

    print("\n" + "="*60)
    print("ALL EXPERIMENTS COMPLETED")
    print("="*60)
    for name in experiment_names:
        print(f"  - {name}")
    print("="*60)

    ray.shutdown()
