#!/usr/bin/env python3
"""
Debug version of abort experiment that produces detailed transaction logs.
This allows visualizing:
1. Which transactions committed vs aborted
2. Dependency trees between transactions (for cascading aborts in Pipelined)
3. Sanity check that Traditional has regular aborts, Pipelined has cascading aborts

=== HOW TO REPRODUCE ===
1. Run on CloudLab:
   cd ~/sangria
   python3 workload-generator/scripts/debug_abort_experiment.py --baseline both

2. Results are saved to ~/ray_results/ with detailed transaction logs

3. Run the plotter to visualize:
   python3 workload-generator/scripts/plot_abort_debug.py
"""

import psutil
import json
import os
from math import prod
import subprocess
from pathlib import Path
import ray
from ray import tune
from coolname import generate_slug
import argparse
from ray.tune.schedulers import FIFOScheduler
from grid_searcher import GridSearcherInOrder
from atomix_setup import atomix_setup
from utils import *
import time
import re


def parse_metrics_with_logs(output):
    """Parse metrics and extract transaction logs from output."""
    metrics = {}

    # Find the metrics section between METRICS_START and METRICS_END
    metrics_section = re.search(r"METRICS_START\n(.*?)\nMETRICS_END", output, re.DOTALL)
    if not metrics_section:
        print("Warning: No metrics section found in output")
        return {"throughput": 0.0}

    metrics_text = metrics_section.group(1)

    # Regular expressions to match the metrics
    patterns = {
        "throughput": r"Throughput: ([\d\.]+) transactions/second",
        "avg_latency": r"Average Latency: ([\d\.]+[µnm]?s)",
        "p50_latency": r"P50 Latency: ([\d\.]+[µnm]?s)",
        "p95_latency": r"P95 Latency: ([\d\.]+[µnm]?s)",
        "p99_latency": r"P99 Latency: ([\d\.]+[µnm]?s)",
        "total_duration": r"Total Duration: ([\d\.]+[µnm]?s)",
        "total_transactions": r"Total Transactions: (\d+)",
        "resolver_stats": r"Resolver stats: (.*)",
        "range_server_stats": r"Range server stats: (.*)",
    }

    for metric, pattern in patterns.items():
        match = re.search(pattern, metrics_text)
        if match:
            value = match.group(1)
            if metric == "resolver_stats":
                metrics[metric] = value
            elif metric == "range_server_stats":
                metrics[metric] = value
            elif "s" in value:
                if "µs" in value:
                    metrics[metric] = float(value.replace("µs", "")) / 1_000_000
                elif "ms" in value:
                    metrics[metric] = float(value.replace("ms", "")) / 1_000
                elif "ns" in value:
                    metrics[metric] = float(value.replace("ns", "")) / 1_000_000_000
                else:
                    metrics[metric] = float(value.replace("s", ""))
            else:
                metrics[metric] = float(value)

    # Also extract transaction logs from stderr if present
    return metrics


def run_debug_workload(config):
    """Run workload and collect detailed transaction logs."""
    global atomix_setup

    iteration = config["iteration"]
    baseline = config["baseline"]
    resolver_background_runtime_core_ids = config["resolver_capacity"]["background_runtime_core_ids"]
    resolver_cpu_percentage = config["resolver_capacity"]["cpu_percentage"]
    resolver_cores = config["resolver_cores"]
    resolver_tx_load = config["resolver_tx_load"]
    main_num_keys = config["num_keys"]
    main_max_concurrency = config["max_concurrency"]
    main_num_queries = config["num_queries"]
    main_name = config["name"]
    main_background_runtime_core_ids = config["background_runtime_core_ids"]
    workload_type = config["workload_type"]
    abort_rate = config.get("abort_rate", 0.0)
    duration_seconds = config.get("duration_seconds", None)

    del config["iteration"]
    del config["baseline"]
    del config["resolver_capacity"]
    del config["resolver_cores"]
    del config["resolver_tx_load"]
    del config["resolver_tx_load_concurrency"]
    if "abort_rate" in config:
        del config["abort_rate"]
    if "duration_seconds" in config:
        del config["duration_seconds"]

    # Main workload generator
    cmd1 = [
        TARGET_RUN_CMD + "workload-generator",
        "--workload-config",
        str(MAIN_RAY_WORKLOAD_CONFIG_PATH),
        "--config",
        str(RAY_SERVERS_CONFIG_PATH),
    ]

    if iteration == 0:
        if workload_type == "ycsb":
            atomix_setup.servers_config["heuristic"] = "LockContention"
        else:
            atomix_setup.servers_config["heuristic"] = "OpenClients"

        atomix_setup.servers_config["commit_strategy"] = baseline
        atomix_setup.servers_config["resolver"]["cpu_percentage"] = resolver_cpu_percentage
        atomix_setup.servers_config["resolver"]["background_runtime_core_ids"] = resolver_background_runtime_core_ids
        atomix_setup.servers_config["range_server"]["artificial_abort_rate"] = abort_rate

        # Enable verbose logging for transaction tracking
        atomix_setup.servers_config["resolver"]["verbose_logging"] = True

        atomix_setup.dump_servers_config()
        atomix_setup.kill_servers()
        atomix_setup.reset_cassandra()
        atomix_setup.start_servers()

        config["fake_transactions"] = False
        os.makedirs(os.path.dirname(MAIN_RAY_WORKLOAD_CONFIG_PATH), exist_ok=True)
        with open(MAIN_RAY_WORKLOAD_CONFIG_PATH, "w") as f:
            json.dump(config, f)
        cmd1.append("--create-keyspace")

    # Add back config params for ray reporting
    config["iteration"] = iteration
    config["baseline"] = baseline
    config["resolver_cores"] = resolver_cores
    config["resolver_tx_load_concurrency"] = resolver_tx_load["max_concurrency"]
    config["abort_rate"] = abort_rate
    if duration_seconds:
        config["duration_seconds"] = duration_seconds

    print("cmd1: ", cmd1)
    print(f"Baseline: {baseline}, Abort Rate: {abort_rate}")
    print(f"Duration mode: {'fixed ' + str(duration_seconds) + 's' if duration_seconds else 'num_queries based'}")

    transaction_logs = {
        "baseline": baseline,
        "abort_rate": abort_rate,
        "transactions": [],
        "resolver_state": {},
    }

    try:
        # Run with RUST_LOG=info to get transaction-level logs
        env = {**os.environ, "RUST_LOG": "info"}

        process1 = subprocess.Popen(
            cmd1,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        try:
            if duration_seconds:
                print(f"Running for {duration_seconds} seconds...")
                time.sleep(duration_seconds)
                print(f"Duration complete, sending SIGUSR1 to stop workload...")
                process1.send_signal(subprocess.signal.SIGUSR1)
                stdout1, stderr1 = process1.communicate(timeout=60)
            else:
                stdout1, stderr1 = process1.communicate(timeout=60 * 60)

            # Parse metrics
            metrics = parse_metrics_with_logs(stdout1)

            # Parse transaction logs from stderr
            # Look for patterns like:
            # - "Transaction X finally committed!"
            # - "Transaction X was aborted"
            # - "Cascading abort to dependent transaction X"
            # - "Dependency X was aborted, transaction Y must also abort"

            committed_txs = []
            aborted_txs = []
            cascading_aborts = []
            dependencies = {}

            for line in stderr1.split('\n'):
                # Committed transactions
                if "finally committed" in line:
                    match = re.search(r'Transaction ([a-f0-9-]+) finally committed', line)
                    if match:
                        committed_txs.append(match.group(1))

                # Aborted transactions
                if "was aborted" in line or "marked as resolved (aborted" in line:
                    match = re.search(r'Transaction ([a-f0-9-]+)', line)
                    if match:
                        tx_id = match.group(1)
                        if tx_id not in aborted_txs:
                            aborted_txs.append(tx_id)

                # Cascading aborts
                if "Cascading abort" in line or "cascading abort" in line:
                    match = re.search(r'cascading abort to (\d+) dependents', line, re.IGNORECASE)
                    if match:
                        cascading_aborts.append({
                            "num_dependents": int(match.group(1)),
                            "line": line
                        })

                # Dependencies
                if "Dependency" in line and "was aborted" in line:
                    match = re.search(r'Dependency ([a-f0-9-]+) was aborted.*transaction ([a-f0-9-]+)', line)
                    if match:
                        dep_id = match.group(1)
                        tx_id = match.group(2)
                        if tx_id not in dependencies:
                            dependencies[tx_id] = []
                        dependencies[tx_id].append(dep_id)

            transaction_logs["committed"] = committed_txs
            transaction_logs["aborted"] = aborted_txs
            transaction_logs["cascading_aborts"] = cascading_aborts
            transaction_logs["dependencies"] = dependencies
            transaction_logs["num_committed"] = len(committed_txs)
            transaction_logs["num_aborted"] = len(aborted_txs)
            transaction_logs["num_cascading"] = len(cascading_aborts)

            # Save detailed logs
            metrics["transaction_logs"] = json.dumps(transaction_logs)

            print(f"\n=== Transaction Summary ({baseline}) ===")
            print(f"Committed: {len(committed_txs)}")
            print(f"Aborted: {len(aborted_txs)}")
            print(f"Cascading abort events: {len(cascading_aborts)}")
            if baseline == PIPELINED and cascading_aborts:
                print(f"  -> Cascading aborts detected (as expected for Pipelined)")
            elif baseline == TRADITIONAL and not cascading_aborts:
                print(f"  -> No cascading aborts (as expected for Traditional)")
            print()

        except subprocess.TimeoutExpired:
            process1.kill()
            print(f"Timeout exceeded for config: {config}")
            metrics = {"throughput": 0.0}

    except Exception as e:
        print(f"Error running workloads: {e}")
        metrics = {"throughput": 0.0}

    return metrics


def debug_abort_experiment(ray_logs_dir, baseline_type):
    """
    Run debug abort experiment with detailed transaction logging.
    """
    if baseline_type == "pipelined":
        BASELINES = [PIPELINED]
        experiment_prefix = "debug_pipelined"
    elif baseline_type == "traditional":
        BASELINES = [TRADITIONAL]
        experiment_prefix = "debug_traditional"
    else:  # both
        BASELINES = [PIPELINED, TRADITIONAL]
        experiment_prefix = "debug_comparison"

    # Use fewer queries for debug run
    ABORT_RATES = [0.0, 0.30]  # Just test 0% and 30% for quick sanity check
    NUM_ITERATIONS = 1  # Single run for debugging
    NUM_QUERIES = [500]  # Fewer queries for faster debug run
    NUM_KEYS = [5]  # Small key space
    MAX_CONCURRENCY = ["20"]  # Lower concurrency for clearer logs
    ZIPFIAN_CONSTANT = [0.9]
    WORKLOAD_TYPE = ["dependency"]  # Use dependency workload for conflicts

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
    experiment_name = f"{experiment_prefix}_{namespace}_{name}"

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
        metric_columns=["throughput", "total_transactions"],
        parameter_columns=[
            "baseline",
            "abort_rate",
            "iteration",
        ],
        max_report_frequency=20,
    )

    print(f"\n{'='*60}")
    print(f"Starting DEBUG abort experiment: {experiment_name}")
    print(f"{'='*60}")
    print(f"Baselines: {BASELINES}")
    print(f"Abort rates: {ABORT_RATES}")
    print(f"Num queries: {NUM_QUERIES[0]} (reduced for debug)")
    print(f"This experiment will produce detailed transaction logs")
    print(f"to verify cascading aborts in Pipelined vs regular aborts in Traditional")
    print()

    analysis = tune.run(
        tune.with_parameters(run_debug_workload),
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

    # Print summary with transaction log analysis
    df = analysis.dataframe()
    if not df.empty:
        print("\n" + "="*60)
        print("=== DEBUG SUMMARY ===")
        print("="*60)

        for _, row in df.iterrows():
            baseline = row.get("config/baseline", "Unknown")
            abort_rate = row.get("config/abort_rate", 0)
            throughput = row.get("throughput", 0)

            print(f"\n{baseline} @ {abort_rate*100:.0f}% abort rate:")
            print(f"  Throughput: {throughput:.2f} tx/s")

            # Parse transaction logs if available
            tx_logs_str = row.get("transaction_logs", "{}")
            try:
                tx_logs = json.loads(tx_logs_str)
                print(f"  Committed: {tx_logs.get('num_committed', 'N/A')}")
                print(f"  Aborted: {tx_logs.get('num_aborted', 'N/A')}")
                print(f"  Cascading abort events: {tx_logs.get('num_cascading', 'N/A')}")

                if baseline == PIPELINED and tx_logs.get('num_cascading', 0) > 0:
                    print(f"  ✓ Cascading aborts detected (expected for Pipelined)")
                elif baseline == TRADITIONAL and tx_logs.get('num_cascading', 0) == 0:
                    print(f"  ✓ No cascading aborts (expected for Traditional)")
                elif abort_rate == 0:
                    print(f"  ✓ No aborts expected at 0% abort rate")
            except:
                pass

        print("\n" + "="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run debug abort experiment with transaction logging")
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

    ray.init(ignore_reinit_error=True)

    print("Skipping build - assuming servers already built")
    atomix_setup.dump_servers_config()

    debug_abort_experiment(ray_logs_dir, args.baseline)

    ray.shutdown()
