#!/usr/bin/env python3
"""
CONTENTION TEST: Run tests with varying contention levels to observe cascading aborts

Batch 1: LOW contention (baseline) - zipf=0.6, 10 keys, concurrency=20
Batch 2: HIGH contention - zipf=0.99, 5 keys, concurrency=50
Batch 3: DEPENDENCY workload - zipf=0.9, 5 keys, concurrency=30, workload_type=dependency

Each batch runs 1000 transactions at abort rates: 0%, 10%, 20%, 30%, 40%, 50%, 60%
"""

import json
import subprocess
import sys
import os
import time
import re
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from atomix_setup import atomix_setup
from utils import *

# Test configuration
NUM_TRANSACTIONS = 1000
TIMEOUT_SECONDS = 300
ABORT_RATES = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60]

# Test batches with different contention configurations
TEST_BATCHES = {
    "low_contention": {
        "description": "LOW contention (baseline)",
        "num_keys": 10,
        "zipf_exponent": 0.6,
        "max_concurrency": "20",
        "workload_type": "custom"
    },
    "high_contention": {
        "description": "HIGH contention (extreme hotspots)",
        "num_keys": 5,
        "zipf_exponent": 0.99,
        "max_concurrency": "50",
        "workload_type": "custom"
    },
    "dependency_workload": {
        "description": "DEPENDENCY workload (designed for chains)",
        "num_keys": 5,
        "zipf_exponent": 0.9,
        "max_concurrency": "30",
        "workload_type": "dependency"
    }
}


def log_message(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")
    sys.stdout.flush()


def parse_metrics_from_output(output):
    metrics = {
        "throughput": 0.0,
        "total_transactions": 0,
    }
    if "METRICS_START" in output and "METRICS_END" in output:
        start = output.index("METRICS_START")
        end = output.index("METRICS_END")
        metrics_block = output[start:end]
        for line in metrics_block.split("\n"):
            if "Throughput:" in line:
                match = re.search(r"Throughput:\s*([\d.]+)", line)
                if match:
                    metrics["throughput"] = float(match.group(1))
            elif "Total Transactions:" in line:
                match = re.search(r"Total Transactions:\s*(\d+)", line)
                if match:
                    metrics["total_transactions"] = int(match.group(1))
    return metrics


def count_from_logs():
    counts = {"artificial_aborts": 0, "resolver_aborts": 0}
    rangeserver_log = ROOT_DIR / "log-rangeserver.txt"
    if rangeserver_log.exists():
        with open(rangeserver_log, 'r') as f:
            counts["artificial_aborts"] = f.read().count("Artificially aborting transaction")
    resolver_log = ROOT_DIR / "log-resolver.txt"
    if resolver_log.exists():
        with open(resolver_log, 'r') as f:
            counts["resolver_aborts"] = f.read().count("cascading to dependents")
    return counts


def run_single_test(baseline, abort_rate, batch_config):
    """Run a single test with given configuration."""
    log_message(f"RUNNING: {baseline} @ {abort_rate*100:.0f}% abort rate")

    result = {
        "baseline": baseline,
        "abort_rate": abort_rate,
        "committed": 0,
        "expected_transactions": NUM_TRANSACTIONS,
        "success": False,
    }

    try:
        # Configure servers
        atomix_setup.servers_config['commit_strategy'] = baseline
        atomix_setup.servers_config['range_server']['artificial_abort_rate'] = abort_rate
        atomix_setup.servers_config['heuristic'] = "OpenClients"
        atomix_setup.dump_servers_config()

        # Kill, reset, start servers
        atomix_setup.kill_servers()
        time.sleep(2)
        atomix_setup.reset_cassandra()
        time.sleep(2)
        atomix_setup.start_servers()
        time.sleep(3)

        # Create workload config
        workload_config = {
            "num_queries": NUM_TRANSACTIONS,
            "num_keys": batch_config["num_keys"],
            "zipf_exponent": batch_config["zipf_exponent"],
            "namespace": "contention",
            "name": f"test-{baseline.lower()}-{int(abort_rate*100)}",
            "max_concurrency": batch_config["max_concurrency"],
            "seed": 12345,
            "background_runtime_core_ids": [],
            "fake_transactions": False,
            "workload_type": batch_config["workload_type"]
        }

        workload_config_path = Path("/tmp/contention_workload.json")
        with open(workload_config_path, 'w') as f:
            json.dump(workload_config, f, indent=2)

        # Run workload
        cmd = [
            TARGET_RUN_CMD + "workload-generator",
            "--workload-config", str(workload_config_path),
            "--config", str(RAY_SERVERS_CONFIG_PATH),
            "--create-keyspace"
        ]

        start_time = time.time()
        proc = subprocess.run(
            cmd, cwd=ROOT_DIR, capture_output=True, text=True,
            timeout=TIMEOUT_SECONDS, env={**os.environ, "RUST_LOG": "info"}
        )
        duration = time.time() - start_time

        metrics = parse_metrics_from_output(proc.stdout)
        log_counts = count_from_logs()

        result["committed"] = metrics["total_transactions"]
        result["throughput"] = metrics["throughput"]
        result["artificial_aborts"] = log_counts["artificial_aborts"]
        result["duration"] = duration

        # Compute cascaded
        total_aborts = NUM_TRANSACTIONS - result["committed"]
        result["cascaded"] = max(0, total_aborts - log_counts["artificial_aborts"])

        log_message(f"  Committed: {result['committed']}/{NUM_TRANSACTIONS}, Cascaded: {result['cascaded']}")
        result["success"] = True

    except subprocess.TimeoutExpired:
        log_message(f"TIMEOUT!", "ERROR")
        result["error"] = "TIMEOUT"
    except Exception as e:
        log_message(f"ERROR: {str(e)}", "ERROR")
        result["error"] = str(e)
    finally:
        atomix_setup.kill_servers()
        time.sleep(2)

    return result


def run_batch(batch_name, batch_config, baselines=["Traditional", "Pipelined"]):
    """Run a full batch of tests."""
    log_message("="*80)
    log_message(f"BATCH: {batch_name} - {batch_config['description']}")
    log_message(f"Config: keys={batch_config['num_keys']}, zipf={batch_config['zipf_exponent']}, "
                f"concurrency={batch_config['max_concurrency']}, workload={batch_config['workload_type']}")
    log_message("="*80)

    results = []
    for baseline in baselines:
        log_message(f"\n--- Testing {baseline} ---")
        for abort_rate in ABORT_RATES:
            result = run_single_test(baseline, abort_rate, batch_config)
            result["batch"] = batch_name
            results.append(result)
            time.sleep(2)

    return results


def print_batch_summary(batch_name, results):
    """Print summary for a batch."""
    print(f"\n{'='*80}")
    print(f"BATCH: {batch_name}")
    print(f"{'='*80}")
    print(f"{'Abort%':<8} {'Trad.Commit':<12} {'Pipe.Commit':<12} {'Difference':<12} {'Pipe.Cascaded':<12}")
    print("-"*60)

    for abort_rate in ABORT_RATES:
        trad = next((r for r in results if r["baseline"] == "Traditional" and r["abort_rate"] == abort_rate), None)
        pipe = next((r for r in results if r["baseline"] == "Pipelined" and r["abort_rate"] == abort_rate), None)

        if trad and pipe:
            diff = trad["committed"] - pipe["committed"]
            diff_str = f"T+{diff}" if diff > 0 else (f"P+{-diff}" if diff < 0 else "0")
            cascaded = pipe.get("cascaded", 0)
            print(f"{abort_rate*100:<8.0f} {trad['committed']:<12} {pipe['committed']:<12} {diff_str:<12} {cascaded:<12}")

    print("-"*60)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Contention test for cascading aborts")
    parser.add_argument("--batch", type=str, choices=list(TEST_BATCHES.keys()) + ["all"],
                        default="all", help="Which batch to run")
    parser.add_argument("--baseline", type=str, choices=["Traditional", "Pipelined", "both"],
                        default="both", help="Which baseline to test")
    args = parser.parse_args()

    baselines = ["Traditional", "Pipelined"] if args.baseline == "both" else [args.baseline]

    log_message("="*80)
    log_message("CONTENTION TEST - Varying contention to observe cascading aborts")
    log_message(f"Testing {NUM_TRANSACTIONS} transactions at abort rates: {[f'{r*100:.0f}%' for r in ABORT_RATES]}")
    log_message("="*80)

    all_results = {}

    if args.batch == "all":
        batches_to_run = TEST_BATCHES.keys()
    else:
        batches_to_run = [args.batch]

    for batch_name in batches_to_run:
        batch_config = TEST_BATCHES[batch_name]
        results = run_batch(batch_name, batch_config, baselines)
        all_results[batch_name] = results
        print_batch_summary(batch_name, results)

    # Final comparison
    print("\n" + "="*80)
    print("FINAL COMPARISON: Committed Transactions at 30% Abort Rate")
    print("="*80)
    print(f"{'Batch':<25} {'Traditional':<12} {'Pipelined':<12} {'Pipe.Cascaded':<12}")
    print("-"*65)

    for batch_name, results in all_results.items():
        trad = next((r for r in results if r["baseline"] == "Traditional" and r["abort_rate"] == 0.30), None)
        pipe = next((r for r in results if r["baseline"] == "Pipelined" and r["abort_rate"] == 0.30), None)
        if trad and pipe:
            print(f"{batch_name:<25} {trad['committed']:<12} {pipe['committed']:<12} {pipe.get('cascaded', 0):<12}")

    # Save all results
    output_path = Path("/tmp/contention_test_results.json")
    with open(output_path, 'w') as f:
        json.dump(all_results, f, indent=2)
    log_message(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    main()
