#!/usr/bin/env python3
"""
SANITY CHECK TEST: Run 1000 transactions to COMPLETION

This test runs exactly 1000 transactions (not time-based) and reports:
- Absolute number of committed transactions
- Absolute number of aborted transactions
- Whether all transactions completed or got stuck

Tests BOTH Traditional and Pipelined 2PC at abort rates:
0%, 10%, 20%, 30%, 40%, 50%, 60%

Expected results:
- Traditional: Gradual decline in commits as abort rate increases
- Pipelined: Steeper decline due to cascading aborts
"""

import json
import subprocess
import sys
import os
import time
import re
from pathlib import Path
from datetime import datetime

# Add the scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))

from atomix_setup import atomix_setup
from utils import *

# Test configuration
NUM_TRANSACTIONS = 1000
NUM_KEYS = 10  # Moderate contention
MAX_CONCURRENCY = "20"
ZIPF_EXPONENT = 0.6  # Create hotspots for dependencies
TIMEOUT_SECONDS = 300  # 5 minute timeout per run

ABORT_RATES = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60]
BASELINES = ["Traditional", "Pipelined"]


def log_message(msg, level="INFO"):
    """Print a timestamped log message."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")
    sys.stdout.flush()


def parse_metrics_from_output(output):
    """Parse the METRICS block from workload generator output."""
    metrics = {
        "throughput": 0.0,
        "total_transactions": 0,
        "avg_latency": "N/A",
        "p50_latency": "N/A",
        "p95_latency": "N/A",
        "p99_latency": "N/A",
        "total_duration": "N/A",
    }

    # Find METRICS block
    if "METRICS_START" in output and "METRICS_END" in output:
        start = output.index("METRICS_START")
        end = output.index("METRICS_END")
        metrics_block = output[start:end]

        # Parse each metric
        for line in metrics_block.split("\n"):
            if "Throughput:" in line:
                match = re.search(r"Throughput:\s*([\d.]+)", line)
                if match:
                    metrics["throughput"] = float(match.group(1))
            elif "Total Transactions:" in line:
                match = re.search(r"Total Transactions:\s*(\d+)", line)
                if match:
                    metrics["total_transactions"] = int(match.group(1))
            elif "Average Latency:" in line:
                metrics["avg_latency"] = line.split(":")[1].strip()
            elif "P50 Latency:" in line:
                metrics["p50_latency"] = line.split(":")[1].strip()
            elif "P95 Latency:" in line:
                metrics["p95_latency"] = line.split(":")[1].strip()
            elif "P99 Latency:" in line:
                metrics["p99_latency"] = line.split(":")[1].strip()
            elif "Total Duration:" in line:
                metrics["total_duration"] = line.split(":")[1].strip()

    return metrics


def count_from_logs():
    """Count aborted/committed from server logs."""
    counts = {
        "artificial_aborts": 0,
        "resolver_aborts": 0,
        "total_aborts_logged": 0,
    }

    # Check rangeserver log for artificial aborts (root cause)
    rangeserver_log = ROOT_DIR / "log-rangeserver.txt"
    if rangeserver_log.exists():
        with open(rangeserver_log, 'r') as f:
            content = f.read()
            counts["artificial_aborts"] = content.count("Artificially aborting transaction")

    # Check resolver log for ALL aborts processed by resolver
    # "cascading to dependents" is logged for every abort call
    resolver_log = ROOT_DIR / "log-resolver.txt"
    if resolver_log.exists():
        with open(resolver_log, 'r') as f:
            content = f.read()
            counts["resolver_aborts"] = content.count("cascading to dependents")

    counts["total_aborts_logged"] = counts["artificial_aborts"] + counts["resolver_aborts"]
    return counts


def compute_cascade_stats(committed, expected, artificial_aborts):
    """
    Compute cascading abort statistics.

    Total aborts = expected - committed
    Cascaded aborts = Total aborts - Artificial aborts
    """
    total_aborts = expected - committed
    cascaded = max(0, total_aborts - artificial_aborts)
    return {
        "total_aborts": total_aborts,
        "cascaded_aborts": cascaded,
        "cascade_ratio": cascaded / artificial_aborts if artificial_aborts > 0 else 0
    }


def run_single_test(baseline, abort_rate):
    """Run a single test with given baseline and abort rate."""
    log_message(f"="*60)
    log_message(f"RUNNING: {baseline} @ {abort_rate*100:.0f}% abort rate")
    log_message(f"="*60)

    result = {
        "baseline": baseline,
        "abort_rate": abort_rate,
        "committed": 0,
        "expected_transactions": NUM_TRANSACTIONS,
        "success": False,
        "error": None,
        "duration_seconds": 0,
    }

    try:
        # Configure servers
        log_message(f"Configuring servers for {baseline} with abort_rate={abort_rate}")
        atomix_setup.servers_config['commit_strategy'] = baseline
        atomix_setup.servers_config['range_server']['artificial_abort_rate'] = abort_rate
        atomix_setup.servers_config['heuristic'] = "OpenClients"
        atomix_setup.dump_servers_config()

        # Kill existing servers
        log_message("Killing existing servers...")
        atomix_setup.kill_servers()
        time.sleep(2)

        # Reset Cassandra
        log_message("Resetting Cassandra...")
        atomix_setup.reset_cassandra()
        time.sleep(2)

        # Start servers
        log_message("Starting servers...")
        atomix_setup.start_servers()
        time.sleep(3)
        log_message("Servers started successfully")

        # Create workload config
        workload_config = {
            "num_queries": NUM_TRANSACTIONS,
            "num_keys": NUM_KEYS,
            "zipf_exponent": ZIPF_EXPONENT,
            "namespace": "sanity",
            "name": f"test-{baseline.lower()}-{int(abort_rate*100)}",
            "max_concurrency": MAX_CONCURRENCY,
            "seed": 12345,  # Fixed seed for reproducibility
            "background_runtime_core_ids": [],
            "fake_transactions": False,
            "workload_type": "custom"
        }

        workload_config_path = Path("/tmp/sanity_check_workload.json")
        with open(workload_config_path, 'w') as f:
            json.dump(workload_config, f, indent=2)

        log_message(f"Workload config: {NUM_TRANSACTIONS} transactions, {NUM_KEYS} keys, concurrency={MAX_CONCURRENCY}")

        # Build command
        cmd = [
            TARGET_RUN_CMD + "workload-generator",
            "--workload-config", str(workload_config_path),
            "--config", str(RAY_SERVERS_CONFIG_PATH),
            "--create-keyspace"
        ]

        # Run workload
        log_message("Starting workload generator...")
        start_time = time.time()

        proc = subprocess.run(
            cmd,
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SECONDS,
            env={**os.environ, "RUST_LOG": "info"}
        )

        duration = time.time() - start_time
        result["duration_seconds"] = duration

        log_message(f"Workload completed in {duration:.1f} seconds")

        # Parse output
        output = proc.stdout + proc.stderr
        metrics = parse_metrics_from_output(proc.stdout)
        log_counts = count_from_logs()

        result["committed"] = metrics["total_transactions"]
        result["throughput"] = metrics["throughput"]
        result["artificial_aborts"] = log_counts["artificial_aborts"]

        # Compute cascading abort stats
        cascade_stats = compute_cascade_stats(
            result["committed"],
            NUM_TRANSACTIONS,
            log_counts["artificial_aborts"]
        )
        result["total_aborts"] = cascade_stats["total_aborts"]
        result["cascaded_aborts"] = cascade_stats["cascaded_aborts"]
        result["cascade_ratio"] = cascade_stats["cascade_ratio"]

        # Log detailed results
        log_message(f"RESULT: Committed = {result['committed']} / {NUM_TRANSACTIONS}")
        log_message(f"  Total Aborted: {cascade_stats['total_aborts']}")
        log_message(f"  Artificial aborts (root cause): {log_counts['artificial_aborts']}")
        log_message(f"  Cascaded aborts (computed): {cascade_stats['cascaded_aborts']}")
        if cascade_stats['cascade_ratio'] > 0:
            log_message(f"  Cascade ratio: {cascade_stats['cascade_ratio']:.2f}x amplification")
        log_message(f"  Throughput: {metrics['throughput']:.2f} tx/sec")
        log_message(f"  Avg Latency: {metrics['avg_latency']}")

        if proc.returncode != 0:
            log_message(f"WARNING: Process exited with code {proc.returncode}", "WARN")
            result["error"] = f"Exit code {proc.returncode}"
        else:
            result["success"] = True

        # Print any errors
        if proc.stderr and "error" in proc.stderr.lower():
            log_message(f"STDERR (errors):", "ERROR")
            for line in proc.stderr.split('\n'):
                if "error" in line.lower():
                    log_message(f"  {line}", "ERROR")

    except subprocess.TimeoutExpired:
        log_message(f"TIMEOUT: Test did not complete within {TIMEOUT_SECONDS} seconds!", "ERROR")
        result["error"] = "TIMEOUT"
        result["success"] = False

    except Exception as e:
        log_message(f"EXCEPTION: {str(e)}", "ERROR")
        result["error"] = str(e)
        result["success"] = False

    finally:
        # Always kill servers
        log_message("Cleaning up servers...")
        atomix_setup.kill_servers()
        time.sleep(2)

    return result


def run_full_sanity_check():
    """Run the full sanity check test suite."""
    log_message("="*80)
    log_message("SANGRIA SANITY CHECK TEST")
    log_message(f"Testing {NUM_TRANSACTIONS} transactions to COMPLETION")
    log_message(f"Baselines: {BASELINES}")
    log_message(f"Abort rates: {[f'{r*100:.0f}%' for r in ABORT_RATES]}")
    log_message("="*80)

    all_results = []

    for baseline in BASELINES:
        log_message(f"\n{'='*80}")
        log_message(f"TESTING BASELINE: {baseline}")
        log_message(f"{'='*80}")

        for abort_rate in ABORT_RATES:
            result = run_single_test(baseline, abort_rate)
            all_results.append(result)

            # Brief pause between tests
            time.sleep(3)

    # Print summary
    print_summary(all_results)

    # Save results
    save_results(all_results)

    return all_results


def print_summary(results):
    """Print a summary table of all results."""
    log_message("\n" + "="*80)
    log_message("SUMMARY TABLE")
    log_message("="*80)

    # Header
    print(f"\n{'Baseline':<12} {'Abort%':<8} {'Committed':<12} {'TotalAbort':<12} {'Artificial':<12} {'Cascaded':<12} {'CascadeX':<10}")
    print("-" * 88)

    # Group by baseline
    for baseline in BASELINES:
        baseline_results = [r for r in results if r["baseline"] == baseline]
        for r in baseline_results:
            total_abort = r.get("total_aborts", 0)
            art_aborts = r.get("artificial_aborts", 0)
            casc_aborts = r.get("cascaded_aborts", 0)
            cascade_ratio = r.get("cascade_ratio", 0)
            ratio_str = f"{cascade_ratio:.2f}x" if cascade_ratio > 0 else "-"
            print(f"{r['baseline']:<12} {r['abort_rate']*100:<8.0f} {r['committed']:<12} {total_abort:<12} {art_aborts:<12} {casc_aborts:<12} {ratio_str:<10}")
        print("-" * 88)

    # Analysis
    log_message("\nANALYSIS:")

    trad_results = [r for r in results if r["baseline"] == "Traditional"]
    pipe_results = [r for r in results if r["baseline"] == "Pipelined"]

    if trad_results and pipe_results:
        # Compare at each abort rate
        for abort_rate in ABORT_RATES:
            trad = next((r for r in trad_results if r["abort_rate"] == abort_rate), None)
            pipe = next((r for r in pipe_results if r["abort_rate"] == abort_rate), None)

            if trad and pipe:
                diff = trad["committed"] - pipe["committed"]
                if diff > 0:
                    log_message(f"  At {abort_rate*100:.0f}% abort: Traditional commits {diff} MORE than Pipelined")
                elif diff < 0:
                    log_message(f"  At {abort_rate*100:.0f}% abort: Pipelined commits {-diff} MORE than Traditional")
                else:
                    log_message(f"  At {abort_rate*100:.0f}% abort: Both committed the same amount")


def save_results(results):
    """Save results to JSON file."""
    output_path = Path("/tmp/sanity_check_results.json")
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    log_message(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sanity check test for Sangria")
    parser.add_argument("--baseline", type=str, choices=["Traditional", "Pipelined", "both"],
                        default="both", help="Which baseline to test")
    parser.add_argument("--abort-rate", type=float, default=None,
                        help="Single abort rate to test (if not specified, tests all)")
    parser.add_argument("--num-transactions", type=int, default=NUM_TRANSACTIONS,
                        help=f"Number of transactions (default: {NUM_TRANSACTIONS})")
    parser.add_argument("--quick", action="store_true",
                        help="Quick test: only 0%, 30%, 60% abort rates")

    args = parser.parse_args()

    # Override globals if specified
    if args.num_transactions:
        NUM_TRANSACTIONS = args.num_transactions

    if args.baseline != "both":
        BASELINES = [args.baseline]

    if args.abort_rate is not None:
        ABORT_RATES = [args.abort_rate]
    elif args.quick:
        ABORT_RATES = [0.0, 0.30, 0.60]

    results = run_full_sanity_check()

    # Exit with error if any tests failed
    if any(not r["success"] for r in results):
        sys.exit(1)
    sys.exit(0)
