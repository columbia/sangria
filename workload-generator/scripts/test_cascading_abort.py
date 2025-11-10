#!/usr/bin/env python3
"""
Test cascading aborts functionality.
Reuses the existing atomix_setup infrastructure.
"""

import json
import subprocess
import sys
import os
from pathlib import Path

# Add the scripts directory to the path so we can import atomix_setup
sys.path.insert(0, str(Path(__file__).parent))

from atomix_setup import atomix_setup
from utils import *


def check_logs_for_cascading_aborts():
    """Check server logs for evidence of cascading aborts"""
    print("\n=== Checking logs for cascading abort evidence ===")

    resolver_log_path = ROOT_DIR / "log-resolver.txt"
    rangeserver_log_path = ROOT_DIR / "log-rangeserver.txt"

    found_cascading = False
    found_artificial = False

    # Check resolver logs for cascading abort messages
    if resolver_log_path.exists():
        with open(resolver_log_path, 'r') as f:
            resolver_content = f.read()
            cascading_lines = [line for line in resolver_content.split('\n') if "Cascading abort" in line]
            if cascading_lines:
                print(f"✅ SUCCESS: Found {len(cascading_lines)} 'Cascading abort' messages in resolver log")
                print("  Sample messages:")
                for line in cascading_lines[:5]:  # Show first 5
                    print(f"    {line.strip()}")
                found_cascading = True
            else:
                print("⚠️  No 'Cascading abort' messages found in resolver log")

    # Check rangeserver logs for artificial abort messages
    if rangeserver_log_path.exists():
        with open(rangeserver_log_path, 'r') as f:
            rangeserver_content = f.read()
            artificial_lines = [line for line in rangeserver_content.split('\n') if "Artificially aborting transaction" in line]
            if artificial_lines:
                print(f"✅ SUCCESS: Found {len(artificial_lines)} 'Artificially aborting transaction' messages in rangeserver log")
                print("  Sample messages:")
                for line in artificial_lines[:3]:  # Show first 3
                    print(f"    {line.strip()}")
                found_artificial = True
            else:
                print("⚠️  No 'Artificially aborting transaction' messages found in rangeserver log")

    return found_cascading, found_artificial


def run_cascading_abort_test(abort_rate=0.5, num_queries=30, num_keys=5, max_concurrency="5"):
    """
    Run a test workload with artificial aborts enabled and check for cascading aborts.

    Args:
        abort_rate: Probability (0.0-1.0) that a transaction will be artificially aborted
        num_queries: Number of transactions to run
        num_keys: Number of keys in the keyspace (smaller = more contention)
        max_concurrency: Maximum concurrent transactions
    """
    print("="*60)
    print("CASCADING ABORTS TEST")
    print("="*60)
    print(f"Configuration:")
    print(f"  abort_rate: {abort_rate}")
    print(f"  num_queries: {num_queries}")
    print(f"  num_keys: {num_keys}")
    print(f"  max_concurrency: {max_concurrency}")
    print("="*60)

    # Configure servers with artificial abort rate
    print("\n=== Configuring servers ===")
    atomix_setup.servers_config['range_server']['artificial_abort_rate'] = abort_rate
    atomix_setup.servers_config['heuristic'] = "OpenClients"
    atomix_setup.dump_servers_config()

    # Kill any existing servers
    print("\n=== Killing existing servers ===")
    atomix_setup.kill_servers()

    # Reset Cassandra
    print("\n=== Resetting Cassandra ===")
    atomix_setup.reset_cassandra()

    # Start servers
    print("\n=== Starting servers ===")
    atomix_setup.start_servers()

    # Create workload config
    workload_config = {
        "num_queries": num_queries,
        "num_keys": num_keys,
        "zipf_exponent": 0.9,  # High contention to create dependencies
        "namespace": "test",
        "name": "cascading-abort-test",
        "max_concurrency": max_concurrency,
        "seed": 42,
        "background_runtime_core_ids": [],
        "fake_transactions": False,
        "workload_type": "custom"
    }

    workload_config_path = Path("/tmp/test_cascading_abort_workload.json")
    with open(workload_config_path, 'w') as f:
        json.dump(workload_config, f, indent=2)

    print(f"\n=== Created workload config: {workload_config_path} ===")

    # Run workload
    print("\n=== Running workload generator ===")
    cmd = [
        TARGET_RUN_CMD + "workload-generator",
        "--workload-config", str(workload_config_path),
        "--config", str(RAY_SERVERS_CONFIG_PATH),
        "--create-keyspace"
    ]

    print(f"Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout
            env={**os.environ, "RUST_LOG": "info"}
        )

        print("\nWorkload output:")
        print(result.stdout)

        if result.stderr:
            print("\nWorkload errors/warnings:")
            print(result.stderr)

        if result.returncode != 0:
            print(f"\n⚠️  Workload exited with code {result.returncode}")

    except subprocess.TimeoutExpired:
        print("\n⚠️  Workload generator timed out")
    except Exception as e:
        print(f"\n❌ Error running workload: {e}")

    # Check logs for cascading abort evidence
    found_cascading, found_artificial = check_logs_for_cascading_aborts()

    # Cleanup
    print("\n=== Cleaning up ===")
    atomix_setup.kill_servers()

    # Determine test result
    print("\n" + "="*60)
    if found_cascading and found_artificial:
        print("✅ TEST PASSED: Cascading aborts are working correctly!")
        print("   - Artificial aborts were injected")
        print("   - Dependent transactions were cascaded")
        success = True
    else:
        print("❌ TEST FAILED:")
        if not found_artificial:
            print("   - No artificial aborts were injected (check abort_rate and RNG)")
        if not found_cascading:
            print("   - No cascading aborts were detected (check dependency tracking)")
        success = False

    print("="*60)

    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test cascading aborts functionality")
    parser.add_argument("--abort-rate", type=float, default=0.5,
                        help="Artificial abort rate (0.0-1.0), default: 0.5")
    parser.add_argument("--num-queries", type=int, default=30,
                        help="Number of transactions to run, default: 30")
    parser.add_argument("--num-keys", type=int, default=5,
                        help="Number of keys (smaller = more contention), default: 5")
    parser.add_argument("--max-concurrency", type=str, default="5",
                        help="Maximum concurrent transactions, default: 5")

    args = parser.parse_args()

    success = run_cascading_abort_test(
        abort_rate=args.abort_rate,
        num_queries=args.num_queries,
        num_keys=args.num_keys,
        max_concurrency=args.max_concurrency
    )

    sys.exit(0 if success else 1)
