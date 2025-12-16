#!/usr/bin/env python3
"""
Test script to run a simple Pipelined experiment and visualize the dependency tree.

This script:
1. Kills any existing servers
2. Starts resolver and rangeserver with Pipelined baseline
3. Runs a short workload with artificial aborts
4. Fetches the dependency tree
5. Generates HTML visualization
"""

import os
import subprocess
import sys
import time
import signal

# Script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SANGRIA_DIR = os.path.expanduser("~/sangria")
CONFIG_DIR = os.path.join(SANGRIA_DIR, "configs")
TARGET_DIR = os.path.join(SANGRIA_DIR, "target", "release")

# Configuration
RESOLVER_ADDR = "localhost:50052"
RANGESERVER_ADDR = "localhost:50051"
ABORT_RATE = 0.3  # 30% abort rate
NUM_TRANSACTIONS = 50
DURATION_SECS = 10


def kill_existing_servers():
    """Kill any existing sangria servers."""
    print("Killing existing servers...")
    subprocess.run(["pkill", "-f", "resolver"], capture_output=True)
    subprocess.run(["pkill", "-f", "rangeserver"], capture_output=True)
    subprocess.run(["pkill", "-f", "workload-generator"], capture_output=True)
    time.sleep(2)


def start_resolver(baseline: str):
    """Start the resolver server."""
    config_file = os.path.join(CONFIG_DIR, "single_machine_config.yaml")

    # Set environment for baseline
    env = os.environ.copy()
    env["BASELINE"] = baseline

    print(f"Starting resolver with baseline={baseline}...")
    proc = subprocess.Popen(
        [os.path.join(TARGET_DIR, "resolver"), "--config", config_file],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


def start_rangeserver(baseline: str):
    """Start the rangeserver."""
    config_file = os.path.join(CONFIG_DIR, "single_machine_config.yaml")

    env = os.environ.copy()
    env["BASELINE"] = baseline

    print(f"Starting rangeserver with baseline={baseline}...")
    proc = subprocess.Popen(
        [os.path.join(TARGET_DIR, "rangeserver"), "--config", config_file],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


def run_workload(abort_rate: float, duration: int):
    """Run the workload generator."""
    config_file = os.path.join(CONFIG_DIR, "single_machine_config.yaml")

    print(f"Running workload with abort_rate={abort_rate} for {duration}s...")
    result = subprocess.run(
        [
            os.path.join(TARGET_DIR, "workload-generator"),
            "--config", config_file,
            "--duration", str(duration),
            "--artificial-abort-rate", str(abort_rate),
            "--num-keys", "100",
            "--zipf", "0.9",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Workload error: {result.stderr}")
    else:
        print("Workload completed")


def fetch_dependency_tree(output_json: str):
    """Fetch the dependency tree from the resolver."""
    print(f"Fetching dependency tree from {RESOLVER_ADDR}...")

    fetch_script = os.path.join(SCRIPT_DIR, "fetch_dependency_tree.py")
    result = subprocess.run(
        [sys.executable, fetch_script,
         "--resolver-addr", RESOLVER_ADDR,
         "--output", output_json],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error fetching tree: {result.stderr}")
        return False

    print(result.stdout)
    return True


def generate_visualization(json_file: str, html_file: str):
    """Generate HTML visualization from JSON."""
    print(f"Generating visualization: {html_file}")

    vis_script = os.path.join(SCRIPT_DIR, "visualize_dependency_tree.py")
    result = subprocess.run(
        [sys.executable, vis_script,
         "--json-file", json_file,
         "--output", html_file],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error generating visualization: {result.stderr}")
        return False

    print(result.stdout)
    return True


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=str, default="Pipelined",
                        choices=["Pipelined", "Traditional"],
                        help="Baseline to test")
    parser.add_argument("--abort-rate", type=float, default=ABORT_RATE,
                        help="Artificial abort rate")
    parser.add_argument("--duration", type=int, default=DURATION_SECS,
                        help="Workload duration in seconds")
    parser.add_argument("--output-dir", type=str, default=".",
                        help="Output directory")
    args = parser.parse_args()

    # Output files
    json_file = os.path.join(args.output_dir, f"dependency_tree_{args.baseline.lower()}.json")
    html_file = os.path.join(args.output_dir, f"dependency_tree_{args.baseline.lower()}.html")

    # Kill existing servers
    kill_existing_servers()

    # Start servers
    resolver_proc = start_resolver(args.baseline)
    time.sleep(2)

    rangeserver_proc = start_rangeserver(args.baseline)
    time.sleep(3)

    try:
        # Run workload
        run_workload(args.abort_rate, args.duration)

        # Wait a bit for all transactions to settle
        time.sleep(2)

        # Fetch dependency tree
        if fetch_dependency_tree(json_file):
            # Generate visualization
            generate_visualization(json_file, html_file)
            print(f"\n=== Done! ===")
            print(f"JSON: {json_file}")
            print(f"HTML: {html_file}")
            print(f"Open the HTML file in a browser to see the visualization.")

    finally:
        # Cleanup
        print("\nShutting down servers...")
        resolver_proc.terminate()
        rangeserver_proc.terminate()
        try:
            resolver_proc.wait(timeout=5)
            rangeserver_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            resolver_proc.kill()
            rangeserver_proc.kill()


if __name__ == "__main__":
    main()
