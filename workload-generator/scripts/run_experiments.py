import os
import json
import subprocess
from pathlib import Path
import psutil
import ray
from ray import tune
import re
import uuid
import time
from coolname import generate_slug
import pandas as pd
import argparse
import random
from termcolor import colored
from typing import Dict, Optional, List, Any
import itertools
from ray.tune.search import Searcher
from ray.tune.schedulers import FIFOScheduler
from copy import deepcopy

ROOT_DIR = Path(__file__).parent.parent.parent
SERVERS_CONFIG_PATH = ROOT_DIR / "configs" / "config.json"
WORKLOAD_GENERATOR_DIR = ROOT_DIR / "workload-generator"
RAY_LOGS_DIR = WORKLOAD_GENERATOR_DIR / "experiments" / "ray_logs"
RAY_SERVERS_CONFIG_PATH = WORKLOAD_GENERATOR_DIR / "configs" / "config-ray.json"
RAY_WORKLOAD_CONFIG_PATH = (
    WORKLOAD_GENERATOR_DIR / "configs" / "workload-config-ray.json"
)

CARGO_RUN_CMD = "cargo run --release --bin "
TARGET_RUN_CMD = str(ROOT_DIR) + "/target/release/"

RUN_CMD = TARGET_RUN_CMD
BUILD_ATOMIX = True

LOCAL = "Local"
REMOTE = "Remote"
TRADITIONAL = "Traditional"
PIPELINED = "Pipelined"
ADAPTIVE = "Adaptive"

os.environ["RAY_AIR_NEW_OUTPUT"] = "0"


def parse_metrics(output):
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
    }

    for metric, pattern in patterns.items():
        match = re.search(pattern, metrics_text)
        if match:
            value = match.group(1)
            # Convert duration strings to seconds
            if "s" in value:
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
    # print(metrics)
    return metrics


class AtomixSetup:
    def __init__(self):
        self.servers_config = json.load(open(SERVERS_CONFIG_PATH, "r"))
        self.servers_start_order = [
            "universe",
            "warden",
            "rangeserver",
            "resolver",
            "frontend",
        ]
        self.servers_kill_order = [
            "warden",
            "rangeserver",
            "frontend",
            "resolver",
            "universe",
        ]
        self.pids = {}

    def get_servers(self, start_order=True, servers=None):
        if servers:
            return servers
        mode = self.servers_config["resolver"]["mode"]
        servers = self.servers_start_order if start_order else self.servers_kill_order
        if mode == LOCAL:
            return [server for server in servers if server != "resolver"]
        elif mode == REMOTE:
            return servers
        else:
            raise ValueError(f"Invalid mode: {mode}")

    def build_servers(self):
        print("Building Atomix servers...")
        subprocess.run(["cargo", "build", "--release"], cwd=ROOT_DIR)

    def dump_servers_config(self):
        with open(RAY_SERVERS_CONFIG_PATH, "w") as f:
            json.dump(self.servers_config, f)

    def kill_servers(self, servers=None):
        if servers:
            return servers
        servers = self.get_servers(start_order=False, servers=servers)
        for server in servers:
            if (
                server == "resolver"
                and self.servers_config["resolver"]["mode"] != REMOTE
            ):
                continue
            try:
                if server in self.pids:
                    subprocess.run(["kill", "-9", str(self.pids[server])])
                    del self.pids[server]
                    print(f"Killed '{server}'")
            except Exception as e:
                print(f"Error killing {server}: {e}")
        time.sleep(1)

    def reset_cassandra(self):
        try:
            print("Cleaning Cassandra...")
            subprocess.run(
                [
                    "sudo",
                    "docker",
                    "exec",
                    "-i",
                    "cassandra",
                    "cqlsh",
                    "-e",
                    "TRUNCATE atomix.range_map; "
                    "TRUNCATE atomix.epoch; "
                    "TRUNCATE atomix.range_leases; "
                    "TRUNCATE atomix.records; "
                    "TRUNCATE atomix.wal; "
                    "TRUNCATE atomix.transactions; "
                    "TRUNCATE atomix.keyspaces;",
                ]
            )
        except Exception as e:
            print(f"Error cleaning Cassandra: {e}")

    def start_servers(self, servers=None):
        servers = self.get_servers(start_order=True, servers=servers)
        for server in servers:
            if (
                server == "resolver"
                and self.servers_config["resolver"]["mode"] != REMOTE
            ):
                continue
            try:
                print(f"- Spinning up {server}")
                p = subprocess.Popen(
                    [
                        RUN_CMD + server,
                        "--config",
                        str(RAY_SERVERS_CONFIG_PATH),
                    ],
                    cwd=ROOT_DIR,
                    start_new_session=True,
                    text=True,
                    env={**os.environ, "RUST_LOG": "error"},
                )
                self.pids[server] = p.pid
                time.sleep(1)
            except Exception as e:
                print(f"Error spinning up {server}: {e}")
                self.kill_servers(servers=servers)
                exit(1)


def run_workload(config):
    global atomix_setup
    iteration = config["iteration"]
    baseline = config["baseline"]

    del config["iteration"]

    cmd = [
        TARGET_RUN_CMD + "workload-generator",
        "--workload-config",
        str(RAY_WORKLOAD_CONFIG_PATH),
        "--config",
        str(RAY_SERVERS_CONFIG_PATH),
    ]

    if iteration == 0:
        atomix_setup.kill_servers()
        atomix_setup.reset_cassandra()
        atomix_setup.start_servers()

        # Create a temporary config file with the current parameters
        os.makedirs(os.path.dirname(RAY_WORKLOAD_CONFIG_PATH), exist_ok=True)
        with open(RAY_WORKLOAD_CONFIG_PATH, "w") as f:
            json.dump(config, f)

        cmd.append("--create-keyspace")

    print(cmd)

    # Run the workload generator with timeout
    try:
        process = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "RUST_LOG": "error"},
        )
        try:
            stdout, stderr = process.communicate(timeout=60 * 10)  # 10 minutes timeout
            print(stderr)
            metrics = parse_metrics(stdout)
        except subprocess.TimeoutExpired:
            process.kill()
            print(f"Timeout exceeded for config: {config}")
            metrics = {"throughput": 0.0}
    except Exception as e:
        print(f"Error running workload: {e}")
        metrics = {"throughput": 0.0}

    config["iteration"] = iteration
    # tune.report(metrics)
    return metrics


def varying_contention_experiment(ray_logs_dir):
    global atomix_setup
    namespace, name = generate_slug(2).split("-")
    experiment_name = f"{namespace}_{name}"

    resolver_modes = [REMOTE]
    baselines = [TRADITIONAL, PIPELINED, ADAPTIVE]

    NUM_KEYS = [1, 5, 10, 25, 50, 75, 100]
    NUM_ITERATIONS = 4
    NUM_QUERIES = [1500]

    num_combinations = len(NUM_KEYS) * len(NUM_QUERIES) * NUM_ITERATIONS

    # Define the search space
    workload_config = {
        "num-keys": NUM_KEYS,
        "max-concurrency": [28],
        "num-queries": NUM_QUERIES,
        "zipf-exponent": [0],
        "namespace": [namespace],
        "name": [name],
        "background-runtime-core-ids": [list(range(4, 32))],
        # "seed": tune.grid_search(seeds),
    }

    reporter = tune.CLIReporter(
        metric_columns=["throughput"],
        parameter_columns=[
            "baseline",
            "num-keys",
            "num-queries",
            "iteration",
        ],
        max_report_frequency=20,
    )
    baseline_names = []
    for baseline in baselines:
        atomix_setup.servers_config["commit_strategy"] = baseline
        # For traditional 2PC, the resolver mode has no effect.
        modes = resolver_modes if baseline != TRADITIONAL else [LOCAL]
        for mode in modes:
            mode_name = "L" if mode == LOCAL else "R"
            suffix = f"+{mode_name}R" if baseline != TRADITIONAL else ""
            baseline_name = baseline + suffix
            workload_config["baseline"] = [baseline_name]
            atomix_setup.servers_config["resolver"]["mode"] = mode
            atomix_setup.dump_servers_config()

            analysis = tune.run(
                tune.with_parameters(run_workload),
                config={},
                num_samples=num_combinations,
                resources_per_trial={"cpu": psutil.cpu_count()},
                storage_path=ray_logs_dir,
                name=experiment_name,
                search_alg=GridSearcherInOrder(
                    atomix_setup, NUM_ITERATIONS, workload_config
                ),
                reuse_actors=True,
                max_concurrent_trials=1,
                scheduler=FIFOScheduler(),
                verbose=1,
                progress_reporter=reporter,
            )
            # ray.shutdown()
            # ray.init(ignore_reinit_error=True)
            results = analysis.results_df

            results["baseline"] = baseline_name
            baseline_names.append(baseline_name)
            results.to_csv(
                ray_logs_dir / experiment_name / f"{baseline_name}_results.csv"
            )

    # atomix_setup.kill_servers()
    results = pd.concat(
        [
            pd.read_csv(ray_logs_dir / experiment_name / f"{baseline_name}_results.csv")
            for baseline_name in baseline_names
        ]
    )
    results.to_csv(ray_logs_dir / experiment_name / "results.csv")

    subprocess.run(
        [
            "python",
            WORKLOAD_GENERATOR_DIR / "scripts" / "plot_experiments.py",
            "--experiment-name",
            experiment_name,
        ]
    )
    ray.shutdown()


class GridSearcherInOrder(Searcher):
    def __init__(
        self,
        atomix_setup: AtomixSetup,
        num_iterations: int,
        param_grid: Dict[str, List[Any]],
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        super().__init__(metric=metric, mode=mode)
        self.num_iterations = num_iterations
        self.atomix_setup = atomix_setup
        self.param_keys = list(param_grid.keys())
        self.param_values = list(param_grid.values())
        self.param_keys.append("seed")
        self.param_keys.append("iteration")

        self.grid = list(itertools.product(*self.param_values))
        self.grid = [
            list(config) + [random.randint(0, 1000000000)] for config in self.grid
        ]
        self.grid = [
            config + [iteration]
            for config in self.grid
            for iteration in range(self.num_iterations)
        ]

        self.trial_queue = self.grid
        self.trial_map = {}

    def suggest(self, trial_id: str) -> Optional[Dict[str, Any]]:
        if not self.trial_queue:
            return None
        values = self.trial_queue.pop(0)
        config = dict(zip(self.param_keys, values))
        print(colored(f"Config: {config}", "blue"))
        self.trial_map[trial_id] = config
        return config

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict], error: bool = False
    ):
        self.trial_map.pop(trial_id, None)

    def save(self, checkpoint_path: str):
        # Optional: save state if you want to support checkpointing
        pass

    def restore(self, checkpoint_path: str):
        # Optional: restore state
        pass


atomix_setup = AtomixSetup()


def main():
    ray.init()
    global atomix_setup
    ray_logs_dir = Path(RAY_LOGS_DIR)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    if BUILD_ATOMIX:
        atomix_setup.build_servers()

    varying_contention_experiment(ray_logs_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Atomix experiments.")
    parser.add_argument(
        "--build",
        action="store_true",
        default=True,
        help="Build Atomix servers before running experiments.",
    )
    args = parser.parse_args()

    BUILD_ATOMIX = args.build
    main()
