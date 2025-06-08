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
from copy import deepcopy

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
        self.servers = ["universe", "warden", "rangeserver", "resolver", "frontend"]
        self.pids = {}

    def get_servers(self, servers=None):
        if servers:
            return servers
        mode = self.servers_config["resolver"]["mode"]
        if mode == LOCAL:
            return [server for server in self.servers if server != "resolver"]
        elif mode == REMOTE:
            return self.servers
        else:
            raise ValueError(f"Invalid mode: {mode}")

    def build_servers(self):
        print("Building Atomix servers...")
        subprocess.run(["cargo", "build", "--release"], cwd=ROOT_DIR)

    def dump_servers_config(self):
        with open(RAY_SERVERS_CONFIG_PATH, "w") as f:
            json.dump(self.servers_config, f)

    def kill_servers(self, servers=None):
        servers = self.get_servers(servers)
        for server in servers:
            if (
                server == "resolver"
                and self.servers_config["resolver"]["mode"] != REMOTE
            ):
                continue
            if server in self.pids:
                subprocess.run(["kill", "-9", str(self.pids[server])])
                del self.pids[server]
            print(f"Killed '{server}' process.")

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
        servers = self.get_servers(servers)
        print(f"- Killing servers: {servers}")
        # self.kill_servers(servers)
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
                    env={**os.environ, "RUST_LOG": "error"},
                )
                time.sleep(1)
                self.pids[server] = p.pid
            except Exception as e:
                print(f"Error spinning up {server}: {e}")
                self.kill_servers(servers=servers)
                exit(1)


def run_workload(config, atomix_setup, seeds_map):
    atomix_setup.reset_cassandra()
    # atomix_setup.kill_servers(["resolver"])
    # atomix_setup.start_servers(["resolver"])
    atomix_setup.kill_servers()
    atomix_setup.start_servers()

    # Overwrite the namespace and name with a random UUID
    uuid_str = uuid.uuid4().hex[:8]
    # config["namespace"] += f"_{uuid_str}"
    config["name"] += f"_{uuid_str}"
    config["seed"] = seeds_map[config["num-keys"], config["iteration"]]
    # print(f"\033[32mRunning workload with seed: {config['seed']} for num-keys: {config['num-keys']} and iteration: {config['iteration']}\033[0m")
    
    del config["iteration"]
    del config["baseline"]

    # Create a temporary config file with the current parameters
    os.makedirs(os.path.dirname(RAY_WORKLOAD_CONFIG_PATH), exist_ok=True)
    with open(RAY_WORKLOAD_CONFIG_PATH, "w") as f:
        json.dump(config, f)

    # Run the workload generator with timeout
    try:
        process = subprocess.Popen(
            [
                TARGET_RUN_CMD + "workload-generator",
                "--workload-config",
                str(RAY_WORKLOAD_CONFIG_PATH),
                "--config",
                str(RAY_SERVERS_CONFIG_PATH),
                "--create-keyspace",
            ],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "RUST_LOG": "error"},
        )

        try:
            stdout, stderr = process.communicate(timeout=60 * 10)  # 10 minutes timeout
            metrics = parse_metrics(stdout)
            if not metrics:
                print("Warning: No metrics found in output")
                metrics = {"throughput": 0.0}

        except subprocess.TimeoutExpired:
            process.kill()
            print(f"Timeout exceeded for config: {config}")
            metrics = {"throughput": 0.0}

    except Exception as e:
        print(f"Error running workload: {e}")
        metrics = {"throughput": 0.0}

    # tune.report(metrics)
    return metrics


def varying_contention_experiment(atomix_setup, ray_logs_dir):
    namespace, name = generate_slug(2).split("-")
    experiment_name = f"{namespace}_{name}_{uuid.uuid4().hex[:8]}"

    resolver_modes = [REMOTE, LOCAL]
    baselines = [TRADITIONAL, PIPELINED, ADAPTIVE]

    num_keys = [1, 5, 10, 25, 50, 75, 100]
    iterations = list(range(3))

    seeds_map = {}
    for k in num_keys:
        for i in iterations:
            seeds_map[k, i] = random.randint(0, 1000000000)

    # Define the search space
    workload_config = {
        "num-keys": tune.grid_search(num_keys),
        "max-concurrency": tune.grid_search([28]),
        "num-queries": tune.grid_search([1500]),
        "zipf-exponent": tune.grid_search([0]),
        "namespace": namespace,
        "name": name,
        "background-runtime-core-ids": list(range(4, 32)),
        "iteration": tune.grid_search(iterations),
        # "seed": tune.grid_search(seeds),
    }

    baseline_names = []
    for baseline in baselines:
        atomix_setup.servers_config["commit_strategy"] = baseline
        # For traditional 2PC, the resolver mode has no effect.
        modes = resolver_modes if baseline != TRADITIONAL else [LOCAL]
        for mode in modes:
            mode_name = "L" if mode == LOCAL else "R"
            suffix = f"+{mode_name}R" if baseline != TRADITIONAL else ""
            baseline_name = baseline + suffix
            workload_config["baseline"] = tune.grid_search([baseline_name])
            atomix_setup.servers_config["resolver"]["mode"] = mode
            # atomix_setup.reset_cassandra()
            atomix_setup.dump_servers_config()
            # atomix_setup.start_servers()

            run_workload_task = tune.with_parameters(
                run_workload, atomix_setup=atomix_setup, seeds_map=seeds_map
            )

            analysis = tune.run(
                run_workload_task,
                config=workload_config,
                num_samples=1,
                resources_per_trial={"cpu": psutil.cpu_count()},
                storage_path=ray_logs_dir,
                name=experiment_name,
            )
            results = analysis.results_df

            results["baseline"] = baseline_name
            baseline_names.append(baseline_name)
            results.to_csv(
                ray_logs_dir / experiment_name / f"{baseline_name}_results.csv"
            )
            # atomix_setup.kill_servers()

    atomix_setup.kill_servers()
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


def main():
    ray.init()

    ray_logs_dir = Path(RAY_LOGS_DIR)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    atomix_setup = AtomixSetup()
    if BUILD_ATOMIX:
        atomix_setup.build_servers()

    varying_contention_experiment(atomix_setup, ray_logs_dir)


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
