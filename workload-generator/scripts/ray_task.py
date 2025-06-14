import json
import os
import re
import subprocess
from utils import *


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
        "resolver_stats": r"Resolver stats: (.*)",
    }

    for metric, pattern in patterns.items():
        match = re.search(pattern, metrics_text)
        if match:
            value = match.group(1)
            # Convert duration strings to seconds
            if metric == "resolver_stats":
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
    # print(metrics)
    return metrics


def run_workload(config):
    global atomix_setup

    iteration = config["iteration"]
    cpu_percentage = config["cpu_percentage"]
    baseline = config["baseline"]
    del config["iteration"]
    del config["baseline"]
    del config["cpu_percentage"]

    cmd = [
        TARGET_RUN_CMD + "workload-generator",
        "--workload-config",
        str(RAY_WORKLOAD_CONFIG_PATH),
        "--config",
        str(RAY_SERVERS_CONFIG_PATH),
    ]

    if iteration == 0:
        atomix_setup.servers_config["commit_strategy"] = baseline
        atomix_setup.servers_config["resolver"]["cpu_percentage"] = cpu_percentage
        atomix_setup.dump_servers_config()
        atomix_setup.kill_servers()
        atomix_setup.reset_cassandra()
        atomix_setup.start_servers()

        # Create a temporary config file with the current parameters
        os.makedirs(os.path.dirname(RAY_WORKLOAD_CONFIG_PATH), exist_ok=True)
        with open(RAY_WORKLOAD_CONFIG_PATH, "w") as f:
            json.dump(config, f)

        cmd.append("--create-keyspace")

    # Add back the config params so that they are reported by ray
    config["iteration"] = iteration
    config["baseline"] = baseline
    config["cpu_percentage"] = cpu_percentage

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

    # tune.report(metrics)
    return metrics
