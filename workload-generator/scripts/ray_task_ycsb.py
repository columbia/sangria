import json
import os
import re
import time
import subprocess
from utils import *
from atomix_setup import atomix_setup


def parse_ycsb_metrics(output):
    """Parse metrics from YCSB output"""
    metrics = {}

    # Parse throughput (operations per second)
    throughput_match = re.search(
        r"\[OVERALL\], Throughput\(ops/sec\), ([\d\.]+)", output
    )
    if throughput_match:
        metrics["throughput"] = float(throughput_match.group(1))
    else:
        metrics["throughput"] = 0.0

    # Parse average latency
    avg_latency_match = re.search(
        r"\[READ-MODIFY-WRITE\], AverageLatency\(us\), ([\d\.]+)", output
    )
    if avg_latency_match:
        metrics["avg_latency"] = (
            float(avg_latency_match.group(1)) / 1_000_000
        )  # Convert to seconds

    # # Parse 95th percentile latency
    # p95_latency_match = re.search(r"\[OVERALL\], 95thPercentileLatency\(us\), ([\d\.]+)", output)
    # if p95_latency_match:
    #     metrics["p95_latency"] = float(p95_latency_match.group(1)) / 1_000_000  # Convert to seconds

    # # Parse 99th percentile latency
    # p99_latency_match = re.search(r"\[OVERALL\], 99thPercentileLatency\(us\), ([\d\.]+)", output)
    # if p99_latency_match:
    #     metrics["p99_latency"] = float(p99_latency_match.group(1)) / 1_000_000  # Convert to seconds

    # # Parse total operations
    # total_ops_match = re.search(r"\[OVERALL\], Operations, ([\d\.]+)", output)
    # if total_ops_match:
    #     metrics["total_transactions"] = int(total_ops_match.group(1))

    return metrics


def create_ycsb_properties(config):
    """Create atomix.properties file for YCSB with the given configuration"""

    # Map zipf_exponent to zipfianconstant (YCSB uses different naming)
    zipfian_constant = config.get("zipf_exponent", 0.0)
    if zipfian_constant == 0.0:
        request_distribution = "uniform"
    else:
        request_distribution = "zipfian"

    properties_content = f"""# Atomix Database Configuration for Ray Experiment
# Generated automatically with ray prefix

# Logging configuration
java.util.logging.ConsoleHandler.level = SEVERE

# Workload configuration
workload=site.ycsb.workloads.CoreWorkload
recordcount={config.get('num_keys', 100)}
operationcount={config.get('num_queries', 1000)}
insertorder=ordered
requestdistribution={request_distribution}

# Zipfian distribution configuration
# zipfianconstant: Controls the skew of the zipfian distribution
# - Values < 1.0: Lower skew (more uniform)
# - Values close to 1.0: Higher skew (more concentrated access)
# - Values >= 1.0: Very high skew (may be slow for large datasets)
# - Recommended range: 0.5 to 0.999
zipfianconstant={zipfian_constant}

# Atomix configuration
atomix.frontend.address=127.0.0.1:50057
atomix.namespace={config.get('namespace', 'ycsb')}
atomix.keyspace={config.get('name', 'default')}
atomix.keyspace.num_keys={config.get('num_keys', 100)}

# Operation distribution
readproportion={0.0}
updateproportion={0.0}
readmodifywriteproportion={1.0}
scanproportion={0.0}
insertproportion={0.0}
"""

    return properties_content


def run_workload_ycsb(config):
    global atomix_setup

    iteration = config["iteration"]
    baseline = config["baseline"]
    resolver_background_runtime_core_ids = config["resolver_capacity"][
        "background_runtime_core_ids"
    ]
    resolver_cpu_percentage = config["resolver_capacity"]["cpu_percentage"]
    resolver_cores = config["resolver_cores"]
    resolver_tx_load = config["resolver_tx_load"]
    main_num_keys = config["num_keys"]
    main_max_concurrency = config["max_concurrency"]
    main_num_queries = config["num_queries"]
    main_name = config["name"]
    main_background_runtime_core_ids = config["background_runtime_core_ids"]

    # Create a copy of config for YCSB
    ycsb_config = config.copy()
    del ycsb_config["iteration"]
    del ycsb_config["baseline"]
    del ycsb_config["resolver_capacity"]
    del ycsb_config["resolver_cores"]
    del ycsb_config["resolver_tx_load"]
    del ycsb_config["resolver_tx_load_concurrency"]

    # Secondary workload generator -- used to overload the resolver with txs
    cmd2 = [
        TARGET_RUN_CMD + "workload-generator",
        "--workload-config",
        str(SECONDARY_RAY_WORKLOAD_CONFIG_PATH),
        "--config",
        str(RAY_SERVERS_CONFIG_PATH),
    ]

    if iteration == 0:
        atomix_setup.servers_config["commit_strategy"] = baseline
        atomix_setup.servers_config["resolver"][
            "cpu_percentage"
        ] = resolver_cpu_percentage
        atomix_setup.servers_config["resolver"][
            "background_runtime_core_ids"
        ] = resolver_background_runtime_core_ids
        atomix_setup.dump_servers_config()
        atomix_setup.kill_servers()
        atomix_setup.reset_cassandra()
        atomix_setup.start_servers()

        # Create YCSB properties file
        ycsb_properties_content = create_ycsb_properties(ycsb_config)
        ycsb_properties_path = YCSB_ATOMIX_PROPERTIES_PATH
        os.makedirs(os.path.dirname(ycsb_properties_path), exist_ok=True)
        with open(ycsb_properties_path, "w") as f:
            f.write(ycsb_properties_content)

        # Create a temporary config file with the parameters of the secondary workload generator
        ycsb_config["fake_transactions"] = True
        ycsb_config["max_concurrency"] = resolver_tx_load["max_concurrency"]
        ycsb_config["num_queries"] = resolver_tx_load["num_queries"]
        ycsb_config["num_keys"] = resolver_tx_load["num_keys"]
        ycsb_config["name"] = f"{main_name}-2"
        ycsb_config["background_runtime_core_ids"] = resolver_tx_load[
            "background_runtime_core_ids"
        ]
        os.makedirs(os.path.dirname(SECONDARY_RAY_WORKLOAD_CONFIG_PATH), exist_ok=True)
        with open(SECONDARY_RAY_WORKLOAD_CONFIG_PATH, "w") as f:
            json.dump(ycsb_config, f)
        del ycsb_config["fake_transactions"]
        ycsb_config["max_concurrency"] = main_max_concurrency
        ycsb_config["num_queries"] = main_num_queries
        ycsb_config["num_keys"] = main_num_keys
        ycsb_config["name"] = main_name
        ycsb_config["background_runtime_core_ids"] = main_background_runtime_core_ids

    # YCSB command for run phase
    ycsb_run_cmd = [
        str(YCSB_DIR / "bin" / "ycsb"),
        "run",
        "atomix",
        "-P",
        str(YCSB_ATOMIX_PROPERTIES_PATH),
        "-threads",
        str(main_max_concurrency),
    ]

    # Add back the config params so that they are reported by ray
    config["iteration"] = iteration
    config["baseline"] = baseline
    config["resolver_cores"] = resolver_cores
    config["resolver_tx_load_concurrency"] = resolver_tx_load["max_concurrency"]
    print("ycsb_run_cmd: ", ycsb_run_cmd)
    print("cmd2: ", cmd2)

    try:
        process2 = None
        if resolver_tx_load["max_concurrency"] != "0":
            process2 = subprocess.Popen(
                cmd2,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, "RUST_LOG": "error"},
            )

        # Let it run for a while
        time.sleep(2)

        # Run YCSB run phase
        print("Starting YCSB run phase...")
        run_process = subprocess.Popen(
            ycsb_run_cmd,
            cwd=str(YCSB_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "java.util.logging.ConsoleHandler.level": "SEVERE"},
        )

        try:
            # Wait for the YCSB run phase to finish
            print("Waiting for YCSB run phase to finish...")
            run_stdout, run_stderr = run_process.communicate(
                timeout=60 * 60
            )  # 60 minutes timeout
            if run_stderr:
                print(f"Run stderr: {run_stderr}")
            print(run_stdout)
            # Parse YCSB metrics
            metrics = parse_ycsb_metrics(run_stdout)
            print("Parsed YCSB metrics:", metrics)

            # Send interrupt signal to the secondary workload generator
            if process2:
                process2.send_signal(subprocess.signal.SIGUSR1)
                print("Sent SIGUSR1 to secondary workload generator")
            # Wait a bit for graceful shutdown, then force kill if needed
            try:
                if process2:
                    process2.wait(timeout=500)
            except subprocess.TimeoutExpired:
                if process2:
                    process2.kill()
                print("Force killed secondary workload generator")

        except subprocess.TimeoutExpired:
            run_process.kill()
            if process2:
                process2.kill()
            print(f"Timeout exceeded for YCSB run phase: {config}")
            metrics = {"throughput": 0.0}

    except Exception as e:
        print(f"Error running YCSB workloads: {e}")
        metrics = {"throughput": 0.0}
    return metrics
