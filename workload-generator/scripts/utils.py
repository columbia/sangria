import os
from pathlib import Path
import subprocess

ROOT_DIR = Path(__file__).parent.parent.parent
SERVERS_CONFIG_PATH = ROOT_DIR / "configs" / "config.json"
WORKLOAD_GENERATOR_DIR = ROOT_DIR / "workload-generator"
RAY_LOGS_DIR = WORKLOAD_GENERATOR_DIR / "experiments" / "ray_logs"
RAY_SERVERS_CONFIG_PATH = WORKLOAD_GENERATOR_DIR / "configs" / "config-ray.json"
RAY_WORKLOAD_CONFIG_PATH = (
    WORKLOAD_GENERATOR_DIR / "configs" / "workload-config-ray.json"
)

TARGET_RUN_CMD = str(ROOT_DIR) + "/target/release/"

BUILD_ATOMIX = True

LOCAL = "Local"
REMOTE = "Remote"
TRADITIONAL = "Traditional"
PIPELINED = "Pipelined"
ADAPTIVE = "Adaptive"

os.environ["RAY_AIR_NEW_OUTPUT"] = "0"


def plot_results_df(experiment_name, max_concurrency, num_queries):
    subprocess.run(
        [
            "python",
            WORKLOAD_GENERATOR_DIR / "scripts" / "plot_experiments.py",
            "--experiment-name",
            experiment_name,
            "--max-concurrency",
            str(max_concurrency),
            "--num-queries",
            str(num_queries),
        ]
    )
