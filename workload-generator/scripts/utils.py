import os
from pathlib import Path
import subprocess
import json

ROOT_DIR = Path(__file__).parent.parent.parent
SERVERS_CONFIG_PATH = ROOT_DIR / "configs" / "config.json"
WORKLOAD_GENERATOR_DIR = ROOT_DIR / "workload-generator"
RAY_LOGS_DIR = WORKLOAD_GENERATOR_DIR / "experiments" / "ray_logs"
RAY_SERVERS_CONFIG_PATH = WORKLOAD_GENERATOR_DIR / "configs" / "config-ray.json"
MAIN_RAY_WORKLOAD_CONFIG_PATH = (
    WORKLOAD_GENERATOR_DIR / "configs" / "workload-config-ray-main.json"
)
SECONDARY_RAY_WORKLOAD_CONFIG_PATH = (
    WORKLOAD_GENERATOR_DIR / "configs" / "workload-config-ray-secondary.json"
)

TARGET_RUN_CMD = str(ROOT_DIR) + "/target/release/"

BUILD_ATOMIX = True

LOCAL = "Local"
REMOTE = "Remote"
TRADITIONAL = "Traditional"
PIPELINED = "Pipelined"
ADAPTIVE = "Adaptive"

os.environ["RAY_AIR_NEW_OUTPUT"] = "0"


def plot_results_df(experiment_name, fixed_params, free_params):
    cmd = [
        "python",
        str(WORKLOAD_GENERATOR_DIR / "scripts" / "plot_experiments.py"),
        "--experiment-name",
        experiment_name,
        "--fixed-params",
        ",".join([f"{k}={v}" for k, v in fixed_params.items()]),
        "--free-params",
        free_params,
    ]
    print(" ".join(cmd))
    subprocess.run(cmd)


def get_unique_key_values(df, key):
    return set(df[key].unique())


def process_resolver_stats_group_sizes(df, free_param):
    group_sizes = set()
    resolver_stats_per_baseline = {}
    cumvalues = {}

    baselines = sorted(get_unique_key_values(df, "baseline"))

    for free_param_value in sorted(get_unique_key_values(df, free_param)):
        resolver_stats_per_baseline[free_param_value] = {}
        for baseline in baselines:
            df_key = df[
                (df["baseline"] == baseline) & (df[free_param] == free_param_value)
            ]
            resolver_stats = {}
            for _, row in df_key.iterrows():
                d = row["resolver_stats"]
                for k, v in d.items():
                    if k != "Group size: 0":
                        resolver_stats[k] = resolver_stats.get(k, 0) + v
            resolver_stats = {
                int(k.split(":")[1]): v for k, v in resolver_stats.items()
            }
            group_sizes.update(resolver_stats.keys())
            resolver_stats_per_baseline[free_param_value][baseline] = resolver_stats
    for free_param_value in resolver_stats_per_baseline.keys():
        cumvalues[free_param_value] = {}
        for baseline in resolver_stats_per_baseline[free_param_value].keys():
            resolver_stats = resolver_stats_per_baseline[free_param_value][baseline]
            for group_size in group_sizes:
                if group_size not in resolver_stats:
                    resolver_stats[group_size] = 0
                sum_resolver_stats = sum(resolver_stats.values())
                resolver_stats = {
                    k: v / sum_resolver_stats for k, v in resolver_stats.items()
                }
                resolver_stats = dict(sorted(resolver_stats.items()))
                cumvalues[free_param_value][baseline] = [
                    sum(list(resolver_stats.values())[: i + 1])
                    for i in range(len(resolver_stats))
                ]
    return cumvalues, group_sizes