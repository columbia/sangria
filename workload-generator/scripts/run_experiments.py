import subprocess
from pathlib import Path
import psutil
import ray
from ray import tune
from coolname import generate_slug
import argparse
from ray.tune.schedulers import FIFOScheduler
from ray_task import run_workload
from grid_searcher import GridSearcherInOrder
from atomix_setup import AtomixSetup
from utils import *
from math import prod

from atomix_setup import atomix_setup


def tradeoff_contention_vs_resolver_capacity_experiment(ray_logs_dir):
    BASELINES = [ADAPTIVE, PIPELINED, TRADITIONAL]
    NUM_ITERATIONS = 2
    NUM_QUERIES = [2500]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["1", "5", "25", "50", "100", "200", "500"]
    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "0",  # zero extra load
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        },
        {
            "max_concurrency": "100",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        },
        {
            "max_concurrency": "1000",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        },
    ]
    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ray_logs_dir,
    )


def runtime_variations_contention_experiment(ray_logs_dir):
    BASELINES = [ADAPTIVE, PIPELINED, TRADITIONAL]
    NUM_ITERATIONS = 2
    NUM_QUERIES = [16000]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["25:4000,500:4000,25:4000,500:4000"]
    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "100",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        },
        {
            "max_concurrency": "1000",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        },
    ]

    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ray_logs_dir,
    )


def runtime_variations_resolver_capacity_experiment(ray_logs_dir):
    BASELINES = [ADAPTIVE, PIPELINED, TRADITIONAL]
    NUM_ITERATIONS = 2
    NUM_QUERIES = [16000]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["50"]

    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "1000:180000",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        }
    ]

    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ray_logs_dir,
    )

def mixed_workload_experiment(ray_logs_dir):
    BASELINES = [ADAPTIVE, PIPELINED, TRADITIONAL]
    NUM_ITERATIONS = 2
    NUM_QUERIES = [16000]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["25:8000;500:8000"]

    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "100",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        },
        {
            "max_concurrency": "1000",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        },
    ]

    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ray_logs_dir,
    )

def run_experiment(
    BASELINES,
    RESOLVER_TX_LOAD,
    NUM_ITERATIONS,
    NUM_QUERIES,
    NUM_KEYS,
    MAX_CONCURRENCY,
    ray_logs_dir,
):
    namespace, name = generate_slug(2).split("-")
    experiment_name = f"{namespace}_{name}"

    RESOLVER_CAPACITY = [
        {
            "cpu_percentage": 1,
            "background_runtime_core_ids": [1],  # + list(range(3, 32))
        },
    ]

    config = {
        "baseline": BASELINES,
        "num_keys": NUM_KEYS,
        "max_concurrency": MAX_CONCURRENCY,
        "resolver_capacity": RESOLVER_CAPACITY,
        "resolver_tx_load": RESOLVER_TX_LOAD,
        "num_queries": NUM_QUERIES,
        "zipf_exponent": [0],
        "namespace": [namespace],
        "name": [name],
        "background_runtime_core_ids": [list(range(3, 32))],
    }
    reporter = tune.CLIReporter(
        metric_columns=["throughput"],
        parameter_columns=[
            "baseline",
            "num_keys",
            "max_concurrency",
            "iteration",
            "resolver_cores",
            "resolver_tx_load_concurrency",
            "num_queries",
        ],
        max_report_frequency=20,
    )
    for baseline in BASELINES:
        config["baseline"] = [baseline]
        if baseline == TRADITIONAL:
            config["resolver_capacity"] = [RESOLVER_CAPACITY[0]]
            config["resolver_tx_load"] = [RESOLVER_TX_LOAD[0]]
        elif baseline == PIPELINED or baseline == ADAPTIVE:
            config["resolver_capacity"] = RESOLVER_CAPACITY
            config["resolver_tx_load"] = RESOLVER_TX_LOAD

        analysis = tune.run(
            tune.with_parameters(run_workload),
            config={},
            num_samples=prod([len(v) for v in list(config.values())]) * NUM_ITERATIONS,
            resources_per_trial={"cpu": psutil.cpu_count()},
            storage_path=ray_logs_dir,
            name=experiment_name,
            search_alg=GridSearcherInOrder(
                atomix_setup, NUM_ITERATIONS, config, experiment_name, ray_logs_dir
            ),
            reuse_actors=True,
            max_concurrent_trials=1,
            scheduler=FIFOScheduler(),
            verbose=1,
            progress_reporter=reporter,
        )
        analysis.results_df.to_csv(
            ray_logs_dir / experiment_name / f"{baseline}_results.csv"
        )

    fixed_params = {
        "num_queries": NUM_QUERIES[0],
        "zipf_exponent": 0.0,
        # "max_concurrency": MAX_CONCURRENCY[0],
        "num_keys": NUM_KEYS[0],
    }
    free_params = "resolver_tx_load_concurrency,max_concurrency"
    plot_results_df(experiment_name, fixed_params, free_params)
    

def main():
    ray.init()
    ray_logs_dir = Path(RAY_LOGS_DIR)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    if BUILD_ATOMIX:
        atomix_setup.build_servers()

    # tradeoff_contention_vs_resolver_capacity_experiment(ray_logs_dir)
    # runtime_variations_contention_experiment(ray_logs_dir)
    # runtime_variations_resolver_capacity_experiment(ray_logs_dir)
    mixed_workload_experiment(ray_logs_dir)
    ray.shutdown()

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
