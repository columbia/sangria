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
    ZIPFIAN_CONSTANT = [0.0]
    NUM_ITERATIONS = 2
    NUM_QUERIES = [2500]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["1", "5", "25", "50", "100", "200", "500"]
    WORKLOAD_TYPE = ["custom"]
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
    fixed_params = {
        "num_queries": NUM_QUERIES[0],
        "zipf_exponent": ZIPFIAN_CONSTANT[0],
        "num_keys": NUM_KEYS[0],
    }
    free_params = "resolver_tx_load_concurrency,max_concurrency"

    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ZIPFIAN_CONSTANT,
        WORKLOAD_TYPE,
        ray_logs_dir,
        fixed_params,
        free_params,
    )


def runtime_variations_contention_experiment(ray_logs_dir):
    BASELINES = [ADAPTIVE, PIPELINED, TRADITIONAL]
    ZIPFIAN_CONSTANT = [0.0]
    NUM_ITERATIONS = 2
    NUM_QUERIES = [16000]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["25:4000,500:4000,25:4000,500:4000"]
    WORKLOAD_TYPE = ["custom"]
    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "0",
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

    fixed_params = {
        "num_queries": NUM_QUERIES[0],
        "zipf_exponent": ZIPFIAN_CONSTANT[0],
        "num_keys": NUM_KEYS[0],
    }
    free_params = "resolver_tx_load_concurrency,max_concurrency"


    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ZIPFIAN_CONSTANT,
        WORKLOAD_TYPE,
        ray_logs_dir,
        fixed_params,
        free_params,
    )


def runtime_variations_resolver_capacity_experiment(ray_logs_dir):
    BASELINES = [ADAPTIVE, PIPELINED, TRADITIONAL]
    NUM_ITERATIONS = 2
    ZIPFIAN_CONSTANT = [0.0]
    NUM_QUERIES = [16000]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["5", "50", "500"]
    WORKLOAD_TYPE = ["custom"]
    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "1000:180000",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        }
    ]
    fixed_params = {
        "num_queries": NUM_QUERIES[0],
        "zipf_exponent": ZIPFIAN_CONSTANT[0],
        "num_keys": NUM_KEYS[0],
    }
    free_params = "resolver_tx_load_concurrency,max_concurrency"

    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ZIPFIAN_CONSTANT,
        WORKLOAD_TYPE,
        ray_logs_dir,
        fixed_params,
        free_params,
    )


def mixed_workload_experiment(ray_logs_dir):
    BASELINES = [ADAPTIVE, PIPELINED, TRADITIONAL]
    ZIPFIAN_CONSTANT = [0.0]
    NUM_ITERATIONS = 2
    NUM_QUERIES = [16000]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["25:8000;500:8000"]
    WORKLOAD_TYPE = ["custom"]
    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "0",
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
    fixed_params = {
        "num_queries": NUM_QUERIES[0],
        "zipf_exponent": ZIPFIAN_CONSTANT[0],
        "num_keys": NUM_KEYS[0],
    }
    free_params = "resolver_tx_load_concurrency,max_concurrency"

    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ZIPFIAN_CONSTANT,
        WORKLOAD_TYPE,
        ray_logs_dir,
        fixed_params,
        free_params,
    )


def ycsb_experiment(ray_logs_dir):
    BASELINES = [ADAPTIVE, PIPELINED, TRADITIONAL]
    NUM_ITERATIONS = 2
    WORKLOAD_TYPE = ["ycsb"]
    NUM_QUERIES = [5000]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["50"]
    ZIPFIAN_CONSTANT = [0.0, 0.5, 1.0]
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

    fixed_params = {
        "num_queries": NUM_QUERIES[0],
        "max_concurrency": MAX_CONCURRENCY[0],
        "num_keys": NUM_KEYS[0],
    }
    free_params = "resolver_tx_load_concurrency,zipf_exponent"

    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ZIPFIAN_CONSTANT,
        WORKLOAD_TYPE,
        ray_logs_dir,
        fixed_params,
        free_params,
    )


def resolver_microbenchmark_experiment(ray_logs_dir):
    """Microbenchmark that isolates Resolver dependency-tracking and commit-processing.

    - Runs the Resolver with all available background cores.
    - Runs only fake transactions as the main workload to isolate Resolver overhead.
    - Sweeps client concurrency so throughput ramps up and then plateaus near capacity.
    """
    BASELINES = [PIPELINED]
    NUM_ITERATIONS = 1
    ZIPFIAN_CONSTANT = [0.0]

    # Use enough queries per point to stabilize throughput measurements.
    NUM_QUERIES = [25000]
    NUM_KEYS = [2000]
    # Sweep concurrency from under-saturated to over-saturated to reveal plateau.
    MAX_CONCURRENCY = ["20", "50", "100", "150", "200", "250"]

    # We run only the fake workload as the main workload (no secondary load process).
    # Set resolver_tx_load to zero so run_workload won't spawn a secondary process,
    # and set `main_fake=True` to make the main workload emit fake transactions.
    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "0",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [],
        }
    ]

    # Hold everything fixed and vary only main concurrency for a clean throughput curve.
    fixed_params = {
        "num_queries": NUM_QUERIES[0],
        "zipf_exponent": ZIPFIAN_CONSTANT[0],
        "num_keys": NUM_KEYS[0],
        "resolver_tx_load_concurrency": RESOLVER_TX_LOAD[0]["max_concurrency"],
    }
    free_params = "max_concurrency"

    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ZIPFIAN_CONSTANT,
        ["one-key"],
        ray_logs_dir,
        fixed_params,
        free_params,
        main_fake=[True],
        resolver_background_runtime_core_ids=[],
    )


def early_lock_release_sensitivity_experiment(ray_logs_dir):
    """Basic sensitivity analysis for Adaptive do_early_lock_release thresholds.

    It runs two sweeps:
    1) Vary resolver-load thresholds while keeping contention thresholds fixed.
    2) Vary contention thresholds while keeping resolver-load thresholds fixed.
    """
    BASELINES = [ADAPTIVE]
    NUM_ITERATIONS = 2
    ZIPFIAN_CONSTANT = [0.0]
    NUM_QUERIES = [2500]
    NUM_KEYS = [50]
    MAX_CONCURRENCY = ["50"]
    WORKLOAD_TYPE = ["custom"]
    free_params = "early_lock_release_tuning"

    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "1000",
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        },
    ]

    fixed_params = {
        "num_queries": NUM_QUERIES[0],
        "zipf_exponent": ZIPFIAN_CONSTANT[0],
        "num_keys": NUM_KEYS[0],
        "resolver_tx_load_concurrency": RESOLVER_TX_LOAD[0]["max_concurrency"],
        "max_concurrency": MAX_CONCURRENCY[0],
    }

    # Explicit OpenClients threshold configs (coarse grid) with invariant: mid > low.
    # This directly tunes regions instead of using a proxy scale.
    open_clients_points = [20.0, 40.0, 60.0, 80.0, 100.0, 120.0]
    threshold_overrides = [
        {
            "open_clients_low": low,
            "open_clients_mid": mid,
        }
        for low in open_clients_points
        for mid in open_clients_points
        if mid > low
    ]

    run_experiment(
        BASELINES,
        RESOLVER_TX_LOAD,
        NUM_ITERATIONS,
        NUM_QUERIES,
        NUM_KEYS,
        MAX_CONCURRENCY,
        ZIPFIAN_CONSTANT,
        WORKLOAD_TYPE,
        ray_logs_dir,
        fixed_params,
        free_params,
        threshold_overrides=threshold_overrides,
    )


def run_experiment(
    BASELINES,
    RESOLVER_TX_LOAD,
    NUM_ITERATIONS,
    NUM_QUERIES,
    NUM_KEYS,
    MAX_CONCURRENCY,
    ZIPFIAN_CONSTANT,
    WORKLOAD_TYPE,
    ray_logs_dir,
    fixed_params,
    free_params,
    main_fake=None,
    resolver_background_runtime_core_ids=None,
    threshold_overrides=None,

):
    namespace, name = generate_slug(2).split("-")
    experiment_name = f"{namespace}_{name}"

    if resolver_background_runtime_core_ids is None:
        resolver_background_runtime_core_ids = [1]
    if threshold_overrides is None:
        threshold_overrides = [{}]

    RESOLVER_CAPACITY = [
        {
            "cpu_percentage": 1,
            "background_runtime_core_ids": resolver_background_runtime_core_ids,
        },
    ]

    config = {
        "baseline": BASELINES,
        "num_keys": NUM_KEYS,
        "max_concurrency": MAX_CONCURRENCY,
        "resolver_capacity": RESOLVER_CAPACITY,
        "resolver_tx_load": RESOLVER_TX_LOAD,
        "num_queries": NUM_QUERIES,
        "zipf_exponent": ZIPFIAN_CONSTANT,
        "namespace": [namespace],
        "name": [name],
        "background_runtime_core_ids": [list(range(3, 32))],
        "workload_type": WORKLOAD_TYPE,
        "threshold_overrides": threshold_overrides,
    }

    # Allow overriding whether the main workload is a fake-transaction generator
    config["main_fake"] = main_fake if main_fake is not None else [False]
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
            "zipf_exponent",
            "threshold_overrides",
        ],
        max_report_frequency=20,
    )
    for baseline in BASELINES:
        config["baseline"] = [baseline]
        if baseline == TRADITIONAL:
            config["resolver_capacity"] = [RESOLVER_CAPACITY[0]]
            config["resolver_tx_load"] = [
                {
                    "max_concurrency": "0",
                    "num_queries": None,
                    "num_keys": 100,
                    "background_runtime_core_ids": [2, 3],
                }
            ]
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

    # plot_results_df(experiment_name, fixed_params, free_params)


def main():
    ray.init()
    ray_logs_dir = Path(RAY_LOGS_DIR)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    if BUILD_ATOMIX:
        atomix_setup.build_servers()
    # resolver_microbenchmark_experiment(ray_logs_dir)
    early_lock_release_sensitivity_experiment(ray_logs_dir)
    # tradeoff_contention_vs_resolver_capacity_experiment(ray_logs_dir)
    # runtime_variations_contention_experiment(ray_logs_dir)
    # runtime_variations_resolver_capacity_experiment(ray_logs_dir)
    # mixed_workload_experiment(ray_logs_dir)
    # ycsb_experiment(ray_logs_dir)
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
