import subprocess
from pathlib import Path
import psutil
import ray
from ray import tune
from coolname import generate_slug
import pandas as pd
import argparse
from ray.tune.schedulers import FIFOScheduler
from ray_task import run_workload
from grid_searcher import GridSearcherInOrder
from atomix_setup import AtomixSetup
from utils import *
from math import prod

from atomix_setup import atomix_setup


def varying_contention_and_resolver_struggling_experiment(ray_logs_dir):
    namespace, name = generate_slug(2).split("-")
    experiment_name = f"{namespace}_{name}"

    # baselines = [TRADITIONAL, PIPELINED, ADAPTIVE]
    BASELINES = [PIPELINED, TRADITIONAL]

    NUM_ITERATIONS = 2
    NUM_QUERIES = [1500]
    # NUM_KEYS = [1, 5, 10, 25, 50, 75, 100]
    NUM_KEYS = [255]
    MAX_CONCURRENCY = [2]

    # Define the search space
    config = {
        "baseline": BASELINES,
        "num-keys": NUM_KEYS,
        "max-concurrency": MAX_CONCURRENCY,
        "cpu_percentage": [1],
        "num-queries": NUM_QUERIES,
        "zipf-exponent": [0],
        "namespace": [namespace],
        "name": [name],
        "background-runtime-core-ids": [list(range(4, 32))],
    }

    reporter = tune.CLIReporter(
        metric_columns=["throughput"],
        parameter_columns=[
            "baseline",
            "num-keys",
            "num-queries",
            "iteration",
            "cpu_percentage",
        ],
        max_report_frequency=20,
    )
    analysis = tune.run(
        tune.with_parameters(run_workload),
        config={},
        num_samples=prod([len(v) for v in list(config.values())]) * NUM_ITERATIONS,
        resources_per_trial={"cpu": psutil.cpu_count()},
        storage_path=ray_logs_dir,
        name=experiment_name,
        search_alg=GridSearcherInOrder(atomix_setup, NUM_ITERATIONS, config),
        reuse_actors=True,
        max_concurrent_trials=1,
        scheduler=FIFOScheduler(),
        verbose=1,
        progress_reporter=reporter,
    )
    analysis.results_df.to_csv(ray_logs_dir / experiment_name / f"results.csv")
    plot_results_df(experiment_name, MAX_CONCURRENCY[0], NUM_QUERIES[0])
    ray.shutdown()


def varying_contention_experiment(ray_logs_dir):
    namespace, name = generate_slug(2).split("-")
    experiment_name = f"{namespace}_{name}"

    resolver_modes = [REMOTE]
    # baselines = [TRADITIONAL, PIPELINED, ADAPTIVE]
    baselines = [PIPELINED, TRADITIONAL]

    NUM_ITERATIONS = 2
    # NUM_QUERIES = [1500]
    # NUM_KEYS = [1, 5, 10, 25, 50, 75, 100]
    NUM_QUERIES = [1500]
    NUM_KEYS = [200]
    MAX_CONCURRENCY = [2]

    num_combinations = len(NUM_KEYS) * len(NUM_QUERIES) * NUM_ITERATIONS

    # Define the search space
    workload_config = {
        "num-keys": NUM_KEYS,
        "max-concurrency": MAX_CONCURRENCY,
        "num-queries": NUM_QUERIES,
        "zipf-exponent": [0],
        "namespace": [namespace],
        "name": [name],
        "background-runtime-core-ids": [list(range(5, 32))],
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
            results = analysis.results_df

            results["config/baseline"] = baseline_name
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
            "--max-concurrency",
            str(MAX_CONCURRENCY[0]),
            "--num-queries",
            str(NUM_QUERIES[0]),
        ]
    )
    ray.shutdown()


def main():
    ray.init()
    ray_logs_dir = Path(RAY_LOGS_DIR)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    if BUILD_ATOMIX:
        atomix_setup.build_servers()

    varying_contention_and_resolver_struggling_experiment(ray_logs_dir)


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
