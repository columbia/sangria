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


def cascading_abort_experiment(ray_logs_dir):
    """
    Test cascading abort performance by varying artificial abort rates.
    Uses artificial_abort_rate to randomly fail transactions during PREPARE.

    Results saved to: cascading_abort_experiment_results/
    - CSV files: spectacular_ant_cascading_abort_Pipelined_*pct_results.csv
    - Plot: cascading_abort_results.png
    - Plotter script: plot_cascading_abort_simple.py

    Last run: 2025-11-05 (spectacular_ant experiment)
    """
    BASELINES = [PIPELINED]
    NUM_ITERATIONS = 2  # Full test with 2 iterations
    NUM_QUERIES = [3000]  # Full workload size
    WORKLOAD_TYPE = ["custom"]
    ZIPFIAN_CONSTANT = [0.0]  # No skew needed

    # Fixed workload parameters
    NUM_KEYS = [100]
    MAX_CONCURRENCY = ["50"]  # Full concurrency

    # Test multiple abort rates
    ABORT_RATES = [0.1, 0.2, 0.3, 0.5]

    print("\n" + "="*80)
    print("🧪 FULL CASCADING ABORT EXPERIMENT")
    print(f"  Transactions: {NUM_QUERIES[0]}")
    print(f"  Concurrency: {MAX_CONCURRENCY[0]}")
    print(f"  Abort rates: {ABORT_RATES}")
    print(f"  Iterations: {NUM_ITERATIONS}")
    print("  Expected time: ~5-10 minutes total")
    print("="*80 + "\n")

    RESOLVER_TX_LOAD = [
        {
            "max_concurrency": "0",  # No extra load on resolver
            "num_queries": None,
            "num_keys": 100,
            "background_runtime_core_ids": [2, 3],
        }
    ]

    namespace, name = generate_slug(2).split("-")
    experiment_name = f"{namespace}_{name}_cascading_abort"

    # Run experiment for each baseline and abort rate
    for baseline in BASELINES:
        for abort_rate in ABORT_RATES:
            abort_rate_label = f"{int(abort_rate * 100)}pct"

            RESOLVER_CAPACITY = [
                {
                    "cpu_percentage": 1,
                    "background_runtime_core_ids": [1],
                },
            ]

            config = {
                "baseline": [baseline],
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
                "enable_cascading_abort": [True],  # Enable cascading abort
                "artificial_abort_rate": [abort_rate],  # Inject random aborts
            }

            reporter = tune.CLIReporter(
                metric_columns=["throughput"],
                parameter_columns=[
                    "baseline",
                    "artificial_abort_rate",
                    "iteration",
                    "num_queries",
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
                search_alg=GridSearcherInOrder(
                    atomix_setup, NUM_ITERATIONS, config, experiment_name, ray_logs_dir
                ),
                reuse_actors=True,
                max_concurrent_trials=1,
                scheduler=FIFOScheduler(),
                verbose=1,
                progress_reporter=reporter,
            )

            # Save results with descriptive filename
            output_path = ray_logs_dir / experiment_name / f"{experiment_name}_{baseline}_{abort_rate_label}_results.csv"
            analysis.results_df.to_csv(output_path)

    # Generate custom plots for cascading abort results
    plot_cascading_abort_results(experiment_name, ray_logs_dir)


def plot_cascading_abort_results(experiment_name, ray_logs_dir):
    """
    Custom plotter for cascading abort experiment results.
    Creates:
    1. Throughput vs Abort Rate graph
    2. Summary table
    """
    import pandas as pd
    import matplotlib.pyplot as plt

    experiment_dir = ray_logs_dir / experiment_name

    # Find all result CSV files
    result_files = list(experiment_dir.glob(f"{experiment_name}_*_results.csv"))

    if not result_files:
        print(f"No result files found in {experiment_dir}")
        return

    # Combine all results
    all_results = []
    for result_file in result_files:
        df = pd.read_csv(result_file)
        all_results.append(df)

    combined_df = pd.concat(all_results, ignore_index=True)

    # Extract baseline and abort rate
    summary = combined_df.groupby(["config/baseline", "config/artificial_abort_rate"])["throughput"].mean().reset_index()

    # Create abort rate label
    summary["abort_rate_pct"] = (summary["config/artificial_abort_rate"] * 100).astype(int).astype(str) + "%"
    summary = summary.sort_values("config/artificial_abort_rate")  # Sort by abort rate ascending

    # Plot throughput vs abort rate
    plt.figure(figsize=(12, 6))

    for baseline in summary["config/baseline"].unique():
        baseline_data = summary[summary["config/baseline"] == baseline]
        plt.plot(baseline_data["abort_rate_pct"], baseline_data["throughput"],
                marker='o', label=baseline, linewidth=2, markersize=8)

    plt.xlabel("Artificial Abort Rate (%)", fontsize=12)
    plt.ylabel("Throughput (txn/s)", fontsize=12)
    plt.title("Throughput vs Abort Rate - Cascading Abort Enabled", fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    # Save plot
    plot_path = experiment_dir / f"{experiment_name}_throughput_vs_abort_rate.png"
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {plot_path}")
    plt.close()

    # Create summary table
    pivot_table = summary.pivot_table(
        index="abort_rate_pct",
        columns="config/baseline",
        values="throughput",
        aggfunc="mean"
    )

    # Save summary CSV
    summary_path = experiment_dir / f"{experiment_name}_summary.csv"
    pivot_table.to_csv(summary_path)
    print(f"Summary table saved to: {summary_path}")

    print("\nSummary Statistics:")
    print(pivot_table.to_string())


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
        "zipf_exponent": ZIPFIAN_CONSTANT,
        "namespace": [namespace],
        "name": [name],
        "background_runtime_core_ids": [list(range(3, 32))],
        "workload_type": WORKLOAD_TYPE,
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
            "zipf_exponent",
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

    plot_results_df(experiment_name, fixed_params, free_params)


def main():
    ray.init()
    ray_logs_dir = Path(RAY_LOGS_DIR)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    if BUILD_ATOMIX:
        atomix_setup.build_servers()

    # Run cascading abort experiment
    cascading_abort_experiment(ray_logs_dir)

    # Other experiments (commented out)
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
