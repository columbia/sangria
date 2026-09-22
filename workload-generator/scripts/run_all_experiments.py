import argparse
from datetime import datetime
import json
from pathlib import Path
import shutil
import subprocess
import sys


ROOT_DIR = Path(__file__).resolve().parents[2]
RAY_LOGS_DIR = ROOT_DIR / "workload-generator" / "experiments" / "ray_logs"
PAPER_RESULTS_DIR = ROOT_DIR / "workload-generator" / "experiments" / "paper_results"
RUN_EXPERIMENTS = ROOT_DIR / "workload-generator" / "scripts" / "run_experiments.py"

PAPER_EXPERIMENTS = [
    "tradeoff-contention-resolver",
    "ycsb",
    "runtime-contention",
    "runtime-resolver",
    "mixed-workload",
    "fig10-contention",
    "fig10-resolver",
    "table4",
]

PRIMARY_FOLDERS = {
    "tradeoff-contention-resolver": Path("figure_04"),
    "ycsb": Path("figure_05"),
    "runtime-contention": Path("figure_07"),
    "runtime-resolver": Path("figure_08"),
    "mixed-workload": Path("figure_09"),
    "fig10-contention": Path("figure_10") / "panel_a_contention",
    "fig10-resolver": Path("figure_10") / "panel_b_resolver",
    "table4": Path("table_04"),
}

REQUIRED_OUTPUTS = {
    "tradeoff-contention-resolver": [
        "throughput.html",
        "q1_crossover_ratio.html",
        "q1_low_capacity_latency.html",
    ],
    "ycsb": ["throughput.html"],
    "runtime-contention": ["throughput.html"],
    "runtime-resolver": ["throughput.html"],
    "mixed-workload": ["throughput.html"],
    "fig10-contention": ["fig10_contention.html"],
    "fig10-resolver": ["fig10_resolver.html"],
    "table4": ["table4_summary.csv", "table4_performance.html"],
}


def result_directories():
    if not RAY_LOGS_DIR.exists():
        return set()
    return {path.resolve() for path in RAY_LOGS_DIR.iterdir() if path.is_dir()}


def run_experiment(name, build):
    before = result_directories()
    command = [sys.executable, str(RUN_EXPERIMENTS), "--experiment", name]
    if not build:
        command.append("--no-build")
    subprocess.run(command, cwd=ROOT_DIR, check=True)

    new_directories = result_directories() - before
    candidates = [
        path for path in new_directories if any(path.glob("*_results.csv"))
    ]
    if len(candidates) != 1:
        names = ", ".join(sorted(path.name for path in candidates)) or "none"
        raise RuntimeError(
            f"Could not identify one result directory for {name}; found: {names}"
        )
    return candidates[0]


def copy_plot_files(files, destination):
    destination.mkdir(parents=True, exist_ok=True)
    files = [path for path in files if path.is_file()]
    if not files:
        raise RuntimeError(f"No plot files found for {destination.name}")
    for path in files:
        shutil.copy2(path, destination / path.name)


def collect_plots(experiment, result_directory, suite_directory):
    plots = result_directory / "plots"
    for relative_path in REQUIRED_OUTPUTS[experiment]:
        if not (plots / relative_path).is_file():
            raise RuntimeError(
                f"{experiment} completed, but {plots / relative_path} is missing"
            )

    primary = suite_directory / PRIMARY_FOLDERS[experiment]
    if experiment == "tradeoff-contention-resolver":
        copy_plot_files(plots.glob("throughput*"), primary)
    else:
        copy_plot_files(plots.iterdir(), primary)
    destinations = [primary]

    if experiment == "tradeoff-contention-resolver":
        figure_6 = suite_directory / "figure_06"
        figure_6.mkdir(parents=True, exist_ok=True)
        for path in plots.glob("q1_*"):
            if path.is_file():
                shutil.copy2(path, figure_6 / path.name)
        destinations.append(figure_6)

        group_sizes = plots / "resolver" / "group_sizes"
        if not group_sizes.is_dir() or not any(group_sizes.glob("*.html")):
            raise RuntimeError("Figure 11 batch-size plots were not generated")
        figure_11 = suite_directory / "figure_11"
        shutil.copytree(group_sizes, figure_11)
        destinations.append(figure_11)

    return [str(path.relative_to(suite_directory)) for path in destinations]


def write_manifest(path, manifest):
    path.write_text(json.dumps(manifest, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Run every paper experiment sequentially and collect its plots."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Collection directory (default: a timestamp under paper_results).",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Reuse existing server binaries instead of building once at the start.",
    )
    args = parser.parse_args()

    if args.output_dir:
        suite_directory = args.output_dir.expanduser().resolve()
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suite_directory = PAPER_RESULTS_DIR / timestamp
    suite_directory.mkdir(parents=True, exist_ok=False)

    manifest = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "experiments": [],
    }
    manifest_path = suite_directory / "manifest.json"
    write_manifest(manifest_path, manifest)

    for index, experiment in enumerate(PAPER_EXPERIMENTS, start=1):
        print(
            f"\n[{index}/{len(PAPER_EXPERIMENTS)}] Running {experiment}",
            flush=True,
        )
        try:
            result_directory = run_experiment(
                experiment, build=(index == 1 and not args.no_build)
            )
            destinations = collect_plots(
                experiment, result_directory, suite_directory
            )
        except Exception as error:
            manifest["experiments"].append(
                {"name": experiment, "status": "failed", "error": str(error)}
            )
            write_manifest(manifest_path, manifest)
            raise

        manifest["experiments"].append(
            {
                "name": experiment,
                "status": "complete",
                "source": str(result_directory.relative_to(ROOT_DIR)),
                "collected_under": destinations,
            }
        )
        write_manifest(manifest_path, manifest)

    print(f"\nCollected paper plots in {suite_directory}")


if __name__ == "__main__":
    main()
