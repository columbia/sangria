import argparse
from datetime import datetime
import json
from pathlib import Path
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


def result_directories():
    if not RAY_LOGS_DIR.exists():
        return set()
    return {path.resolve() for path in RAY_LOGS_DIR.iterdir() if path.is_dir()}


def run_experiment(name, build):
    before = result_directories()
    command = [
        sys.executable,
        str(RUN_EXPERIMENTS),
        "--experiment",
        name,
        "--no-plot",
    ]
    if not build:
        command.append("--no-build")
    subprocess.run(command, cwd=ROOT_DIR, check=True)

    candidates = [
        path
        for path in result_directories() - before
        if any(path.glob("*_results.csv"))
    ]
    if len(candidates) != 1:
        names = ", ".join(sorted(path.name for path in candidates)) or "none"
        raise RuntimeError(
            f"Could not identify one result directory for {name}; found: {names}"
        )
    return candidates[0]


def write_manifest(path, manifest):
    path.write_text(json.dumps(manifest, indent=2) + "\n")


def set_experiment_entry(manifest, name, **values):
    entries = manifest.setdefault("experiments", [])
    entry = next((item for item in entries if item.get("name") == name), None)
    if entry is None:
        entry = {"name": name}
        entries.append(entry)
    entry.update(values)
    return entry


def existing_source(manifest, experiment):
    entry = next(
        (
            item
            for item in manifest.get("experiments", [])
            if item.get("name") == experiment
        ),
        None,
    )
    if not entry or not entry.get("source"):
        return None
    source = Path(entry["source"])
    if not source.is_absolute():
        source = ROOT_DIR / source
    if source.is_dir() and any(source.glob("*_results.csv")):
        return source.resolve()
    return None


def validate_source(source, experiment):
    from paper_plots.common import load_results

    results = load_results(source)
    if experiment == "table4":
        warmup_column = "warmup_before_measurement"
        if warmup_column not in results:
            raise ValueError("Table 4 source predates the unmeasured warm-up")
        warmed = results[warmup_column].astype(str).str.lower().isin(("true", "1"))
        if not warmed.all():
            raise ValueError("Table 4 contains a measurement without warm-up")
        counts = results.groupby("protocol").size()
        expected = {"Sangria", "Pipelined-2PC", "Strict-2PC"}
        if set(counts.index) != expected or not (counts == 2).all():
            raise ValueError(
                "Table 4 requires two warmed measurements per protocol"
            )


def render_paper_outputs(sources, suite_directory):
    from paper_plots.figure_04 import plot as plot_figure_04
    from paper_plots.figure_05 import plot as plot_figure_05
    from paper_plots.figure_06 import plot as plot_figure_06
    from paper_plots.figure_07 import plot as plot_figure_07
    from paper_plots.figure_08 import plot as plot_figure_08
    from paper_plots.figure_09 import plot as plot_figure_09
    from paper_plots.figure_10 import plot as plot_figure_10
    from paper_plots.figure_11 import plot as plot_figure_11
    from paper_plots.table_04 import plot as plot_table_04

    tradeoff = sources["tradeoff-contention-resolver"]
    plot_figure_04(tradeoff, suite_directory / "figure_04")
    plot_figure_05(sources["ycsb"], suite_directory / "figure_05")
    plot_figure_06(tradeoff, suite_directory / "figure_06")
    plot_figure_07(
        sources["runtime-contention"], suite_directory / "figure_07"
    )
    plot_figure_08(sources["runtime-resolver"], suite_directory / "figure_08")
    plot_figure_09(sources["mixed-workload"], suite_directory / "figure_09")
    plot_figure_10(
        sources["fig10-contention"],
        sources["fig10-resolver"],
        suite_directory / "figure_10",
    )
    plot_figure_11(tradeoff, suite_directory / "figure_11")
    plot_table_04(sources["table4"], suite_directory / "table_04")


def main():
    parser = argparse.ArgumentParser(
        description="Run every paper experiment and render publication figures."
    )
    destination = parser.add_mutually_exclusive_group()
    destination.add_argument(
        "--output-dir",
        type=Path,
        help="New suite directory (default: timestamp under paper_results).",
    )
    destination.add_argument(
        "--resume",
        type=Path,
        help="Resume an existing suite without rerunning completed measurements.",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Reuse existing server binaries instead of building once at the start.",
    )
    args = parser.parse_args()

    if args.resume:
        suite_directory = args.resume.expanduser().resolve()
        manifest_path = suite_directory / "manifest.json"
        if not manifest_path.is_file():
            parser.error(f"No manifest found at {manifest_path}")
        manifest = json.loads(manifest_path.read_text())
    else:
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

    sources = {}
    build_next = not args.no_build and not args.resume
    for index, experiment in enumerate(PAPER_EXPERIMENTS, start=1):
        source = existing_source(manifest, experiment)
        if source is not None:
            try:
                validate_source(source, experiment)
            except ValueError as error:
                print(
                    f"[{index}/{len(PAPER_EXPERIMENTS)}] Cannot reuse "
                    f"{source.name}: {error}",
                    flush=True,
                )
                source = None
            else:
                print(
                    f"[{index}/{len(PAPER_EXPERIMENTS)}] Reusing {experiment}: "
                    f"{source.name}",
                    flush=True,
                )
        if source is None:
            print(
                f"\n[{index}/{len(PAPER_EXPERIMENTS)}] Running {experiment}",
                flush=True,
            )
            try:
                source = run_experiment(experiment, build=build_next)
                validate_source(source, experiment)
            except Exception as error:
                set_experiment_entry(
                    manifest, experiment, status="failed", error=str(error)
                )
                write_manifest(manifest_path, manifest)
                raise
            build_next = False

        sources[experiment] = source
        set_experiment_entry(
            manifest,
            experiment,
            status="measured",
            source=str(source.relative_to(ROOT_DIR)),
            error=None,
        )
        write_manifest(manifest_path, manifest)

    print("\nRendering publication figures", flush=True)
    try:
        render_paper_outputs(sources, suite_directory)
    except Exception as error:
        manifest["plots"] = {"status": "failed", "error": str(error)}
        write_manifest(manifest_path, manifest)
        raise

    for experiment in PAPER_EXPERIMENTS:
        set_experiment_entry(manifest, experiment, status="complete", error=None)
    manifest["plots"] = {
        "status": "complete",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    write_manifest(manifest_path, manifest)
    print(f"\nCollected paper figures in {suite_directory}")


if __name__ == "__main__":
    main()
