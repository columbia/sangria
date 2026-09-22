# Sangria

Sangria is an adaptive commit protocol for distributed transactions. At each
participant, it chooses between retaining locks until commit (Strict-2PC) and
releasing locks after prepare (Pipelined-2PC). Transactions that use early lock
release are committed through the Resolver, which tracks their dependencies.

This repository contains the Rust prototype and the driver used for the
experiments in *Dances with Locks: An Adaptive Commit Protocol for Distributed
Transactions*.

## Repository layout

- `frontend/`, `rangeserver/`, `resolver/`, and `universe/`: principal server
  components.
- `coordinator/`, `coordinator-rangeclient/`, and `rangeclient/`: transaction
  coordination and participant clients.
- `tx_state_store/`: transaction-state persistence backed by Cassandra.
- `workload-generator/`: custom and YCSB workloads, experiment definitions,
  and plotting scripts.
- `configs/config.json`: server addresses and default Sangria thresholds.
- `schema/cassandra/atomix/`: Cassandra schema used by the prototype.

The paper experiments run all prototype components as native processes on one
host. Docker is used only for Cassandra; Kubernetes is not required.

## Prerequisites

The current experiment configuration targets Ubuntu 22.04 on a machine with at
least 32 logical CPUs. It has been exercised with Rust 1.86, Python 3.10,
Cassandra 5.0, Protocol Buffers 3.12, and FlatBuffers 24.3.25. Several CPU IDs
are assigned explicitly by the experiment driver, so a smaller machine requires
adjusting those lists first.

Install the system packages on a fresh Ubuntu host:

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential clang cmake git curl pkg-config libssl-dev \
  protobuf-compiler python3-venv python3-pip docker.io \
  libnspr4 libnss3 libgbm1
sudo systemctl enable --now docker
```

Install the Rust toolchain and FlatBuffers compiler:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --default-toolchain 1.86.0
source "$HOME/.cargo/env"
./scripts/linux_install_flatbuffers.sh
```

Create the Python environment used by the experiment and plotting scripts:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r workload-generator/scripts/requirements.txt
```

HTML plots require no browser. To enable automatic PNG export as well, install
the Chrome runtime used by Kaleido:

```bash
plotly_get_chrome -y
```

If the repository was not cloned recursively, initialize the YCSB submodule:

```bash
git submodule update --init --recursive
```

Finally, start a dedicated Cassandra container and load the schema:

```bash
sudo ./scripts/reset_cassandra.sh hard
```

The experiment driver truncates the `atomix` tables between trials. Do not
point it at a Cassandra instance containing data that must be retained.

## Running the experiments

Run experiments from the repository root. The driver builds the release
binaries, starts and stops the local server processes, resets Cassandra between
configurations, executes two repetitions where configured, and plots the
resulting data when the experiment finishes.

```bash
source "$HOME/.cargo/env"
source .venv/bin/activate
python workload-generator/scripts/run_experiments.py \
  --experiment fig10-contention
```

The available experiment names are:

| Name | Paper result or purpose |
| --- | --- |
| `tradeoff-contention-resolver` | Figure 4: contention versus Resolver load |
| `ycsb` | Figure 5: YCSB workload |
| `runtime-contention` | Figure 7: changing contention |
| `runtime-resolver` | Figure 8: changing Resolver load |
| `mixed-workload` | Figure 9: mixed hot and cold keys |
| `fig10-contention` | Figure 10(a): contention-threshold sensitivity |
| `fig10-resolver` | Figure 10(b): Resolver-load-threshold sensitivity |
| `table4` | Table 4: dependency-chain stress |
| `resolver-calibration` | Maps background clients to the Resolver-load signal |
| `resolver-microbenchmark` | Auxiliary Resolver throughput sweep |
| `early-lock-release-sensitivity` | Legacy two-threshold sensitivity sweep |

Only one experiment should run on a host at a time: experiments share fixed
localhost ports and the `cassandra` container.

## Results and plots

Each invocation creates a randomly named directory under:

```text
workload-generator/experiments/ray_logs/<experiment-name>/
```

The directory contains one result file per evaluated protocol, for example
`Adaptive_results.csv`, `Pipelined_results.csv`, and
`Traditional_results.csv`, together with Ray's per-trial output. Derived data
and paper-like plots are written to its `plots/` subdirectory after all
protocols finish. The plotting step always writes interactive HTML and summary
CSV files; it also writes PNG files when Kaleido's image renderer is available.

The random directory name is printed by Ray near the beginning of a run. To
locate the most recently completed result directory later, use:

```bash
find workload-generator/experiments/ray_logs -mindepth 1 -maxdepth 1 \
  -type d -printf '%T@ %p\n' | sort -n | tail -1
```

To regenerate plots without rerunning an experiment, set `RESULT_DIR` to that
run's random directory name (not its full path), activate the environment, and
run the matching command below. For example: `RESULT_DIR=rousing_taipan`.

```bash
source .venv/bin/activate
RESULT_DIR=rousing_taipan
```

**`tradeoff-contention-resolver` (Figure 4):**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=2500,zipf_exponent=0.0,num_keys=50 \
  --free-params resolver_tx_load_concurrency,max_concurrency
```

**`ycsb` (Figure 5):**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=5000,max_concurrency=50,num_keys=50 \
  --free-params resolver_tx_load_concurrency,zipf_exponent
```

**`runtime-contention` (Figure 7):**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=16000,zipf_exponent=0.0,num_keys=50 \
  --free-params resolver_tx_load_concurrency,max_concurrency
```

**`runtime-resolver` (Figure 8):**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=16000,zipf_exponent=0.0,num_keys=50 \
  --free-params resolver_tx_load_concurrency,max_concurrency
```

**`mixed-workload` (Figure 9):**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=16000,zipf_exponent=0.0,num_keys=50 \
  --free-params resolver_tx_load_concurrency,max_concurrency
```

**`fig10-contention` (Figure 10(a)):**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=16000,num_keys=50 \
  --free-params threshold_overrides
```

**`fig10-resolver` (Figure 10(b)):**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=32000,num_keys=50 \
  --free-params threshold_overrides
```

**`table4`:**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=5000,num_keys=50,max_concurrency=50 \
  --free-params baseline
```

**`resolver-calibration`:**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_keys=50 \
  --free-params resolver_tx_load_concurrency
```

**`resolver-microbenchmark`:**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=25000,zipf_exponent=0.0,num_keys=2000,resolver_tx_load_concurrency=0 \
  --free-params max_concurrency
```

**`early-lock-release-sensitivity`:**

```bash
python workload-generator/scripts/plot_experiments.py \
  --experiment-name "$RESULT_DIR" \
  --fixed-params num_queries=2500,zipf_exponent=0.0,num_keys=50,resolver_tx_load_concurrency=1000,max_concurrency=50 \
  --free-params early_lock_release_tuning
```

Manual plotting rewrites the derived files under the existing `plots/`
subdirectory but does not modify the raw `*_results.csv` measurements.

## Configuration notes

- Default adaptive-policy thresholds are in
  `configs/config.json` under `early_lock_release_tuning`; experiment-specific
  values are applied by `workload-generator/scripts/run_experiments.py`.
- Foreground concurrency schedules use `clients:transactions` phases separated
  by commas. Mixed hot/cold schedules use semicolon-separated groups.
- The setup is a single-host deployment using loopback addresses from
  `configs/config.json`.
- Server logs are written as `log-<component>.txt` in the repository root while
  an experiment is running.

## License

See [LICENSE](LICENSE).
