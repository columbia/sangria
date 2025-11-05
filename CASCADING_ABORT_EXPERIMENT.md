# Cascading Abort Experiment

## Overview
This experiment tests the performance of cascading abort functionality by varying **artificial abort rates** in Pipelined and Adaptive commit strategies.

## What Was Added

### 1. Artificial Abort Rate Configuration

**Location:** `common/src/config.rs`

**New field:** `artificial_abort_rate: f64`

**What it does:**
- Controls the probability that a transaction will fail during PREPARE phase
- Value range: 0.0 (no failures) to 1.0 (all transactions fail)
- Example: 0.2 means 20% of transactions will randomly abort during PREPARE

### 2. Random Failure Injection (`rangeserver/src/range_manager/impl.rs`)

**Location:** Line 259-269 in `prepare()` function

**What it does:**
- Before processing any transaction, checks `artificial_abort_rate`
- Generates random number and compares to abort rate
- If random < abort_rate, artificially fails with `TransactionAbortReason::PrepareFailed`

### 3. New Experiment Function (`run_experiments.py`)

**Location:** `workload-generator/scripts/run_experiments.py`

**Function:** `cascading_abort_experiment(ray_logs_dir)`

**What it does:**
- Tests **Pipelined** and **Adaptive** strategies only (Traditional doesn't support cascading abort)
- Uses **fixed workload**: 100 keys, 50 concurrent transactions
- Varies **artificial abort rate**: 10%, 20%, 30%, 50%
- Enables cascading abort: `enable_cascading_abort=True`
- Runs 3000 queries per configuration
- 2 iterations for statistical significance

**Key insight:** Higher abort rate = more cascading aborts = lower throughput

### 4. Custom Plotter (`run_experiments.py`)

**Function:** `plot_cascading_abort_results(experiment_name, ray_logs_dir)`

**Generates:**
1. **Throughput vs Abort Rate graph** (PNG):
   - X-axis: Artificial abort rate (%)
   - Y-axis: Throughput (txn/s)
   - Shows Pipelined vs Adaptive performance

2. **Summary table** (CSV):
   - Pivot table showing throughput for each strategy at each abort rate

### 5. Updated Configuration Handling (`ray_task.py`)

**Changes:**
- Extracts `enable_cascading_abort` and `artificial_abort_rate` from config
- Sets them in `atomix_setup.servers_config`
- Properly cleans up parameters after use

## How to Run

### On Remote Server:

```bash
# 1. SSH into the server
ssh -i /path/to/key ifesi@c220g1-030617.wisc.cloudlab.us

# 2. Navigate to sangria directory
cd ~/sangria

# 3. Make sure you're on cascading-aborts branch
git branch  # Should show cascading-aborts

# 4. Build release binary
source ~/.cargo/env
cargo build --release

# 5. Run the experiment
cd workload-generator/scripts
python run_experiments.py
```

## What to Expect

### During Execution:
- Experiment will test **4 abort rates × 2 baselines × 2 iterations = 16 trials**
- Each trial runs 3000 transactions
- Cassandra is cleaned between trials
- Servers are restarted for each configuration

### After Completion:

**Results directory:** `workload-generator/experiments/ray_logs/<experiment_name>_cascading_abort/`

**Files generated:**
1. `<experiment_name>_Pipelined_<X>pct_results.csv` - Raw results for each abort rate
2. `<experiment_name>_Adaptive_<X>pct_results.csv` - Raw results for each abort rate
3. `<experiment_name>_throughput_vs_abort_rate.png` - Performance graph
4. `<experiment_name>_summary.csv` - Summary table

### Expected Results:

**Hypothesis:**
- **10% abort rate**: Moderate throughput reduction, some cascading
- **20% abort rate**: More significant throughput drop, increased cascading
- **30% abort rate**: Substantial performance impact
- **50% abort rate**: Severe throughput degradation, many cascading aborts
- **Pipelined vs Adaptive**: May show different resilience to cascading aborts

## Metrics Collected

For each trial:
- **Throughput** (txn/s)
- **Baseline** (Pipelined/Adaptive)
- **artificial_abort_rate** (probability of random abort)
- **num_queries** (total transactions)
- **iteration** (for averaging)

## Troubleshooting

### If build fails:
```bash
cargo clean
cargo build --release
```

### If Cassandra errors:
```bash
# Re-initialize Cassandra schema
sudo docker exec -i cassandra cqlsh < ~/sangria/schema/cassandra/atomix/keyspace.cql
sudo docker exec -i cassandra cqlsh -k atomix < ~/sangria/schema/cassandra/atomix/schema.cql
```

### If experiment hangs:
- Check Ray dashboard for errors
- Look at `/tmp/ray/session_*/logs/` for detailed logs
- Verify all servers started: `ps aux | grep -E 'universe|warden|rangeserver|resolver|frontend'`

## Next Steps

1. **Run the experiment** to collect baseline data
2. **Analyze results** to understand cascading abort performance
3. **Compare** with traditional abort (by setting `enable_cascading_abort=false`)
4. **Tune parameters** if needed (more/fewer keys, different concurrency levels)
5. **Write test** that explicitly forces aborts to validate cascading logic

## Notes

- Cascading abort is **disabled by default** (`enable_cascading_abort=false`)
- Artificial abort rate is **0.0 by default** (no random failures)
- This experiment **explicitly enables both** to test cascading abort behavior
- Traditional 2PC does NOT support cascading abort (not tested)
- The experiment uses **artificial random failures**, not natural contention-based aborts
- Random failures are injected in `rangeserver/src/range_manager/impl.rs:prepare()`
