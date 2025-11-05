# Cascading Abort Experiment

## Overview
This experiment tests the performance of cascading abort functionality by varying contention levels in Pipelined and Adaptive commit strategies.

## What Was Added

### 1. New Experiment Function (`run_experiments.py`)

**Location:** `workload-generator/scripts/run_experiments.py`

**Function:** `cascading_abort_experiment(ray_logs_dir)`

**What it does:**
- Tests **Pipelined** and **Adaptive** strategies only (Traditional doesn't support cascading abort)
- Varies contention to create different abort rates:
  - **Low contention**: 100 keys, 10 concurrent txns
  - **Medium-low**: 50 keys, 25 concurrent txns
  - **Medium**: 30 keys, 50 concurrent txns
  - **Medium-high**: 20 keys, 75 concurrent txns
  - **High contention**: 10 keys, 100 concurrent txns
- Enables cascading abort: `enable_cascading_abort=True`
- Runs 3000 queries per configuration
- 2 iterations for statistical significance

**Key insight:** Fewer keys + more concurrency = more conflicts = more aborts = higher chance of cascading aborts

### 2. Custom Plotter (`run_experiments.py`)

**Function:** `plot_cascading_abort_results(experiment_name, ray_logs_dir)`

**Generates:**
1. **Throughput vs Contention graph** (PNG):
   - X-axis: Contention level (fewer keys = more contention)
   - Y-axis: Throughput (txn/s)
   - Shows Pipelined vs Adaptive performance

2. **Summary table** (CSV):
   - Pivot table showing throughput for each strategy at each contention level

### 3. Updated Configuration Handling (`ray_task.py`)

**Changes:**
- Extracts `enable_cascading_abort` parameter from config
- Sets it in `atomix_setup.servers_config["enable_cascading_abort"]`
- Properly cleans up the parameter after use

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
- Experiment will test 5 contention levels × 2 baselines × 2 iterations = **20 trials**
- Each trial runs 3000 transactions
- Cassandra is cleaned between trials
- Servers are restarted for each configuration

### After Completion:

**Results directory:** `workload-generator/experiments/ray_logs/<experiment_name>_cascading_abort/`

**Files generated:**
1. `<experiment_name>_Pipelined_<X>keys_<Y>conc_results.csv` - Raw results for each configuration
2. `<experiment_name>_throughput_vs_contention.png` - Performance graph
3. `<experiment_name>_summary.csv` - Summary table

### Expected Results:

**Hypothesis:**
- **Low contention**: High throughput, few aborts, minimal cascading
- **High contention**: Lower throughput, more aborts, more cascading
- **Pipelined vs Adaptive**: May show different performance characteristics under cascading abort load

## Metrics Collected

For each trial:
- **Throughput** (txn/s)
- **Baseline** (Pipelined/Adaptive)
- **num_keys** (contention parameter)
- **max_concurrency** (contention parameter)
- **zipf_exponent** (access pattern skew)
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
- This experiment **explicitly enables it** to test the feature
- Traditional 2PC does NOT support cascading abort (not tested)
- The experiment uses **natural aborts** from contention, not artificial injection
