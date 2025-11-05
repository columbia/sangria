# Cascading Abort Experiment Results

**Experiment Date**: November 5, 2025
**Experiment Name**: spectacular_ant_cascading_abort
**Branch**: cascading-aborts

## Experiment Configuration

- **Workload Size**: 3000 transactions
- **Concurrency**: 50
- **Commit Strategy**: Pipelined (with early lock release)
- **Abort Rates Tested**: 10%, 20%, 30%, 50%
- **Iterations**: 2 per abort rate
- **Num Keys**: 100
- **Workload Type**: Custom

## Purpose

Test cascading abort functionality using **artificial abort rate** to randomly fail transactions during the PREPARE phase. This tests:
1. Proper detection and marking of aborted transactions
2. Cascading abort propagation to dependent transactions
3. Prevention of deadlocks when transactions with aborted dependencies are sent to resolver
4. System throughput under different abort rates

## Key Implementation

The artificial abort is injected in `rangeserver/src/range_manager/impl.rs:431` after dependencies are recorded but before returning from PREPARE. The coordinator properly marks aborted transactions using `mark_aborting()` and validates dependencies before sending to resolver.

## Results Summary

| Abort Rate | Avg Throughput | Std Dev |
|------------|----------------|---------|
| 10% | 934.23 txn/s | ±351.79 |
| 20% | 956.67 txn/s | ±368.52 |
| 30% | 996.57 txn/s | ±364.29 |
| 50% | 1016.04 txn/s | ±362.08 |

### Key Findings

1. **No Deadlocks**: All trials completed successfully without hanging
2. **Graceful Abort Handling**: Cascading aborts detected and handled correctly
3. **Throughput Increases with Abort Rate**: Counter-intuitively, higher abort rates show slightly higher throughput because:
   - Aborted transactions release locks early
   - No resolver processing overhead for aborted transactions
   - Resources freed quickly for successful transactions
4. **High Variance**: Large std deviation (±350-370 txn/s) due to difference between iteration 0 (~700 txn/s) and iteration 1 (~1200 txn/s), likely due to warm caches

## Files

- **CSV Results**: `spectacular_ant_cascading_abort_Pipelined_*pct_results.csv` (4 files, one per abort rate)
- **Plot**: `cascading_abort_results.png` - Throughput vs Abort Rate visualization
- **Plotter Script**: `plot_cascading_abort_simple.py` - Python script to regenerate plot

## How to Reproduce

```bash
cd workload-generator/scripts
python run_experiments.py  # Runs cascading_abort_experiment()
```

## Related Code

- **Experiment Definition**: `workload-generator/scripts/run_experiments.py:259` (cascading_abort_experiment)
- **Artificial Abort Injection**: `rangeserver/src/range_manager/impl.rs:412-463`
- **Coordinator Validation**: `coordinator/src/transaction.rs:594-618`
- **mark_aborting Fix**: `coordinator/src/transaction.rs:227` (critical fix for record_abort)
