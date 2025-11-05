# Simple Cascading Abort Testing (No Race Conditions)

## The Solution: Synchronous Cleanup

### Key Insight
Clean up **synchronously** before releasing the lock. This prevents any race condition.

### Implementation

```rust
// Line 406 in rangeserver/src/range_manager/impl.rs

// 1. Dependencies have been recorded
info!("Dependencies for transaction {:?}: {:?}", tx.id, dependencies);

// 2. Check for artificial abort
if should_artificially_abort {
    // 3. SYNCHRONOUS CLEANUP (while still holding lock)
    let mut pending_state = state.pending_state.write().await;

    // Remove from pending_prepare_records
    pending_state.pending_prepare_records.remove(&tx.id);

    // Remove from key_version_chain
    for key in prepare_record.changes.keys() {
        if let Some(chain) = pending_state.key_version_chain.get_mut(&key) {
            chain.retain(|&id| id != tx.id);
            // Update pending_commit_table to previous version
            if let Some(&prev_tx) = chain.last() {
                pending_state.pending_commit_table.insert(key.clone(), prev_tx);
            } else {
                pending_state.pending_commit_table.remove(&key);
            }
        }
    }

    drop(pending_state);

    // 4. NOW release lock (state is already clean)
    state.lock_table.release(None).await;

    return Err(TransactionAborted);
}
```

### Why This Works

**Timeline (No Race):**
```
T2: Records in key_version_chain          ✅
T2: Artificially aborts                   ✅
T2: Cleans up (removes from chain)        ✅
T2: Releases lock                          ✅
------- Lock is released -------
T3: Acquires lock                          ✅
T3: Sees clean state (no zombie T2)       ✅
T3: Prepares successfully                 ✅
```

**Key point:** Cleanup happens BEFORE lock release, so T3 can never see zombie T2.

### What This Tests

✅ **Cascading aborts with dependencies**
   - Dependencies ARE recorded in key_version_chain
   - If T2 depends on T1, and T1 aborts, T2 cascades

✅ **No race conditions**
   - Cleanup is synchronous (before lock release)
   - No zombie dependencies possible

✅ **No hangs**
   - Subsequent transactions see clean state
   - No waiting for zombies

✅ **Throughput degradation**
   - Measures impact of abort rate on performance

### Running the Experiment

```bash
# On remote server
cd ~/sangria
git pull
cargo build --release

cd workload-generator/scripts
python run_experiments.py
```

**Expected behavior:**
- Completes without hangs (~5-10 minutes total)
- Throughput decreases as abort rate increases
- Some transactions cascade abort (if they depend on aborted transactions)
- Logs show: "🎲 Artificial abort triggered" and cleanup messages

### Verification

Check logs for successful cleanup:
```bash
tail -f /tmp/ray/session_*/logs/worker-*.out | grep -A 5 "Artificial abort"
```

Should see:
```
🎲 Artificial abort triggered for transaction <uuid>
  - Removed from pending_prepare_records
  - Removed from key_version_chain for key <key>
  - Cleanup complete, releasing lock
```

### Unit Test

Run the unit test to verify the approach:
```bash
cargo test --test artificial_abort_integration_test test_synchronous_cleanup -- --nocapture
```

This test documents the expected flow with detailed logging.
