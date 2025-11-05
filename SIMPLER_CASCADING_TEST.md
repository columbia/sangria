# Simpler Cascading Abort Test (No Race Conditions)

## Problem with Current Approach

**Artificial abort injected AFTER dependencies recorded** creates a race:
- T2 records itself in key_version_chain
- T2 artificially aborts and releases lock
- T3 can acquire lock BEFORE T2's cleanup completes
- T3 sees zombie T2 in key_version_chain
- **DEADLOCK**: T3 waits for T2 that will never commit

## Proposed Solutions

### Option 1: Move Artificial Abort Earlier (NO DEPENDENCIES)

**Change**: Inject artificial abort at line 327 (BEFORE any state modifications)

**Pro**:
- No race condition
- No dangling state
- No deadlocks

**Con**:
- **Doesn't test cascading aborts** (no dependencies recorded)
- Only tests regular abort handling

**When to use**: Test throughput degradation under high abort rates WITHOUT cascading

### Option 2: Controlled Cascading Test (NO ARTIFICIAL ABORT)

**Approach**: Don't use artificial abort at all. Instead:

1. T1 prepares successfully (no abort)
2. T2 prepares with dependency on T1 (no abort)
3. **Manually abort T1** using coordinator.abort()
4. Verify T2 cascades

**Pro**:
- No race conditions (controlled timing)
- Actually tests cascading logic
- Deterministic

**Con**:
- Requires manual orchestration (not a simple config change)
- Can't use with existing Ray framework easily

### Option 3: Synchronization Window

**Change**: Add a "cleanup window" where new transactions can't acquire lock

**Implementation**:
```rust
// After artificial abort, hold lock briefly for cleanup
if should_artificially_abort {
    // Mark as "aborting" in lock table (new state)
    state.lock_table.mark_aborting(tx.id).await;

    // Send cleanup immediately
    send_abort_to_self().await;

    // Wait for cleanup confirmation
    wait_for_cleanup().await;

    // Now release lock
    state.lock_table.release(None).await;
    return Err(...);
}
```

**Pro**:
- Tests cascading with dependencies
- Avoids race condition

**Con**:
- Complex implementation
- Adds latency
- Changes lock semantics

## Recommendation: Use Option 1 for Now

**Immediate fix**: Move artificial abort to line 327 (before state modifications)

```rust
// At line 327 (after lock acquisition, before state changes)
self.acquire_range_lock(state, tx.clone()).await?;

// Artificial abort injection (NEW LOCATION)
if self.config.artificial_abort_rate > 0.0 {
    let random_value: f64 = {
        let mut rng = rand::thread_rng();
        rng.gen()
    };
    if random_value < self.config.artificial_abort_rate {
        state.lock_table.release(None).await;
        return Err(Error::TransactionAborted(TransactionAbortReason::Other));
    }
}

// NO state modifications above this point!
```

**What this tests**:
- ✅ System handles high abort rates without crashing
- ✅ Throughput degrades gracefully under failures
- ✅ No deadlocks or hangs
- ❌ Does NOT test cascading aborts with dependencies

**For cascading abort testing**: Use Option 2 (separate deterministic test)

## Updated Experiment Design

### Experiment 1: Artificial Abort (No Cascading)
- `artificial_abort_rate`: 0.1, 0.2, 0.3, 0.5
- `enable_cascading_abort`: false (not tested anyway)
- Inject abort at line 327
- **Tests**: Regular abort handling, throughput degradation

### Experiment 2: Cascading Abort (Deterministic)
- Manual test (not Ray experiment)
- No artificial abort
- Manually orchestrate: T1 → T2 → abort(T1) → verify T2 cascades
- **Tests**: Actual cascading abort logic

This separates concerns and avoids race conditions!
