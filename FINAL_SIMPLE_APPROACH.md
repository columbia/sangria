# FINAL SIMPLE APPROACH: Remove Artificial Abort from Production Code

## The Problem

Artificial abort in the prepare() path creates unavoidable issues:
1. **Before state modifications**: Doesn't test cascading (no dependencies recorded)
2. **After state modifications**: Creates race conditions or deadlocks
3. **With synchronous cleanup**: Holds lock too long, causes contention

## The Solution: Separate Concerns

### 1. Remove Artificial Abort from Production Code

The artificial abort code in `rangeserver/src/range_manager/impl.rs` should be **completely removed**.

Why:
- It's test code mixed with production code
- It creates complexity and bugs
- It can't properly test cascading without causing problems

### 2. Test Regular Abort Handling Separately

**Experiment**: Test throughput under natural contention
- Use varying `num_keys` to create natural contention
- Lower num_keys = more contention = more natural aborts
- No artificial abort needed

**Config**:
```python
NUM_KEYS = [10, 20, 50, 100, 200]  # Lower = more contention/aborts
enable_cascading_abort = True
```

### 3. Test Cascading Abort Logic with Unit Test

Create a deterministic unit test (not Ray experiment):

```rust
#[tokio::test]
async fn test_cascading_abort_deterministic() {
    // Setup: Real rangeserver, coordinator, cassandra

    // T1 prepares on key "user:123"
    let t1_result = coordinator.prepare(t1, vec![("user:123", "value")]).await;
    assert!(t1_result.is_ok());

    // T2 prepares on same key (creates dependency on T1)
    let t2_result = coordinator.prepare(t2, vec![("user:123", "value2")]).await;
    assert!(t2_result.is_ok());
    assert!(t2_result.dependencies.contains(&t1));

    // Manually abort T1
    coordinator.abort(t1).await;

    // Verify: T2 was cascading aborted
    let t2_status = coordinator.get_transaction_status(t2).await;
    assert_eq!(t2_status, TransactionStatus::Aborted);
    assert_eq!(t2_abort_reason, CascadingAbort);
}
```

This:
- ✅ Tests actual cascading logic
- ✅ No race conditions (controlled timing)
- ✅ No artificial abort complexity
- ✅ Deterministic

## Recommended Next Steps

1. **Remove all artificial abort code** from rangeserver
2. **Run contention-based experiment** (varying num_keys)
3. **Write deterministic cascading unit test** (separate PR)

This separates concerns and avoids mixing test infrastructure with production code.
