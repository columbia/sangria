# Critical Implementation Details


### 1. Why `aborting_transactions` is Permanent

**Problem (TOCTOU race):**
```
1. T1 aborts, cleanup completes
2. If we called unmark_aborting(T1)  ← BAD!
3. T2 reads A (race: depends on T1)
4. T2 checks: is_aborting(T1)? → FALSE (was unmarked!)
5. T2 sent to resolver
6. Resolver waits for T1 → DEADLOCK!
```

**Solution:**
```rust
// Never call unmark_aborting()
// Once marked, stays marked FOREVER
```

**Benefit:** Any future check always returns `true`, preventing all TOCTOU races.

---

### 2. Why Both `record_abort()` and `cascade_abort()` Need `mark_aborting()`

**Two abort paths based on `any_early_lock_releases` flag:**

| Path | When | Example |
|------|------|---------|
| `record_abort()` | Abort **before** any early lock release | T1 aborts on first PREPARE |
| `cascade_abort()` | Abort **after** early lock release | T2 with dependencies aborts |

**Bug:** Originally `record_abort()` didn't call `mark_aborting()`!
- Aborts on first PREPARE weren't tracked
- Dependent transactions couldn't detect them

**Fix:** Both paths now call `mark_aborting()` → complete tracking.

---

### 3. Synchronous Cleanup in RangeServer

```rust
// In rangeserver PREPARE phase:
if should_artificially_abort {
    // BEFORE releasing lock:
    {
        let mut pending_state = state.pending_state.write().await;

        // Remove from all tracking structures
        pending_state.pending_prepare_records.remove(&tx.id);

        for key in prepare_record.changes.keys() {
            if let Some(chain) = pending_state.key_version_chain.get_mut(key) {
                chain.retain(|&id| id != tx.id);

                if let Some(&prev_tx) = chain.last() {
                    pending_state.pending_commit_table.insert(key.clone(), prev_tx);
                } else {
                    pending_state.pending_commit_table.remove(key);
                    pending_state.key_version_chain.remove(key);
                }
            }
        }
    }  // Lock released here

    // AFTER cleanup complete:
    state.lock_table.release(None).await;
    return Err(Error::TransactionAborted);
}
```

**Benefit:** Minimizes race window where other transactions could see stale state.
