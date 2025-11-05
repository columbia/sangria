# Pipelined 2PC with Cascading Aborts - Diagram Guide

## Overview

This guide explains the visual diagrams showing how Pipelined 2PC works with cascading abort functionality. The diagrams show data structure states at each phase of the protocol.

## How to Read the Diagrams

### Component Boxes

Each diagram shows three types of components:

1. **Coordinator** (Light Blue): Transaction state and dependency tracking
2. **RangeServer** (Light Green): Pending writes and version tracking
3. **Resolver** (Light Orange): Dependency resolution and commit coordination

**Red backgrounds** indicate aborted transactions.

### Data Structures Shown

**Coordinator:**
- `Transaction`: Individual transaction state (id, dependencies, state)
- `DependencyTracker`: Global tracking (reverse_deps, aborting_transactions)

**RangeServer:**
- `pending_prepare_records`: Uncommitted writes (staged in PREPARE)
- `pending_commit_table`: Maps key → last transaction that wrote it
- `key_version_chain`: Ordered list of transactions that wrote each key

**Resolver:**
- Waits for dependencies before committing
- Not shown in detail (stateless waiting logic)

---

## Diagram Sets

### 1. Summary Comparison (`summary_comparison.png`)

**Start here!** Side-by-side comparison of:
- **Left**: Happy path where all transactions commit
- **Right**: Cascading abort path where T1 aborts → T2,T3 cascade

**Key Insights Highlighted:**
- Happy path: Dependency validation passes, resolver works correctly
- Abort path: Dependency validation BLOCKS T2 from resolver, preventing deadlock

---

## Happy Path Diagrams (5 phases)

Transaction chain: **T1** writes A → **T2** reads A, writes B → **T3** reads B

All transactions succeed.

### Phase 1: `happy_path_phase1_t1_prepare.png`
**T1 PREPARE & Lock Release**

**What happens:**
1. T1 writes key A (value 0→1)
2. Coordinator sends PREPARE to Range0
3. Range0 records prepare, updates commit table, adds to version chain
4. **Releases lock early** (pipelining!)
5. Returns PrepareResult with empty dependencies

**Key data structures:**
```
Range0:
  pending_prepare_records: {T1: {A:1}}
  pending_commit_table: {A: T1}  ← A now points to T1
  key_version_chain: {A: [T1]}   ← Version history
```

**Critical**: Lock released before commit! This enables concurrency but requires tracking.

---

### Phase 2: `happy_path_phase2_t1_commit.png`
**T1 COMMIT (Direct - No Dependencies)**

**What happens:**
1. Coordinator checks: `dependencies = []` → commit directly (skip resolver)
2. Commit in tx_state_store
3. Send COMMIT to Range0
4. Range0 removes from pending_prepare_records

**Key insight:**
- `pending_commit_table` and `key_version_chain` **remain** for T1
- Needed for cascading abort tracking even after commit!

---

### Phase 3: `happy_path_phase3_t2_get.png`
**T2 GET(A) - No Dependency (T1 committed)**

**What happens:**
1. T2 does GET(A)
2. Range0 checks: `pending_commit_table[A] = T1`
3. Range0 checks: `pending_prepare_records[T1]` → **None** (T1 committed!)
4. Read from Cassandra instead (T1's committed value)
5. Return: `GetResult {val: 1, dependencies: []}`

**Key insight:**
- T2 has **no dependency** on T1 because T1 already committed
- If T1 were still in PREPARE, T2 would depend on T1

---

### Phase 4: `happy_path_phase4_t3_get.png`
**T3 GET(B) - Dependency on Uncommitted T2**

**What happens:**
1. T3 does GET(B)
2. Range0 checks: `pending_commit_table[B] = T2`
3. Range0 checks: `pending_prepare_records[T2]` → **Found!** (T2 uncommitted)
4. Read from T2's prepare record
5. Return: `GetResult {val: 2, dependencies: [T2]}`

**Critical dependency created:**
```
Coordinator:
  T3.dependencies = [T2]

DependencyTracker:
  reverse_deps: {T2: [T3]}  ← T3 depends on T2
```

---

### Phase 5: `happy_path_phase5_t3_resolver.png`
**T3 COMMIT via Resolver (waits for T2)**

**What happens:**
1. Coordinator checks: `dependencies = [T2]` → must use resolver
2. **Validation check**: `is_aborting(T2)? → false` ✓
3. Send to resolver: `commit(T3, deps=[T2])`
4. Resolver waits for T2 to commit in tx_state_store
5. T2 commits → T3 can now commit
6. Success!

**Key insight:**
- Resolver **waits** for dependencies before committing
- This is why we must prevent sending transactions with **aborted** dependencies
- Otherwise, resolver would deadlock waiting forever

---

## Cascading Abort Diagrams (4 phases)

Transaction chain: **T1** writes A (aborts) → **T2** reads A (cascades) → **T3** reads B (cascades)

### Phase 1: `abort_path_phase1_t1_abort.png`
**T1 Artificial Abort & Cleanup**

**What happens:**
1. T1 reaches PREPARE phase
2. **Artificial abort triggered** (random < abort_rate)
3. **Synchronous cleanup** (before releasing lock):
   - Remove from `pending_prepare_records`
   - Remove from `key_version_chain`
   - Remove from `pending_commit_table`
4. Release lock
5. Return error to coordinator

**Coordinator response:**
1. Calls `record_abort()`
2. **CRITICAL FIX**: `mark_aborting([T1])`
3. T1 added to `aborting_transactions` set **permanently**

**After cleanup:**
```
Range0:
  pending_prepare_records: {}  ← T1 removed
  pending_commit_table: {}     ← A removed
  key_version_chain: {}        ← A removed

DependencyTracker:
  aborting_transactions: {T1}  ← PERMANENT!
```

---

### Phase 2: `abort_path_phase2_t2_race.png`
**T2 GET(A) - Race: Depends on Aborting T1**

**What happens (race condition):**
1. T2 does GET(A) **before T1's cleanup completes**
2. Range0 sees: `pending_commit_table[A] = T1`
3. Range0 sees: `pending_prepare_records[T1]` → **Still there!**
4. Read T1's uncommitted value
5. Return: `GetResult {val: 1, dependencies: [T1]}`

**Dangerous state created:**
```
Coordinator:
  T2.dependencies = [T1]  ← T2 depends on T1

DependencyTracker:
  aborting_transactions: {T1}  ← T1 is aborting!
  reverse_deps: {T1: [T2]}      ← T2 depends on T1
```

**Problem:** T2 depends on T1, but T1 is aborting!

---

### Phase 3: `abort_path_phase3_t2_validation.png`
**T2 COMMIT - Validation Prevents Resolver Deadlock**

**What happens:**
1. T2 completes PREPARE, tries to commit
2. T2 has `dependencies = [T1]` → would normally go to resolver
3. **VALIDATION CHECK** (coordinator/src/transaction.rs:594-618):
   ```rust
   for dep in dependencies {  // [T1]
       if is_aborting(dep) {  // is_aborting(T1)? → TRUE!
           cascade_abort(T2);
           return Err(CascadingAbort);
       }
   }
   ```
4. **T2 BLOCKED** from resolver!
5. Call `cascade_abort(T2)`

**Critical insight:**
- **Without this check**: T2 would be sent to resolver
- Resolver would `wait_for(T1)` → **DEADLOCK** (T1 never commits)
- **With this check**: T2 caught early, aborted cleanly

**Resolver (NOT REACHED):**
```
❌ T2 never sent here!

If T2 was sent:
  wait_for(T1)
  → DEADLOCK! (T1 never commits)

✓ Validation prevented deadlock
```

---

### Phase 4: `abort_path_phase4_cascade.png`
**Cascade Abort: T2→T3 Propagation**

**What happens (cascade_abort algorithm):**

1. **Find all dependents** (transitive):
   ```rust
   reverse_deps[T2] = [T3]
   to_abort = {T2, T3}
   ```

2. **Mark all as aborting** (permanent):
   ```rust
   mark_aborting([T2, T3])
   aborting_transactions: {T1, T2, T3}  ← ALL permanent
   ```

3. **Abort in leaf-to-root order**:
   - Find leaves (no dependents): T3
   - Abort T3 → send ABORT to ranges, clean up
   - Next leaf: T2
   - Abort T2 → send ABORT to ranges, clean up

**Final state:**
```
DependencyTracker:
  aborting_transactions: {T1, T2, T3}  ← PERMANENT
  reverse_deps: {}  ← Cleaned up

Range0:
  All aborted transactions removed from:
    - pending_prepare_records
    - key_version_chain
    - pending_commit_table
```

**Result:** T1 ✗ → T2 ✗ (cascade) → T3 ✗ (cascade)

---

## Key Implementation Details

### 1. Why `aborting_transactions` is Permanent

**Old (buggy) behavior:**
```rust
// At end of cascade:
unmark_aborting(T1);  // ❌ BAD
```

**Problem:** TOCTOU race
1. T1 aborts, cleans up, **unmarked**
2. T2 reads (race: depends on T1)
3. T2 checks `is_aborting(T1)` → **false** (was unmarked!)
4. T2 sent to resolver → **deadlock**

**New (correct) behavior:**
```rust
// Never call unmark_aborting
// Transactions stay in set FOREVER
```

**Benefit:** Any future check of `is_aborting(T1)` returns `true`, preventing deadlock.

---

### 2. Why Both `record_abort()` and `cascade_abort()` Need `mark_aborting()`

**Two abort paths:**

1. **`record_abort()`**: Called when abort happens **before** any early lock releases
   - Example: Artificial abort on first PREPARE
   - Condition: `any_early_lock_releases = false`

2. **`cascade_abort()`**: Called when abort happens **after** early lock releases
   - Example: Transaction with dependencies aborts
   - Condition: `any_early_lock_releases = true`

**Bug:** Originally `record_abort()` didn't call `mark_aborting()`!
- Result: Aborts on first PREPARE weren't tracked
- Dependent transactions couldn't detect them

**Fix:** Both paths now call `mark_aborting()` → all aborts tracked.

---

### 3. Why Cleanup is Synchronous in RangeServer

```rust
// BEFORE releasing lock:
{
    let mut pending_state = state.pending_state.write().await;
    pending_state.pending_prepare_records.remove(&T1);
    // ... more cleanup
}  // Lock released here

state.lock_table.release(None).await;  // AFTER cleanup
```

**Benefit:** Minimizes race window where T2 could see stale T1 dependency.

---

## Data Structure Reference

### Coordinator

```rust
// Per-transaction state
Transaction {
    id: Uuid,
    dependencies: Vec<Uuid>,      // Who I depend on
    participant_ranges: HashMap,  // Ranges I touched
    state: State,                 // Running/Committed/Aborted
}

// Global dependency tracking (shared across all transactions)
DependencyTracker {
    reverse_deps: HashMap<Uuid, HashSet<Uuid>>,    // T1 → [T2,T3] means T2,T3 depend on T1
    aborting_transactions: RwLock<HashSet<Uuid>>,  // PERMANENT set of aborted txns
    participant_ranges: HashMap<Uuid, HashSet>,    // Txn → ranges it touched
}
```

### RangeServer

```rust
// Per-range state
PendingState {
    // Uncommitted writes (removed on commit/abort)
    pending_prepare_records: HashMap<Uuid, PrepareRecord>,

    // Key → last transaction that wrote it
    // Used to find dependencies during GET
    pending_commit_table: HashMap<Key, Uuid>,

    // Key → ordered list of transactions that wrote it
    // Used for cascading abort cleanup
    key_version_chain: HashMap<Key, Vec<Uuid>>,
}
```

---

## Viewing the Diagrams

**Recommended order:**

1. **`summary_comparison.png`** - Big picture overview
2. **Happy path (all 5 phases)** - Understand normal flow
3. **Abort path (all 4 phases)** - Understand cascading abort
4. **Re-read summary** - Now it makes perfect sense!

**To regenerate diagrams:**
```bash
python3 create_2pc_diagrams.py
```

---

## Related Code Locations

- **Artificial Abort**: `rangeserver/src/range_manager/impl.rs:412-463`
- **Dependency Validation**: `coordinator/src/transaction.rs:594-618`
- **mark_aborting Fix**: `coordinator/src/transaction.rs:227`
- **cascade_abort Algorithm**: `coordinator/src/transaction.rs:273-373`
- **DependencyTracker**: `coordinator/src/coordinator.rs:30-95`
