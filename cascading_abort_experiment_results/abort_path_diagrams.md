# Cascading Abort Path: Pipelined 2PC Data Structure Evolution

## Scenario
- **T1**: Writes key A (value 0→1) → **🎲 ARTIFICIAL ABORT** ✗
- **T2**: Reads A (from aborting T1) → **CASCADE ABORT** ✗
- **T3**: Reads B (from T2) → **CASCADE ABORT** ✗

---

## Phase 1: T1 PREPARE with Artificial Abort

```mermaid
graph TB
    subgraph "Coordinator - AFTER ABORT"
        T1_State["<b>Transaction T1</b><br/>id: T1<br/>dependencies: []<br/>state: Aborted ✗"]
        AbortCall["<b>record_abort() called</b><br/>1. mark_aborting([T1])<br/>2. tx_state_store.abort(T1)<br/>3. Send ABORT to ranges"]
        DepTracker1["<b>DependencyTracker</b><br/>reverse_deps: {}<br/>aborting_txns: {T1}  ← PERMANENT!<br/>participant_ranges: {}"]
    end

    subgraph "Range0 - AFTER CLEANUP"
        PrepRec1["<b>pending_prepare_records</b><br/>{}  ← T1 removed"]
        CommitTbl1["<b>pending_commit_table</b><br/>{}  ← A removed"]
        VerChain1["<b>key_version_chain</b><br/>{}  ← A removed"]
        Cleanup["<b>Synchronous Cleanup</b><br/>🎲 Artificial abort triggered!<br/>1. Remove from prepare_records<br/>2. Remove from version_chain<br/>3. Remove from commit_table<br/>4. Release lock<br/>5. Return Err(TransactionAborted)"]
    end

    T1_State --> AbortCall
    AbortCall -->|"mark_aborting"| DepTracker1
    Cleanup --> PrepRec1
    Cleanup --> CommitTbl1
    Cleanup --> VerChain1

    style T1_State fill:#f8d7da
    style DepTracker1 fill:#f8d7da
    style Cleanup fill:#fff3cd
```

**What happened:**
1. T1 reaches PREPARE phase in Range0
2. **Artificial abort triggered** (random < abort_rate)
3. **Synchronous cleanup** (while holding lock):
   - Remove T1 from `pending_prepare_records`
   - Remove T1 from `key_version_chain[A]`
   - Remove A from `pending_commit_table`
4. Release lock
5. Return `Err(TransactionAborted)` to coordinator
6. Coordinator calls `record_abort()`:
   - **CRITICAL**: `mark_aborting([T1])`
   - T1 added to `aborting_transactions` **PERMANENTLY**
   - Send ABORT to all ranges

**Key Insight:** T1 marked in `aborting_transactions` forever. Any future `is_aborting(T1)` returns `true`.

---

## Phase 2: T2 GET(A) - Race Condition

```mermaid
graph TB
    subgraph "Coordinator"
        T2_State["<b>Transaction T2</b><br/>id: T2<br/>dependencies: [T1]  ← DANGEROUS!<br/>state: Running"]
        DepTracker2["<b>DependencyTracker</b><br/>reverse_deps: {T1: [T2]}  ← T2 depends on T1<br/>aborting_txns: {T1}  ← T1 is aborting!"]
    end

    subgraph "Range0 - RACE WINDOW"
        RaceNote["<b>⚠️ RACE CONDITION</b><br/>T2's GET happens BEFORE<br/>T1's cleanup completes"]
        PrepRec2["<b>pending_prepare_records</b><br/>{T1: PrepareRecord{A:1}}  ← still here!"]
        CommitTbl2["<b>pending_commit_table</b><br/>{A: T1}  ← still here!"]
        Check2["<b>GET(A) Processing</b><br/>1. Check commit_table[A] → T1<br/>2. Check prepare_records[T1] → Found!<br/>3. T1 uncommitted! Read from prepare"]
    end

    T2_State -->|"GET(A)"| Check2
    Check2 --> CommitTbl2
    Check2 --> PrepRec2
    PrepRec2 -.->|"GetResult{val:1, deps:[T1]}"| T2_State
    T2_State -->|"add_reverse_dep(T1,T2)"| DepTracker2
    RaceNote -.-> PrepRec2

    style RaceNote fill:#fff3cd
    style DepTracker2 fill:#f8d7da
```

**What happened (race condition):**
1. T2 does GET(A) **before** T1's cleanup completes
2. Range0 finds: `pending_commit_table[A] = T1` ✓
3. Range0 finds: `pending_prepare_records[T1]` → **Still exists!**
4. Read T1's uncommitted value
5. Return: `GetResult{val: 1, dependencies: [T1]}`
6. Coordinator adds: `reverse_deps[T1] = [T2]`

**Dangerous state:**
- T2 depends on T1
- But T1 is in `aborting_transactions`!
- If T2 sent to resolver → **DEADLOCK** (waits for T1 to commit, which will never happen)

**Key Insight:** This race is why we need dependency validation before sending to resolver!

---

## Phase 3: T2 COMMIT - Validation Prevents Deadlock

```mermaid
graph TB
    subgraph "Coordinator - VALIDATION"
        T2_State["<b>Transaction T2</b><br/>id: T2<br/>dependencies: [T1]<br/>state: Running"]
        Validation["<b>🔍 Dependency Validation</b><br/>for dep in [T1]:<br/>  is_aborting(T1)?<br/>  → Check aborting_txns<br/>  → TRUE! ✗<br/><br/>🛑 BLOCKED from resolver!<br/>Call cascade_abort(T2)"]
        DepTracker3["<b>DependencyTracker</b><br/>aborting_txns: {T1}  ← T1 is aborting!"]
        CascadeCall["<b>cascade_abort(T2)</b><br/>1. Find dependents: {T3}<br/>2. to_abort = {T2, T3}<br/>3. mark_aborting([T2, T3])"]
    end

    subgraph "Resolver - NOT REACHED"
        ResolverBlock["<b>❌ DEADLOCK PREVENTED</b><br/><br/>If T2 was sent here:<br/>  wait_for(T1)<br/>  → DEADLOCK!<br/>  (T1 never commits)<br/><br/>✓ Validation caught it!"]
    end

    T2_State --> Validation
    Validation --> DepTracker3
    DepTracker3 -.->|"is_aborting(T1) = true"| Validation
    Validation -->|"BLOCKED"| ResolverBlock
    Validation --> CascadeCall

    style Validation fill:#fff3cd
    style ResolverBlock fill:#f8d7da
    style CascadeCall fill:#f8d7da
```

**What happened:**
1. T2 completes PREPARE, tries to commit
2. T2 has `dependencies = [T1]` → would normally go to resolver
3. **VALIDATION CHECK** (coordinator/src/transaction.rs:597-617):
   ```rust
   for dep in dependencies {  // [T1]
       if is_aborting(dep) {  // is_aborting(T1)?
           // Check aborting_transactions set
           // → TRUE! (T1 was marked in Phase 1)
           cascade_abort(T2);
           return Err(CascadingAbort);
       }
   }
   ```
4. **T2 BLOCKED** from resolver!
5. Call `cascade_abort(T2)`

**Critical insight:**
- **Without this check**: T2 sent to resolver
- Resolver calls `wait_for(T1)` → **DEADLOCK** (T1 never commits)
- **With this check**: T2 caught early, cascade abort triggered

**This is the key fix that prevents resolver deadlock!**

---

## Phase 4: Cascade Abort Propagation (T2 → T3)

```mermaid
graph TB
    subgraph "Coordinator - CASCADE ALGORITHM"
        CascadeSteps["<b>cascade_abort(T2) Steps</b><br/><br/>1. Find all dependents (transitive):<br/>   reverse_deps[T2] = [T3]<br/>   all_dependents = {T3}<br/><br/>2. Build abort set:<br/>   to_abort = {T2, T3}<br/><br/>3. Mark ALL as aborting:<br/>   mark_aborting([T2, T3])<br/><br/>4. Abort in leaf order:<br/>   - Leaves: [T3] (no dependents)<br/>   - Abort T3 → ranges, tx_state_store<br/>   - Next: [T2]<br/>   - Abort T2 → ranges, tx_state_store"]

        DepTrackerFinal["<b>DependencyTracker (Final)</b><br/>aborting_txns: {T1, T2, T3}  ← ALL permanent!<br/>reverse_deps: {}  ← cleaned up<br/>participant_ranges: {}  ← cleaned up"]
    end

    subgraph "Range0 - CLEANUP"
        AbortMsgs["<b>Abort Messages Received</b><br/><br/>ABORT(T3):<br/>  ✓ Remove from prepare_records<br/>  ✓ Clean version_chain<br/>  ✓ Clean commit_table<br/><br/>ABORT(T2):<br/>  ✓ Remove from prepare_records<br/>  ✓ Clean version_chain<br/>  ✓ Clean commit_table"]

        FinalState["<b>Final State</b><br/>All aborted transactions cleaned<br/>T1 ✗ → T2 ✗ → T3 ✗"]
    end

    CascadeSteps --> DepTrackerFinal
    CascadeSteps -->|"Send ABORT"| AbortMsgs
    AbortMsgs --> FinalState

    style DepTrackerFinal fill:#f8d7da
    style FinalState fill:#f8d7da
```

**What happened (cascade_abort algorithm):**

1. **Find all dependents** (line 275-280):
   ```rust
   let all_dependents = find_all_dependents(&dependency_tracker, T2, &mut visited);
   // Searches reverse_deps transitively
   // reverse_deps[T2] = [T3]
   // Result: {T3}
   ```

2. **Build full abort set** (line 279-280):
   ```rust
   let mut to_abort = all_dependents;  // {T3}
   to_abort.insert(T2);                // {T2, T3}
   ```

3. **Mark all as aborting** (line 283-284):
   ```rust
   dependency_tracker.mark_aborting(&[T2, T3]).await;
   // aborting_transactions: {T1, T2, T3}  ← ALL permanent!
   ```

4. **Abort in leaf-to-root order** (line 286-351):
   ```rust
   while !to_abort.is_empty() {
       // Find leaves: transactions with no dependents in to_abort set
       let leaves = find_leaves(&to_abort);  // [T3]

       for tx_id in leaves {
           // Send ABORT to all ranges
           for range_id in participant_ranges[tx_id] {
               range_client.abort_transaction(tx_id, range_id).await;
           }

           // Abort in tx_state_store
           tx_state_store.try_abort_transaction(tx_id).await;

           // Clean up dependency tracker
           dependency_tracker.remove_reverse_deps(tx_id).await;
           dependency_tracker.remove_participant_ranges(tx_id).await;

           // NOTE: We do NOT unmark_aborting(tx_id)!
           // Transaction stays in aborting set FOREVER

           to_abort.remove(tx_id);
       }
       // Next iteration: to_abort = {T2}
       // T2 is now a leaf → abort T2...
   }
   ```

**Final state:**
```
DependencyTracker:
  aborting_transactions: {T1, T2, T3}  ← PERMANENT markers
  reverse_deps: {}                      ← All cleaned up
  participant_ranges: {}                ← All cleaned up

Transactions:
  T1: Aborted (artificial)
  T2: Aborted (cascading)
  T3: Aborted (cascading)
```

**Key Insight:** Leaf-to-root order ensures dependencies are aborted before dependents. `aborting_transactions` set is permanent - never cleared.

---

## Summary: Cascading Abort Flow

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant R0 as Range0
    participant DT as DependencyTracker
    participant Res as Resolver

    Note over C,Res: T1: Write A
    C->>R0: PREPARE(T1, write A:1)
    R0->>R0: Record prepare, update tables
    R0->>R0: 🎲 Artificial abort triggered!
    R0->>R0: Synchronous cleanup (remove from all tables)
    R0-->>C: Err(TransactionAborted)
    C->>DT: mark_aborting([T1])
    Note over DT: aborting_txns: {T1} PERMANENT!

    Note over C,R0: T2: Read A (race condition)
    C->>R0: GET(A)
    R0->>R0: T1 still in prepare_records (race!)
    R0-->>C: GetResult{val:1, deps:[T1]}
    C->>DT: add_reverse_dep(T1, T2)
    Note over DT: reverse_deps: {T1: [T2]}

    Note over C,R0: T2: PREPARE & COMMIT attempt
    C->>R0: PREPARE(T2, write B:2)
    R0-->>C: PrepareResult{...}

    Note over C,Res: T2 tries to commit
    C->>C: dependencies = [T1]
    C->>DT: is_aborting(T1)?
    DT-->>C: TRUE! ✗
    C->>C: 🛑 BLOCKED from resolver
    C->>C: cascade_abort(T2)

    Note over C: Find dependents: {T3}
    Note over C: to_abort = {T2, T3}
    C->>DT: mark_aborting([T2, T3])
    Note over DT: aborting_txns: {T1, T2, T3}

    Note over C: Abort in leaf order
    C->>R0: ABORT(T3)
    C->>R0: ABORT(T2)

    Note over C,R0: Result: All aborted
    Note over DT: aborting_txns: {T1, T2, T3}<br/>PERMANENT!
```

**Result:** T1 ✗ (artificial) → T2 ✗ (cascading) → T3 ✗ (cascading)

**Key Protection:** Resolver never receives T2 → no deadlock!

---

## Critical Implementation Details

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
