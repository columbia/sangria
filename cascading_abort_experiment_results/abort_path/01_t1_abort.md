# Phase 1: T1 PREPARE with Artificial Abort


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

