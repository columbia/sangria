# Phase 1: T1 PREPARE


```mermaid
graph TB
    subgraph "Coordinator"
        T1_State["<b>Transaction T1</b><br/>id: T1<br/>dependencies: []<br/>state: Running"]
        DepTracker1["<b>DependencyTracker</b><br/>reverse_deps: {}<br/>aborting_txns: {}<br/>participant_ranges: {T1: [Range0]}"]
    end

    subgraph "Range0"
        PrepRec1["<b>pending_prepare_records</b><br/>{T1: PrepareRecord{A:1}}"]
        CommitTbl1["<b>pending_commit_table</b><br/>{A: T1}"]
        VerChain1["<b>key_version_chain</b><br/>{A: [T1]}"]
    end

    T1_State -->|"PREPARE(T1)"| PrepRec1
    PrepRec1 -.->|"PrepareResult{deps:[], early_release:true}"| T1_State

    style PrepRec1 fill:#d4edda
    style CommitTbl1 fill:#d4edda
    style VerChain1 fill:#d4edda
```

**What happened:**
1. Coordinator sends PREPARE(T1) to Range0
2. Range0 records prepare with changes {A:1}
3. Range0 updates `pending_commit_table[A] = T1`
4. Range0 adds T1 to `key_version_chain[A]`
5. **Lock released early** (pipelining!)
6. Returns PrepareResult with empty dependencies

**Key Insight:** T1 has no dependencies, will commit directly without resolver.

---

