# Phase 3: T2 GET(A) - No Dependency Created


```mermaid
graph TB
    subgraph "Coordinator"
        T2_State["<b>Transaction T2</b><br/>id: T2<br/>dependencies: []  ← empty!<br/>state: Running"]
        DepTracker3["<b>DependencyTracker</b><br/>reverse_deps: {}<br/>aborting_txns: {}"]
    end

    subgraph "Range0"
        PrepRec3["<b>pending_prepare_records</b><br/>{}  ← T1 gone"]
        CommitTbl3["<b>pending_commit_table</b><br/>{A: T1}"]
        Check["<b>GET(A) Processing</b><br/>1. Check commit_table[A] → T1<br/>2. Check prepare_records[T1] → None<br/>3. T1 committed! Read from Cassandra"]
    end

    subgraph "Cassandra"
        Data["<b>Storage</b><br/>{A: 1}"]
    end

    T2_State -->|"GET(A)"| CommitTbl3
    CommitTbl3 --> Check
    Check --> PrepRec3
    PrepRec3 -.->|"Not found"| Check
    Check --> Data
    Data -.->|"GetResult{val:1, deps:[]}"| T2_State

    style Check fill:#fff3cd
```

**What happened:**
1. T2 does GET(A)
2. Range0 checks: `pending_commit_table[A] = T1` ✓
3. Range0 checks: `pending_prepare_records[T1]` → **None** (T1 committed!)
4. Since T1 committed, read from Cassandra instead
5. Return: `GetResult{val: 1, dependencies: []}`

**Key Insight:** T2 has **NO dependency** on T1 because T1 already committed. No entry in reverse_deps.

---

