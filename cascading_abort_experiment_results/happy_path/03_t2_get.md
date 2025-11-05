# Phase 3: T2 GET(A) - No Dependency Created


```mermaid
graph TB
    subgraph "Coordinator"
        T2_State["<b>Transaction T2</b><br/>id: T2<br/>dependencies: []  ← empty!<br/>state: Running"]
        DepTracker3["<b>DependencyTracker</b><br/>reverse_deps: {}<br/>aborting_txns: {}"]
    end

    subgraph "Range0"
        PrepRec3["<b>pending_prepare_records</b><br/>{}  ← T1 gone"]
        CommitTbl3["<b>pending_commit_table</b><br/>{}  ← A removed when T1 committed"]
        Check["<b>GET(A) Processing</b><br/>1. Check commit_table[A] → None<br/>2. No uncommitted dependency!<br/>3. Read from Cassandra"]
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
2. Range0 checks: `pending_commit_table[A]` → **None** (line 205)
3. A is not in pending_commit_table → no uncommitted writer exists
4. Read from Cassandra (committed data)
5. Return: `GetResult{val: 1, dependencies: []}`

**Key Insight:** Since T1 committed, A was **removed from pending_commit_table** (line 693). This means T2 finds no uncommitted dependency and reads directly from Cassandra. No entry created in reverse_deps.

---

