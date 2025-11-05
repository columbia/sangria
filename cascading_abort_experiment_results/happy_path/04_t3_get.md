# Phase 4: T2 PREPARE & T3 GET(B) - Dependency Created


```mermaid
graph TB
    subgraph "Coordinator"
        T2_State["<b>Transaction T2</b><br/>id: T2<br/>dependencies: []<br/>state: Running"]
        T3_State["<b>Transaction T3</b><br/>id: T3<br/>dependencies: [T2]  ← NEW!<br/>state: Running"]
        DepTracker4["<b>DependencyTracker</b><br/>reverse_deps: {T2: [T3]}  ← T3 depends on T2<br/>aborting_txns: {}"]
    end

    subgraph "Range0"
        PrepRec4["<b>pending_prepare_records</b><br/>{T2: PrepareRecord{B:2}}  ← T2 uncommitted"]
        CommitTbl4["<b>pending_commit_table</b><br/>{B: T2}  ← Only uncommitted writes tracked"]
        VerChain4["<b>key_version_chain</b><br/>{A: [T1], B: [T2]}  ← Full history kept"]
        Check4["<b>T3 GET(B) Processing</b><br/>1. Check commit_table[B] → T2<br/>2. Check prepare_records[T2] → Found!<br/>3. T2 uncommitted! Read from prepare"]
    end

    T2_State -->|"PREPARE(T2)"| PrepRec4
    T3_State -->|"GET(B)"| Check4
    Check4 --> PrepRec4
    PrepRec4 -.->|"GetResult{val:2, deps:[T2]}"| T3_State
    T3_State -->|"add_reverse_dep(T2,T3)"| DepTracker4

    style PrepRec4 fill:#fff3cd
    style DepTracker4 fill:#fff3cd
```

**What happened:**
1. T2 prepares (writes B), releases lock early
2. T3 does GET(B)
3. Range0 finds: `pending_commit_table[B] = T2`
4. Range0 finds: `pending_prepare_records[T2]` → **Exists!** (T2 uncommitted)
5. Read from T2's prepare record (dirty read)
6. Return: `GetResult{val: 2, dependencies: [T2]}`
7. Coordinator adds reverse dependency: `T2 → T3`

**Key Insight:** T3 **depends on uncommitted T2**. Must wait for T2 via resolver.

---

