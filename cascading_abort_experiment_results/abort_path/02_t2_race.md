# Phase 2: T2 GET(A) - Race Condition


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

