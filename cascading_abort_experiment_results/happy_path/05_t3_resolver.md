# Phase 5: T3 COMMIT via Resolver


```mermaid
graph TB
    subgraph "Coordinator"
        T3_State["<b>Transaction T3</b><br/>id: T3<br/>dependencies: [T2]<br/>state: Running"]
        Validation["<b>Validation Check</b><br/>for dep in [T2]:<br/>  is_aborting(T2)? → false ✓<br/>All clean! Send to resolver"]
        DepTracker5["<b>DependencyTracker</b><br/>aborting_txns: {}  ← T2 not aborting"]
    end

    subgraph "Resolver"
        ResolverLogic["<b>Dependency Resolution</b><br/>1. Received: commit(T3, deps:[T2])<br/>2. Wait for T2 in tx_state_store<br/>3. T2 committed ✓<br/>4. Commit T3"]
        TxStateStore["<b>tx_state_store</b><br/>T1: Committed ✓<br/>T2: Committed ✓<br/>T3: Committed ✓"]
    end

    subgraph "Range0"
        Final["<b>Final State</b><br/>All transactions committed<br/>T1 ✓ → T2 ✓ → T3 ✓"]
    end

    T3_State --> Validation
    Validation --> DepTracker5
    Validation -->|"Send to resolver"| ResolverLogic
    ResolverLogic --> TxStateStore
    TxStateStore -.->|"All deps committed"| ResolverLogic
    ResolverLogic -->|"COMMIT(T3)"| Final

    style TxStateStore fill:#d4edda
    style Final fill:#d4edda
```

**What happened:**
1. T3 tries to commit with dependencies: [T2]
2. **Validation check**: `is_aborting(T2)? → false` ✓
3. Safe to send to resolver
4. Resolver receives: `commit(T3, deps:[T2])`
5. Resolver waits for T2 in tx_state_store
6. T2 commits → T3 can now commit
7. All transactions committed successfully!

**Key Insight:** Validation prevents sending transactions with **aborted** dependencies to resolver (prevents deadlock).

---

## Summary: Happy Path Flow

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant R0 as Range0
    participant Res as Resolver
    participant Cass as Cassandra

    Note over C,Cass: T1: Write A
    C->>R0: PREPARE(T1, write A:1)
    R0->>R0: Record prepare, update tables
    R0->>R0: Release lock early
    R0-->>C: PrepareResult{deps:[]}
    C->>C: No dependencies → Direct commit
    C->>R0: COMMIT(T1)
    R0->>Cass: Write A:1

    Note over C,Cass: T2: Read A, Write B
    C->>R0: GET(A)
    R0->>R0: Check: T1 in commit_table but not in prepare_records
    R0->>Cass: Read A (T1 committed)
    R0-->>C: GetResult{val:1, deps:[]}
    C->>R0: PREPARE(T2, write B:2)
    R0-->>C: PrepareResult{deps:[]}
    C->>R0: COMMIT(T2)

    Note over C,Cass: T3: Read B (uncommitted)
    C->>R0: GET(B)
    R0->>R0: Check: T2 in prepare_records (uncommitted!)
    R0-->>C: GetResult{val:2, deps:[T2]}
    C->>C: Add reverse_dep: T2→T3
    C->>R0: PREPARE(T3)
    R0-->>C: PrepareResult{...}

    Note over C,Res: T3 has dependencies
    C->>C: Validate: is_aborting(T2)? → false ✓
    C->>Res: commit(T3, deps:[T2])
    Res->>Res: Wait for T2 to commit
    Note over Res: T2 commits
    Res->>C: T3 can commit
    C->>R0: COMMIT(T3)
```

**Result:** T1 ✓ → T2 ✓ → T3 ✓ (All committed successfully)
