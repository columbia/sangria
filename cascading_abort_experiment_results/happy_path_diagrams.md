# Happy Path: Pipelined 2PC Data Structure Evolution

## Scenario
- **T1**: Writes key A (value 0→1) ✓
- **T2**: Reads A (from committed T1), writes B ✓
- **T3**: Reads B (from uncommitted T2), waits via resolver ✓

---

## Phase 1: T1 PREPARE

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

## Phase 2: T1 COMMIT (Direct)

```mermaid
graph TB
    subgraph "Coordinator"
        T1_State["<b>Transaction T1</b><br/>id: T1<br/>dependencies: []<br/>state: Committed ✓"]
    end

    subgraph "Range0 - After COMMIT"
        PrepRec2["<b>pending_prepare_records</b><br/>{}  ← T1 removed"]
        CommitTbl2["<b>pending_commit_table</b><br/>{A: T1}  ← still here!"]
        VerChain2["<b>key_version_chain</b><br/>{A: [T1]}  ← still here!"]
    end

    subgraph "Cassandra"
        Stored["<b>Durable Storage</b><br/>{A: 1}  ← T1 committed"]
    end

    T1_State -->|"COMMIT(T1)"| PrepRec2
    PrepRec2 -->|"Write"| Stored

    style T1_State fill:#d4edda
    style Stored fill:#d4edda
```

**What happened:**
1. T1 has no dependencies → direct commit (skip resolver)
2. Coordinator commits T1 in tx_state_store
3. Coordinator sends COMMIT to Range0
4. Range0 removes T1 from `pending_prepare_records`
5. T1's value written to Cassandra (durable)

**Key Insight:** `pending_commit_table` and `key_version_chain` still contain T1 for tracking future dependencies.

---

## Phase 3: T2 GET(A) - No Dependency Created

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

## Phase 4: T2 PREPARE & T3 GET(B) - Dependency Created

```mermaid
graph TB
    subgraph "Coordinator"
        T2_State["<b>Transaction T2</b><br/>id: T2<br/>dependencies: []<br/>state: Running"]
        T3_State["<b>Transaction T3</b><br/>id: T3<br/>dependencies: [T2]  ← NEW!<br/>state: Running"]
        DepTracker4["<b>DependencyTracker</b><br/>reverse_deps: {T2: [T3]}  ← T3 depends on T2<br/>aborting_txns: {}"]
    end

    subgraph "Range0"
        PrepRec4["<b>pending_prepare_records</b><br/>{T2: PrepareRecord{B:2}}  ← T2 uncommitted"]
        CommitTbl4["<b>pending_commit_table</b><br/>{A: T1, B: T2}"]
        VerChain4["<b>key_version_chain</b><br/>{A: [T1], B: [T2]}"]
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

## Phase 5: T3 COMMIT via Resolver

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
