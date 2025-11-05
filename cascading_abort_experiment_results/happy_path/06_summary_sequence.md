# Summary: Happy Path Flow

Complete sequence diagram showing all message flows between components.

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

## Result
✅ **T1 ✓ → T2 ✓ → T3 ✓** (All committed successfully)

## Key Observations

1. **T1**: No dependencies → direct commit (skip resolver)
2. **T2**: Reads from committed T1 → no dependency → direct commit
3. **T3**: Reads from uncommitted T2 → has dependency → uses resolver
4. **Validation**: `is_aborting(T2)? → false` allows T3 to proceed

## Data Flow Summary

| Transaction | Dependencies | Commit Path | Reason |
|-------------|--------------|-------------|--------|
| T1 | [] | Direct | No dependencies |
| T2 | [] | Direct | T1 committed before T2's GET |
| T3 | [T2] | Resolver | T2 uncommitted during T3's GET |

The resolver successfully waits for T2 to commit before committing T3.
