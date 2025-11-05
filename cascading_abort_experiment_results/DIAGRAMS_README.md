# Cascading Abort Diagrams - Walkthrough Guide

Visual explanation of Pipelined 2PC with cascading aborts using Mermaid diagrams.

## Structure

Two diagram sets showing data structure evolution at each phase:

### 📁 `happy_path/` - All Transactions Commit
Walk through in order:
1. `00_overview.md` - Scenario overview
2. `01_t1_prepare.md` - T1 PREPARE with early lock release
3. `02_t1_commit.md` - T1 direct commit (no dependencies)
4. `03_t2_get.md` - T2 reads A (T1 already committed → no dependency)
5. `04_t3_get.md` - T3 reads B (T2 uncommitted → dependency created)
6. `05_t3_resolver.md` - T3 validation passes, uses resolver
7. `06_summary_sequence.md` - Complete sequence diagram

**Result:** T1 ✓ → T2 ✓ → T3 ✓

### 📁 `abort_path/` - Cascading Abort Chain
Walk through in order:
1. `00_overview.md` - Scenario overview
2. `01_t1_abort.md` - T1 artificial abort & cleanup
3. `02_t2_race.md` - T2 race condition (depends on aborting T1)
4. `03_t2_validation.md` - **Validation prevents resolver deadlock**
5. `04_cascade.md` - Cascade abort propagation (T2→T3)
6. `05_summary_sequence.md` - Complete sequence diagram
7. `06_implementation_details.md` - Critical fixes explained
8. `07_key_version_chain_cleanup.md` - **Advanced: Multi-writer cleanup mechanism**

**Result:** T1 ✗ (artificial) → T2 ✗ (cascading) → T3 ✗ (cascading)

**Note:** File #8 covers an advanced scenario (multiple transactions writing to the same key) that wasn't shown in the main flow diagrams, which use simple non-overlapping writes (T1→A, T2→B, T3→C) for clarity.

## How to View

### GitHub / VS Code
Mermaid diagrams render automatically in:
- GitHub (view .md files directly)
- VS Code with Mermaid extension
- Many markdown previewer tools

### Command Line
Install mermaid-cli to convert to images:
```bash
npm install -g @mermaid-js/mermaid-cli
mmdc -i happy_path/01_t1_prepare.md -o t1_prepare.png
```

### Online
Paste diagram code into: https://mermaid.live/

## Components Shown

Each phase diagram shows:

### Coordinator
- **Transaction state**: id, dependencies, state
- **DependencyTracker**: reverse_deps, aborting_transactions

### RangeServer (Range0)
- **pending_prepare_records**: Uncommitted writes
- **pending_commit_table**: Key → last writer mapping
- **key_version_chain**: Transaction history per key

### Resolver
- Dependency resolution logic
- Wait behavior

## Key Concepts Illustrated

### Pipelined 2PC
- Locks released after PREPARE (before COMMIT)
- Enables concurrency but creates uncommitted dependencies
- Requires tracking structures

### Dependency Tracking
- `pending_commit_table[key] = last_uncommitted_writer_txn`
- Used during GET to find dependencies
- If writer still in `pending_prepare_records` → dependency created
- Removed when transaction commits (line 693)

### Cleanup with key_version_chain
- `key_version_chain[key] = [tx1, tx2, tx3, ...]` ordered list of all writers
- When transaction aborts, revert `pending_commit_table` to previous uncommitted writer
- Critical for multi-writer scenarios (see `abort_path/07_key_version_chain_cleanup.md`)
- Example: T1, T2, T3 write A → T3 aborts → revert to T2

### Cascading Abort Protection
**The critical fix:**
```rust
// Before sending to resolver:
for dep in dependencies {
    if is_aborting(dep) {  // Check permanent aborting set
        cascade_abort(self);
        return Err(CascadingAbort);
    }
}
```

**Without this:** Resolver would deadlock waiting for aborted dependencies.

## Implementation Details

See `abort_path/06_implementation_details.md` for:
- Why `aborting_transactions` is permanent (TOCTOU prevention)
- Why both abort paths need `mark_aborting()`
- Synchronous cleanup in rangeserver
- Leaf-to-root cascade order

## Code References

| Concept | File | Lines |
|---------|------|-------|
| Artificial abort injection | `rangeserver/src/range_manager/impl.rs` | 412-463 |
| Dependency validation | `coordinator/src/transaction.rs` | 594-618 |
| mark_aborting fix | `coordinator/src/transaction.rs` | 227 |
| cascade_abort algorithm | `coordinator/src/transaction.rs` | 273-373 |
| DependencyTracker | `coordinator/src/coordinator.rs` | 30-95 |

## Quick Comparison

| Aspect | Happy Path | Abort Path |
|--------|------------|------------|
| **T1** | Commits normally | Artificial abort during PREPARE |
| **T2** | Reads committed T1 | Reads aborting T1 (race) |
| **T2 dependencies** | [] (clean) | [T1] (dangerous) |
| **Validation** | Passes | **Blocks T2 from resolver** |
| **T3** | Waits via resolver | Cascade aborted |
| **Result** | All commit ✓ | All abort ✗ |

The validation check is the **key protection** preventing resolver deadlock!
