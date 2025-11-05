# Happy Path Overview

## Scenario
**Transaction Chain:**
- **T1**: Writes key A (value 0→1) ✓
- **T2**: Reads A (from committed T1), writes B ✓  
- **T3**: Reads B (from uncommitted T2), waits via resolver ✓

**Result:** All transactions commit successfully

## Files in Order
1. `01_t1_prepare.md` - T1 PREPARE with early lock release
2. `02_t1_commit.md` - T1 direct commit (no dependencies)
3. `03_t2_get.md` - T2 reads A (T1 already committed)
4. `04_t3_get.md` - T3 reads B (T2 still uncommitted)  
5. `05_t3_resolver.md` - T3 waits via resolver for T2
6. `06_summary_sequence.md` - Complete sequence diagram

## Key Concepts
- **Pipelined 2PC**: Locks released after PREPARE, before COMMIT
- **Dependency Tracking**: `pending_commit_table`, `key_version_chain`
- **Resolver**: Waits for dependencies before committing
