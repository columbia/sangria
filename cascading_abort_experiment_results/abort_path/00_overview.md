# Cascading Abort Path Overview

## Scenario
**Transaction Chain:**
- **T1**: Writes key A → 🎲 **ARTIFICIAL ABORT** ✗
- **T2**: Reads A (from aborting T1) → **CASCADE ABORT** ✗
- **T3**: Reads B (from T2) → **CASCADE ABORT** ✗

**Result:** All transactions abort via cascading

## Files in Order
1. `01_t1_abort.md` - T1 artificial abort & cleanup
2. `02_t2_race.md` - T2 race condition (depends on aborting T1)
3. `03_t2_validation.md` - Validation prevents resolver deadlock
4. `04_cascade.md` - Cascade abort propagation (T2→T3)
5. `05_summary_sequence.md` - Complete sequence diagram
6. `06_implementation_details.md` - Critical fixes explained

## Critical Fix
**Dependency Validation** before sending to resolver:
```rust
for dep in dependencies {
    if is_aborting(dep) {  // Check aborting_transactions set
        cascade_abort(self);  // Don't send to resolver!
        return Err(CascadingAbort);
    }
}
```

**Without this:** Resolver would deadlock waiting for aborted dependencies.
