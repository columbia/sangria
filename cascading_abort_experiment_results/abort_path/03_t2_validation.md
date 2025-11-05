# Phase 3: T2 COMMIT - Validation Prevents Deadlock


```mermaid
graph TB
    subgraph "Coordinator - VALIDATION"
        T2_State["<b>Transaction T2</b><br/>id: T2<br/>dependencies: [T1]<br/>state: Running"]
        Validation["<b>🔍 Dependency Validation</b><br/>for dep in [T1]:<br/>  is_aborting(T1)?<br/>  → Check aborting_txns<br/>  → TRUE! ✗<br/><br/>🛑 BLOCKED from resolver!<br/>Call cascade_abort(T2)"]
        DepTracker3["<b>DependencyTracker</b><br/>aborting_txns: {T1}  ← T1 is aborting!"]
        CascadeCall["<b>cascade_abort(T2)</b><br/>1. Find dependents: {T3}<br/>2. to_abort = {T2, T3}<br/>3. mark_aborting([T2, T3])"]
    end

    subgraph "Resolver - NOT REACHED"
        ResolverBlock["<b>❌ DEADLOCK PREVENTED</b><br/><br/>If T2 was sent here:<br/>  wait_for(T1)<br/>  → DEADLOCK!<br/>  (T1 never commits)<br/><br/>✓ Validation caught it!"]
    end

    T2_State --> Validation
    Validation --> DepTracker3
    DepTracker3 -.->|"is_aborting(T1) = true"| Validation
    Validation -->|"BLOCKED"| ResolverBlock
    Validation --> CascadeCall

    style Validation fill:#fff3cd
    style ResolverBlock fill:#f8d7da
    style CascadeCall fill:#f8d7da
```

**What happened:**
1. T2 completes PREPARE, tries to commit
2. T2 has `dependencies = [T1]` → would normally go to resolver
3. **VALIDATION CHECK** (coordinator/src/transaction.rs:597-617):
   ```rust
   for dep in dependencies {  // [T1]
       if is_aborting(dep) {  // is_aborting(T1)?
           // Check aborting_transactions set
           // → TRUE! (T1 was marked in Phase 1)
           cascade_abort(T2);
           return Err(CascadingAbort);
       }
   }
   ```
4. **T2 BLOCKED** from resolver!
5. Call `cascade_abort(T2)`

**Critical insight:**
- **Without this check**: T2 sent to resolver
- Resolver calls `wait_for(T1)` → **DEADLOCK** (T1 never commits)
- **With this check**: T2 caught early, cascade abort triggered

**This is the key fix that prevents resolver deadlock!**

---

