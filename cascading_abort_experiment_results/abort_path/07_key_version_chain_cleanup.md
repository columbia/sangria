# Key Version Chain Cleanup Mechanism

**Why this wasn't highlighted before:** The previous diagrams showed simple scenarios where only one transaction wrote to each key (T1→A, T2→B, T3→C). In those cases, cleanup is straightforward - just remove the entry. But when **multiple transactions write to the SAME key**, we need `key_version_chain` to properly revert `pending_commit_table` to the previous uncommitted writer.

---

## The Problem

When T2 aborts but T1 and T3 also wrote to the same key, we need to know:
- **Who was the previous uncommitted writer before T2?**
- **Who is the next uncommitted writer after T2?**

This is what `key_version_chain` solves.

---

## Scenario: Multiple Writers to Same Key

```mermaid
graph TB
    subgraph "Initial State - Three Transactions Write Key A"
        InitState["<b>Timeline</b><br/>T1 PREPARE: write A:1<br/>T2 PREPARE: write A:2<br/>T3 PREPARE: write A:3<br/><br/>All uncommitted!"]

        PrepRec["<b>pending_prepare_records</b><br/>{T1: {A:1}, T2: {A:2}, T3: {A:3}}"]
        CommitTbl["<b>pending_commit_table</b><br/>{A: T3}  ← last uncommitted writer"]
        VerChain["<b>key_version_chain</b><br/>{A: [T1, T2, T3]}  ← ordered history"]
    end

    InitState --> PrepRec
    InitState --> CommitTbl
    InitState --> VerChain

    style VerChain fill:#fff3cd
```

---

## Case 1: T3 Aborts (Last Writer)

```mermaid
graph TB
    subgraph "T3 Abort Cleanup"
        Code1["<b>Cleanup Code (line 443-451)</b><br/><br/>chain = [T1, T2, T3]<br/>chain.retain(|&id| id != T3)<br/>→ chain = [T1, T2]<br/><br/>prev_tx = chain.last() = T2<br/>pending_commit_table[A] = T2"]

        After1["<b>After T3 Abort</b><br/><br/>pending_prepare_records:<br/>{T1: {A:1}, T2: {A:2}}<br/><br/>pending_commit_table:<br/>{A: T2}  ← reverted!<br/><br/>key_version_chain:<br/>{A: [T1, T2]}"]
    end

    Code1 --> After1

    style Code1 fill:#fff3cd
    style After1 fill:#d4edda
```

**Key Insight:** `pending_commit_table[A]` reverted from T3 to T2 using `key_version_chain`.

**Why this matters:** If T4 does `GET(A)` now, it will correctly depend on T2 (not T3).

---

## Case 2: T2 Aborts (Middle Writer)

```mermaid
graph TB
    subgraph "Starting from Case 1 Result"
        Start2["<b>Current State</b><br/>key_version_chain[A] = [T1, T2]<br/>pending_commit_table[A] = T2"]
    end

    subgraph "T2 Abort Cleanup"
        Code2["<b>Cleanup Code</b><br/><br/>chain = [T1, T2]<br/>chain.retain(|&id| id != T2)<br/>→ chain = [T1]<br/><br/>prev_tx = chain.last() = T1<br/>pending_commit_table[A] = T1"]

        After2["<b>After T2 Abort</b><br/><br/>pending_prepare_records:<br/>{T1: {A:1}}<br/><br/>pending_commit_table:<br/>{A: T1}  ← reverted again!<br/><br/>key_version_chain:<br/>{A: [T1]}"]
    end

    Start2 --> Code2
    Code2 --> After2

    style Code2 fill:#fff3cd
    style After2 fill:#d4edda
```

**Key Insight:** Even though T2 was in the middle, cleanup still works correctly.

---

## Case 3: T1 Aborts (Last Remaining Writer)

```mermaid
graph TB
    subgraph "T1 Abort Cleanup"
        Code3["<b>Cleanup Code</b><br/><br/>chain = [T1]<br/>chain.retain(|&id| id != T1)<br/>→ chain = []<br/><br/>if chain.is_empty():<br/>  pending_commit_table.remove(A)<br/>  key_version_chain.remove(A)"]

        After3["<b>After T1 Abort</b><br/><br/>pending_prepare_records:<br/>{}<br/><br/>pending_commit_table:<br/>{}  ← A removed entirely<br/><br/>key_version_chain:<br/>{}  ← A removed entirely"]
    end

    Code3 --> After3

    style Code3 fill:#fff3cd
    style After3 fill:#d4edda
```

**Key Insight:** When last writer aborts, key A is completely removed from tracking.

**Why this matters:** If T4 does `GET(A)` now, it reads from Cassandra (committed data) with no dependencies.

---

## The Algorithm (rangeserver/src/range_manager/impl.rs:443-451)

```rust
for key in prepare_record.changes.keys() {
    if let Some(chain) = pending_state.key_version_chain.get_mut(key) {
        // 1. Remove aborting transaction from chain
        chain.retain(|&id| id != tx.id);

        // 2. Revert pending_commit_table to previous uncommitted writer
        if let Some(&prev_tx) = chain.last() {
            // Still have uncommitted writers → update to previous
            pending_state.pending_commit_table.insert(key.clone(), prev_tx);
        } else {
            // No more uncommitted writers → remove key entirely
            pending_state.pending_commit_table.remove(key);
            pending_state.key_version_chain.remove(key);
        }
    }
}
```

---

## Why This Mechanism is Critical

### Without `key_version_chain`:

```
T1, T2, T3 all write A
pending_commit_table[A] = T3

T3 aborts...
❌ How do we know T2 was the previous writer?
❌ We'd have to scan all pending_prepare_records!
❌ O(n) operation for every abort!
```

### With `key_version_chain`:

```
key_version_chain[A] = [T1, T2, T3]

T3 aborts:
✓ chain.retain(|&id| id != T3) → [T1, T2]
✓ chain.last() → T2
✓ pending_commit_table[A] = T2
✓ O(1) operation!
```

---

## Complete Example Timeline

```mermaid
sequenceDiagram
    participant T1 as T1 (A:1)
    participant T2 as T2 (A:2)
    participant T3 as T3 (A:3)
    participant RS as RangeServer

    T1->>RS: PREPARE(A:1)
    Note over RS: chain[A]=[T1]<br/>commit_table[A]=T1

    T2->>RS: PREPARE(A:2)
    Note over RS: chain[A]=[T1,T2]<br/>commit_table[A]=T2

    T3->>RS: PREPARE(A:3)
    Note over RS: chain[A]=[T1,T2,T3]<br/>commit_table[A]=T3

    Note over T3: 🎲 T3 Aborts!
    T3->>RS: ABORT(T3)
    Note over RS: chain.retain(≠T3)<br/>chain[A]=[T1,T2]<br/>commit_table[A]=T2 ✓

    Note over T2: T2 Cascade Aborts
    T2->>RS: ABORT(T2)
    Note over RS: chain.retain(≠T2)<br/>chain[A]=[T1]<br/>commit_table[A]=T1 ✓

    Note over T1: T1 Aborts
    T1->>RS: ABORT(T1)
    Note over RS: chain.retain(≠T1)<br/>chain[A]=[]<br/>Remove A entirely ✓
```

---

## Summary

| Data Structure | Role in Cleanup |
|----------------|-----------------|
| **key_version_chain** | Maintains ordered list of all writers to find previous uncommitted writer |
| **pending_commit_table** | Updated during cleanup to point to previous uncommitted writer |
| **pending_prepare_records** | Transaction entry simply removed (no revert needed) |

**Why it wasn't highlighted before:** The original diagrams (T1→A, T2→B, T3→C) had no overlapping writes, so cleanup was trivial (just remove). This mechanism only becomes critical when **multiple transactions write to the same key** - a common real-world scenario in high-contention workloads.

**Performance:** O(k) where k = number of writers to the key (typically small), vs O(n) scanning all transactions.
