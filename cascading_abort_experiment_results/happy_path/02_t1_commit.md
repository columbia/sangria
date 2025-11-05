# Phase 2: T1 COMMIT (Direct)


```mermaid
graph TB
    subgraph "Coordinator"
        T1_State["<b>Transaction T1</b><br/>id: T1<br/>dependencies: []<br/>state: Committed ✓"]
    end

    subgraph "Range0 - After COMMIT"
        PrepRec2["<b>pending_prepare_records</b><br/>{}  ← T1 removed"]
        CommitTbl2["<b>pending_commit_table</b><br/>{}  ← A removed (T1 committed!)"]
        VerChain2["<b>key_version_chain</b><br/>{A: [T1]}  ← stays for cascading abort tracking"]
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
5. Range0 **removes A from `pending_commit_table`** (line 693) - no longer uncommitted!
6. Range0 **keeps T1 in `key_version_chain`** - used for cascading abort tracking
7. T1's value written to Cassandra (durable)

**Key Insight:** `pending_commit_table` tracks UNCOMMITTED writes only. Once committed, the entry is removed. But `key_version_chain` remains for cascading abort detection.

---

