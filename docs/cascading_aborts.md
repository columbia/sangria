# Cascading Aborts in Sangria

## Overview

Cascading aborts are a critical correctness mechanism in Sangria's Pipelined 2PC implementation. When a transaction aborts, all transactions that depend on it must also abort to prevent reading uncommitted (dirty) data and maintain database consistency.

## Why Cascading Aborts Are Necessary

### Pipelined 2PC Background

In traditional 2PC:
```
PREPARE → Wait for ACK → COMMIT → Release locks
```

In Pipelined 2PC (higher concurrency):
```
PREPARE → Release locks immediately → COMMIT later
```

**Key difference**: Locks are released after PREPARE, BEFORE the transaction commits. This allows:
- Higher concurrency (more transactions in flight)
- Better throughput (no waiting for commit)
- **Risk**: Transactions can read uncommitted data

### The Problem Without Cascading Aborts

```
T1: PREPARE(key=5, value=100) → releases lock → [ABORTS before commit]
T2: Read key=5 → sees 100 (T1's uncommitted write) → writes based on 100
T3: Read key=5 → sees T2's write → writes based on T2's value

If T1 aborts but T2 and T3 commit:
  ❌ T2 and T3 committed based on aborted data
  ❌ Database is inconsistent
```

### The Solution: Cascading Aborts

```
T1 aborts → T2 MUST abort → T3 MUST abort
All transactions that directly or transitively depend on T1 must abort.
```

## System Architecture

### Components

```
┌─────────────┐
│  Workload   │  Generates transactions
│  Generator  │  (Client)
└──────┬──────┘
       │
       ↓
┌─────────────┐
│   Frontend  │  Routes requests, collects dependencies
│(Coordinator)│
└──────┬──────┘
       │
       ├──────→ Range Servers (handle PREPARE, track dependencies)
       │
       └──────→ Resolver (dependency resolution, cascading aborts)
```

**Single-instance setup (Ray tests):**
- 1 Frontend
- 1 Resolver
- 1 Range Server
- All clients connect to the same Frontend instance

### Key Data Structures in Resolver

**resolver/src/core/resolver.rs**

```rust
struct State {
    // All transaction metadata
    info_per_transaction: HashMap<Uuid, TransactionInfo>,

    // Transactions whose fate is decided (committed OR aborted)
    resolved_transactions: HashSet<Uuid>,

    // ONLY successfully committed transactions (subset of resolved)
    committed_transactions: HashSet<Uuid>,
}

struct TransactionInfo {
    id: Uuid,
    num_dependencies: u32,        // How many txs must commit before me
    dependents: HashSet<Uuid>,    // Who is waiting on me (for cascading)
    participant_ranges_info: Vec<ParticipantRangeInfo>,
    fake: bool,
}

// Oneshot channels for notifying waiting clients
waiting_transactions: RwLock<HashMap<Uuid, oneshot::Sender<Result<(), Error>>>>
```

**Important distinction:**
- **resolved** = fate decided (could be commit OR abort)
- **committed** = successfully committed (subset of resolved)

## Transaction Flow

### Normal Commit Flow

```
1. Client → Frontend: START_TRANSACTION
   ↓
2. Client → Frontend: GET(keys...)
   Frontend → Range Servers → Frontend → Client
   ↓
3. Client → Frontend: PUT(keys, values...)
   (Frontend buffers writes)
   ↓
4. Client → Frontend: COMMIT
   ↓
5. Frontend → Range Servers: PREPARE
   - Range servers detect conflicts
   - Build dependency set: {T1, T2, T3...}
   - Release locks (Pipelined 2PC)
   - Return dependencies
   ↓
6. Frontend → Resolver: COMMIT(tx_id, dependencies, participant_ranges)
   ↓
7. Resolver processes commit:
   - Create oneshot channel for notification
   - For each dependency:
     * If NOT resolved: increment pending count, add as dependent
     * If resolved: skip (already committed or aborted)
   - If no pending dependencies: add to group commit queue
   - Block on oneshot channel waiting for notification
   ↓
8. Resolver triggers group commit when ready
   ↓
9. Resolver notifies client: sender.send(Ok(()))
   ↓
10. Client receives success, transaction counted as committed
```

### Cascading Abort Flow

```
1. Range Server detects abort during PREPARE
   (artificial abort rate OR real conflict)
   ↓
2. Frontend → Resolver: ABORT(tx_id)
   ↓
3. Resolver.abort() - Build complete abort set:

   a) Graph traversal (BFS):
      - Start with aborted tx: {T1}
      - Find T1's dependents: {T2, T4}
      - Find T2's dependents: {T3}
      - Find T4's dependents: {}
      - Result: {T1, T2, T3, T4}

   b) Only after finding ALL dependents:
      - Call register_aborted_transactions()
   ↓
4. Resolver.register_aborted_transactions():

   a) Mark all as resolved (but NOT committed):
      - state.resolved_transactions.insert(T1, T2, T3, T4)
      - Do NOT add to committed_transactions

   b) Notify all waiting clients with error:
      - For each tx in abort set:
        * Get oneshot sender
        * sender.send(Err(TransactionAborted(DependencyAborted)))
   ↓
5. Clients receive abort notification:
   - commit().await returns Err(FrontendError::InternalError(...))
   - Workload generator counts as failed transaction
   - NOT counted in committed transaction metrics
```

## Concrete Example: Branching Dependencies

### Timeline

```
Time t1: T1 writes key=5 value=100, PREPARE → releases lock

Time t2 (CONCURRENT):
  T2: reads key=5 (sees 100), writes key=5 value=200, PREPARE
  T4: reads key=5 (sees 100), writes key=10 value=400, PREPARE

Time t3:
  T2's PREPARE completes → writes visible
  T4's PREPARE completes → writes visible

Time t4:
  T3: reads key=5 (sees 200 from T2), writes key=5 value=300, PREPARE

Time t5: T1 ABORTS
```

**Dependency Graph:**
```
       T1 (aborts)
      /  \
     T2   T4
     |
     T3
```

### Code Trace

**Step 1: Building Dependencies**

When T2 registers with resolver (resolver/src/core/resolver.rs:86-104):

```rust
// T2 depends on T1
for dependency in dependencies {  // dependency = T1
    if !state.resolved_transactions.contains(&T1) {
        num_pending_dependencies += 1;  // T2 must wait

        // Add T2 as a dependent of T1
        state.info_per_transaction
            .entry(T1)
            .or_insert(TransactionInfo::default(T1, fake))
            .dependents
            .insert(T2);  // T1.dependents = {T2}
    }
}
```

**After all transactions register, state:**

```rust
State {
    info_per_transaction: {
        T1: { num_dependencies: 0, dependents: {T2, T4}, ... },
        T2: { num_dependencies: 1, dependents: {T3}, ... },
        T3: { num_dependencies: 1, dependents: {}, ... },
        T4: { num_dependencies: 1, dependents: {}, ... },
    },
    resolved_transactions: {},
    committed_transactions: {},
}
```

**Step 2: T1 Aborts - Graph Traversal**

resolver/src/core/resolver.rs:275-305:

```rust
pub async fn abort(resolver: Arc<Self>, transaction_id: Uuid) -> Result<(), Error> {
    let mut transactions_to_abort = HashSet::new();
    transactions_to_abort.insert(T1);  // {T1}

    {
        let state = resolver.state.read().await;
        let mut to_explore = vec![T1];

        while let Some(tx_id) = to_explore.pop() {
            if let Some(tx_info) = state.info_per_transaction.get(&tx_id) {
                for dependent in &tx_info.dependents {
                    if transactions_to_abort.insert(*dependent) {
                        to_explore.push(*dependent);  // Recursive exploration
                    }
                }
            }
        }
    }

    // Iteration 1: tx_id=T1 → finds {T2, T4} → to_explore=[T4, T2]
    // Iteration 2: tx_id=T4 → finds {} → to_explore=[T2]
    // Iteration 3: tx_id=T2 → finds {T3} → to_explore=[T3]
    // Iteration 4: tx_id=T3 → finds {} → to_explore=[]

    // Result: transactions_to_abort = {T1, T2, T3, T4}

    Self::register_aborted_transactions(resolver,
        transactions_to_abort.into_iter().collect()).await
}
```

**Console output:**
```
Aborting transaction T1 and cascading to dependents
Cascading abort to dependent transaction T2
Cascading abort to dependent transaction T4
Cascading abort to dependent transaction T3
Collected 4 transactions to abort (including dependents)
```

**Step 3: Register Aborted Transactions**

resolver/src/core/resolver.rs:307-357:

```rust
pub async fn register_aborted_transactions(
    resolver: Arc<Self>,
    transaction_ids: Vec<Uuid>,  // [T1, T2, T3, T4]
) -> Result<(), Error> {
    {
        let mut state = resolver.state.write().await;

        // Mark all as resolved (fate decided) but NOT committed
        for transaction_id in &transaction_ids {
            state.resolved_transactions.insert(*transaction_id);
            // DO NOT add to committed_transactions!
        }

        // Unblock waiting clients with abort error
        let mut waiting_transactions = resolver.waiting_transactions.write().await;
        for transaction_id in &transaction_ids {
            if let Some(sender) = waiting_transactions.remove(transaction_id) {
                sender.send(Err(Error::TransactionAborted(
                    TransactionAbortReason::DependencyAborted
                ))).unwrap();
            }
        }
    }
    Ok(())
}
```

**Console output:**
```
Registering 4 transactions as aborted
Transaction T1 marked as resolved (aborted)
Transaction T2 marked as resolved (aborted)
Notified transaction T2 of abort
Transaction T3 marked as resolved (aborted)
Notified transaction T3 of abort
Transaction T4 marked as resolved (aborted)
Notified transaction T4 of abort
```

**Step 4: Clients Receive Abort**

workload-generator/src/transaction_impl/rw_transaction.rs:127-134:

```rust
client_clone
    .commit(CommitRequest {
        transaction_id: transaction_id.to_string(),
    })
    .await
    .map_err(|e| FrontendError::InternalError(Arc::new(e)))?;
    // ↑ Returns Err, propagated with ?
    // Transaction NOT counted as committed
```

**Final State:**

```rust
State {
    info_per_transaction: { T1: {...}, T2: {...}, T3: {...}, T4: {...} },
    resolved_transactions: {T1, T2, T3, T4},  // All marked resolved
    committed_transactions: {},                // NONE committed!
}
```

## Critical Implementation Details

### The Two-Set Design

**Why have both `resolved_transactions` and `committed_transactions`?**

When a new transaction T5 arrives with dependency on T1:

```rust
for dependency in dependencies {  // dependency = T1
    if !state.resolved_transactions.contains(&T1) {
        // T1 not resolved yet → T5 must wait
        num_pending_dependencies += 1;
    } else {
        // T1 is resolved (committed OR aborted)
        // T5 doesn't wait, but:
        //   - If T1 committed: T5 can proceed
        //   - If T1 aborted: T5 will be aborted via cascading
    }
}
```

The check is: "Has T1's fate been decided?"
- If yes (in `resolved_transactions`), don't wait
- If no, wait for notification

Whether T1 committed or aborted is determined by presence in `committed_transactions`.

### The Oneshot Channel Fix

**Before (Broken):**
```rust
waiting_transactions: HashMap<Uuid, oneshot::Sender<()>>

// On abort:
sender.send(()).unwrap();  // ❌ Sent success signal!
```

**After (Fixed):**
```rust
waiting_transactions: HashMap<Uuid, oneshot::Sender<Result<(), Error>>>>

// On commit:
sender.send(Ok(())).unwrap();  // ✅

// On abort:
sender.send(Err(Error::TransactionAborted(
    TransactionAbortReason::DependencyAborted
))).unwrap();  // ✅
```

This allows clients to distinguish commits from aborts and handle them correctly.

### No Group Aborts

Unlike commits (which are batched for efficiency), aborts are immediate:
- When T1 aborts, we immediately find ALL dependents
- Mark them all as aborted
- Notify all clients immediately
- No waiting, no batching

This is intentional - aborts should be fast to:
- Free up waiting clients quickly
- Prevent cascade amplification
- Release system resources

## Testing Setup

### Artificial Abort Injection

For testing cascading aborts, the system supports artificial abort rate configuration:

```rust
// atomix_setup.servers_config["range_server"]["artificial_abort_rate"] = 0.15

// During PREPARE phase:
if rand::random::<f64>() < artificial_abort_rate {
    return Err(PrepareError::ArtificialAbort);
}
```

### Ray Experiment Configuration

```python
# cascading_abort_experiment.py
ABORT_RATES = [0.05, 0.15, 0.30]  # 5%, 15%, 30%
NUM_ITERATIONS = 2
NUM_QUERIES = 100
MAX_CONCURRENCY = 50  # High concurrency creates dependencies
ZIPFIAN_CONSTANT = 0.9  # High contention creates dependencies
```

**Experimental Results:**
- 5% abort rate: 509.6 txn/s committed
- 15% abort rate: 349.1 txn/s committed (-31.5%)
- 30% abort rate: 213.3 txn/s committed (-58.1%)

Clear throughput degradation due to cascading abort amplification.

## References

### Key Files

- `resolver/src/core/resolver.rs` - Main cascading abort logic
  - Lines 67-152: commit() - dependency registration
  - Lines 154-184: trigger_commit() - group commit
  - Lines 200-273: register_committed_transactions() - success path
  - Lines 275-305: abort() - cascading abort graph traversal
  - Lines 307-357: register_aborted_transactions() - abort notification

- `resolver/src/remote/client.rs` - Resolver client interface
  - Lines 140-151: abort() - client-side abort call

- `workload-generator/src/transaction_impl/rw_transaction.rs` - Client handling
  - Lines 127-134: commit error handling

- `workload-generator/scripts/cascading_abort_experiment.py` - Test harness

### Testing

Run diagnostic test:
```bash
python3 workload-generator/scripts/test_cascading_abort.py \
  --abort-rate 0.2 \
  --num-queries 100 \
  --num-keys 3 \
  --max-concurrency 15
```

Run full experiment:
```bash
python3 workload-generator/scripts/cascading_abort_experiment.py
```
