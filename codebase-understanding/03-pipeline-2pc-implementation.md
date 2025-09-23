# Pipeline 2PC Implementation - How Pipelined 2PC Actually Works

## What Is "Pipelined" 2PC?

Traditional 2PC processes transactions one at a time:
```
Transaction A: Prepare → Commit → Done
Transaction B:              Prepare → Commit → Done
Transaction C:                          Prepare → Commit → Done
```

Pipelined 2PC processes multiple transactions simultaneously:
```
Transaction A: Prepare → Commit → Done
Transaction B:   Prepare → Commit → Done
Transaction C:     Prepare → Commit → Done
```

Multiple transactions flow through different stages of 2PC at the same time, like cars on an assembly line.

## How Pipelined 2PC Actually Works in Code

Let's trace through what happens when multiple transactions hit the system simultaneously:

### Step 1: Transaction Reaches Commit Decision Point

**File**: `coordinator/src/transaction.rs:356-375`

When a transaction is ready to commit, it makes a **smart routing decision**:

```rust
CommitStrategy::Adaptive | CommitStrategy::Pipelined => {
    if !self.dependencies.is_empty() {
        // HAS DEPENDENCIES -> Go through resolver (pipeline path)
        info!("Delegating commit to resolver for transaction {}", self.id);
        let participants_info = self.participant_ranges
            .iter()
            .map(|(range_id, info)| {
                ParticipantRangeInfo::new(*range_id, !info.writeset.is_empty())
            })
            .collect();

        self.resolver.commit(self.id, self.dependencies.clone(), participants_info).await?;
    } else {
        // NO DEPENDENCIES -> Direct commit (fast path)
        info!("Committing transaction {:?} without Resolver", self.id);
        // Skip the resolver entirely...
    }
}
```

**In English**:
- **Has Dependencies**: "I need to wait for other transactions, so use the pipeline system"
- **No Dependencies**: "I'm independent, so take the express lane"

### Step 2A: Fast Path (No Dependencies) - Parallel Processing

**File**: `coordinator/src/transaction.rs:376-430`

Independent transactions can commit **simultaneously**:

```rust
// NO DEPENDENCIES -> Direct commit (fast path)
// 1. Record commit decision atomically
let _ = self.tx_state_store.try_commit_transaction(self.id, 0).await.unwrap();
self.state = State::Committed;

// 2. Notify all participant ranges IN PARALLEL
let mut commit_join_set = JoinSet::new();
for (range_id, info) in self.participant_ranges.iter() {
    let range_id = *range_id;
    let has_writes = !info.writeset.is_empty();
    if has_writes {
        commit_join_set.spawn_on(async move {
            range_client.commit_transactions(vec![transaction_info.id], &range_id, 0).await
        }, &self.runtime);
    }
}
while commit_join_set.join_next().await.is_some() {}
```

**What Actually Happens**:
- Multiple independent transactions can execute this code **simultaneously**
- Each transaction commits to state store independently
- Each transaction notifies its ranges in parallel with other transactions
- **No coordination needed** between independent transactions

### Step 2B: Pipeline Path (Has Dependencies) - Batched Processing

**File**: `resolver/src/core/resolver.rs:65-144`

Dependent transactions enter the resolver pipeline:

```rust
pub async fn commit(
    resolver: Arc<Self>,
    transaction_id: Uuid,
    dependencies: HashSet<Uuid>,
    participant_ranges_info: Vec<ParticipantRangeInfo>,
    fake: bool,
) -> Result<(), Error> {
    let (s, r) = oneshot::channel();

    {
        let mut state = resolver.state.write().await;

        // Check dependencies and register transaction
        for dependency in dependencies {
            if !state.resolved_transactions.contains(&dependency) {
                num_pending_dependencies += 1;
                state.info_per_transaction
                    .entry(dependency)
                    .or_insert(TransactionInfo::default(dependency, fake))
                    .dependents
                    .insert(transaction_id);
            }
        }

        if num_pending_dependencies == 0 {
            // Ready to commit immediately! Add to batch.
            resolver.group_commit.add_transactions(&vec![transaction_info.clone()]).await?;
            resolver.bg_runtime.spawn(async move {
                let _ = Self::trigger_commit(resolver_clone, vec![transaction_info]).await;
            });
        }
    }

    r.await.unwrap();  // Wait for commit to complete
}
```

**Key Insight**: Transactions that are ready to commit get **batched together** instead of committing individually!

## The Pipeline Magic: Group Commit Batching

### Step 3: Multiple Transactions Get Batched Together

**File**: `resolver/src/core/group_commit.rs:152-191`

Instead of committing transactions one by one, the resolver **groups them by range**:

```rust
pub async fn add_transactions(&self, transactions: &Vec<TransactionInfo>) -> Result<(), Error> {
    // Group transactions by participant range
    info!("Grouping transactions by participant range");
    let mut tmp_group_per_participant = HashMap::new();

    for transaction in transactions {
        for participant_range in transaction.participant_ranges_info.iter() {
            if participant_range.has_writes {
                tmp_group_per_participant
                    .entry(participant_range.participant_range)  // Range A, B, C, etc.
                    .or_insert_with(|| Vec::new())
                    .push(transaction.clone());  // Add transaction to this range's batch
            }
        }
    }

    // Add all batches to their respective ranges IN PARALLEL
    let mut join_set = JoinSet::<()>::new();
    for (participant_range, transactions) in tmp_group_per_participant.iter() {
        let participant_range = participant_range.clone();
        let transactions = transactions.clone();

        join_set.spawn(async move {
            let state = state_clone.read().await;
            let mut group = state.group_per_participant
                .get(&participant_range)
                .unwrap()
                .write().await;
            group.extend(transactions.iter().cloned());  // Add to range's batch
        });
    }
    while let Some(_) = join_set.join_next().await {}
}
```

**Example**: If we have 5 transactions ready to commit:
- **Transaction 1**: writes to Range A, Range B
- **Transaction 2**: writes to Range B, Range C
- **Transaction 3**: writes to Range A
- **Transaction 4**: writes to Range C
- **Transaction 5**: writes to Range A, Range C

The batching creates:
- **Range A batch**: [Transaction 1, Transaction 3, Transaction 5]
- **Range B batch**: [Transaction 1, Transaction 2]
- **Range C batch**: [Transaction 2, Transaction 4, Transaction 5]

### Step 4: Parallel Batch Execution

**File**: `resolver/src/core/group_commit.rs:227-325`

Now the real pipeline magic happens - **all ranges commit their batches simultaneously**:

```rust
pub async fn commit(&self) -> Result<Vec<TransactionInfo>, Error> {
    let mut commit_join_set = JoinSet::<Result<(), Error>>::new();

    // For each range with pending transactions
    for participant_range in non_empty_groups.iter() {
        let participant_range_clone = participant_range.clone();
        let tx_state_store_clone = self.tx_state_store.clone();
        let range_client = self.range_client.clone();

        // Spawn parallel task for each range
        commit_join_set.spawn(async move {
            // 1. Get all transactions for this range
            let mut group_clone = group_guard_clone.write().await;
            let transactions = std::mem::take(&mut *group_clone);  // Extract batch
            drop(group_clone);

            if transactions.is_empty() {
                return Ok(());
            }

            let tx_ids_vec = transactions.iter().map(|tx| tx.id).collect();

            // 2. Batch commit to transaction state store
            tx_state_store_clone.try_batch_commit_transactions(&tx_ids_vec, 0).await?;

            // 3. Notify range to apply all changes at once
            range_client.commit_transactions(tx_ids_vec, &participant_range_clone, 0).await?;

            Ok(())
        });
    }

    // Wait for all ranges to complete their batches
    while let Some(res) = commit_join_set.join_next().await {}
}
```

**What Actually Happens**:
```
Time →
Range A: Batch commits [Tx1, Tx3, Tx5] ←─┐
Range B: Batch commits [Tx1, Tx2]      ←─┼─ All happening in parallel!
Range C: Batch commits [Tx2, Tx4, Tx5] ←─┘
```

## Pipeline Flow Example

Let's trace through a concrete example with 3 transactions:

### Initial State
```
Transaction A: WRITE key1="hello"    (no dependencies, ready immediately)
Transaction B: WRITE key2="world"    (no dependencies, ready immediately)
Transaction C: WRITE key3="!"        (depends on A, must wait)
```

### Pipeline Execution Timeline

**T=0: All transactions start committing**
- **Transaction A**: Takes fast path (no dependencies)
- **Transaction B**: Takes fast path (no dependencies)
- **Transaction C**: Goes to resolver, waits for A

**T=1: Fast path transactions execute in parallel**
```rust
// A and B execute simultaneously
Transaction A: tx_state_store.try_commit_transaction(A.id, 0).await
Transaction B: tx_state_store.try_commit_transaction(B.id, 0).await

// Both notify their ranges in parallel
Transaction A: range_client.commit_transactions([A.id], range_1, 0).await
Transaction B: range_client.commit_transactions([B.id], range_2, 0).await
```

**T=2: Transaction A finishes, wakes up C**
```rust
// A registers as committed
resolver.register_committed_transactions(vec![A.id]).await;

// C becomes ready (A was its only dependency)
new_ready_to_commit = vec![C];
resolver.group_commit.add_transactions(&new_ready_to_commit).await;
```

**T=3: Pipeline processes C**
```rust
// C gets added to group commit batch (might batch with other newly ready transactions)
resolver.trigger_commit(resolver_clone, vec![C]).await;
```

### The "Pipeline" Visualization

```
Timeline →

Fast Path (A,B):    [Prepare] → [Commit] → [Done]
                    [Prepare] → [Commit] → [Done]

Pipeline Path (C):     [Wait] → [Batch] → [Commit] → [Done]

Dependency Flow:    A finishes → Wakes C → C batches with others → C commits
```

## Key Pipeline Optimizations in Code

### 1. Early Lock Release

**File**: `coordinator/src/transaction.rs:422-430`

```rust
if any_early_lock_releases {
    info!("At least one early lock release happened, registering transaction as committed in the resolver");
    // Spawn async and don't wait for it to complete.
    let resolver = self.resolver.clone();
    let tx_id = self.id;
    self.runtime.spawn(async move {
        let _ = resolver.register_committed_transactions(vec![tx_id]).await;
    });
}
```

**What This Means**: Range servers can release locks **before** getting final confirmation, improving concurrency. The resolver is notified asynchronously to maintain dependency tracking.

### 2. Batch State Store Operations

**File**: `resolver/src/core/group_commit.rs:285-287`

```rust
// Instead of individual commits:
// tx_state_store.try_commit_transaction(tx1.id, 0).await;
// tx_state_store.try_commit_transaction(tx2.id, 0).await;
// tx_state_store.try_commit_transaction(tx3.id, 0).await;

// Batch multiple transactions together:
tx_state_store_clone.try_batch_commit_transactions(&tx_ids_vec, 0).await?;
```

**Performance Gain**: One database round-trip instead of N round-trips for N transactions.

### 3. Parallel Range Processing

**File**: `resolver/src/core/group_commit.rs:190-213`

```rust
// Add transactions to ALL ranges simultaneously
let mut join_set = JoinSet::<()>::new();
for (participant_range, transactions) in tmp_group_per_participant.iter() {
    join_set.spawn(async move {
        // Each range processes its batch concurrently
        let mut group = state.group_per_participant.get(&participant_range).unwrap().write().await;
        group.extend(transactions.iter().cloned());
    });
}
while let Some(_) = join_set.join_next().await {}
```

**Concurrency Gain**: Lock acquisition for different ranges happens in parallel instead of sequentially.

### 4. Dependency-Based Smart Routing

The routing decision happens **before** any expensive operations:

```rust
if !self.dependencies.is_empty() {
    // Route to pipeline (will batch with others)
} else {
    // Route to fast path (immediate execution)
}
```

**Efficiency**: Independent transactions skip the resolver entirely, while dependent transactions get proper ordering through batching.

## Why Pipelined 2PC Is Faster

### Traditional 2PC Bottlenecks
1. **Serial Processing**: One transaction at a time
2. **Individual State Store Writes**: One round-trip per transaction
3. **Range Notification Delays**: Each transaction notifies ranges separately

### Pipeline 2PC Solutions
1. **Parallel Processing**: Independent transactions execute simultaneously
2. **Batch State Store Writes**: Multiple transactions per round-trip
3. **Batch Range Notifications**: Multiple transactions notify each range together
4. **Smart Routing**: Bypass pipeline for independent transactions

### Performance Numbers (Conceptual)
```
Traditional: 3 transactions × 2 phases × 10ms = 60ms total
Pipelined:   3 transactions ÷ 2 batch size × 10ms = 15ms total
```

The pipeline keeps the system busy with multiple transactions in different phases instead of idle time between individual transaction commits.

This is how Sangria achieves high throughput while maintaining the correctness guarantees of 2PC!