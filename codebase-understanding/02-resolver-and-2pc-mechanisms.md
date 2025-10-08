# Resolver and 2PC Mechanisms - How Dependencies Actually Work

## The Problem: Why Do We Need a Resolver?

Imagine three transactions running simultaneously:
```
Transaction A: WRITE key1 = "hello"
Transaction B: READ key1, WRITE key2 = "world"
Transaction C: READ key2, WRITE key3 = "!"
```

**The Problem**: B depends on A, and C depends on B. If they commit in wrong order (C, A, B), we get inconsistent results!

**The Solution**: The resolver tracks these dependencies and ensures correct commit ordering.

## How the Resolver Actually Works

Let's trace through what happens when Transaction B (depends on A) tries to commit:

### Step 1: Transaction B Calls Resolver Commit

**File**: `resolver/src/core/resolver.rs:65-76`

Transaction B arrives at the resolver saying "I want to commit, but I depend on Transaction A":

```rust
pub async fn commit(
    resolver: Arc<Self>,
    transaction_id: Uuid,                    // B's ID
    dependencies: HashSet<Uuid>,             // {A's ID}
    participant_ranges_info: Vec<ParticipantRangeInfo>,  // Where B wants to write
    fake: bool,
) -> Result<(), Error> {
    // Quick optimization: read-only transactions don't need dependency tracking
    if participant_ranges_info.iter().all(|info| !info.has_writes) {
        return Ok(());
    }

    let (s, r) = oneshot::channel();  // Create a "wake me up" channel
    let mut num_pending_dependencies = 0;
```

### Step 2: Dependency Registration

**File**: `resolver/src/core/resolver.rs:82-102`

```rust
{
    let mut state = resolver.state.write().await;  // Lock the dependency graph

    for dependency in dependencies {  // For each transaction B depends on
        if !state.resolved_transactions.contains(&dependency) {
            // Transaction A hasn't committed yet, so B must wait
            num_pending_dependencies += 1;

            // CRITICAL: Register B as waiting for A
            state.info_per_transaction
                .entry(dependency)                    // Get A's info
                .or_insert(TransactionInfo::default(dependency, fake))
                .dependents                          // A's list of who's waiting for it
                .insert(transaction_id);             // Add B to that list
        } else {
            info!("Dependency {:?} was already resolved", dependency);
        }
    }
```

**What Actually Happened**:
- Resolver looks up Transaction A: "Has A committed yet? No."
- Resolver adds B to A's `dependents` list: `A.dependents = {B}`
- Now the resolver knows: "When A commits, wake up B"

### Step 3: Waiting vs. Ready to Commit

**File**: `resolver/src/core/resolver.rs:104-141`

```rust
    let transaction_info = state.info_per_transaction
        .entry(transaction_id)
        .or_insert(TransactionInfo::default(transaction_id, fake));

    transaction_info.num_dependencies = num_pending_dependencies;  // B.num_dependencies = 1
    transaction_info.participant_ranges_info = participant_ranges_info;

    resolver.waiting_transactions.write().await.insert(transaction_id, s);  // Store B's "wake up" channel

    if num_pending_dependencies == 0 {
        // B has no pending dependencies - can commit immediately!
        info!("No pending dependencies, committing transaction {:?}", transaction_id);

        resolver.group_commit.add_transactions(&vec![transaction_info.clone()]).await?;
        resolver.bg_runtime.spawn(async move {
            let _ = Self::trigger_commit(resolver_clone, vec![transaction_info]).await;
        });
    }
}

// Block until transaction is committed (or dependency resolved)
r.await.unwrap();  // Transaction B waits here until A commits
```

**What Actually Happened**:
- Transaction B is **blocked** waiting for A to commit
- The resolver stores B's "wake up" channel
- B's thread literally waits at `r.await` until someone signals the channel

## The Cascade Effect - When Dependencies Resolve

Now let's see what happens when Transaction A finally commits:

### Step 1: Transaction A Finishes and Notifies Resolver

**File**: `resolver/src/core/resolver.rs:195-208`

When Transaction A successfully commits, the resolver is notified:

```rust
pub async fn register_committed_transactions(
    resolver: Arc<Self>,
    transaction_ids: Vec<Uuid>,  // [A's ID] - A just committed!
) -> Result<(), Error> {
    let mut new_ready_to_commit = Vec::new();

    {
        let mut state = resolver.state.write().await;
        for transaction_id in transaction_ids {
            state.resolved_transactions.insert(transaction_id);  // Mark A as resolved
        }

        let mut new_resolved_dependencies = vec![A's ID];
```

### Step 2: Find Who Was Waiting for A

**File**: `resolver/src/core/resolver.rs:211-242`

The resolver now finds all transactions that were waiting for A:

```rust
        while !new_resolved_dependencies.is_empty() {
            let transaction_id = new_resolved_dependencies.pop().unwrap();  // A's ID

            if let Some(transaction_info) = state.info_per_transaction.get_mut(&transaction_id) {
                // Get everyone who was waiting for A
                let dependents = mem::take(&mut transaction_info.dependents);  // dependents = {B}

                for dependent in dependents.iter() {  // For each transaction waiting for A
                    let dependent_transaction_info = state.info_per_transaction.get_mut(&dependent).unwrap();

                    // B was waiting for 1 transaction (A), now it's 0
                    dependent_transaction_info.num_dependencies -= 1;

                    if dependent_transaction_info.num_dependencies == 0 {
                        // B now has no pending dependencies! Ready to commit!
                        new_ready_to_commit.push(dependent_transaction_info.clone());
                        new_resolved_dependencies.push(*dependent);  // B might wake up others too
                    }
                }
            }
        }
```

**What Actually Happened**:
- A committed, so resolver looks at A's `dependents` list: finds {B}
- B was waiting for 1 transaction, now 0 → B is ready to commit!
- If C was waiting for B, it would be added to the next round

### Step 3: Wake Up Waiting Transactions

**File**: `resolver/src/core/resolver.rs:245-265`

```rust
        // Add newly ready transactions to group commit
        if !new_ready_to_commit.is_empty() {
            resolver.group_commit.add_transactions(&new_ready_to_commit).await;
        }
    }

    // Trigger commit for newly ready transactions
    if !new_ready_to_commit.is_empty() {
        info!("New ready to commit transactions: {:?}",
              new_ready_to_commit.iter().map(|tx| tx.id).collect::<Vec<_>>());

        let resolver_clone = resolver.clone();
        resolver.bg_runtime.spawn(async move {
            let _ = Resolver::trigger_commit(resolver_clone, new_ready_to_commit).await;
        });
    }
```

### Step 4: Group Commit Executes

**File**: `resolver/src/core/resolver.rs:149-178`

Now the resolver triggers a group commit for Transaction B:

```rust
async fn trigger_commit(
    resolver: Arc<Self>,
    transactions: Vec<TransactionInfo>,  // [B's info]
) -> Result<(), Error> {
    info!("Triggering commit for transactions {:?}",
          transactions.iter().map(|tx| tx.id).collect::<Vec<_>>());

    // Execute the actual commit
    let finished_transactions = resolver.group_commit.commit().await?;

    // Wake up the waiting transactions
    {
        let mut waiting_transactions = resolver.waiting_transactions.write().await;
        for transaction in finished_transactions {
            let sender = waiting_transactions.remove(&transaction.id).unwrap();
            sender.send(()).unwrap();  // This unblocks Transaction B's await!
        }
    }

    // Register B as committed (might wake up C if C depends on B)
    let finished_transaction_ids = finished_transactions.iter().map(|tx| tx.id).collect::<Vec<_>>();
    if !finished_transaction_ids.is_empty() {
        Self::spawn_register_committed_transactions(resolver, finished_transaction_ids);
    }
}
```

**What Actually Happened**:
- B gets added to group commit and commits to the database
- B's "wake up" channel gets signaled: `sender.send(())`
- Transaction B's thread unblocks from `r.await`
- Resolver registers B as committed (might wake up C)

## Group Commit - Batching for Performance

### The Problem with Individual Commits

Without group commit, each transaction would commit individually:
```
Transaction A commits → tells database → waits for ack
Transaction B commits → tells database → waits for ack
Transaction C commits → tells database → waits for ack
```

### The Group Commit Solution

**File**: `resolver/src/core/group_commit.rs:152-170`

Instead, transactions get batched together:

```rust
pub async fn add_transactions(&self, transactions: &Vec<TransactionInfo>) -> Result<(), Error> {
    // Group transactions by which database ranges they touch
    let mut tmp_group_per_participant = HashMap::new();

    for transaction in transactions {
        for participant_range in transaction.participant_ranges_info.iter() {
            if participant_range.has_writes {  // Only ranges with writes need commits
                tmp_group_per_participant
                    .entry(participant_range.participant_range)  // Range ID
                    .or_insert_with(|| Vec::new())
                    .push(transaction.clone());  // Add transaction to this range's batch
            }
        }
    }
```

**Example**: If we have:
- Transaction A: writes to Range 1, Range 2
- Transaction B: writes to Range 2, Range 3
- Transaction C: writes to Range 1

The grouping becomes:
- **Range 1**: [Transaction A, Transaction C]
- **Range 2**: [Transaction A, Transaction B]
- **Range 3**: [Transaction B]

### Parallel Range Commits

**File**: `resolver/src/core/group_commit.rs:227-325`

Now all ranges commit their batches in parallel:

```rust
pub async fn commit(&self) -> Result<Vec<TransactionInfo>, Error> {
    let mut commit_join_set = JoinSet::<Result<(), Error>>::new();

    for participant_range in non_empty_groups.iter() {
        commit_join_set.spawn(async move {
            // Get all transactions for this range
            let transactions = std::mem::take(&mut *group_clone);
            let tx_ids_vec = transactions.iter().map(|tx| tx.id).collect();

            // Batch commit to transaction state store
            tx_state_store_clone.try_batch_commit_transactions(&tx_ids_vec, 0).await?;

            // Tell range to apply all changes at once
            range_client.commit_transactions(tx_ids_vec, &participant_range_clone, 0).await?;
        });
    }

    while let Some(res) = commit_join_set.join_next().await {}
```

**What Actually Happened**:
```
Range 1: Batch commits [A, C] simultaneously
Range 2: Batch commits [A, B] simultaneously
Range 3: Batch commits [B] simultaneously

All ranges commit in parallel!
```

## The Complete Dependency Resolution Flow

Let's put it all together with a concrete example:

### Initial State
```
Transaction A: WRITE key1 = "hello"           (no dependencies)
Transaction B: READ key1, WRITE key2 = "world"  (depends on A)
Transaction C: READ key2, WRITE key3 = "!"      (depends on B)
```

### Step-by-Step Execution

**1. Transaction A Commits (No Dependencies)**
- A takes fast path, commits directly
- A notifies resolver: "I'm done"
- Resolver looks up A's dependents: finds {B}
- B becomes ready to commit

**2. Transaction B Commits (Was Waiting for A)**
- B gets added to group commit batch
- B commits to database
- B notifies resolver: "I'm done"
- Resolver looks up B's dependents: finds {C}
- C becomes ready to commit

**3. Transaction C Commits (Was Waiting for B)**
- C gets added to group commit batch
- C commits to database
- Done!

### The Data Structures During Execution

**Resolver State at Start**:
```rust
info_per_transaction: {
    A: TransactionInfo { id: A, num_dependencies: 0, dependents: {B} },
    B: TransactionInfo { id: B, num_dependencies: 1, dependents: {C} },
    C: TransactionInfo { id: C, num_dependencies: 1, dependents: {} }
}
resolved_transactions: {}
waiting_transactions: {B: channel, C: channel}
```

**After A Commits**:
```rust
info_per_transaction: {
    A: TransactionInfo { id: A, num_dependencies: 0, dependents: {} },  // dependents cleared
    B: TransactionInfo { id: B, num_dependencies: 0, dependents: {C} }, // decremented to 0
    C: TransactionInfo { id: C, num_dependencies: 1, dependents: {} }
}
resolved_transactions: {A}
waiting_transactions: {C: channel}  // B removed, woken up
```

**After B Commits**:
```rust
info_per_transaction: {
    A: TransactionInfo { id: A, num_dependencies: 0, dependents: {} },
    B: TransactionInfo { id: B, num_dependencies: 0, dependents: {} },  // dependents cleared
    C: TransactionInfo { id: C, num_dependencies: 0, dependents: {} }   // decremented to 0
}
resolved_transactions: {A, B}
waiting_transactions: {}  // C removed, woken up
```

This is exactly how Sangria ensures that transactions commit in the correct dependency order while maximizing parallelism through batching!