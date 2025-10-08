# Cascading Abort Implementation - How to Abort Dependent Transactions

## The Problem: What Happens When Dependencies Fail?

Imagine this scenario happening in the current Sangria system:

```
Transaction A: WRITE key1 = "hello"           (commits successfully)
Transaction B: READ key1, WRITE key2 = "world"  (depends on A, commits successfully)
Transaction C: READ key2, WRITE key3 = "!"      (depends on B, FAILS during commit)
Transaction D: READ key3, WRITE key4 = "done"   (depends on C, still waiting...)
```

**Current Behavior**: Transaction D will wait forever for C to commit, but C has already failed!

**Needed Behavior**: When C fails, D should be automatically aborted and all its pending changes rolled back.

## How Cascading Abort Should Work

Let's trace through what should happen when Transaction C fails:

### Step 1: Transaction C Fails During Group Commit

**Current Code**: `resolver/src/core/group_commit.rs:285-293`

```rust
// Current code - panics on failure!
if let Err(e) = tx_state_store_clone
    .try_batch_commit_transactions(&tx_ids_vec, 0)
    .await
{
    panic!(
        "Error committing transactions to tx_state_store {:?}: {:?}",
        participant_range_clone, e
    );
}
```

**What Actually Happens Now**: The entire system panics and crashes!

**What Should Happen**: Register the failed transactions for cascading abort.

### Step 2: Resolver Should Track Failed Transactions

**Current Code**: `resolver/src/core/resolver.rs:37-41`

```rust
// Current resolver state
#[derive(Default)]
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,  // Successfully committed
}
```

**What We Need**: Add tracking for aborted transactions so we know which ones failed.

### Step 3: Find Who Was Waiting for Failed Transaction

**Current Code**: `resolver/src/core/resolver.rs:219-242`

```rust
// This code currently only handles successful commits
let dependents = mem::take(&mut transaction_info.dependents);
for dependent in dependents.iter() {
    let dependent_transaction_info = state.info_per_transaction.get_mut(&dependent).unwrap();
    dependent_transaction_info.num_dependencies -= 1;

    if dependent_transaction_info.num_dependencies == 0 {
        // Dependent becomes ready to commit
        new_ready_to_commit.push(dependent_transaction_info.clone());
    }
}
```

**What We Need**: Similar logic but for aborts - cascade the abort to all dependents.

## Concrete Implementation Walkthrough

Let's walk through the exact code changes needed to make cascading abort work:

### Change 1: Add Aborted Transaction Tracking

**File**: `resolver/src/core/resolver.rs:37-41`

**Current Code**:
```rust
#[derive(Default)]
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,
}
```

**New Code**:
```rust
#[derive(Default)]
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,      // Successfully committed
    aborted_transactions: HashSet<Uuid>,       // Failed transactions
}
```

### Change 2: Implement Cascading Abort Logic

**File**: `resolver/src/core/resolver.rs` (add new method after line 267)

**New Code**:
```rust
pub async fn register_aborted_transactions(
    resolver: Arc<Self>,
    aborted_transaction_ids: Vec<Uuid>,
) -> Result<Vec<Uuid>, Error> {
    let mut cascaded_aborts = Vec::new();

    {
        let mut state = resolver.state.write().await;
        let mut aborts_to_propagate = aborted_transaction_ids.clone();

        // Process each failed transaction and find its dependents
        while !aborts_to_propagate.is_empty() {
            let abort_tx_id = aborts_to_propagate.pop().unwrap();

            // Mark this transaction as aborted
            state.aborted_transactions.insert(abort_tx_id);

            if let Some(transaction_info) = state.info_per_transaction.get_mut(&abort_tx_id) {
                // Get all transactions waiting for this failed one
                let dependents = mem::take(&mut transaction_info.dependents);

                for dependent_id in dependents {
                    // Check if dependent hasn't already been resolved or aborted
                    if !state.resolved_transactions.contains(&dependent_id)
                        && !state.aborted_transactions.contains(&dependent_id) {

                        // This dependent must also be aborted!
                        aborts_to_propagate.push(dependent_id);
                        cascaded_aborts.push(dependent_id);

                        info!("Cascading abort from {} to dependent {}", abort_tx_id, dependent_id);
                    }
                }
            }

            // Wake up any transactions waiting for the aborted transaction
            if let Some(sender) = resolver.waiting_transactions.write().await.remove(&abort_tx_id) {
                let _ = sender.send(());  // This will cause waiting transaction to check abort status
            }
        }
    }

    info!("Cascaded abort to {} transactions: {:?}", cascaded_aborts.len(), cascaded_aborts);
    Ok(cascaded_aborts)
}
```

**What This Does**:
- Marks failed transactions as aborted
- Finds all their dependents using existing `dependents` field
- Recursively aborts dependents (cascading effect)
- Wakes up waiting transactions so they can check if they've been aborted

### Change 3: Handle Group Commit Failures

**File**: `resolver/src/core/group_commit.rs:285-293`

**Current Code**:
```rust
if let Err(e) = tx_state_store_clone
    .try_batch_commit_transactions(&tx_ids_vec, 0)
    .await
{
    panic!(
        "Error committing transactions to tx_state_store {:?}: {:?}",
        participant_range_clone, e
    );
}
```

**New Code**:
```rust
if let Err(e) = tx_state_store_clone
    .try_batch_commit_transactions(&tx_ids_vec, 0)
    .await
{
    error!("Batch commit failed for range {:?}: {:?}", participant_range_clone, e);

    // Instead of panicking, register these transactions as aborted
    let resolver_clone = resolver.clone();  // Need resolver reference passed to group_commit
    let failed_tx_ids = tx_ids_vec.clone();

    tokio::spawn(async move {
        if let Err(abort_err) = Resolver::register_aborted_transactions(resolver_clone, failed_tx_ids).await {
            error!("Failed to register aborted transactions: {:?}", abort_err);
        }
    });

    return Err(Error::GroupCommitFailed);
}
```

**What This Does**: Instead of crashing, register failed transactions for cascading abort.

### Change 4: Check for Aborts in Commit Method

**File**: `resolver/src/core/resolver.rs:65-144` (modify existing method)

**Current Code**:
```rust
// Block until the transaction is actually committed
r.await.unwrap();
info!("Transaction {} finally committed!", transaction_id);
Ok(())
```

**New Code**:
```rust
// Block until the transaction is committed OR aborted
r.await.unwrap();

// Check if transaction was aborted while waiting
{
    let state = resolver.state.read().await;
    if state.aborted_transactions.contains(&transaction_id) {
        info!("Transaction {} was aborted due to cascading abort", transaction_id);
        return Err(Error::TransactionAborted(TransactionAbortReason::CascadingAbort));
    }
}

info!("Transaction {} finally committed!", transaction_id);
Ok(())
```

**What This Does**: After waiting, check if the transaction was aborted due to cascading failure.

### Change 5: Prevent New Dependencies on Aborted Transactions

**File**: `resolver/src/core/resolver.rs:82-102` (modify existing loop)

**Current Code**:
```rust
for dependency in dependencies {
    if !state.resolved_transactions.contains(&dependency) {
        // Dependency is not yet resolved, so we need to wait for it
        num_pending_dependencies += 1;
        state.info_per_transaction
            .entry(dependency)
            .or_insert(TransactionInfo::default(dependency, fake))
            .dependents
            .insert(transaction_id);
    }
}
```

**New Code**:
```rust
for dependency in dependencies {
    if state.aborted_transactions.contains(&dependency) {
        // Dependency was aborted! This transaction should abort immediately
        info!("Transaction {} depends on aborted transaction {}, aborting",
              transaction_id, dependency);
        return Err(Error::TransactionAborted(TransactionAbortReason::DependencyAborted));
    }

    if !state.resolved_transactions.contains(&dependency) {
        // Dependency is not yet resolved, so we need to wait for it
        num_pending_dependencies += 1;
        state.info_per_transaction
            .entry(dependency)
            .or_insert(TransactionInfo::default(dependency, fake))
            .dependents
            .insert(transaction_id);
    }
}
```

**What This Does**: Check if any dependency is already aborted before waiting for it.

## Concrete Example: How Cascading Abort Works

Let's trace through the exact execution with our example transactions:

### Initial State
```
Transaction A: WRITE key1 = "hello"           (committed successfully)
Transaction B: READ key1, WRITE key2 = "world"  (committed successfully)
Transaction C: READ key2, WRITE key3 = "!"      (depends on B, about to commit)
Transaction D: READ key3, WRITE key4 = "done"   (depends on C, waiting in resolver)
```

**Resolver State**:
```rust
info_per_transaction: {
    C: TransactionInfo { id: C, num_dependencies: 0, dependents: {D} },
    D: TransactionInfo { id: D, num_dependencies: 1, dependents: {} }
}
resolved_transactions: {A, B}
aborted_transactions: {}
waiting_transactions: {D: channel}
```

### Step 1: Transaction C Fails During Group Commit

**What Happens**: Range server or transaction state store fails C's commit

```rust
// In group_commit.rs - C's commit fails
tx_state_store_clone.try_batch_commit_transactions(&[C.id], 0).await  // FAILS!

// New error handling kicks in
tokio::spawn(async move {
    Resolver::register_aborted_transactions(resolver_clone, vec![C.id]).await;
});
```

### Step 2: Cascading Abort Processes C's Failure

```rust
// register_aborted_transactions processes C
let mut aborts_to_propagate = vec![C.id];

while !aborts_to_propagate.is_empty() {
    let abort_tx_id = aborts_to_propagate.pop().unwrap();  // C.id

    // Mark C as aborted
    state.aborted_transactions.insert(C.id);

    // Find C's dependents
    let dependents = mem::take(&mut transaction_info.dependents);  // dependents = {D}

    for dependent_id in dependents {  // For D
        if !state.resolved_transactions.contains(&D.id) && !state.aborted_transactions.contains(&D.id) {
            // D must be aborted too!
            aborts_to_propagate.push(D.id);
            cascaded_aborts.push(D.id);
        }
    }

    // Wake up D so it can see it's been aborted
    let sender = resolver.waiting_transactions.write().await.remove(&C.id);
    sender.send(()).unwrap();  // D's thread wakes up
}
```

### Step 3: Transaction D Wakes Up and Sees It's Aborted

```rust
// Transaction D's thread wakes up from r.await
r.await.unwrap();

// Check abort status
let state = resolver.state.read().await;
if state.aborted_transactions.contains(&D.id) {
    return Err(Error::TransactionAborted(TransactionAbortReason::CascadingAbort));
}
```

### Final State After Cascading Abort
```rust
info_per_transaction: {
    C: TransactionInfo { id: C, num_dependencies: 0, dependents: {} },  // dependents cleared
    D: TransactionInfo { id: D, num_dependencies: 1, dependents: {} }   // still marked as waiting
}
resolved_transactions: {A, B}
aborted_transactions: {C, D}  // Both C and D are now aborted
waiting_transactions: {}      // D was removed and woken up
```

## Rollback of Pending Operations

### How Pending Writes Get Rolled Back

**Current Transaction Structure**: `coordinator/src/transaction.rs:33-38`

```rust
struct ParticipantRange {
    readset: HashSet<Bytes>,        // Keys read by transaction
    writeset: HashMap<Bytes, Bytes>, // Pending writes that need rollback
    deleteset: HashSet<Bytes>,      // Pending deletes that need rollback
    leader_sequence_number: u64,
}
```

**When Cascading Abort Happens**: The coordinator's existing `record_abort()` method already handles rollback:

```rust
// coordinator/src/transaction.rs:170-203 - existing abort handling
async fn record_abort(&mut self) -> Result<(), Error> {
    self.state = State::Aborted;

    // Notify all participant ranges to roll back prepared changes
    let mut abort_join_set = JoinSet::new();
    for range_id in self.participant_ranges.keys() {
        let range_client = self.range_client.clone();
        abort_join_set.spawn_on(async move {
            range_client.abort_transaction(transaction_info, &range_id).await
        }, &self.runtime);
    }

    // Record abort in transaction state store
    self.tx_state_store.try_abort_transaction(self.id).await.unwrap();

    while abort_join_set.join_next().await.is_some() {}
    Ok(())
}
```

**What Gets Rolled Back**:
- All pending writes in `participant_ranges[].writeset`
- All pending deletes in `participant_ranges[].deleteset`
- Any locks held by the transaction
- The transaction's prepare records in range servers

## Integration with Existing Error Handling

### Coordinator Integration

**File**: `coordinator/src/transaction.rs:170-203` (modify existing method)

Add resolver notification to existing abort handling:

```rust
async fn record_abort(&mut self) -> Result<(), Error> {
    self.state = State::Aborted;

    // NEW: Notify resolver of abort to trigger cascading
    if let Err(e) = self.resolver.register_aborted_transactions(vec![self.id]).await {
        error!("Failed to register abort with resolver: {:?}", e);
    }

    // Existing abort logic continues unchanged
    let mut abort_join_set = JoinSet::new();
    for range_id in self.participant_ranges.keys() {
        // ... existing range abort notifications
    }

    self.tx_state_store.try_abort_transaction(self.id).await.unwrap();
    while abort_join_set.join_next().await.is_some() {}
    Ok(())
}
```

This ensures that any transaction abort (whether from timeout, explicit abort, or commit failure) triggers cascading abort checking.

## Summary: The Complete Cascading Flow

1. **Transaction C fails** during group commit (range server error, state store failure, etc.)
2. **Group commit error handler** registers C for cascading abort instead of panicking
3. **Resolver processes cascading abort**:
   - Marks C as aborted
   - Finds C's dependents (D) using existing dependency graph
   - Recursively marks D as aborted
   - Wakes up D's waiting thread
4. **Transaction D wakes up** and checks its status, sees it's aborted
5. **D's coordinator calls record_abort()** which:
   - Notifies all ranges to rollback D's pending writes
   - Records abort in transaction state store
   - Cleans up D's locks and prepared state

The cascading abort leverages Sangria's existing dependency tracking (`dependents` field) and abort infrastructure (`record_abort()` method) to ensure that when ancestor transactions fail, all their dependent children are properly aborted and rolled back.