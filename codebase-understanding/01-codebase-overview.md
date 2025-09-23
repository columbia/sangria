# Sangria Codebase Overview - How It Actually Works

## What Happens When You Run a Transaction

Let's trace through what actually happens when you execute a simple transaction like:
```
START TRANSACTION
PUT key1 = "value1"
GET key2
COMMIT
```

Here's the **actual code flow** through Sangria's distributed system:

### Step 1: Client Starts Transaction

**File**: `frontend/src/main.rs` → **gRPC Handler**

```rust
// frontend/src/server.rs (gRPC service implementation)
async fn start_transaction(
    &self,
    request: Request<StartTransactionRequest>,
) -> Result<Response<StartTransactionResponse>, Status> {
    let keyspace_id = request.get_ref().keyspace_id;

    // Create new transaction with unique ID
    let transaction_info = Arc::new(TransactionInfo::new());
    let transaction = Transaction::new(
        transaction_info.clone(),
        self.range_client.clone(),
        self.range_assignment_oracle.clone(),
        // ... other dependencies
    );

    // Store transaction in coordinator
    self.transactions.write().await.insert(transaction_info.id, transaction);

    Ok(Response::new(StartTransactionResponse {
        transaction_id: transaction_info.id.to_string(),
    }))
}
```

**What actually happened**: Frontend creates a `Transaction` object and stores it in memory. This transaction will track all your reads/writes.

### Step 2: Client Executes PUT Operation

**File**: `coordinator/src/transaction.rs:152-159`

```rust
pub async fn put(&mut self, keyspace: &Keyspace, key: Bytes, val: Bytes) -> Result<(), Error> {
    self.check_still_running()?;  // Make sure transaction hasn't been aborted

    // Figure out which range owns this key
    let full_record_key = self.resolve_full_record_key(keyspace, key.clone()).await?;

    // Get or create participant range for this range
    let participant_range = self.get_participant_range(full_record_key.range_id);

    // Store the write locally (doesn't hit database yet!)
    participant_range.deleteset.remove(&key);  // Remove any pending deletes
    participant_range.writeset.insert(key, val.clone());  // Add to local writeset

    Ok(())
}
```

**What actually happened**: The PUT doesn't write to the database yet! It just stores `key1 = "value1"` in the transaction's local `writeset`. The actual write happens during commit.

### Step 3: Client Executes GET Operation

**File**: `coordinator/src/transaction.rs:103-150`

```rust
pub async fn get(&mut self, keyspace: &Keyspace, key: Bytes) -> Result<Option<Bytes>, Error> {
    self.check_still_running()?;

    let full_record_key = self.resolve_full_record_key(keyspace, key.clone()).await?;
    let participant_range = self.get_participant_range(full_record_key.range_id);

    // Read-your-writes: Check if we wrote to this key already
    if let Some(v) = participant_range.writeset.get(&key) {
        return Ok(Some(v.clone()));  // Return our own write
    }
    if participant_range.deleteset.contains(&key) {
        return Ok(None);  // We deleted it
    }

    // Actually read from the database
    let get_result = self.range_client.get(
        self.transaction_info.clone(),
        &full_record_key.range_id,
        vec![key.clone()],
    ).await.unwrap();

    // CRITICAL: Update transaction dependencies!
    self.dependencies.extend(get_result.dependencies);

    // Add to readset for conflict detection
    participant_range.readset.insert(key.clone());

    let val = get_result.vals.first().unwrap().clone();
    Ok(val)
}
```

**What actually happened**:
1. GET checks local writeset first (read-your-writes)
2. If not found locally, reads from range server
3. **Critically**: Range server returns not just the value, but also `dependencies` - other transactions this read depends on
4. These dependencies will determine commit order later!

### Step 4: Client Calls COMMIT - The 2PC Begins

**File**: `coordinator/src/transaction.rs:220-226`

```rust
pub async fn commit(&mut self, resolver_average_load: f64, num_open_clients: u32) -> Result<(), Error> {
    self.check_still_running()?;

    // --- PHASE 1: PREPARE ---
    self.state = State::Preparing;
    let mut prepare_join_set = JoinSet::new();

    // Send prepare to ALL ranges we touched
    for (range_id, info) in &self.participant_ranges {
        let has_reads = !info.readset.is_empty();
        let writes: Vec<Record> = info.writeset.iter().map(|(k, v)| Record {
            key: k.clone(), val: v.clone()
        }).collect();
        let deletes: Vec<Bytes> = info.deleteset.iter().cloned().collect();

        prepare_join_set.spawn_on(async move {
            range_client.prepare_transaction(
                transaction_info,
                &range_id,
                has_reads,
                &writes,      // Our pending writes
                &deletes,     // Our pending deletes
                resolver_average_load,
                num_open_clients,
            ).await
        }, &self.runtime);
    }
```

**What actually happened**: The coordinator sends "prepare" requests **in parallel** to every range server that this transaction touched. Each prepare request includes:
- All writes this transaction wants to make to that range
- All deletes this transaction wants to make
- Whether this transaction read from that range

### Step 5: Range Servers Respond to PREPARE

**File**: `rangeserver/src/range_manager/impl.rs` (prepare_transaction handler)

Each range server:
1. **Locks the keys** this transaction wants to write/delete
2. **Validates** the transaction can proceed (no conflicts, leader hasn't changed)
3. **Returns more dependencies** based on what it finds
4. **Stores prepare record** but doesn't actually apply the changes yet

### Step 6: Coordinator Makes Commit Decision

**File**: `coordinator/src/transaction.rs:290-434`

```rust
// After all prepare responses come back...
self.dependencies.extend(res.dependencies);  // Collect ALL dependencies

// Now decide how to commit based on strategy
match self.commit_strategy {
    CommitStrategy::Adaptive | CommitStrategy::Pipelined => {
        if !self.dependencies.is_empty() {
            // HAS DEPENDENCIES -> Use resolver pipeline
            info!("Delegating commit to resolver for transaction {}", self.id);
            let participants_info = self.participant_ranges.iter()
                .map(|(range_id, info)| ParticipantRangeInfo::new(*range_id, !info.writeset.is_empty()))
                .collect();

            self.resolver.commit(self.id, self.dependencies.clone(), participants_info).await?;
        } else {
            // NO DEPENDENCIES -> Direct commit (fast path)
            info!("Committing transaction {:?} without Resolver", self.id);

            // 1. Record commit decision in transaction state store
            self.tx_state_store.try_commit_transaction(self.id, 0).await.unwrap();
            self.state = State::Committed;

            // 2. Tell all ranges to apply the changes in parallel
            let mut commit_join_set = JoinSet::new();
            for (range_id, info) in self.participant_ranges.iter() {
                if !info.writeset.is_empty() {  // Only notify ranges with writes
                    commit_join_set.spawn_on(async move {
                        range_client.commit_transactions(vec![transaction_info.id], &range_id, 0).await
                    }, &self.runtime);
                }
            }
            while commit_join_set.join_next().await.is_some() {}
        }
    }
}
```

## The Two Commit Paths

### Fast Path (No Dependencies)
```
Transaction → Prepare → Decision → Commit → Done
```
**When**: Transaction doesn't conflict with any ongoing transactions
**Code Path**: Direct to range servers, bypasses resolver entirely

### Pipeline Path (Has Dependencies)
```
Transaction → Prepare → Decision → Resolver → Group Commit → Done
```
**When**: Transaction depends on other transactions (read something another transaction wrote)
**Code Path**: Goes through resolver for dependency ordering

## Key Data Structures - What They Actually Store

### Transaction Object (`coordinator/src/transaction.rs:40-54`)
```rust
pub struct Transaction {
    id: Uuid,                                    // Unique transaction ID
    state: State,                                // Running/Preparing/Committed/Aborted
    participant_ranges: HashMap<FullRangeId, ParticipantRange>,  // WHO we're talking to
    dependencies: HashSet<Uuid>,                 // WHO we're waiting for
    range_client: Arc<RangeClient>,             // HOW to talk to ranges
    resolver: Arc<dyn ResolverClient>,          // HOW to handle dependencies
    // ...
}

struct ParticipantRange {
    readset: HashSet<Bytes>,        // Keys we read (for conflict detection)
    writeset: HashMap<Bytes, Bytes>, // Key-value pairs we want to write
    deleteset: HashSet<Bytes>,      // Keys we want to delete
    leader_sequence_number: u64,    // To detect if range leader changed
}
```

**In English**: Each transaction keeps track of:
- **What it wants to do**: `writeset`, `deleteset`, `readset`
- **Where it needs to do it**: `participant_ranges` (which database ranges)
- **What it's waiting for**: `dependencies` (other transaction IDs)

### Resolver State (`resolver/src/core/resolver.rs:37-41`)
```rust
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,  // All pending transactions
    resolved_transactions: HashSet<Uuid>,                  // Successfully committed
}

pub struct TransactionInfo {
    id: Uuid,
    num_dependencies: u32,              // How many transactions I'm waiting for
    dependents: HashSet<Uuid>,          // Who is waiting for ME
    participant_ranges_info: Vec<ParticipantRangeInfo>,  // Where I need to commit
}
```

**In English**: The resolver maintains a **dependency graph**:
- "Transaction B is waiting for Transaction A"
- "When A finishes, wake up B"
- "B has writes to Range 1 and Range 3"

## Component Architecture - How They Actually Communicate

```
[Client]
   ↓ gRPC
[Frontend] ←→ [Coordinator]
   ↓ gRPC                ↓ gRPC
[RangeServer1]          [Resolver] ←→ [GroupCommit]
[RangeServer2]             ↓ gRPC
[RangeServer3]          [TxStateStore]
```

### Communication Patterns in Code:

**Client → Frontend**: Standard gRPC calls
```rust
// Proto definition
service Frontend {
    rpc StartTransaction(StartTransactionRequest) returns (StartTransactionResponse);
    rpc Put(PutRequest) returns (PutResponse);
    rpc Get(GetRequest) returns (GetResponse);
    rpc Commit(CommitRequest) returns (CommitResponse);
}
```

**Coordinator → RangeServer**: Parallel async calls
```rust
// Multiple ranges contacted simultaneously
for (range_id, info) in &self.participant_ranges {
    prepare_join_set.spawn_on(async move {
        range_client.prepare_transaction(/* ... */).await
    }, &self.runtime);
}
```

**Coordinator → Resolver**: Dependency submission
```rust
self.resolver.commit(
    self.id,                    // Who I am
    self.dependencies.clone(),  // Who I depend on
    participants_info          // What I want to do
).await?;
```

## The Complete Transaction Flow

1. **Client sends PUT/GET operations**
   - Stored locally in transaction's `writeset`/`readset`
   - Dependencies collected from range servers during GETs

2. **Client sends COMMIT**
   - Coordinator enters PREPARE phase
   - Parallel prepare requests sent to all participant ranges

3. **Range servers validate and lock**
   - Lock requested keys
   - Return additional dependencies
   - Store prepare records

4. **Coordinator decides commit strategy**
   - **No dependencies**: Fast path (direct commit)
   - **Has dependencies**: Pipeline path (through resolver)

5. **Pipeline processing** (if dependencies exist)
   - Transaction waits in resolver for dependencies to clear
   - When ready, added to group commit batch
   - Batch committed to all ranges simultaneously

6. **Range servers apply changes**
   - Apply prepared writes/deletes to storage
   - Release locks
   - Update dependency tracking

This is how a simple PUT/GET/COMMIT transaction actually flows through the entire distributed system!