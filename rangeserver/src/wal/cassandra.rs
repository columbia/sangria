use std::collections::VecDeque;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use super::*;
use crate::wal::Wal;
use async_trait::async_trait;
use colored::Colorize;
use flatbuffers::FlatBufferBuilder;
use scylla::batch::Batch;
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use scylla::Session;
use scylla::SessionBuilder;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::time::{sleep, Duration, Instant};
use tracing::info;
use uuid::Uuid;

// Buffer configuration constants
// const BUFFER_CAPACITY: usize = 32;
// const FLUSH_INTERVAL: Duration = Duration::from_micros(200);

#[derive(Debug)]
struct BufferedEntry {
    entry_type: Entry,
    data: Vec<u8>,
    completion_sender: oneshot::Sender<Result<(), Error>>,
}

struct BufferState {
    entries: VecDeque<BufferedEntry>,
    last_flush: Instant,
}

pub struct CassandraWal {
    session: Arc<Session>,
    wal_id: Uuid,
    state: Arc<RwLock<State>>,
    buffer: Arc<Mutex<BufferState>>,
    flush_task_handle: Option<tokio::task::JoinHandle<()>>,
    bg_runtime: tokio::runtime::Handle,
    buffer_capacity: usize,
    flush_interval: Duration,
}

enum State {
    NotSynced,
    Synced(LogState),
}

struct LogState {
    first_offset: Option<i64>,
    next_offset: i64,
    flatbuf_builder: FlatBufferBuilder<'static>,
}

fn scylla_query_error_to_wal_error(qe: QueryError) -> Error {
    match qe {
        QueryError::TimeoutError | QueryError::DbError(DbError::WriteTimeout { .. }, _) => {
            Error::Timeout
        }
        _ => Error::Internal(Arc::new(qe)),
    }
}

static SYNC_WAL_QUERY: &str = r#"
  SELECT first_offset, next_offset FROM atomix.wal
    WHERE wal_id = ? and offset = ?;
"#;

static UPDATE_FIRST_OFFSET_QUERY: &str = r#"
    UPDATE atomix.wal SET first_offset = ? 
      WHERE wal_id = ? and offset = ? 
      IF first_offset = ? 
"#;

static TRIM_LOG_QUERY: &str = r#"
    DELETE FROM atomix.wal
      WHERE wal_id = ? and offset < ?
"#;

static INIT_METADATA_QUERY: &str = r#"
    INSERT INTO atomix.wal (wal_id, offset, content, first_offset, next_offset, write_id)
    VALUES (?, ?, ?, ?, ?, ?)
"#;

static UPDATE_METADATA_QUERY: &str = r#"
    UPDATE atomix.wal SET next_offset = ?, first_offset = ?
      WHERE wal_id = ? and offset = ? 
      IF next_offset = ? 
"#;

static APPEND_ENTRY_QUERY: &str = r#"
    INSERT INTO atomix.wal (wal_id, offset, content, write_id)
    VALUES (?, ?, ?, ?)
"#;

static RETRIEVE_LOG_ENTRY: &str = r#"
    SELECT * FROM atomix.wal
      WHERE wal_id = ? and offset = ? and write_id = ?
      ALLOW FILTERING
"#;

static METADATA_OFFSET: i64 = i64::MAX;

impl CassandraWal {
    pub async fn new(
        known_node: String,
        buffer_capacity: usize,
        flush_interval: Duration,
        wal_id: Uuid,
        bg_runtime: tokio::runtime::Handle,
    ) -> CassandraWal {
        let session = SessionBuilder::new()
            .known_node(known_node)
            .build()
            .await
            .unwrap();

        let buffer = Arc::new(Mutex::new(BufferState {
            entries: VecDeque::new(),
            last_flush: Instant::now(),
        }));

        let mut wal = CassandraWal {
            session: Arc::new(session),
            wal_id,
            state: Arc::new(RwLock::new(State::NotSynced)),
            buffer,
            flush_task_handle: None,
            bg_runtime,
            buffer_capacity,
            flush_interval,
        };
        // Start the background flush task
        wal.start_flush_task().await;
        wal
    }

    async fn start_flush_task(&mut self) {
        let session = self.session.clone();
        let wal_id = self.wal_id;
        let buffer = self.buffer.clone();
        let state = self.state.clone();
        let bg_runtime = self.bg_runtime.clone();
        let buffer_capacity = self.buffer_capacity;
        let flush_interval = self.flush_interval;
        let handle = bg_runtime.spawn(async move {
            Self::flush_task(
                session,
                wal_id,
                buffer,
                state,
                buffer_capacity,
                flush_interval,
            )
            .await;
        });

        self.flush_task_handle = Some(handle);
    }

    async fn flush_task(
        session: Arc<Session>,
        wal_id: Uuid,
        buffer: Arc<Mutex<BufferState>>,
        state: Arc<RwLock<State>>,
        buffer_capacity: usize,
        flush_interval: Duration,
    ) {
        loop {
            sleep(flush_interval).await;

            let should_flush = {
                let buffer_clone = buffer.clone();
                let buffer_guard = buffer_clone.lock().await;
                buffer_guard.entries.len() >= buffer_capacity
                    || buffer_guard.last_flush.elapsed() >= flush_interval
            };

            if should_flush {
                Self::flush_buffer(session.clone(), wal_id, buffer.clone(), state.clone()).await;
            }
        }
    }

    async fn flush_buffer(
        session: Arc<Session>,
        wal_id: Uuid,
        buffer: Arc<Mutex<BufferState>>,
        state: Arc<RwLock<State>>,
    ) {
        let entries_to_flush = {
            let mut buffer_guard = buffer.lock().await;
            if buffer_guard.entries.is_empty() {
                return;
            }

            let entries: Vec<_> = buffer_guard.entries.drain(..).collect();
            buffer_guard.last_flush = Instant::now();
            entries
        };

        if entries_to_flush.is_empty() {
            return;
        }

        let flush_result =
            Self::flush_entries_to_database(&session, wal_id, &state, &entries_to_flush).await;

        // Notify all waiting callers
        for entry in entries_to_flush {
            let _ = entry.completion_sender.send(Ok(()));
        }
    }

    async fn flush_entries_to_database(
        session: &Arc<Session>,
        wal_id: Uuid,
        state: &Arc<RwLock<State>>,
        entries: &Vec<BufferedEntry>,
    ) -> Result<(), Error> {
        let mut state_guard = state.write().await;
        match state_guard.deref_mut() {
            State::NotSynced => Err(Error::NotSynced),
            State::Synced(log_state) => {
                let mut entry_batch: Batch = Default::default();
                entry_batch.set_serial_consistency(Some(SerialConsistency::Serial));

                let mut metadata_batch: Batch = Default::default();
                metadata_batch.set_serial_consistency(Some(SerialConsistency::Serial));

                let mut entry_batch_args: Vec<_> = Vec::new();
                let mut metadata_batch_args: Vec<_> = Vec::new();

                let mut write_ids = Vec::new();
                let mut offsets = Vec::new();

                // Prepare all entries
                info!(
                    "{}",
                    format!("Number of WAL entries flushed: {}", entries.len()).purple()
                );

                for entry in entries {
                    let bytes = log_state.flatbuf_builder.create_vector(&entry.data);
                    let fb_root = LogEntry::create(
                        &mut log_state.flatbuf_builder,
                        &LogEntryArgs {
                            entry: entry.entry_type,
                            bytes: Some(bytes),
                        },
                    );
                    log_state.flatbuf_builder.finish(fb_root, None);
                    let content = Vec::from(log_state.flatbuf_builder.finished_data());
                    log_state.flatbuf_builder.reset();

                    let write_id = Uuid::new_v4();
                    let offset: i64 = log_state.next_offset;
                    log_state.next_offset += 1;
                    log_state.first_offset = Some(log_state.first_offset.unwrap_or(offset));

                    entry_batch.append_statement(Query::new(APPEND_ENTRY_QUERY));
                    entry_batch_args.push((wal_id, offset, content, write_id));
                    write_ids.push(write_id);
                    offsets.push(offset);
                }

                // Update metadata
                metadata_batch.append_statement(Query::new(UPDATE_METADATA_QUERY));
                metadata_batch_args.push((
                    log_state.next_offset,
                    log_state.first_offset,
                    wal_id,
                    METADATA_OFFSET,
                    offsets[0], // Use first offset for conditional check
                ));

                // Execute batches
                session
                    .batch(&entry_batch, entry_batch_args)
                    .await
                    .map_err(scylla_query_error_to_wal_error)?;

                session
                    .batch(&metadata_batch, metadata_batch_args)
                    .await
                    .map_err(scylla_query_error_to_wal_error)?;

                // Verify writes (checking first entry as a sample)
                if !write_ids.is_empty() {
                    let mut post_write_check_query = Query::new(RETRIEVE_LOG_ENTRY);
                    post_write_check_query.set_serial_consistency(Some(SerialConsistency::Serial));
                    let rows = session
                        .query(post_write_check_query, (wal_id, offsets[0], write_ids[0]))
                        .await
                        .map_err(scylla_query_error_to_wal_error)?
                        .rows;
                    if rows.is_none() || rows.unwrap().len() != 1 {
                        *state_guard = State::NotSynced;
                        return Err(Error::NotSynced);
                    }
                }

                Ok(())
            }
        }
    }

    async fn append_entry(
        &self,
        entry_type: Entry,
        entry: &[u8],
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (sender, receiver) = oneshot::channel();

        info!("{}", format!("Appending entry: {:?}", entry_type).purple());
        // Add entry to buffer
        {
            let mut buffer_guard = self.buffer.lock().await;
            buffer_guard.entries.push_back(BufferedEntry {
                entry_type,
                data: entry.to_vec(),
                completion_sender: sender,
            });

            // Trigger immediate flush if buffer is full
            if buffer_guard.entries.len() >= self.buffer_capacity {
                drop(buffer_guard);
                let bg_runtime = self.bg_runtime.clone();
                let session = self.session.clone();
                let wal_id = self.wal_id;
                let buffer = self.buffer.clone();
                let state = self.state.clone();
                bg_runtime.spawn(async move {
                    Self::flush_buffer(session, wal_id, buffer, state).await;
                });
            }
        }

        Ok(receiver)
    }
}

impl Drop for CassandraWal {
    fn drop(&mut self) {
        if let Some(handle) = self.flush_task_handle.take() {
            handle.abort();
        }
    }
}

#[async_trait]
impl Wal for CassandraWal {
    async fn sync(&self) -> Result<(), Error> {
        let mut state = self.state.write().await;
        (*state) = State::NotSynced;
        let mut query = Query::new(SYNC_WAL_QUERY);
        query.set_serial_consistency(Some(SerialConsistency::Serial));
        let rows = self
            .session
            .query(query, (self.wal_id, METADATA_OFFSET))
            .await
            .map_err(scylla_query_error_to_wal_error)?
            .rows;

        let res = match rows {
            None => Err(Error::NotSynced),
            Some(mut rows) => {
                if rows.len() == 0 {
                    // Log doesn't exist, initialize it
                    let mut init_query = Query::new(INIT_METADATA_QUERY);
                    init_query.set_serial_consistency(Some(SerialConsistency::Serial));
                    let write_id = Uuid::new_v4();

                    // Insert initial metadata row
                    self.session
                        .query(
                            init_query,
                            (
                                self.wal_id,
                                METADATA_OFFSET,
                                Vec::<u8>::new(),
                                None::<i64>,
                                0 as i64,
                                write_id,
                            ),
                        )
                        .await
                        .map_err(scylla_query_error_to_wal_error)?;

                    // Set initial state
                    (*state) = State::Synced(LogState {
                        first_offset: None,
                        next_offset: 0,
                        flatbuf_builder: FlatBufferBuilder::new(),
                    });
                    Ok(())
                } else if rows.len() > 1 {
                    panic!("found multiple rows for the same WAL metadata!");
                } else {
                    let row = rows.pop().unwrap();
                    let (first_offset, next_offset) =
                        row.into_typed::<(Option<i64>, i64)>().unwrap();
                    (*state) = State::Synced(LogState {
                        first_offset,
                        next_offset,
                        flatbuf_builder: FlatBufferBuilder::new(),
                    });
                    Ok(())
                }
            }
        };
        res
    }

    async fn first_offset(&self) -> Result<Option<u64>, Error> {
        let state = self.state.read().await;
        match state.deref() {
            State::NotSynced => Err(Error::NotSynced),
            State::Synced(log) => Ok(log.first_offset.map(|o| o as u64)),
        }
    }

    async fn next_offset(&self) -> Result<u64, Error> {
        let state = self.state.read().await;
        match state.deref() {
            State::NotSynced => Err(Error::NotSynced),
            State::Synced(log) => Ok(log.next_offset as u64),
        }
    }

    async fn trim_before_offset(&self, offset: u64) -> Result<(), Error> {
        let first_offset = self.first_offset().await?;
        let first_offset = match first_offset {
            None => return Ok(()),
            Some(first_offset) => {
                if offset < first_offset {
                    return Ok(());
                };
                first_offset
            }
        };
        let mut batch: Batch = Default::default();
        batch.set_serial_consistency(Some(SerialConsistency::Serial));
        batch.append_statement(Query::new(UPDATE_FIRST_OFFSET_QUERY));
        batch.append_statement(Query::new(TRIM_LOG_QUERY));

        let batch_args = (
            (
                offset as i64,
                self.wal_id,
                METADATA_OFFSET,
                first_offset as i64,
            ),
            (self.wal_id, offset as i64),
        );
        self.session
            .batch(&batch, batch_args)
            .await
            .map_err(scylla_query_error_to_wal_error)?;

        // Unfortunately, the scylladb driver does not tell us whether the
        // conditional check passed or not, so as a hack we just re-sync here.
        self.sync().await
    }

    async fn append_prepare(
        &self,
        entry: PrepareRequest<'_>,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        self.append_entry(Entry::Prepare, entry._tab.buf()).await
    }

    async fn append_abort(
        &self,
        entry: AbortRequest<'_>,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        self.append_entry(Entry::Abort, entry._tab.buf()).await
    }

    async fn append_commit(
        &self,
        entry: CommitRequest<'_>,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        self.append_entry(Entry::Commit, entry._tab.buf()).await
    }

    // fn iterator(&self) -> InMemIterator {
    //     todo!()
    // }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use common::util;

    impl CassandraWal {
        async fn create_test() -> CassandraWal {
            let cassandra = CassandraWal::new("127.0.0.1:9042".to_string(), Uuid::new_v4()).await;
            let mut query =
                Query::new("INSERT INTO atomix.wal (wal_id, offset, next_offset) VALUES (?, ?, ?)");
            query.set_serial_consistency(Some(SerialConsistency::Serial));
            cassandra
                .session
                .query(query, (cassandra.wal_id, METADATA_OFFSET, 0_i64))
                .await
                .unwrap();
            cassandra
        }

        async fn cleanup(&self) {
            let mut query = Query::new("DELETE FROM atomix.wal WHERE wal_id = ?");
            query.set_serial_consistency(Some(SerialConsistency::Serial));
            let _ = self.session.query(query, (self.wal_id,)).await;
        }
    }

    #[tokio::test]
    async fn initial_sync() {
        let cassandra = CassandraWal::create_test().await;
        cassandra.sync().await.unwrap();
        let first_offset = cassandra.first_offset().await.unwrap();
        assert!(first_offset.is_none());
        let next_offset = cassandra.next_offset().await.unwrap();
        assert!(next_offset == 0);
        cassandra.cleanup().await;
    }

    #[tokio::test]
    async fn basic_append_and_trim() {
        let cassandra = CassandraWal::create_test().await;
        cassandra.sync().await.unwrap();
        let mut fbb = FlatBufferBuilder::new();
        let transaction_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(Uuid::new_v4()),
        ));
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(Uuid::new_v4()),
        ));
        let fbb_root = AbortRequest::create(
            &mut fbb,
            &AbortRequestArgs {
                request_id,
                transaction_id,
                range_id: None,
            },
        );
        fbb.finish(fbb_root, None);
        let abort_record_bytes = fbb.finished_data();
        let abort_record = flatbuffers::root::<AbortRequest>(abort_record_bytes).unwrap();

        cassandra.append_abort(abort_record).await.unwrap();
        let first_offset = cassandra.first_offset().await.unwrap().unwrap();
        assert!(first_offset == 0);
        let next_offset = cassandra.next_offset().await.unwrap();
        assert!(next_offset == 1);

        cassandra.append_abort(abort_record).await.unwrap();
        let first_offset = cassandra.first_offset().await.unwrap().unwrap();
        assert!(first_offset == 0);
        let next_offset = cassandra.next_offset().await.unwrap();
        assert!(next_offset == 2);

        cassandra.trim_before_offset(1).await.unwrap();
        let first_offset = cassandra.first_offset().await.unwrap().unwrap();
        assert!(first_offset == 1);
        let next_offset = cassandra.next_offset().await.unwrap();
        assert!(next_offset == 2);

        cassandra.cleanup().await;
    }
}
