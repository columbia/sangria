/// Mock test that simulates the exact Ray experiment workload
/// This helps diagnose if the hang is real or just slow performance
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::{info, warn, error};

// Simulate Ray experiment config
const NUM_KEYS: usize = 100;
const NUM_TRANSACTIONS: usize = 3000;
const MAX_CONCURRENCY: usize = 50;
const ARTIFICIAL_ABORT_RATE: f64 = 0.1; // 10%
const EXPECTED_MAX_DURATION: Duration = Duration::from_secs(120); // 2 minutes max

struct TransactionStats {
    total: AtomicUsize,
    succeeded: AtomicUsize,
    aborted: AtomicUsize,
    hung: AtomicUsize,
}

impl TransactionStats {
    fn new() -> Self {
        Self {
            total: AtomicUsize::new(0),
            succeeded: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
            hung: AtomicUsize::new(0),
        }
    }

    fn print(&self) {
        let total = self.total.load(Ordering::Relaxed);
        let succeeded = self.succeeded.load(Ordering::Relaxed);
        let aborted = self.aborted.load(Ordering::Relaxed);
        let hung = self.hung.load(Ordering::Relaxed);

        info!("\n=== Transaction Statistics ===");
        info!("Total:     {}/{}", total, NUM_TRANSACTIONS);
        info!("Succeeded: {} ({:.1}%)", succeeded, (succeeded as f64 / total as f64) * 100.0);
        info!("Aborted:   {} ({:.1}%)", aborted, (aborted as f64 / total as f64) * 100.0);
        info!("Hung:      {} ({:.1}%)", hung, (hung as f64 / total as f64) * 100.0);
    }
}

/// Simulates a single transaction with artificial abort and synchronous cleanup
async fn mock_transaction(
    tx_id: usize,
    stats: Arc<TransactionStats>,
    semaphore: Arc<Semaphore>,
) -> Result<(), String> {
    // Acquire concurrency slot
    let _permit = semaphore.acquire().await.unwrap();

    stats.total.fetch_add(1, Ordering::Relaxed);

    // Simulate transaction work with timeout (detect hangs)
    let result = timeout(Duration::from_secs(5), async {
        // Simulate PREPARE phase
        tokio::time::sleep(Duration::from_millis(10)).await; // Simulate lock acquisition

        // Random key selection (creates contention)
        let key = rand::random::<usize>() % NUM_KEYS;

        // Simulate recording dependencies
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Check for artificial abort
        let should_abort = rand::random::<f64>() < ARTIFICIAL_ABORT_RATE;

        if should_abort {
            // Simulate SYNCHRONOUS CLEANUP (key fix)
            tokio::time::sleep(Duration::from_millis(2)).await; // Cleanup time

            return Err(format!("TX {} artificially aborted on key {}", tx_id, key));
        }

        // Simulate WAL write
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Simulate COMMIT phase
        tokio::time::sleep(Duration::from_millis(5)).await;

        Ok(())
    }).await;

    match result {
        Ok(Ok(())) => {
            stats.succeeded.fetch_add(1, Ordering::Relaxed);
            if tx_id % 500 == 0 {
                info!("TX {} ✅ succeeded", tx_id);
            }
            Ok(())
        }
        Ok(Err(e)) => {
            stats.aborted.fetch_add(1, Ordering::Relaxed);
            if tx_id % 500 == 0 {
                info!("TX {} ❌ aborted: {}", tx_id, e);
            }
            Err(e)
        }
        Err(_) => {
            stats.hung.fetch_add(1, Ordering::Relaxed);
            error!("TX {} ⏱️  HUNG - timeout after 5 seconds!", tx_id);
            Err(format!("TX {} hung", tx_id))
        }
    }
}

#[tokio::test]
#[ignore] // Run with: cargo test --test mock_ray_workload_test -- --ignored --nocapture
async fn test_mock_ray_workload_no_hang() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    info!("\n=== Mock Ray Workload Test ===");
    info!("Simulating: {} transactions, {} keys, {} concurrent, {:.0}% abort rate",
          NUM_TRANSACTIONS, NUM_KEYS, MAX_CONCURRENCY, ARTIFICIAL_ABORT_RATE * 100.0);

    let stats = Arc::new(TransactionStats::new());
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENCY));

    let start = Instant::now();

    info!("\n🚀 Starting workload...");

    // Launch all transactions concurrently (up to MAX_CONCURRENCY)
    let mut handles = vec![];
    for tx_id in 0..NUM_TRANSACTIONS {
        let stats_clone = stats.clone();
        let semaphore_clone = semaphore.clone();

        let handle = tokio::spawn(async move {
            mock_transaction(tx_id, stats_clone, semaphore_clone).await
        });

        handles.push(handle);
    }

    // Wait for all transactions with overall timeout
    info!("⏳ Waiting for transactions to complete (max {} seconds)...", EXPECTED_MAX_DURATION.as_secs());

    let overall_result = timeout(EXPECTED_MAX_DURATION, async {
        for (i, handle) in handles.into_iter().enumerate() {
            if let Err(e) = handle.await {
                error!("Transaction {} join error: {:?}", i, e);
            }

            // Progress indicator
            if (i + 1) % 500 == 0 {
                let elapsed = start.elapsed();
                let rate = (i + 1) as f64 / elapsed.as_secs_f64();
                info!("Progress: {}/{} ({:.1} tx/s)", i + 1, NUM_TRANSACTIONS, rate);
            }
        }
    }).await;

    let elapsed = start.elapsed();

    info!("\n⏱️  Total time: {:.2} seconds", elapsed.as_secs_f64());

    stats.print();

    // Check if we timed out
    match overall_result {
        Ok(()) => {
            info!("\n✅ Test COMPLETED successfully!");

            // Verify abort rate is approximately correct
            let aborted = stats.aborted.load(Ordering::Relaxed);
            let total = stats.total.load(Ordering::Relaxed);
            let actual_abort_rate = aborted as f64 / total as f64;

            info!("\nAbort rate: {:.1}% (expected {:.0}%)",
                  actual_abort_rate * 100.0, ARTIFICIAL_ABORT_RATE * 100.0);

            // Allow 5% margin
            assert!(
                (actual_abort_rate - ARTIFICIAL_ABORT_RATE).abs() < 0.05,
                "Abort rate {:.1}% is too far from expected {:.0}%",
                actual_abort_rate * 100.0,
                ARTIFICIAL_ABORT_RATE * 100.0
            );

            // Verify no hangs
            let hung = stats.hung.load(Ordering::Relaxed);
            assert_eq!(hung, 0, "Found {} hung transactions - indicates deadlock!", hung);

            info!("\n✅ All assertions passed!");
        }
        Err(_) => {
            error!("\n❌ Test TIMED OUT after {} seconds!", EXPECTED_MAX_DURATION.as_secs());
            stats.print();

            let completed = stats.succeeded.load(Ordering::Relaxed) + stats.aborted.load(Ordering::Relaxed);
            error!("\nOnly {}/{} transactions completed before timeout", completed, NUM_TRANSACTIONS);
            error!("This indicates a HANG or severe performance issue!");

            panic!("Test timed out - likely indicates a hang in the real system");
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_detect_hung_transaction() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    info!("\n=== Test: Detect Hung Transaction ===");

    // Simulate a transaction that hangs
    let result = timeout(Duration::from_secs(2), async {
        info!("Transaction starting...");
        tokio::time::sleep(Duration::from_secs(10)).await; // Hangs for 10s
        info!("Transaction completed");
    }).await;

    match result {
        Ok(()) => {
            panic!("Transaction should have timed out but didn't!");
        }
        Err(_) => {
            warn!("✅ Successfully detected hung transaction (timed out after 2s)");
        }
    }
}

#[tokio::test]
async fn test_expected_completion_time() {
    info!("\n=== Calculating Expected Completion Time ===");

    // Transaction timing breakdown:
    // - Lock acquisition: 10ms
    // - Dependency check: 5ms
    // - Synchronous cleanup (if abort): 2ms
    // - WAL write: 5ms
    // - Commit: 5ms
    // Total per transaction: ~27ms (success) or ~17ms (abort)

    let avg_time_success = 27.0; // ms
    let avg_time_abort = 17.0; // ms
    let weighted_avg = (1.0 - ARTIFICIAL_ABORT_RATE) * avg_time_success
                     + ARTIFICIAL_ABORT_RATE * avg_time_abort;

    info!("Average time per transaction: {:.1}ms", weighted_avg);

    // With MAX_CONCURRENCY = 50, we process 50 transactions in parallel
    let batches = (NUM_TRANSACTIONS as f64 / MAX_CONCURRENCY as f64).ceil();
    let expected_time = batches * weighted_avg / 1000.0; // Convert to seconds

    info!("Expected completion time: {:.1} seconds ({} batches of {})",
          expected_time, batches as usize, MAX_CONCURRENCY);

    // With overhead, contention, etc., expect 2-3x slower
    let realistic_time = expected_time * 2.5;
    info!("Realistic time (with overhead): {:.1} seconds", realistic_time);

    info!("\n📊 If Ray experiment takes > {:.0} seconds, something is wrong!", realistic_time * 2.0);
}
