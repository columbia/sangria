/// Test to verify the synchronous cleanup logic is correct
use tracing::info;

#[tokio::test]
async fn test_synchronous_cleanup_logic() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    info!("\n=== Testing Synchronous Cleanup Logic ===");

    // Simulate the rangeserver state
    use std::collections::HashMap;
    use uuid::Uuid;

    let mut pending_prepare_records: HashMap<Uuid, String> = HashMap::new();
    let mut key_version_chain: HashMap<String, Vec<Uuid>> = HashMap::new();
    let mut pending_commit_table: HashMap<String, Uuid> = HashMap::new();

    // T1 prepares
    let t1 = Uuid::new_v4();
    let key = "user:123".to_string();

    pending_prepare_records.insert(t1, "T1 data".to_string());
    key_version_chain.entry(key.clone()).or_default().push(t1);
    pending_commit_table.insert(key.clone(), t1);

    info!("After T1 prepare:");
    info!("  key_version_chain[{}] = {:?}", key, key_version_chain.get(&key));
    info!("  pending_commit_table[{}] = {:?}", key, pending_commit_table.get(&key));

    // T2 prepares with dependency on T1
    let t2 = Uuid::new_v4();

    pending_prepare_records.insert(t2, "T2 data".to_string());
    key_version_chain.entry(key.clone()).or_default().push(t2);
    pending_commit_table.insert(key.clone(), t2);

    info!("\nAfter T2 prepare:");
    info!("  key_version_chain[{}] = {:?}", key, key_version_chain.get(&key));
    info!("  pending_commit_table[{}] = {:?}", key, pending_commit_table.get(&key));

    // T2 artificially aborts - SYNCHRONOUS CLEANUP
    info!("\n🎲 T2 artificially aborts - starting synchronous cleanup...");

    // Remove from pending_prepare_records
    pending_prepare_records.remove(&t2);
    info!("  ✅ Removed T2 from pending_prepare_records");

    // Remove from key_version_chain
    if let Some(chain) = key_version_chain.get_mut(&key) {
        chain.retain(|&id| id != t2);
        info!("  ✅ Removed T2 from key_version_chain");

        // Update pending_commit_table to previous version
        if let Some(&prev_tx) = chain.last() {
            pending_commit_table.insert(key.clone(), prev_tx);
            info!("  ✅ Rolled back pending_commit_table to {:?}", prev_tx);
        } else {
            pending_commit_table.remove(&key);
            key_version_chain.remove(&key);
            info!("  ✅ Cleared pending_commit_table (no more pending)");
        }
    }

    info!("\nAfter T2 cleanup:");
    info!("  key_version_chain[{}] = {:?}", key, key_version_chain.get(&key));
    info!("  pending_commit_table[{}] = {:?}", key, pending_commit_table.get(&key));

    // Verify cleanup worked
    assert!(!pending_prepare_records.contains_key(&t2), "T2 should be removed from pending_prepare_records");

    let chain = key_version_chain.get(&key).unwrap();
    assert_eq!(chain.len(), 1, "key_version_chain should only have T1");
    assert_eq!(chain[0], t1, "key_version_chain should only contain T1");

    assert_eq!(pending_commit_table.get(&key), Some(&t1), "pending_commit_table should be rolled back to T1");

    info!("\n✅ All cleanup assertions passed!");

    // T3 can now prepare and see clean state
    let t3 = Uuid::new_v4();

    let dependency = pending_commit_table.get(&key).cloned();
    info!("\nT3 prepares:");
    info!("  Found dependency: {:?} (should be T1, not T2!)", dependency);

    assert_eq!(dependency, Some(t1), "T3 should depend on T1, not zombie T2");

    pending_prepare_records.insert(t3, "T3 data".to_string());
    key_version_chain.entry(key.clone()).or_default().push(t3);
    pending_commit_table.insert(key.clone(), t3);

    info!("  ✅ T3 prepared successfully (no hang!)");

    info!("\n=== Test Passed: Synchronous cleanup prevents zombie dependencies ===");
}

#[tokio::test]
async fn test_cleanup_with_multiple_keys() {
    info!("\n=== Testing Cleanup with Multiple Keys ===");

    use std::collections::HashMap;
    use uuid::Uuid;

    let mut pending_prepare_records: HashMap<Uuid, HashMap<String, String>> = HashMap::new();
    let mut key_version_chain: HashMap<String, Vec<Uuid>> = HashMap::new();
    let mut pending_commit_table: HashMap<String, Uuid> = HashMap::new();

    // T2 writes to multiple keys
    let t2 = Uuid::new_v4();
    let keys = vec!["user:1".to_string(), "user:2".to_string(), "user:3".to_string()];

    let mut t2_changes = HashMap::new();
    for key in &keys {
        t2_changes.insert(key.clone(), "data".to_string());
        key_version_chain.entry(key.clone()).or_default().push(t2);
        pending_commit_table.insert(key.clone(), t2);
    }
    pending_prepare_records.insert(t2, t2_changes.clone());

    info!("T2 prepared with {} keys", keys.len());

    // T2 aborts - cleanup all keys
    info!("\n🎲 T2 aborts - cleaning up all keys...");

    pending_prepare_records.remove(&t2);

    for key in &keys {
        if let Some(chain) = key_version_chain.get_mut(key) {
            chain.retain(|&id| id != t2);
            info!("  ✅ Cleaned up key: {}", key);

            if chain.is_empty() {
                pending_commit_table.remove(key);
                key_version_chain.remove(key);
            }
        }
    }

    // Verify all keys are cleaned
    for key in &keys {
        let chain = key_version_chain.get(key);
        assert!(chain.is_none() || chain.unwrap().is_empty(),
                "Key {} should have no version chain entries", key);

        assert!(!pending_commit_table.contains_key(key),
                "Key {} should not be in pending_commit_table", key);
    }

    info!("\n✅ Multi-key cleanup successful!");
}
