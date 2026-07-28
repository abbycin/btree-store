#[test]
#[cfg(not(windows))]
fn reproduce_btree_corruption() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use btree_store::BTree;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("reproduce_manifest.db");

    let btree = Arc::new(BTree::open(path)?);

    // Initialize multiple buckets.
    let buckets = ["bucket_1", "bucket_2", "bucket_3", "bucket_4", "bucket_5"];
    for bucket in buckets {
        btree.new_bucket(bucket, false)?;
    }

    let mut handles = Vec::new();

    // Simulate concurrent independent commits that exercise shared catalog state.
    for thread_id in 0..4 {
        let btree_clone = btree.clone();
        let handle = thread::spawn(move || {
            for i in 0..10_000 {
                let bucket = buckets[i % buckets.len()];
                let key = format!("thread_{thread_id}_key_{i}");
                let value = vec![thread_id as u8; 3840];

                btree_clone
                    .exec(bucket, |txn| txn.put(key.as_bytes(), &value))
                    .unwrap_or_else(|error| {
                        panic!(
                            "REPRODUCED: BTree failure at thread {thread_id}, iteration {i}: {error:?}"
                        )
                    });
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}
