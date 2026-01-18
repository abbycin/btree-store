use btree_store::BTree;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn test_view_isolation_blocks_writer() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("isolation.db");
    let tree = BTree::open(&db_path).unwrap();
    let bucket_name = "iso_bucket";

    // 1. Setup initial data
    tree.exec(bucket_name, |txn| {
        for i in 0..100 {
            txn.put(
                format!("key_{:03}", i).as_bytes(),
                format!("val_{:03}", i).as_bytes(),
            )
            .unwrap();
        }
        Ok(())
    })
    .unwrap();

    let tree_clone_reader = tree.clone();
    let tree_clone_writer = tree.clone();

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();

    // 2. Reader Thread
    let reader_handle = thread::spawn(move || {
        tree_clone_reader
            .view(bucket_name, |txn| {
                // Read first item
                let _ = txn.get("key_000").unwrap();

                // Signal writer to start
                barrier_clone.wait();

                // Sleep to hold the lock for a significant time
                // This forces the writer to wait because view holds writer_lock.read()
                thread::sleep(Duration::from_millis(500));

                // Continue reading to verify isolation
                let mut count = 0;
                let mut iter = txn.iter();
                let mut key_buf = Vec::new();
                let mut val_buf = Vec::new();
                while iter.next_ref(&mut key_buf, &mut val_buf) {
                    count += 1;
                }
                Ok(count)
            })
            .unwrap()
    });

    // 3. Writer Thread
    let writer_handle = thread::spawn(move || {
        // Wait for reader to start and grab lock
        barrier.wait();

        // Give reader a tiny bit of time to enter sleep
        thread::sleep(Duration::from_millis(10));

        let start = Instant::now();

        // This EXEC should be BLOCKED because View holds the global read lock.
        tree_clone_writer
            .exec(bucket_name, |txn| {
                txn.put(b"new_key", b"new_value").unwrap();
                Ok(())
            })
            .unwrap();

        start.elapsed()
    });

    let reader_count = reader_handle.join().unwrap();
    let writer_duration = writer_handle.join().unwrap();

    // 4. Verifications

    // Consistency: Reader must see exactly the original 100 items.
    assert_eq!(reader_count, 100, "Reader should see all original items");

    // Isolation: Writer must have been blocked.
    assert!(
        writer_duration > Duration::from_millis(200),
        "Writer should be blocked by Reader's lock. Actual duration: {:?}",
        writer_duration
    );

    // Verify write eventually succeeded
    tree.view(bucket_name, |txn| {
        assert_eq!(txn.get(b"new_key").unwrap(), b"new_value".to_vec());
        Ok(())
    })
    .unwrap();
}
