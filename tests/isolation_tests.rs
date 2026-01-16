use btree_store::BTree;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.pending", path));
}

#[test]
fn test_iterator_isolation_blocks_writer() {
    let path = "test_isolation.db";
    cleanup(path);

    let tree = Arc::new(BTree::open(path).unwrap());
    let bucket_name = "iso_bucket";

    // 1. Setup initial data
    {
        let bucket = tree.new_bucket(bucket_name).unwrap();
        for i in 0..100 {
            bucket
                .put(
                    format!("key_{:03}", i).as_bytes(),
                    format!("val_{:03}", i).as_bytes(),
                )
                .unwrap();
        }
        tree.commit().unwrap();
    }

    let tree_clone_reader = tree.clone();
    let tree_clone_writer = tree.clone();

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();

    // 2. Reader Thread
    let reader_handle = thread::spawn(move || {
        let bucket = tree_clone_reader.get_bucket(bucket_name).unwrap();
        let mut iter = bucket.iter().unwrap();

        // Read first item to ensure lock is acquired
        let _ = iter.next().unwrap();

        // Signal writer to start
        barrier_clone.wait();

        // Sleep to hold the lock for a significant time
        // This forces the writer to wait
        thread::sleep(Duration::from_millis(500));

        // Continue reading remaining items
        let mut count = 1;
        while let Some(_) = iter.next() {
            count += 1;
        }

        // iter is dropped here, releasing the lock
        count
    });

    // 3. Writer Thread
    let writer_handle = thread::spawn(move || {
        let bucket = tree_clone_writer.get_bucket(bucket_name).unwrap();

        // Wait for reader to start and grab lock
        barrier.wait();

        // Give reader a tiny bit of time to enter sleep
        thread::sleep(Duration::from_millis(10));

        let start = Instant::now();

        // This PUT should be BLOCKED because Reader holds the global read lock.
        // It should only proceed after Reader drops the iterator (~500ms).
        bucket.put(b"new_key", b"new_value").unwrap();
        tree_clone_writer.commit().unwrap();

        start.elapsed()
    });

    let reader_count = reader_handle.join().unwrap();
    let writer_duration = writer_handle.join().unwrap();

    // 4. Verifications

    // Consistency: Reader must see exactly the original 100 items.
    // If isolation failed, it might crash or see weird state.
    assert_eq!(reader_count, 100, "Reader should see all original items");

    // Isolation: Writer must have been blocked.
    // We expect duration > 500ms (reader sleep) - 10ms (writer wait) ~= 490ms.
    // We use 200ms as a safe threshold to rule out OS scheduling noise.
    assert!(
        writer_duration > Duration::from_millis(200),
        "Writer should be blocked by Reader's lock. Actual duration: {:?}",
        writer_duration
    );

    // Verify write eventually succeeded
    let bucket = tree.get_bucket(bucket_name).unwrap();
    assert_eq!(bucket.get(b"new_key").unwrap(), b"new_value".to_vec());

    cleanup(path);
}
