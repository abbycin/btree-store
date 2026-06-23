use btree_store::BTree;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn test_reopen_same_path_shares_writer_lock() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("reuse.db");

    let handle_a = Arc::new(BTree::open(&db_path).unwrap());
    let handle_b = Arc::new(BTree::open(&db_path).unwrap());

    handle_a
        .exec("reuse", |txn| {
            txn.put(b"k0", b"v0")?;
            Ok(())
        })
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));

    let reader = {
        let h = handle_a.clone();
        let b = barrier.clone();
        thread::spawn(move || {
            h.view("reuse", |txn| {
                let _ = txn.get(b"k0").unwrap();
                b.wait();
                thread::sleep(Duration::from_millis(500));
                Ok(())
            })
            .unwrap();
        })
    };

    let writer = {
        let h = handle_b.clone();
        let b = barrier.clone();
        thread::spawn(move || {
            b.wait();
            thread::sleep(Duration::from_millis(10));
            let start = Instant::now();
            h.exec("reuse", |txn| {
                txn.put(b"k1", b"v1")?;
                Ok(())
            })
            .unwrap();
            start.elapsed()
        })
    };

    reader.join().unwrap();
    let blocked = writer.join().unwrap();
    assert!(
        blocked > Duration::from_millis(200),
        "writer should be blocked by reader on reused handle; actual: {:?}",
        blocked
    );

    handle_a
        .view("reuse", |txn| {
            assert_eq!(txn.get(b"k1").unwrap(), b"v1".to_vec());
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_reopen_after_commit_allows_empty_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("reuse_commit.db");

    let handle_a = BTree::open(&db_path).unwrap();
    handle_a
        .exec("reuse", |txn| {
            txn.put(b"k", b"v1")?;
            Ok(())
        })
        .unwrap();

    let handle_b = BTree::open(&db_path).unwrap();

    // Reopened handle must align to latest snapshot; empty commit should be a no-op.
    handle_b
        .commit()
        .expect("empty commit on reopened handle should succeed");

    handle_b
        .view("reuse", |txn| {
            assert_eq!(txn.get(b"k").unwrap(), b"v1".to_vec());
            Ok(())
        })
        .unwrap();
}
