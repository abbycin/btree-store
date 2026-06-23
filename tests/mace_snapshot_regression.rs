use btree_store::{BTree, Error};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

const BUCKET: &str = "data_interval_0";

fn interval_key(lo: u64) -> [u8; 8] {
    lo.to_be_bytes()
}

#[test]
fn noop_exec_multi_missing_key_touch_does_not_commit_catalog() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("noop_exec_multi_catalog_rewrite.db");
    let tree = BTree::open(&db_path).unwrap();

    tree.exec(BUCKET, |txn| txn.put(interval_key(1), b"file1"))
        .unwrap();

    let before = tree.current_seq();
    tree.exec_multi(|multi| {
        multi.exec(BUCKET, |txn| {
            assert_eq!(txn.get(interval_key(2)).unwrap_err(), Error::NotFound);
            Ok(())
        })?;
        Ok(())
    })
    .unwrap();
    let after = tree.current_seq();

    assert_eq!(
        after, before,
        "a successful exec_multi closure that only observes missing keys should not commit unchanged bucket roots"
    );
}

#[test]
fn exec_multi_same_bucket_write_then_noop_keeps_changed_root() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("same_bucket_write_then_noop.db");
    let tree = BTree::open(&db_path).unwrap();

    tree.exec_multi(|multi| {
        multi.exec(BUCKET, |txn| txn.put(interval_key(1), b"file1"))?;
        multi.exec(BUCKET, |txn| {
            assert_eq!(txn.get(interval_key(2)).unwrap_err(), Error::NotFound);
            Ok(())
        })?;
        Ok(())
    })
    .unwrap();

    tree.view(BUCKET, |txn| {
        assert_eq!(txn.get(interval_key(1)).unwrap(), b"file1".to_vec());
        Ok(())
    })
    .unwrap();
}

#[test]
fn exec_multi_empty_new_bucket_creates_catalog_entry() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("empty_new_bucket_catalog_entry.db");
    let tree = BTree::open(&db_path).unwrap();

    tree.exec_multi(|multi| {
        multi.exec("empty_bucket", |_| Ok(()))?;
        Ok(())
    })
    .unwrap();

    let buckets = tree.buckets().unwrap();
    assert!(
        buckets.iter().any(|bucket| bucket == "empty_bucket"),
        "a successful exec_multi touch of a missing bucket should create an empty bucket catalog entry"
    );
}

#[test]
fn exec_multi_existing_empty_bucket_noop_does_not_commit_catalog() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("existing_empty_bucket_noop.db");
    let tree = BTree::open(&db_path).unwrap();

    tree.exec("empty_bucket", |_| Ok(())).unwrap();

    let before = tree.current_seq();
    tree.exec_multi(|multi| {
        multi.exec("empty_bucket", |_| Ok(()))?;
        Ok(())
    })
    .unwrap();
    let after = tree.current_seq();

    assert_eq!(
        after, before,
        "a successful exec_multi no-op on an existing empty bucket should not rewrite catalog"
    );
}

#[test]
fn exec_existing_empty_bucket_noop_does_not_commit_catalog() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("existing_empty_bucket_single_noop.db");
    let tree = BTree::open(&db_path).unwrap();

    tree.exec("empty_bucket", |_| Ok(())).unwrap();

    let before = tree.current_seq();
    tree.exec("empty_bucket", |_| Ok(())).unwrap();
    let after = tree.current_seq();

    assert_eq!(
        after, before,
        "a successful exec no-op on an existing empty bucket should not rewrite catalog"
    );
}

#[test]
fn clone_during_inflight_writer_uses_consistent_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("clone_writer_snapshot_boundary.db");
    let tree = Arc::new(BTree::open(&db_path).unwrap());
    let key = interval_key(42);

    let barrier = Arc::new(Barrier::new(2));
    let writer = {
        let tree = tree.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            tree.exec(BUCKET, |txn| {
                txn.put(key, b"marker")?;
                barrier.wait();
                thread::sleep(Duration::from_millis(250));
                Ok(())
            })
            .unwrap();
        })
    };

    barrier.wait();
    let cloned = BTree::clone(tree.as_ref());
    writer.join().unwrap();

    cloned
        .exec_multi(|multi| {
            multi.exec(BUCKET, |txn| {
                assert_eq!(txn.get(key).unwrap(), b"marker".to_vec());
                txn.del(key)?;
                Ok(())
            })?;
            Ok(())
        })
        .unwrap();
}

#[test]
fn clone_inside_active_view_callback_does_not_deadlock() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("clone_active_view_snapshot_boundary.db");
    let stale_tree = Arc::new(BTree::open(&db_path).unwrap());
    let writer_tree = BTree::open(&db_path).unwrap();
    let key = interval_key(43);

    writer_tree
        .exec(BUCKET, |txn| {
            txn.put(key, b"marker")?;
            Ok(())
        })
        .unwrap();

    let (tx, rx) = mpsc::channel();
    {
        let tree = stale_tree.clone();
        let clone_target = tree.clone();
        thread::spawn(move || {
            let res = tree.view(BUCKET, |txn| {
                assert_eq!(txn.get(key).unwrap(), b"marker".to_vec());
                Ok(BTree::clone(clone_target.as_ref()))
            });
            tx.send(res).unwrap();
        });
    }

    let cloned = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("clone from inside an active view callback should not deadlock")
        .unwrap();

    cloned
        .exec_multi(|multi| {
            multi.exec(BUCKET, |txn| {
                assert_eq!(txn.get(key).unwrap(), b"marker".to_vec());
                txn.del(key)?;
                Ok(())
            })?;
            Ok(())
        })
        .unwrap();
}

#[test]
fn open_inside_active_view_callback_does_not_deadlock() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("open_active_view_snapshot_boundary.db");
    let stale_tree = Arc::new(BTree::open(&db_path).unwrap());
    let writer_tree = BTree::open(&db_path).unwrap();
    let key = interval_key(44);

    writer_tree
        .exec(BUCKET, |txn| {
            txn.put(key, b"marker")?;
            Ok(())
        })
        .unwrap();

    let (tx, rx) = mpsc::channel();
    {
        let tree = stale_tree.clone();
        let open_path = db_path.clone();
        thread::spawn(move || {
            let res = tree.view(BUCKET, |txn| {
                assert_eq!(txn.get(key).unwrap(), b"marker".to_vec());
                BTree::open(&open_path)
            });
            tx.send(res).unwrap();
        });
    }

    let reopened = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("same-path open from inside an active view callback should not deadlock")
        .unwrap();

    reopened
        .exec_multi(|multi| {
            multi.exec(BUCKET, |txn| {
                assert_eq!(txn.get(key).unwrap(), b"marker".to_vec());
                txn.del(key)?;
                Ok(())
            })?;
            Ok(())
        })
        .unwrap();
}
