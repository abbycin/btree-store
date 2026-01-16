use btree_store::{BTree, Error};
use tempfile::TempDir;

#[test]
fn test_cross_instance_visibility() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("visibility_test.db");

    // 1. Thread 1 opens the database and creates a bucket
    let bt1 = BTree::open(&db_path).unwrap();
    let _ = bt1.new_bucket("shared").unwrap();
    bt1.commit().unwrap();

    // 2. Thread 2 opens the SAME database independently
    let bt2 = BTree::open(&db_path).unwrap();

    // 3. Thread 2 puts some data and commits
    let bucket2 = bt2.get_bucket("shared").unwrap();
    bucket2.put(b"key2", b"val2").unwrap();
    bt2.commit().unwrap();

    // 4. Thread 1 MUST refresh or commit to see changes in SI.
    // In our current implementation, a successful commit or re-opening refreshes.
    // Let's re-open bt1 (simulating a new transaction)
    let bt1_new = BTree::open(&db_path).unwrap();
    let bucket1 = bt1_new.get_bucket("shared").unwrap();
    let val = bucket1
        .get(b"key2")
        .expect("New transaction should see committed data");
    assert_eq!(val, b"val2");
}

#[test]
fn test_cross_instance_conflict_detection() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("conflict_test.db");

    let bt1 = BTree::open(&db_path).unwrap();
    let _ = bt1.new_bucket("test").unwrap();
    bt1.commit().unwrap();

    let bt2 = BTree::open(&db_path).unwrap();

    // T1 modifies
    let bucket1 = bt1.get_bucket("test").unwrap();
    bucket1.put(b"t1", b"v1").unwrap();

    // T2 modifies
    let bucket2 = bt2.get_bucket("test").unwrap();
    bucket2.put(b"t2", b"v2").unwrap();

    // T1 commits first
    bt1.commit().unwrap();

    // T2 commits second -> Should fail with Conflict
    let result = bt2.commit();
    assert!(matches!(result, Err(Error::Conflict)));
}

#[test]
fn test_refresh_visibility() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("refresh_vis_test.db");

    let bt1 = BTree::open(&db_path).unwrap();
    let bt2 = BTree::open(&db_path).unwrap();

    // 1. T2 creates a new bucket and commits
    let _ = bt2.new_bucket("bucket_from_t2").unwrap();
    bt2.commit().unwrap();

    // 2. T1 cannot see the new bucket yet because its snapshot is old
    assert!(matches!(
        bt1.get_bucket("bucket_from_t2"),
        Err(Error::NotFound)
    ));

    // 3. T1 calls refresh() to advance its snapshot
    bt1.refresh().unwrap();

    // 4. Now T1 can see the bucket
    assert!(bt1.get_bucket("bucket_from_t2").is_ok());
}

#[test]
fn test_conflict_resolution_with_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("conflict_res_test.db");

    let bt1 = BTree::open(&db_path).unwrap();
    let _ = bt1.new_bucket("data").unwrap();
    bt1.commit().unwrap();

    let bt2 = BTree::open(&db_path).unwrap();

    // T1 modifies something
    let b1 = bt1.get_bucket("data").unwrap();
    b1.put(b"key", b"val_from_t1").unwrap();

    // T2 modifies the same or different thing and commits first
    let b2 = bt2.get_bucket("data").unwrap();
    b2.put(b"other", b"val_from_t2").unwrap();
    bt2.commit().unwrap();

    // T1 tries to commit and fails
    assert!(matches!(bt1.commit(), Err(Error::Conflict)));

    // T1 resolves conflict: refresh, then retry
    bt1.refresh().unwrap();

    let b1_retry = bt1.get_bucket("data").unwrap();
    // T1 can now see T2's data
    assert_eq!(b1_retry.get(b"other").unwrap(), b"val_from_t2");

    // T1 re-applies its change
    b1_retry.put(b"key", b"val_from_t1").unwrap();
    bt1.commit().unwrap();

    // Final check: both changes exist
    let bt_final = BTree::open(&db_path).unwrap();
    let b_final = bt_final.get_bucket("data").unwrap();
    assert_eq!(b_final.get(b"key").unwrap(), b"val_from_t1");
    assert_eq!(b_final.get(b"other").unwrap(), b"val_from_t2");
}
