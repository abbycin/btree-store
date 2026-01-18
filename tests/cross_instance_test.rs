use btree_store::BTree;
use tempfile::TempDir;

#[test]
fn test_cross_instance_automatic_visibility() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("visibility_test.db");

    let bt1 = BTree::open(&db_path).unwrap();
    let bt2 = BTree::open(&db_path).unwrap();

    // 1. bt2 creates a bucket
    bt2.exec("shared", |_txn| Ok(())).unwrap();

    // 2. bt1 now automatically sees it because view auto-refreshes
    bt1.view("shared", |_txn| Ok(()))
        .expect("bt1 should automatically see bt2's commit");
}

#[test]
fn test_cross_instance_sequential_execution() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("sequential_test.db");

    let bt1 = BTree::open(&db_path).unwrap();
    bt1.exec("test", |_txn| Ok(())).unwrap();

    let bt2 = BTree::open(&db_path).unwrap();

    // bt1 commits change
    bt1.exec("test", |txn| {
        txn.put(b"k", b"v1").unwrap();
        Ok(())
    })
    .unwrap();

    // bt2 should see v1 and be able to update it without Conflict
    bt2.exec("test", |txn| {
        assert_eq!(txn.get(b"k").unwrap(), b"v1");
        txn.put(b"k", b"v2").unwrap();
        Ok(())
    })
    .expect("bt2 should auto-refresh and avoid Conflict");
}

#[test]
fn test_buckets_list_auto_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("buckets_refresh.db");

    let bt1 = BTree::open(&db_path).unwrap();
    let bt2 = BTree::open(&db_path).unwrap();

    bt2.exec("new_bucket", |_txn| Ok(())).unwrap();

    // bt1.buckets() should see the new bucket without manual refresh
    let buckets = bt1.buckets().unwrap();
    assert!(buckets.contains(&"new_bucket".to_string()));
}
