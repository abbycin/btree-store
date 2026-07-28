use btree_store::BTree;
use tempfile::TempDir;

#[test]
fn test_cross_instance_automatic_visibility() {
    assert_eq!(btree_store::FORMAT_VERSION, 1);
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("visibility_test.db");
    // Refresh installs one coherent root and allocator generation before the
    // second handle observes the first handle's commit.

    let bt1 = BTree::open(&db_path).unwrap();
    let bt2 = BTree::open(&db_path).unwrap();
    bt1.new_bucket("shared", false).unwrap();

    bt1.exec("shared", |txn| txn.put(b"key", b"old")).unwrap();
    bt2.view("shared", |txn| {
        assert_eq!(txn.get(b"key").unwrap(), b"old");
        Ok(())
    })
    .unwrap();

    bt1.exec("shared", |txn| txn.put(b"key", b"new")).unwrap();

    bt2.view("shared", |txn| {
        assert_eq!(txn.get(b"key").unwrap(), b"new");
        Ok(())
    })
    .expect("a stale physical-PID cache entry must miss after refresh");

    drop(bt1);
    drop(bt2);
    let reopened = BTree::open(&db_path).unwrap();
    reopened
        .view("shared", |txn| {
            assert_eq!(txn.get(b"key").unwrap(), b"new");
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_cross_instance_sequential_execution() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("sequential_test.db");

    let bt1 = BTree::open(&db_path).unwrap();
    bt1.new_bucket("test", false).unwrap();
    bt1.exec("test", |_txn| Ok(())).unwrap();

    let bt2 = BTree::open(&db_path).unwrap();

    // bt1 commits change
    bt1.exec("test", |txn| {
        txn.put(b"k", b"v1").unwrap();
        Ok(())
    })
    .unwrap();

    // bt2 should see v1 and update it after refreshing its shared snapshot
    bt2.exec("test", |txn| {
        assert_eq!(txn.get(b"k").unwrap(), b"v1");
        txn.put(b"k", b"v2").unwrap();
        Ok(())
    })
    .expect("bt2 should auto-refresh before updating");
}

#[test]
fn test_buckets_list_auto_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("buckets_refresh.db");

    let bt1 = BTree::open(&db_path).unwrap();
    let bt2 = BTree::open(&db_path).unwrap();

    // bt2 creates the bucket; bt1 must observe it without a manual refresh,
    // which is the cross-handle auto-refresh invariant this test witnesses.
    bt2.new_bucket("new_bucket", false).unwrap();

    let buckets = bt1.buckets().unwrap();
    assert!(buckets.contains(&"new_bucket".to_string()));
    assert_eq!(bt1.pending_pages(), (0, 0));

    drop(bt1);
    drop(bt2);
    let reopened = BTree::open(&db_path).unwrap();
    assert_eq!(reopened.buckets(), Ok(vec!["new_bucket".to_string()]));
}
