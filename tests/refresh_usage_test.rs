use btree_store::BTree;
use tempfile::TempDir;

#[test]
fn test_auto_refresh_removes_redundancy() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("refresh_example.db");

    // Initialize
    {
        let db = BTree::open(&db_path).unwrap();
        db.exec("data", |_txn| Ok(())).unwrap();
    }

    let handle_a = BTree::open(&db_path).unwrap();
    let handle_b = BTree::open(&db_path).unwrap();

    // 1. A commits a change.
    handle_a
        .exec("data", |txn| {
            txn.put(b"key", b"v1").unwrap();
            Ok(())
        })
        .unwrap();

    // 2. B starts exec. In the OLD design, this needed handle_b.refresh().
    // Now, it just works!
    handle_b
        .exec("data", |txn| {
            // Should see v1 because of auto-refresh inside exec
            assert_eq!(txn.get(b"key").unwrap(), b"v1");
            txn.put(b"key", b"v2").unwrap();
            Ok(())
        })
        .expect("B should auto-refresh and succeed without manual refresh()");

    // 3. Verify final state
    handle_a
        .view("data", |txn| {
            assert_eq!(txn.get(b"key").unwrap(), b"v2");
            Ok(())
        })
        .unwrap();
}
