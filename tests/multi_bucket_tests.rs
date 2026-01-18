use btree_store::BTree;
use tempfile::TempDir;

#[test]
fn test_exec_multi_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_exec_multi_basic.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Execute multi-bucket transaction
    tree.exec_multi(|multi| {
        multi.execute("bucket1", |txn| txn.put(b"key1", b"value1"))?;
        multi.execute("bucket2", |txn| txn.put(b"key2", b"value2"))?;
        Ok(())
    })
    .expect("Failed to execute multi-bucket transaction");

    // Verify both buckets were updated
    tree.view("bucket1", |txn| {
        let val = txn.get(b"key1").expect("Failed to get key1");
        assert_eq!(val, b"value1");
        Ok(())
    })
    .expect("Failed to view bucket1");

    tree.view("bucket2", |txn| {
        let val = txn.get(b"key2").expect("Failed to get key2");
        assert_eq!(val, b"value2");
        Ok(())
    })
    .expect("Failed to view bucket2");
}

#[test]
fn test_exec_multi_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_exec_multi_rollback.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Initial state
    tree.exec("bucket1", |txn| txn.put(b"initial", b"state"))
        .unwrap();

    // Execute multi-bucket transaction that fails
    let res: btree_store::Result<()> = tree.exec_multi(|multi| {
        multi.execute("bucket1", |txn| txn.put(b"key1", b"value1"))?;
        multi.execute("bucket2", |txn| txn.put(b"key2", b"value2"))?;
        Err(btree_store::Error::Internal)
    });

    assert!(res.is_err());

    // Verify bucket1 is still in initial state
    tree.view("bucket1", |txn| {
        let val = txn.get(b"initial").expect("Failed to get initial");
        assert_eq!(val, b"state");
        assert!(txn.get(b"key1").is_err());
        Ok(())
    })
    .expect("Failed to view bucket1");

    // Verify bucket2 doesn't exist (or at least doesn't have the key)
    let res2 = tree.view("bucket2", |txn| txn.get(b"key2"));
    assert!(res2.is_err());
}

#[test]
fn test_exec_multi_sequential_on_same_bucket() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_exec_multi_sequential.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    tree.exec_multi(|multi| {
        multi.execute("bucket1", |txn| txn.put(b"key1", b"value1"))?;
        // Second execute on same bucket should see first change
        multi.execute("bucket1", |txn| {
            let val = txn.get(b"key1").expect("Should see key1");
            assert_eq!(val, b"value1");
            txn.put(b"key1", b"value2")
        })?;
        Ok(())
    })
    .expect("Failed exec_multi");

    tree.view("bucket1", |txn| {
        let val = txn.get(b"key1").unwrap();
        assert_eq!(val, b"value2");
        Ok(())
    })
    .unwrap();
}
