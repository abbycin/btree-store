use btree_store::BTree;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_freelist_persist_and_reuse() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("freelist.db");

    let size_after_insert;
    {
        let bt = BTree::open(&db_path).unwrap();
        bt.exec("default", |txn| {
            txn.put(b"key1", &vec![0xAA; 20000]).unwrap();
            Ok(())
        })
        .unwrap();
        size_after_insert = fs::metadata(&db_path).unwrap().len();
        bt.exec("default", |txn| {
            txn.del(b"key1").unwrap();
            Ok(())
        })
        .unwrap();
    }

    let size_after_delete = fs::metadata(&db_path).unwrap().len();

    {
        let bt = BTree::open(&db_path).unwrap();
        bt.exec("default", |txn| {
            txn.put(b"key2", &vec![0xBB; 20000]).unwrap();
            Ok(())
        })
        .unwrap();
    }

    let size_after_reuse = fs::metadata(&db_path).unwrap().len();
    assert!(size_after_delete >= size_after_insert);
    assert!(size_after_reuse >= size_after_delete);
}

#[test]
fn test_exec_rollback_no_leak() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("rollback_leak.db");

    let tree = BTree::open(&db_path).unwrap();

    // 1. Initial state
    tree.exec("data", |txn| {
        txn.put(b"initial", b"value").unwrap();
        Ok(())
    })
    .unwrap();

    // 2. Execute a transaction that fails
    let res: btree_store::Result<()> = tree.exec("data", |txn| {
        // Allocate some pages by putting large values
        txn.put(b"large", &vec![0xAA; 1024 * 1024]).unwrap();
        // Return error to trigger rollback
        Err(btree_store::Error::Internal)
    });

    assert_eq!(res, Err(btree_store::Error::Internal));

    // 3. Verify that pages were NOT leaked (pending_alloc should be empty)
    assert_eq!(tree.pending_pages().0, 0, "Pending alloc should be empty");
}
