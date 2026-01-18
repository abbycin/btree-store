use btree_store::BTree;
use tempfile::TempDir;

#[test]
fn test_super_large_value() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("large.db");

    let tree = BTree::open(&db_path).unwrap();

    // 3MB Value
    let size = 3 * 1024 * 1024;
    let large_value = vec![0x55u8; size];

    tree.exec("large_data", |txn| {
        txn.put(b"big_key", &large_value).unwrap();
        Ok(())
    })
    .unwrap();

    tree.view("large_data", |txn| {
        let retrieved = txn.get(b"big_key").unwrap();
        assert_eq!(retrieved.len(), size);
        assert_eq!(retrieved, large_value);
        Ok(())
    })
    .unwrap();
}
