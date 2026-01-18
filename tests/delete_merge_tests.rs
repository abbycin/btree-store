use btree_store::{BTree, Error};
use tempfile::TempDir;

#[test]
fn test_simple_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_simple_delete.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Add a key-value pair
    tree.exec("default", |txn| {
        txn.put(b"key1", b"value1").expect("Failed to put key1");
        Ok(())
    })
    .unwrap();

    // Verify key exists
    tree.view("default", |txn| {
        assert_eq!(txn.get(b"key1").unwrap(), b"value1");
        Ok(())
    })
    .unwrap();

    // Delete key
    tree.exec("default", |txn| {
        txn.del(b"key1").expect("Failed to delete key1");
        Ok(())
    })
    .unwrap();

    // Verify key no longer exists
    tree.view("default", |txn| {
        assert_eq!(txn.get(b"key1"), Err(Error::NotFound));
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_delete_until_node_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_until_node_empty.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Add several keys
    tree.exec("default", |txn| {
        txn.put(b"key1", b"value1").unwrap();
        txn.put(b"key2", b"value2").unwrap();
        txn.put(b"key3", b"value3").unwrap();
        Ok(())
    })
    .unwrap();

    // Delete all keys
    tree.exec("default", |txn| {
        txn.del(b"key1").unwrap();
        txn.del(b"key2").unwrap();
        txn.del(b"key3").unwrap();
        Ok(())
    })
    .unwrap();

    // Verify none exists
    tree.view("default", |txn| {
        assert_eq!(txn.get(b"key1"), Err(Error::NotFound));
        assert_eq!(txn.get(b"key2"), Err(Error::NotFound));
        assert_eq!(txn.get(b"key3"), Err(Error::NotFound));
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_delete_from_root() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_from_root.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Add a key
    tree.exec("default", |txn| {
        txn.put(b"only_key", b"only_value").unwrap();
        Ok(())
    })
    .unwrap();

    // Delete root key
    tree.exec("default", |txn| {
        txn.del(b"only_key").unwrap();
        Ok(())
    })
    .unwrap();

    // Re-add verify tree works
    tree.exec("default", |txn| {
        txn.put(b"new_key", b"new_value").unwrap();
        assert_eq!(txn.get(b"new_key").unwrap(), b"new_value");
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_delete_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_nonexistent.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Attempt to delete a non-existent key
    let result = tree.exec("default", |txn| txn.del(b"nonexistent_key"));
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), btree_store::Error::NotFound));
}

#[test]
fn test_sequence_of_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_sequence_of_deletes.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Add 10 keys
    tree.exec("default", |txn| {
        for i in 0..10 {
            txn.put(
                format!("key{}", i).as_bytes(),
                format!("value{}", i).as_bytes(),
            )
            .unwrap();
        }
        Ok(())
    })
    .unwrap();

    // Delete half
    tree.exec("default", |txn| {
        for i in 0..5 {
            txn.del(format!("key{}", i).as_bytes()).unwrap();
        }
        Ok(())
    })
    .unwrap();

    // Verify remaining
    tree.view("default", |txn| {
        for i in 0..5 {
            assert_eq!(
                txn.get(format!("key{}", i).as_bytes()),
                Err(Error::NotFound)
            );
        }
        for i in 5..10 {
            assert_eq!(
                txn.get(format!("key{}", i).as_bytes()).unwrap(),
                format!("value{}", i).as_bytes()
            );
        }
        Ok(())
    })
    .unwrap();
}
