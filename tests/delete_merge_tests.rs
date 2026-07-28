use btree_store::{BTree, Error};
use tempfile::TempDir;

#[test]
fn test_delete_until_node_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_until_node_empty.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

    let keys: Vec<_> = (0..256u32)
        .map(|i| format!("key-{i:04}").into_bytes())
        .collect();

    // Build enough leaves to force branch traversal and branch-child removal.
    tree.exec("default", |txn| {
        for key in &keys {
            txn.put(key, vec![0x5a; 128]).unwrap();
        }
        Ok(())
    })
    .unwrap();

    // Right-to-left deletion repeatedly removes the right-most child before root collapse.
    tree.exec("default", |txn| {
        for key in keys.iter().rev() {
            txn.del(key).unwrap();
        }
        Ok(())
    })
    .unwrap();

    tree.view("default", |txn| {
        for index in [0, 127, 255] {
            assert_eq!(txn.get(&keys[index]), Err(Error::KeyNotFound));
        }
        let mut iter = txn.iter();
        let mut key = Vec::new();
        let mut value = Vec::new();
        assert!(!iter.next_ref(&mut key, &mut value));
        Ok(())
    })
    .unwrap();

    tree.exec("default", |txn| txn.put(b"after-collapse", b"value"))
        .unwrap();
    tree.view("default", |txn| {
        assert_eq!(txn.get(b"after-collapse").unwrap(), b"value");
        Ok(())
    })
    .unwrap();
    assert_eq!(tree.pending_pages(), (0, 0));
}

#[test]
fn test_delete_from_root() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_from_root.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

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
fn test_sequence_of_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_sequence_of_deletes.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

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
                Err(Error::KeyNotFound)
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
