use btree_store::{BTree, Error};
use tempfile::TempDir;

#[test]
fn test_basic_put_get() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_basic_put_get.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Test basic put/get operations
    let key = b"test_key";
    let value = b"test_value";

    tree.exec("default", |txn| {
        txn.put(key, value).expect("Failed to put key-value");
        Ok(())
    })
    .expect("Failed to execute transaction");

    tree.view("default", |txn| {
        let retrieved = txn.get(key).expect("Failed to get value");
        assert_eq!(retrieved, value);
        Ok(())
    })
    .expect("Failed to view bucket");
}

#[test]
fn test_delete_existing_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_existing_key.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    let key = b"key_to_delete";
    let value = b"some_value";

    // Insert key-value pair
    tree.exec("default", |txn| {
        txn.put(key, value).expect("Failed to put key-value");
        Ok(())
    })
    .expect("Failed to put");

    // Verify key exists and delete it
    tree.exec("default", |txn| {
        let retrieved = txn.get(key).expect("Failed to get value");
        assert_eq!(retrieved, value);
        txn.del(key).expect("Failed to delete key");
        Ok(())
    })
    .expect("Failed to del");

    // Verify key has been deleted
    tree.view("default", |txn| {
        let retrieved = txn.get(key);
        assert_eq!(retrieved, Err(Error::NotFound));
        Ok(())
    })
    .expect("Failed to view");
}

#[test]
fn test_delete_nonexistent_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_nonexistent_key.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    let key = b"nonexistent_key";

    // Attempt to delete a non-existent key, should return NotFound error
    let result = tree.exec("default", |txn| txn.del(key));
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), btree_store::Error::NotFound);
}

#[test]
fn test_multiple_puts_and_gets() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_multiple_puts_and_gets.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Insert multiple key-value pairs
    let test_data = vec![
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
        ("apple", "fruit"),
        ("carrot", "vegetable"),
    ];

    tree.exec("default", |txn| {
        for (key, value) in &test_data {
            txn.put(key.as_bytes(), value.as_bytes())
                .expect("Failed to put key-value");
        }
        Ok(())
    })
    .expect("Failed to put multiple");

    // Verify all key-value pairs can be correctly retrieved
    tree.view("default", |txn| {
        for (key, value) in &test_data {
            let retrieved = txn.get(key.as_bytes()).expect("Failed to get value");
            assert_eq!(retrieved, value.as_bytes());
        }
        Ok(())
    })
    .expect("Failed to view multiple");
}

#[test]
fn test_overwrite_existing_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_overwrite_existing_key.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    let key = b"overwrite_key";
    let old_value = b"old_value";
    let new_value = b"new_value";

    // Insert initial key-value pair
    tree.exec("default", |txn| {
        txn.put(key, old_value)
            .expect("Failed to put initial value");
        Ok(())
    })
    .unwrap();

    tree.view("default", |txn| {
        let retrieved = txn.get(key).expect("Failed to get initial value");
        assert_eq!(retrieved, old_value);
        Ok(())
    })
    .unwrap();

    // Overwrite key-value pair
    tree.exec("default", |txn| {
        txn.put(key, new_value).expect("Failed to put new value");
        Ok(())
    })
    .unwrap();

    tree.view("default", |txn| {
        let retrieved = txn.get(key).expect("Failed to get new value");
        assert_eq!(retrieved, new_value);
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_delete_complex_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_complex_scenario.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Insert multiple sets of key-value pairs
    let keys = vec!["key1", "key2", "key3", "key4", "key5"];
    tree.exec("default", |txn| {
        for (i, key) in keys.iter().enumerate() {
            let value = format!("value{}", i).into_bytes();
            txn.put(key.as_bytes(), &value)
                .expect("Failed to put key-value");
        }
        Ok(())
    })
    .unwrap();

    // Verify all keys exist
    tree.view("default", |txn| {
        for (i, key) in keys.iter().enumerate() {
            let value = format!("value{}", i).into_bytes();
            let retrieved = txn.get(key.as_bytes()).expect("Failed to get value");
            assert_eq!(retrieved, value);
        }
        Ok(())
    })
    .unwrap();

    // Delete middle key
    tree.exec("default", |txn| {
        txn.del(b"key3").expect("Failed to delete key3");
        Ok(())
    })
    .unwrap();

    tree.view("default", |txn| {
        assert_eq!(txn.get(b"key3"), Err(Error::NotFound));
        // Verify other keys still exist
        assert!(txn.get(b"key1").is_ok());
        assert!(txn.get(b"key2").is_ok());
        assert!(txn.get(b"key4").is_ok());
        assert!(txn.get(b"key5").is_ok());
        Ok(())
    })
    .unwrap();

    // Delete first and last keys
    tree.exec("default", |txn| {
        txn.del(b"key1").expect("Failed to delete key1");
        txn.del(b"key5").expect("Failed to delete key5");
        Ok(())
    })
    .unwrap();

    tree.view("default", |txn| {
        assert_eq!(txn.get(b"key1"), Err(Error::NotFound));
        assert_eq!(txn.get(b"key5"), Err(Error::NotFound));
        assert!(txn.get(b"key2").is_ok());
        assert!(txn.get(b"key4").is_ok());
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_empty_tree_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_empty_tree_operations.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // Attempt to get a non-existent key on an empty tree
    tree.view("default", |txn| {
        let retrieved = txn.get(b"nonexistent");
        assert_eq!(retrieved, Err(Error::NotFound));
        Ok(())
    })
    .unwrap_err(); // Should fail because "default" bucket doesn't exist yet

    // Attempt to delete a non-existent key on an empty tree
    let result = tree.exec("default", |txn| txn.del(b"nonexistent"));
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), btree_store::Error::NotFound);
}

#[test]
fn test_large_values() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_large_values.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    let large_value = vec![42u8; 3 * 1024 * 1024];
    let key = b"large_value_key";

    tree.exec("default", |txn| {
        txn.put(key, &large_value)
            .expect("Failed to put large value");
        Ok(())
    })
    .expect("Failed to put large value");

    tree.view("default", |txn| {
        let retrieved = txn.get(key).expect("Failed to get large value");
        assert_eq!(retrieved, large_value);
        Ok(())
    })
    .expect("Failed to get large value");
}

#[test]
fn test_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_persistence.db");

    {
        // First open, add data
        let tree = BTree::open(&db_path).expect("Failed to open BTree first time");
        tree.exec("default", |txn| {
            txn.put(b"persistent_key", b"persistent_value")
                .expect("Failed to put value");
            Ok(())
        })
        .expect("Failed to exec");
    }

    {
        // Second open, verify data still exists
        let tree = BTree::open(&db_path).expect("Failed to open BTree second time");
        tree.view("default", |txn| {
            let retrieved = txn
                .get(b"persistent_key")
                .expect("Failed to get persistent value");
            assert_eq!(retrieved, b"persistent_value");
            Ok(())
        })
        .expect("Failed to view persistence");
    }
}

#[test]
fn no_change() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("no_change.db");

    {
        let btree = BTree::open(&path).unwrap();
        btree
            .exec("default", |txn| {
                txn.put("foo", "bar").unwrap();
                Ok(())
            })
            .unwrap();
    }

    let btree = BTree::open(&path).unwrap();
    btree
        .view("default", |txn| {
            let r = txn.get("foo").unwrap();
            assert_eq!(r.as_slice(), "bar".as_bytes());
            Ok(())
        })
        .unwrap();

    // test if commit before make changes
    let btree = BTree::open(&path).unwrap();
    btree
        .view("default", |txn| {
            let r = txn.get("foo").unwrap();
            assert_eq!(r.as_slice(), "bar".as_bytes());
            Ok(())
        })
        .unwrap();
    btree.commit().unwrap();
}
