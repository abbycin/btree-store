use btree_store::{BTree, Error};
use tempfile::TempDir;

#[test]
fn test_basic_put_get() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_basic_put_get.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Test basic put/get operations
    let key = b"test_key";
    let value = b"test_value";

    bucket.put(key, value).expect("Failed to put key-value");

    let retrieved = bucket.get(key).expect("Failed to get value");
    assert_eq!(retrieved, value);
}

#[test]
fn test_delete_existing_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_existing_key.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    let key = b"key_to_delete";
    let value = b"some_value";

    // Insert key-value pair
    bucket.put(key, value).expect("Failed to put key-value");

    // Verify key exists
    let retrieved = bucket.get(key).expect("Failed to get value");
    assert_eq!(retrieved, value);

    // Delete key
    bucket.del(key).expect("Failed to delete key");

    // Verify key has been deleted
    let retrieved = bucket.get(key);
    assert_eq!(retrieved, Err(Error::NotFound));
}

#[test]
fn test_delete_nonexistent_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_nonexistent_key.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    let key = b"nonexistent_key";

    // Attempt to delete a non-existent key, should return NotFound error
    let result = bucket.del(key);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), btree_store::Error::NotFound);
}

#[test]
fn test_multiple_puts_and_gets() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_multiple_puts_and_gets.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Insert multiple key-value pairs
    let test_data = vec![
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
        ("apple", "fruit"),
        ("carrot", "vegetable"),
    ];

    for (key, value) in &test_data {
        bucket
            .put(key.as_bytes(), value.as_bytes())
            .expect("Failed to put key-value");
    }

    // Verify all key-value pairs can be correctly retrieved
    for (key, value) in &test_data {
        let retrieved = bucket.get(key.as_bytes()).expect("Failed to get value");
        assert_eq!(retrieved, value.as_bytes());
    }
}

#[test]
fn test_overwrite_existing_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_overwrite_existing_key.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    let key = b"overwrite_key";
    let old_value = b"old_value";
    let new_value = b"new_value";

    // Insert initial key-value pair
    bucket
        .put(key, old_value)
        .expect("Failed to put initial value");
    let retrieved = bucket.get(key).expect("Failed to get initial value");
    assert_eq!(retrieved, old_value);

    // Overwrite key-value pair
    bucket.put(key, new_value).expect("Failed to put new value");
    let retrieved = bucket.get(key).expect("Failed to get new value");
    assert_eq!(retrieved, new_value);
}

#[test]
fn test_delete_complex_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_complex_scenario.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Insert multiple sets of key-value pairs
    let keys = vec!["key1", "key2", "key3", "key4", "key5"];
    for (i, key) in keys.iter().enumerate() {
        let value = format!("value{}", i).into_bytes();
        bucket
            .put(key.as_bytes(), &value)
            .expect("Failed to put key-value");
    }

    // Verify all keys exist
    for (i, key) in keys.iter().enumerate() {
        let value = format!("value{}", i).into_bytes();
        let retrieved = bucket.get(key.as_bytes()).expect("Failed to get value");
        assert_eq!(retrieved, value);
    }

    // Delete middle key
    bucket.del(b"key3").expect("Failed to delete key3");
    assert_eq!(bucket.get(b"key3"), Err(Error::NotFound));

    // Verify other keys still exist
    assert!(bucket.get(b"key1").is_ok());
    assert!(bucket.get(b"key2").is_ok());
    assert!(bucket.get(b"key4").is_ok());
    assert!(bucket.get(b"key5").is_ok());

    // Delete first and last keys
    bucket.del(b"key1").expect("Failed to delete key1");
    bucket.del(b"key5").expect("Failed to delete key5");

    assert_eq!(bucket.get(b"key1"), Err(Error::NotFound));
    assert_eq!(bucket.get(b"key5"), Err(Error::NotFound));

    assert!(bucket.get(b"key2").is_ok());
    assert!(bucket.get(b"key4").is_ok());
}

#[test]
fn test_empty_tree_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_empty_tree_operations.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Attempt to get a non-existent key on an empty tree
    let retrieved = bucket.get(b"nonexistent");
    assert_eq!(retrieved, Err(Error::NotFound));

    // Attempt to delete a non-existent key on an empty tree
    let result = bucket.del(b"nonexistent");
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), btree_store::Error::NotFound);
}

#[test]
fn test_large_values() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_large_values.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    let large_value = vec![42u8; 3 * 1024 * 1024];
    let key = b"large_value_key";

    bucket
        .put(key, &large_value)
        .expect("Failed to put large value");

    let retrieved = bucket.get(key).expect("Failed to get large value");
    assert_eq!(retrieved, large_value);
}

#[test]
fn test_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_persistence.db");

    {
        // First open, add data
        let tree = BTree::open(&db_path).expect("Failed to open BTree first time");
        let bucket = tree.new_bucket("default").expect("Failed to create bucket");

        bucket
            .put(b"persistent_key", b"persistent_value")
            .expect("Failed to put value");
        let retrieved = bucket.get(b"persistent_key").expect("Failed to get value");
        assert_eq!(retrieved, b"persistent_value");
        tree.commit().unwrap();
    }

    {
        // Second open, verify data still exists
        let tree = BTree::open(&db_path).expect("Failed to open BTree second time");
        let bucket = tree.get_bucket("default").expect("Failed to get bucket");

        let retrieved = bucket
            .get(b"persistent_key")
            .expect("Failed to get persistent value");
        assert_eq!(retrieved, b"persistent_value");
    }
}

#[test]
fn no_change() {
    let mut tmp_path = std::env::temp_dir();
    tmp_path.push("no_change.db");
    let path = tmp_path.as_path();
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    {
        let btree = BTree::open(path).unwrap();
        let b = btree.new_bucket("default").unwrap();
        b.put("foo", "bar").unwrap();
        btree.commit().unwrap();
    }

    let btree = BTree::open(path).unwrap();
    let b = btree.get_bucket("default").unwrap();
    let r = b.get("foo").unwrap();
    assert_eq!(r.as_slice(), "bar".as_bytes());
    btree.commit().unwrap();

    // test if commit before make changes
    let btree = BTree::open(path).unwrap();
    let b = btree.get_bucket("default").unwrap();
    let r = b.get("foo").unwrap();
    assert_eq!(r.as_slice(), "bar".as_bytes());
    btree.commit().unwrap();
}
