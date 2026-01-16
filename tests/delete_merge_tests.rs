use btree_store::{BTree, Error};
use tempfile::TempDir;

#[test]
fn test_simple_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_simple_delete.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Add a key-value pair
    bucket.put(b"key1", b"value1").expect("Failed to put key1");

    // Verify key exists
    assert_eq!(bucket.get(b"key1").unwrap(), b"value1");

    // Delete key
    bucket.del(b"key1").expect("Failed to delete key1");

    // Verify key no longer exists
    assert_eq!(bucket.get(b"key1"), Err(Error::NotFound));
}

#[test]
fn test_delete_until_node_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_until_node_empty.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Add several keys, which may be in the same leaf node
    bucket.put(b"key1", b"value1").expect("Failed to put key1");
    bucket.put(b"key2", b"value2").expect("Failed to put key2");
    bucket.put(b"key3", b"value3").expect("Failed to put key3");

    // Verify all keys exist
    assert_eq!(bucket.get(b"key1").unwrap(), b"value1");
    assert_eq!(bucket.get(b"key2").unwrap(), b"value2");
    assert_eq!(bucket.get(b"key3").unwrap(), b"value3");

    // Delete all keys
    bucket.del(b"key1").expect("Failed to delete key1");
    bucket.del(b"key2").expect("Failed to delete key2");
    bucket.del(b"key3").expect("Failed to delete key3");

    // Verify none of the keys exist
    assert_eq!(bucket.get(b"key1"), Err(Error::NotFound));
    assert_eq!(bucket.get(b"key2"), Err(Error::NotFound));
    assert_eq!(bucket.get(b"key3"), Err(Error::NotFound));
}

#[test]
fn test_delete_from_root() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_from_root.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Add a key
    bucket
        .put(b"only_key", b"only_value")
        .expect("Failed to put only key");

    // Verify key exists
    assert_eq!(bucket.get(b"only_key").unwrap(), b"only_value");

    // Delete the only key, making the root empty
    bucket.del(b"only_key").expect("Failed to delete only key");

    // Verify key no longer exists
    assert_eq!(bucket.get(b"only_key"), Err(Error::NotFound));

    // Add a new key, verify tree works correctly
    bucket
        .put(b"new_key", b"new_value")
        .expect("Failed to put new key");
    assert_eq!(bucket.get(b"new_key").unwrap(), b"new_value");
}

#[test]
fn test_delete_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_nonexistent.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Attempt to delete a non-existent key, should return NotFound error
    let result = bucket.del(b"nonexistent_key");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), btree_store::Error::NotFound));

    // Ensure tree state hasn't changed
    assert_eq!(bucket.get(b"nonexistent_key"), Err(Error::NotFound));
}

#[test]
fn test_sequence_of_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_sequence_of_deletes.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Add a sequence of keys
    for i in 0..10 {
        bucket
            .put(
                format!("key{}", i).as_bytes(),
                format!("value{}", i).as_bytes(),
            )
            .expect("Failed to put key");
    }

    // Verify all keys exist
    for i in 0..10 {
        assert_eq!(
            bucket.get(format!("key{}", i).as_bytes()).unwrap(),
            format!("value{}", i).as_bytes()
        );
    }

    // Delete half of the keys
    for i in (0..5).step_by(1) {
        bucket
            .del(format!("key{}", i).as_bytes())
            .expect("Failed to delete key");
        assert_eq!(
            bucket.get(format!("key{}", i).as_bytes()),
            Err(Error::NotFound)
        );
    }

    // Verify remaining keys still exist
    for i in 5..10 {
        assert_eq!(
            bucket.get(format!("key{}", i).as_bytes()).unwrap(),
            format!("value{}", i).as_bytes()
        );
    }

    // Delete remaining keys
    for i in 5..10 {
        bucket
            .del(format!("key{}", i).as_bytes())
            .expect("Failed to delete key");
        assert_eq!(
            bucket.get(format!("key{}", i).as_bytes()),
            Err(Error::NotFound)
        );
    }
}

#[test]
fn test_interleaved_puts_and_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_interleaved_puts_and_deletes.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    let bucket = tree.new_bucket("default").expect("Failed to create bucket");

    // Add some keys
    for i in 0..5 {
        bucket
            .put(
                format!("key{}", i).as_bytes(),
                format!("value{}", i).as_bytes(),
            )
            .expect("Failed to put key");
    }

    // Delete some keys
    bucket.del(b"key1").expect("Failed to delete key1");
    bucket.del(b"key3").expect("Failed to delete key3");

    // Add more keys
    for i in 5..10 {
        bucket
            .put(
                format!("key{}", i).as_bytes(),
                format!("value{}", i).as_bytes(),
            )
            .expect("Failed to put key");
    }

    // Verify state
    assert_eq!(bucket.get(b"key0").unwrap(), b"value0"); // Exists
    assert_eq!(bucket.get(b"key1"), Err(Error::NotFound)); // Deleted
    assert_eq!(bucket.get(b"key2").unwrap(), b"value2"); // Exists
    assert_eq!(bucket.get(b"key3"), Err(Error::NotFound)); // Deleted
    assert_eq!(bucket.get(b"key4").unwrap(), b"value4"); // Exists
    for i in 5..10 {
        assert_eq!(
            bucket.get(format!("key{}", i).as_bytes()).unwrap(),
            format!("value{}", i).as_bytes()
        );
    }

    // Delete all remaining keys
    for i in (0..10).filter(|x| *x != 1 && *x != 3) {
        // Skip already deleted keys
        bucket
            .del(format!("key{}", i).as_bytes())
            .expect("Failed to delete key");
        assert_eq!(
            bucket.get(format!("key{}", i).as_bytes()),
            Err(Error::NotFound)
        );
    }
}
