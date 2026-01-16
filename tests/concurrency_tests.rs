use btree_store::{BTree, Error};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
fn test_concurrent_put_get() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("concurrent_test.db");
    let btree = Arc::new(BTree::open(&db_path).unwrap());
    let bucket = Arc::new(btree.new_bucket("concurrent").unwrap());

    let mut handles = vec![];
    let num_threads = 8;
    let ops_per_thread = 1000;

    // Concurrent Puts
    for t in 0..num_threads {
        let bucket = bucket.clone();
        let btree = btree.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", t, i);
                let val = format!("value_{}", i);
                bucket.put(key.as_bytes(), val.as_bytes()).unwrap();
                if i % 10 == 0 {
                    btree.commit().unwrap();
                }
            }
            btree.commit().unwrap();
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Concurrent Gets
    let mut handles = vec![];
    for t in 0..num_threads {
        let bucket = bucket.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", t, i);
                let expected_val = format!("value_{}", i);
                let val = bucket.get(key.as_bytes()).unwrap();
                assert_eq!(val, expected_val.as_bytes());
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_mixed_ops() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("concurrent_mixed.db");
    let btree = Arc::new(BTree::open(&db_path).unwrap());
    let bucket = Arc::new(btree.new_bucket("mixed").unwrap());

    // Pre-fill some data
    for i in 0..1000 {
        bucket
            .put(format!("key_{}", i).as_bytes(), b"initial")
            .unwrap();
    }
    btree.commit().unwrap();

    let mut handles = vec![];
    let btree_c = btree.clone();
    let bucket_c = bucket.clone();

    // Thread 1: Continuous Puts and Commits
    handles.push(thread::spawn(move || {
        for i in 1000..2000 {
            bucket_c
                .put(format!("key_{}", i).as_bytes(), b"new")
                .unwrap();
            if i % 50 == 0 {
                btree_c.commit().unwrap();
            }
        }
        btree_c.commit().unwrap();
    }));

    let btree_c2 = btree.clone();
    let bucket_c2 = bucket.clone();
    // Thread 2: Continuous Deletes and Commits
    handles.push(thread::spawn(move || {
        for i in 0..500 {
            let _ = bucket_c2.del(format!("key_{}", i).as_bytes());
            if i % 50 == 0 {
                btree_c2.commit().unwrap();
            }
        }
        btree_c2.commit().unwrap();
    }));

    let bucket_c3 = bucket.clone();
    // Thread 3: Continuous Gets
    handles.push(thread::spawn(move || {
        for _ in 0..2000 {
            // Just check if it doesn't crash
            let _ = bucket_c3.get(b"key_750");
        }
    }));

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_mass_delete_merge_shrink() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("merge_shrink.db");
    let btree = BTree::open(&db_path).unwrap();
    let bucket = btree.new_bucket("merge").unwrap();

    let num_entries = 20000;
    // 1. Insert many entries to create a deep tree
    for i in 0..num_entries {
        let key = format!("key_{:05}", i);
        bucket.put(key.as_bytes(), b"some_value").unwrap();
    }
    btree.commit().unwrap();

    // 2. Verify all exist
    for i in 0..num_entries {
        let key = format!("key_{:05}", i);
        assert_eq!(bucket.get(key.as_bytes()).unwrap(), b"some_value");
    }

    // 3. Delete almost all entries to trigger shrinking/merging
    // We delete in a way that leaves nodes nearly empty or empty
    for i in 0..num_entries - 1 {
        let key = format!("key_{:05}", i);
        bucket
            .del(key.as_bytes())
            .expect(&format!("failed to delete {}", key));
    }
    btree.commit().unwrap();

    // 4. Verify only the last one remains
    let last_key = format!("key_{:05}", num_entries - 1);
    assert_eq!(bucket.get(last_key.as_bytes()).unwrap(), b"some_value");

    for i in 0..num_entries - 1 {
        let key = format!("key_{:05}", i);
        assert!(matches!(bucket.get(key.as_bytes()), Err(Error::NotFound)));
    }

    // 5. Re-insert and verify tree still works
    for i in 0..1000 {
        let key = format!("new_key_{:05}", i);
        bucket.put(key.as_bytes(), b"new_value").unwrap();
    }
    btree.commit().unwrap();

    for i in 0..1000 {
        let key = format!("new_key_{:05}", i);
        assert_eq!(bucket.get(key.as_bytes()).unwrap(), b"new_value");
    }
}
