use btree_store::BTree;
use std::thread;
use tempfile::TempDir;

#[test]
fn test_concurrent_put_get() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("concurrent_test.db");
    let btree = BTree::open(&db_path).unwrap();
    btree.new_bucket("concurrent", false).unwrap();

    let mut handles = vec![];
    let num_threads = 4;
    let ops_per_thread = 200;

    // Concurrent Puts - writers serialize on the shared writer mutex while
    // readers and the writer no longer block each other (epoch-pinned snapshots).
    for t in 0..num_threads {
        let btree_clone = btree.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", t, i);
                let val = format!("value_{}", i);
                btree_clone
                    .exec("concurrent", |txn| {
                        txn.put(key.as_bytes(), val.as_bytes()).unwrap();
                        Ok(())
                    })
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Concurrent Gets
    let mut handles = vec![];
    for t in 0..num_threads {
        let btree_clone = btree.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{}", t, i);
                let expected_val = format!("value_{}", i);
                btree_clone
                    .view("concurrent", |txn| {
                        let val = txn
                            .get(key.as_bytes())
                            .unwrap_or_else(|_| panic!("Key {} not found", key));
                        assert_eq!(val, expected_val.as_bytes());
                        Ok(())
                    })
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_mass_delete_merge_shrink() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("merge_shrink.db");
    let btree = BTree::open(&db_path).unwrap();
    btree.new_bucket("merge", false).unwrap();

    let num_entries = 1000;
    btree
        .exec("merge", |txn| {
            for i in 0..num_entries {
                let key = format!("key_{:05}", i);
                txn.put(key.as_bytes(), b"some_value").unwrap();
            }
            Ok(())
        })
        .unwrap();

    btree
        .exec("merge", |txn| {
            for i in 0..num_entries - 1 {
                let key = format!("key_{:05}", i);
                txn.del(key.as_bytes()).unwrap();
            }
            Ok(())
        })
        .unwrap();

    btree
        .view("merge", |txn| {
            let last_key = format!("key_{:05}", num_entries - 1);
            assert_eq!(txn.get(last_key.as_bytes()).unwrap(), b"some_value");
            Ok(())
        })
        .unwrap();
}
