use btree_store::{BTree, BucketError, Error, KeyError, MAX_KEY_LEN};
use tempfile::TempDir;

#[test]
fn test_basic_put_get() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_basic_put_get.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

    let entries: Vec<_> = (0..256u32)
        .map(|i| {
            (
                format!("key-{i:04}").into_bytes(),
                vec![(i % 251) as u8; 128],
            )
        })
        .collect();

    tree.exec("default", |txn| {
        for (key, value) in &entries {
            txn.put(key, value).expect("Failed to put key-value");
        }
        Ok(())
    })
    .expect("Failed to execute transaction");

    tree.view("default", |txn| {
        for index in [0, 1, 63, 127, 191, 254, 255] {
            let (key, value) = &entries[index];
            let retrieved = txn.get(key).expect("Failed to get value");
            assert_eq!(&retrieved, value);
        }
        Ok(())
    })
    .expect("Failed to view bucket");
    assert_eq!(tree.pending_pages(), (0, 0));
}

#[test]
fn test_delete_existing_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_existing_key.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

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
        assert_eq!(retrieved, Err(Error::KeyNotFound));
        Ok(())
    })
    .expect("Failed to view");
}

#[test]
fn test_delete_nonexistent_key() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_nonexistent_key.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

    let key = b"nonexistent_key";

    // Attempt to delete a non-existent key; this returns KeyNotFound
    let result = tree.exec("default", |txn| txn.del(key));
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), btree_store::Error::KeyNotFound);
}

#[test]
fn test_multiple_puts_and_gets() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_multiple_puts_and_gets.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

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
    tree.new_bucket("default", false).unwrap();

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
fn test_update_existing_key_returns_true() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_update_existing_key.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

    tree.exec("default", |txn| txn.put(b"key", b"old")).unwrap();

    let updated = tree
        .exec("default", |txn| txn.update(b"key", b"new"))
        .expect("Failed to update existing key");
    assert!(updated, "update should report true for an existing key");

    tree.view("default", |txn| {
        assert_eq!(txn.get(b"key").unwrap(), b"new".to_vec());
        Ok(())
    })
    .unwrap();
    assert_eq!(tree.pending_pages(), (0, 0));
}

#[test]
fn test_update_missing_key_in_existing_bucket_returns_false_without_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test_update_missing_key_existing_bucket.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

    tree.exec("default", |txn| txn.put(b"present", b"value"))
        .unwrap();

    let before = tree.current_seq();
    let updated = tree
        .exec("default", |txn| txn.update(b"missing", b"value"))
        .expect("Failed to run update on missing key");
    let after = tree.current_seq();

    assert!(!updated, "update should report false for a missing key");
    assert_eq!(after, before, "missing-key update must stay a no-op");

    tree.view("default", |txn| {
        assert_eq!(txn.get(b"present").unwrap(), b"value".to_vec());
        assert_eq!(txn.get(b"missing"), Err(Error::KeyNotFound));
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_update_miss_then_put_in_precreated_bucket() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_update_miss_then_put.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

    tree.exec("default", |txn| {
        assert!(!txn.update(b"missing", b"value")?);
        txn.put(b"present", b"value")?;
        Ok(())
    })
    .expect("Failed to commit after update miss");

    tree.view("default", |txn| {
        assert_eq!(txn.get(b"present").unwrap(), b"value".to_vec());
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_update_missing_key_in_empty_bucket_is_noop() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test_update_missing_key_empty_bucket_noop.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("empty_bucket", false).unwrap();

    let before = tree.current_seq();
    let updated = tree
        .exec("empty_bucket", |txn| txn.update(b"missing", b"value"))
        .expect("Failed to run update on empty bucket");
    let after = tree.current_seq();

    assert!(!updated, "update should report false for a missing key");
    assert_eq!(after, before, "missing-key update must stay a no-op");

    tree.view("empty_bucket", |txn| {
        assert_eq!(txn.get(b"missing"), Err(Error::KeyNotFound));
        Ok(())
    })
    .unwrap();
}

#[test]
fn fuzz_minimized_observe_then_empty_value_put_keeps_bucket_root_valid() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("fuzz_minimized_empty_value_root_valid.db");
    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("kv", false).unwrap();

    tree.exec("kv", |txn| {
        assert_eq!(txn.get([0]), Err(Error::KeyNotFound));
        Ok(())
    })
    .unwrap();

    tree.exec("kv", |txn| txn.put([0], [])).unwrap();

    tree.exec("kv", |txn| {
        assert_eq!(txn.get([0]).unwrap(), Vec::<u8>::new());
        Ok(())
    })
    .unwrap();

    tree.exec("kv", |txn| txn.put([39, 0, 0, 0, 0, 0], []))
        .unwrap();

    tree.view("kv", |txn| {
        assert_eq!(txn.get([0]).unwrap(), Vec::<u8>::new());
        assert_eq!(txn.get([39, 0, 0, 0, 0, 0]).unwrap(), Vec::<u8>::new());
        let mut iter = txn.iter();
        let mut key = Vec::new();
        let mut value = Vec::new();
        let mut entries = Vec::new();
        while iter.next_ref(&mut key, &mut value) {
            entries.push((key.clone(), value.clone()));
        }
        assert_eq!(
            entries,
            vec![
                (vec![0], Vec::<u8>::new()),
                (vec![39, 0, 0, 0, 0, 0], Vec::<u8>::new()),
            ]
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_delete_complex_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_delete_complex_scenario.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

    // Insert multiple sets of key-value pairs
    let keys = ["key1", "key2", "key3", "key4", "key5"];
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
        assert_eq!(txn.get(b"key3"), Err(Error::KeyNotFound));
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
        assert_eq!(txn.get(b"key1"), Err(Error::KeyNotFound));
        assert_eq!(txn.get(b"key5"), Err(Error::KeyNotFound));
        assert!(txn.get(b"key2").is_ok());
        assert!(txn.get(b"key4").is_ok());
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_operations_on_missing_bucket_return_bucket_not_found() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_empty_tree_operations.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");

    // A bucket that was never created must be rejected before any closure runs.
    let view: btree_store::Result<()> = tree.view("default", |_txn| Ok(()));
    assert_eq!(view, Err(Error::BucketNotFound));

    let result = tree.exec("default", |txn| txn.del(b"nonexistent"));
    assert_eq!(result, Err(Error::BucketNotFound));

    // Once created, the empty bucket accepts normal operations.
    tree.new_bucket("default", false).unwrap();
    let result = tree.exec("default", |txn| txn.del(b"nonexistent"));
    assert_eq!(result, Err(Error::KeyNotFound));
}

#[test]
fn test_bucket_name_must_be_non_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_empty_bucket_name_invalid.db");
    let tree = BTree::open(&db_path).unwrap();

    assert_eq!(
        tree.exec("", |_| Ok(())).unwrap_err(),
        Error::InvalidBucket(BucketError::Empty)
    );
    assert_eq!(
        tree.view("", |_| Ok(())).unwrap_err(),
        Error::InvalidBucket(BucketError::Empty)
    );
    assert_eq!(
        tree.del_bucket("").unwrap_err(),
        Error::InvalidBucket(BucketError::Empty)
    );
    assert_eq!(
        tree.exec_multi(|multi| multi.exec("", |_| Ok(())))
            .unwrap_err(),
        Error::InvalidBucket(BucketError::Empty)
    );
    assert!(
        tree.buckets().unwrap().is_empty(),
        "rejected empty bucket names must not create catalog entries"
    );
}

#[test]
fn test_bucket_name_must_not_exceed_key_limit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_large_bucket_name_invalid.db");
    let tree = BTree::open(&db_path).unwrap();
    let bucket = "x".repeat(MAX_KEY_LEN + 1);
    let unicode_bucket = "é".repeat(MAX_KEY_LEN / 2 + 1);

    let expected = Error::InvalidBucket(BucketError::TooLarge {
        len: MAX_KEY_LEN + 1,
        max: MAX_KEY_LEN,
    });
    assert_eq!(
        tree.exec(&bucket, |_| Ok(())).unwrap_err(),
        expected.clone()
    );
    assert_eq!(
        tree.view(&bucket, |_| Ok(())).unwrap_err(),
        expected.clone()
    );
    assert_eq!(tree.del_bucket(&bucket).unwrap_err(), expected.clone());
    assert_eq!(
        tree.exec_multi(|multi| multi.exec(&bucket, |_| Ok(())))
            .unwrap_err(),
        expected
    );
    assert_eq!(
        tree.exec(&unicode_bucket, |_| Ok(())).unwrap_err(),
        Error::InvalidBucket(BucketError::TooLarge {
            len: unicode_bucket.len(),
            max: MAX_KEY_LEN,
        })
    );
    assert!(
        tree.buckets().unwrap().is_empty(),
        "rejected overlong bucket names must not create catalog entries"
    );
}

#[test]
fn test_user_key_contract_is_enforced_without_mutating_bucket() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_user_key_invalid.db");
    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("bucket", false).unwrap();
    let too_large_key = vec![b'k'; MAX_KEY_LEN + 1];

    assert_eq!(
        tree.exec("bucket", |txn| txn.put(b"", b"value"))
            .unwrap_err(),
        Error::InvalidKey(KeyError::Empty)
    );
    assert!(
        tree.buckets().unwrap().contains(&"bucket".to_string()),
        "the pre-created bucket remains in the catalog after a rejected write"
    );

    tree.exec("bucket", |txn| txn.put(b"valid", b"value"))
        .unwrap();

    assert_eq!(
        tree.view("bucket", |txn| txn.get(b"")).unwrap_err(),
        Error::InvalidKey(KeyError::Empty)
    );
    assert_eq!(
        tree.exec("bucket", |txn| txn.del(b"")).unwrap_err(),
        Error::InvalidKey(KeyError::Empty)
    );
    assert_eq!(
        tree.exec("bucket", |txn| txn.put(&too_large_key, b"value"))
            .unwrap_err(),
        Error::InvalidKey(KeyError::TooLarge {
            len: MAX_KEY_LEN + 1,
            max: MAX_KEY_LEN,
        })
    );
    assert_eq!(
        tree.view("bucket", |txn| txn.get(&too_large_key))
            .unwrap_err(),
        Error::InvalidKey(KeyError::TooLarge {
            len: MAX_KEY_LEN + 1,
            max: MAX_KEY_LEN,
        })
    );
    assert_eq!(
        tree.exec("bucket", |txn| txn.del(&too_large_key))
            .unwrap_err(),
        Error::InvalidKey(KeyError::TooLarge {
            len: MAX_KEY_LEN + 1,
            max: MAX_KEY_LEN,
        })
    );
    tree.view("bucket", |txn| {
        assert_eq!(txn.get(b"valid").unwrap(), b"value".to_vec());
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_large_values() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_large_values.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("default", false).unwrap();

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
        tree.new_bucket("default", false).unwrap();
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
        btree.new_bucket("default", false).unwrap();
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

#[test]
fn preserves_page_state_and_snapshot_publication() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tree-context-refactor.db");
    let tree = BTree::open(&path).unwrap();
    tree.new_bucket("failed", false).unwrap();
    tree.new_bucket("kept", false).unwrap();
    tree.new_bucket("aborted", false).unwrap();
    let observer = tree.clone();
    let before = tree.current_seq();

    let failed: btree_store::Result<()> = tree.exec("failed", |txn| {
        txn.put(b"overflow", vec![0x41; 2 * 4096])?;
        let (pending_alloc, pending_free) = observer.pending_pages();
        assert!(pending_alloc > 0);
        assert_eq!(pending_free, 0);
        Err(Error::KeyNotFound)
    });
    assert_eq!(failed, Err(Error::KeyNotFound));
    assert!(tree.current_seq() > before);
    assert_eq!(observer.pending_pages(), (0, 0));

    tree.exec_multi(|multi| {
        multi.exec("kept", |txn| txn.put(b"before", vec![0x31; 2 * 4096]))?;

        let aborted: btree_store::Result<()> = multi.exec("aborted", |txn| {
            txn.put(b"large", vec![0x7a; 2 * 4096])?;
            Err(Error::KeyNotFound)
        });
        assert_eq!(aborted, Err(Error::KeyNotFound));

        multi.exec("kept", |txn| txn.put(b"after", b"continued"))?;
        Ok(())
    })
    .unwrap();
    assert_eq!(tree.pending_pages(), (0, 0));
    assert!(
        tree.buckets()
            .unwrap()
            .iter()
            .any(|bucket| bucket == "kept"),
        "the kept bucket must remain after the multi transaction"
    );

    tree.view("kept", |txn| {
        assert_eq!(txn.get(b"before")?, vec![0x31; 2 * 4096]);
        assert_eq!(txn.get(b"after")?, b"continued");
        Ok::<_, Error>(())
    })
    .unwrap();
    // Repeated views reuse the physical catalog and bucket-root nodes through
    // the PID-keyed NodeCache after the bucket-level caches are removed.
    tree.view("kept", |txn| {
        assert_eq!(txn.get(b"after")?, b"continued");
        Ok::<_, Error>(())
    })
    .unwrap();
    drop(tree);

    let reopened = BTree::open(&path).unwrap();
    reopened
        .view("kept", |txn| {
            assert_eq!(txn.get(b"before")?, vec![0x31; 2 * 4096]);
            assert_eq!(txn.get(b"after")?, b"continued");
            Ok::<_, Error>(())
        })
        .unwrap();
    assert_eq!(
        reopened.view("aborted", |txn| txn.get(b"large")),
        Err(Error::KeyNotFound)
    );
    assert_eq!(reopened.pending_pages(), (0, 0));
}

/// A workload with long shared prefixes run against both a plain and a
/// prefix-encoded bucket must produce identical results. This exercises the
/// encoded write path: tail-only inserts, cross-prefix rebuilds, splits,
/// deletes, overflow values, and iteration.
#[test]
fn prefix_encoded_bucket_refines_plain_bucket() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("prefix-encoding-oracle.db");
    let tree = BTree::open(&path).unwrap();
    tree.new_bucket("plain", false).unwrap();
    tree.new_bucket("encoded", true).unwrap();

    // Shared-prefix keys plus occasional out-of-domain keys that force the
    // encoded leaf to rebuild with a fresh prefix.
    for i in 0..120u32 {
        let key = format!("user/{i:03}/profile");
        let value = vec![(i % 251) as u8; 128];
        tree.exec("plain", |txn| txn.put(key.as_bytes(), &value))
            .unwrap();
        tree.exec("encoded", |txn| txn.put(key.as_bytes(), &value))
            .unwrap();
    }
    for i in 0..30u32 {
        let key = format!("admin/{i:04}");
        let value = vec![(i as u8).wrapping_mul(7); 300]; // overflow-sized
        tree.exec("plain", |txn| txn.put(key.as_bytes(), &value))
            .unwrap();
        tree.exec("encoded", |txn| txn.put(key.as_bytes(), &value))
            .unwrap();
    }

    let read_bucket = |tree: &BTree, bucket: &str| -> Vec<(Vec<u8>, Vec<u8>)> {
        tree.view(bucket, |txn| {
            let mut iter = txn.iter();
            let mut key = Vec::new();
            let mut value = Vec::new();
            let mut entries = Vec::new();
            while iter.next_ref(&mut key, &mut value) {
                entries.push((key.clone(), value.clone()));
            }
            Ok::<_, Error>(entries)
        })
        .unwrap()
    };

    let plain = read_bucket(&tree, "plain");
    let encoded = read_bucket(&tree, "encoded");
    assert_eq!(
        encoded, plain,
        "encoded iterator must match plain order/values"
    );

    // Point reads and updates agree.
    for i in 0..120u32 {
        let key = format!("user/{i:03}/profile");
        let expect = tree.view("plain", |txn| txn.get(key.as_bytes())).unwrap();
        let got = tree.view("encoded", |txn| txn.get(key.as_bytes())).unwrap();
        assert_eq!(got, expect);
    }

    // Update an existing and delete some keys in both buckets.
    for i in (0..120u32).step_by(3) {
        let key = format!("user/{i:03}/profile");
        let value = format!("updated-{i}");
        tree.exec("plain", |txn| {
            assert!(txn.update(key.as_bytes(), value.as_bytes())?);
            Ok::<_, Error>(())
        })
        .unwrap();
        tree.exec("encoded", |txn| {
            assert!(txn.update(key.as_bytes(), value.as_bytes())?);
            Ok::<_, Error>(())
        })
        .unwrap();
    }
    for i in (0..120u32).step_by(4) {
        let key = format!("user/{i:03}/profile");
        tree.exec("plain", |txn| txn.del(key.as_bytes())).unwrap();
        tree.exec("encoded", |txn| txn.del(key.as_bytes())).unwrap();
    }
    assert_eq!(read_bucket(&tree, "encoded"), read_bucket(&tree, "plain"));

    // Reopen: the per-bucket flag and encoded pages survive.
    drop(tree);
    let tree = BTree::open(&path).unwrap();
    assert_eq!(read_bucket(&tree, "encoded"), read_bucket(&tree, "plain"));

    // exec_multi can mix encoded and plain buckets in one atomic transaction.
    tree.exec_multi(|multi| {
        multi.exec("encoded", |txn| txn.put(b"user/999/name", b"alice"))?;
        multi.exec("plain", |txn| txn.put(b"user/999/name", b"alice"))?;
        Ok::<_, Error>(())
    })
    .unwrap();
    assert_eq!(read_bucket(&tree, "encoded"), read_bucket(&tree, "plain"));
}

/// A cross-prefix MIN key sorts before every shared-prefix key, so after a
/// split it routes to the LEFT half. The left half re-encodes under a shorter
/// prefix, its tails grow, and it can overflow again; the engine must split it
/// recursively instead of aborting. This is the left-side mirror of the right
/// half overflow covered by the oracle test's max-key inserts.
#[test]
fn prefix_encoded_bucket_splits_left_half_for_cross_prefix_min_key() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("encoded-left-split.db");
    let tree = BTree::open(&path).unwrap();
    tree.new_bucket("enc", true).unwrap();

    let prefix = "u".repeat(120);
    for i in 0..300u32 {
        let key = format!("{prefix}/{i:04}");
        tree.exec("enc", |txn| txn.put(key.as_bytes(), b"v"))
            .unwrap();
    }
    // The min key must land first and survive reopen; before the fix this
    // aborted with LEAF_SPLIT_LEFT_OVERFLOW.
    tree.exec("enc", |txn| txn.put(b"aaa", b"v")).unwrap();

    let read_first = |tree: &BTree| -> Vec<u8> {
        tree.view("enc", |txn| {
            let mut iter = txn.iter();
            let mut key = Vec::new();
            let mut value = Vec::new();
            assert!(
                iter.next_ref(&mut key, &mut value),
                "must have at least one entry"
            );
            Ok::<_, Error>(key)
        })
        .unwrap()
    };
    assert_eq!(
        read_first(&tree),
        b"aaa".to_vec(),
        "min key must sort first"
    );
    drop(tree);

    let reopened = BTree::open(&path).unwrap();
    assert_eq!(read_first(&reopened), b"aaa".to_vec());
    let count = reopened
        .view("enc", |txn| {
            let mut iter = txn.iter();
            let mut key = Vec::new();
            let mut value = Vec::new();
            let mut count = 0usize;
            while iter.next_ref(&mut key, &mut value) {
                count += 1;
            }
            Ok::<_, Error>(count)
        })
        .unwrap();
    assert_eq!(count, 301);
}

/// Alternating cross-prefix min and max keys with deletes keeps the encoded
/// tree ordered through repeated left- and right-half recursive splits.
#[test]
fn prefix_encoded_bucket_stays_ordered_through_both_split_sides() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("encoded-both-split-sides.db");
    let tree = BTree::open(&path).unwrap();
    tree.new_bucket("enc", true).unwrap();

    let prefix = "u".repeat(120);
    let mut oracle = std::collections::BTreeMap::new();
    for round in 0..24u32 {
        for i in 0..60u32 {
            let key = format!("{prefix}/{round:02}/{i:04}").into_bytes();
            tree.exec("enc", |txn| txn.put(&key, b"v")).unwrap();
            oracle.insert(key, b"v".to_vec());
        }
        let mink = format!("aaa-{round:04}").into_bytes();
        tree.exec("enc", |txn| txn.put(&mink, b"m")).unwrap();
        oracle.insert(mink, b"m".to_vec());

        let maxk = format!("zzz-{round:04}").into_bytes();
        tree.exec("enc", |txn| txn.put(&maxk, vec![0x5a; 4000]))
            .unwrap();
        oracle.insert(maxk, vec![0x5a; 4000]);

        for i in (0..60u32).step_by(2) {
            let key = format!("{prefix}/{round:02}/{i:04}").into_bytes();
            tree.exec("enc", |txn| txn.del(&key)).unwrap();
            oracle.remove(&key);
        }

        let actual = tree
            .view("enc", |txn| {
                let mut iter = txn.iter();
                let mut key = Vec::new();
                let mut value = Vec::new();
                let mut entries = Vec::new();
                while iter.next_ref(&mut key, &mut value) {
                    entries.push((key.clone(), value.clone()));
                }
                Ok::<_, Error>(entries)
            })
            .unwrap();
        let expected: Vec<_> = oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(
            actual, expected,
            "round {round}: iteration diverged from oracle"
        );
    }
    drop(tree);
    let reopened = BTree::open(&path).unwrap();
    let count = reopened
        .view("enc", |txn| {
            let mut iter = txn.iter();
            let mut key = Vec::new();
            let mut value = Vec::new();
            let mut count = 0usize;
            while iter.next_ref(&mut key, &mut value) {
                count += 1;
            }
            Ok::<_, Error>(count)
        })
        .unwrap();
    assert_eq!(count, oracle.len());
}
