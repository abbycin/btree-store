use btree_store::{BTree, Error};
use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;

#[test]
fn test_exec_multi_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_exec_multi_basic.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("bucket1", false).unwrap();
    tree.new_bucket("bucket2", false).unwrap();

    // Execute multi-bucket transaction
    tree.exec_multi(|multi| {
        multi.exec("bucket1", |txn| txn.put(b"key1", b"value1"))?;
        multi.exec("bucket2", |txn| txn.put(b"key2", b"value2"))?;
        Ok(())
    })
    .expect("Failed to execute multi-bucket transaction");

    // Verify both buckets were updated
    tree.view("bucket1", |txn| {
        let val = txn.get(b"key1").expect("Failed to get key1");
        assert_eq!(val, b"value1");
        Ok(())
    })
    .expect("Failed to view bucket1");

    tree.view("bucket2", |txn| {
        let val = txn.get(b"key2").expect("Failed to get key2");
        assert_eq!(val, b"value2");
        Ok(())
    })
    .expect("Failed to view bucket2");
}

#[test]
fn test_exec_multi_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_exec_multi_rollback.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("bucket1", false).unwrap();
    tree.new_bucket("bucket2", false).unwrap();

    // Initial state
    tree.exec("bucket1", |txn| txn.put(b"initial", b"state"))
        .unwrap();
    let before_abort_seq = tree.current_seq();

    // Execute multi-bucket transaction that fails
    let res: btree_store::Result<()> = tree.exec_multi(|multi| {
        multi.exec("bucket1", |txn| txn.put(b"key1", b"value1"))?;
        multi.exec("bucket2", |txn| txn.put(b"key2", b"value2"))?;
        Err(Error::KeyNotFound)
    });

    assert!(res.is_err());
    assert!(tree.current_seq() > before_abort_seq);
    assert_eq!(tree.pending_pages(), (0, 0));

    // Verify bucket1 is still in initial state
    tree.view("bucket1", |txn| {
        let val = txn.get(b"initial").expect("Failed to get initial");
        assert_eq!(val, b"state");
        assert!(txn.get(b"key1").is_err());
        Ok(())
    })
    .expect("Failed to view bucket1");

    // Verify bucket2 doesn't exist (or at least doesn't have the key)
    let res2 = tree.view("bucket2", |txn| txn.get(b"key2"));
    assert!(res2.is_err());

    drop(tree);
    let reopened = BTree::open(&db_path).unwrap();
    assert!(reopened.current_seq() > before_abort_seq);
    assert_eq!(
        reopened.view("bucket2", |txn| txn.get(b"key2")),
        Err(Error::KeyNotFound)
    );
}

#[test]
fn aborted_inner_exec_publishes_consumed_meta_before_outer_noop_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("nested_meta_rollback.db");
    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("aborted", false).unwrap();
    let before_seq = tree.current_seq();
    let mut nested_seq = 0;

    tree.exec_multi(|multi| {
        let result = multi.exec("aborted", |txn| {
            txn.put(b"large", vec![0x7a; 16 * 1024])?;
            Err::<(), _>(Error::KeyNotFound)
        });
        assert_eq!(result, Err(Error::KeyNotFound));
        nested_seq = tree.current_seq();
        assert!(nested_seq > before_seq);
        Ok::<_, Error>(())
    })
    .unwrap();

    let published_seq = tree.current_seq();
    assert_eq!(published_seq, nested_seq);
    assert!(
        published_seq > before_seq,
        "the consumed MetaNode state must remain published after the outer no-op commit"
    );
    drop(tree);

    let reopened = BTree::open(&db_path).unwrap();
    assert_eq!(reopened.current_seq(), published_seq);
    assert_eq!(
        reopened.view("aborted", |txn| txn.get(b"large")),
        Err(Error::KeyNotFound)
    );
}

#[test]
fn test_exec_multi_sequential_on_same_bucket() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_exec_multi_sequential.db");

    let tree = BTree::open(&db_path).expect("Failed to open BTree");
    tree.new_bucket("bucket1", false).unwrap();

    tree.exec_multi(|multi| {
        multi.exec("bucket1", |txn| txn.put(b"key1", b"value1"))?;
        // Second execute on same bucket should see first change
        multi.exec("bucket1", |txn| {
            let val = txn.get(b"key1").expect("Should see key1");
            assert_eq!(val, b"value1");
            txn.put(b"key1", b"value2")
        })?;
        Ok(())
    })
    .expect("Failed exec_multi");

    tree.view("bucket1", |txn| {
        let val = txn.get(b"key1").unwrap();
        assert_eq!(val, b"value2");
        Ok(())
    })
    .unwrap();
}

#[test]
fn nested_abort_restores_same_and_other_bucket_before_outer_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("nested_savepoint_reopen.db");

    {
        let tree = BTree::open(&db_path).unwrap();
        tree.new_bucket("same", false).unwrap();
        tree.new_bucket("other", false).unwrap();
        tree.new_bucket("kept", false).unwrap();
        tree.exec("same", |txn| txn.put(b"stable", b"before"))
            .unwrap();

        tree.exec_multi(|multi| {
            let same = multi.exec("same", |txn| {
                txn.put(b"rolled-back", vec![7; 8192])?;
                txn.del(b"stable")?;
                Err::<(), _>(Error::KeyNotFound)
            });
            assert_eq!(same, Err(Error::KeyNotFound));

            multi.exec("same", |txn| {
                assert_eq!(txn.get(b"stable")?, b"before");
                assert_eq!(txn.get(b"rolled-back"), Err(Error::KeyNotFound));
                txn.put(b"committed", b"same")
            })?;

            let other = multi.exec("other", |txn| {
                txn.put(b"rolled-back", vec![9; 8192])?;
                Err::<(), _>(Error::KeyNotFound)
            });
            assert_eq!(other, Err(Error::KeyNotFound));

            multi.exec("kept", |txn| txn.put(b"committed", b"other"))?;
            Ok::<_, Error>(())
        })
        .unwrap();
        tree.view("same", |txn| {
            assert_eq!(txn.get(b"stable")?, b"before");
            assert_eq!(txn.get(b"committed")?, b"same");
            assert_eq!(txn.get(b"rolled-back"), Err(Error::KeyNotFound));
            Ok::<_, Error>(())
        })
        .unwrap();
        assert_eq!(
            tree.view("other", |txn| txn.get(b"rolled-back")),
            Err(Error::KeyNotFound)
        );
    }

    {
        let tree = BTree::open(&db_path).unwrap();
        tree.view("same", |txn| {
            assert_eq!(txn.get(b"stable")?, b"before");
            assert_eq!(txn.get(b"committed")?, b"same");
            assert_eq!(txn.get(b"rolled-back"), Err(Error::KeyNotFound));
            Ok::<_, Error>(())
        })
        .unwrap();
        tree.exec("same", |txn| txn.put(b"after-reopen", b"ok"))
            .unwrap();
        tree.view("kept", |txn| {
            assert_eq!(txn.get(b"committed")?, b"other");
            Ok::<_, Error>(())
        })
        .unwrap();
    }
}

#[test]
fn nested_abort_preserves_pages_allocated_before_savepoint() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("nested_savepoint_page_pins.db");
    let stable = vec![0x5A; 8192];

    {
        let tree = BTree::open(&db_path).unwrap();
        tree.new_bucket("bucket", false).unwrap();
        tree.exec_multi(|multi| {
            multi.exec("bucket", |txn| txn.put(b"stable", &stable))?;

            let aborted = multi.exec("bucket", |txn| {
                txn.put(b"stable", vec![0xA5; 8192])?;
                for i in 0..256u32 {
                    txn.put(format!("churn-{i:04}").as_bytes(), vec![i as u8; 512])?;
                }
                Err::<(), _>(Error::KeyNotFound)
            });
            assert_eq!(aborted, Err(Error::KeyNotFound));

            multi.exec("bucket", |txn| {
                assert_eq!(txn.get(b"stable")?, stable);
                assert_eq!(txn.get(b"churn-0000"), Err(Error::KeyNotFound));
                txn.put(b"continued", b"ok")
            })?;
            Ok::<_, Error>(())
        })
        .unwrap();
    }

    let tree = BTree::open(&db_path).unwrap();
    tree.view("bucket", |txn| {
        assert_eq!(txn.get(b"stable")?, stable);
        assert_eq!(txn.get(b"continued")?, b"ok");
        assert_eq!(txn.get(b"churn-0000"), Err(Error::KeyNotFound));
        Ok::<_, Error>(())
    })
    .unwrap();
}

#[test]
fn randomized_reopen_and_nested_abort_keeps_physical_ownership_valid() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("physical_ownership_churn.db");
    let mut rng = StdRng::seed_from_u64(0xB1_6A_4C_05);

    {
        let tree = BTree::open(&db_path).unwrap();
        tree.new_bucket("alpha", false).unwrap();
        tree.new_bucket("beta", false).unwrap();
        tree.new_bucket("gamma", false).unwrap();
    }

    for round in 0..48u32 {
        let tree = BTree::open(&db_path).unwrap();
        let bucket = match round % 3 {
            0 => "alpha",
            1 => "beta",
            _ => "gamma",
        };
        let key = format!("key-{}", rng.random_range(0..96));
        let value = vec![round as u8; 384 + rng.random_range(0..2048)];

        tree.exec_multi(|multi| {
            multi.exec(bucket, |txn| txn.put(key.as_bytes(), &value))?;
            let aborted = multi.exec(bucket, |txn| {
                txn.put(b"aborted", vec![0xA5; 8192])?;
                Err::<(), _>(Error::KeyNotFound)
            });
            assert_eq!(aborted, Err(Error::KeyNotFound));
            Ok::<_, Error>(())
        })
        .unwrap();
    }

    let tree = BTree::open(&db_path).unwrap();
    tree.exec("alpha", |txn| txn.put(b"continued", b"write"))
        .unwrap();
}
