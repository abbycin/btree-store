use btree_store::{BTree, Error};
use std::fs;
use tempfile::TempDir;

#[test]
fn test_compaction_preserves_data_and_shrinks_or_holds_size() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("compact.db");

    let tree = BTree::open(&db_path).unwrap();

    let value = vec![0x42u8; 8192];
    tree.exec("default", |txn| {
        for i in 0..400u32 {
            let key = format!("k{:04}", i);
            txn.put(key.as_bytes(), &value).unwrap();
        }
        Ok(())
    })
    .unwrap();

    tree.exec("default", |txn| {
        for i in 0..200u32 {
            let key = format!("k{:04}", i);
            txn.del(key.as_bytes()).unwrap();
        }
        Ok(())
    })
    .unwrap();

    tree.exec("default", |txn| {
        txn.put(b"__promote__", b"x").unwrap();
        Ok(())
    })
    .unwrap();

    let size_before = fs::metadata(&db_path).unwrap().len();

    let stats = tree.compact(u64::MAX).unwrap();
    assert!(stats.moved_pages > 0);
    assert_eq!(stats.remaining_candidates, 0);

    tree.view("default", |txn| {
        for i in 0..200u32 {
            let key = format!("k{:04}", i);
            assert_eq!(txn.get(key.as_bytes()), Err(Error::NotFound));
        }
        for i in 200..400u32 {
            let key = format!("k{:04}", i);
            assert_eq!(txn.get(key.as_bytes()).unwrap(), value);
        }
        assert_eq!(txn.get(b"__promote__").unwrap(), b"x");
        Ok(())
    })
    .unwrap();

    let size_after = fs::metadata(&db_path).unwrap().len();
    assert!(size_after <= size_before);
}

#[test]
fn test_compaction_moves_tail_pages() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_compaction_shrinks_file.db");

    let tree = BTree::open(&db_path).expect("open btree failed");

    let base_value = vec![0x11; 256];
    tree.exec("base", |txn| {
        for i in 0..1000u32 {
            let key = format!("b{:05}", i);
            txn.put(key.as_bytes(), &base_value)?;
        }
        Ok(())
    })
    .expect("commit base failed");

    tree.exec("base", |txn| {
        for i in 0..200u32 {
            let key = format!("b{:05}", i);
            txn.del(key.as_bytes())?;
        }
        Ok(())
    })
    .expect("delete base failed");

    let tail_value = vec![0x22; 16 * 1024];
    tree.exec("tail", |txn| {
        for _ in 0..40u32 {
            txn.put(b"hot", &tail_value)?;
        }
        Ok(())
    })
    .expect("commit tail failed");

    tree.exec("base", |txn| {
        txn.put(b"b00999", &base_value)?;
        Ok(())
    })
    .expect("promote freelist failed");

    let stats = tree.compact(u64::MAX).expect("compact failed");
    assert!(stats.moved_pages > 0);
    assert_eq!(stats.remaining_candidates, 0);

    tree.view("base", |txn| {
        let key = b"b00200";
        let got = txn.get(key).expect("missing key after compact");
        assert_eq!(got.len(), base_value.len());

        let key = b"b00999";
        let got = txn.get(key).expect("missing key after compact");
        assert_eq!(got.len(), base_value.len());
        Ok(())
    })
    .expect("verify after compact failed");
}

#[test]
fn test_compaction_relocates_tail_and_shrinks_file() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_compaction_relocates_tail.db");

    let tree = BTree::open(&db_path).expect("open btree failed");

    let base_value = vec![0x33u8; 8192];
    tree.exec("base", |txn| {
        for i in 0..3000u32 {
            let key = format!("k{:05}", i);
            txn.put(key.as_bytes(), &base_value)?;
        }
        Ok(())
    })
    .expect("commit base failed");

    tree.exec("base", |txn| {
        for i in 0..2000u32 {
            let key = format!("k{:05}", i);
            txn.del(key.as_bytes())?;
        }
        Ok(())
    })
    .expect("delete base failed");

    tree.exec("base", |txn| {
        txn.put(b"__promote__", b"x")?;
        Ok(())
    })
    .expect("promote freelist failed");

    let tail_value = vec![0x55u8; 16 * 1024];
    tree.exec("tail", |txn| {
        for i in 0..200u32 {
            let key = format!("t{:05}", i);
            txn.put(key.as_bytes(), &tail_value)?;
        }
        Ok(())
    })
    .expect("commit tail failed");

    let size_before = fs::metadata(&db_path).unwrap().len();

    let stats = tree.compact(u64::MAX).expect("compact failed");
    assert!(stats.moved_pages > 0);

    let size_after = fs::metadata(&db_path).unwrap().len();
    assert!(size_after < size_before);

    tree.view("base", |txn| {
        assert_eq!(txn.get(b"k00000"), Err(Error::NotFound));
        assert_eq!(txn.get(b"k01999"), Err(Error::NotFound));
        assert_eq!(txn.get(b"k02000").unwrap(), base_value);
        assert_eq!(txn.get(b"k02999").unwrap(), base_value);
        Ok(())
    })
    .expect("verify base after compact failed");

    tree.view("tail", |txn| {
        assert_eq!(txn.get(b"t00000").unwrap(), tail_value);
        assert_eq!(txn.get(b"t00199").unwrap(), tail_value);
        Ok(())
    })
    .expect("verify tail after compact failed");
}

#[test]
fn test_default_compaction_skips_when_low_address_budget_is_insufficient() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_compaction_default_guard.db");

    let tree = BTree::open(&db_path).expect("open btree failed");
    let value = vec![0xABu8; 8 * 1024];

    tree.exec("default", |txn| {
        for i in 0..1500u32 {
            let key = format!("k{:05}", i);
            txn.put(key.as_bytes(), &value)?;
        }
        Ok(())
    })
    .expect("commit failed");

    let size_before = fs::metadata(&db_path).unwrap().len();
    let stats = tree.compact(0).expect("compact failed");
    let size_after = fs::metadata(&db_path).unwrap().len();

    assert_eq!(stats.moved_pages, 0);
    assert!(stats.remaining_candidates > 0);
    assert_eq!(size_after, size_before);
}

#[test]
fn test_compaction_preserves_empty_bucket_catalog_across_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test_compaction_empty_bucket_catalog.db");

    let tree = BTree::open(&db_path).expect("open btree failed");

    tree.exec("zz", |_| Ok(()))
        .expect("first touch of empty bucket failed");
    assert_eq!(tree.buckets().unwrap(), vec!["zz".to_string()]);

    tree.exec("zz", |_| Ok(()))
        .expect("second touch of empty bucket failed");
    assert_eq!(
        tree.buckets().unwrap(),
        vec!["zz".to_string()],
        "re-touching an existing empty bucket must not lose it"
    );

    tree.compact(u64::MAX).expect("first compact failed");
    assert_eq!(
        tree.buckets().unwrap(),
        vec!["zz".to_string()],
        "first compact must preserve empty bucket catalog entry"
    );

    drop(tree);

    let reopened = BTree::open(&db_path).expect("reopen btree failed");
    assert_eq!(
        reopened.buckets().unwrap(),
        vec!["zz".to_string()],
        "reopen must preserve empty bucket catalog entry"
    );

    reopened.compact(u64::MAX).expect("second compact failed");
    assert_eq!(
        reopened.buckets().unwrap(),
        vec!["zz".to_string()],
        "second compact must preserve empty bucket catalog entry"
    );

    reopened.compact(u64::MAX).expect("third compact failed");
    assert_eq!(
        reopened.buckets().unwrap(),
        vec!["zz".to_string()],
        "repeated compact must preserve empty bucket catalog entry"
    );

    reopened
        .exec("empty", |_| Ok(()))
        .expect("touch empty bucket failed");
    let mut buckets = reopened.buckets().unwrap();
    buckets.sort();
    assert_eq!(
        buckets,
        vec!["empty".to_string(), "zz".to_string()],
        "touching another empty bucket must not lose zz"
    );

    reopened
        .exec("b", |_| Ok(()))
        .expect("touch b bucket failed");

    let mut buckets = reopened.buckets().unwrap();
    buckets.sort();
    assert_eq!(
        buckets,
        vec!["b".to_string(), "empty".to_string(), "zz".to_string()],
        "later bucket creation must not lose the original empty bucket"
    );
}

#[test]
fn test_compaction_preserves_multiple_empty_buckets_across_repeated_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir
        .path()
        .join("test_compaction_multiple_empty_buckets.db");

    let tree = BTree::open(&db_path).expect("open btree failed");

    tree.exec("empty", |_| Ok(()))
        .expect("touch empty bucket failed");
    assert_eq!(
        sorted_buckets(&tree),
        vec!["empty".to_string()],
        "first empty bucket must be visible immediately"
    );

    tree.exec("zz", |_| Ok(())).expect("touch zz bucket failed");
    assert_eq!(
        sorted_buckets(&tree),
        vec!["empty".to_string(), "zz".to_string()],
        "second empty bucket must be visible immediately"
    );

    tree.compact(u64::MAX).expect("first compact failed");
    assert_eq!(
        sorted_buckets(&tree),
        vec!["empty".to_string(), "zz".to_string()],
        "first full compact must preserve both empty buckets"
    );

    drop(tree);

    let reopened = BTree::open(&db_path).expect("first reopen failed");
    assert_eq!(
        sorted_buckets(&reopened),
        vec!["empty".to_string(), "zz".to_string()],
        "first reopen must preserve both empty buckets"
    );

    reopened.compact(u64::MAX).expect("second compact failed");
    assert_eq!(
        sorted_buckets(&reopened),
        vec!["empty".to_string(), "zz".to_string()],
        "second full compact must preserve both empty buckets"
    );

    reopened.compact(u64::MAX).expect("third compact failed");
    assert_eq!(
        sorted_buckets(&reopened),
        vec!["empty".to_string(), "zz".to_string()],
        "third full compact must preserve both empty buckets"
    );

    drop(reopened);

    let reopened = BTree::open(&db_path).expect("second reopen failed");
    assert_eq!(
        sorted_buckets(&reopened),
        vec!["empty".to_string(), "zz".to_string()],
        "second reopen must preserve both empty buckets"
    );

    drop(reopened);

    let reopened = BTree::open(&db_path).expect("third reopen failed");
    assert_eq!(
        sorted_buckets(&reopened),
        vec!["empty".to_string(), "zz".to_string()],
        "third reopen must preserve both empty buckets"
    );
}

fn sorted_buckets(tree: &BTree) -> Vec<String> {
    let mut buckets = tree.buckets().expect("list buckets failed");
    buckets.sort();
    buckets
}
