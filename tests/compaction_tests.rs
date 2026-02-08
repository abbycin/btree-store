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
