use btree_store::BTree;
use tempfile::TempDir;

#[test]
fn test_bucket_iterator() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("iterator.db");
    let tree = BTree::open(&db_path).unwrap();

    let mut expected = std::collections::BTreeMap::new();
    tree.exec("my_bucket", |txn| {
        for i in 0..100 {
            let key = format!("key_{:03}", i).into_bytes();
            let value = format!("val_{:03}", i).into_bytes();
            txn.put(&key, &value).unwrap();
            expected.insert(key, value);
        }
        Ok(())
    })
    .unwrap();

    tree.view("my_bucket", |txn| {
        let mut iter = txn.iter();
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        let mut count = 0;
        while iter.next_ref(&mut key_buf, &mut val_buf) {
            assert_eq!(
                expected.get(key_buf.as_slice()).map(|ev| ev.as_slice()),
                Some(val_buf.as_slice())
            );
            count += 1;
        }
        assert_eq!(count, 100);
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_btree_buckets_list() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("buckets.db");
    let tree = BTree::open(&db_path).unwrap();

    let bucket_names = vec!["bucket_1", "bucket_2", "bucket_3"];
    for name in &bucket_names {
        tree.exec(*name, |_txn| Ok(())).unwrap();
    }

    let mut buckets = tree.buckets().unwrap();
    buckets.sort();
    assert_eq!(buckets, vec!["bucket_1", "bucket_2", "bucket_3"]);

    for name in &buckets {
        tree.exec(name, |txn| {
            txn.put(b"k", b"v").unwrap();
            assert_eq!(txn.get(b"k").unwrap(), b"v".to_vec());
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn test_empty_bucket_iterator() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("empty_iter.db");
    let tree = BTree::open(&db_path).unwrap();

    tree.exec("empty_bucket", |_txn| Ok(())).unwrap();

    tree.view("empty_bucket", |txn| {
        let mut iter = txn.iter();
        let mut k = Vec::new();
        let mut v = Vec::new();
        assert!(!iter.next_ref(&mut k, &mut v));
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_iterator_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("reopen_iter.db");

    {
        let tree = BTree::open(&db_path).unwrap();
        tree.exec("data", |txn| {
            txn.put(b"k1", b"v1").unwrap();
            txn.put(b"k2", b"v2").unwrap();
            Ok(())
        })
        .unwrap();
    }

    {
        let tree = BTree::open(&db_path).unwrap();
        tree.view("data", |txn| {
            let mut iter = txn.iter();
            let mut key_buf = Vec::new();
            let mut val_buf = Vec::new();
            let mut kv = Vec::new();
            while iter.next_ref(&mut key_buf, &mut val_buf) {
                kv.push((key_buf.clone(), val_buf.clone()));
            }
            kv.sort();
            assert_eq!(kv.len(), 2);
            assert_eq!(kv[0], (b"k1".to_vec(), b"v1".to_vec()));
            assert_eq!(kv[1], (b"k2".to_vec(), b"v2".to_vec()));
            Ok(())
        })
        .unwrap();
    }
}
