use btree_store::BTree;

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.pending", path));
}

#[test]
fn test_bucket_iterator() {
    let path = "test_bucket_iter.db";
    cleanup(path);

    let tree = BTree::open(path).unwrap();
    let bucket = tree.new_bucket("my_bucket").unwrap();

    let mut expected = std::collections::BTreeMap::new();
    for i in 0..100 {
        let key = format!("key_{:03}", i).into_bytes();
        let value = format!("val_{:03}", i).into_bytes();
        bucket.put(&key, &value).unwrap();
        expected.insert(key, value);
    }
    tree.commit().unwrap();

    let mut iter = bucket.iter().unwrap();
    let mut count = 0;
    while let Some((k, v)) = iter.next() {
        assert_eq!(expected.get(k).map(|ev| ev.as_slice()), Some(v));
        count += 1;
    }
    assert_eq!(count, 100);

    cleanup(path);
}

#[test]
fn test_btree_iterator() {
    let path = "test_btree_iter.db";
    cleanup(path);

    let tree = BTree::open(path).unwrap();

    let bucket_names = vec!["bucket_1", "bucket_2", "bucket_3"];
    for name in &bucket_names {
        tree.new_bucket(*name).unwrap();
    }
    tree.commit().unwrap();

    let mut iter = tree.iter();
    let mut buckets = Vec::new();
    while let Some(bucket) = iter.next() {
        buckets.push(bucket);
    }
    // Explicitly drop iterator to release the read lock
    drop(iter);

    assert_eq!(buckets.len(), 3);

    for bucket in buckets {
        // Verify we can use the bucket for writes after iteration
        bucket.put(b"k", b"v").unwrap();
        assert_eq!(bucket.get(b"k").unwrap(), b"v".to_vec());
    }

    cleanup(path);
}

#[test]
fn test_empty_bucket_iterator() {
    let path = "test_empty_bucket_iter.db";
    cleanup(path);

    let tree = BTree::open(path).unwrap();
    let bucket = tree.new_bucket("empty_bucket").unwrap();
    tree.commit().unwrap();

    let mut iter = bucket.iter().unwrap();
    assert!(iter.next().is_none());

    cleanup(path);
}

#[test]
fn test_iterator_after_reopen() {
    let path = "test_iter_reopen.db";
    cleanup(path);

    {
        let tree = BTree::open(path).unwrap();
        let bucket = tree.new_bucket("data").unwrap();
        bucket.put(b"k1", b"v1").unwrap();
        bucket.put(b"k2", b"v2").unwrap();
        tree.commit().unwrap();
    }

    {
        let tree = BTree::open(path).unwrap();
        let bucket = tree.get_bucket("data").unwrap();
        let mut iter = bucket.iter().unwrap();

        let mut kv = Vec::new();
        while let Some((k, v)) = iter.next() {
            kv.push((k.to_vec(), v.to_vec()));
        }
        kv.sort();
        assert_eq!(kv.len(), 2);
        assert_eq!(kv[0], (b"k1".to_vec(), b"v1".to_vec()));
        assert_eq!(kv[1], (b"k2".to_vec(), b"v2".to_vec()));
    }

    cleanup(path);
}
