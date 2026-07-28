use btree_store::{BTree, Error, MAX_KEY_LEN};
use std::collections::BTreeMap;
use tempfile::TempDir;

fn padded_key(id: u32) -> Vec<u8> {
    let mut key = format!("{id:03}").into_bytes();
    key.resize(MAX_KEY_LEN, b'k');
    key
}

fn padded_value(id: u32) -> Vec<u8> {
    let mut value = format!("value-{id:03}").into_bytes();
    value.resize(128, b'v');
    value
}

fn collect_bucket(tree: &BTree, bucket: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    tree.view(bucket, |txn| {
        let mut iter = txn.iter();
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        let mut entries = Vec::new();
        while iter.next_ref(&mut key_buf, &mut val_buf) {
            entries.push((key_buf.clone(), val_buf.clone()));
        }
        Ok::<_, btree_store::Error>(entries)
    })
    .unwrap()
}

fn assert_bucket_matches(
    tree: &BTree,
    bucket: &str,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    check_range_end: u32,
) {
    let actual = collect_bucket(tree, bucket);
    let expected_entries: Vec<(Vec<u8>, Vec<u8>)> = expected
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(actual, expected_entries, "iterator order/content diverged");

    tree.view(bucket, |txn| {
        for key_id in 0..=check_range_end {
            let key = padded_key(key_id);
            match expected.get(key.as_slice()) {
                Some(value) => assert_eq!(txn.get(&key).unwrap(), *value),
                None => assert_eq!(txn.get(&key), Err(Error::KeyNotFound)),
            }
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn duplicate_separator_witness_survives_drop_reopen_and_continued_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("branch_separator_reopen.db");
    let bucket = "witness";
    let mut expected = BTreeMap::new();

    {
        let tree = BTree::open(&db_path).unwrap();
        tree.new_bucket(bucket, false).unwrap();

        tree.exec(bucket, |txn| {
            for key_id in 0..700u32 {
                let key = padded_key(key_id);
                let value = padded_value(key_id);
                txn.put(&key, &value)?;
                expected.insert(key, value);
            }
            Ok(())
        })
        .unwrap();

        tree.exec(bucket, |txn| {
            for key_id in 0..550u32 {
                let key = padded_key(key_id);
                txn.del(&key)?;
                expected.remove(key.as_slice());
            }
            Ok(())
        })
        .unwrap();

        tree.exec(bucket, |txn| {
            for key_id in [84u32, 85, 86, 87, 88, 89, 90, 83] {
                let key = padded_key(key_id);
                let value = padded_value(key_id);
                txn.put(&key, &value)?;
                expected.insert(key, value);
            }
            Ok(())
        })
        .unwrap();

        assert_bucket_matches(&tree, bucket, &expected, 720);
    }

    {
        let tree = BTree::open(&db_path).unwrap();
        assert_bucket_matches(&tree, bucket, &expected, 720);

        tree.exec(bucket, |txn| {
            let left_key = padded_key(82);
            let left_value = padded_value(82);
            txn.put(&left_key, &left_value)?;
            expected.insert(left_key, left_value);

            let delete_key = padded_key(620);
            txn.del(&delete_key)?;
            expected.remove(delete_key.as_slice());

            let right_key = padded_key(700);
            let right_value = padded_value(700);
            txn.put(&right_key, &right_value)?;
            expected.insert(right_key, right_value);
            Ok(())
        })
        .unwrap();

        assert_bucket_matches(&tree, bucket, &expected, 720);
    }

    {
        let tree = BTree::open(&db_path).unwrap();
        assert_bucket_matches(&tree, bucket, &expected, 720);
    }
    assert!(expected.contains_key(&padded_key(700)));
}
