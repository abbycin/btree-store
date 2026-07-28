use btree_store::{BTree, Error};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::{BTreeMap, BTreeSet};
use tempfile::TempDir;

type BucketModel = BTreeMap<Vec<u8>, Vec<u8>>;
type StoreModel = BTreeMap<String, BucketModel>;

fn assert_refines_model(tree: &BTree, model: &StoreModel, known_buckets: &BTreeSet<String>) {
    for bucket in known_buckets {
        let actual = tree.view(bucket, |txn| {
            let mut items = Vec::new();
            let mut key = Vec::new();
            let mut value = Vec::new();
            let mut iter = txn.iter();
            while iter.next_ref(&mut key, &mut value) {
                items.push((key.clone(), value.clone()));
            }
            Ok::<_, Error>(items)
        });

        match model.get(bucket) {
            Some(expected) => {
                let expected: Vec<_> = expected
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                assert_eq!(actual.unwrap(), expected, "bucket {bucket}");
            }
            None => assert_eq!(actual, Err(Error::BucketNotFound), "bucket {bucket}"),
        }
    }
}

#[test]
fn deterministic_storage_trace_refines_btreemap() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("refinement-model.db");
    let mut tree = BTree::open(&path).unwrap();
    let mut model = StoreModel::new();
    let known_buckets: BTreeSet<_> = ["alpha", "beta", "gamma", "delta"]
        .into_iter()
        .map(str::to_owned)
        .collect();

    // All known buckets exist up front; the model tracks each one as an empty
    // bucket map until the workload populates it.
    for bucket in &known_buckets {
        tree.new_bucket(bucket, false).unwrap();
        model.insert(bucket.clone(), BucketModel::new());
    }

    for bucket in ["alpha", "beta", "gamma"] {
        tree.exec(bucket, |txn| {
            for index in 0..24u32 {
                let key = format!("seed-{index:03}");
                let value = vec![(index as u8).wrapping_mul(17); 512 + index as usize * 23];
                txn.put(key.as_bytes(), &value)?;
            }
            Ok::<_, Error>(())
        })
        .unwrap();

        let bucket_model = model.get_mut(bucket).unwrap();
        for index in 0..24u32 {
            bucket_model.insert(
                format!("seed-{index:03}").into_bytes(),
                vec![(index as u8).wrapping_mul(17); 512 + index as usize * 23],
            );
        }
    }
    assert_refines_model(&tree, &model, &known_buckets);
    for _ in 0..2 {
        let value = tree
            .view("alpha", |txn| Ok::<_, Error>(txn.get(b"seed-007")))
            .unwrap();
        assert_eq!(
            value,
            Ok(model["alpha"].get(b"seed-007" as &[u8]).cloned().unwrap()),
        );
    }

    let aborted: btree_store::Result<()> = tree.exec("alpha", |txn| {
        txn.put(b"aborted", b"must-not-publish")?;
        Err(Error::KeyNotFound)
    });
    assert_eq!(aborted, Err(Error::KeyNotFound));

    tree.exec_multi(|multi| {
        multi.exec("alpha", |txn| txn.put(b"multi-a", b"committed"))?;
        let nested: btree_store::Result<()> = multi.exec("beta", |txn| {
            txn.put(b"nested-abort", b"must-not-publish")?;
            Err(Error::KeyNotFound)
        });
        assert_eq!(nested, Err(Error::KeyNotFound));
        multi.exec("gamma", |txn| txn.put(b"multi-g", b"committed"))?;
        Ok::<_, Error>(())
    })
    .unwrap();
    model
        .get_mut("alpha")
        .unwrap()
        .insert(b"multi-a".to_vec(), b"committed".to_vec());
    model
        .get_mut("gamma")
        .unwrap()
        .insert(b"multi-g".to_vec(), b"committed".to_vec());
    assert_refines_model(&tree, &model, &known_buckets);

    let mut rng = StdRng::seed_from_u64(0xD4_5EED_0123_4567);
    let buckets = ["alpha", "beta", "gamma", "delta"];

    for step in 0..72u32 {
        let bucket = buckets[rng.random_range(0..buckets.len())];
        let key = format!("key-{:03}", rng.random_range(0..32u32)).into_bytes();
        let value = vec![(step as u8).wrapping_mul(29); 96 + rng.random_range(0..1400usize)];

        match step % 8 {
            0 => {
                if !model.contains_key(bucket) {
                    tree.new_bucket(bucket, false).unwrap();
                    model.insert(bucket.to_owned(), BucketModel::new());
                }
                tree.exec(bucket, |txn| txn.put(&key, &value)).unwrap();
                model.get_mut(bucket).unwrap().insert(key, value);
            }
            1 => {
                if !model.contains_key(bucket) {
                    tree.new_bucket(bucket, false).unwrap();
                    model.insert(bucket.to_owned(), BucketModel::new());
                }
                let existed = model.get(bucket).unwrap().contains_key(&key);
                let updated = tree.exec(bucket, |txn| txn.update(&key, &value)).unwrap();
                assert_eq!(updated, existed);
                if existed {
                    model.get_mut(bucket).unwrap().insert(key, value);
                }
            }
            2 => {
                if !model.contains_key(bucket) {
                    assert_eq!(
                        tree.exec(bucket, |txn| txn.del(&key)),
                        Err(Error::BucketNotFound)
                    );
                } else {
                    let existed = model.get(bucket).unwrap().contains_key(&key);
                    let deleted = tree.exec(bucket, |txn| txn.del(&key));
                    if existed {
                        deleted.unwrap();
                        model.get_mut(bucket).unwrap().remove(&key);
                    } else {
                        assert_eq!(deleted, Err(Error::KeyNotFound));
                    }
                }
            }
            3 => {
                if model.contains_key(bucket) {
                    let aborted: btree_store::Result<()> = tree.exec(bucket, |txn| {
                        txn.put(&key, &value)?;
                        Err(Error::KeyNotFound)
                    });
                    assert_eq!(aborted, Err(Error::KeyNotFound));
                } else {
                    let aborted: btree_store::Result<()> = tree.exec(bucket, |_| {
                        panic!("closure must not run for a missing bucket")
                    });
                    assert_eq!(aborted, Err(Error::BucketNotFound));
                }
            }
            4 => {
                let other = buckets[(step as usize + 1) % buckets.len()];
                for name in [bucket, other] {
                    if !model.contains_key(name) {
                        tree.new_bucket(name, false).unwrap();
                        model.insert(name.to_owned(), BucketModel::new());
                    }
                }
                let other_key = format!("multi-{step:03}").into_bytes();
                tree.exec_multi(|multi| {
                    multi.exec(bucket, |txn| txn.put(&key, &value))?;
                    multi.exec(other, |txn| txn.put(&other_key, b"paired"))?;
                    Ok::<_, Error>(())
                })
                .unwrap();
                model.get_mut(bucket).unwrap().insert(key, value);
                model
                    .get_mut(other)
                    .unwrap()
                    .insert(other_key, b"paired".to_vec());
            }
            5 => {
                if model.contains_key(bucket) {
                    let aborted: btree_store::Result<()> = tree.exec_multi(|multi| {
                        multi.exec(bucket, |txn| txn.put(&key, &value))?;
                        Err(Error::KeyNotFound)
                    });
                    assert_eq!(aborted, Err(Error::KeyNotFound));
                } else {
                    let aborted: btree_store::Result<()> = tree.exec_multi(|multi| {
                        multi.exec(bucket, |_| {
                            panic!("closure must not run for a missing bucket")
                        })?;
                        Ok::<_, Error>(())
                    });
                    assert_eq!(aborted, Err(Error::BucketNotFound));
                }
            }
            6 => {
                let deleted = tree.del_bucket(bucket);
                if model.remove(bucket).is_some() {
                    deleted.unwrap();
                } else {
                    assert_eq!(deleted, Err(Error::BucketNotFound));
                }
            }
            7 => {
                drop(tree);
                tree = BTree::open(&path).unwrap();
            }
            _ => unreachable!(),
        }

        assert_refines_model(&tree, &model, &known_buckets);
    }

    drop(tree);
    let reopened = BTree::open(&path).unwrap();
    assert_refines_model(&reopened, &model, &known_buckets);
    assert_eq!(reopened.pending_pages(), (0, 0));
}
