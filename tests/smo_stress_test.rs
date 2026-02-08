use btree_store::{BTree, Error};
use rand::{Rng, seq::SliceRandom};
use std::collections::HashMap;
use tempfile::TempDir;

#[test]
fn test_smo_stress() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("smo_stress.db");

    let btree = BTree::open(&db_path).expect("failed to open btree");

    let mut expected = HashMap::new();
    let mut rng = rand::rng();

    let num_ops = 5000;
    let mut keys: Vec<Vec<u8>> = Vec::new();

    // Phase 1: Random Insertions
    btree
        .exec("stress", |txn| {
            for i in 0..num_ops {
                let key = format!("key_{:08}_{}", i, rng.random::<u32>()).into_bytes();
                let val = format!("val_{}", i).into_bytes();

                txn.put(&key, &val).expect("put failed");
                expected.insert(key.clone(), val);
                keys.push(key);
            }
            Ok(())
        })
        .expect("commit failed");

    // Verification
    btree
        .view("stress", |txn| {
            for (k, v) in &expected {
                let res = txn.get(k).expect("get failed");
                assert_eq!(res, *v);
            }
            Ok(())
        })
        .unwrap();

    // Phase 2: Random Deletions and Overwrites
    keys.shuffle(&mut rng);
    let (to_delete, to_overwrite) = keys.split_at(keys.len() / 2);

    btree
        .exec("stress", |txn| {
            for k in to_delete {
                txn.del(k).expect("del failed");
                expected.remove(k);
            }
            Ok(())
        })
        .expect("commit failed");

    btree
        .exec("stress", |txn| {
            for k in to_overwrite {
                let new_val = b"new_value".to_vec();
                txn.put(k, &new_val).expect("put failed");
                expected.insert(k.clone(), new_val);
            }
            Ok(())
        })
        .expect("commit failed");

    // Final Verification
    btree
        .view("stress", |txn| {
            for (k, v) in &expected {
                let res = txn.get(k).expect("get failed");
                assert_eq!(res, *v);
            }
            // Ensure deleted keys are gone
            for k in to_delete {
                match txn.get(k) {
                    Err(Error::NotFound) => {}
                    _ => panic!("key should be deleted"),
                }
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_sequential_split_stress() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("seq_stress.db");

    let btree = BTree::open(&db_path).expect("failed to open btree");

    // Sequential keys often trigger edge cases in splitting
    btree
        .exec("seq", |txn| {
            for i in 0..10000 {
                let key = format!("{:010}", i).into_bytes();
                txn.put(&key, &key).expect("put failed");
            }
            Ok(())
        })
        .unwrap();

    btree
        .view("seq", |txn| {
            for i in 0..10000 {
                let key = format!("{:010}", i).into_bytes();
                let res = txn.get(&key).expect("get failed");
                assert_eq!(res, key);
            }
            Ok(())
        })
        .unwrap();
}
