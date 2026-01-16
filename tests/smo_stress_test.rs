use btree_store::BTree;
use rand::{Rng, seq::SliceRandom};
use std::collections::HashMap;

#[test]
fn test_smo_stress() {
    let mut path = std::env::temp_dir();
    path.push("smo_stress.db");
    let db_path = path.to_str().unwrap().to_string();
    let _ = std::fs::remove_file(&db_path);

    let btree = BTree::open(&db_path).expect("failed to open btree");
    let bucket = btree.new_bucket("stress").expect("failed to create bucket");

    let mut expected = HashMap::new();
    let mut rng = rand::thread_rng();

    let num_ops = 50000;
    let mut keys: Vec<Vec<u8>> = Vec::new();

    // Phase 1: Random Insertions
    for i in 0..num_ops {
        let key = format!("key_{:08}_{}", i, rng.r#gen::<u32>()).into_bytes();
        let val = format!("val_{}", i).into_bytes();

        bucket.put(&key, &val).expect("put failed");
        expected.insert(key.clone(), val);
        keys.push(key);

        if i % 1000 == 0 {
            btree.commit().expect("commit failed");
        }
    }
    btree.commit().expect("commit failed");

    // Verification
    for (k, v) in &expected {
        let res = bucket.get(k).expect("get failed");
        assert_eq!(res, *v);
    }

    // Phase 2: Random Deletions and Overwrites
    keys.shuffle(&mut rng);
    let (to_delete, to_overwrite) = keys.split_at(keys.len() / 2);

    for k in to_delete {
        bucket.del(k).expect("del failed");
        expected.remove(k);
    }
    btree.commit().expect("commit failed");

    for k in to_overwrite {
        let new_val = b"new_value".to_vec();
        bucket.put(k, &new_val).expect("put failed");
        expected.insert(k.clone(), new_val);
    }
    btree.commit().expect("commit failed");

    // Final Verification
    for (k, v) in &expected {
        let res = bucket.get(k).expect("get failed");
        assert_eq!(res, *v);
    }

    // Ensure deleted keys are gone
    for k in keys.split_at(keys.len() / 2).0 {
        match bucket.get(k) {
            Err(btree_store::Error::NotFound) => {}
            _ => panic!("key should be deleted"),
        }
    }

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_sequential_split_stress() {
    let mut path = std::env::temp_dir();
    path.push("seq_stress.db");
    let db_path = path.to_str().unwrap().to_string();
    let _ = std::fs::remove_file(&db_path);

    let btree = BTree::open(&db_path).expect("failed to open btree");
    let bucket = btree.new_bucket("seq").expect("failed to create bucket");

    // Sequential keys often trigger edge cases in splitting
    for i in 0..100000 {
        let key = format!("{:010}", i).into_bytes();
        bucket.put(&key, &key).expect("put failed");
    }
    btree.commit().expect("commit failed");

    for i in 0..100000 {
        let key = format!("{:010}", i).into_bytes();
        let res = bucket.get(&key).expect("get failed");
        assert_eq!(res, key);
    }

    let _ = std::fs::remove_file(db_path);
}
