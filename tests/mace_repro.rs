#[test]
#[cfg(not(windows))]
fn reproduce_mace_corruption() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use btree_store::BTree;
    use std::path::Path;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("mace_repro.db");
    if Path::new(&path).exists() {
        let _ = std::fs::remove_file(&path);
    }

    let btree = Arc::new(BTree::open(&path)?);
    let bucket = "mace_bucket";
    btree.new_bucket(bucket, false)?;

    let mut handles = Vec::new();

    // Thread 1: only performs Put (simulating a flusher).
    let put_tree = btree.clone();
    handles.push(thread::spawn(move || {
        for i in 0..10_000 {
            let key = format!("put_only_key_{i}");
            let value = vec![1u8; 100];
            put_tree
                .exec(bucket, |txn| txn.put(key.as_bytes(), &value))
                .unwrap_or_else(|error| panic!("THREAD 1 REPRODUCED FAILURE: {error:?}"));
        }
    }));

    // Thread 2: Put + Del (simulating a GC rewrite followed by reclamation).
    let delete_tree = btree.clone();
    handles.push(thread::spawn(move || {
        for i in 0..10_000 {
            let key = format!("mixed_key_{i}");
            let value = vec![2u8; 100];

            delete_tree
                .exec(bucket, |txn| txn.put(key.as_bytes(), &value))
                .unwrap_or_else(|error| panic!("THREAD 2 PUT FAILURE at iteration {i}: {error:?}"));
            delete_tree
                .exec(bucket, |txn| txn.del(key.as_bytes()))
                .unwrap_or_else(|error| {
                    panic!("THREAD 2 DELETE FAILURE at iteration {i}: {error:?}")
                });
        }
    }));

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}
