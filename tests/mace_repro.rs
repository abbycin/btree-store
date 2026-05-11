use btree_store::{BTree, Result};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
#[cfg(not(windows))]
fn reproduce_mace_corruption() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("mace_repro.db");
    if Path::new(&path).exists() {
        let _ = std::fs::remove_file(&path);
    }

    let btree = Arc::new(BTree::open(&path)?);
    let bucket = "mace_bucket";
    btree.exec(bucket, |_| Ok(()))?;

    let mut handles = vec![];

    // Thread 1: only performs Put (simulating Flusher)
    let bt1 = btree.clone();
    handles.push(thread::spawn(move || {
        for i in 0..10000 {
            // Increase loop count to intensify stress testing
            let key = format!("put_only_key_{}", i);
            let val = vec![1u8; 100];
            match bt1.exec(bucket, |txn| txn.put(key.as_bytes(), &val)) {
                Err(e) if e.to_string().contains("Corruption") => {
                    panic!("THREAD 1 REPRODUCED CORRUPTION: {:?}", e);
                }
                _ => {}
            }
        }
    }));

    // Thread 2: Put + Del (simulating GC)
    let bt2 = btree.clone();
    handles.push(thread::spawn(move || {
        for i in 0..10000 {
            // Increase loop count to intensify stress testing
            let key = format!("mixed_key_{}", i);
            let val = vec![2u8; 100];

            // Put
            let _ = bt2.exec(bucket, |txn| txn.put(key.as_bytes(), &val));

            // Del (triggers shrink_slot)
            match bt2.exec(bucket, |txn| txn.del(key.as_bytes())) {
                Err(e) if e.to_string().contains("Corruption") => {
                    panic!("THREAD 2 REPRODUCED CORRUPTION at iteration {}: {:?}", i, e);
                }
                _ => {}
            }
        }
    }));

    for h in handles {
        h.join().unwrap();
    }

    Ok(())
}
