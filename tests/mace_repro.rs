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

    // 线程 1: 只负责 Put (模拟 Flusher)
    let bt1 = btree.clone();
    handles.push(thread::spawn(move || {
        for i in 0..10000 {
            // 增加循环次数以加强压力测试
            let key = format!("put_only_key_{}", i);
            let val = vec![1u8; 100];
            if let Err(e) = bt1.exec(bucket, |txn| txn.put(key.as_bytes(), &val)) {
                if e.to_string().contains("Corruption") {
                    panic!("THREAD 1 REPRODUCED CORRUPTION: {:?}", e);
                }
            }
        }
    }));

    // 线程 2: Put + Del (模拟 GC)
    let bt2 = btree.clone();
    handles.push(thread::spawn(move || {
        for i in 0..10000 {
            // 增加循环次数以加强压力测试
            let key = format!("mixed_key_{}", i);
            let val = vec![2u8; 100];

            // Put
            let _ = bt2.exec(bucket, |txn| txn.put(key.as_bytes(), &val));

            // Del (触发 shrink_slot)
            if let Err(e) = bt2.exec(bucket, |txn| txn.del(key.as_bytes())) {
                if e.to_string().contains("Corruption") {
                    panic!("THREAD 2 REPRODUCED CORRUPTION at iteration {}: {:?}", i, e);
                }
            }
        }
    }));

    for h in handles {
        h.join().unwrap();
    }

    Ok(())
}
