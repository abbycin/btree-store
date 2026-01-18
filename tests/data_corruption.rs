use btree_store::{BTree, Result};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
#[cfg(not(windows))]
fn reproduce_btree_corruption() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("reproduce_manifest.db");

    let btree = Arc::new(BTree::open(path)?);

    // 初始化多个桶
    let buckets = ["bucket_1", "bucket_2", "bucket_3", "bucket_4", "bucket_5"];
    for b in buckets {
        btree.exec(b, |_| Ok(()))?;
    }

    let mut handles = vec![];

    // 模拟 mace 的并发场景：Flusher 和 GC 同时更新不同的元数据桶
    for t in 0..4 {
        let btree_clone = btree.clone();
        let handle = thread::spawn(move || {
            for i in 0..10000 {
                let bucket = buckets[i % buckets.len()];
                let key = format!("thread_{}_key_{}", t, i);
                let val = vec![t as u8; 3840];

                // 模拟 mace 的 internal_commit 逻辑：
                // 每次 exec 都是一次独立的磁盘提交
                if let Err(e) = btree_clone.exec(bucket, |txn| txn.put(key.as_bytes(), &val)) {
                    // 如果复现成功，这里应该会触发 Corruption 或 Panic
                    eprintln!("Thread {} failed at iteration {}: {:?}", t, i, e);
                    if e.to_string().contains("Corruption") {
                        panic!("REPRODUCED: BTree Corruption detected!");
                    }
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    Ok(())
}
