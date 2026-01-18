use btree_store::BTree;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

#[test]
fn test_complex_leak_and_reuse() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("complex_leak.db");
    let pending_path = temp_dir.path().join("complex_leak.pending");

    // 1. Setup
    {
        let bt = BTree::open(&db_path).unwrap();
        bt.exec("default", |txn| {
            txn.put(b"large", &vec![0u8; 10000]).unwrap();
            Ok(())
        })
        .unwrap();
    }

    // Determine current seq
    let current_seq = {
        let bt = BTree::open(&db_path).unwrap();
        bt.current_seq()
    };

    // Simulate: Seq N is committed, but PID 500 (hypothetical) was never freed.
    let mut log_content = Vec::new();
    let data = {
        let mut d = Vec::new();
        d.extend_from_slice(&500u64.to_le_bytes()); // PID 500
        d.extend_from_slice(&1u32.to_le_bytes()); // 1 page
        d.extend_from_slice(&[0u8; 4]); // Padding
        d
    };
    let checksum = crc32c::crc32c(&data);

    log_content.extend_from_slice(&current_seq.to_le_bytes());
    log_content.extend_from_slice(&1u32.to_le_bytes()); // nr_freed = 1
    log_content.extend_from_slice(&0u32.to_le_bytes()); // nr_alloc = 0
    log_content.extend_from_slice(&checksum.to_le_bytes()); // checksum
    log_content.extend_from_slice(&[0u8; 4]); // padding

    log_content.extend_from_slice(&data);

    fs::write(&pending_path, log_content).unwrap();

    {
        let _bt = BTree::open(&db_path).unwrap();
    }

    if Path::new(&pending_path).exists() {
        assert_eq!(fs::metadata(&pending_path).unwrap().len(), 0);
    }
}

#[test]
fn test_stale_log_ignored() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("stale.db");
    let pending_path = temp_dir.path().join("stale.pending");

    let current_seq = {
        let bt = BTree::open(&db_path).unwrap();
        bt.exec("default", |txn| {
            txn.put(b"a", b"b").unwrap();
            Ok(())
        })
        .unwrap();
        bt.current_seq()
    };

    // Log with OLD seq
    let mut log_content = Vec::new();
    let data = vec![0u8; 16]; // Dummy PendingEntry
    let checksum = crc32c::crc32c(&data);

    log_content.extend_from_slice(&(current_seq - 1).to_le_bytes());
    log_content.extend_from_slice(&1u32.to_le_bytes());
    log_content.extend_from_slice(&0u32.to_le_bytes());
    log_content.extend_from_slice(&checksum.to_le_bytes());
    log_content.extend_from_slice(&[0u8; 4]);
    log_content.extend_from_slice(&data);

    fs::write(&pending_path, log_content).unwrap();

    {
        let _bt = BTree::open(&db_path).unwrap();
    }
    if Path::new(&pending_path).exists() {
        assert_eq!(fs::metadata(&pending_path).unwrap().len(), 0);
    }
}

#[test]
fn test_empty_tree_cycle() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("empty_cycle.db");

    {
        let bt = BTree::open(&db_path).unwrap();
        bt.exec("default", |txn| {
            txn.put(b"k1", b"v1").unwrap();
            txn.put(b"k2", b"v2").unwrap();
            Ok(())
        })
        .unwrap();

        bt.exec("default", |txn| {
            txn.del(b"k1").unwrap();
            txn.del(b"k2").unwrap();
            Ok(())
        })
        .unwrap();
    }

    {
        let bt = BTree::open(&db_path).unwrap();
        bt.view("default", |txn| {
            assert!(txn.get(b"k1").is_err());
            assert!(txn.get(b"k2").is_err());
            Ok(())
        })
        .unwrap();

        bt.exec("default", |txn| {
            txn.put(b"k3", b"v3").unwrap();
            Ok(())
        })
        .unwrap();
    }

    {
        let bt = BTree::open(&db_path).unwrap();
        bt.view("default", |txn| {
            assert_eq!(txn.get(b"k3").unwrap(), b"v3");
            Ok(())
        })
        .unwrap();
    }
}
