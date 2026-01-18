use btree_store::BTree;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

#[test]
fn test_page_leak_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("leak.db");
    let pending_path = temp_dir.path().join("leak.pending");

    {
        let bt = BTree::open(&db_path).unwrap();
        bt.exec("default", |_txn| Ok(())).unwrap();
        bt.exec("default", |txn| {
            txn.put(b"key1", b"val1").unwrap();
            Ok(())
        })
        .unwrap();
    }

    // Determine current seq
    let current_seq = {
        let bt = BTree::open(&db_path).unwrap();
        bt.current_seq()
    };

    // Create a "fake" pending log for the CURRENT seq (Redo-Free)
    let mut log_content = Vec::new();
    let data = {
        let mut d = Vec::new();
        d.extend_from_slice(&100u64.to_le_bytes()); // PID 100
        d.extend_from_slice(&1u32.to_le_bytes()); // 1 page
        d.extend_from_slice(&[0u8; 4]); // Padding for PendingEntry
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

    // Open the BTree. It should detect the log and clear it
    {
        let _bt = BTree::open(&db_path).unwrap();
    }

    if Path::new(&pending_path).exists() {
        assert_eq!(
            fs::metadata(&pending_path).unwrap().len(),
            0,
            "Pending log should be cleared after recovery"
        );
    }
}

#[test]
fn test_exec_rollback_no_leak() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("rollback_leak.db");

    let tree = BTree::open(&db_path).unwrap();

    // 1. Initial state
    tree.exec("data", |txn| {
        txn.put(b"initial", b"value").unwrap();
        Ok(())
    })
    .unwrap();

    // 2. Execute a transaction that fails
    let res: btree_store::Result<()> = tree.exec("data", |txn| {
        // Allocate some pages by putting large values
        txn.put(b"large", &vec![0xAA; 1024 * 1024]).unwrap();
        // Return error to trigger rollback
        Err(btree_store::Error::Internal)
    });

    assert_eq!(res, Err(btree_store::Error::Internal));

    // 3. Verify that pages were NOT leaked (pending_alloc should be empty)
    assert_eq!(tree.pending_pages().0, 0, "Pending alloc should be empty");
}
