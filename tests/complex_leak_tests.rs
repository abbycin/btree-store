use btree_store::BTree;
use std::fs;
use std::path::Path;

#[test]
fn test_complex_leak_and_reuse() {
    let db_path = "complex_leak.db";
    let pending_path = "complex_leak.pending";
    if Path::new(db_path).exists() {
        fs::remove_file(db_path).unwrap();
    }
    if Path::new(pending_path).exists() {
        fs::remove_file(pending_path).unwrap();
    }

    // 1. Setup: Create a large value
    {
        let bt = BTree::open(db_path).unwrap();
        let bucket = bt.new_bucket("default").unwrap();

        bucket.put(b"large", &vec![0u8; 10000]).unwrap();
        bt.commit().unwrap(); // Seq 2
    }

    // 2. Simulate a partial commit:
    // We want to simulate: SB updated to Seq 3, but pages not yet freed.
    {
        let bt = BTree::open(db_path).unwrap();
        let bucket = bt.get_bucket("default").unwrap();

        bucket.put(b"large", b"new_small").unwrap();
        bt.commit().unwrap(); // This normally cleans up.
    }

    // Let's do a better one: Manually construct a valid log for an existing SB
    let _current_seq = {
        let bt = BTree::open(db_path).unwrap();
        let bucket = bt.new_bucket("temp").unwrap();
        bucket.put(b"k", b"v").unwrap();
        bt.commit().unwrap();
        // After 2 commits (init + 2 puts), seq should be 4
        // (Init=1, 1st commit=2, 2nd commit=3, 3rd commit=4)
        // Wait, init is 1, so 3 commits total = seq 4.
        4
    };

    // Simulate: Seq 4 is committed, but PID 500 (hypothetical) was never freed.
    let mut log_content = Vec::new();
    // Header: seq (8) + nr_freed (4) + nr_alloc (4) + checksum (4) + padding (4) = 24 bytes

    // Construct data payload (entries)
    let mut data = Vec::new();
    data.extend_from_slice(&500u64.to_le_bytes()); // page_id
    data.extend_from_slice(&1u32.to_le_bytes()); // nr_pages

    let checksum = crc32c::crc32c(&data);

    log_content.extend_from_slice(&4u64.to_le_bytes()); // seq
    log_content.extend_from_slice(&1u32.to_le_bytes()); // nr_freed = 1
    log_content.extend_from_slice(&0u32.to_le_bytes()); // nr_alloc = 0
    log_content.extend_from_slice(&checksum.to_le_bytes()); // checksum
    log_content.extend_from_slice(&[0u8; 4]); // padding

    log_content.extend_from_slice(&data);

    fs::write(pending_path, log_content).unwrap();

    {
        // This open should trigger recovery for Seq 4
        let _bt = BTree::open(db_path).unwrap();
    }

    assert!(
        !Path::new(pending_path).exists(),
        "Log should be replayed and deleted"
    );

    fs::remove_file(db_path).unwrap();
}

// ... other tests omitted for brevity, but I should update them too.
// I'll update the whole file.

#[test]
fn test_stale_log_ignored() {
    let db_path = "stale_log.db";
    let pending_path = "stale_log.pending";
    if Path::new(db_path).exists() {
        fs::remove_file(db_path).unwrap();
    }

    let _seq = {
        let bt = BTree::open(db_path).unwrap();
        let bucket = bt.new_bucket("default").unwrap();
        bucket.put(b"a", b"b").unwrap();
        bt.commit().unwrap();
        2 // Initial(1) + Commit(1) = 2
    };

    // Log with OLD seq (1)
    let mut log_content = Vec::new();
    let data = vec![0u8; 12]; // Dummy entry (8+4)
    let checksum = crc32c::crc32c(&data);

    log_content.extend_from_slice(&1u64.to_le_bytes()); // seq
    log_content.extend_from_slice(&1u32.to_le_bytes()); // nr_freed = 1
    log_content.extend_from_slice(&0u32.to_le_bytes()); // nr_alloc = 0
    log_content.extend_from_slice(&checksum.to_le_bytes()); // checksum
    log_content.extend_from_slice(&[0u8; 4]); // padding

    log_content.extend_from_slice(&data);

    fs::write(pending_path, log_content).unwrap();

    {
        let _bt = BTree::open(db_path).unwrap();
    }

    // Log should be deleted because it's stale (Seq 1 < Seq 2)
    assert!(!Path::new(pending_path).exists());

    fs::remove_file(db_path).unwrap();
}

#[test]
fn test_future_log_ignored() {
    let db_path = "future_log.db";
    let pending_path = "future_log.pending";
    if Path::new(db_path).exists() {
        fs::remove_file(db_path).unwrap();
    }

    {
        let _bt = BTree::open(db_path).unwrap();
    }

    // Log with FUTURE seq (10)
    let mut log_content = Vec::new();
    let data = vec![0u8; 12];
    let checksum = crc32c::crc32c(&data);

    log_content.extend_from_slice(&10u64.to_le_bytes()); // seq
    log_content.extend_from_slice(&1u32.to_le_bytes()); // nr_freed = 1
    log_content.extend_from_slice(&0u32.to_le_bytes()); // nr_alloc = 0
    log_content.extend_from_slice(&checksum.to_le_bytes()); // checksum
    log_content.extend_from_slice(&[0u8; 4]); // padding

    log_content.extend_from_slice(&data);

    fs::write(pending_path, log_content).unwrap();

    {
        let _bt = BTree::open(db_path).unwrap();
    }

    // Log should be deleted because Seq 10 > current Seq
    assert!(!Path::new(pending_path).exists());

    fs::remove_file(db_path).unwrap();
}

#[test]
fn test_empty_tree_cycle() {
    let db_path = "empty_cycle.db";
    if Path::new(db_path).exists() {
        fs::remove_file(db_path).unwrap();
    }

    {
        let bt = BTree::open(db_path).unwrap();
        let bucket = bt.new_bucket("default").unwrap();
        bt.commit().unwrap();

        bucket.put(b"k1", b"v1").unwrap();
        bucket.put(b"k2", b"v2").unwrap();
        bt.commit().unwrap();

        bucket.del(b"k1").unwrap();
        bucket.del(b"k2").unwrap();
        bt.commit().unwrap();
    }

    // Re-open and check if it's really empty
    {
        let bt = BTree::open(db_path).unwrap();
        let bucket = bt.get_bucket("default").unwrap();

        assert!(bucket.get(b"k1").is_err());
        assert!(bucket.get(b"k2").is_err());

        // Put something new back
        bucket.put(b"k3", b"v3").unwrap();
        bt.commit().unwrap();
    }

    {
        let bt = BTree::open(db_path).unwrap();
        let bucket = bt.get_bucket("default").unwrap();
        assert_eq!(bucket.get(b"k3").unwrap(), b"v3");
    }

    fs::remove_file(db_path).unwrap();
}
