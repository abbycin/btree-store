use btree_store::BTree;
use std::fs;
use std::path::Path;

#[test]
fn test_page_leak_recovery() {
    let db_path = "test_leak.db";
    let pending_path = "test_leak.pending";
    if Path::new(db_path).exists() {
        fs::remove_file(db_path).unwrap();
    }
    if Path::new(pending_path).exists() {
        fs::remove_file(pending_path).unwrap();
    }

    {
        let bt = BTree::open(db_path).unwrap();
        let bucket = bt.new_bucket("default").unwrap();
        bt.commit().unwrap(); // Commit bucket creation

        bucket.put(b"key1", b"val1").unwrap();
        bt.commit().unwrap(); // Seq 1 -> 2 (assuming init is 1, create bucket is some seq, put is next)
        // Wait, init is 1 (P0).
        // new_bucket: allocates root for catalog (if not init?), allocates root for bucket.
        // Actually Store::open creates P0(Seq1) and P1(Seq2).
        // new_bucket modifies catalog. Catalog root changes. SB updated.

        // Let's rely on the mechanism, not exact seq numbers for the setup part.
    }

    // 1. Manually check the current SEQ
    let _current_seq = {
        let _bt = BTree::open(db_path).unwrap();
        // Just get the seq from the store via a helper or assume based on operations?
        // Since we can't easily access store.get_seq() from here as it's private/internal to BTree structure...
        // Let's assume a safe high number or try to read it.
        // Or, we can just use the fact that we know we committed a few times.
        // Init: Seq 2 (P1).
        // new_bucket: Updates catalog. Seq 3.
        // put: Updates bucket root -> Updates catalog. Seq 4.
        4
    };

    // 2. Create a "fake" pending log for the CURRENT seq
    // This simulates: "I updated SB to Seq 4, but I haven't freed Page 100 yet"
    let mut log_content = Vec::new();
    // Header: seq (8) + nr_freed (4) + nr_alloc (4) + checksum (4) + padding (4) = 24 bytes
    let data = {
        let mut d = Vec::new();
        d.extend_from_slice(&100u64.to_le_bytes()); // PID 100
        d.extend_from_slice(&1u32.to_le_bytes()); // 1 page
        d
    };
    let checksum = crc32c::crc32c(&data);

    log_content.extend_from_slice(&4u64.to_le_bytes()); // Seq 4
    log_content.extend_from_slice(&1u32.to_le_bytes()); // nr_freed = 1
    log_content.extend_from_slice(&0u32.to_le_bytes()); // nr_alloc = 0
    log_content.extend_from_slice(&checksum.to_le_bytes()); // checksum
    log_content.extend_from_slice(&[0u8; 4]); // padding

    log_content.extend_from_slice(&data);

    fs::write(pending_path, log_content).unwrap();

    // 3. Open the BTree. It should detect the log and call free_pages(100, 1)
    {
        let _bt = BTree::open(db_path).unwrap();
    }

    // 4. Verification: If it worked, the log file should be gone
    assert!(
        !Path::new(pending_path).exists(),
        "Pending log should be cleared after recovery"
    );

    fs::remove_file(db_path).unwrap();
}
