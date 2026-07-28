use btree_store::{BTree, MetaNode, OpenError};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::TempDir;

const PAGE_SIZE: usize = 4096;

fn read_latest_meta(file: &mut std::fs::File) -> MetaNode {
    let mut pages = [[0u8; PAGE_SIZE]; 2];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut pages[0]).unwrap();
    file.read_exact(&mut pages[1]).unwrap();
    let meta0 = MetaNode::from_slice(&pages[0]);
    let meta1 = MetaNode::from_slice(&pages[1]);
    if meta0.seq >= meta1.seq { meta0 } else { meta1 }
}

fn open_error(path: &std::path::Path, context: &str) -> OpenError {
    match BTree::open(path) {
        Ok(_) => panic!("{context}"),
        Err(error) => error,
    }
}

fn assert_meta_mutation_rejected(name: &str, expected_code: &str, mutate: impl Fn(&mut MetaNode)) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join(format!("meta-{name}.db"));
    drop(BTree::open(&path).unwrap());

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    for offset in [0, PAGE_SIZE as u64] {
        let mut page = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut page).unwrap();
        let mut meta = MetaNode::from_slice(&page);
        mutate(&mut meta);
        meta.update_checksum();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(meta.as_page_slice()).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);

    let error = open_error(&path, "mutated meta must not publish a handle");
    assert!(
        matches!(error, OpenError::Corruption(ref report) if report.code == expected_code),
        "unexpected error for {name}: {error:?}"
    );
}

fn assert_extent_mutation_rejected(
    name: &str,
    expected_code: &str,
    mutate: impl FnOnce(&MetaNode, &mut [u8; PAGE_SIZE]),
) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join(format!("extent-{name}.db"));
    {
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("bucket", false).unwrap();
        for key in [b"key-1".as_slice(), b"key-2", b"key-3"] {
            tree.exec("bucket", |txn| txn.put(key, b"value")).unwrap();
        }
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let meta = read_latest_meta(&mut file);
    assert_ne!(meta.reusable_root, 0);
    let mut page = [0u8; PAGE_SIZE];
    file.seek(SeekFrom::Start(
        u64::from(meta.reusable_root) * PAGE_SIZE as u64,
    ))
    .unwrap();
    file.read_exact(&mut page).unwrap();
    mutate(&meta, &mut page);
    file.seek(SeekFrom::Start(
        u64::from(meta.reusable_root) * PAGE_SIZE as u64,
    ))
    .unwrap();
    file.write_all(&page).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let error = open_error(&path, "mutated extent must not publish a handle");
    assert!(
        matches!(error, OpenError::Corruption(ref report) if report.code == expected_code),
        "unexpected error for {name}: {error:?}"
    );
}

#[test]
fn invalid_meta_returns_open_corruption_without_aborting() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("invalid-meta.db");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.write_all(&vec![0xa5; PAGE_SIZE * 2]).unwrap();
    file.sync_all().unwrap();

    // Corrupt metadata is an ordinary open error; it must not abort the caller
    // before the structured corruption report can be inspected.
    assert!(matches!(
        open_error(&path, "invalid meta must not publish a handle"),
        OpenError::Corruption(ref report) if report.page_kind == "meta" && report.pid.is_none()
    ));
}

#[test]
fn new_database_reserves_both_meta_pages_and_uses_initial_format_version() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("initial-format.db");
    drop(BTree::open(&path).unwrap());

    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        (PAGE_SIZE * 2) as u64
    );
    let mut file = OpenOptions::new().read(true).open(&path).unwrap();
    let meta = read_latest_meta(&mut file);
    assert_eq!(meta.format_version, 1);
    assert_eq!(meta.reusable_root, 0);
}

#[test]
fn meta_native_record_has_checksum_at_final_word() {
    let meta = MetaNode::new();
    let page = meta.as_page_slice();
    assert_eq!(page.len(), 40);
    assert_eq!(
        &page[36..40],
        &meta.checksum.to_ne_bytes(),
        "the 40-byte MetaNode stores its checksum in the final u32"
    );
}

#[test]
fn truncated_second_meta_falls_back_to_first_valid_meta() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("truncated-meta.db");
    // Recovery must select the complete MetaNode together with its reusable
    // and retired allocator roots.

    drop(BTree::open(&path).unwrap());
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(PAGE_SIZE as u64)
        .unwrap();

    let tree = BTree::open(&path).expect("the complete first meta page must remain recoverable");
    tree.new_bucket("bucket", false).unwrap();
    tree.exec("bucket", |txn| txn.put(b"key", b"value"))
        .unwrap();
}

#[test]
fn checksum_valid_meta_is_not_rejected_by_structural_fields() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("checksum-only-meta.db");
    drop(BTree::open(&path).unwrap());

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    for offset in [0, PAGE_SIZE as u64] {
        let mut page = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut page).unwrap();
        let mut meta = MetaNode::from_slice(&page);
        meta.next_page_id = u32::MAX;
        meta.update_checksum();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(meta.as_page_slice()).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);

    drop(BTree::open(&path).expect("meta validation must stop after checksum"));
}

#[test]
fn opening_accepts_unchecked_extent_payload_bytes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("extent-unchecked.db");
    {
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("bucket", false).unwrap();
        for key in [b"key-1".as_slice(), b"key-2", b"key-3"] {
            tree.exec("bucket", |txn| txn.put(key, b"value")).unwrap();
        }
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let meta = read_latest_meta(&mut file);
    assert_ne!(meta.reusable_root, 0);
    let mut page = [0u8; PAGE_SIZE];
    file.seek(SeekFrom::Start(
        u64::from(meta.reusable_root) * PAGE_SIZE as u64,
    ))
    .unwrap();
    file.read_exact(&mut page).unwrap();
    page[PAGE_SIZE - 1] ^= 1;
    file.seek(SeekFrom::Start(
        u64::from(meta.reusable_root) * PAGE_SIZE as u64,
    ))
    .unwrap();
    file.write_all(&page).unwrap();
    file.sync_all().unwrap();

    drop(BTree::open(&path).expect("extent payload has no checksum validation"));
}

#[test]
fn extent_keeps_baseline_cycle_count_and_nonzero_checks() {
    assert_extent_mutation_rejected("cycle", "EXTENT_CYCLE", |meta, page| {
        page[0..4].copy_from_slice(&meta.reusable_root.to_le_bytes());
    });
    assert_extent_mutation_rejected("count", "INVALID_EXTENT_COUNT", |_meta, page| {
        page[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    });
    assert_extent_mutation_rejected("zero-extent", "INVALID_EXTENT_ENTRY", |_meta, page| {
        page[4..8].copy_from_slice(&1u32.to_le_bytes());
        page[8..12].fill(0);
        page[12..16].copy_from_slice(&1u32.to_le_bytes());
    });
}

#[test]
fn extent_rejects_out_of_range_payload_ids() {
    assert_extent_mutation_rejected("out-of-range", "EXTENT_OUT_OF_RANGE", |meta, page| {
        page[4..8].copy_from_slice(&1u32.to_le_bytes());
        page[8..12].copy_from_slice(&meta.next_page_id.to_le_bytes());
        page[12..16].copy_from_slice(&1u32.to_le_bytes());
    });
}

#[test]
fn extent_rejects_entries_covering_allocator_list_pages() {
    assert_extent_mutation_rejected("list-overlap", "ALLOCATOR_STATE_OVERLAP", |meta, page| {
        page[4..8].copy_from_slice(&1u32.to_le_bytes());
        page[8..12].copy_from_slice(&meta.reusable_root.to_le_bytes());
        page[12..16].copy_from_slice(&1u32.to_le_bytes());
    });
}

#[test]
fn allocator_rejects_invalid_list_page_roots() {
    assert_meta_mutation_rejected("invalid-root", "INVALID_EXTENT_LIST_PAGE", |meta| {
        meta.reusable_root = 1;
    });
}

#[test]
fn allocator_rejects_cross_list_page_overlap() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("meta-cross-list-overlap.db");
    {
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("bucket", false).unwrap();
        for key in [b"key-1".as_slice(), b"key-2", b"key-3"] {
            tree.exec("bucket", |txn| txn.put(key, b"value")).unwrap();
        }
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    for offset in [0, PAGE_SIZE as u64] {
        let mut page = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut page).unwrap();
        let mut meta = MetaNode::from_slice(&page);
        assert_ne!(meta.reusable_root, 0);
        meta.retired_root = meta.reusable_root;
        meta.update_checksum();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(meta.as_page_slice()).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);

    let error = open_error(
        &path,
        "overlapping allocator list roots must not publish a handle",
    );
    assert!(
        matches!(error, OpenError::Corruption(ref report) if report.code == "ALLOCATOR_STATE_OVERLAP"),
        "unexpected error: {error:?}"
    );
}
