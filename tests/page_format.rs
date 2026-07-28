use btree_store::{BTree, Error};
use tempfile::TempDir;

const PAGE_SIZE: usize = 4096;

#[test]
fn overflow_and_indirect_pages_round_trip_full_page_chunks() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("page-layout.db");
    let tree = BTree::open(&path).unwrap();
    tree.new_bucket("bucket", false).unwrap();

    let values = [
        vec![0x11; PAGE_SIZE - 1],
        vec![0x22; PAGE_SIZE],
        vec![0x33; PAGE_SIZE + 1],
        vec![0x44; PAGE_SIZE * 5],
        vec![0x55; PAGE_SIZE * 5 + 1],
        vec![0x66; PAGE_SIZE * 1_023 + 17],
    ];
    assert_eq!(values.len(), 6);

    tree.exec("bucket", |txn| {
        for (index, value) in values.iter().enumerate() {
            txn.put((index as u32).to_be_bytes(), value)?;
        }
        Ok::<_, Error>(())
    })
    .unwrap();

    tree.view("bucket", |txn| {
        for (index, expected) in values.iter().enumerate() {
            assert_eq!(txn.get((index as u32).to_be_bytes()).unwrap(), *expected);
        }
        Ok::<_, Error>(())
    })
    .unwrap();

    drop(tree);
    let reopened = BTree::open(&path).unwrap();
    reopened
        .view("bucket", |txn| {
            for (index, expected) in values.iter().enumerate() {
                assert_eq!(txn.get((index as u32).to_be_bytes()).unwrap(), *expected);
            }
            Ok::<_, Error>(())
        })
        .unwrap();
}
