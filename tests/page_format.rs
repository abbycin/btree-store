//! Value pagination across the v2 page boundaries.
//!
//! A v2 physical page carries 4092 bytes of content plus a 4-byte CRC trailer, so
//! the boundaries that matter here are the page content capacity, the five-page
//! direct/indirect switch and the 1022 page ids an indirect page holds.

use btree_store::{BTree, Error};
use tempfile::TempDir;

const PAGE_CONTENT: usize = 4092;
const INDIRECT_IDS_PER_PAGE: usize = 1022;

#[test]
fn value_pagination_round_trips_across_v2_page_boundaries() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("page-layout.db");
    let tree = BTree::open(&path).unwrap();
    tree.new_bucket("bucket", false).unwrap();

    let values = [
        vec![0x11; PAGE_CONTENT - 1],
        vec![0x22; PAGE_CONTENT],
        vec![0x33; PAGE_CONTENT + 1],
        vec![0x44; 5 * PAGE_CONTENT - 1],
        vec![0x55; 5 * PAGE_CONTENT],
        vec![0x66; 5 * PAGE_CONTENT + 1],
        vec![0x77; INDIRECT_IDS_PER_PAGE * PAGE_CONTENT + 1],
        vec![0x88; (INDIRECT_IDS_PER_PAGE + 1) * PAGE_CONTENT + 17],
    ];
    assert_eq!(values.len(), 8);

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
