//! Read/write non-blocking behavior of the MVCC concurrency model.
//!
//! Readers pin an epoch snapshot instead of taking the writer lock, so a
//! writer must never be blocked by an in-flight view, and a view's fixed
//! snapshot must never observe pages reused by later generations.

use btree_store::BTree;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

const KEY_COUNT: u32 = 200;

fn seed(tree: &BTree, bucket: &str, val: &[u8]) {
    tree.exec(bucket, |txn| {
        for i in 0..KEY_COUNT {
            txn.put(format!("k{i:05}").as_bytes(), val).unwrap();
        }
        Ok(())
    })
    .unwrap();
}

/// A reader that keeps its view open across several writer commits must keep
/// seeing its fixed snapshot; every later commit stays visible to newer views.
#[test]
fn snapshot_consistency_across_commits() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("snapshot-consistency.db");
    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("b", false).unwrap();
    seed(&tree, "b", b"v0");

    let ready = Arc::new(Barrier::new(2));
    let reader_done = Arc::new(Barrier::new(2));

    let reader_tree = tree.clone();
    let ready_r = ready.clone();
    let done_r = reader_done.clone();
    let reader = thread::spawn(move || {
        reader_tree
            .view("b", |txn| {
                assert_eq!(txn.get(b"k00000").unwrap(), b"v0");
                ready_r.wait();
                // writers commit v1/v2/v3 while this view stays open
                done_r.wait();
                for i in 0..KEY_COUNT {
                    let k = format!("k{i:05}");
                    assert_eq!(txn.get(k.as_bytes()).unwrap(), b"v0", "fixed snapshot");
                }
                Ok::<_, btree_store::Error>(())
            })
            .unwrap();
    });

    ready.wait();
    for v in [b"v1".as_slice(), b"v2".as_slice(), b"v3".as_slice()] {
        seed(&tree, "b", v);
    }
    reader_done.wait();
    reader.join().unwrap();
    tree.view("b", |txn| {
        assert_eq!(txn.get(b"k00000").unwrap(), b"v3");
        Ok(())
    })
    .unwrap();
}

/// A reader pinned on an old generation must not have its pages reused while
/// later commits retire and promote those pages. The third commit would reuse
/// the reader's pages if the epoch-gated promotion were missing.
#[test]
fn page_reuse_race_preserves_reader_snapshot() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("page-reuse-race.db");
    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("b", false).unwrap();
    seed(&tree, "b", b"v0");

    let ready = Arc::new(Barrier::new(2));
    let writer_done = Arc::new(Barrier::new(2));

    let reader_tree = tree.clone();
    let ready_r = ready.clone();
    let done_r = writer_done.clone();
    let reader = thread::spawn(move || {
        reader_tree
            .view("b", |txn| {
                let _ = txn.get(b"k00000").unwrap();
                ready_r.wait();
                done_r.wait();
                // re-read every key from the fixed snapshot after the writer
                // retired (commit 1) and would have promoted (commit 2) and
                // reused (commit 3) the v0 pages
                for i in 0..KEY_COUNT {
                    let k = format!("k{i:05}");
                    assert_eq!(txn.get(k.as_bytes()).unwrap(), b"v0", "reader page reused");
                }
                Ok::<_, btree_store::Error>(())
            })
            .unwrap();
    });

    ready.wait();
    seed(&tree, "b", b"v1");
    seed(&tree, "b", b"v2");
    seed(&tree, "b", b"v3");
    writer_done.wait();
    reader.join().unwrap();
    tree.view("b", |txn| {
        assert_eq!(txn.get(b"k00000").unwrap(), b"v3");
        Ok(())
    })
    .unwrap();
}

/// More concurrent views than the fixed 256-slot registry: the overflow path
/// must serve every view correct data.
#[test]
fn more_than_256_concurrent_views_are_consistent() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("overflow-views.db");
    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("b", false).unwrap();
    seed(&tree, "b", b"v0");

    const THREADS: usize = 300;
    let ready = Arc::new(Barrier::new(THREADS + 1));
    let release = Arc::new(Barrier::new(THREADS + 1));
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let tree = tree.clone();
        let ready = ready.clone();
        let release = release.clone();
        handles.push(thread::spawn(move || {
            tree.view("b", |txn| {
                ready.wait();
                release.wait();
                assert_eq!(txn.get(b"k00000").unwrap(), b"v0");
                Ok::<_, btree_store::Error>(())
            })
            .unwrap();
        }));
    }
    ready.wait();
    release.wait();
    for h in handles {
        h.join().unwrap();
    }
}
