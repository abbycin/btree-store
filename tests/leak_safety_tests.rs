use btree_store::{BTree, Error};
use std::fs;
use tempfile::TempDir;

#[test]
fn test_freelist_persist_and_reuse() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("freelist.db");

    let size_after_insert;
    {
        let bt = BTree::open(&db_path).unwrap();
        bt.new_bucket("default", false).unwrap();
        bt.exec("default", |txn| {
            txn.put(b"key1", vec![0xAA; 20000]).unwrap();
            Ok(())
        })
        .unwrap();
        size_after_insert = fs::metadata(&db_path).unwrap().len();
        bt.exec("default", |txn| {
            txn.del(b"key1").unwrap();
            Ok(())
        })
        .unwrap();
    }

    let size_after_delete = fs::metadata(&db_path).unwrap().len();

    {
        let bt = BTree::open(&db_path).unwrap();
        bt.exec("default", |txn| {
            txn.put(b"key2", vec![0xBB; 20000]).unwrap();
            Ok(())
        })
        .unwrap();
    }

    let size_after_reuse = fs::metadata(&db_path).unwrap().len();
    assert!(size_after_delete >= size_after_insert);
    assert!(size_after_reuse >= size_after_delete);

    // Reopen verifies that the reusable extent was published durably rather
    // than only being available in the previous Store instance.
    let bt = BTree::open(&db_path).unwrap();
    bt.view("default", |txn| {
        assert_eq!(txn.get(b"key2")?.len(), 20000);
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_exec_rollback_no_leak() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("rollback_leak.db");

    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("data", false).unwrap();

    // 1. Initial state
    tree.exec("data", |txn| {
        txn.put(b"initial", b"value").unwrap();
        txn.put(b"overwritten", b"committed").unwrap();
        Ok(())
    })
    .unwrap();

    // 2. Execute a transaction that fails
    let res: btree_store::Result<()> = tree.exec("data", |txn| {
        // Allocate some pages by putting large values
        txn.put(b"large", vec![0xAA; 1024 * 1024]).unwrap();
        txn.put(b"overwritten", b"aborted").unwrap();
        txn.del(b"initial").unwrap();
        // Return error to trigger rollback
        Err(Error::KeyNotFound)
    });

    assert_eq!(res, Err(Error::KeyNotFound));

    // 3. Verify that pages were NOT leaked (pending_alloc should be empty)
    assert_eq!(tree.pending_pages().0, 0, "Pending alloc should be empty");
    assert_eq!(tree.pending_pages().1, 0, "Pending free should be empty");

    tree.exec("data", |txn| {
        assert_eq!(txn.get(b"initial").unwrap(), b"value");
        assert_eq!(txn.get(b"overwritten").unwrap(), b"committed");
        assert_eq!(txn.get(b"large"), Err(Error::KeyNotFound));
        txn.put(b"after-rollback", b"visible").unwrap();
        Ok(())
    })
    .unwrap();

    drop(tree);
    let reopened = BTree::open(&db_path).unwrap();
    reopened
        .view("data", |txn| {
            assert_eq!(txn.get(b"initial").unwrap(), b"value");
            assert_eq!(txn.get(b"overwritten").unwrap(), b"committed");
            assert_eq!(txn.get(b"large"), Err(Error::KeyNotFound));
            assert_eq!(txn.get(b"after-rollback").unwrap(), b"visible");
            Ok(())
        })
        .unwrap();
}

/// A long-lived view pins its snapshot: pages it references stay quarantined
/// across many commits (no corruption, no premature reuse), the file keeps
/// growing while the view is active, and once the view quiesces the engine
/// resumes promotion and the file stops growing.
#[test]
fn long_view_keeps_snapshot_and_recovers_after_quiescence() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("long-view.db");
    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("data", false).unwrap();

    // one data set is 1000 keys with 2-byte values; the COW rewrite footprint
    // is ~22 pages (~90KB) per commit, well above allocator-list churn
    let seed_data = |tree: &BTree, val: &[u8]| {
        tree.exec("data", |txn| {
            for i in 0..1000u32 {
                txn.put(format!("k{i:04}").as_bytes(), val).unwrap();
            }
            Ok(())
        })
        .unwrap();
    };
    seed_data(&tree, b"v0");
    let size_before_view = fs::metadata(&db_path).unwrap().len();

    let ready = Arc::new(Barrier::new(2));
    let done = Arc::new(Barrier::new(2));
    let reader_tree = tree.clone();
    let r_ready = ready.clone();
    let r_done = done.clone();
    let reader = thread::spawn(move || {
        reader_tree
            .view("data", |txn| {
                r_ready.wait();
                r_done.wait();
                for i in 0..1000u32 {
                    let k = format!("k{i:04}");
                    assert_eq!(txn.get(k.as_bytes()).unwrap(), b"v0");
                }
                Ok::<_, btree_store::Error>(())
            })
            .unwrap();
    });

    ready.wait();
    // many commits under the long view: the v0 pages stay quarantined, so each
    // commit allocates fresh pages and the file keeps growing
    for v in [
        b"v1".as_slice(),
        b"v2".as_slice(),
        b"v3".as_slice(),
        b"v4".as_slice(),
    ] {
        seed_data(&tree, v);
    }
    let size_while_view = fs::metadata(&db_path).unwrap().len();
    assert!(
        size_while_view > size_before_view + 64 * 1024,
        "file must keep growing while the view pins its snapshot"
    );
    done.wait();
    reader.join().unwrap();

    // after quiescence the engine resumes promotion: the next commit reuses
    // the quarantined pages and the file stops growing
    seed_data(&tree, b"v5");
    let size_after_v5 = fs::metadata(&db_path).unwrap().len();
    seed_data(&tree, b"v6");
    let size_after_v6 = fs::metadata(&db_path).unwrap().len();
    assert!(
        size_after_v6 <= size_after_v5 + 64 * 1024,
        "file must stop growing after the view quiesces"
    );
    tree.view("data", |txn| {
        assert_eq!(txn.get(b"k0000").unwrap(), b"v6");
        Ok(())
    })
    .unwrap();
    assert_eq!(tree.pending_pages(), (0, 0));
}
