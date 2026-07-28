use btree_store::BTree;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_reuse_within_txn() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("reuse_txn.db");
    // Transaction-local reuse must consume tracked reusable ownership without
    // exposing the deleted value page as a leaked or reachable page.

    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("default", false).unwrap();
    tree.exec("default", |txn| {
        // COW node writes exercise the single-page allocator path as well as
        // the multi-page overflow path covered below.
        txn.put(b"large1", vec![0u8; 12000]).unwrap();
        let size_after_put = fs::metadata(&db_path).unwrap().len();
        txn.del(b"large1").unwrap();
        txn.put(b"large2", vec![0u8; 12000]).unwrap();
        let size_after_reuse = fs::metadata(&db_path).unwrap().len();
        assert_eq!(size_after_reuse, size_after_put);
        assert!(txn.get(b"large1").is_err());
        assert_eq!(txn.get(b"large2").unwrap().len(), 12000);
        Ok(())
    })
    .unwrap();
    assert_eq!(tree.pending_pages(), (0, 0));

    drop(tree);
    let reopened = BTree::open(&db_path).unwrap();
    reopened
        .view("default", |txn| {
            assert_eq!(txn.get(b"large2")?.len(), 12000);
            assert_eq!(txn.get(b"large1"), Err(btree_store::Error::KeyNotFound));
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_empty_tree_cycle() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("empty_cycle.db");

    {
        let bt = BTree::open(&db_path).unwrap();
        bt.new_bucket("default", false).unwrap();
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
