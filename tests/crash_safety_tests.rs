use btree_store::{BTree, Error, MetaNode, OpenOptions, SyncMode};
use std::fs;
use std::io;
use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt as WinFileExt;

trait TestFileExt {
    fn tread_exact(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn twrite_all(&self, buf: &[u8], offset: u64) -> io::Result<()>;
}

impl TestFileExt for fs::File {
    #[cfg(unix)]
    fn tread_exact(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        FileExt::read_exact_at(self, buf, offset)
    }

    #[cfg(unix)]
    fn twrite_all(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        FileExt::write_all_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn tread_exact(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match WinFileExt::seek_read(self, buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    ));
                }
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    #[cfg(windows)]
    fn twrite_all(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match WinFileExt::seek_write(self, buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

#[test]
fn failed_exec_publishes_consumed_meta_before_returning_error() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("failed_exec_meta.db");
    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("data", false).unwrap();
    let before_seq = tree.current_seq();
    // The failed write still publishes allocator metadata before the caller
    // receives the closure error, so reopen can continue from that generation.

    let result: btree_store::Result<()> = tree.exec("data", |txn| {
        txn.put(b"aborted", vec![0x7a; 16 * 1024]).unwrap();
        Err(Error::KeyNotFound)
    });
    assert!(result.is_err());
    assert_eq!(tree.pending_pages(), (0, 0));

    let published_seq = tree.current_seq();
    assert!(published_seq > before_seq);
    drop(tree);

    let reopened = BTree::open(&db_path).unwrap();
    let reopened_seq = reopened.current_seq();
    assert_eq!(reopened_seq, published_seq);
    assert_eq!(
        reopened.view("data", |txn| txn.get(b"aborted")),
        Err(Error::KeyNotFound)
    );
    reopened
        .exec("data", |txn| txn.put(b"continued", b"ok"))
        .unwrap();
}

#[test]
fn test_torn_superblock_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("torn_sb.db");

    // 1. Create a database with several commits
    {
        let tree = BTree::open(&db_path).unwrap();
        tree.new_bucket("default", false).unwrap();
        tree.exec("default", |txn| {
            txn.put(b"stable", b"data").unwrap();
            Ok(())
        })
        .unwrap();

        tree.exec("default", |txn| {
            txn.put(b"latest", b"version").unwrap();
            Ok(())
        })
        .unwrap();
    }

    // 2. Simulate a torn write on the LATEST SB
    {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();

        let mut buf0 = [0u8; 4096];
        let mut buf1 = [0u8; 4096];
        file.tread_exact(&mut buf0, 0).unwrap();
        file.tread_exact(&mut buf1, 4096).unwrap();

        let seq0 = MetaNode::from_slice(&buf0).seq;
        let seq1 = MetaNode::from_slice(&buf1).seq;
        let offset_to_corrupt = if seq0 >= seq1 { 0 } else { 4096 };

        file.twrite_all(&[0u8; 100], offset_to_corrupt).unwrap();
    }

    // 3. Reopen and verify fallback to previous SB
    {
        let tree = BTree::open(&db_path).expect("Should open even with one corrupted SB");
        tree.commit().unwrap();
        tree.view("default", |txn| {
            assert_eq!(txn.get(b"stable").unwrap(), b"data");
            // If we fall back to the previous SB, "latest" might be gone depending on seq.
            Ok(())
        })
        .unwrap();
        tree.exec("default", |txn| txn.put(b"continued", b"ok"))
            .unwrap();
    }
}

#[test]
fn test_exec_error_revert() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("exec_error.db");

    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("data", false).unwrap();
    tree.exec("data", |txn| {
        txn.put(b"k1", b"v1").unwrap();
        Ok(())
    })
    .unwrap();

    let _: btree_store::Result<()> = tree.exec("data", |txn| {
        txn.put(b"k1", b"v2").unwrap();
        txn.put(b"k2", b"v2").unwrap();
        Err(Error::KeyNotFound)
    });

    tree.view("data", |txn| {
        assert_eq!(txn.get(b"k1").unwrap(), b"v1");
        assert_eq!(txn.get(b"k2"), Err(Error::KeyNotFound));
        Ok(())
    })
    .unwrap();
}

#[test]
fn data_sync_publication_reopens_allocator_state_and_continues() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("data_sync_reopen.db");
    let mut options = OpenOptions::new();
    options.sync_mode = SyncMode::Data;
    // This exercises durable reusable/retired list reconstruction across a
    // publication and a subsequent allocation.

    {
        let tree = options.open(&db_path).unwrap();
        tree.new_bucket("data", false).unwrap();
        tree.exec("data", |txn| txn.put(b"retired", b"old"))
            .unwrap();
        tree.exec("data", |txn| {
            txn.del(b"retired")?;
            txn.put(b"stable", b"value")
        })
        .unwrap();
    }

    let tree = options.open(&db_path).unwrap();
    tree.view("data", |txn| {
        assert_eq!(txn.get(b"retired"), Err(Error::KeyNotFound));
        assert_eq!(txn.get(b"stable")?, b"value");
        Ok::<_, Error>(())
    })
    .unwrap();
    tree.exec("data", |txn| txn.put(b"continued", b"ok"))
        .unwrap();
    drop(tree);

    let tree = options.open(&db_path).unwrap();
    tree.view("data", |txn| {
        assert_eq!(txn.get(b"stable")?, b"value");
        assert_eq!(txn.get(b"continued")?, b"ok");
        Ok::<_, Error>(())
    })
    .unwrap();
}

/// Deferred promotion must survive a crash: pages held in the retired set by
/// an in-flight reader are persisted with the generation, reopen restores a
/// valid allocator, and the pages become reusable again.
#[test]
fn deferred_promotion_survives_crash_and_reopen() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("deferred-crash.db");
    let tree = BTree::open(&db_path).unwrap();
    tree.new_bucket("data", false).unwrap();

    let seed = |tree: &BTree, val: &[u8]| {
        tree.exec("data", |txn| {
            for i in 0..1000u32 {
                txn.put(format!("k{i:04}").as_bytes(), val).unwrap();
            }
            Ok(())
        })
        .unwrap();
    };
    seed(&tree, b"v0");

    // a reader pins the v0 snapshot; the next commits must defer promotion
    let ready = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let reader_tree = tree.clone();
    let r_ready = ready.clone();
    let r_release = release.clone();
    let reader = thread::spawn(move || {
        reader_tree
            .view("data", |txn| {
                assert_eq!(txn.get(b"k0000").unwrap(), b"v0");
                r_ready.wait();
                r_release.wait();
                Ok::<_, btree_store::Error>(())
            })
            .unwrap();
    });

    ready.wait();
    seed(&tree, b"v1");
    seed(&tree, b"v2");
    release.wait();
    reader.join().unwrap();

    // crash: drop the live handle without a clean shutdown, then reopen
    drop(tree);
    let reopened = BTree::open(&db_path).unwrap();
    reopened
        .view("data", |txn| {
            assert_eq!(txn.get(b"k0000").unwrap(), b"v2");
            Ok(())
        })
        .unwrap();

    // the restored retired set is promoted and reused: the file stops growing
    seed(&reopened, b"v3");
    let size_after_v3 = fs::metadata(&db_path).unwrap().len();
    seed(&reopened, b"v4");
    let size_after_v4 = fs::metadata(&db_path).unwrap().len();
    assert!(
        size_after_v4 <= size_after_v3 + 64 * 1024,
        "restored retired pages must be reusable after reopen"
    );
    reopened
        .view("data", |txn| {
            assert_eq!(txn.get(b"k0000").unwrap(), b"v4");
            Ok(())
        })
        .unwrap();
    assert_eq!(reopened.pending_pages(), (0, 0));
}
