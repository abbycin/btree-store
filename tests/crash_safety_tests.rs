use btree_store::{BTree, Error};
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
fn test_uncommitted_changes_revert() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("revert.db");

    // 1. Initial state
    {
        let tree = BTree::open(&db_path).unwrap();
        tree.exec("default", |txn| {
            txn.put(b"key1", b"val1").unwrap();
            Ok(())
        })
        .unwrap();
    }

    // 2. Perform changes but DON'T commit (via manual clone or separate process simulation)
    {
        let tree = BTree::open(&db_path).unwrap();
        // Here we can't use exec because it auto-commits.
        // We simulate a failed process by just opening and doing nothing,
        // or by testing that if exec fails, it reverts.
        let _: btree_store::Result<()> = tree.exec("default", |txn| {
            txn.put(b"key1", b"new_val").unwrap();
            txn.put(b"key2", b"val2").unwrap();
            Err(Error::Internal) // Abort
        });
    }
    // 3. Reopen and verify it reverted to committed state
    {
        let tree = BTree::open(&db_path).unwrap();
        tree.view("default", |txn| {
            assert_eq!(txn.get(b"key1").unwrap(), b"val1");
            assert_eq!(txn.get(b"key2"), Err(Error::NotFound));
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn test_torn_superblock_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("torn_sb.db");

    // 1. Create a database with several commits
    {
        let tree = BTree::open(&db_path).unwrap();
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

        let get_seq = |buf: &[u8]| -> u64 {
            let seq_bytes = &buf[64..72];
            u64::from_le_bytes(seq_bytes.try_into().unwrap())
        };

        let seq0 = get_seq(&buf0);
        let seq1 = get_seq(&buf1);
        let offset_to_corrupt = if seq0 >= seq1 { 0 } else { 4096 };

        file.twrite_all(&[0u8; 100], offset_to_corrupt).unwrap();
    }

    // 3. Reopen and verify fallback to previous SB
    {
        let tree = BTree::open(&db_path).expect("Should open even with one corrupted SB");
        tree.view("default", |txn| {
            assert_eq!(txn.get(b"stable").unwrap(), b"data");
            // If we fall back to the previous SB, "latest" might be gone depending on seq.
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn test_exec_error_revert() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("exec_error.db");

    let tree = BTree::open(&db_path).unwrap();
    tree.exec("data", |txn| {
        txn.put(b"k1", b"v1").unwrap();
        Ok(())
    })
    .unwrap();

    let _: btree_store::Result<()> = tree.exec("data", |txn| {
        txn.put(b"k1", b"v2").unwrap();
        txn.put(b"k2", b"v2").unwrap();
        Err(Error::Internal)
    });

    tree.view("data", |txn| {
        assert_eq!(txn.get(b"k1").unwrap(), b"v1");
        assert_eq!(txn.get(b"k2"), Err(Error::NotFound));
        Ok(())
    })
    .unwrap();
}
