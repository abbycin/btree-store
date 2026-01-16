use btree_store::{BTree, Error};
use std::fs;
use std::io;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt as WinFileExt;

fn setup_temp_db(name: &str) -> String {
    let mut path = std::env::temp_dir();
    path.push(name);
    let path_str = path.to_str().unwrap().to_string();
    if path.exists() {
        fs::remove_file(&path).unwrap();
    }
    path_str
}

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
    let path = setup_temp_db("uncommitted_revert.db");

    // 1. Initial state
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.new_bucket("default").unwrap();
        tree.commit().unwrap(); // Commit the new bucket creation

        bucket.put(b"key1", b"val1").unwrap();
        tree.commit().unwrap();
    }

    // 2. Perform changes but DON'T commit
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.get_bucket("default").unwrap();

        bucket.put(b"key1", b"new_val").unwrap();
        bucket.put(b"key2", b"val2").unwrap();
        // tree.commit() is NOT called
    }

    // 3. Reopen and verify it reverted to committed state
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.get_bucket("default").unwrap();

        assert_eq!(bucket.get(b"key1").unwrap(), b"val1");
        assert_eq!(bucket.get(b"key2"), Err(Error::NotFound));
    }

    fs::remove_file(&path).ok();
}

#[test]
fn test_torn_superblock_recovery() {
    let path = setup_temp_db("torn_sb.db");

    // 1. Create a database with 2 commits to ensure both SBs are initialized
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.new_bucket("default").unwrap();
        tree.commit().unwrap(); // Commit creation, SB at 0

        bucket.put(b"stable", b"data").unwrap();
        tree.commit().unwrap(); // SB at 4096 (seq is higher)

        bucket.put(b"latest", b"version").unwrap();
        tree.commit().unwrap(); // SB at 0 
    }

    // 2. Simulate a torn write/corruption on the LATEST SB
    {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        // Read both SBs to find the latest
        let mut buf0 = [0u8; 4096];
        let mut buf1 = [0u8; 4096];
        file.tread_exact(&mut buf0, 0).unwrap();
        file.tread_exact(&mut buf1, 4096).unwrap();

        // Helper to get seq
        let get_seq = |buf: &[u8]| -> u64 {
            let seq_bytes = &buf[16..24]; // offset of seq in MetaNode
            u64::from_le_bytes(seq_bytes.try_into().unwrap())
        };

        let seq0 = get_seq(&buf0);
        let seq1 = get_seq(&buf1);

        let offset_to_corrupt = if seq0 >= seq1 { 0 } else { 4096 };

        // Corruption: write zeros to the latest SB
        file.twrite_all(&[0u8; 100], offset_to_corrupt).unwrap();
    }

    // 3. Reopen and verify.
    // NOTE: With the double-SB update fix for leaks, a commit writes TWICE.
    // Update 1: Commits data (Root ID), but with Old Free List.
    // Update 2: Commits New Free List.
    // If we corrupt Update 2, we fall back to Update 1.
    // Update 1 HAS the data. So we expect to see "latest" value.
    // We are testing that the DB is usable and consistent, even if we revert to the intermediate state.
    {
        let tree = BTree::open(&path).expect("Should open even with one corrupted SB");
        let bucket = tree.get_bucket("default").unwrap();

        assert_eq!(bucket.get(b"stable").unwrap(), b"data");
        // We expect the latest value to be present because the first SB update committed it.
        assert_eq!(bucket.get(b"latest").unwrap(), b"version");
    }

    fs::remove_file(&path).ok();
}

#[test]
fn test_large_value_cow_consistency() {
    let path = setup_temp_db("large_val_crash.db");
    let large_val = vec![0x42u8; 10000]; // Multi-page value
    let new_large_val = vec![0x43u8; 10000];

    // 1. Commit a large value
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.new_bucket("default").unwrap();

        bucket.put(b"large", &large_val).unwrap();
        tree.commit().unwrap();
    }

    // 2. Update large value but CRASH before commit
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.get_bucket("default").unwrap();

        bucket.put(b"large", &new_large_val).unwrap();
        // CRASH (no commit)
    }

    // 3. Reopen and verify old large value is still intact and uncorrupted
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.get_bucket("default").unwrap();

        let res = bucket.get(b"large").unwrap();
        assert_eq!(res, large_val);
    }

    fs::remove_file(&path).ok();
}

#[test]
fn test_root_split_crash_consistency() {
    let path = setup_temp_db("root_split_crash.db");

    // 1. Fill a leaf until it's nearly full
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.new_bucket("default").unwrap();

        for i in 0..50 {
            let k = format!("key{:03}", i);
            bucket.put(k.as_bytes(), b"value").unwrap();
        }
        tree.commit().unwrap();
    }

    // 2. Put a key that triggers root split, but DON'T commit
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.get_bucket("default").unwrap();

        // This will cause multiple page allocations and root change in memory
        for i in 50..150 {
            let k = format!("key{:03}", i);
            bucket.put(k.as_bytes(), b"value").unwrap();
        }
    }

    // 3. Verify original 50 keys are still there and structure is valid
    {
        let tree = BTree::open(&path).unwrap();
        let bucket = tree.get_bucket("default").unwrap();

        for i in 0..50 {
            let k = format!("key{:03}", i);
            assert_eq!(bucket.get(k.as_bytes()).unwrap(), b"value");
        }
        assert_eq!(bucket.get(b"key051"), Err(Error::NotFound));
    }

    fs::remove_file(&path).ok();
}
