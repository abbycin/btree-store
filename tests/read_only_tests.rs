//! Read-only open acceptance tests: opening a database without write access must
//! create nothing, write nothing, coexist with other readers, and serve exactly the
//! generation a read-write handle serves.

mod common;

use btree_store::{BTree, Error, OpenError};
use common::{child_test_command, seal_page};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use tempfile::TempDir;

const PAGE: usize = 4096;
const SLOT_SIZE: usize = 32;
/// The v2 plain header: checksum, class word, element count, payload offset.
const PLAIN_HEADER_SIZE: usize = 16;
const CHILD_DIR_ENV: &str = "BTREE_READ_ONLY_CHILD_DIR";
const CHILD_MODE_ENV: &str = "BTREE_READ_ONLY_CHILD_MODE";

fn key(index: u32) -> Vec<u8> {
    format!("k{index:06}").into_bytes()
}

fn value(index: u32) -> Vec<u8> {
    let mut bytes = vec![0u8; 64];
    bytes[..4].copy_from_slice(&index.to_le_bytes());
    bytes
}

fn seed(tree: &BTree, range: std::ops::Range<u32>) {
    tree.exec("data", |txn| {
        for index in range {
            txn.put(key(index), value(index))?;
        }
        Ok(())
    })
    .unwrap();
}

fn fingerprint(path: &Path) -> (u64, u64, u128) {
    let bytes = fs::read(path).unwrap();
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in &bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let modified = fs::metadata(path)
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    (hash, bytes.len() as u64, modified)
}

fn open_read_only_err(path: &Path) -> OpenError {
    match BTree::open_read_only(path) {
        Ok(_) => panic!(
            "read-only open of {} unexpectedly succeeded",
            path.display()
        ),
        Err(error) => error,
    }
}

fn prepared(dir: &TempDir, name: &str) -> PathBuf {
    let path = dir.path().join(name);
    let tree = BTree::open(&path).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, 0..200);
    drop(tree);
    path
}

fn read_all(tree: &BTree, limit: u32) {
    tree.view("data", |txn| {
        for index in 0..limit {
            assert_eq!(txn.get(key(index))?.len(), 64);
        }
        Ok(())
    })
    .unwrap();
}

/// A read-only open never creates the file, and the path stays absent.
#[test]
fn read_only_open_does_not_create() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("absent.db");

    match open_read_only_err(&missing) {
        OpenError::Io(io) => assert_eq!(io.source_error().kind(), ErrorKind::NotFound),
        other => panic!("expected Io(NotFound), got {other:?}"),
    }
    assert!(!missing.exists(), "the path must still not exist");

    let created = dir.path().join("created.db");
    drop(BTree::open(&created).unwrap());
    assert!(fs::metadata(&created).unwrap().len() > 0);
}

#[test]
fn read_only_open_does_not_initialise_an_empty_file() {
    let dir = TempDir::new().unwrap();
    let empty = dir.path().join("empty.db");
    fs::write(&empty, b"").unwrap();

    let error = open_read_only_err(&empty);
    assert!(
        matches!(error, OpenError::Corruption(_) | OpenError::Io(_)),
        "an empty file must be a typed error, got {error:?}"
    );
    assert_eq!(
        fs::metadata(&empty).unwrap().len(),
        0,
        "the file must stay empty"
    );

    let initialised = dir.path().join("initialised.db");
    fs::write(&initialised, b"").unwrap();
    drop(BTree::open(&initialised).unwrap());
    assert!(fs::metadata(&initialised).unwrap().len() > 0);
}

/// Reading through a read-only handle writes nothing at all.
#[test]
fn read_only_open_writes_no_bytes() {
    let dir = TempDir::new().unwrap();
    let path = prepared(&dir, "readonly.db");
    let before = fingerprint(&path);

    let tree = BTree::open_read_only(&path).unwrap();
    assert!(tree.current_seq() > 0);
    assert_eq!(tree.buckets().unwrap(), vec!["data".to_string()]);
    read_all(&tree, 200);
    drop(tree);

    assert_eq!(
        fingerprint(&path),
        before,
        "a read-only handle must leave bytes, length and mtime untouched"
    );
}

/// Read-only handles take a **shared** lock. The lock mode is only
/// observable across processes: an in-process second open reuses the live
/// instance and never touches the file.
#[test]
fn read_only_open_uses_a_shared_lock() {
    let dir = TempDir::new().unwrap();
    let path = prepared(&dir, "shared.db");
    let tree = BTree::open_read_only(&path).unwrap();
    read_all(&tree, 200);

    // A second reader in this process reuses the live instance.
    let second = BTree::open_read_only(&path).unwrap();
    assert_eq!(second.current_seq(), tree.current_seq());
    read_all(&second, 200);
    drop(second);

    // Another process must be able to read while this handle is held: that only
    // holds if the lock is shared.
    let reader = run_lock_child(&dir, &path, "read-only");
    assert!(
        reader.contains("lock-child: read-only ok"),
        "a second process must be able to open read-only while a reader is live: {reader}"
    );
    // ... and it must not be able to write, until every reader is gone.
    let writer = run_lock_child(&dir, &path, "read-write");
    assert!(
        writer.contains("lock-child: busy"),
        "a writer must be refused while a read-only handle is held: {writer}"
    );

    drop(tree);
    let writer = BTree::open(&path).unwrap();
    seed(&writer, 200..300);
    drop(writer);
}

/// Runs the lock subprocess and returns everything it printed.
fn run_lock_child(dir: &TempDir, path: &Path, mode: &str) -> String {
    let output = child_test_command(&std::env::current_exe().unwrap())
        .args([
            "--exact",
            "read_only_lock_child",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_DIR_ENV, dir.path())
        .env(CHILD_MODE_ENV, mode)
        .env("BTREE_READ_ONLY_CHILD_PATH", path)
        .output()
        .unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
#[ignore = "subprocess target for the read-only lock witness"]
fn read_only_lock_child() {
    let (Ok(dir), Ok(path)) = (
        std::env::var(CHILD_DIR_ENV),
        std::env::var("BTREE_READ_ONLY_CHILD_PATH"),
    ) else {
        return;
    };
    let _ = dir;
    let path = PathBuf::from(path);
    let mode = std::env::var(CHILD_MODE_ENV).unwrap_or_else(|_| "read-only".to_string());
    match mode.as_str() {
        "read-only" => match BTree::open_read_only(&path) {
            Ok(tree) => {
                let seq = tree.current_seq();
                println!("lock-child: read-only ok seq={seq}");
            }
            Err(error) => println!("lock-child: read-only failed {error:?}"),
        },
        "read-write" => match BTree::open(&path) {
            Ok(_) => println!("lock-child: read-write ok"),
            Err(OpenError::DatabaseBusy { .. }) => println!("lock-child: busy"),
            Err(error) => println!("lock-child: read-write failed {error:?}"),
        },
        other => println!("lock-child: unknown mode {other}"),
    }
}

/// A live read-write instance is never joined by a read-only open.
#[test]
fn read_only_open_rejects_a_live_read_write_instance() {
    let dir = TempDir::new().unwrap();
    let path = prepared(&dir, "live.db");

    let writer = BTree::open(&path).unwrap();
    let error = open_read_only_err(&path);
    assert!(
        matches!(error, OpenError::InvalidOptions(_)),
        "a live read-write instance must not be joined, got {error:?}"
    );
    drop(writer);

    // Two read-only handles with identical options share one instance.
    let first = BTree::open_read_only(&path).unwrap();
    let second = BTree::open_read_only(&path).unwrap();
    assert_eq!(first.current_seq(), second.current_seq());
}

/// The same file read read-only and read-write yields the same generation.
#[test]
fn read_only_open_matches_read_write_reads() {
    let dir = TempDir::new().unwrap();
    let path = prepared(&dir, "parity.db");

    let collect = |tree: &BTree| {
        let mut pairs = Vec::new();
        tree.view("data", |txn| {
            for index in (0..200u32).step_by(7) {
                pairs.push((key(index), txn.get(key(index))?));
            }
            let mut iter = txn.iter();
            let mut key_buf = Vec::new();
            let mut val_buf = Vec::new();
            let mut iterated = Vec::new();
            while iter.next_ref(&mut key_buf, &mut val_buf) {
                iterated.push((key_buf.clone(), val_buf.clone()));
            }
            assert_eq!(iterated.len(), 200);
            pairs.extend(iterated.iter().step_by(7).cloned());
            Ok(())
        })
        .unwrap();
        pairs
    };

    // Sequential: a read-only handle and a read-write handle cannot be held on
    // one path at the same time, so the comparison is taken one after the other.
    let read_only = BTree::open_read_only(&path).unwrap();
    let seq = read_only.current_seq();
    let buckets = read_only.buckets().unwrap();
    let pairs = collect(&read_only);
    drop(read_only);

    let read_write = BTree::open(&path).unwrap();
    assert_eq!(read_write.current_seq(), seq);
    assert_eq!(read_write.buckets().unwrap(), buckets);
    assert_eq!(collect(&read_write), pairs);
}

/// Every mutating call on a read-only handle is a typed error, and none of
/// them touches the file.
#[test]
fn read_only_handle_rejects_mutations() {
    let dir = TempDir::new().unwrap();
    let path = prepared(&dir, "mutations.db");
    let before = fingerprint(&path);
    let tree = BTree::open_read_only(&path).unwrap();
    let copy = dir.path().join("copy.db");

    assert!(matches!(
        tree.exec("data", |txn| {
            txn.put(key(500), value(500))?;
            Ok(())
        }),
        Err(Error::ReadOnly)
    ));
    assert!(matches!(tree.exec_multi(|_| Ok(())), Err(Error::ReadOnly)));
    assert!(matches!(tree.commit(), Err(Error::ReadOnly)));
    assert!(matches!(
        tree.new_bucket("another", false),
        Err(Error::ReadOnly)
    ));
    assert!(matches!(tree.del_bucket("data"), Err(Error::ReadOnly)));
    // The read-only gate is checked before argument validation, so a name that
    // would otherwise be rejected as invalid is still reported as read-only.
    assert!(matches!(tree.del_bucket(""), Err(Error::ReadOnly)));
    assert!(matches!(
        tree.take_snapshot(&copy),
        Err(OpenError::ReadOnly)
    ));
    assert!(!copy.exists(), "a rejected snapshot must not create a file");

    drop(tree);
    assert_eq!(fingerprint(&path), before, "rejected calls must not write");
}

/// Corruption found while opening stays a typed error; a torn newest slot
/// still serves the older generation.
#[test]
fn read_only_open_reports_corruption_and_serves_the_older_generation() {
    let dir = TempDir::new().unwrap();
    let path = prepared(&dir, "corruption.db");
    let older_seq = BTree::open(&path).unwrap().current_seq();
    {
        let tree = BTree::open(&path).unwrap();
        seed(&tree, 200..210);
    }
    let newest_seq = BTree::open(&path).unwrap().current_seq();
    assert!(newest_seq > older_seq);

    // (a) Destroy both slots: typed corruption, no panic.
    let both = dir.path().join("both-slots.db");
    fs::copy(&path, &both).unwrap();
    {
        let mut bytes = fs::read(&both).unwrap();
        bytes[..40].fill(0);
        bytes[PAGE..PAGE + 40].fill(0);
        fs::write(&both, &bytes).unwrap();
    }
    let error = open_read_only_err(&both);
    assert!(
        matches!(error, OpenError::Corruption(_)),
        "both slots invalid must be corruption, got {error:?}"
    );

    // (b) Tear only the newest slot: the older generation is served.
    let newest_slot = if newest_seq.is_multiple_of(2) {
        PAGE
    } else {
        0
    };
    let torn = dir.path().join("torn.db");
    fs::copy(&path, &torn).unwrap();
    {
        let mut bytes = fs::read(&torn).unwrap();
        bytes[newest_slot..newest_slot + 40].fill(0);
        fs::write(&torn, &bytes).unwrap();
    }
    let tree = BTree::open_read_only(&torn).unwrap();
    assert_eq!(tree.current_seq(), older_seq);
    read_all(&tree, 200);
}

/// A file the process may not write can still be opened read-only.
#[test]
fn read_only_open_works_without_write_permission() {
    let dir = TempDir::new().unwrap();
    let path = prepared(&dir, "readonly-perm.db");

    let original = fs::metadata(&path).unwrap().permissions();
    let mut permissions = original.clone();
    permissions.set_readonly(true);
    fs::set_permissions(&path, permissions).unwrap();

    drop(
        BTree::open_read_only(&path).expect("a read-only open must work without write permission"),
    );

    if fs::OpenOptions::new().write(true).open(&path).is_ok() {
        eprintln!(
            "skipping the read-write denial assertion: this process can write a 0444 file \
             (root or CAP_DAC_OVERRIDE), so the failing-write half of this case is untested here"
        );
        fs::set_permissions(&path, original).unwrap();
        return;
    }
    assert!(
        BTree::open(&path).is_err(),
        "a read-write open must fail without write permission"
    );
    fs::set_permissions(&path, original).unwrap();
}

/// Opening and dropping read-only handles leaks neither the lock nor a
/// registry entry.
#[test]
fn read_only_lifecycle_is_clean() {
    let dir = TempDir::new().unwrap();
    let path = prepared(&dir, "lifecycle.db");

    for _ in 0..100 {
        let tree = BTree::open_read_only(&path).unwrap();
        read_all(&tree, 4);
    }
    let writer = BTree::open(&path).unwrap();
    seed(&writer, 200..210);
    drop(writer);
}

/// In child processes: a read that hits a damaged page aborts with the engine
/// diagnostic — never a bare panic. Two damage classes, both invisible to the
/// header-level validation: a node page overwritten with garbage, and a page
/// whose header validates while a slot's range points past the page.
#[test]
fn read_only_midread_failure_reports_diagnostics() {
    for mode in ["garbage-node", "slot-range"] {
        let dir = TempDir::new().unwrap();
        let output = child_test_command(&std::env::current_exe().unwrap())
            .args([
                "--exact",
                "read_only_midread_failure_child",
                "--ignored",
                "--nocapture",
            ])
            .env(CHILD_DIR_ENV, dir.path())
            .env(CHILD_MODE_ENV, mode)
            .output()
            .unwrap();

        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !output.status.success(),
            "[{mode}] a damaged page must not be served silently: {combined}"
        );
        assert!(
            combined.contains("btree-store fatal code=BTREE_FATAL_"),
            "[{mode}] the failure must carry the engine diagnostic: {combined}"
        );
        assert!(
            !combined.contains("panicked at"),
            "[{mode}] the failure must not be a bare panic: {combined}"
        );
    }
}

#[test]
#[ignore = "subprocess target for read-only mid-read failure"]
fn read_only_midread_failure_child() {
    let Ok(dir) = std::env::var(CHILD_DIR_ENV) else {
        return;
    };
    let mode = std::env::var(CHILD_MODE_ENV).unwrap_or_else(|_| "garbage-node".to_string());
    let dir = PathBuf::from(dir);
    let path = dir.join("midread.db");

    let tree = BTree::open(&path).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, 0..20_000);
    drop(tree);

    let mut bytes = fs::read(&path).unwrap();
    let mut damaged = 0usize;
    for page_index in 2..bytes.len() / PAGE {
        let page = &bytes[page_index * PAGE..(page_index + 1) * PAGE];
        let is_leaf = u32::from_le_bytes(page[4..8].try_into().unwrap());
        let elems = u32::from_le_bytes(page[8..12].try_into().unwrap());
        let offset = u32::from_le_bytes(page[12..16].try_into().unwrap());
        let looks_like_node = is_leaf <= 1
            && elems > 0
            && (offset as usize) <= PAGE
            && (offset as usize) >= PLAIN_HEADER_SIZE + elems as usize * SLOT_SIZE;
        if !looks_like_node {
            continue;
        }
        damaged += 1;
        match mode.as_str() {
            "garbage-node" => {
                let start = page_index * PAGE;
                bytes[start..start + PAGE].fill(0xFF);
            }
            "slot-range" => {
                let slot = page_index * PAGE + PLAIN_HEADER_SIZE;
                bytes[slot..slot + 4].copy_from_slice(&0xFFFFu32.to_le_bytes());
                seal_page(
                    &mut bytes[page_index * PAGE..(page_index + 1) * PAGE],
                    page_index as u32,
                );
            }
            other => panic!("unknown child mode {other}"),
        }
    }
    assert!(damaged > 0, "the fixture must contain node pages");
    fs::write(&path, &bytes).unwrap();

    let tree = BTree::open_read_only(&path).unwrap();
    let outcome = tree.view("data", |txn| {
        for index in 0..20_000u32 {
            let _ = txn.get(key(index));
        }
        Ok(())
    });
    println!("read returned without a fault: {outcome:?}");
}
