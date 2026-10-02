//! Snapshot (consistent copy) acceptance tests.

use btree_store::{BTree, MetaNode, OpenOptions};
use std::fs;
use std::path::Path;
use std::thread;
use tempfile::TempDir;

const META_SLOT: usize = 4096;
const VALUE_LEN: usize = 64;

fn key(index: u32) -> Vec<u8> {
    format!("k{index:06}").into_bytes()
}

fn value(index: u32, len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    bytes[..4].copy_from_slice(&index.to_le_bytes());
    bytes
}

fn marker(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[..4].try_into().unwrap())
}

fn fingerprint(path: &Path) -> (u64, u64) {
    let bytes = fs::read(path).unwrap();
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in &bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash, bytes.len() as u64)
}

fn seed(tree: &BTree, bucket: &str, range: std::ops::Range<u32>) {
    tree.exec(bucket, |txn| {
        for index in range {
            txn.put(key(index), value(index, VALUE_LEN))?;
        }
        Ok(())
    })
    .unwrap();
}

/// The snapshot equals the generation frozen at call time: later writes never reach
/// it, and the copied keys carry the values of that generation.
#[test]
fn snapshot_equals_the_frozen_generation() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source.db");
    let dst = dir.path().join("copy.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..300);

    let snapshot = tree.take_snapshot(&dst).unwrap();

    seed(&tree, "data", 300..600);
    let frozen = fingerprint(&dst);
    seed(&tree, "data", 600..900);
    assert_eq!(
        fingerprint(&dst),
        frozen,
        "later writes must not reach the snapshot"
    );

    let copy = BTree::open(&dst).unwrap();
    assert_eq!(copy.current_seq(), snapshot.seq);
    assert_eq!(copy.buckets().unwrap(), vec!["data".to_string()]);
    copy.view("data", |txn| {
        for index in 0..300 {
            assert_eq!(marker(&txn.get(key(index))?), index);
        }
        for index in 300..900 {
            assert!(
                txn.get(key(index)).is_err(),
                "k{index} leaked into the snapshot"
            );
        }
        Ok(())
    })
    .unwrap();
}

/// The destination is used exactly as given: an existing file is truncated and
/// replaced, so the snapshot is a valid database whatever was there before.
#[test]
fn snapshot_replaces_and_truncates_the_destination() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("replace-source.db");
    let dst = dir.path().join("replace-copy.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..200);

    fs::write(&dst, vec![0xAB; 200 * 1024]).unwrap();
    let snapshot = tree.take_snapshot(&dst).unwrap();

    assert_eq!(
        fs::metadata(&dst).unwrap().len(),
        fs::metadata(&source).unwrap().len(),
        "the destination must be truncated, not written over"
    );
    let copy = BTree::open(&dst).unwrap();
    assert_eq!(copy.current_seq(), snapshot.seq);
    copy.view("data", |txn| {
        assert_eq!(marker(&txn.get(key(199))?), 199);
        Ok(())
    })
    .unwrap();
}

/// A published snapshot is a complete store: it can be written to and reopened.
#[test]
fn snapshot_is_writable_and_reopenable() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("writable-source.db");
    let dst = dir.path().join("writable-copy.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..150);
    let snapshot = tree.take_snapshot(&dst).unwrap();

    let copy = BTree::open(&dst).unwrap();
    assert_eq!(copy.current_seq(), snapshot.seq);
    copy.exec("data", |txn| {
        for index in 150..300 {
            txn.put(key(index), value(index, VALUE_LEN))?;
        }
        Ok(())
    })
    .unwrap();
    let committed_seq = copy.current_seq();
    drop(copy);

    let reopened = BTree::open(&dst).unwrap();
    assert_eq!(reopened.current_seq(), committed_seq);
    reopened
        .view("data", |txn| {
            for index in 0..300 {
                assert_eq!(marker(&txn.get(key(index))?), index);
            }
            Ok(())
        })
        .unwrap();
}

/// Both metadata slots name the frozen generation, so `open` cannot select a
/// newer one whose pages the snapshot does not hold.
#[test]
fn both_metadata_slots_name_the_frozen_generation() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("slots-source.db");
    let dst = dir.path().join("slots-copy.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..400);

    let writer = {
        let tree = tree.clone();
        thread::spawn(move || seed(&tree, "data", 400..900))
    };
    let snapshot = tree.take_snapshot(&dst).unwrap();
    writer.join().unwrap();

    let bytes = fs::read(&dst).unwrap();
    assert_eq!(
        &bytes[..40],
        &bytes[META_SLOT..META_SLOT + 40],
        "both slots must carry the frozen record"
    );
    assert_eq!(MetaNode::from_slice(&bytes[..40]).seq, snapshot.seq);
    assert_eq!(BTree::open(&dst).unwrap().current_seq(), snapshot.seq);
}

/// Writers keep running while the snapshot is built: commits advance across the
/// call, and the snapshot still equals the generation it froze.
///
/// The fixture is large enough (32 MiB) that the copy window comfortably spans
/// several commits even in a heavily instrumented build, so the assertion is not
/// a scheduling race.
#[test]
fn writers_progress_while_the_snapshot_runs() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let dir = TempDir::new().unwrap();
    let source = dir.path().join("progress-source.db");
    let dst = dir.path().join("progress-copy.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    tree.exec("data", |txn| {
        for index in 0..8192u32 {
            txn.put(key(index), vec![0x5A; 4096])?;
        }
        Ok(())
    })
    .unwrap();

    let committed = std::sync::Arc::new(AtomicUsize::new(0));
    let latencies = std::sync::Arc::new(std::sync::Mutex::new(Vec::<std::time::Duration>::new()));
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let writer = {
        let tree = tree.clone();
        let committed = committed.clone();
        let latencies = latencies.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut round = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let started = std::time::Instant::now();
                seed(&tree, "data", 100_000 + round..100_001 + round);
                round += 1;
                latencies.lock().unwrap().push(started.elapsed());
                committed.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    // Let the writer establish a latency baseline before the copy starts.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while committed.load(Ordering::Relaxed) < 20 && std::time::Instant::now() < deadline {
        std::hint::spin_loop();
    }
    let before = committed.load(Ordering::Relaxed);
    let snapshot = tree.take_snapshot(&dst).unwrap();
    let after = committed.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    assert!(
        after > before,
        "writers must keep committing while the snapshot is taken"
    );

    let samples = latencies.lock().unwrap().clone();
    let percentile = |values: &[std::time::Duration], pct: usize| {
        if values.is_empty() {
            return std::time::Duration::ZERO;
        }
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        sorted[(sorted.len() * pct / 100).min(sorted.len() - 1)]
    };
    let in_window = &samples[before.min(samples.len())..after.min(samples.len())];
    println!(
        "snapshot commit latency: {} commits before, {} inside the copy window; p99 {:?} before, {:?} inside",
        before,
        in_window.len(),
        percentile(&samples[..before.min(samples.len())], 99),
        percentile(in_window, 99)
    );
    assert!(
        percentile(in_window, 99) < std::time::Duration::from_secs(1),
        "a commit inside the copy window stalled for {:?}: the snapshot must not serialize writers",
        percentile(in_window, 99)
    );
    let copy = BTree::open(&dst).unwrap();
    assert_eq!(copy.current_seq(), snapshot.seq);
    copy.view("data", |txn| {
        for index in 0..8192 {
            assert_eq!(txn.get(key(index))?.len(), 4096);
        }
        Ok(())
    })
    .unwrap();
}

/// Heavy page recycling inside the copy window must not produce a
/// cross-generation mixture: each churn round writes the whole 64-key set in one
/// transaction, so a frozen generation has to show one round's tags on all of
/// them. The churn keeps running for as long as the copy does, on a fixture
/// large enough that the window spans many commits — that is the shape in which
/// a closure page rewritten while it is read would show up.
#[test]
fn snapshot_survives_heavy_page_recycling() {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    let dir = TempDir::new().unwrap();
    let source = dir.path().join("recycle-source.db");
    let dst = dir.path().join("recycle-snapshot.db");
    let options = OpenOptions {
        cache_capacity: 16,
        ..Default::default()
    };
    let tree = BTree::open_with_options(&source, options).unwrap();
    tree.new_bucket("data", false).unwrap();
    tree.exec("data", |txn| {
        for index in 0..8192u32 {
            txn.put(format!("s{index:05}").into_bytes(), vec![0x11; 4096])?;
        }
        Ok(())
    })
    .unwrap();
    seed(&tree, "data", 10_000..10_100);
    // One complete round before the copy, so every later freeze sees a full set.
    churn_round(&tree, 0);

    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let rounds = std::sync::Arc::new(AtomicU32::new(1));
    let churner = {
        let tree = tree.clone();
        let stop = stop.clone();
        let rounds = rounds.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let round = rounds.fetch_add(1, Ordering::Relaxed);
                churn_round(&tree, round);
            }
        })
    };

    let before_rounds = rounds.load(Ordering::Relaxed);
    let snapshot = tree.take_snapshot(&dst).unwrap();
    let after_rounds = rounds.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    churner.join().unwrap();
    assert!(
        after_rounds > before_rounds,
        "the churn must keep committing while the copy runs"
    );

    let copy = BTree::open(&dst).unwrap();
    assert_eq!(copy.current_seq(), snapshot.seq);
    copy.view("data", |txn| {
        let mut observed_round = None;
        for index in 0..64u32 {
            let stored = txn.get(key(index))?;
            assert_eq!(stored.len(), 8192, "churned k{index}");
            let tagged = marker(&stored);
            assert_eq!(
                tagged % 64,
                index,
                "k{index} carries another key's payload: the snapshot mixed generations"
            );
            let round = tagged / 64;
            match observed_round {
                None => observed_round = Some(round),
                Some(first) => assert_eq!(
                    first, round,
                    "churn keys came from different generations: the snapshot is a cross-generation mixture"
                ),
            }
        }
        for index in 10_000..10_100u32 {
            assert_eq!(marker(&txn.get(key(index))?), index);
        }
        Ok(())
    })
    .unwrap();
}

fn churn_round(tree: &BTree, round: u32) {
    tree.exec("data", |txn| {
        for index in 0..64u32 {
            txn.put(key(index), value(round * 64 + index, 8192))?;
        }
        Ok(())
    })
    .unwrap();
}

/// Opening a snapshot and reading from it writes no bytes: callers rely on the
/// snapshot as a read-only input (for example to enumerate content).
#[test]
fn opening_a_snapshot_writes_no_bytes() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("readonly-source.db");
    let dst = dir.path().join("readonly-snapshot.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..250);
    let snapshot = tree.take_snapshot(&dst).unwrap();

    let before = fingerprint(&dst);
    let copy = BTree::open(&dst).unwrap();
    copy.view("data", |txn| {
        assert_eq!(marker(&txn.get(key(3))?), 3);
        Ok(())
    })
    .unwrap();
    assert_eq!(copy.buckets().unwrap(), vec!["data".to_string()]);
    assert_eq!(copy.current_seq(), snapshot.seq);
    drop(copy);

    assert_eq!(
        fingerprint(&dst),
        before,
        "opening a snapshot must not write"
    );
}

/// A long-lived view pins its own snapshot while the copy is built: neither
/// must deadlock the other, and the copy must still be the frozen generation.
#[test]
fn snapshot_while_a_view_is_pinned() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("view-source.db");
    let dst = dir.path().join("view-snapshot.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..200);

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let viewer = {
        let tree = tree.clone();
        thread::spawn(move || {
            tree.view("data", |txn| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                assert_eq!(marker(&txn.get(key(7))?), 7);
                Ok(())
            })
            .unwrap();
        })
    };
    entered_rx.recv().unwrap();

    let snapshot = tree.take_snapshot(&dst).unwrap();
    release_tx.send(()).unwrap();
    viewer.join().unwrap();

    let copy = BTree::open(&dst).unwrap();
    assert_eq!(copy.current_seq(), snapshot.seq);
    copy.view("data", |txn| {
        for index in 0..200 {
            assert_eq!(marker(&txn.get(key(index))?), index);
        }
        Ok(())
    })
    .unwrap();
}

/// Failures are reported, never fatal, and the source store stays usable: a
/// destination that cannot be created, or that cannot be written, must not take
/// the process or the database with it. A failed call may leave a partial file
/// at the destination, which the caller must discard.
#[test]
fn unwritable_destinations_fail_without_harming_the_store() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("failure-source.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..100);

    let as_directory = dir.path().join("destination-is-a-directory");
    fs::create_dir(&as_directory).unwrap();
    assert!(
        tree.take_snapshot(&as_directory).is_err(),
        "a directory destination must be reported, not accepted"
    );

    let missing_parent = dir.path().join("no-such-dir").join("snapshot.db");
    assert!(
        tree.take_snapshot(&missing_parent).is_err(),
        "a missing parent directory must be reported"
    );
    assert!(!missing_parent.exists());

    tree.view("data", |txn| {
        assert_eq!(marker(&txn.get(key(99))?), 99);
        Ok(())
    })
    .unwrap();
    tree.exec("data", |txn| {
        txn.put(key(100), value(100, VALUE_LEN))?;
        Ok(())
    })
    .unwrap();

    if let Ok(dev_full) = std::fs::OpenOptions::new().write(true).open("/dev/full") {
        drop(dev_full);
        let full = std::path::Path::new("/dev/full");
        let error = tree.take_snapshot(full);
        assert!(error.is_err(), "writing to /dev/full must be reported");
    }
}

/// The snapshot file is cut back to the frozen page-id space, so bytes past it
/// are never carried over as unreachable dead weight.
///
/// In production that tail appears when the source keeps appending after the
/// freeze (the file only grows while the copy runs); the same shape is produced
/// deterministically here by extending the source file before the call.
#[test]
fn snapshot_length_is_the_frozen_id_space() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("length-source.db");
    let dst = dir.path().join("length-snapshot.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..4096);

    // A pre-existing larger destination must be replaced, not partly kept.
    fs::write(&dst, vec![0xCD; 8 * META_SLOT]).unwrap();

    let frozen_source_len = fs::metadata(&source).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&source)
        .unwrap()
        .set_len(frozen_source_len + 4 * META_SLOT as u64)
        .unwrap();

    let snapshot = tree.take_snapshot(&dst).unwrap();

    let bytes = fs::read(&dst).unwrap();
    let meta = MetaNode::from_slice(&bytes[..40]);
    assert_eq!(meta.seq, snapshot.seq);
    assert!(
        fs::metadata(&source).unwrap().len() > frozen_source_len,
        "the fixture must leave a tail past the frozen id space"
    );
    assert_eq!(
        bytes.len() as u64,
        u64::from(meta.next_page_id) * META_SLOT as u64,
        "the file must be cut to the frozen page-id space, not carry the source tail"
    );
    BTree::open(&dst)
        .unwrap()
        .view("data", |txn| {
            assert_eq!(marker(&txn.get(key(0))?), 0);
            Ok(())
        })
        .unwrap();
}

/// Passing this store's own file as the destination must be refused: the call
/// would otherwise truncate the database it is reading.
#[test]
fn snapshot_onto_its_own_file_is_refused() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("own-file.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..100);

    assert!(
        tree.take_snapshot(&source).is_err(),
        "a snapshot onto the store's own file must be refused"
    );
    let reopened = BTree::open(&source).unwrap();
    reopened
        .view("data", |txn| {
            assert_eq!(marker(&txn.get(key(99))?), 99);
            Ok(())
        })
        .unwrap();
}

/// Documented behaviour: a snapshot taken inside a transaction closure copies
/// the last published generation, so the closure's uncommitted changes are not
/// part of it.
#[test]
fn snapshot_from_inside_a_closure_excludes_uncommitted_changes() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("closure-source.db");
    let dst = dir.path().join("closure-copy.db");
    let tree = BTree::open(&source).unwrap();
    tree.new_bucket("data", false).unwrap();
    seed(&tree, "data", 0..100);

    let mut inside = None;
    tree.exec("data", |txn| {
        txn.put(key(5000), value(5000, VALUE_LEN))?;
        inside = Some(tree.take_snapshot(&dst));
        Ok(())
    })
    .unwrap();
    let snapshot = inside.expect("closure must run").unwrap();

    let copy = BTree::open(&dst).unwrap();
    assert_eq!(copy.current_seq(), snapshot.seq);
    copy.view("data", |txn| {
        assert_eq!(marker(&txn.get(key(0))?), 0);
        assert!(
            txn.get(key(5000)).is_err(),
            "an uncommitted change must not reach the snapshot"
        );
        Ok(())
    })
    .unwrap();
}
