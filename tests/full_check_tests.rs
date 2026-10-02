mod common;

const BIN: &str = env!("CARGO_BIN_EXE_btree-store");

use btree_store::{BTree, CheckError, CheckStatus, DiagnosticSeverity, MetaNode, check_path};
use std::fs;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use tempfile::TempDir;

fn seed(path: &Path) {
    let tree = BTree::open(path).unwrap();
    tree.new_bucket("plain", false).unwrap();
    tree.new_bucket("prefixed", true).unwrap();
    tree.exec("plain", |txn| {
        for index in 0..128u32 {
            let key = format!("k{index:04}");
            txn.put(key.as_bytes(), vec![0x41 + index as u8; 64])?;
        }
        txn.put(b"empty", b"")?;
        txn.put(b"overflow", vec![0x41; 10_000])
    })
    .unwrap();
    tree.exec("prefixed", |txn| {
        for index in 0..128u32 {
            let key = format!("user:{index:04}:profile");
            txn.put(key.as_bytes(), vec![0x42 + index as u8; 64])?;
        }
        txn.put(b"user:9999:profile", vec![0x42; 100_000])
    })
    .unwrap();
}

const PAGE_SIZE: usize = 4096;
const NODE_CRC: usize = 0;
const TRAILER_CRC: usize = PAGE_SIZE - 4;

#[derive(Clone, Copy)]
struct RawSlot {
    at: usize,
    pos: u32,
    klen: u32,
    vlen: u32,
    page_ids: [u32; 5],
}

fn page_mut(bytes: &mut [u8], pid: usize) -> &mut [u8] {
    &mut bytes[pid * PAGE_SIZE..(pid + 1) * PAGE_SIZE]
}

fn read_u32(bytes: &[u8], pid: usize, offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[pid * PAGE_SIZE + offset..pid * PAGE_SIZE + offset + 4]
            .try_into()
            .unwrap(),
    )
}

fn write_u32(bytes: &mut [u8], pid: usize, offset: usize, value: u32) {
    bytes[pid * PAGE_SIZE + offset..pid * PAGE_SIZE + offset + 4]
        .copy_from_slice(&value.to_le_bytes());
}

fn seal_page(bytes: &mut [u8], pid: usize, field: usize) {
    let value = page_mut(bytes, pid);
    value[field..field + 4].copy_from_slice(&[0; 4]);
    let mut crc = 0u32;
    crc = crc32c::crc32c_append(crc, &value[..field]);
    crc = crc32c::crc32c_append(crc, &[0; 4]);
    crc = crc32c::crc32c_append(crc, &(pid as u32).to_le_bytes());
    crc = crc32c::crc32c_append(crc, &value[field + 4..]);
    value[field..field + 4].copy_from_slice(&crc.to_le_bytes());
}

fn selected_meta(bytes: &[u8]) -> MetaNode {
    let a = MetaNode::from_slice(&bytes[..40]);
    let b = MetaNode::from_slice(&bytes[PAGE_SIZE..PAGE_SIZE + 40]);
    if b.seq > a.seq { b } else { a }
}

fn plain_slots(bytes: &[u8], pid: usize) -> Option<Vec<RawSlot>> {
    if read_u32(bytes, pid, 4) != 1 {
        return None;
    }
    let elems = read_u32(bytes, pid, 8) as usize;
    let base = 16;
    Some(
        (0..elems)
            .map(|index| {
                let at = base + index * 32;
                RawSlot {
                    at,
                    pos: read_u32(bytes, pid, at),
                    klen: read_u32(bytes, pid, at + 4),
                    vlen: read_u32(bytes, pid, at + 8),
                    page_ids: std::array::from_fn(|n| read_u32(bytes, pid, at + 12 + n * 4)),
                }
            })
            .collect(),
    )
}

fn plain_branch_slots(bytes: &[u8], pid: usize) -> Option<Vec<RawSlot>> {
    if read_u32(bytes, pid, 4) != 0 {
        return None;
    }
    let elems = read_u32(bytes, pid, 8) as usize;
    let base = 16;
    Some(
        (0..elems)
            .map(|index| {
                let at = base + index * 32;
                RawSlot {
                    at,
                    pos: read_u32(bytes, pid, at),
                    klen: read_u32(bytes, pid, at + 4),
                    vlen: read_u32(bytes, pid, at + 8),
                    page_ids: std::array::from_fn(|n| read_u32(bytes, pid, at + 12 + n * 4)),
                }
            })
            .collect(),
    )
}

fn encoded_leaf_slots(bytes: &[u8], pid: usize) -> Option<Vec<RawSlot>> {
    if read_u32(bytes, pid, 4) != 3 {
        return None;
    }
    let prefix_len = read_u32(bytes, pid, 16) as usize;
    let elems = read_u32(bytes, pid, 8) as usize;
    let base = (20 + prefix_len + 3) & !3;
    Some(
        (0..elems)
            .map(|index| {
                let at = base + index * 32;
                RawSlot {
                    at,
                    pos: read_u32(bytes, pid, at),
                    klen: read_u32(bytes, pid, at + 4),
                    vlen: read_u32(bytes, pid, at + 8),
                    page_ids: std::array::from_fn(|n| read_u32(bytes, pid, at + 12 + n * 4)),
                }
            })
            .collect(),
    )
}

fn extent_entry(bytes: &[u8], pid: usize, index: usize) -> (u32, u32) {
    let at = 12 + index * 8;
    (read_u32(bytes, pid, at), read_u32(bytes, pid, at + 4))
}

fn has_code(report: &btree_store::CheckReport, code: &str) -> bool {
    report
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == code)
}

/// A definite format violation must fail the check, not only make it incomplete.
fn assert_failed(report: &btree_store::CheckReport) {
    assert_eq!(
        report.status,
        CheckStatus::Failed,
        "diagnostics: {:#?}",
        report.diagnostics
    );
}

/// A code the runtime itself tolerates must be reported as a warning and leave the
/// report `Ok`: a file the engine opens is not a failed check.
fn assert_warning(report: &btree_store::CheckReport, code: &str) {
    let diagnostic = report
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == code)
        .unwrap_or_else(|| panic!("{code} not reported: {report:?}"));
    assert_eq!(diagnostic.severity, DiagnosticSeverity::Warning);
    assert_eq!(
        report.status,
        CheckStatus::Ok,
        "diagnostics: {:#?}",
        report.diagnostics
    );
}

fn seed_reusable(path: &Path) {
    seed(path);
    let tree = BTree::open(path).unwrap();
    tree.exec("plain", |txn| {
        for index in 0..64u32 {
            txn.del(format!("k{index:04}").as_bytes())?;
        }
        Ok(())
    })
    .unwrap();
    tree.exec("plain", |txn| txn.put(b"k0000", vec![0x44; 128]))
        .unwrap();
}

fn corrupt_report(path: &Path, mutate: impl FnOnce(&mut Vec<u8>)) -> btree_store::CheckReport {
    seed(path);
    let mut bytes = fs::read(path).unwrap();
    mutate(&mut bytes);
    fs::write(path, &bytes).unwrap();
    check_path(path).unwrap()
}

fn corrupt_reusable_report(
    path: &Path,
    mutate: impl FnOnce(&mut Vec<u8>),
) -> btree_store::CheckReport {
    seed_reusable(path);
    let mut bytes = fs::read(path).unwrap();
    mutate(&mut bytes);
    fs::write(path, &bytes).unwrap();
    check_path(path).unwrap()
}

fn find_bucket_root(bytes: &[u8]) -> usize {
    let meta = selected_meta(bytes);
    (2..meta.next_page_id as usize)
        .find_map(|pid| {
            let slots = plain_slots(bytes, pid)?;
            slots.iter().find_map(|slot| {
                if slot.vlen != 8 || slot.page_ids.iter().any(|pid| *pid != 0) {
                    return None;
                }
                let start = pid * PAGE_SIZE + slot.pos as usize + slot.klen as usize;
                let root = u32::from_le_bytes(bytes[start..start + 4].try_into().ok()?);
                (root >= 2
                    && root < meta.next_page_id
                    && find_branch_with_branch_child(bytes, root as usize).is_some())
                .then_some(root as usize)
            })
        })
        .expect("seeded database must contain a multi-level bucket root")
}

fn find_leaf_under(bytes: &[u8], root: usize) -> Option<usize> {
    let mut stack = vec![root];
    let mut seen = std::collections::HashSet::new();
    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        if let Some(slots) = plain_slots(bytes, pid) {
            let _ = slots;
            return Some(pid);
        }
        if let Some(slots) = plain_branch_slots(bytes, pid) {
            for slot in slots {
                stack.push(slot.page_ids[0] as usize);
            }
        }
    }
    None
}

fn find_branch_with_branch_child(bytes: &[u8], root: usize) -> Option<(usize, usize, usize)> {
    let mut stack = vec![root];
    let mut seen = std::collections::HashSet::new();
    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        let Some(slots) = plain_branch_slots(bytes, pid) else {
            continue;
        };
        for (index, slot) in slots.iter().enumerate() {
            let child = slot.page_ids[0] as usize;
            if plain_branch_slots(bytes, child).is_some()
                && let Some(leaf) = find_leaf_under(bytes, child)
            {
                return Some((pid, index, leaf));
            }
            stack.push(child);
        }
    }
    None
}

#[test]
fn leaf_depth_mismatch_is_reported() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leaf-depth.db");
    {
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("deep", false).unwrap();
        tree.exec("deep", |txn| {
            for index in 0..20_000u32 {
                txn.put(format!("k{index:06}").as_bytes(), b"v")?;
            }
            Ok(())
        })
        .unwrap();
    }
    let mut bytes = fs::read(&path).unwrap();
    let root = find_bucket_root(&bytes);
    let (parent, index, leaf) = find_branch_with_branch_child(&bytes, root).unwrap();
    let slots = plain_branch_slots(&bytes, parent).unwrap();
    write_u32(&mut bytes, parent, slots[index].at + 12, leaf as u32);
    seal_page(&mut bytes, parent, NODE_CRC);
    fs::write(&path, &bytes).unwrap();

    let report = check_path(&path).unwrap();
    assert!(
        has_code(&report, "LEAF_DEPTH_MISMATCH"),
        "report={report:?}"
    );
}

#[test]
fn multi_page_indirect_cycle_is_reported() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("indirect-cycle-multi.db");
    {
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("big", true).unwrap();
        tree.exec("big", |txn| txn.put(b"big", vec![0x42; 4_300_000]))
            .unwrap();
    }
    let mut bytes = fs::read(&path).unwrap();
    let slots = (2..bytes.len() / PAGE_SIZE)
        .find_map(|pid| encoded_leaf_slots(&bytes, pid))
        .unwrap();
    let slot = slots
        .iter()
        .find(|slot| slot.vlen > (4092 * 1022) as u32 && slot.page_ids[0] != 0)
        .unwrap();
    let indirect = slot.page_ids[0] as usize;
    assert_ne!(read_u32(&bytes, indirect, PAGE_SIZE - 8), 0);
    write_u32(&mut bytes, indirect, PAGE_SIZE - 8, indirect as u32);
    seal_page(&mut bytes, indirect, TRAILER_CRC);
    fs::write(&path, &bytes).unwrap();

    let report = check_path(&path).unwrap();
    assert_failed(&report);
    assert!(has_code(&report, "INDIRECT_CYCLE"), "report={report:?}");
}

#[test]
fn structure_corruption_matrix_reports_stable_codes() {
    let dir = TempDir::new().unwrap();

    let path = dir.path().join("child-out-of-range.db");
    let report = corrupt_report(&path, |bytes| {
        let mut mutated = false;
        for pid in 2..bytes.len() / PAGE_SIZE {
            if let Some(slots) = plain_branch_slots(bytes, pid)
                && slots.len() >= 2
            {
                write_u32(bytes, pid, slots[1].at + 12, u32::MAX);
                seal_page(bytes, pid, NODE_CRC);
                mutated = true;
            }
        }
        assert!(mutated);
    });
    assert_failed(&report);
    assert!(has_code(&report, "PID_OUT_OF_RANGE"));

    let path = dir.path().join("node-revisit.db");
    let report = corrupt_report(&path, |bytes| {
        let mut mutated = false;
        for pid in 2..bytes.len() / PAGE_SIZE {
            if let Some(slots) = plain_branch_slots(bytes, pid)
                && slots.len() >= 2
            {
                write_u32(bytes, pid, slots[1].at + 12, slots[0].page_ids[0]);
                seal_page(bytes, pid, NODE_CRC);
                mutated = true;
            }
        }
        assert!(mutated);
    });
    assert_failed(&report);
    assert!(has_code(&report, "DUPLICATE_NODE_REFERENCE"));

    let path = dir.path().join("key-order.db");
    let report = corrupt_report(&path, |bytes| {
        let mut mutated = false;
        for pid in 2..bytes.len() / PAGE_SIZE {
            let Some(slots) = plain_slots(bytes, pid) else {
                continue;
            };
            if slots.len() >= 2 && slots[0].klen == slots[1].klen {
                // Duplicate the first key into the second slot. The second key stays
                // inside the node's routing range, so only the ordering rule can
                // reject it.
                let base = pid * PAGE_SIZE;
                let key: Vec<u8> = bytes[base + slots[0].pos as usize
                    ..base + slots[0].pos as usize + slots[0].klen as usize]
                    .to_vec();
                let to = base + slots[1].pos as usize;
                bytes[to..to + key.len()].copy_from_slice(&key);
                seal_page(bytes, pid, NODE_CRC);
                mutated = true;
            }
        }
        assert!(mutated);
    });
    assert_failed(&report);
    assert!(
        has_code(&report, "KEYS_NOT_STRICTLY_INCREASING"),
        "report={report:?}"
    );

    let path = dir.path().join("branch-range.db");
    let report = corrupt_report(&path, |bytes| {
        let mut mutated = false;
        for pid in 2..bytes.len() / PAGE_SIZE {
            if let Some(slots) = plain_branch_slots(bytes, pid)
                && let Some(slot) = slots.iter().skip(1).find(|slot| slot.klen > 0)
            {
                bytes[pid * PAGE_SIZE + slot.pos as usize + slot.klen as usize - 1] = b'1';
                seal_page(bytes, pid, NODE_CRC);
                mutated = true;
            }
        }
        assert!(mutated);
    });
    assert!(
        has_code(&report, "KEY_OUTSIDE_BRANCH_RANGE"),
        "report={report:?}"
    );

    let path = dir.path().join("zero-value-pid.db");
    let report = corrupt_report(&path, |bytes| {
        let mut mutated = false;
        for pid in 2..bytes.len() / PAGE_SIZE {
            let Some(slots) = plain_slots(bytes, pid) else {
                continue;
            };
            for slot in slots.iter().filter(|slot| slot.vlen == 0) {
                write_u32(bytes, pid, slot.at + 12, 1);
                mutated = true;
            }
            if mutated {
                seal_page(bytes, pid, NODE_CRC);
            }
        }
        assert!(mutated);
    });
    assert!(
        has_code(&report, "ZERO_VALUE_HAS_PAGE_ID"),
        "report={report:?}"
    );

    let path = dir.path().join("value-pid-out-of-range.db");
    let report = corrupt_report(&path, |bytes| {
        let mut mutated = false;
        for pid in 2..bytes.len() / PAGE_SIZE {
            let Some(slots) = plain_slots(bytes, pid) else {
                continue;
            };
            for slot in slots.iter().filter(|slot| {
                slot.vlen > 256 && slot.page_ids[0] != 0 && slot.vlen.div_ceil(4092) <= 5
            }) {
                write_u32(bytes, pid, slot.at + 12, u32::MAX);
                mutated = true;
            }
            if mutated {
                seal_page(bytes, pid, NODE_CRC);
            }
        }
        assert!(mutated);
    });
    assert_failed(&report);
    assert!(has_code(&report, "PID_OUT_OF_RANGE"));

    let path = dir.path().join("indirect-unterminated.db");
    let report = corrupt_report(&path, |bytes| {
        let mut mutated = false;
        for pid in 2..bytes.len() / PAGE_SIZE {
            let Some(slots) = encoded_leaf_slots(bytes, pid) else {
                continue;
            };
            for slot in slots
                .iter()
                .filter(|slot| slot.vlen > 4092 * 5 && slot.page_ids[0] != 0)
            {
                let indirect = slot.page_ids[0] as usize;
                write_u32(bytes, indirect, PAGE_SIZE - 8, indirect as u32);
                seal_page(bytes, indirect, TRAILER_CRC);
                mutated = true;
            }
        }
        assert!(mutated);
    });
    assert_failed(&report);
    // These values fit in one indirect page, so a self-referential `next` violates
    // the termination rule before any cycle can repeat a page; a cycle over several
    // index pages is covered by `multi_page_indirect_cycle_is_reported`.
    assert!(
        has_code(&report, "INDIRECT_CHAIN_NOT_TERMINATED"),
        "report={report:?}"
    );

    let path = dir.path().join("indirect-pid-out-of-range.db");
    let report = corrupt_report(&path, |bytes| {
        let mut mutated = false;
        for pid in 2..bytes.len() / PAGE_SIZE {
            let Some(slots) = encoded_leaf_slots(bytes, pid) else {
                continue;
            };
            for slot in slots
                .iter()
                .filter(|slot| slot.vlen > 4092 * 5 && slot.page_ids[0] != 0)
            {
                let indirect = slot.page_ids[0] as usize;
                write_u32(bytes, indirect, 0, u32::MAX);
                seal_page(bytes, indirect, TRAILER_CRC);
                mutated = true;
            }
        }
        assert!(mutated);
    });
    assert_failed(&report);
    assert!(has_code(&report, "PID_OUT_OF_RANGE"));

    let path = dir.path().join("extent-not-merged.db");
    let report = corrupt_reusable_report(&path, |bytes| {
        let meta = selected_meta(bytes);
        let root = meta.reusable_root as usize;
        let (start, len) = extent_entry(bytes, root, 0);
        let count = read_u32(bytes, root, 8) as usize;
        assert!(len >= 2 && count < 510);
        // Shift the existing entries up one slot, then split entry 0 in two.
        // Writing the halves over slots 0 and 1 instead would drop the second
        // entry's pages from the ownership accounting.
        let page = root * PAGE_SIZE;
        let shifted = bytes[page + 12..page + 12 + count * 8].to_vec();
        bytes[page + 20..page + 20 + count * 8].copy_from_slice(&shifted);
        write_u32(bytes, root, 8, (count + 1) as u32);
        write_u32(bytes, root, 12, start);
        write_u32(bytes, root, 16, 1);
        write_u32(bytes, root, 20, start + 1);
        write_u32(bytes, root, 24, len - 1);
        seal_page(bytes, root, NODE_CRC);
    });
    // The runtime opens a file with adjacent extents and merges the pair in memory,
    // so this is a canonical-form note rather than a failure.
    assert_warning(&report, "EXTENT_NOT_MERGED");

    let path = dir.path().join("extent-overlap.db");
    let report = corrupt_reusable_report(&path, |bytes| {
        let meta = selected_meta(bytes);
        let root = meta.reusable_root as usize;
        let (start, len) = extent_entry(bytes, root, 0);
        assert!(len >= 2);
        write_u32(bytes, root, 8, 2);
        write_u32(bytes, root, 12, start);
        write_u32(bytes, root, 16, 1);
        write_u32(bytes, root, 20, start);
        write_u32(bytes, root, 24, len - 1);
        seal_page(bytes, root, NODE_CRC);
    });
    assert_failed(&report);
    assert!(has_code(&report, "EXTENT_UNSORTED_OR_OVERLAP"));

    let path = dir.path().join("extent-list-cycle.db");
    let report = corrupt_reusable_report(&path, |bytes| {
        let meta = selected_meta(bytes);
        let root = meta.reusable_root as usize;
        write_u32(bytes, root, 4, root as u32);
        seal_page(bytes, root, NODE_CRC);
    });
    assert_failed(&report);
    assert!(has_code(&report, "EXTENT_CYCLE"));

    let path = dir.path().join("owner-overlap.db");
    let report = corrupt_reusable_report(&path, |bytes| {
        let meta = selected_meta(bytes);
        let root = meta.reusable_root as usize;
        write_u32(bytes, root, 12, meta.catalog_root);
        write_u32(bytes, root, 16, 1);
        seal_page(bytes, root, NODE_CRC);
    });
    assert_failed(&report);
    assert!(has_code(&report, "REACHABLE_REUSED"));

    let path = dir.path().join("owner-unaccounted.db");
    let report = corrupt_reusable_report(&path, |bytes| {
        let meta = selected_meta(bytes);
        let root = meta.reusable_root as usize;
        write_u32(bytes, root, 8, 0);
        seal_page(bytes, root, NODE_CRC);
    });
    assert_failed(&report);
    assert!(has_code(&report, "OWNERSHIP_UNACCOUNTED"));
}

#[test]
fn generated_v2_database_passes_full_check() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("valid.db");
    seed(&path);

    let report = check_path(&path).unwrap();
    assert_eq!(
        report.status,
        CheckStatus::Ok,
        "diagnostics: {:#?}",
        report.diagnostics
    );
    assert!(report.diagnostics.is_empty());
    assert!(report.reachable_pages > 0);
    assert!(report.generation.is_some());
}

#[test]
fn churned_reusable_and_retired_state_passes_full_check() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("churn.db");
    seed(&path);
    {
        let tree = BTree::open(&path).unwrap();
        tree.exec("plain", |txn| {
            txn.del(b"k0000")?;
            txn.put(b"replacement", vec![0x43; 7000])
        })
        .unwrap();
        tree.exec("prefixed", |txn| txn.del(b"user:0000:profile"))
            .unwrap();
    }

    let report = check_path(&path).unwrap();
    assert_eq!(
        report.status,
        CheckStatus::Ok,
        "diagnostics: {:#?}",
        report.diagnostics
    );
    assert!(report.diagnostics.is_empty());
}
#[test]
fn active_reader_leaves_retired_pages_for_checker() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("retired.db");
    seed(&path);
    let tree = BTree::open(&path).unwrap();
    let reader = tree.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let view = thread::spawn(move || {
        reader.view("plain", |_txn| {
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<(), btree_store::Error>(())
        })
    });
    ready_rx.recv().unwrap();
    tree.exec("plain", |txn| {
        for index in 0..64u32 {
            txn.del(format!("k{index:04}").as_bytes())?;
        }
        Ok(())
    })
    .unwrap();
    release_tx.send(()).unwrap();
    view.join().unwrap().unwrap();
    drop(tree);

    let report = check_path(&path).unwrap();
    assert_eq!(
        report.status,
        CheckStatus::Ok,
        "diagnostics: {:#?}",
        report.diagnostics
    );
    assert!(report.retired_pages > 0, "report={report:?}");
    assert!(report.allocator_list_pages > 0);
}

#[test]
fn retired_pages_promote_to_reusable_on_later_generation() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("reusable.db");
    seed(&path);
    {
        let tree = BTree::open(&path).unwrap();
        tree.exec("plain", |txn| txn.del(b"k0000")).unwrap();
        tree.exec("plain", |txn| txn.put(b"k0000", vec![0x44; 128]))
            .unwrap();
    }

    let report = check_path(&path).unwrap();
    assert_eq!(
        report.status,
        CheckStatus::Ok,
        "diagnostics: {:#?}",
        report.diagnostics
    );
    assert!(report.reusable_pages > 0, "report={report:?}");
    assert!(report.allocator_list_pages > 0);
}

#[test]
fn full_check_does_not_modify_source_bytes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("immutable.db");
    seed(&path);
    let before_metadata = fs::metadata(&path).unwrap();
    let before_modified = before_metadata.modified().unwrap();
    let before = fs::read(&path).unwrap();
    let before_len = before_metadata.len();

    let report = check_path(&path).unwrap();
    assert_eq!(report.status, CheckStatus::Ok);

    let after_metadata = fs::metadata(&path).unwrap();
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(after_metadata.len(), before_len);
    assert_eq!(after_metadata.modified().unwrap(), before_modified);
}

#[test]
fn check_cli_reports_lock_busy() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cli-locked.db");
    seed(&path);
    let _tree = BTree::open(&path).unwrap();
    let output = common::child_test_command(Path::new(BIN))
        .args(["check"])
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("lock-busy"));
}

#[test]
fn missing_data_page_is_a_failed_report() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("truncated.db");
    seed(&path);
    let len = fs::metadata(&path).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 4096)
        .unwrap();

    let report = check_path(&path).unwrap();
    assert_eq!(report.status, CheckStatus::Failed);
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "MISSING_PHYSICAL_PAGE")
    );
}
#[test]
fn half_page_truncation_is_classified() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("half-truncated.db");
    seed(&path);
    let len = fs::metadata(&path).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 2048)
        .unwrap();

    let report = check_path(&path).unwrap();
    assert_eq!(report.status, CheckStatus::Failed);
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "TRUNCATED_PAGE")
    );
}

#[test]
fn generation_external_trailing_bytes_are_informational() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("trailing.db");
    seed(&path);
    let len = fs::metadata(&path).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len + 4)
        .unwrap();

    let report = check_path(&path).unwrap();
    assert_eq!(
        report.status,
        CheckStatus::Ok,
        "diagnostics: {:#?}",
        report.diagnostics
    );
    assert_eq!(report.trailing_bytes, 4);
}

#[test]
fn check_cli_reports_success_without_stderr() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cli-valid.db");
    seed(&path);
    let output = common::child_test_command(Path::new(BIN))
        .args(["check"])
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&output.stdout).contains("status=ok"));
    assert!(output.stderr.is_empty());
}

#[test]
fn check_cli_failure_keeps_stdout_empty() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cli-failed.db");
    seed(&path);
    let len = fs::metadata(&path).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 4096)
        .unwrap();
    let output = common::child_test_command(Path::new(BIN))
        .args(["check"])
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).starts_with("error: check: failed:"));
}

#[test]
fn corrupted_data_page_returns_diagnostics() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("corrupt-page.db");
    seed(&path);
    let mut bytes = fs::read(&path).unwrap();
    let last_pid = bytes.len() / 4096 - 1;
    bytes[last_pid * 4096 + 100] ^= 1;
    fs::write(&path, &bytes).unwrap();

    let report = check_path(&path).unwrap();
    assert_failed(&report);
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "PAGE_CRC_MISMATCH")
    );
}
#[test]
fn meta_checksum_failure_and_version_errors_are_distinct() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("meta-errors.db");
    seed(&path);

    let mut bad_crc = fs::read(&path).unwrap();
    bad_crc[8] ^= 1;
    bad_crc[4096 + 8] ^= 1;
    fs::write(&path, &bad_crc).unwrap();
    assert!(matches!(check_path(&path), Err(CheckError::NoValidMeta)));

    let mut unsupported = fs::read(&path).unwrap();
    for slot in [0usize, 4096] {
        let mut meta = MetaNode::from_slice(&unsupported[slot..slot + 40]);
        meta.format_version = 1;
        meta.update_checksum();
        unsupported[slot..slot + 40].copy_from_slice(meta.as_page_slice());
    }
    fs::write(&path, &unsupported).unwrap();
    assert!(matches!(
        check_path(&path),
        Err(CheckError::UnsupportedFormatVersion { found: 1 })
    ));

    // Build the mixed state from a current-version file: a version-1 file cannot be
    // reopened, and reaching into the previous case's bytes would make this
    // assertion depend on the order the cases run in.
    let mixed_path = dir.path().join("meta-mixed.db");
    seed(&mixed_path);
    let mut mixed = fs::read(&mixed_path).unwrap();
    let mut meta = MetaNode::from_slice(&mixed[PAGE_SIZE..PAGE_SIZE + 40]);
    meta.format_version = 1;
    meta.update_checksum();
    mixed[PAGE_SIZE..PAGE_SIZE + 40].copy_from_slice(meta.as_page_slice());
    fs::write(&mixed_path, &mixed).unwrap();
    assert!(matches!(
        check_path(&mixed_path),
        Err(CheckError::MixedFormatVersions { .. })
    ));
}

#[test]
fn truncated_second_meta_slot_can_still_be_checked() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("one-meta.db");
    drop(BTree::open(&path).unwrap());
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(4096)
        .unwrap();

    let report = check_path(&path).unwrap();
    assert_eq!(
        report.status,
        CheckStatus::Ok,
        "diagnostics: {:#?}",
        report.diagnostics
    );
    assert_eq!(report.selected_slot, Some(btree_store::MetaSlot::A));
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "META_CANDIDATE_IGNORED")
    );
}

#[test]
fn torn_newest_meta_slot_falls_back_to_the_older_generation() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("torn-newest.db");
    seed(&path);
    let mut bytes = fs::read(&path).unwrap();
    let a = MetaNode::from_slice(&bytes[..40]);
    let b = MetaNode::from_slice(&bytes[PAGE_SIZE..PAGE_SIZE + 40]);
    let newest = if b.seq > a.seq { PAGE_SIZE } else { 0 };
    bytes[newest + 8] ^= 1;
    fs::write(&path, &bytes).unwrap();

    let report = check_path(&path).unwrap();
    assert_eq!(
        report.status,
        CheckStatus::Ok,
        "diagnostics: {:#?}",
        report.diagnostics
    );
    assert!(
        has_code(&report, "META_CANDIDATE_IGNORED"),
        "report={report:?}"
    );
    // The pages the torn generation allocated lie past the selected id space: they
    // are reported as trailing bytes instead of making the check fail.
    assert!(report.trailing_bytes > 0, "report={report:?}");
}

#[test]
fn a_reference_to_a_metadata_slot_is_out_of_range() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pid-one.db");
    let report = corrupt_report(&path, |bytes| {
        let mut mutated = false;
        for pid in 2..bytes.len() / PAGE_SIZE {
            let Some(slots) = plain_slots(bytes, pid) else {
                continue;
            };
            for slot in slots.iter().filter(|slot| {
                slot.vlen > 256 && slot.page_ids[0] != 0 && slot.vlen.div_ceil(4092) <= 5
            }) {
                // Page 1 is the second metadata slot. The engine never names it from
                // a value slot, and neither may the checker: this must be reported as
                // a range violation, not as whatever page 1 happens to contain.
                write_u32(bytes, pid, slot.at + 12, 1);
                mutated = true;
            }
            if mutated {
                seal_page(bytes, pid, NODE_CRC);
            }
        }
        assert!(mutated);
    });
    assert_failed(&report);
    assert!(has_code(&report, "PID_OUT_OF_RANGE"), "report={report:?}");
}

#[test]
fn a_directory_is_not_a_database_file() {
    let dir = TempDir::new().unwrap();
    assert!(matches!(
        check_path(dir.path()),
        Err(CheckError::NotRegularFile { .. })
    ));
}

#[test]
fn check_cli_rejects_a_non_file() {
    let dir = TempDir::new().unwrap();
    let output = common::child_test_command(Path::new(BIN))
        .args(["check"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .starts_with("error: check: io: not a regular file:"),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn open_failures_expose_the_underlying_io_error() {
    let dir = TempDir::new().unwrap();
    let error = check_path(dir.path().join("missing.db")).unwrap_err();
    assert!(matches!(error, CheckError::Io(_)), "{error:?}");
    assert!(std::error::Error::source(&error).is_some(), "{error:?}");
}

/// the reserved `inplace` spelling refuses with exactly the documented line,
/// exits on the `arg` class, and touches nothing. `--info` does not
/// change that: the mode is decided first.
#[test]
fn compact_inplace_is_a_reserved_spelling_that_refuses() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cli-inplace.db");
    seed(&path);
    let destination = dir.path().join("cli-inplace.out");
    let before = fs::read(&path).unwrap();
    let mtime = fs::metadata(&path).unwrap().modified().unwrap();

    for extra in [Vec::new(), vec!["--info".to_string()]] {
        let mut command = common::child_test_command(Path::new(BIN));
        command
            .args(["compact", "inplace"])
            .arg(&path)
            .arg(&destination);
        for flag in &extra {
            command.arg(flag);
        }
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{:?}", extra);
        assert!(output.stdout.is_empty());
        assert_eq!(
            String::from_utf8_lossy(&output.stderr),
            "error: arguments: arg: inplace is not implemented in this build; use rebuild\n",
            "{extra:?}"
        );
        assert!(!destination.exists(), "the refusal creates nothing");
    }

    // Nothing was created and the source is byte-identical.
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), mtime);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1, "no new file");
}

/// The mode word is optional and `rebuild` is what an omitted one means, so both
/// spellings report the same numbers. A word that is not a mode is a path, and
/// three paths are clap's own usage error - which is where a typo'd mode word
/// lands, too.
#[test]
fn compact_defaults_to_rebuild_and_refuses_an_unknown_mode() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cli-mode.db");
    seed(&path);

    let run = |args: &[&std::ffi::OsStr]| {
        let mut command = common::child_test_command(Path::new(BIN));
        command.arg("compact").args(args);
        command.output().unwrap()
    };
    let source = path.as_os_str();
    let destination_path = dir.path().join("cli-mode.out");
    let destination = destination_path.as_os_str();

    // Compared through `--info`, which writes nothing: the comparison is
    // about the mode, not about what was written.
    let default_run = run(&[source, destination, "--info".as_ref()]);
    let explicit_run = run(&["rebuild".as_ref(), source, destination, "--info".as_ref()]);
    assert_eq!(default_run.status.code(), Some(0));
    assert_eq!(default_run.status.code(), explicit_run.status.code());
    assert_eq!(default_run.stderr, explicit_run.stderr);
    assert_eq!(default_run.stdout, explicit_run.stdout);

    // A third path is not a mode word: the mode is a subcommand, so an unknown
    // spelling is read as the source and the extra path is what clap rejects.
    let unknown = run(&["sideways".as_ref(), source, destination]);
    assert_eq!(unknown.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&unknown.stderr);
    assert!(stderr.contains("Usage:"), "{stderr}");
    assert!(
        !stderr.contains("inplace is not implemented"),
        "an unknown mode must not reach the reserved-spelling branch: {stderr}"
    );
    assert!(!destination_path.exists(), "a usage error writes nothing");
}

fn compact(args: &[&str]) -> std::process::Output {
    common::child_test_command(Path::new(BIN))
        .args(["compact"])
        .args(args)
        .output()
        .unwrap()
}

/// Wherever `--info` is written it means the same run: it is global, and a mode
/// subcommand must not demand a destination for a run that names none. The mode
/// still decides first, so `inplace` reports its own refusal and not a missing
/// destination.
#[test]
fn the_info_flag_is_accepted_wherever_it_is_written() {
    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let source = path.to_str().unwrap();

    let mut reports = Vec::new();
    for args in [
        vec!["--info", source],
        vec!["--info", "rebuild", source],
        vec!["rebuild", "--info", source],
        vec![source, "--info"],
    ] {
        let output = compact(&args);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty(), "{args:?} prints no summary");
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(stderr.contains("estimated=true"), "{args:?}: {stderr}");
        reports.push(stderr);
    }
    assert!(
        reports.windows(2).all(|pair| pair[0] == pair[1]),
        "every placement is the same run: {reports:?}"
    );

    let stub = compact(&["--info", "inplace", source]);
    assert_eq!(stub.status.code(), Some(2));
    assert_eq!(
        String::from_utf8_lossy(&stub.stderr),
        "error: arguments: arg: inplace is not implemented in this build; use rebuild\n",
        "the mode decides before the destination does"
    );
}

/// A run that writes needs a destination, and the error says how to run without
/// one. Nothing is read and nothing is created.
#[test]
fn a_writing_run_without_a_destination_is_an_argument_error() {
    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let source = path.to_str().unwrap();

    for args in [vec![source], vec!["rebuild", source]] {
        let output = compact(&args);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.starts_with("error: arguments: arg:"),
            "{args:?}: {stderr}"
        );
        assert!(
            stderr.contains("--info"),
            "the way out is named: {args:?}: {stderr}"
        );
        assert!(
            !stderr.contains("estimate "),
            "{args:?}: nothing is read: {stderr}"
        );
        assert!(output.stdout.is_empty(), "{args:?} prints no summary");
    }
    assert_eq!(
        fs::read_dir(dir.path()).unwrap().count(),
        1,
        "nothing was created beside the source"
    );
}

/// `--info` reports the same numbers twice over one unchanged
/// source, writes nothing, and leaves stdout empty.
#[test]
fn compact_info_is_reproducible_and_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("estimate.db");
    seed(&path);
    let before = fs::read(&path).unwrap();
    let mtime = fs::metadata(&path).unwrap().modified().unwrap();
    let source = path.to_str().unwrap().to_string();

    let first = compact(&[&source, "--info"]);
    let second = compact(&[&source, "--info"]);
    assert_eq!(first.status.code(), Some(0));
    assert_eq!(
        first.stderr, second.stderr,
        "the estimate must be reproducible"
    );
    assert!(
        first.stdout.is_empty(),
        "no summary before a rebuild exists"
    );

    let stderr = String::from_utf8_lossy(&first.stderr);
    assert!(stderr.contains("estimated=true"), "{stderr}");
    assert!(stderr.contains("records="), "{stderr}");
    assert!(stderr.contains("logical_bytes="), "{stderr}");

    // Zero writes: the source is byte-identical and nothing else appeared.
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), mtime);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}

/// the estimate must scale with the data. An empty bucket list and a populated
/// one cannot report the same numbers.
#[test]
fn compact_estimate_tracks_the_source_contents() {
    let dir = TempDir::new().unwrap();
    let small = dir.path().join("small.db");
    let large = dir.path().join("large.db");
    seed(&small);
    {
        let tree = BTree::open(&large).unwrap();
        tree.new_bucket("plain", false).unwrap();
        tree.exec("plain", |txn| {
            for index in 0..8_000u32 {
                txn.put(format!("k{index:06}").as_bytes(), vec![b'v'; 200])?;
            }
            Ok(())
        })
        .unwrap();
    }

    let read_estimate = |path: &Path| {
        let output = compact(&[path.to_str().unwrap(), "--info"]);
        assert_eq!(output.status.code(), Some(0));
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let field = |name: &str| {
            stderr
                .split_whitespace()
                .find_map(|word| word.strip_prefix(&format!("{name}=")))
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("{name} missing from {stderr}"))
        };
        (field("bytes_low"), field("bytes_high"), field("records"))
    };

    let (small_low, small_high, small_records) = read_estimate(&small);
    let (large_low, large_high, large_records) = read_estimate(&large);
    assert!(small_low <= small_high && large_low <= large_high);
    assert!(
        large_records > small_records,
        "{large_records} vs {small_records}"
    );
    assert!(large_low > small_low, "{large_low} vs {small_low}");
    assert!(large_high > small_high, "{large_high} vs {small_high}");
}

/// a v1 source is refused as `source-version-unsupported`, never as
/// `source-corrupt`, and nothing is created.
#[test]
fn compact_refuses_an_old_format_source_by_name() {
    let dir = TempDir::new().unwrap();
    let v1 = dir.path().join("old.v1");
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v1_shapes.v1"),
        &v1,
    )
    .unwrap();
    let before = fs::read(&v1).unwrap();

    let output = compact(&[v1.to_str().unwrap(), "--info"]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("source-version-unsupported"), "{stderr}");
    assert!(
        !stderr.contains("source-corrupt"),
        "a v1 source must not be reported as corrupt: {stderr}"
    );
    assert!(
        stderr.contains("migrate"),
        "the refusal should name the way out: {stderr}"
    );

    assert_eq!(fs::read(&v1).unwrap(), before);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1, "no new file");
}

/// a source whose pages are damaged is refused by the audit, after it opened.
#[test]
fn compact_refuses_a_damaged_source_after_it_opened() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("damaged.db");
    seed(&path);
    let mut bytes = fs::read(&path).unwrap();
    let last_pid = bytes.len() / 4096 - 1;
    bytes[last_pid * 4096 + 100] ^= 1;
    fs::write(&path, &bytes).unwrap();

    let output = compact(&[path.to_str().unwrap(), "--info"]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("failed"), "{stderr}");
    assert!(
        stderr.starts_with("error: check: failed:"),
        "the audit is the `check` step: {stderr}"
    );
    assert!(
        !stderr.contains("estimate "),
        "nothing is estimated from a source that failed the audit: {stderr}"
    );
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1, "no new file");
}

/// The read stage asks for a shared lock, so an **exclusive** holder elsewhere
/// makes it fail. The contention is cross-process on purpose: a second
/// `BTree::open` in this process would be answered from the instance registry
/// without ever taking a file lock, so it would prove nothing about the lock.
///
/// This is evidence for how an `OpenError::DatabaseBusy` is classified, not for
/// the read stage's own lock-holding. The other direction — a reader arriving
/// while the read stage runs — is `a_concurrent_reader_is_served_during_the_read_stage`.
#[test]
fn compact_holds_the_source_against_another_process() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("locked.db");
    seed(&path);
    // A read-write handle takes the exclusive lock for its whole life.
    let _writer = BTree::open(&path).unwrap();

    let output = compact(&[path.to_str().unwrap(), "--info"]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("lock-busy"), "{stderr}");
    assert!(
        stderr.starts_with("error: source: lock-busy:"),
        "the refusal comes from opening the source: {stderr}"
    );
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1, "no new file");
}

/// the remaining open-gate shapes. A source whose two metadata slots are both
/// unusable is `source-corrupt`; slots that disagree on the version are
/// `source-version-mixed`; a path that is not a regular file is `io`. Each must
/// refuse before anything is created.
#[test]
fn compact_classifies_the_remaining_open_failures() {
    let dir = TempDir::new().unwrap();

    // Both slots damaged: no valid record at all.
    let broken = dir.path().join("both-slots-broken.db");
    seed(&broken);
    let mut bytes = fs::read(&broken).unwrap();
    bytes[8] ^= 1;
    bytes[4096 + 8] ^= 1;
    fs::write(&broken, &bytes).unwrap();
    let broken_before = fs::read(&broken).unwrap();

    // Two valid records that name different versions.
    let mixed = dir.path().join("mixed-versions.db");
    seed(&mixed);
    let mut bytes = fs::read(&mixed).unwrap();
    let mut meta = MetaNode::from_slice(&bytes[..40]);
    meta.format_version = 1;
    meta.update_checksum();
    bytes[..40].copy_from_slice(meta.as_page_slice());
    fs::write(&mixed, &bytes).unwrap();
    let mixed_before = fs::read(&mixed).unwrap();

    // A directory is not a database file.
    let not_a_file = dir.path().join("a-directory");
    fs::create_dir(&not_a_file).unwrap();
    let entries = fs::read_dir(dir.path()).unwrap().count();

    for (path, class, before) in [
        (&broken, "source-corrupt", Some(&broken_before)),
        (&mixed, "source-version-mixed", Some(&mixed_before)),
        (&not_a_file, "io", None),
    ] {
        let output = compact(&[path.to_str().unwrap(), "--info"]);
        assert_eq!(output.status.code(), Some(1), "{}: {class}", path.display());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.starts_with(&format!("error: source: {class}:")),
            "{}: expected {class}, got {stderr}",
            path.display()
        );
        assert!(!stderr.contains("estimate "), "nothing estimated: {stderr}");
        if let Some(before) = before {
            assert_eq!(&fs::read(path).unwrap(), before, "the source was modified");
        }
        assert_eq!(
            fs::read_dir(dir.path()).unwrap().count(),
            entries,
            "no file was created"
        );
    }
}

/// The read stage runs under a **shared** lock, so a concurrent reader in another
/// process is served.
///
/// This covers the "shared, not exclusive" half. The other half — that an
/// *exclusive* request from elsewhere is refused while the run is in progress —
/// is `a_rebuild_run_blocks_a_writer_in_another_process`, which asks for the lock
/// directly once the run has said it holds the source. The direction where
/// compact itself meets an existing exclusive holder is
/// `compact_holds_the_source_against_another_process`.
#[test]
fn a_concurrent_reader_is_served_during_the_read_stage() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("shared-lock.db");
    {
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("plain", false).unwrap();
        tree.exec("plain", |txn| {
            for index in 0..40_000u32 {
                txn.put(format!("k{index:07}").as_bytes(), vec![b'v'; 96])?;
            }
            Ok(())
        })
        .unwrap();
    }

    let child = common::child_test_command(Path::new(BIN))
        .args(["compact"])
        .arg(&path)
        .arg(dir.path().join("shared-lock.out"))
        .arg("--info")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let concurrent = common::child_test_command(Path::new(BIN))
        .args(["check"])
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(
        concurrent.status.code(),
        Some(0),
        "a reader must be served during the read stage: {}",
        String::from_utf8_lossy(&concurrent.stderr)
    );

    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("estimated=true"), "{stderr}");
}

/// A destination that cannot name a file is an `arg` error in a run that would
/// write, refused before the source is even read. `--info` names no
/// target, so the same destination is ignored there - nothing is judged but the
/// source.
#[test]
fn compact_refuses_a_destination_that_names_no_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("out-name.db");
    seed(&path);
    let before = fs::read(&path).unwrap();

    for destination in ["out/", "out/.", "."] {
        let result = compact(&[path.to_str().unwrap(), destination]);
        assert_eq!(result.status.code(), Some(2), "destination={destination:?}");
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(
            stderr.starts_with("error: arguments: arg:"),
            "destination={destination:?} -> {stderr}"
        );
        assert!(!stderr.contains("estimate "), "nothing read: {stderr}");

        let ignored = compact(&[path.to_str().unwrap(), destination, "--info"]);
        assert_eq!(
            ignored.status.code(),
            Some(0),
            "destination={destination:?} is not named by an estimate: {}",
            String::from_utf8_lossy(&ignored.stderr)
        );
        assert!(ignored.stdout.is_empty());
    }
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        fs::read_dir(dir.path()).unwrap().count(),
        1,
        "nothing was created beside the source"
    );
}

/// A delete-heavy source: most records are gone, so the file is much larger than
/// what a rebuild has to carry. Returns the source and the directory it lives in.
fn compactable_source(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("compactable.db");
    {
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("plain", false).unwrap();
        tree.new_bucket("pref", true).unwrap();
        // An empty bucket is the shape that costs a `new_bucket` and no commit:
        // a rebuild that skipped it would look healthy while holding one bucket
        // fewer, so the fixture carries one.
        tree.new_bucket("void", false).unwrap();
        tree.exec("plain", |txn| {
            for index in 0..5_000u32 {
                txn.put(format!("k{index:06}").as_bytes(), vec![b'v'; 180])?;
            }
            Ok(())
        })
        .unwrap();
        tree.exec("pref", |txn| {
            for index in 0..500u32 {
                txn.put(format!("u{index:04}:x").as_bytes(), b"p")?;
            }
            Ok(())
        })
        .unwrap();
    }
    let tree = BTree::open(&path).unwrap();
    tree.exec("plain", |txn| {
        for index in 0..4_800u32 {
            txn.del(format!("k{index:06}").as_bytes())?;
        }
        Ok(())
    })
    .unwrap();
    drop(tree);
    path
}

fn summary_field(stdout: &str, key: &str) -> String {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("{key} missing from:\n{stdout}"))
        .to_string()
}

/// Every record of one bucket, in key order, as raw bytes.
fn bucket_records(path: &Path, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let tree = BTree::open_read_only(path).unwrap();
    let mut records = Vec::new();
    tree.view(name, |txn| {
        let mut iterator = txn.iter();
        let mut key = Vec::new();
        let mut value = Vec::new();
        while iterator.next_ref(&mut key, &mut value) {
            records.push((key.clone(), value.clone()));
        }
        Ok(())
    })
    .unwrap();
    records
}

/// A successful rebuild publishes a target that passes the audit, holds the same
/// logical content, and leaves the source byte-identical.
#[test]
fn a_rebuild_publishes_an_equivalent_smaller_file() {
    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let before = fs::read(&path).unwrap();
    let before_mtime = fs::metadata(&path).unwrap().modified().unwrap();
    let target = dir.path().join("out.db");

    let output = common::child_test_command(Path::new(BIN))
        .args(["compact"])
        .arg(&path)
        .arg(&target)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(summary_field(&stdout, "verified"), "true");
    assert_eq!(summary_field(&stdout, "records"), "700");
    assert_eq!(summary_field(&stdout, "buckets"), "3");
    assert_eq!(summary_field(&stdout, "mode"), "rebuild");
    let reclaimed: u64 = summary_field(&stdout, "reclaimed_bytes").parse().unwrap();
    assert!(reclaimed > 0, "a delete-heavy source must shrink: {stdout}");
    let source_bytes: u64 = summary_field(&stdout, "source_bytes").parse().unwrap();
    let physical_bytes: u64 = summary_field(&stdout, "physical_bytes").parse().unwrap();
    assert_eq!(source_bytes, before.len() as u64);
    assert_eq!(physical_bytes, fs::metadata(&target).unwrap().len());
    assert_eq!(reclaimed, source_bytes.saturating_sub(physical_bytes));
    let estimated = summary_field(&stdout, "estimated_bytes");
    let (low, high) = estimated.split_once("..").expect("estimate is a range");
    let (low, high): (u64, u64) = (low.parse().unwrap(), high.parse().unwrap());
    assert!(low <= high && high > 0);

    let source_catalog = BTree::open_read_only(&path)
        .unwrap()
        .buckets_with_policy()
        .unwrap();
    let target_catalog = BTree::open_read_only(&target)
        .unwrap()
        .buckets_with_policy()
        .unwrap();
    assert_eq!(
        source_catalog, target_catalog,
        "catalog names, order and prefix policy must match"
    );
    assert_eq!(
        source_catalog.iter().filter(|(_, policy)| *policy).count(),
        1,
        "the fixture has exactly one prefix bucket, so the policy bit is compared"
    );

    for (name, _) in &source_catalog {
        let source_records = bucket_records(&path, name);
        let target_records = bucket_records(&target, name);
        assert_eq!(
            source_records, target_records,
            "bucket {name:?} must hold the same records"
        );
        if name == "void" {
            assert!(
                target_records.is_empty(),
                "the fixture's empty bucket is still empty in the target"
            );
        } else {
            assert!(!target_records.is_empty(), "bucket {name:?} has records");
        }
    }

    let check = common::child_test_command(Path::new(BIN))
        .args(["check"])
        .arg(&target)
        .output()
        .unwrap();
    assert_eq!(
        check.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );
    assert!(fs::metadata(&target).unwrap().len() < before.len() as u64);

    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        fs::metadata(&path).unwrap().modified().unwrap(),
        before_mtime
    );
}

/// The estimate must not depend on whether a target gets built: one
/// `--info` run and one real rebuild over the same source report the
/// same four numbers. Comparing two info runs would not catch a rebuild
/// path that recomputed its counts from the staging file.
#[test]
fn the_estimate_matches_the_rebuild_it_precedes() {
    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let target = dir.path().join("out.db");

    let estimate = compact(&[path.to_str().unwrap(), "--info"]);
    assert_eq!(estimate.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&estimate.stderr);

    let output = common::child_test_command(Path::new(BIN))
        .args(["compact"])
        .arg(&path)
        .arg(&target)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert_eq!(
        summary_field(&stdout, "estimated_bytes"),
        format!(
            "{}..{}",
            estimate_field(&stderr, "bytes_low"),
            estimate_field(&stderr, "bytes_high")
        ),
        "the summary must carry the estimate this run printed: {stderr}"
    );
    for key in ["records", "logical_bytes"] {
        assert_eq!(
            summary_field(&stdout, key),
            estimate_field(&stderr, key),
            "{key} must be the same whether or not a target is built"
        );
    }
}

fn estimate_field(stderr: &str, key: &str) -> String {
    stderr
        .split_whitespace()
        .find_map(|field| field.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("{key} missing from the estimate line: {stderr}"))
        .to_string()
}

/// An existing destination is not an error: it is replaced by the verified
/// rebuild. The old bytes are gone, the source is untouched, no staging name is
/// left behind, and running again over the previous output works the same way.
#[test]
fn compact_replaces_an_existing_destination() {
    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let target = dir.path().join("out.db");
    let stale = b"an existing file, not a database".to_vec();
    fs::write(&target, &stale).unwrap();
    let source_before = fs::read(&path).unwrap();
    let source_records = bucket_records(&path, "plain");

    for run in 0..2 {
        let output = compact(&[path.to_str().unwrap(), target.to_str().unwrap()]);
        assert_eq!(
            output.status.code(),
            Some(0),
            "run {run}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(summary_field(&stdout, "verified"), "true");
        assert_eq!(
            summary_field(&stdout, "physical_bytes")
                .parse::<u64>()
                .unwrap(),
            fs::metadata(&target).unwrap().len(),
            "run {run}: the summary describes the file that is actually there"
        );
        assert_ne!(
            fs::read(&target).unwrap(),
            stale,
            "run {run}: the previous bytes must be gone"
        );
        assert_eq!(
            bucket_records(&target, "plain"),
            source_records,
            "run {run}: the replacement holds the source's records"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            source_before,
            "run {run}: the source is untouched"
        );
    }

    let check = common::child_test_command(Path::new(BIN))
        .args(["check"])
        .arg(&target)
        .output()
        .unwrap();
    assert_eq!(
        check.status.code(),
        Some(0),
        "the replacement must be a valid database: {}",
        String::from_utf8_lossy(&check.stderr)
    );

    let leftovers: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("compact-v") || name.contains(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "the rename consumes the staging name: {leftovers:?}"
    );
}

/// A run that replaces a file must refuse to replace its own source, whatever
/// spelling names it: the same path, another path to the same inode, or a link to
/// it. `--info` writes nothing, so it names no target and is allowed.
#[test]
fn compact_refuses_to_replace_its_own_source() {
    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let before = fs::read(&path).unwrap();
    let canonical = fs::canonicalize(&path).unwrap();

    let mut spellings = vec![
        path.clone(),
        path.to_str().unwrap().to_string().into(),
        canonical.clone(),
        // `./x` and `x` are one file, and so are a path through a symlinked
        // directory and the real one.
        dir.path().join(".").join("compactable.db"),
    ];
    #[cfg(unix)]
    {
        let hard = dir.path().join("hard-link.db");
        fs::hard_link(&path, &hard).unwrap();
        let soft = dir.path().join("symlink.db");
        std::os::unix::fs::symlink(&path, &soft).unwrap();
        spellings.push(hard);
        spellings.push(soft);
    }

    for destination in spellings {
        let output = compact(&[path.to_str().unwrap(), destination.to_str().unwrap()]);
        assert_eq!(output.status.code(), Some(2), "{destination:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.starts_with("error: arguments: arg:"),
            "{destination:?}: {stderr}"
        );
        assert!(
            !stderr.contains("estimate "),
            "{destination:?}: the refusal precedes the read: {stderr}"
        );
        assert!(
            stderr.contains("the destination must be another file"),
            "{destination:?}: {stderr}"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            before,
            "{destination:?}: the source is untouched"
        );
    }

    // Nothing is written, so nothing is judged - not even which file the source is.
    let estimate = compact(&[path.to_str().unwrap(), path.to_str().unwrap(), "--info"]);
    assert_eq!(estimate.status.code(), Some(0));
    assert_eq!(fs::read(&path).unwrap(), before);
}

/// A source that does not exist is reported as a source error, however the
/// destination looks: a stale destination is not a substitute for the input, and
/// the run must not touch it while failing.
#[test]
fn a_missing_source_is_reported_as_a_source_error() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("missing.db");
    let source = source.to_str().unwrap();
    let typed = dir.path().join("out.db");
    let typed = typed.to_str().unwrap();
    fs::write(typed, b"a stale destination").unwrap();
    let before = fs::read(typed).unwrap();

    for args in [
        vec![source, typed],
        // A destination that is also missing is not the point either.
        vec![source, dir.path().join("none.db").to_str().unwrap()],
    ] {
        let result = compact(&args);
        assert_eq!(result.status.code(), Some(1), "{args:?}");
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(
            stderr.starts_with("error: source: io:"),
            "{args:?}: the source's own error wins: {stderr}"
        );
        assert!(
            stderr.contains("missing.db"),
            "{args:?}: the message must name the path that was typed: {stderr}"
        );
    }
    assert_eq!(
        fs::read(typed).unwrap(),
        before,
        "a failed run leaves the destination alone"
    );
}

/// The destination must be replaceable by a file, which is a property of the names
/// alone and is therefore answered before the source is read. What the destination
/// *contains* is not asked about at all: a dangling symlink is a name like any
/// other and is replaced by the file, while a name that cannot hold one - a path
/// under a regular file, or a name that is a directory - is refused with the `io`
/// class, the same way on every platform.
#[test]
fn compact_replaces_what_it_can_and_refuses_what_it_cannot() {
    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let source_before = fs::read(&path).unwrap();

    let parent_is_a_file = dir.path().join("plain-file");
    fs::write(&parent_is_a_file, b"not a directory").unwrap();
    let under_a_file = parent_is_a_file.join("out.db");

    let a_directory = dir.path().join("a-directory");
    fs::create_dir(&a_directory).unwrap();

    for (label, destination) in [
        ("parent is a regular file", under_a_file),
        ("the name is a directory", a_directory),
    ] {
        let output = compact(&[path.to_str().unwrap(), destination.to_str().unwrap()]);
        assert_eq!(output.status.code(), Some(1), "{label}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.starts_with("error: target: io:"),
            "{label} must be refused by the target step: {stderr}"
        );
        assert!(
            !stderr.contains("estimate "),
            "{label}: the name is judged before the source is read: {stderr}"
        );
        assert!(output.stdout.is_empty(), "{label} prints no summary");
    }

    #[cfg(unix)]
    {
        // A dangling link is a name whose content does not exist; the file that
        // replaces it is the database, not the target the link pointed at.
        let dangling = dir.path().join("dangling.db");
        std::os::unix::fs::symlink(dir.path().join("nothing-here.db"), &dangling).unwrap();
        let output = compact(&[path.to_str().unwrap(), dangling.to_str().unwrap()]);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            fs::symlink_metadata(&dangling).unwrap().is_file(),
            "the link itself was replaced by the database"
        );
        assert!(
            !dir.path().join("nothing-here.db").exists(),
            "nothing was created where the link pointed"
        );
    }

    assert_eq!(fs::read(&path).unwrap(), source_before);
    let leftovers: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("compact-v") || name.contains(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "no staging name may remain: {leftovers:?}"
    );
}

/// The two gates and the staging cleanup. Faults are injected at the two points
/// this command owns — the rebuild and the comparison — because the publication
/// steps belong to the shared protocol.
///
/// The injection points only exist in a binary built with the
/// `test-fault-injection` feature. A plain `cargo test` builds one without it, and
/// the fault would then be a no-op that the run ignores, so the case is gated
/// rather than the whole file.
#[cfg(feature = "test-fault-injection")]
#[test]
fn a_fault_before_publication_leaves_no_target() {
    for fault in ["rebuild", "compare"] {
        let dir = TempDir::new().unwrap();
        let path = compactable_source(&dir);
        let target = dir.path().join("out.db");

        let output = common::child_test_command(Path::new(BIN))
            .args(["compact"])
            .arg(&path)
            .arg(&target)
            .env("BTREE_STORE_MIGRATE_FAULT", fault)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "fault {fault}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("injected failure"),
            "fault {fault}: {stderr}"
        );
        assert!(
            output.stdout.is_empty(),
            "a failed run must not print a summary: fault {fault}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(!target.exists(), "fault {fault} must not publish a target");
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("compact-v") || name.contains(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "fault {fault} must remove its own staging: {leftovers:?}"
        );
    }
}

/// The replacement is one step, so its failure ladder has two rungs: a fault before
/// the swap must leave the destination exactly as it was, and a fault after it must
/// report `published-durability-unknown` with the verified file already in place.
/// The swap consumes the staging name, so there is never a leftover to clean.
#[cfg(feature = "test-fault-injection")]
#[test]
fn the_replacement_ladder_reports_what_reached_the_destination() {
    for (fault, published) in [("sync", false), ("dirsync1", true)] {
        let dir = TempDir::new().unwrap();
        let path = compactable_source(&dir);
        let target = dir.path().join("out.db");
        let stale = b"the previous destination".to_vec();
        fs::write(&target, &stale).unwrap();

        let output = common::child_test_command(Path::new(BIN))
            .args(["compact"])
            .arg(&path)
            .arg(&target)
            .env("BTREE_STORE_MIGRATE_FAULT", fault)
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(1), "fault {fault}");
        assert!(output.stdout.is_empty(), "fault {fault} prints no summary");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("injected failure"),
            "fault {fault}: {stderr}"
        );
        if published {
            assert!(
                stderr.contains("published-durability-unknown"),
                "fault {fault}: the swap already happened: {stderr}"
            );
            let check = common::child_test_command(Path::new(BIN))
                .args(["check"])
                .arg(&target)
                .output()
                .unwrap();
            assert_eq!(
                check.status.code(),
                Some(0),
                "fault {fault}: what reached the destination is complete: {}",
                String::from_utf8_lossy(&check.stderr)
            );
        } else {
            assert!(
                stderr.contains("error: publish: io:"),
                "fault {fault}: the swap never happened: {stderr}"
            );
            assert_eq!(
                fs::read(&target).unwrap(),
                stale,
                "fault {fault}: a destination that was not replaced keeps every byte"
            );
        }
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("compact-v") || name.contains(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "fault {fault} leaves no staging name: {leftovers:?}"
        );
    }
}

/// The same source rebuilt twice, to two different targets, must produce
/// identical bytes: the batch boundaries come from the traversal, not from chance.
#[test]
fn two_rebuilds_of_one_source_are_byte_identical() {
    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let mut outputs = Vec::new();
    for name in ["one.db", "two.db"] {
        let target = dir.path().join(name);
        let output = common::child_test_command(Path::new(BIN))
            .args(["compact"])
            .arg(&path)
            .arg(&target)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        outputs.push(fs::read(&target).unwrap());
    }
    assert_eq!(
        outputs[0], outputs[1],
        "two rebuilds of one source must agree byte for byte"
    );
}

/// stdout carries the success summary and nothing else; the estimate, which is
/// not a result, goes to stderr on every run.
#[test]
fn the_summary_is_the_only_thing_on_stdout() {
    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let target = dir.path().join("out.db");

    let output = common::child_test_command(Path::new(BIN))
        .args(["compact"])
        .arg(&path)
        .arg(&target)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        assert!(
            line.contains('='),
            "stdout carries only key=value summary lines, found {line:?}"
        );
    }
    assert!(
        !stdout.contains("estimate "),
        "the estimate is not a summary"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("estimated=true"), "{stderr}");

    // An info run has no summary to print, so stdout stays empty.
    let estimate = common::child_test_command(Path::new(BIN))
        .args(["compact"])
        .arg(&path)
        .arg(dir.path().join("estimate.out"))
        .arg("--info")
        .output()
        .unwrap();
    assert_eq!(estimate.status.code(), Some(0));
    assert!(estimate.stdout.is_empty());
}

/// While a run is in progress the source is under a shared lock, so an exclusive
/// request from anywhere else is refused; once it ends the source is free again.
/// The lock is on the open file description, so the test asks for it directly with
/// `try_lock` rather than through the library: a second `BTree::open` in this
/// process would be answered by the instance registry, which is registry semantics
/// rather than lock semantics.
///
/// The run announces its estimate on stderr only after the read-only stage has
/// opened the source, and it holds that handle until the end of the run, so that
/// line is what tells this process the source is locked *now*. Polling while the
/// child merely lives proves the claim only on the runs where this thread happens
/// to be scheduled before the child exits, which is a property of the scheduler
/// and not of the program.
#[test]
fn a_rebuild_run_blocks_a_writer_in_another_process() {
    use std::fs::File;
    use std::io::{BufRead, Read};

    let dir = TempDir::new().unwrap();
    let path = compactable_source(&dir);
    let target = dir.path().join("out.db");

    let mut child = common::child_test_command(Path::new(BIN))
        .args(["compact"])
        .arg(&path)
        .arg(&target)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stderr = std::io::BufReader::new(child.stderr.take().unwrap());
    let mut announced = String::new();
    let heard = stderr.read_line(&mut announced).unwrap();
    if heard == 0 || !announced.contains("estimated=true") {
        let output = child.wait_with_output().unwrap();
        let mut rest = String::new();
        let _ = stderr.read_to_string(&mut rest);
        panic!(
            "a run must announce its estimate while it holds the source, \
             it said {announced:?}{rest:?} and exited with {:?}",
            output.status
        );
    }

    let probe = File::open(&path).unwrap();
    if probe.try_lock().is_ok() {
        // `run` drops the source before main prints its summary and exits. The
        // target appears only at publication, so that normal tail of the process
        // must not be mistaken for an unlocked rebuild.
        assert!(
            target.is_file() || child.try_wait().unwrap().is_some(),
            "the source must remain locked until the rebuilt target is published"
        );
    }
    drop(probe);

    let output = child.wait_with_output().unwrap();
    let mut rest = String::new();
    let _ = stderr.read_to_string(&mut rest);
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}{announced}{rest}",
        String::from_utf8_lossy(&output.stdout)
    );

    // The other half: once the run is over the source is free, so this process can
    // even take it exclusively. The read handle is dropped first, because holding
    // it is itself a shared lock. The release is retried rather than sampled once:
    // a single reading is a race against the kernel and the scheduler, and under
    // load it fails without anything being wrong.
    let reopened = BTree::open_read_only(&path);
    assert!(
        reopened.is_ok(),
        "the source must be readable once the run has finished"
    );
    drop(reopened);
    let mut released = false;
    for _ in 0..50 {
        match File::open(&path).unwrap().try_lock() {
            Ok(()) => {
                released = true;
                break;
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
        }
    }
    assert!(
        released,
        "the exclusive lock must be free once the run has finished"
    );
}
