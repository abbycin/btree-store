//! Direct tests for the statistics `check` derives from the selected
//! generation: the space its allocator chains imply, and what each bucket tree
//! holds when a scan is asked for.

mod common;

use btree_store::{
    BTree, CheckOptions, CheckStatus, MetaNode, check_path, check_path_with_options,
    check_path_with_space,
};
use std::fs;
use std::path::Path;
use tempfile::TempDir;

const PAGE_SIZE: usize = 4096;

/// A 10,000-byte value needs `ceil(10000 / 4092)` value pages, written in one
/// commit, so deleting the record frees one contiguous run.
const OVERFLOW_VALUE: usize = 10_000;
const OVERFLOW_VALUE_PAGES: usize = 3;

fn selected_meta(bytes: &[u8]) -> MetaNode {
    let a = MetaNode::from_slice(&bytes[..40]);
    let b = MetaNode::from_slice(&bytes[PAGE_SIZE..PAGE_SIZE + 40]);
    if b.seq > a.seq { b } else { a }
}

/// Three buckets covering the shapes a scan has to tell apart: a single inline
/// record, a prefix-encoded bucket whose value overflows onto real pages, and a
/// bucket that was created and never written.
fn seed(path: &Path) {
    let tree = BTree::open(path).unwrap();
    tree.new_bucket("plain", false).unwrap();
    tree.new_bucket("prefixed", true).unwrap();
    tree.new_bucket("empty", false).unwrap();
    tree.exec("plain", |txn| txn.put(b"k", b"v")).unwrap();
    tree.exec("prefixed", |txn| {
        txn.put(b"a", vec![0x41; 10])?;
        txn.put(b"b", vec![0x42; OVERFLOW_VALUE])
    })
    .unwrap();
}

/// Reads one allocator extent chain straight out of the file and reports how
/// many extent entries and how many pages it holds. This is the definition the
/// report's counters must match, computed without the checker under test.
fn chain_totals(bytes: &[u8], root: u32) -> (usize, usize) {
    let mut current = root;
    let mut entries = 0;
    let mut pages = 0;
    while current != 0 {
        let start = current as usize * PAGE_SIZE;
        let page = &bytes[start..start + PAGE_SIZE];
        let next = u32::from_le_bytes(page[4..8].try_into().unwrap());
        let count = u32::from_le_bytes(page[8..12].try_into().unwrap()) as usize;
        for index in 0..count {
            let at = 12 + index * 8;
            pages += u32::from_le_bytes(page[at + 4..at + 8].try_into().unwrap()) as usize;
        }
        entries += count;
        current = next;
    }
    (entries, pages)
}

/// The byte fields are the report's page counts scaled, and nothing else: a
/// byte field fed by any other counter - the allocator-list pages, say - fails
/// here even though the report field it is paired with would move with it. The
/// other half of that chain, the page counts themselves against the file's own
/// bytes, is `extent_counts_are_the_entries_of_the_persisted_chains`.
#[test]
fn space_bytes_are_the_reported_page_counts_times_the_page_size() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("space.db");
    seed(&path);

    let checked = check_path_with_space(&path).unwrap();
    assert_eq!(checked.report.status, CheckStatus::Ok);
    let space = checked.space.expect("a passing check publishes its space");
    let page = PAGE_SIZE as u64;

    assert_eq!(
        space.reusable_bytes,
        checked.report.reusable_pages as u64 * page
    );
    assert_eq!(
        space.retired_bytes,
        checked.report.retired_pages as u64 * page
    );
    assert_eq!(
        space.reusable_if_all_retired_promoted_bytes,
        space.reusable_bytes + space.retired_bytes
    );
    assert!(
        checked.buckets.is_none(),
        "a call without a scan collects no buckets"
    );
}

// Count pages from file length independently; exclude whole trailing pages so
// subtracting trailing_bytes cannot reduce the oracle to next_page_id.
#[test]
fn a_passing_check_accounts_for_every_data_page() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("accounted.db");
    seed(&path);
    {
        let tree = BTree::open(&path).unwrap();
        tree.exec("prefixed", |txn| txn.del(b"b")).unwrap();
    }

    let report = check_path(&path).unwrap();
    assert_eq!(report.status, CheckStatus::Ok);
    assert!(
        report.trailing_bytes < PAGE_SIZE as u64,
        "this anchor is only independent without whole trailing pages, got {}",
        report.trailing_bytes
    );
    let file_pages =
        (fs::metadata(&path).unwrap().len() - report.trailing_bytes) / PAGE_SIZE as u64;
    let data_pages = file_pages - 2;
    let active_data_pages = report.reachable_pages + report.allocator_list_pages;
    assert_eq!(
        active_data_pages + report.reusable_pages + report.retired_pages,
        data_pages as usize,
        "the reported ownership classes must account for every data page the file holds"
    );
}

/// The counters count extent *entries*, not the pages inside them: a run of
/// pages the runtime must walk once is one extent, however long it is.
#[test]
fn extent_counts_are_the_entries_of_the_persisted_chains() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("extents.db");
    seed(&path);

    let tree = BTree::open(&path).unwrap();
    tree.exec("prefixed", |txn| txn.del(b"b")).unwrap();
    drop(tree);

    let bytes = fs::read(&path).unwrap();
    let meta = selected_meta(&bytes);
    let (reusable_entries, reusable_pages) = chain_totals(&bytes, meta.reusable_root);
    let (retired_entries, retired_pages) = chain_totals(&bytes, meta.retired_root);

    let checked = check_path_with_space(&path).unwrap();
    assert_eq!(checked.report.status, CheckStatus::Ok);
    let space = checked.space.expect("a passing check publishes its space");

    assert_eq!(checked.report.reusable_pages, reusable_pages);
    assert_eq!(checked.report.retired_pages, retired_pages);
    assert_eq!(space.reusable_extent_count, reusable_entries);
    assert_eq!(space.retired_extent_count, retired_entries);

    let (entries, pages) = (
        reusable_entries + retired_entries,
        reusable_pages + retired_pages,
    );
    assert!(
        pages > 0,
        "the freed value chain must reach an extent chain"
    );
    assert!(
        entries < pages,
        "a freed run of pages is one extent, so entries ({entries}) and pages \
         ({pages}) must differ; a fixture that merged them would pin nothing"
    );
    assert_eq!(
        space.reusable_if_all_retired_promoted_bytes,
        (reusable_pages + retired_pages) as u64 * PAGE_SIZE as u64
    );
}

/// A report that did not pass describes how far the walk got. Publishing the
/// counters gathered on the way would dress a damaged file up as a partly
/// measured healthy one.
#[test]
fn a_failed_check_publishes_no_statistics() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("damaged.db");
    seed(&path);

    let mut bytes = fs::read(&path).unwrap();
    let meta = selected_meta(&bytes);
    assert!(
        meta.catalog_root != 0,
        "the fixture must have a catalog to damage"
    );
    // The catalog root is one page the checker certainly reads, unlike the
    // pages an extent chain covers, which it only claims and never opens.
    bytes[meta.catalog_root as usize * PAGE_SIZE + 64] ^= 0xFF;
    fs::write(&path, &bytes).unwrap();

    let checked = check_path_with_options(&path, &CheckOptions { scan: true }).unwrap();
    assert_eq!(checked.report.status, CheckStatus::Failed);
    assert!(
        checked.space.is_none(),
        "a failed check has no space to report"
    );
    assert!(
        checked.buckets.is_none(),
        "a failed check has no buckets to report"
    );
    assert_eq!(check_path(&path).unwrap(), checked.report);
}

/// The two reports that are built before the walk starts - a data id space that
/// is not physically covered, and a metadata record that reserves fewer than
/// the two meta slots - must withhold their statistics the same way a walk that
/// failed does. They are the only reports that never pass through the checker.
#[test]
fn the_pre_walk_failures_publish_no_statistics() {
    let dir = TempDir::new().unwrap();

    let truncated = dir.path().join("truncated.db");
    seed(&truncated);
    let bytes = fs::read(&truncated).unwrap();
    let meta = selected_meta(&bytes);
    let keep = (meta.next_page_id as usize - 1) * PAGE_SIZE;
    assert!(keep > 2 * PAGE_SIZE, "the fixture must keep its meta slots");
    fs::write(&truncated, &bytes[..keep]).unwrap();

    let checked = check_path_with_options(&truncated, &CheckOptions { scan: true }).unwrap();
    assert_eq!(checked.report.status, CheckStatus::Failed);
    assert!(
        checked
            .report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "MISSING_PHYSICAL_PAGE"),
        "the fixture must reach the coverage check: {:?}",
        checked.report.diagnostics
    );
    assert!(
        checked.space.is_none(),
        "no space beside a report that failed"
    );
    assert!(
        checked.buckets.is_none(),
        "no buckets beside a report that failed"
    );

    // A record that reserves fewer than the two metadata slots: the file is
    // long enough to hold it, so this is a metadata failure rather than a
    // coverage one.
    let short = dir.path().join("short.db");
    seed(&short);
    let mut bytes = fs::read(&short).unwrap();
    for offset in [0usize, PAGE_SIZE] {
        let mut record = MetaNode::from_slice(&bytes[offset..offset + 40]);
        record.next_page_id = 1;
        record.update_checksum();
        bytes[offset..offset + 40].copy_from_slice(record.as_page_slice());
    }
    fs::write(&short, &bytes).unwrap();

    let checked = check_path_with_options(&short, &CheckOptions { scan: true }).unwrap();
    assert_eq!(checked.report.status, CheckStatus::Failed);
    assert!(
        checked
            .report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "INVALID_NEXT_PAGE_ID"),
        "{:?}",
        checked.report.diagnostics
    );
    assert!(checked.space.is_none());
    assert!(checked.buckets.is_none());
}

/// Two shapes the single-leaf fixtures never reach: a bucket whose value
/// outgrows the direct page slots and needs an indirect page to find its value
/// pages, and a bucket that outgrows one leaf and needs a branch above them.
#[test]
fn a_scan_counts_an_indirect_chain_and_a_deep_tree() {
    const RECORDS: u32 = 2_000;
    const INDIRECT_VALUE: usize = 40_000;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("deep.db");
    {
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("chain", false).unwrap();
        tree.new_bucket("deep", false).unwrap();
        tree.exec("chain", |txn| txn.put(b"k", vec![0x42; INDIRECT_VALUE]))
            .unwrap();
        tree.exec("deep", |txn| {
            for index in 0..RECORDS {
                txn.put(format!("k{index:05}").as_bytes(), vec![0x41; 24])?;
            }
            Ok(())
        })
        .unwrap();
    }

    let checked = check_path_with_options(&path, &CheckOptions { scan: true }).unwrap();
    assert_eq!(checked.report.status, CheckStatus::Ok);
    let buckets = checked
        .buckets
        .expect("a scan over a passing check has buckets");
    let by_name = |name: &str| {
        buckets
            .iter()
            .find(|b| b.name == name)
            .unwrap_or_else(|| panic!("{name} must be reported"))
    };

    // One record, so the tree is a single leaf and the page count is the value
    // arithmetic the walk had to do plus that one node page.
    let value_pages = INDIRECT_VALUE.div_ceil(4092);
    let indirect_pages = value_pages.div_ceil((4096 - 8) / 4);
    assert!(value_pages > 5, "the fixture must need an indirect chain");
    let chain = by_name("chain");
    assert_eq!(chain.records, 1);
    assert_eq!(chain.logical_value_bytes, INDIRECT_VALUE as u64);
    assert_eq!(chain.tree_height, 1);
    assert_eq!(
        chain.reachable_pages,
        1 + value_pages + indirect_pages,
        "the leaf, its value pages, and the indirect page indexing them"
    );

    // The exact leaf count is the writer's packing decision, so what is pinned
    // is that the walk found a branch, and that every level it walked is
    // counted as a page.
    let deep = by_name("deep");
    assert_eq!(deep.records, RECORDS as usize);
    assert_eq!(deep.logical_key_bytes, RECORDS as u64 * 6);
    assert_eq!(deep.logical_value_bytes, RECORDS as u64 * 24);
    assert!(
        deep.tree_height >= 2,
        "{} records cannot fit in one leaf, so the walk must have gone through a branch",
        RECORDS
    );
    assert!(
        deep.reachable_pages >= deep.tree_height,
        "every level of the walk is a node page, and every record is one"
    );
    assert!(
        deep.reachable_pages <= deep.records + deep.tree_height - 1,
        "a node holds at least one record or one child, so it cannot be wider"
    );

    let attributed: usize = buckets.iter().map(|b| b.reachable_pages).sum();
    assert!(attributed <= checked.report.reachable_pages);
}

/// A scan must not change the report it is reading: a caller that scans and one
/// that does not are looking at the same check.
#[test]
fn a_scan_does_not_change_the_report_it_reads() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("unchanged.db");
    seed(&path);

    let plain = check_path(&path).unwrap();
    let scanned = check_path_with_options(&path, &CheckOptions { scan: true }).unwrap();
    assert_eq!(scanned.report, plain);
    assert!(scanned.buckets.is_some());
}

#[test]
fn a_scan_reports_exactly_what_each_bucket_holds() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("scan.db");
    seed(&path);

    let checked = check_path_with_options(&path, &CheckOptions { scan: true }).unwrap();
    assert_eq!(checked.report.status, CheckStatus::Ok);
    let buckets = checked
        .buckets
        .expect("a scan over a passing check has buckets");

    // The catalog is a tree keyed by name, so its in-order walk is name order
    // whatever order the buckets were created in.
    let names: Vec<&str> = buckets.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(names, ["empty", "plain", "prefixed"]);

    let by_name = |name: &str| {
        buckets
            .iter()
            .find(|b| b.name == name)
            .unwrap_or_else(|| panic!("{name} must be reported"))
    };

    let plain = by_name("plain");
    assert!(!plain.prefix_encoding);
    assert_eq!(plain.records, 1);
    assert_eq!(plain.logical_key_bytes, 1);
    assert_eq!(plain.logical_value_bytes, 1);
    assert_eq!(plain.tree_height, 1, "a single leaf node is one level");
    assert_eq!(
        plain.reachable_pages, 1,
        "an inline record needs no other page"
    );

    let prefixed = by_name("prefixed");
    assert!(prefixed.prefix_encoding);
    assert_eq!(prefixed.records, 2);
    assert_eq!(prefixed.logical_key_bytes, 2);
    assert_eq!(prefixed.logical_value_bytes, 10 + OVERFLOW_VALUE as u64);
    assert_eq!(prefixed.tree_height, 1);
    assert_eq!(
        prefixed.reachable_pages,
        1 + OVERFLOW_VALUE_PAGES,
        "the node page plus the value pages the long record overflowed onto"
    );

    let empty = by_name("empty");
    assert_eq!(
        (
            empty.records,
            empty.logical_key_bytes,
            empty.logical_value_bytes,
            empty.tree_height,
            empty.reachable_pages
        ),
        (0, 0, 0, 0, 0),
        "a bucket with no root page owns nothing and has no height"
    );

    let attributed: usize = buckets.iter().map(|b| b.reachable_pages).sum();
    assert!(
        attributed <= checked.report.reachable_pages,
        "bucket pages are part of the report's reachable pages, never more"
    );
    assert!(attributed > 0);
}

#[test]
fn multi_page_allocator_lists_report_nonzero_retired_space_and_reject_overlap() {
    const EXTENTS: usize = 520;
    const CAPACITY: usize = (PAGE_SIZE - 12) / 8;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("allocator-chains.db");
    let mut meta = MetaNode::new();
    meta.next_page_id = (6 + EXTENTS * 2) as u32;
    meta.reusable_root = 2;
    meta.retired_root = 4;
    meta.update_checksum();
    let mut bytes = vec![0; meta.next_page_id as usize * PAGE_SIZE];
    for offset in [0, PAGE_SIZE] {
        bytes[offset..offset + 40].copy_from_slice(meta.as_page_slice());
    }
    for (root, parity) in [(2usize, 0usize), (4, 1)] {
        for (part, start) in [0, CAPACITY].into_iter().enumerate() {
            let pid = root + part;
            let page = &mut bytes[pid * PAGE_SIZE..(pid + 1) * PAGE_SIZE];
            let count = (EXTENTS - start).min(CAPACITY);
            let next = if part == 0 { (pid + 1) as u32 } else { 0 };
            page[4..8].copy_from_slice(&next.to_le_bytes());
            page[8..12].copy_from_slice(&(count as u32).to_le_bytes());
            for index in 0..count {
                let at = 12 + index * 8;
                let data_pid = (6 + 2 * (start + index) + parity) as u32;
                page[at..at + 4].copy_from_slice(&data_pid.to_le_bytes());
                page[at + 4..at + 8].copy_from_slice(&1u32.to_le_bytes());
            }
            common::seal_page(page, pid as u32);
        }
    }
    fs::write(&path, &bytes).unwrap();
    let checked = check_path_with_space(&path).unwrap();
    assert_eq!(
        checked.report.status,
        CheckStatus::Ok,
        "{:?}",
        checked.report
    );
    assert_eq!(checked.report.allocator_list_pages, 4);
    assert_eq!(checked.report.reusable_pages, EXTENTS);
    assert_eq!(checked.report.retired_pages, EXTENTS);
    let space = checked.space.unwrap();
    assert_eq!(space.reusable_extent_count, EXTENTS);
    assert_eq!(space.retired_extent_count, EXTENTS);
    assert_eq!(space.retired_bytes, (EXTENTS * PAGE_SIZE) as u64);
    assert_eq!(
        space.reusable_if_all_retired_promoted_bytes,
        (2 * EXTENTS * PAGE_SIZE) as u64
    );
    assert_eq!(
        checked.report.allocator_list_pages
            + checked.report.reusable_pages
            + checked.report.retired_pages,
        fs::metadata(&path).unwrap().len() as usize / PAGE_SIZE - 2
    );

    let page = &mut bytes[4 * PAGE_SIZE..5 * PAGE_SIZE];
    page[12..16].copy_from_slice(&6u32.to_le_bytes());
    common::seal_page(page, 4);
    fs::write(&path, bytes).unwrap();
    let checked = check_path_with_space(&path).unwrap();
    assert_eq!(checked.report.status, CheckStatus::Failed);
    assert!(
        checked
            .report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "REUSABLE_RETIRED_OVERLAP")
    );
    assert!(checked.space.is_none());
}

#[test]
fn a_snapshot_with_a_pinned_reader_counts_actual_retirement_history() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("retirement.db");
    let snapshot = dir.path().join("retired-snapshot.db");
    seed(&path);
    let tree = BTree::open(&path).unwrap();
    let writer = tree.clone();
    tree.view("prefixed", |old| {
        for value in [0x43, 0x44, 0x45] {
            writer.exec("prefixed", |txn| txn.put(b"b", vec![value; OVERFLOW_VALUE]))?;
        }
        assert_eq!(old.get(b"b")?, vec![0x42; OVERFLOW_VALUE]);
        tree.take_snapshot(&snapshot).unwrap();
        Ok(())
    })
    .unwrap();
    let bytes = fs::read(&snapshot).unwrap();
    let meta = selected_meta(&bytes);
    let (entries, pages) = chain_totals(&bytes, meta.retired_root);
    assert!(
        entries > 0 && pages > 0,
        "the reader must keep old generations retired"
    );
    let checked = check_path_with_space(&snapshot).unwrap();
    assert_eq!(
        checked.report.status,
        CheckStatus::Ok,
        "{:?}",
        checked.report
    );
    assert_eq!(checked.report.retired_pages, pages);
    let space = checked.space.unwrap();
    assert_eq!(space.retired_extent_count, entries);
    assert_eq!(space.retired_bytes, (pages * PAGE_SIZE) as u64);
}
