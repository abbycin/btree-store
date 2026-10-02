//! Migration acceptance: rebuild each committed v1 fixture with the real CLI and
//! check the target against that fixture's own expectation list, byte for byte.
//!
//! The expectation list is `tests/fixtures/v1_shapes.manifest.txt`, generated with
//! the fixture by the pre-v2 writer. Parsing it here keeps the check independent
//! of the CLI's own comparison.

mod common;

use btree_store::BTree;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_btree-store");

struct Fixture {
    file: &'static str,
    manifest: &'static str,
    expected_buckets: usize,
    expected_records: usize,
    min_batches: u64,
}

const SMALL: Fixture = Fixture {
    file: "tests/fixtures/v1_shapes.v1",
    manifest: "tests/fixtures/v1_shapes.manifest.txt",
    expected_buckets: 3,
    expected_records: 72,
    min_batches: 1,
};

const LARGE: Fixture = Fixture {
    file: "tests/fixtures/v1_large.v1",
    manifest: "tests/fixtures/v1_large.manifest.txt",
    expected_buckets: 3,
    expected_records: 4302,
    min_batches: 2,
};

fn repo_file(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn run_migrate(source: &Path, destination: &Path) -> Output {
    let mut command: Command = common::child_test_command(Path::new(BIN));
    command
        .args(["migrate"])
        .arg(source)
        .arg("--output")
        .arg(destination)
        .args(["--to", "2"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("the CLI must run")
}

type Expectations = BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>>;

/// The provenance header of a manifest: the format version its writer used and the
/// generation sequence it left behind. The acceptance test compares these against
/// the summary, so a misreported source version or generation cannot pass.
struct Provenance {
    format_version: u32,
    generation: u64,
}

fn parse_manifest(manifest: &str) -> (Expectations, Vec<String>, Vec<(String, bool)>, Provenance) {
    let text = fs::read_to_string(repo_file(manifest)).unwrap();
    let mut entries: Expectations = BTreeMap::new();
    let mut empty_buckets = Vec::new();
    let mut policies: Vec<(String, bool)> = Vec::new();
    let mut provenance = Provenance {
        format_version: 0,
        generation: 0,
    };

    for line in text.lines() {
        let line = line.trim_end();
        if let Some(rest) = line.strip_prefix("generator-format-version: ") {
            provenance.format_version = rest.trim().parse().expect("generator-format-version");
            continue;
        }
        if let Some(rest) = line.strip_prefix("generation-seq: ") {
            provenance.generation = rest.trim().parse().expect("generation-seq");
            continue;
        }
        if let Some(rest) = line.strip_prefix("empty-buckets: ") {
            empty_buckets = rest
                .split(',')
                .map(|name| name.trim().to_string())
                .collect();
            continue;
        }
        if let Some(rest) = line.strip_prefix("bucket-policies: ") {
            for entry in rest.split(',') {
                let (name, policy) = entry.split_once("=prefix:").expect("bucket-policies entry");
                policies.push((name.trim().to_string(), policy.trim() == "true"));
            }
            continue;
        }
        let Some(rest) = line.strip_prefix("  ") else {
            continue;
        };
        let mut parts = rest.splitn(4, ' ');
        let bucket = parts.next().unwrap().to_string();
        let key_repr = parts.next().unwrap();
        let len_field = parts.next().unwrap();
        let value_repr = parts
            .next()
            .unwrap()
            .strip_prefix("value=")
            .expect("the value descriptor is introduced by value=");

        let key = parse_key(key_repr);
        let value = parse_value(value_repr);
        let declared: usize = len_field
            .strip_prefix("len=")
            .unwrap()
            .parse()
            .expect("len field");
        assert_eq!(declared, value.len(), "manifest length for {key_repr}");

        entries.entry(bucket).or_default().push((key, value));
    }

    (entries, empty_buckets, policies, provenance)
}

fn parse_key(repr: &str) -> Vec<u8> {
    let inner = repr
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .expect("keys are rendered as quoted strings");
    let mut bytes = Vec::new();
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            assert!(ch.is_ascii(), "the fixture keys are ASCII");
            bytes.push(ch as u8);
            continue;
        }
        match chars.next().expect("escape") {
            'x' => {
                let hex: String = (&mut chars).take(2).collect();
                bytes.push(u8::from_str_radix(&hex, 16).expect("hex escape"));
            }
            other => bytes.push(other as u8),
        }
    }
    bytes
}

fn parse_value(repr: &str) -> Vec<u8> {
    if repr == "empty" {
        return Vec::new();
    }
    if let Some(rest) = repr.strip_prefix("fill:0x") {
        let (byte, len) = rest.split_once(':').expect("fill:0xNN:LEN");
        let byte = u8::from_str_radix(byte, 16).expect("fill byte");
        let len: usize = len.parse().expect("fill length");
        return vec![byte; len];
    }
    if let Some(len) = repr.strip_prefix("pattern:i%251:") {
        let len: usize = len.parse().expect("pattern length");
        return (0..len).map(|i| (i % 251) as u8).collect();
    }
    let inner = repr
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .expect("literals are rendered as byte arrays");
    if inner.is_empty() {
        return Vec::new();
    }
    inner
        .split(", ")
        .map(|byte| byte.parse().expect("byte literal"))
        .collect()
}

/// A source whose graph cannot be validated must be refused before anything is
/// created: no target, no staging, and no attempt to salvage the readable part.
#[test]
fn a_corrupt_source_is_refused_before_anything_is_created() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("broken.v1");
    let destination = dir.path().join("broken-out.db");
    fs::copy(repo_file(SMALL.file), &source).unwrap();

    {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&source)
            .unwrap();
        for offset in [0u64, 4096] {
            let mut page = [0u8; 4096];
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.read_exact(&mut page).unwrap();
            let mut meta = btree_store::MetaNode::from_slice(&page);
            meta.next_page_id = 5;
            meta.update_checksum();
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(meta.as_page_slice()).unwrap();
        }
        file.sync_all().unwrap();
    }

    let output = run_migrate(&source, &destination);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("error: source: source-corrupt:"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("V1_") && !stderr.contains("V1_NO_VALID_META"),
        "the refusal must come from the graph check, not from an unreadable meta: {stderr}"
    );
    assert!(output.stdout.is_empty(), "no summary on failure");
    assert!(!destination.exists(), "a refused source creates no target");
    assert!(
        fs::read_dir(dir.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".migrate-")),
        "a refused source creates no staging file"
    );
}

/// A source that declares more pages than the file holds cannot be honoured: the
/// pages its id space covers do not exist. The refusal is phase 1 corruption - not a
/// decoder failure and not a half-migrated output.
#[test]
fn a_source_declaring_more_pages_than_it_holds_is_refused() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("over-declared.v1");
    let destination = dir.path().join("over-declared-out.db");
    let catalog = crafted_leaf(b"b", 8, 0, &[3, 0, 0, 0, 0, 0, 0, 0]);
    let bucket = crafted_leaf(b"a", 1, 0, &[0x41]);
    fs::write(
        &source,
        crafted_source(4, &[(0, crafted_meta(2, 10)), (2, catalog), (3, bucket)]),
    )
    .unwrap();

    let output = run_migrate(&source, &destination);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("error: source: source-corrupt: V1_OWNERSHIP_UNACCOUNTED"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("; staging "),
        "the source must be refused before the staging file is created: {stderr}"
    );
    assert!(output.stdout.is_empty(), "no summary on refusal");
    assert!(!destination.exists(), "a refused source creates no target");
    assert_no_staging(dir.path());
}

/// The same for a truncated source: pages the graph names must exist.
#[test]
fn a_truncated_source_is_refused() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("short.v1");
    let destination = dir.path().join("short-out.db");
    fs::copy(repo_file(SMALL.file), &source).unwrap();
    let file = fs::OpenOptions::new().write(true).open(&source).unwrap();
    file.set_len(8192).unwrap();
    drop(file);

    let output = run_migrate(&source, &destination);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("error: source: source-corrupt:"),
        "stderr: {stderr}"
    );
    assert!(output.stdout.is_empty(), "no summary on refusal");
    assert!(!destination.exists());
    assert_no_staging(dir.path());
}

/// A source that references a value page it does not hold must be refused before
/// anything is created. `next_page_id` is the source's own claim, so it cannot bound
/// a reference on its own: the page below is inside the declared id space and still
/// missing, and only the file length says so. Node and indirect pages are read during
/// the walk and would be caught there anyway, while a *data* page is read later, by
/// the rebuild - after the staging file exists.
#[test]
fn a_value_page_past_the_end_of_the_file_is_refused_before_anything_is_created() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("dangling-value.v1");
    let destination = dir.path().join("dangling-out.db");
    fs::write(&source, v1_with_a_dangling_value_page()).unwrap();

    let output = run_migrate(&source, &destination);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("error: source: source-corrupt: V1_PAGE_BEYOND_EOF"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("; staging "),
        "the source must be refused before the staging file is created: {stderr}"
    );
    assert!(output.stdout.is_empty(), "no summary on refusal");
    assert!(!destination.exists(), "a refused source creates no target");
    assert_no_staging(dir.path());
}

/// Four pages: two meta slots (the second one torn), a catalog leaf naming one
/// bucket, a bucket leaf whose single value is an overflow of one page - and no such
/// page. The value page (4) is inside `next_page_id` (5) and outside the file, so
/// every other page is accounted for by the graph.
fn v1_with_a_dangling_value_page() -> Vec<u8> {
    let catalog = crafted_leaf(b"b", 8, 0, &[3, 0, 0, 0, 0, 0, 0, 0]);
    let bucket = crafted_leaf(b"a", V1_PAGE as u32, 4, &[]);
    crafted_source(4, &[(0, crafted_meta(2, 5)), (2, catalog), (3, bucket)])
}

/// Page size of the frozen v1 format. The crafted sources below write the format by
/// hand, so they keep their own constants instead of importing the decoder's.
const V1_PAGE: usize = 4096;
const V1_NODE_HEADER: usize = 12; // plain node: is_leaf, elems, payload offset
const V1_SLOT: usize = 32;
const V1_PAYLOAD: usize = V1_NODE_HEADER + V1_SLOT;
const V1_IDS_PER_INDIRECT: usize = (V1_PAGE - 4) / 4;

/// The 40-byte v1 meta record, checksummed through the library so the record
/// checksum is the format's own rather than a second implementation of it.
fn crafted_meta(catalog_root: u32, next_page_id: u32) -> Vec<u8> {
    let mut meta = btree_store::MetaNode {
        magic: btree_store::MAGIC,
        seq: 1,
        format_version: 1,
        catalog_root,
        next_page_id,
        reusable_root: 0,
        retired_root: 0,
        checksum: 0,
    };
    meta.update_checksum();
    meta.as_page_slice().to_vec()
}

/// One plain leaf page with a single entry: `key` at the payload start, then `tail`
/// (the inline value, or nothing for an overflow), and the slot. `pid` is
/// `page_id[0]`, which decides between an inline value (0) and an overflow.
fn crafted_leaf(key: &[u8], vlen: u32, pid: u32, tail: &[u8]) -> Vec<u8> {
    let mut page = vec![0u8; V1_PAGE];
    page[0..4].copy_from_slice(&1u32.to_le_bytes()); // is_leaf
    page[4..8].copy_from_slice(&1u32.to_le_bytes()); // elems
    page[8..12].copy_from_slice(&(V1_PAYLOAD as u32).to_le_bytes());
    page[V1_PAYLOAD..V1_PAYLOAD + key.len()].copy_from_slice(key);
    page[V1_PAYLOAD + key.len()..V1_PAYLOAD + key.len() + tail.len()].copy_from_slice(tail);
    let mut slot = [0u8; V1_SLOT];
    slot[0..4].copy_from_slice(&(V1_PAYLOAD as u32).to_le_bytes());
    slot[4..8].copy_from_slice(&(key.len() as u32).to_le_bytes());
    slot[8..12].copy_from_slice(&vlen.to_le_bytes());
    slot[12..16].copy_from_slice(&pid.to_le_bytes());
    page[V1_NODE_HEADER..V1_NODE_HEADER + V1_SLOT].copy_from_slice(&slot);
    page
}

fn crafted_source(pages: usize, placements: &[(usize, Vec<u8>)]) -> Vec<u8> {
    let mut file = vec![0u8; pages * V1_PAGE];
    for (pid, bytes) in placements {
        file[pid * V1_PAGE..pid * V1_PAGE + bytes.len()].copy_from_slice(bytes);
    }
    file
}

/// A bucket value whose *page count* the file cannot supply must be refused before
/// anything is created, even when every page the chain names does exist: a chain may
/// name one page over and over, so the count - not only each reference - has to be
/// held to the file. Without it, `read_value` sizes its buffer from a `vlen` bounded
/// only by the 2 GiB format maximum, so a few kilobytes of source can ask for
/// gigabytes, and the refusal would arrive after the staging file exists.
#[test]
fn a_value_claiming_more_pages_than_the_source_holds_is_refused_before_anything_is_created() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("overclaimed-value.v1");
    let destination = dir.path().join("overclaimed-out.db");
    fs::write(&source, v1_claiming_more_value_pages_than_it_holds()).unwrap();

    let output = run_migrate(&source, &destination);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("error: source: source-corrupt: V1_VALUE_PAGES_OUT_OF_RANGE"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("; staging "),
        "the source must be refused before the staging file is created: {stderr}"
    );
    assert!(output.stdout.is_empty(), "no summary on refusal");
    assert!(!destination.exists(), "a refused source creates no target");
    assert_no_staging(dir.path());
}

/// Ten pages: two meta slots (the second one torn), a catalog leaf naming one bucket,
/// a bucket leaf whose value claims `5 * 1023 = 5115` data pages, then a complete
/// five-page indirect chain that lists one *existing* data page that many times, and
/// that data page itself. Every reference is inside the file and every page in
/// `[2, next_page_id)` has exactly one owner - no page is named twice by two
/// ownership classes - so nothing but the page count is out of range.
fn v1_claiming_more_value_pages_than_it_holds() -> Vec<u8> {
    const INDEX_PAGES: usize = 5;
    const FIRST_INDEX: usize = 4; // the chain, right after the two nodes
    const DATA_PAGE: u32 = (FIRST_INDEX + INDEX_PAGES) as u32; // the page it reuses
    let pages = DATA_PAGE as usize + 1;

    let claimed = INDEX_PAGES * V1_IDS_PER_INDIRECT;
    let mut chain = Vec::new();
    for index in 0..INDEX_PAGES {
        let mut page = vec![0u8; V1_PAGE];
        for slot in 0..V1_IDS_PER_INDIRECT {
            page[slot * 4..slot * 4 + 4].copy_from_slice(&DATA_PAGE.to_le_bytes());
        }
        let next = if index + 1 == INDEX_PAGES {
            0
        } else {
            (FIRST_INDEX + index + 1) as u32
        };
        page[V1_PAGE - 4..].copy_from_slice(&next.to_le_bytes());
        chain.push((FIRST_INDEX + index, page));
    }

    let mut placements = vec![
        (0, crafted_meta(2, pages as u32)),
        (2, crafted_leaf(b"b", 8, 0, &[3, 0, 0, 0, 0, 0, 0, 0])),
        (
            3,
            crafted_leaf(b"a", (claimed * V1_PAGE) as u32, FIRST_INDEX as u32, &[]),
        ),
    ];
    placements.extend(chain);
    crafted_source(pages, &placements)
}

/// A slot whose length fields are zeroed while its position is garbage is what a
/// torn sector can leave behind: the decoder must report it, not panic.
#[test]
fn a_patched_slot_position_is_reported_not_a_panic() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let dir = TempDir::new().unwrap();
    let source = dir.path().join("patched.v1");
    let destination = dir.path().join("patched-out.db");
    fs::copy(repo_file(SMALL.file), &source).unwrap();

    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&source)
        .unwrap();
    let mut patched = None;
    for pid in 2u64..64 {
        let mut page = [0u8; 4096];
        file.seek(SeekFrom::Start(pid * 4096)).unwrap();
        if file.read_exact(&mut page).is_err() {
            break;
        }
        if u32::from_le_bytes(page[0..4].try_into().unwrap()) == 1 {
            page[12..16].copy_from_slice(&u32::MAX.to_le_bytes()); // pos
            page[16..20].copy_from_slice(&0u32.to_le_bytes()); // klen
            page[20..24].copy_from_slice(&0u32.to_le_bytes()); // vlen
            page[24..28].copy_from_slice(&26u32.to_le_bytes()); // page_id[0] != 0
            file.seek(SeekFrom::Start(pid * 4096)).unwrap();
            file.write_all(&page).unwrap();
            patched = Some(pid);
            break;
        }
    }
    file.sync_all().unwrap();
    drop(file);
    assert!(patched.is_some(), "the fixture must contain a plain leaf");

    let output = run_migrate(&source, &destination);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a garbage slot must be a reported failure, not a panic: {stderr}"
    );
    assert!(
        stderr.contains("error: source: source-corrupt: V1_SLOT_KEY_OUT_OF_RANGE"),
        "stderr: {stderr}"
    );
    assert!(output.stdout.is_empty(), "no summary on refusal");
    assert!(!destination.exists());
    assert_no_staging(dir.path());
}

fn assert_no_staging(dir: &Path) {
    assert!(
        fs::read_dir(dir).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".migrate-")),
        "a run must not leave a staging file behind"
    );
}

#[test]
fn the_cli_migrates_the_v1_fixture_byte_for_byte() {
    check_fixture(&SMALL);
}

#[test]
fn the_cli_migrates_a_fixture_larger_than_one_batch() {
    check_fixture(&LARGE);
}

fn check_fixture(fixture: &Fixture) {
    let (mut expectations, empty_buckets, mut declared_policies, provenance) =
        parse_manifest(fixture.manifest);
    declared_policies.sort();
    assert_eq!(
        expectations.len() + empty_buckets.len(),
        fixture.expected_buckets,
        "the manifest must describe every bucket"
    );
    for entries in expectations.values_mut() {
        entries.sort_by(|a, b| a.0.cmp(&b.0));
    }

    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source.v1");
    let destination = dir.path().join("migrated.db");
    fs::copy(repo_file(fixture.file), &source).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&source, fs::Permissions::from_mode(0o400)).unwrap();
    }
    let source_before = fs::read(&source).unwrap();

    let output = run_migrate(&source, &destination);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "stderr: {stderr}");

    for key in [
        "source=",
        "source_version=",
        "source_generation=",
        "source_ignored_invalid_candidate=",
        "target=",
        "target_version=",
        "target_generation=",
        "buckets=",
        "records=",
        "logical_bytes=",
        "physical_bytes=",
        "verified=true",
    ] {
        assert!(stdout.contains(key), "summary is missing {key}: {stdout}");
    }
    assert!(
        stdout.contains(&format!("buckets={}", fixture.expected_buckets)),
        "summary: {stdout}"
    );
    // The fixture's own manifest records which writer produced it and which
    // generation it holds: the summary must agree, not merely print the keys.
    assert!(
        stdout.contains(&format!("source_version={}", provenance.format_version)),
        "summary: {stdout}"
    );
    assert!(
        stdout.contains(&format!("source_generation={}", provenance.generation)),
        "summary: {stdout}"
    );
    assert!(
        stdout.contains("source_ignored_invalid_candidate=false"),
        "an intact fixture must report no ignored slot: {stdout}"
    );
    let batches: u64 = stdout
        .lines()
        .find_map(|line| line.strip_prefix("batches="))
        .and_then(|value| value.parse().ok())
        .expect("the summary reports the batch count");
    assert!(
        batches >= fixture.min_batches,
        "expected at least {} batches, summary says {batches}",
        fixture.min_batches
    );

    // The source is never touched.
    assert_eq!(fs::read(&source).unwrap(), source_before);
    assert_eq!(
        fs::read_dir(dir.path())
            .unwrap()
            .filter(|entry| entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".migrate-"))
            .count(),
        0,
        "no staging file may survive a successful migration"
    );

    // The published output carries the source's owner bits narrowed to 0600.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let source_mode = fs::metadata(&source).unwrap().permissions().mode() & 0o7777;
        let target_mode = fs::metadata(&destination).unwrap().permissions().mode() & 0o7777;
        let expected = {
            let narrowed = source_mode & 0o600;
            if narrowed & 0o400 == 0 {
                0o400
            } else {
                narrowed
            }
        };
        assert_eq!(
            target_mode, expected,
            "the target must be the source's owner bits (source {source_mode:o})"
        );
        assert_eq!(
            target_mode & 0o077,
            0,
            "the target must not be group/world accessible"
        );
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o600)).unwrap();
    }

    let target = BTree::open(&destination).expect("the target must open");
    let policies = target.buckets_with_policy().unwrap();
    let mut actual = policies.clone();
    actual.sort();
    assert_eq!(
        actual, declared_policies,
        "bucket names and policies must survive exactly as the manifest declares"
    );
    if fixture.file == SMALL.file {
        assert_eq!(
            policies,
            vec![
                ("empty".to_string(), false),
                ("plain".to_string(), false),
                ("prefixed".to_string(), true),
            ],
            "catalog order must be preserved"
        );
    }
    for name in &empty_buckets {
        target
            .view(name, |txn| {
                let mut iter = txn.iter();
                let (mut key, mut value) = (Vec::new(), Vec::new());
                assert!(
                    !iter.next_ref(&mut key, &mut value),
                    "bucket {name} must stay empty"
                );
                Ok(())
            })
            .unwrap();
    }

    let mut records = 0usize;
    let mut logical_bytes = 0u64;
    for (bucket, entries) in &expectations {
        let mut index = 0usize;
        target
            .view(bucket, |txn| {
                let mut iter = txn.iter();
                let (mut key, mut value) = (Vec::new(), Vec::new());
                while iter.next_ref(&mut key, &mut value) {
                    let Some((expected_key, expected_value)) = entries.get(index) else {
                        panic!("bucket {bucket} has an extra record at index {index}: {key:?}");
                    };
                    assert_eq!(&key, expected_key, "bucket {bucket} record {index} key");
                    assert_eq!(&value, expected_value, "bucket {bucket} key {key:?}");
                    logical_bytes += key.len() as u64 + value.len() as u64;
                    records += 1;
                    index += 1;
                }
                assert_eq!(
                    index,
                    entries.len(),
                    "bucket {bucket} is missing records after index {index}"
                );
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(
        records, fixture.expected_records,
        "the fixture holds {} records",
        fixture.expected_records
    );

    // The summary's counters must match what was just verified, exactly.
    let summary = |key: &str| -> String {
        stdout
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key}=")))
            .unwrap_or_else(|| panic!("summary is missing {key}: {stdout}"))
            .to_string()
    };
    assert_eq!(summary("records"), records.to_string());
    assert_eq!(summary("logical_bytes"), logical_bytes.to_string());
    assert_eq!(
        summary("physical_bytes"),
        fs::metadata(&destination).unwrap().len().to_string()
    );
    assert_eq!(summary("verified"), "true");
}
