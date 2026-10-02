use crate::node::PAGE_SIZE;
use crate::node::{ENCODED_BRANCH, ENCODED_LEAF, IDS_PER_INDIRECT_PAGE, SLOT_SIZE};
use crate::{FORMAT_VERSION, MAGIC, OpenIoError, PageId, page};
use std::collections::HashSet;
use std::fmt;
use std::fs::{File, OpenOptions as FileOpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const META_RECORD_LEN: u64 = 40;
const META_CHECKSUM_END: usize = 36;
const META_SLOT_SIZE: u64 = PAGE_SIZE as u64;
const OPEN_LOCK_TIMEOUT: Duration = Duration::from_secs(1);
const OPEN_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(1);
const PLAIN_HEADER_SIZE: usize = 16;
const ENCODED_HEADER_SIZE: usize = 20;
const NR_INLINE_PAGE: usize = 5;
/// The indirect page's next-pointer sits in the four bytes before the trailer,
/// so it moves with the trailer instead of being typed against the page size.
const INDIRECT_NEXT_OFFSET: usize = crate::page::TRAILER_CRC_OFFSET - 4;
const INDIRECT_IDS_PER_PAGE: usize = IDS_PER_INDIRECT_PAGE;
const EXTENT_HEADER_SIZE: usize = 12;
const EXTENT_SIZE: usize = 8;
const EXTENT_PER_PAGE: usize = (PAGE_SIZE - EXTENT_HEADER_SIZE) / EXTENT_SIZE;
const MAX_KEY_LEN: usize = crate::node::MAX_KEY_LEN;
const MAX_VAL_LEN: u32 = crate::node::MAX_VAL_LEN as u32;
const MAX_INLINE_LEN: usize = crate::node::MAX_INLINE_LEN;
const TRAILER_CONTENT_SIZE: usize = PAGE_SIZE - 4;

const REACHABLE_NODE: u8 = 1 << 0;
const REACHABLE_VALUE: u8 = 1 << 1;
const REACHABLE_INDIRECT: u8 = 1 << 2;
const REUSABLE: u8 = 1 << 3;
const RETIRED: u8 = 1 << 4;
const REUSABLE_LIST: u8 = 1 << 5;
const RETIRED_LIST: u8 = 1 << 6;

const META_SLOT_A: u64 = 0;

/// Which of the two metadata slots a generation was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaSlot {
    A,
    B,
}

/// Outcome of a check.
///
/// `Failed` and `Incomplete` both mean the file is not usable; only `Ok` means
/// every check ran and no error was recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    /// Every check ran and no error was recorded.
    Ok,
    /// At least one definite format violation was recorded.
    Failed,
    /// A page could not be read, so the conclusions that depend on it were not
    /// computed; the diagnostics found before that point are still reported.
    Incomplete,
}

/// Whether a diagnostic fails the check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    /// The check did not pass: this diagnostic set the report's status.
    Error,
    /// A note about the file; it never changes the report status.
    Warning,
}

/// One finding about the checked file.
///
/// Codes and rule texts are fixed strings, and the payloads are boxed, so a
/// diagnostic stays small enough to travel in a `Result` without a heap hop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckDiagnostic {
    /// Whether this finding fails the check.
    pub severity: DiagnosticSeverity,
    /// Stable machine-readable code.
    pub code: &'static str,
    /// The rule that was violated, or the note being made.
    pub check: &'static str,
    /// The generation the finding belongs to.
    pub generation: Option<u64>,
    /// The physical page the finding is about, when it is page-local.
    pub pid: Option<PageId>,
    /// Where the page was reached from, e.g. `bucket/plain/node=7/child=0`.
    pub reference_path: Option<Box<str>>,
    /// What the checker required.
    pub expected: Option<Box<str>>,
    /// What the file contained.
    pub actual: Option<Box<str>>,
}

impl CheckDiagnostic {
    fn error(
        code: &'static str,
        check: &'static str,
        pid: Option<PageId>,
        path: Option<String>,
        expected: Option<String>,
        actual: Option<String>,
    ) -> Self {
        Self {
            severity: DiagnosticSeverity::Error,
            code,
            check,
            generation: None,
            pid,
            reference_path: path.map(String::into_boxed_str),
            expected: expected.map(String::into_boxed_str),
            actual: actual.map(String::into_boxed_str),
        }
    }

    fn warning(
        code: &'static str,
        check: &'static str,
        pid: Option<PageId>,
        path: Option<String>,
        expected: Option<String>,
        actual: Option<String>,
    ) -> Self {
        Self {
            severity: DiagnosticSeverity::Warning,
            code,
            check,
            generation: None,
            pid,
            reference_path: path.map(String::into_boxed_str),
            expected: expected.map(String::into_boxed_str),
            actual: actual.map(String::into_boxed_str),
        }
    }
}

/// Why a check could not produce a report at all.
#[derive(Debug)]
pub enum CheckError {
    /// The file could not be opened, stat'ed or read.
    Io(OpenIoError),
    /// The path is not a regular file: a directory, device, socket or pipe.
    NotRegularFile { path: PathBuf },
    /// Another process holds the exclusive lock on the file.
    LockBusy { path: PathBuf },
    /// Neither metadata slot holds a record this build can read.
    NoValidMeta,
    /// The only readable metadata record declares a format version this build
    /// does not implement.
    UnsupportedFormatVersion { found: u32 },
    /// The two metadata slots declare different format versions, so no decoder can
    /// be chosen for the file.
    MixedFormatVersions { found: u32 },
    /// The file was modified while it was checked.
    ChangedDuringCheck {
        path: PathBuf,
        initial_len: u64,
        final_len: u64,
    },
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::NotRegularFile { path } => {
                write!(f, "not a regular file: {}", path.display())
            }
            Self::LockBusy { path } => write!(f, "database is locked: {}", path.display()),
            Self::NoValidMeta => write!(f, "neither metadata slot is valid"),
            Self::UnsupportedFormatVersion { found } => {
                write!(f, "unsupported format version {found}")
            }
            Self::MixedFormatVersions { found } => {
                write!(f, "metadata slots disagree on format version {found}")
            }
            Self::ChangedDuringCheck {
                path,
                initial_len,
                final_len,
            } => write!(
                f,
                "file changed during check: {} ({initial_len} -> {final_len} bytes)",
                path.display()
            ),
        }
    }
}

impl std::error::Error for CheckError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

/// A check either yields a report or fails before one can be built.
pub type CheckResult<T> = std::result::Result<T, CheckError>;

/// What a check found in the selected generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckReport {
    /// `Ok`, `Failed` or `Incomplete`; see [`CheckStatus`].
    pub status: CheckStatus,
    /// The path that was checked.
    pub path: PathBuf,
    /// Physical length of the file in bytes.
    pub file_len: u64,
    /// The slot the generation was read from, when one was selected.
    pub selected_slot: Option<MetaSlot>,
    /// Sequence number of the selected generation.
    pub generation: Option<u64>,
    /// Exclusive end of the selected generation's data id space.
    pub next_page_id: Option<PageId>,
    /// Pages reachable from the selected generation's roots.
    pub reachable_pages: usize,
    /// Pages held by the reusable allocator extents.
    pub reusable_pages: usize,
    /// Pages held by the retired allocator extents.
    pub retired_pages: usize,
    /// Allocator extent-list pages of the selected generation.
    pub allocator_list_pages: usize,
    /// Bytes past the selected generation's data id space. They are not checked
    /// and take no part in this report's ownership conclusions.
    pub trailing_bytes: u64,
    /// Findings in discovery order.
    pub diagnostics: Vec<CheckDiagnostic>,
}

/// What the selected generation's allocator chains imply about free space.
///
/// The page counts stay in [`CheckReport`], and the byte fields are those
/// counts scaled by the page size; what this adds to the report is the shape of
/// the two chains - how many separate ranges they hold - and the promoted
/// total. `retired_bytes` is space a write cannot use yet: it is retired and
/// either still held by a reader or deferred to a later generation, so
/// `reusable_if_all_retired_promoted_bytes` is what the total would become once
/// every retired extent is promoted, never a promise about the next write.
/// Allocator-list pages are deliberately excluded: they are the bookkeeping
/// that holds the extents, not free space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckSpaceStats {
    /// Extent pages a writer may allocate right now.
    pub reusable_bytes: u64,
    /// Extent pages that are retired and not allocatable yet, because a reader
    /// may still hold them or because they were deferred to a later
    /// generation.
    pub retired_bytes: u64,
    /// Extent entries in the reusable chain, as persisted.
    pub reusable_extent_count: usize,
    /// Extent entries in the retired chain, as persisted.
    pub retired_extent_count: usize,
    /// `reusable_bytes + retired_bytes`.
    pub reusable_if_all_retired_promoted_bytes: u64,
}

/// What one bucket tree holds, as the scan saw it.
///
/// Every counter is what the walk actually visited. `reachable_pages` is a
/// physical page count and must never be inferred from the logical byte
/// counts, which say nothing about how many pages the values overflow onto.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketStats {
    /// The catalog key, already validated as non-empty UTF-8 by the walk.
    pub name: String,
    /// Whether the bucket's nodes are prefix-encoded.
    pub prefix_encoding: bool,
    /// Leaf entries in the tree.
    pub records: usize,
    /// Sum of the leaf key byte lengths.
    pub logical_key_bytes: u64,
    /// Sum of the leaf values' logical lengths.
    pub logical_value_bytes: u64,
    /// Levels from the root to a leaf; 0 for a bucket with no root page.
    pub tree_height: usize,
    /// Node, value and indirect pages this bucket's own walk touched.
    pub reachable_pages: usize,
}

/// A check plus the statistics that only make sense for a passing one.
///
/// `space` and `buckets` are `Some` only when the report is `Ok`. A `Failed` or
/// `Incomplete` report describes how far the walk got, so the statistics
/// gathered before it stopped would be a partial view of a file that was just
/// found to be damaged; they are withheld rather than published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckReportWithSpace {
    /// The report `check_path` would have returned.
    pub report: CheckReport,
    /// Space derived from the selected generation, when the check passed.
    pub space: Option<CheckSpaceStats>,
    /// Per-bucket statistics in catalog order, when the check passed and a scan
    /// was requested. `check_path_with_space` never asks for a scan, so it
    /// leaves this `None`.
    pub buckets: Option<Vec<BucketStats>>,
}

/// Which statistics an extended check should collect.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckOptions {
    /// Collect per-bucket statistics during the walk that already happens.
    pub scan: bool,
}

#[derive(Debug, Clone, Copy)]
struct Meta {
    seq: u64,
    catalog_root: PageId,
    next_page_id: PageId,
    reusable_root: PageId,
    retired_root: PageId,
}

#[derive(Debug, Clone, Copy)]
enum MetaCandidate {
    Valid(Meta),
    Invalid,
    Unsupported(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    len: u64,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

struct OfflineFile {
    file: File,
    path: PathBuf,
    identity: FileIdentity,
}

impl OfflineFile {
    fn open(path: &Path) -> CheckResult<Self> {
        // A directory or a device cannot be a database file: name it instead of
        // classifying whatever bytes a read happens to return. A dangling symlink
        // still falls through to `open`, which reports the real error.
        if let Ok(metadata) = std::fs::metadata(path)
            && !metadata.is_file()
        {
            return Err(CheckError::NotRegularFile {
                path: path.to_path_buf(),
            });
        }
        let file = FileOpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .truncate(false)
            .open(path)
            .map_err(|source| Self::io_error(path, "open", None, None, source))?;

        let deadline = Instant::now() + OPEN_LOCK_TIMEOUT;
        loop {
            match file.try_lock_shared() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(OPEN_LOCK_RETRY_INTERVAL);
                }
                Err(TryLockError::WouldBlock) => {
                    return Err(CheckError::LockBusy {
                        path: path.to_path_buf(),
                    });
                }
                Err(TryLockError::Error(source)) => {
                    return Err(Self::io_error(path, "try_lock_shared", None, None, source));
                }
            }
        }

        let identity = Self::identity(&file, path)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
            identity,
        })
    }

    fn identity(file: &File, path: &Path) -> CheckResult<FileIdentity> {
        let metadata = file
            .metadata()
            .map_err(|source| Self::io_error(path, "metadata", None, None, source))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(FileIdentity {
                len: metadata.len(),
                dev: metadata.dev(),
                ino: metadata.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            Ok(FileIdentity {
                len: file
                    .metadata()
                    .map_err(|source| Self::io_error(path, "metadata", None, None, source))?
                    .len(),
            })
        }
    }

    fn io_error(
        path: &Path,
        operation: &'static str,
        offset: Option<u64>,
        length: Option<u64>,
        source: io::Error,
    ) -> CheckError {
        CheckError::Io(OpenIoError {
            operation,
            path: path.to_path_buf(),
            offset,
            length,
            source,
        })
    }

    fn read_page(&self, pid: PageId) -> CheckResult<[u8; PAGE_SIZE]> {
        let offset = u64::from(pid) * PAGE_SIZE as u64;
        let mut page = [0u8; PAGE_SIZE];
        read_exact_at(&self.file, &mut page, offset).map_err(|source| {
            Self::io_error(
                &self.path,
                "pread",
                Some(offset),
                Some(PAGE_SIZE as u64),
                source,
            )
        })?;
        Ok(page)
    }

    fn read_meta(&self, offset: u64) -> CheckResult<Option<MetaCandidate>> {
        if offset + PAGE_SIZE as u64 > self.identity.len {
            return Ok(None);
        }
        let mut record = [0u8; META_RECORD_LEN as usize];
        match read_exact_at(&self.file, &mut record, offset) {
            Ok(()) => Ok(Some(decode_meta(&record))),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(source) => Err(Self::io_error(
                &self.path,
                "pread_meta",
                Some(offset),
                Some(META_RECORD_LEN),
                source,
            )),
        }
    }

    fn ensure_unchanged(&self) -> CheckResult<()> {
        let final_identity = Self::identity(&self.file, &self.path)?;
        if self.identity == final_identity {
            return Ok(());
        }
        Err(CheckError::ChangedDuringCheck {
            path: self.path.clone(),
            initial_len: self.identity.len,
            final_len: final_identity.len,
        })
    }
}

fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buffer, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut read = 0usize;
        while read < buffer.len() {
            let n = file.seek_read(&mut buffer[read..], offset + read as u64)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            read += n;
        }
        Ok(())
    }
}

fn decode_meta(record: &[u8; 40]) -> MetaCandidate {
    let magic = u64::from_le_bytes(record[0..8].try_into().expect("fixed meta field"));
    let seq = u64::from_le_bytes(record[8..16].try_into().expect("fixed meta field"));
    let format_version = u32::from_le_bytes(record[16..20].try_into().expect("fixed meta field"));
    let catalog_root = u32::from_le_bytes(record[20..24].try_into().expect("fixed meta field"));
    let next_page_id = u32::from_le_bytes(record[24..28].try_into().expect("fixed meta field"));
    let reusable_root = u32::from_le_bytes(record[28..32].try_into().expect("fixed meta field"));
    let retired_root = u32::from_le_bytes(record[32..36].try_into().expect("fixed meta field"));
    let stored_crc = u32::from_le_bytes(record[36..40].try_into().expect("fixed meta field"));
    let computed_crc = crc32c::crc32c(&record[..META_CHECKSUM_END]);
    if magic != MAGIC || stored_crc != computed_crc {
        return MetaCandidate::Invalid;
    }
    if format_version != FORMAT_VERSION {
        return MetaCandidate::Unsupported(format_version);
    }
    MetaCandidate::Valid(Meta {
        seq,
        catalog_root,
        next_page_id,
        reusable_root,
        retired_root,
    })
}

fn select_meta(
    a: Option<MetaCandidate>,
    b: Option<MetaCandidate>,
) -> CheckResult<(Meta, MetaSlot)> {
    let mut supported = Vec::new();
    let mut unsupported = Vec::new();
    for (slot, candidate) in [(MetaSlot::A, a), (MetaSlot::B, b)] {
        match candidate {
            Some(MetaCandidate::Valid(meta)) => supported.push((slot, meta)),
            Some(MetaCandidate::Unsupported(found)) => unsupported.push(found),
            Some(MetaCandidate::Invalid) | None => {}
        }
    }
    if !unsupported.is_empty() {
        let mut versions = unsupported.clone();
        versions.sort_unstable();
        versions.dedup();
        if versions.len() > 1 || !supported.is_empty() {
            return Err(CheckError::MixedFormatVersions { found: versions[0] });
        }
        return Err(CheckError::UnsupportedFormatVersion { found: versions[0] });
    }
    let Some((slot, chosen)) = supported.into_iter().reduce(|(slot_a, a), (slot_b, b)| {
        if b.seq > a.seq {
            (slot_b, b)
        } else {
            (slot_a, a)
        }
    }) else {
        return Err(CheckError::NoValidMeta);
    };
    Ok((chosen, slot))
}

#[derive(Debug, Clone, Copy)]
enum ExpectedPage {
    PlainNode,
    PrefixNode,
    Value,
    Indirect,
    ExtentList,
}

impl ExpectedPage {
    fn checksum_offset(self) -> usize {
        match self {
            Self::PlainNode | Self::PrefixNode | Self::ExtentList => page::HEADER_CRC_OFFSET,
            Self::Value | Self::Indirect => page::TRAILER_CRC_OFFSET,
        }
    }
}

#[derive(Debug, Clone)]
struct DecodedSlot {
    value_len: usize,
    page_ids: [PageId; NR_INLINE_PAGE],
    inline_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct DecodedEntry {
    key: Vec<u8>,
    slot: DecodedSlot,
}

#[derive(Debug, Clone)]
struct DecodedNode {
    leaf: bool,
    entries: Vec<DecodedEntry>,
}

#[derive(Clone)]
struct Bound {
    key: Vec<u8>,
    inclusive: bool,
}

struct Work {
    pid: PageId,
    depth: usize,
    lower: Option<Bound>,
    upper: Option<Bound>,
    path: String,
}

/// What one tree walk saw. Returned by the walk itself so a caller can report
/// a bucket without reaching back into the global ownership totals, which mix
/// catalog and allocator pages into the same counter.
#[derive(Debug, Clone, Copy, Default)]
struct TreeStats {
    records: usize,
    logical_key_bytes: u64,
    logical_value_bytes: u64,
    tree_height: usize,
    reachable_pages: usize,
}

/// A finished walk: the report plus the statistics derived from it.
struct Collected {
    report: CheckReport,
    space: CheckSpaceStats,
    buckets: Vec<BucketStats>,
}

/// Called for every catalog entry; a bucket tree is walked without one.
type EntryVisitor<'a> = &'a mut dyn FnMut(&DecodedEntry, &str) -> Result<(), CheckDiagnostic>;

struct Checker {
    source: OfflineFile,
    meta: Meta,
    report: CheckReport,
    owners: Vec<u8>,
    reachable_pages: usize,
    reusable_pages: usize,
    retired_pages: usize,
    allocator_list_pages: usize,
    reusable_extent_count: usize,
    retired_extent_count: usize,
    /// Whether a caller asked to keep the per-bucket statistics. The walk
    /// counts them either way - it is counting what it already reads - and this
    /// decides whether they are retained and reported at all.
    scan: bool,
    buckets: Vec<BucketStats>,
}

struct CatalogBucket {
    name: String,
    root: PageId,
    layout: ExpectedPage,
    prefix_encoding: bool,
}

impl Checker {
    fn new(source: OfflineFile, meta: Meta, selected_slot: MetaSlot, scan: bool) -> Self {
        let file_len = source.identity.len;
        let trailing_bytes =
            file_len.saturating_sub(u64::from(meta.next_page_id) * PAGE_SIZE as u64);
        let owners = vec![0u8; meta.next_page_id as usize];
        Self {
            source,
            meta,
            report: CheckReport {
                status: CheckStatus::Ok,
                path: PathBuf::new(),
                file_len,
                selected_slot: Some(selected_slot),
                generation: None,
                next_page_id: None,
                reachable_pages: 0,
                reusable_pages: 0,
                retired_pages: 0,
                allocator_list_pages: 0,
                trailing_bytes,
                diagnostics: Vec::new(),
            },
            owners,
            reachable_pages: 0,
            reusable_pages: 0,
            retired_pages: 0,
            allocator_list_pages: 0,
            reusable_extent_count: 0,
            retired_extent_count: 0,
            scan,
            buckets: Vec::new(),
        }
    }

    fn finish(self) -> CheckResult<Collected> {
        self.source.ensure_unchanged()?;
        let page = PAGE_SIZE as u64;
        let reusable_bytes = self.report.reusable_pages as u64 * page;
        let retired_bytes = self.report.retired_pages as u64 * page;
        Ok(Collected {
            report: self.report,
            space: CheckSpaceStats {
                reusable_bytes,
                retired_bytes,
                reusable_extent_count: self.reusable_extent_count,
                retired_extent_count: self.retired_extent_count,
                reusable_if_all_retired_promoted_bytes: reusable_bytes + retired_bytes,
            },
            buckets: self.buckets,
        })
    }

    /// Walks the selected generation.
    ///
    /// The caller has already established that the file covers every page of
    /// `[2, next_page_id)`, so a page that cannot be read here is a read failure
    /// (`IO_READ`), not a format violation.
    fn run(mut self, path: PathBuf) -> CheckResult<Collected> {
        self.report.path = path;
        self.report.generation = Some(self.meta.seq);
        self.report.next_page_id = Some(self.meta.next_page_id);

        if self.meta.next_page_id < 2 {
            self.add_error(CheckDiagnostic::error(
                "INVALID_NEXT_PAGE_ID",
                "next_page_id must reserve both metadata slots",
                None,
                Some("meta".to_string()),
                Some(">= 2".to_string()),
                Some(self.meta.next_page_id.to_string()),
            ));
            return self.finish();
        }

        for (root, name) in [
            (self.meta.catalog_root, "catalog_root"),
            (self.meta.reusable_root, "reusable_root"),
            (self.meta.retired_root, "retired_root"),
        ] {
            if root != 0 && !(2..self.meta.next_page_id).contains(&root) {
                self.add_error(CheckDiagnostic::error(
                    "META_ROOT_OUT_OF_RANGE",
                    "metadata roots must stay within the data id space",
                    Some(root),
                    Some(name.to_string()),
                    Some(format!("[2, {})", self.meta.next_page_id)),
                    Some(root.to_string()),
                ));
            }
        }
        if self.report.status != CheckStatus::Ok {
            return self.finish();
        }

        let buckets = self.check_catalog();
        if self.report.status != CheckStatus::Incomplete {
            self.check_allocator();
        }
        if let Some(buckets) = buckets {
            for bucket in buckets {
                let path = format!("bucket/{}", bucket.name);
                // A bucket that failed its walk reports no statistics: its
                // counters would describe a tree the check has just rejected.
                match self.check_tree(bucket.root, bucket.layout, &path, None) {
                    Ok(tree) if self.scan => self.buckets.push(BucketStats {
                        name: bucket.name,
                        prefix_encoding: bucket.prefix_encoding,
                        records: tree.records,
                        logical_key_bytes: tree.logical_key_bytes,
                        logical_value_bytes: tree.logical_value_bytes,
                        tree_height: tree.tree_height,
                        reachable_pages: tree.reachable_pages,
                    }),
                    Ok(_) => {}
                    Err(error) => self.add_error(error),
                }
            }
        }
        if self.report.status == CheckStatus::Ok {
            self.check_complete_ownership();
        }
        self.report.reachable_pages = self.reachable_pages;
        self.report.reusable_pages = self.reusable_pages;
        self.report.retired_pages = self.retired_pages;
        self.report.allocator_list_pages = self.allocator_list_pages;
        self.finish()
    }

    /// Records an error diagnostic.
    ///
    /// A page-read failure (`IO_*`) means the check could not be carried out and
    /// leaves it incomplete; every other code is a definite format violation and
    /// fails it. `Failed` outranks `Incomplete` whichever arrived first.
    fn add_error(&mut self, mut diagnostic: CheckDiagnostic) {
        let incomplete = diagnostic.code.starts_with("IO_");
        diagnostic.generation = Some(self.meta.seq);
        self.report.diagnostics.push(diagnostic);
        if !incomplete {
            self.report.status = CheckStatus::Failed;
        } else if self.report.status == CheckStatus::Ok {
            self.report.status = CheckStatus::Incomplete;
        }
    }

    fn add_warning(&mut self, mut diagnostic: CheckDiagnostic) {
        diagnostic.generation = Some(self.meta.seq);
        self.report.diagnostics.push(diagnostic);
    }

    /// Records one ownership class for `pid`.
    ///
    /// The data id space starts at 2: pids 0 and 1 are the metadata slots, and no
    /// node, value, indirect or allocator reference may name them.
    fn add_owner(
        &mut self,
        pid: PageId,
        bit: u8,
        code: &'static str,
        path: &str,
    ) -> Result<(), CheckDiagnostic> {
        if pid < 2 || pid >= self.meta.next_page_id {
            return Err(CheckDiagnostic::error(
                "PID_OUT_OF_RANGE",
                "page reference must be inside the data id space",
                Some(pid),
                Some(path.to_string()),
                Some(format!("[2, {})", self.meta.next_page_id)),
                Some(pid.to_string()),
            ));
        }
        let owner = &mut self.owners[pid as usize];
        if *owner & bit != 0 {
            return Err(CheckDiagnostic::error(
                code,
                "a physical page may not have the same ownership class twice",
                Some(pid),
                Some(path.to_string()),
                Some("one owner".to_string()),
                Some("duplicate reference".to_string()),
            ));
        }
        if *owner != 0 {
            return Err(CheckDiagnostic::error(
                ownership_overlap_code(*owner, bit),
                "a physical page must have exactly one ownership class",
                Some(pid),
                Some(path.to_string()),
                Some("one class".to_string()),
                Some(format!("existing class bits {owner:#x}, new bit {bit:#x}")),
            ));
        }
        *owner |= bit;
        if bit & (REACHABLE_NODE | REACHABLE_VALUE | REACHABLE_INDIRECT) != 0 {
            self.reachable_pages += 1;
        }
        Ok(())
    }

    fn check_catalog(&mut self) -> Option<Vec<CatalogBucket>> {
        let mut buckets = Vec::new();
        let next_page_id = self.meta.next_page_id;
        let result = self.check_tree(
            self.meta.catalog_root,
            ExpectedPage::PlainNode,
            "catalog",
            Some(&mut |entry, path| {
                if entry.key.is_empty() {
                    return Err(CheckDiagnostic::error(
                        "CATALOG_EMPTY_NAME",
                        "catalog keys must be non-empty bucket names",
                        None,
                        Some(path.to_string()),
                        Some("non-empty UTF-8 bucket name".to_string()),
                        Some(String::new()),
                    ));
                }
                let name = match std::str::from_utf8(&entry.key) {
                    Ok(name) if !name.is_empty() && name.len() <= MAX_KEY_LEN => name.to_string(),
                    _ => {
                        return Err(CheckDiagnostic::error(
                            "INVALID_BUCKET_NAME",
                            "catalog bucket names must be valid UTF-8 within the key limit",
                            None,
                            Some(path.to_string()),
                            Some("valid UTF-8 bucket name".to_string()),
                            Some(String::from_utf8_lossy(&entry.key).into_owned()),
                        ));
                    }
                };
                let Some(value) = entry.slot.inline_value.as_ref() else {
                    return Err(CheckDiagnostic::error(
                        "CATUCKET_METADATA_NOT_INLINE",
                        "bucket metadata must be an inline catalog value",
                        None,
                        Some(path.to_string()),
                        Some("inline 8-byte metadata".to_string()),
                        Some("overflow value".to_string()),
                    ));
                };
                if value.len() != 8 {
                    return Err(CheckDiagnostic::error(
                        "BUCKET_METADATA_LENGTH",
                        "bucket metadata must be exactly 8 bytes",
                        None,
                        Some(path.to_string()),
                        Some("8".to_string()),
                        Some(value.len().to_string()),
                    ));
                }
                let root =
                    u32::from_le_bytes(value[0..4].try_into().expect("fixed metadata field"));
                let flags =
                    u32::from_le_bytes(value[4..8].try_into().expect("fixed metadata field"));
                if flags & !1 != 0 {
                    return Err(CheckDiagnostic::error(
                        "BUCKET_FLAGS_UNKNOWN",
                        "only the prefix-encoding policy bit is defined",
                        None,
                        Some(path.to_string()),
                        Some("flags & !1 == 0".to_string()),
                        Some(format!("{flags:#x}")),
                    ));
                }
                if root != 0 && !(2..next_page_id).contains(&root) {
                    return Err(CheckDiagnostic::error(
                        "BUCKET_ROOT_OUT_OF_RANGE",
                        "bucket roots must stay within the data id space",
                        Some(root),
                        Some(path.to_string()),
                        Some(format!("[2, {next_page_id})")),
                        Some(root.to_string()),
                    ));
                }
                buckets.push(CatalogBucket {
                    name,
                    root,
                    layout: if flags & 1 == 0 {
                        ExpectedPage::PlainNode
                    } else {
                        ExpectedPage::PrefixNode
                    },
                    prefix_encoding: flags & 1 == 1,
                });
                Ok(())
            }),
        );
        if let Err(error) = result {
            self.add_error(error);
            None
        } else {
            Some(buckets)
        }
    }

    /// Walks one tree and reports what it saw.
    ///
    /// `reachable_pages` counts only the node, value and indirect pages this
    /// walk touched, so a caller can attribute physical pages to one bucket
    /// without subtracting the catalog and allocator pages that the global
    /// counter also holds.
    fn check_tree(
        &mut self,
        root: PageId,
        layout: ExpectedPage,
        tree_path: &str,
        mut visit: Option<EntryVisitor<'_>>,
    ) -> Result<TreeStats, CheckDiagnostic> {
        if root == 0 {
            return Ok(TreeStats::default());
        }
        let mut tree = TreeStats::default();

        let mut stack = vec![Work {
            pid: root,
            depth: 0,
            lower: None,
            upper: None,
            path: tree_path.to_string(),
        }];
        let mut leaf_depth: Option<usize> = None;
        let mut previous_leaf_key: Option<Vec<u8>> = None;
        while let Some(work) = stack.pop() {
            if !(2..self.meta.next_page_id).contains(&work.pid) {
                return Err(CheckDiagnostic::error(
                    "PID_OUT_OF_RANGE",
                    "node page ID must stay within [2, next_page_id)",
                    Some(work.pid),
                    Some(work.path),
                    Some(format!("[2, {})", self.meta.next_page_id)),
                    Some(work.pid.to_string()),
                ));
            }
            self.add_owner(
                work.pid,
                REACHABLE_NODE,
                "DUPLICATE_NODE_REFERENCE",
                &work.path,
            )?;
            tree.reachable_pages += 1;
            let node = self.read_node(work.pid, layout, &work.path)?;
            if node.leaf {
                if let Some(expected) = leaf_depth {
                    if expected != work.depth {
                        return Err(CheckDiagnostic::error(
                            "LEAF_DEPTH_MISMATCH",
                            "all leaves in one tree must have the same depth",
                            Some(work.pid),
                            Some(work.path),
                            Some(expected.to_string()),
                            Some(work.depth.to_string()),
                        ));
                    }
                } else {
                    leaf_depth = Some(work.depth);
                }
                for entry in &node.entries {
                    if !entry.key.is_empty() && entry.key.len() > MAX_KEY_LEN {
                        return Err(CheckDiagnostic::error(
                            "KEY_TOO_LARGE",
                            "leaf keys must not exceed MAX_KEY_LEN",
                            Some(work.pid),
                            Some(work.path.clone()),
                            Some(format!("<= {MAX_KEY_LEN}")),
                            Some(entry.key.len().to_string()),
                        ));
                    }
                    if !bound_matches(&entry.key, work.lower.as_ref(), true)
                        || !bound_matches(&entry.key, work.upper.as_ref(), false)
                    {
                        return Err(CheckDiagnostic::error(
                            "KEY_OUTSIDE_BRANCH_RANGE",
                            "leaf key falls outside its branch routing range",
                            Some(work.pid),
                            Some(work.path.clone()),
                            Some("branch range".to_string()),
                            Some(String::from_utf8_lossy(&entry.key).into_owned()),
                        ));
                    }
                    if let Some(previous) = &previous_leaf_key
                        && previous >= &entry.key
                    {
                        return Err(CheckDiagnostic::error(
                            "KEYS_NOT_STRICTLY_INCREASING",
                            "leaf keys must be strictly increasing in DFS order",
                            Some(work.pid),
                            Some(work.path.clone()),
                            Some("strictly increasing".to_string()),
                            Some(String::from_utf8_lossy(&entry.key).into_owned()),
                        ));
                    }
                    previous_leaf_key = Some(entry.key.clone());
                    if let Some(visitor) = visit.as_deref_mut() {
                        visitor(entry, &work.path)?;
                    }
                    tree.records += 1;
                    tree.logical_key_bytes += entry.key.len() as u64;
                    tree.logical_value_bytes += entry.slot.value_len as u64;
                    self.check_value(entry, &work.path, &mut tree)?;
                }
            } else {
                if node.entries.is_empty() {
                    return Err(CheckDiagnostic::error(
                        "EMPTY_BRANCH",
                        "a published branch node must have at least one child",
                        Some(work.pid),
                        Some(work.path),
                        Some(">= 1 child".to_string()),
                        Some("0 children".to_string()),
                    ));
                }
                let mut previous_separator: Option<Vec<u8>> = None;
                for (index, entry) in node.entries.iter().enumerate() {
                    if index == 0 {
                        if !entry.key.is_empty() {
                            return Err(CheckDiagnostic::error(
                                "BRANCH_SENTINEL_INVALID",
                                "the first branch slot must carry an empty separator",
                                Some(work.pid),
                                Some(work.path.clone()),
                                Some("empty separator".to_string()),
                                Some(String::from_utf8_lossy(&entry.key).into_owned()),
                            ));
                        }
                    } else {
                        if entry.key.is_empty() || entry.key.len() > MAX_KEY_LEN {
                            return Err(CheckDiagnostic::error(
                                "BRANCH_SEPARATOR_INVALID",
                                "non-sentinel branch separators must be non-empty and bounded",
                                Some(work.pid),
                                Some(work.path.clone()),
                                Some(format!("1..={MAX_KEY_LEN}")),
                                Some(entry.key.len().to_string()),
                            ));
                        }
                        if previous_separator
                            .as_ref()
                            .is_some_and(|previous| previous >= &entry.key)
                        {
                            return Err(CheckDiagnostic::error(
                                "SEPARATORS_NOT_STRICTLY_INCREASING",
                                "branch separators must be strictly increasing",
                                Some(work.pid),
                                Some(work.path.clone()),
                                Some("strictly increasing".to_string()),
                                Some(String::from_utf8_lossy(&entry.key).into_owned()),
                            ));
                        }
                        previous_separator = Some(entry.key.clone());
                    }
                    if entry.slot.value_len != 0
                        || entry.slot.page_ids[1..].iter().any(|pid| *pid != 0)
                    {
                        return Err(CheckDiagnostic::error(
                            "BRANCH_SLOT_NONCANONICAL",
                            "branch slots may only use their child PID",
                            Some(work.pid),
                            Some(work.path.clone()),
                            Some("vlen=0 and one child PID".to_string()),
                            Some(format!(
                                "vlen={}, pids={:?}",
                                entry.slot.value_len, entry.slot.page_ids
                            )),
                        ));
                    }
                }
                for index in (0..node.entries.len()).rev() {
                    let entry = &node.entries[index];
                    let lower = if index == 0 {
                        work.lower.clone()
                    } else {
                        Some(Bound {
                            key: entry.key.clone(),
                            inclusive: true,
                        })
                    };
                    let upper = if index + 1 == node.entries.len() {
                        work.upper.clone()
                    } else {
                        Some(Bound {
                            key: node.entries[index + 1].key.clone(),
                            inclusive: false,
                        })
                    };
                    let child = entry.slot.page_ids[0];
                    stack.push(Work {
                        pid: child,
                        depth: work.depth + 1,
                        lower,
                        upper,
                        path: format!("{}/node={}/child={}", work.path, work.pid, index),
                    });
                }
            }
        }
        // A root that is not 0 always reaches a leaf, and the walk above has
        // already forced every leaf to one depth.
        tree.tree_height = leaf_depth.map_or(0, |depth| depth + 1);
        Ok(tree)
    }

    fn read_node(
        &self,
        pid: PageId,
        layout: ExpectedPage,
        path: &str,
    ) -> Result<DecodedNode, CheckDiagnostic> {
        let page = self.read_typed_page(pid, layout, path)?;
        parse_node(&page, layout, pid, path)
    }

    fn read_typed_page(
        &self,
        pid: PageId,
        expected: ExpectedPage,
        path: &str,
    ) -> Result<[u8; PAGE_SIZE], CheckDiagnostic> {
        // Every page of `[2, next_page_id)` is physically present (the caller
        // checked), so a failure here is a read or device failure rather than a
        // format violation: `IO_READ` keeps the two apart and leaves the check
        // incomplete.
        let page = self.source.read_page(pid).map_err(|error| {
            CheckDiagnostic::error(
                "IO_READ",
                "offline page read failed",
                Some(pid),
                Some(path.to_string()),
                Some("readable physical page".to_string()),
                Some(error.to_string()),
            )
        })?;
        if let Err(mismatch) = page::verify_page(&page, expected.checksum_offset(), pid) {
            return Err(CheckDiagnostic::error(
                "PAGE_CRC_MISMATCH",
                "ordinary physical page checksum must match its expected PID",
                Some(pid),
                Some(path.to_string()),
                Some(format!("{:#x}", mismatch.expected)),
                Some(format!("{:#x}", mismatch.actual)),
            ));
        }
        Ok(page)
    }

    fn check_value(
        &mut self,
        entry: &DecodedEntry,
        path: &str,
        tree: &mut TreeStats,
    ) -> Result<(), CheckDiagnostic> {
        let slot = &entry.slot;
        if slot.value_len == 0 {
            if slot.page_ids.iter().any(|pid| *pid != 0) {
                return Err(CheckDiagnostic::error(
                    "ZERO_VALUE_HAS_PAGE_ID",
                    "zero-length values may not reference physical pages",
                    None,
                    Some(path.to_string()),
                    Some("all page IDs zero".to_string()),
                    Some(format!("{:?}", slot.page_ids)),
                ));
            }
            return Ok(());
        }
        if slot.page_ids.iter().all(|pid| *pid == 0) {
            return Ok(());
        }
        let value_len = slot.value_len;
        let needed = value_len.div_ceil(TRAILER_CONTENT_SIZE);
        if needed <= NR_INLINE_PAGE {
            for (index, pid) in slot.page_ids.iter().take(needed).enumerate() {
                if *pid == 0 {
                    return Err(CheckDiagnostic::error(
                        "VALUE_PID_ZERO",
                        "direct overflow slots must name every required value page",
                        None,
                        Some(path.to_string()),
                        Some("non-zero PID".to_string()),
                        Some("0".to_string()),
                    ));
                }
                self.add_owner(*pid, REACHABLE_VALUE, "DUPLICATE_VALUE_REFERENCE", path)?;
                tree.reachable_pages += 1;
                let page = self.read_typed_page(*pid, ExpectedPage::Value, path)?;
                if index + 1 == needed {
                    let take = value_len - index * TRAILER_CONTENT_SIZE;
                    if page[take..TRAILER_CONTENT_SIZE]
                        .iter()
                        .any(|byte| *byte != 0)
                    {
                        return Err(CheckDiagnostic::error(
                            "VALUE_PADDING_NOT_ZERO",
                            "unused bytes in the final value page must be zero",
                            Some(*pid),
                            Some(path.to_string()),
                            Some("zero padding".to_string()),
                            Some(format!(
                                "value_len={value_len}, needed={needed}, first_nonzero={:?}",
                                page[take..TRAILER_CONTENT_SIZE]
                                    .iter()
                                    .position(|byte| *byte != 0)
                                    .map(|index| take + index)
                            )),
                        ));
                    }
                }
            }
            return Ok(());
        }

        if slot.page_ids[1..].iter().any(|pid| *pid != 0) {
            return Err(CheckDiagnostic::error(
                "INDIRECT_SLOT_NONCANONICAL",
                "indirect overflow slots may only use the first PID",
                None,
                Some(path.to_string()),
                Some("only page_ids[0]".to_string()),
                Some(format!("{:?}", slot.page_ids)),
            ));
        }
        let mut current = slot.page_ids[0];
        let mut seen = HashSet::new();
        let mut data_pages = 0usize;
        let index_pages = needed.div_ceil(INDIRECT_IDS_PER_PAGE);
        for index in 0..index_pages {
            if !seen.insert(current) {
                return Err(CheckDiagnostic::error(
                    "INDIRECT_CYCLE",
                    "indirect page chains must be acyclic",
                    Some(current),
                    Some(path.to_string()),
                    Some("acyclic".to_string()),
                    Some("repeated page".to_string()),
                ));
            }
            self.add_owner(
                current,
                REACHABLE_INDIRECT,
                "DUPLICATE_INDIRECT_REFERENCE",
                path,
            )?;
            tree.reachable_pages += 1;
            let page = self.read_typed_page(current, ExpectedPage::Indirect, path)?;
            let next = u32::from_le_bytes(
                page[INDIRECT_NEXT_OFFSET..INDIRECT_NEXT_OFFSET + 4]
                    .try_into()
                    .expect("fixed indirect field"),
            );
            let remaining = needed - data_pages;
            let used = remaining.min(INDIRECT_IDS_PER_PAGE);
            for entry_index in 0..INDIRECT_IDS_PER_PAGE {
                let start = entry_index * 4;
                let pid = u32::from_le_bytes(
                    page[start..start + 4]
                        .try_into()
                        .expect("fixed indirect entry"),
                );
                if entry_index < used {
                    if pid == 0 {
                        return Err(CheckDiagnostic::error(
                            "INDIRECT_VALUE_PID_ZERO",
                            "required indirect entries must name value pages",
                            Some(current),
                            Some(path.to_string()),
                            Some("non-zero PID".to_string()),
                            Some("0".to_string()),
                        ));
                    }
                    self.add_owner(pid, REACHABLE_VALUE, "DUPLICATE_VALUE_REFERENCE", path)?;
                    tree.reachable_pages += 1;
                    let value_page = self.read_typed_page(pid, ExpectedPage::Value, path)?;
                    let current_page = data_pages;
                    let take = if current_page + 1 == needed {
                        value_len - current_page * TRAILER_CONTENT_SIZE
                    } else {
                        TRAILER_CONTENT_SIZE
                    };
                    if take < TRAILER_CONTENT_SIZE
                        && value_page[take..TRAILER_CONTENT_SIZE]
                            .iter()
                            .any(|byte| *byte != 0)
                    {
                        return Err(CheckDiagnostic::error(
                            "VALUE_PADDING_NOT_ZERO",
                            "unused bytes in the final value page must be zero",
                            Some(pid),
                            Some(path.to_string()),
                            Some("zero padding".to_string()),
                            Some(format!(
                                "value_len={value_len}, needed={needed}, current_page={current_page}, first_nonzero={:?}",
                                value_page[take..TRAILER_CONTENT_SIZE]
                                    .iter()
                                    .position(|byte| *byte != 0)
                                    .map(|index| take + index)
                            )),
                        ));
                    }
                    data_pages += 1;
                } else if pid != 0 {
                    return Err(CheckDiagnostic::error(
                        "INDIRECT_UNUSED_ENTRY_NONZERO",
                        "unused indirect entries must be zero",
                        Some(current),
                        Some(path.to_string()),
                        Some("0".to_string()),
                        Some(pid.to_string()),
                    ));
                }
            }
            if index + 1 == index_pages {
                if next != 0 {
                    return Err(CheckDiagnostic::error(
                        "INDIRECT_CHAIN_NOT_TERMINATED",
                        "the final indirect page must terminate with next=0",
                        Some(current),
                        Some(path.to_string()),
                        Some("0".to_string()),
                        Some(next.to_string()),
                    ));
                }
            } else if next == 0 {
                return Err(CheckDiagnostic::error(
                    "INDIRECT_CHAIN_TRUNCATED",
                    "a partial indirect chain must name its successor",
                    Some(current),
                    Some(path.to_string()),
                    Some("non-zero next".to_string()),
                    Some("0".to_string()),
                ));
            }
            current = next;
        }
        if data_pages != needed {
            return Err(CheckDiagnostic::error(
                "INDIRECT_VALUE_COUNT",
                "indirect chain must provide exactly the required value pages",
                Some(current),
                Some(path.to_string()),
                Some(needed.to_string()),
                Some(data_pages.to_string()),
            ));
        }
        Ok(())
    }

    fn check_allocator(&mut self) {
        if let Err(error) =
            self.check_extent_chain(self.meta.reusable_root, REUSABLE, REUSABLE_LIST)
        {
            self.add_error(error);
        }
        if let Err(error) = self.check_extent_chain(self.meta.retired_root, RETIRED, RETIRED_LIST) {
            self.add_error(error);
        }
    }

    fn check_extent_chain(
        &mut self,
        root: PageId,
        extent_bit: u8,
        list_bit: u8,
    ) -> Result<(), CheckDiagnostic> {
        if root == 0 {
            return Ok(());
        }
        let mut current = root;
        let mut seen = HashSet::new();
        let mut previous_end: Option<u64> = None;
        while current != 0 {
            if !seen.insert(current) {
                return Err(CheckDiagnostic::error(
                    "EXTENT_CYCLE",
                    "extent list chains must be acyclic",
                    Some(current),
                    Some("allocator".to_string()),
                    Some("acyclic".to_string()),
                    Some("repeated page".to_string()),
                ));
            }
            self.add_owner(
                current,
                list_bit,
                "DUPLICATE_ALLOCATOR_LIST_REFERENCE",
                "allocator",
            )?;
            self.allocator_list_pages += 1;
            let page = self.read_typed_page(current, ExpectedPage::ExtentList, "allocator")?;
            let next = u32::from_le_bytes(page[4..8].try_into().expect("fixed extent header"));
            let count =
                u32::from_le_bytes(page[8..12].try_into().expect("fixed extent header")) as usize;
            if count > EXTENT_PER_PAGE {
                return Err(CheckDiagnostic::error(
                    "INVALID_EXTENT_COUNT",
                    "extent count must fit the page capacity",
                    Some(current),
                    Some("allocator".to_string()),
                    Some(EXTENT_PER_PAGE.to_string()),
                    Some(count.to_string()),
                ));
            }
            for index in 0..count {
                let offset = EXTENT_HEADER_SIZE + index * EXTENT_SIZE;
                let start = u32::from_le_bytes(
                    page[offset..offset + 4]
                        .try_into()
                        .expect("fixed extent entry"),
                );
                let length = u32::from_le_bytes(
                    page[offset + 4..offset + 8]
                        .try_into()
                        .expect("fixed extent entry"),
                );
                if start < 2 || length == 0 {
                    return Err(CheckDiagnostic::error(
                        "INVALID_EXTENT_ENTRY",
                        "extent entries must have non-zero ranges inside the data id space",
                        Some(current),
                        Some("allocator".to_string()),
                        Some("start >= 2 and length > 0".to_string()),
                        Some(format!("start={start}, length={length}")),
                    ));
                }
                let end = u64::from(start) + u64::from(length);
                if end > u64::from(self.meta.next_page_id) {
                    return Err(CheckDiagnostic::error(
                        "EXTENT_OUT_OF_RANGE",
                        "extent ranges must stay inside [2, next_page_id)",
                        Some(start),
                        Some("allocator".to_string()),
                        Some(format!("< {}", self.meta.next_page_id)),
                        Some(end.to_string()),
                    ));
                }
                if let Some(previous_end) = previous_end {
                    if u64::from(start) < previous_end {
                        return Err(CheckDiagnostic::error(
                            "EXTENT_UNSORTED_OR_OVERLAP",
                            "extent ranges must be sorted and disjoint",
                            Some(start),
                            Some("allocator".to_string()),
                            Some(format!("start >= {previous_end}")),
                            Some(start.to_string()),
                        ));
                    }
                    if u64::from(start) == previous_end {
                        // The runtime opens such a file and merges the pair in
                        // memory (see `read_extent_pages`), so this is a canonical-form
                        // note, not a format violation: it must not fail the check.
                        self.add_warning(CheckDiagnostic::warning(
                            "EXTENT_NOT_MERGED",
                            "adjacent extents are merged by the runtime on the next open",
                            Some(start),
                            Some("allocator".to_string()),
                            Some(format!("one extent ending at {previous_end}")),
                            Some("two adjacent extents".to_string()),
                        ));
                    }
                }
                previous_end = Some(end);
                for pid in start..start as PageId + length {
                    self.add_owner(pid, extent_bit, "DUPLICATE_EXTENT_REFERENCE", "allocator")?;
                    if extent_bit == REUSABLE {
                        self.reusable_pages += 1;
                    } else {
                        self.retired_pages += 1;
                    }
                }
                // Counted after the pages are claimed: the `?` above returns
                // out of the whole chain, so a rejected extent is never
                // counted, and neither is anything after it. A report that got
                // that far publishes no counters at all.
                if extent_bit == REUSABLE {
                    self.reusable_extent_count += 1;
                } else {
                    self.retired_extent_count += 1;
                }
            }
            current = next;
        }
        Ok(())
    }

    /// Reports every data page no ownership class claimed.
    ///
    /// One diagnostic per unowned page would let the file choose how many lines
    /// it costs: `next_page_id` is a claim the file need not back with data, so a
    /// sparse file of a few kilobytes could name a billion pages. The count is
    /// the finding, so it is reported once, with the first offending id as the
    /// page and the total as the observed value.
    fn check_complete_ownership(&mut self) {
        let mut unaccounted = 0u64;
        let mut first = None;
        for pid in 2..self.meta.next_page_id {
            if self.owners.get(pid as usize).copied().unwrap_or(0) == 0 {
                first.get_or_insert(pid);
                unaccounted += 1;
            }
        }
        if unaccounted > 0 {
            self.add_error(CheckDiagnostic::error(
                "OWNERSHIP_UNACCOUNTED",
                "every physical data page must have exactly one ownership class",
                first,
                Some("data-id-space".to_string()),
                Some("one owner".to_string()),
                Some(format!("{unaccounted} page(s) with no owner")),
            ));
        }
    }
}

fn ownership_overlap_code(existing: u8, new: u8) -> &'static str {
    let reachable = REACHABLE_NODE | REACHABLE_VALUE | REACHABLE_INDIRECT;
    if (new == REUSABLE && existing & reachable != 0)
        || (existing == REUSABLE && new & reachable != 0)
    {
        "REACHABLE_REUSED"
    } else if (new == RETIRED && existing & reachable != 0)
        || (existing == RETIRED && new & reachable != 0)
    {
        "REACHABLE_RETIRED"
    } else if ((new == REUSABLE_LIST || new == RETIRED_LIST) && existing & reachable != 0)
        || ((existing == REUSABLE_LIST || existing == RETIRED_LIST) && new & reachable != 0)
    {
        "REACHABLE_ALLOCATOR_LIST"
    } else if (new == REUSABLE && existing & RETIRED != 0)
        || (new == RETIRED && existing & REUSABLE != 0)
    {
        "REUSABLE_RETIRED_OVERLAP"
    } else if ((new == REUSABLE || new == RETIRED)
        && (existing & (REUSABLE_LIST | RETIRED_LIST) != 0))
        || ((new == REUSABLE_LIST || new == RETIRED_LIST) && (existing & (REUSABLE | RETIRED) != 0))
    {
        "ALLOCATOR_LIST_IN_EXTENT"
    } else {
        "OWNERSHIP_OVERLAP"
    }
}

fn bound_matches(key: &[u8], bound: Option<&Bound>, lower: bool) -> bool {
    let Some(bound) = bound else { return true };
    let ordering = key.cmp(&bound.key);
    if lower {
        if bound.inclusive {
            ordering.is_ge()
        } else {
            ordering.is_gt()
        }
    } else if bound.inclusive {
        ordering.is_le()
    } else {
        ordering.is_lt()
    }
}

fn parse_node(
    page: &[u8; PAGE_SIZE],
    layout: ExpectedPage,
    pid: PageId,
    path: &str,
) -> Result<DecodedNode, CheckDiagnostic> {
    let (leaf, elems, offset, prefix, slot_base) = match layout {
        ExpectedPage::PlainNode => {
            let leaf_word = read_u32(page, 4, pid, path)?;
            if leaf_word > 1 {
                return Err(node_error(
                    "INVALID_NODE_KIND",
                    "plain node discriminant must be 0 or 1",
                    pid,
                    path,
                ));
            }
            (
                leaf_word == 1,
                read_u32(page, 8, pid, path)? as usize,
                read_u32(page, 12, pid, path)? as usize,
                Vec::new(),
                PLAIN_HEADER_SIZE,
            )
        }
        ExpectedPage::PrefixNode => {
            let kind = read_u32(page, 4, pid, path)?;
            let leaf = match kind {
                ENCODED_BRANCH => false,
                ENCODED_LEAF => true,
                _ => {
                    return Err(node_error(
                        "INVALID_NODE_KIND",
                        "encoded node discriminant must be 2 or 3",
                        pid,
                        path,
                    ));
                }
            };
            let prefix_len = read_u32(page, 16, pid, path)? as usize;
            let raw = ENCODED_HEADER_SIZE.checked_add(prefix_len).ok_or_else(|| {
                node_error(
                    "PREFIX_OVERFLOW",
                    "encoded prefix length overflows",
                    pid,
                    path,
                )
            })?;
            if raw > PAGE_SIZE {
                return Err(node_error(
                    "PREFIX_OVERFLOW",
                    "encoded prefix must fit in the page",
                    pid,
                    path,
                ));
            }
            let slot_base = (raw + 3) & !3;
            (
                leaf,
                read_u32(page, 8, pid, path)? as usize,
                read_u32(page, 12, pid, path)? as usize,
                page[ENCODED_HEADER_SIZE..raw].to_vec(),
                slot_base,
            )
        }
        _ => {
            return Err(node_error(
                "UNEXPECTED_NODE_LAYOUT",
                "node decoder received a non-node role",
                pid,
                path,
            ));
        }
    };
    if elems > (PAGE_SIZE - slot_base) / SLOT_SIZE {
        return Err(node_error(
            "NODE_ELEMS_OVERFLOW",
            "node slot count exceeds page capacity",
            pid,
            path,
        ));
    }
    let slot_end = slot_base + elems * SLOT_SIZE;
    if offset < slot_end || offset > PAGE_SIZE {
        return Err(node_error(
            "NODE_OFFSET_OUT_OF_RANGE",
            "node offset must be in [slot_array_end, PAGE_SIZE]",
            pid,
            path,
        ));
    }
    let mut entries = Vec::with_capacity(elems);
    for index in 0..elems {
        let at = slot_base + index * SLOT_SIZE;
        let pos = read_u32(page, at, pid, path)? as usize;
        let key_len = read_u32(page, at + 4, pid, path)? as usize;
        let value_len = read_u32(page, at + 8, pid, path)? as usize;
        if value_len as u64 > u64::from(MAX_VAL_LEN) {
            return Err(node_error(
                "VALUE_TOO_LARGE",
                "value length exceeds MAX_VAL_LEN",
                pid,
                path,
            ));
        }
        let mut page_ids = [0u32; NR_INLINE_PAGE];
        for (pid_index, page_id) in page_ids.iter_mut().enumerate() {
            *page_id = read_u32(page, at + 12 + pid_index * 4, pid, path)?;
        }
        let key_end = pos
            .checked_add(key_len)
            .ok_or_else(|| node_error("KEY_RANGE_OVERFLOW", "key range overflows", pid, path))?;
        let is_branch_sentinel = !leaf && index == 0 && key_len == 0;
        if (!is_branch_sentinel && (pos < slot_end || pos < offset))
            || pos > PAGE_SIZE
            || key_end > PAGE_SIZE
        {
            return Err(CheckDiagnostic::error(
                "NODE_PAYLOAD_OUT_OF_RANGE",
                "node payload must stay between the slot array and header offset",
                Some(pid),
                Some(path.to_string()),
                Some(format!("slot_array_end={slot_end}, offset={offset}")),
                Some(format!("pos={pos}, key_end={key_end}")),
            ));
        }
        let full_key = if matches!(layout, ExpectedPage::PrefixNode) {
            let mut key = prefix.clone();
            key.extend_from_slice(&page[pos..key_end]);
            key
        } else {
            page[pos..key_end].to_vec()
        };
        let full_key_len = full_key.len();
        let entry_key = if is_branch_sentinel {
            Vec::new()
        } else {
            full_key
        };
        if leaf && (full_key_len == 0 || full_key_len > MAX_KEY_LEN) {
            return Err(node_error(
                "INVALID_LEAF_KEY",
                "leaf keys must be non-empty and bounded",
                pid,
                path,
            ));
        }
        if !leaf {
            if value_len != 0 || page_ids[1..].iter().any(|value| *value != 0) {
                return Err(node_error(
                    "INVALID_BRANCH_SLOT",
                    "branch slots only carry one child PID",
                    pid,
                    path,
                ));
            }
            if index > 0 && (entry_key.is_empty() || entry_key.len() > MAX_KEY_LEN) {
                return Err(node_error(
                    "INVALID_BRANCH_SEPARATOR",
                    "branch separators must be non-empty and bounded",
                    pid,
                    path,
                ));
            }
            if index == 0 && !entry_key.is_empty() {
                return Err(node_error(
                    "INVALID_BRANCH_SENTINEL",
                    "the first branch separator must be empty",
                    pid,
                    path,
                ));
            }
            if page_ids[0] == 0 {
                return Err(node_error(
                    "ZERO_CHILD_PID",
                    "branch child PID must be non-zero",
                    pid,
                    path,
                ));
            }
        }
        if leaf && value_len == 0 && page_ids.iter().any(|value| *value != 0) {
            return Err(node_error(
                "ZERO_VALUE_HAS_PAGE_ID",
                "zero-length values may not reference physical pages",
                pid,
                path,
            ));
        }
        let inline = leaf
            && value_len
                <= MAX_INLINE_LEN.saturating_sub(if matches!(layout, ExpectedPage::PrefixNode) {
                    key_len
                } else {
                    full_key_len
                });
        let mut inline_value = None;
        if leaf && inline {
            let value_start = pos.checked_add(key_len).ok_or_else(|| {
                node_error(
                    "VALUE_RANGE_OVERFLOW",
                    "inline value range overflows",
                    pid,
                    path,
                )
            })?;
            let value_end = value_start.checked_add(value_len).ok_or_else(|| {
                node_error(
                    "VALUE_RANGE_OVERFLOW",
                    "inline value range overflows",
                    pid,
                    path,
                )
            })?;
            if value_end > PAGE_SIZE {
                return Err(node_error(
                    "VALUE_OUT_OF_RANGE",
                    "inline value must stay inside the page",
                    pid,
                    path,
                ));
            }
            if page_ids.iter().any(|value| *value != 0) {
                return Err(node_error(
                    "INLINE_VALUE_HAS_PAGE_ID",
                    "inline values may not reference physical pages",
                    pid,
                    path,
                ));
            }
            inline_value = Some(page[value_start..value_end].to_vec());
        } else if leaf {
            if value_len == 0 {
                if page_ids.iter().any(|value| *value != 0) {
                    return Err(node_error(
                        "ZERO_VALUE_HAS_PAGE_ID",
                        "zero-length values may not reference physical pages",
                        pid,
                        path,
                    ));
                }
            } else {
                let needed = value_len.div_ceil(TRAILER_CONTENT_SIZE);
                if needed <= NR_INLINE_PAGE {
                    if page_ids.iter().skip(needed).any(|value| *value != 0) {
                        return Err(node_error(
                            "DIRECT_UNUSED_PID_NONZERO",
                            "unused direct value PIDs must be zero",
                            pid,
                            path,
                        ));
                    }
                    if page_ids[..needed].contains(&0) {
                        return Err(node_error(
                            "DIRECT_VALUE_PID_ZERO",
                            "direct value PIDs must be non-zero",
                            pid,
                            path,
                        ));
                    }
                } else if page_ids[0] == 0 || page_ids[1..].iter().any(|value| *value != 0) {
                    return Err(node_error(
                        "INDIRECT_SLOT_NONCANONICAL",
                        "indirect slots only use the first PID",
                        pid,
                        path,
                    ));
                }
            }
        }
        entries.push(DecodedEntry {
            key: entry_key,
            slot: DecodedSlot {
                value_len,
                page_ids,
                inline_value,
            },
        });
    }
    Ok(DecodedNode { leaf, entries })
}

fn read_u32(
    page: &[u8; PAGE_SIZE],
    offset: usize,
    pid: PageId,
    path: &str,
) -> Result<u32, CheckDiagnostic> {
    let end = offset.checked_add(4).ok_or_else(|| {
        node_error(
            "FIELD_RANGE_OVERFLOW",
            "fixed field range overflows",
            pid,
            path,
        )
    })?;
    let bytes = page
        .get(offset..end)
        .ok_or_else(|| node_error("FIELD_OUT_OF_RANGE", "fixed field exceeds page", pid, path))?;
    Ok(u32::from_le_bytes(
        bytes.try_into().expect("four-byte field"),
    ))
}

fn node_error(code: &'static str, check: &'static str, pid: PageId, path: &str) -> CheckDiagnostic {
    CheckDiagnostic::error(code, check, Some(pid), Some(path.to_string()), None, None)
}

/// Checks a static current-format database without modifying it.
///
/// This is [`check_path_with_options`] with no scan: it runs the same checker
/// and discards the statistics it already derived, so a caller that wants only
/// the report pays no extra traversal.
pub fn check_path<P: AsRef<Path>>(path: P) -> CheckResult<CheckReport> {
    Ok(check_path_with_options(path, &CheckOptions::default())?.report)
}

/// [`check_path_with_options`] without a scan: the report plus the space the
/// selected generation's allocator chains imply.
///
/// It asks for no scan, so `buckets` is `None`; a caller that wants the
/// per-bucket statistics calls `check_path_with_options` with
/// `CheckOptions { scan: true }` instead.
pub fn check_path_with_space<P: AsRef<Path>>(path: P) -> CheckResult<CheckReportWithSpace> {
    check_path_with_options(path, &CheckOptions::default())
}

/// Checks a static database and, when `options.scan` is set, also reports what
/// each bucket holds.
///
/// The walk is the one [`check_path`] already performs; the scan only keeps
/// counters it would otherwise throw away. Statistics are published for a
/// passing check only: a report that failed or stopped early would otherwise
/// publish the part of the file that happened to be readable.
pub fn check_path_with_options<P: AsRef<Path>>(
    path: P,
    options: &CheckOptions,
) -> CheckResult<CheckReportWithSpace> {
    let scan = options.scan;
    let path = path.as_ref().to_path_buf();
    let source = OfflineFile::open(&path)?;
    let slot_a = source.read_meta(META_SLOT_A)?;
    let slot_b = source.read_meta(META_SLOT_SIZE)?;
    let (meta, selected_slot) = select_meta(slot_a, slot_b)?;
    let ignored_candidate = match selected_slot {
        MetaSlot::A => slot_b,
        MetaSlot::B => slot_a,
    };
    let ignored_warning =
        (!matches!(ignored_candidate, Some(MetaCandidate::Valid(_)))).then(|| {
            CheckDiagnostic::warning(
                "META_CANDIDATE_IGNORED",
                "an unreadable or invalid metadata candidate was ignored",
                None,
                Some("metadata".to_string()),
                Some("selected current generation".to_string()),
                Some("invalid/truncated candidate".to_string()),
            )
        });
    if meta.next_page_id < 2 {
        source.ensure_unchanged()?;
        let mut diagnostics = Vec::new();
        diagnostics.extend(ignored_warning);
        diagnostics.push(CheckDiagnostic::error(
            "INVALID_NEXT_PAGE_ID",
            "next_page_id must reserve both metadata slots",
            None,
            Some("meta".to_string()),
            Some(">= 2".to_string()),
            Some(meta.next_page_id.to_string()),
        ));
        for diagnostic in &mut diagnostics {
            diagnostic.generation = Some(meta.seq);
        }
        return Ok(withheld_space(CheckReport {
            status: CheckStatus::Failed,
            path,
            file_len: source.identity.len,
            selected_slot: Some(selected_slot),
            generation: Some(meta.seq),
            next_page_id: Some(meta.next_page_id),
            reachable_pages: 0,
            reusable_pages: 0,
            retired_pages: 0,
            allocator_list_pages: 0,
            trailing_bytes: 0,
            diagnostics,
        }));
    }
    let data_end = u64::from(meta.next_page_id) * PAGE_SIZE as u64;
    if meta.next_page_id > 2 && data_end > source.identity.len {
        source.ensure_unchanged()?;
        let page_len = PAGE_SIZE as u64;
        let pid = (source.identity.len / page_len)
            .max(2)
            .min(u64::from(meta.next_page_id - 1)) as PageId;
        let start = u64::from(pid) * page_len;
        let truncated = start < source.identity.len;
        let mut diagnostics = Vec::new();
        diagnostics.extend(ignored_warning);
        diagnostics.push(CheckDiagnostic::error(
            if truncated {
                "TRUNCATED_PAGE"
            } else {
                "MISSING_PHYSICAL_PAGE"
            },
            "the data id space must be physically covered by complete pages",
            Some(pid),
            Some("data-id-space".to_string()),
            Some(format!("file length >= {}", start + page_len)),
            Some(source.identity.len.to_string()),
        ));
        for diagnostic in &mut diagnostics {
            diagnostic.generation = Some(meta.seq);
        }
        return Ok(withheld_space(CheckReport {
            status: CheckStatus::Failed,
            path,
            file_len: source.identity.len,
            selected_slot: Some(selected_slot),
            generation: Some(meta.seq),
            next_page_id: Some(meta.next_page_id),
            reachable_pages: 0,
            reusable_pages: 0,
            retired_pages: 0,
            allocator_list_pages: 0,
            trailing_bytes: 0,
            diagnostics,
        }));
    }
    let mut checker = Checker::new(source, meta, selected_slot, scan);
    if let Some(warning) = ignored_warning {
        checker.add_warning(warning);
    }
    let collected = checker.run(path)?;
    let passed = collected.report.status == CheckStatus::Ok;
    Ok(CheckReportWithSpace {
        space: passed.then_some(collected.space),
        buckets: if passed && scan {
            Some(collected.buckets)
        } else {
            None
        },
        report: collected.report,
    })
}

/// A report that did not pass carries no statistics: its counters describe how
/// far the walk got, not what the file holds.
fn withheld_space(report: CheckReport) -> CheckReportWithSpace {
    CheckReportWithSpace {
        report,
        space: None,
        buckets: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each checker gets its own file: a live check holds a whole-file shared
    /// lock, and Windows denies a writer access to a locked range even from the
    /// same process, so a second checker could not rewrite the first one's file.
    fn checker(dir: &Path, name: &str) -> Checker {
        let path = dir.join(name);
        std::fs::write(&path, [0u8; 2 * PAGE_SIZE]).unwrap();
        let source = OfflineFile::open(&path).unwrap();
        let meta = Meta {
            seq: 7,
            catalog_root: 0,
            next_page_id: 2,
            reusable_root: 0,
            retired_root: 0,
        };
        Checker::new(source, meta, MetaSlot::A, false)
    }

    fn probe(code: &'static str) -> CheckDiagnostic {
        CheckDiagnostic::error(code, "probe", None, None, None, None)
    }

    /// No static-file fixture can make a page read fail, so the classification
    /// rule is pinned here: a definite violation fails the check, an unreadable
    /// page only leaves it incomplete, and `Failed` wins in either arrival order.
    #[test]
    fn a_definite_violation_fails_the_check_whatever_arrives_first() {
        let dir = tempfile::tempdir().unwrap();

        let mut incomplete_first = checker(dir.path(), "incomplete-first.db");
        incomplete_first.add_error(probe("IO_READ"));
        assert_eq!(incomplete_first.report.status, CheckStatus::Incomplete);
        incomplete_first.add_error(probe("PAGE_CRC_MISMATCH"));
        assert_eq!(incomplete_first.report.status, CheckStatus::Failed);

        let mut failed_first = checker(dir.path(), "failed-first.db");
        failed_first.add_error(probe("PAGE_CRC_MISMATCH"));
        assert_eq!(failed_first.report.status, CheckStatus::Failed);
        failed_first.add_error(probe("IO_READ"));
        assert_eq!(failed_first.report.status, CheckStatus::Failed);
    }
}
