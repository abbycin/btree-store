//! The offline `compact` command: rebuild a compact copy of a current-format file.
//!
//! `compact <source> <destination>` rebuilds the source into the destination,
//! replacing whatever held that name; the source itself is never a destination.
//! Two mode words are accepted. `inplace` is reserved: relocating a page in place
//! is not implemented in this build, and it must not half-run, so it refuses before
//! anything is created or opened. `rebuild` (the default) copies the live data into
//! a fresh file; `--info` reports what that copy would cost without writing it.

use crate::check::{error_class, status_class};
use crate::cli::{Class, CliError};
use crate::staging::{Staging, batch_is_full};
use btree_store::{BTree, CheckStatus, MetaSlot, check_path};
use std::path::PathBuf;

/// Parsed `compact` invocation.
pub(crate) struct CompactArgs {
    pub(crate) source: PathBuf,
    /// The file to write. The parser requires it for a run that would write, and
    /// permits its absence for `--info`, which names no target.
    pub(crate) destination: Option<PathBuf>,
    pub(crate) mode: Mode,
    pub(crate) info: bool,
}

/// The two spellings the design accepts.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Rebuild,
    Inplace,
}

impl Mode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Rebuild => "rebuild",
            Self::Inplace => "inplace",
        }
    }
}

pub(crate) fn run(args: CompactArgs) -> Result<String, CliError> {
    // The mode decides first: `--info` combined with `inplace` still
    // refuses, and nothing is created, named or opened before this point.
    if args.mode == Mode::Inplace {
        return Err(CliError::new(
            "arguments",
            Class::Arg,
            "inplace is not implemented in this build; use rebuild",
        ));
    }
    // Mode subcommands cannot enforce a destination conditional on a parent --info.
    let destination = if args.info {
        None
    } else {
        Some(args.destination.clone().ok_or_else(|| {
            CliError::new(
                "arguments",
                Class::Arg,
                "a run that writes needs a destination; pass one, or use --info to only report what it would cost",
            )
        })?)
    };
    // Reject unusable destinations before spending time auditing and rebuilding.
    if let Some(destination) = &destination {
        crate::staging::check_output_name("the destination", destination)?;
        crate::staging::refuse_self_destination(destination, &args.source)?;
        crate::staging::check_replaceable_destination(destination)?;
    }

    // Keep the source locked from audit through publication.
    let (source_handle, stage) = read_stage(&args.source)?;
    let estimate = estimate_bytes(&stage.stats);
    eprintln!(
        "estimate mode={} estimated=true bytes_low={} bytes_high={} records={} logical_bytes={}",
        args.mode.as_str(),
        estimate.bytes_low,
        estimate.bytes_high,
        stage.stats.records,
        stage.stats.logical_bytes,
    );
    if args.info {
        return Ok(String::new());
    }
    let destination = destination.expect("a writing run has a destination, decided above");

    let staging = create_staging(&destination)?;
    let outcome: Result<Summary, CliError> = (|| {
        let written = rebuild(&source_handle, &stage.buckets, &staging)?;
        let target_generation = verify_target(&staging.path)?;
        compare_v2(&source_handle, &staging.path)?;
        ensure_source_identity(&source_handle, &args.source, "rebuild")?;
        let physical_bytes = std::fs::metadata(&staging.path)
            .map_err(|error| CliError::io("staging", "stat", &staging.path, error))?
            .len();
        #[cfg(unix)]
        let source_mode = source_mode(&args.source)?;
        crate::staging::publish(
            &staging,
            &destination,
            #[cfg(unix)]
            source_mode,
        )?;
        Ok(Summary {
            mode: args.mode,
            source: args.source.clone(),
            source_generation: stage.generation,
            source_selected_slot: stage.selected_slot,
            source_bytes: stage.source_bytes,
            target: destination.clone(),
            target_generation,
            buckets: written.buckets,
            records: written.records,
            logical_bytes: written.logical_bytes,
            batches: written.batches,
            physical_bytes,
            estimated_low: estimate.bytes_low,
            estimated_high: estimate.bytes_high,
            verified: true,
        })
    })();

    match outcome {
        Ok(summary) => Ok(summary.render()),
        Err(error) => {
            if crate::staging::removes_staging_after(error.class) {
                crate::staging::cleanup_staging(&staging);
            }
            let present = crate::staging::staging_is_present(&staging.path);
            Err(error.with_staging(&staging.path, present))
        }
    }
}

/// The staging name's middle segment for this command. Each command has its own:
/// a leftover from one is never mistaken for a leftover from the other.
fn staging_suffix() -> String {
    format!(".compact-v{}", btree_store::FORMAT_VERSION)
}

/// The staging file for this run, created beside the destination under a name no
/// other command uses, so a leftover can never be mistaken for a published one.
fn create_staging(destination: &std::path::Path) -> Result<Staging, CliError> {
    let parent = match destination.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    let name = destination
        .file_name()
        .expect("the destination was validated to name a file");
    crate::staging::create_staging_in(&parent, name, std::process::id(), &staging_suffix())
}

/// The permission bits the published target inherits from the source.
#[cfg(unix)]
fn source_mode(source: &std::path::Path) -> Result<u32, CliError> {
    use std::os::unix::fs::PermissionsExt;
    let metadata =
        std::fs::metadata(source).map_err(|error| CliError::io("source", "stat", source, error))?;
    Ok(metadata.permissions().mode() & 0o7777)
}

/// What a successful rebuild reports on stdout, in the same `key=value` shape as
/// the other commands' summaries.
struct Summary {
    /// The spelling this run used. It is always `rebuild`, because `inplace`
    /// refuses before a summary is possible; it is part of the published summary
    /// shape, so a reader of a saved summary is not left guessing which of the
    /// command's two spellings produced it.
    mode: Mode,

    source: std::path::PathBuf,
    source_generation: u64,
    source_selected_slot: Option<MetaSlot>,
    source_bytes: u64,
    target: std::path::PathBuf,
    target_generation: u64,
    buckets: u64,
    records: u64,
    logical_bytes: u64,
    batches: u64,
    physical_bytes: u64,
    estimated_low: u64,
    estimated_high: u64,
    verified: bool,
}

impl Summary {
    /// `reclaimed_bytes` is a saturating subtraction: this command does not
    /// promise a smaller file, so a target that grew reports zero rather than
    /// wrapping around.
    fn render(&self) -> String {
        let mut out = String::new();
        let mut line = |key: &str, value: String| {
            out.push_str(key);
            out.push('=');
            out.push_str(&value);
            out.push('\n');
        };
        line("mode", self.mode.as_str().to_string());
        line("source", self.source.display().to_string());
        line("source_version", btree_store::FORMAT_VERSION.to_string());
        line("source_generation", self.source_generation.to_string());
        line(
            "source_selected_slot",
            match self.source_selected_slot {
                Some(MetaSlot::A) => "A".to_string(),
                Some(MetaSlot::B) => "B".to_string(),
                None => "-".to_string(),
            },
        );
        line("source_bytes", self.source_bytes.to_string());
        line("target", self.target.display().to_string());
        line("target_version", btree_store::FORMAT_VERSION.to_string());
        line("target_generation", self.target_generation.to_string());
        line("buckets", self.buckets.to_string());
        line("records", self.records.to_string());
        line("logical_bytes", self.logical_bytes.to_string());
        line("batches", self.batches.to_string());
        line("physical_bytes", self.physical_bytes.to_string());
        line(
            "estimated_bytes",
            format!("{}..{}", self.estimated_low, self.estimated_high),
        );
        line(
            "reclaimed_bytes",
            self.source_bytes
                .saturating_sub(self.physical_bytes)
                .to_string(),
        );
        line("verified", self.verified.to_string());
        out
    }
}

/// What the rebuild wrote, counted as it commits.
#[derive(Default)]
struct Written {
    buckets: u64,
    records: u64,
    logical_bytes: u64,
    batches: u64,
}

/// Writes the source's records into the staging file through the normal writer,
/// in batched transactions. Every bucket is created with the policy it carries in
/// the source, so the copy's layout rules are the source's.
fn rebuild(
    source: &BTree,
    buckets: &[(String, bool)],
    staging: &Staging,
) -> Result<Written, CliError> {
    crate::staging::injected_fault("rebuild").map_err(|error| {
        CliError::new("rebuild", Class::Io, format!("staging database: {error}"))
    })?;
    let tree = BTree::open(&staging.path).map_err(|error| {
        CliError::new(
            "rebuild",
            Class::Io,
            format!("opening the staging database: {error:?}"),
        )
    })?;
    let mut written = Written::default();
    for (name, prefix_encoding) in buckets {
        tree.new_bucket(name, *prefix_encoding).map_err(|error| {
            CliError::new(
                "rebuild",
                Class::Io,
                format!("creating bucket {name:?}: {error:?}"),
            )
        })?;
    }
    written.buckets = buckets.len() as u64;

    for (name, _) in buckets {
        let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut batch_bytes = 0u64;
        // The read closure's own error type is fixed by `view`, so a failed
        // write is parked here as the `CliError` `flush` built and returned
        // after the view returns.
        let mut write_error: Option<CliError> = None;
        source
            .view(name, |txn| {
                let mut iterator = txn.iter();
                let mut key = Vec::new();
                let mut value = Vec::new();
                while iterator.next_ref(&mut key, &mut value) {
                    let size = (key.len() + value.len()) as u64;
                    if batch_is_full(batch.len(), batch_bytes, size) {
                        if let Err(error) =
                            flush(&tree, name, &mut batch, &mut batch_bytes, &mut written)
                        {
                            write_error = Some(error);
                            return Ok(());
                        }
                    }
                    batch_bytes += size;
                    batch.push((key.clone(), value.clone()));
                    if size > crate::staging::BATCH_BYTES {
                        if let Err(error) =
                            flush(&tree, name, &mut batch, &mut batch_bytes, &mut written)
                        {
                            write_error = Some(error);
                            return Ok(());
                        }
                    }
                }
                Ok(())
            })
            .map_err(|error| {
                CliError::new(
                    "rebuild",
                    Class::Io,
                    format!("reading bucket {name:?}: {error}"),
                )
            })?;
        if write_error.is_none() {
            write_error = flush(&tree, name, &mut batch, &mut batch_bytes, &mut written).err();
        }
        if let Some(error) = write_error {
            return Err(error);
        }
    }
    Ok(written)
}

/// Commits one batch and folds it into the totals. An empty batch is not a
/// transaction: a bucket with no records still exists, but costs no commit.
fn flush(
    tree: &BTree,
    bucket: &str,
    batch: &mut Vec<(Vec<u8>, Vec<u8>)>,
    batch_bytes: &mut u64,
    written: &mut Written,
) -> Result<(), CliError> {
    if batch.is_empty() {
        return Ok(());
    }
    let records = batch.len() as u64;
    let logical = *batch_bytes;
    tree.exec(bucket, |txn| {
        for (key, value) in batch.drain(..) {
            txn.put(key, value)?;
        }
        Ok(())
    })
    .map_err(|error| {
        CliError::new(
            "rebuild",
            Class::Io,
            format!("writing {records} records into {bucket:?}: {error:?}"),
        )
    })?;
    written.records += records;
    written.logical_bytes += logical;
    written.batches += 1;
    *batch_bytes = 0;
    Ok(())
}

/// The two gates a staging file must pass before it may be published: the same
/// audit the source had to pass, and a record-by-record comparison against the
/// source. Returns the target's generation for the summary.
fn verify_target(staging: &std::path::Path) -> Result<u64, CliError> {
    // The subject of this gate is the file this run built, not the operator's
    // source (which passed the same audit moments ago), so both of its failure
    // shapes are an invalid target: one where the audit could not be read, and
    // one where it was read and did not accept the file. Neither is an unhealthy
    // source, and neither should be reported against the source's own classes.
    let report = check_path(staging).map_err(|error| {
        CliError::new(
            "target",
            Class::TargetInvalid,
            format!("auditing the staging file: {error}"),
        )
    })?;
    if report.status != CheckStatus::Ok {
        return Err(CliError::new(
            "target",
            Class::TargetInvalid,
            format!(
                "the staging file did not pass the audit: {} diagnostic(s)",
                report.diagnostics.len()
            ),
        ));
    }
    Ok(report.generation.unwrap_or(0))
}

/// Compares the staging file with the source record by record. The two must agree
/// on which buckets exist, in which order, with which prefix policy, and on every
/// key and value byte.
fn compare_v2(source: &BTree, staging: &std::path::Path) -> Result<(), CliError> {
    crate::staging::injected_fault("compare").map_err(|error| {
        CliError::new("compare", Class::Io, format!("target comparison: {error}"))
    })?;
    let target = BTree::open_read_only(staging).map_err(|error| {
        CliError::new(
            "compare",
            Class::TargetInvalid,
            format!("reopening {}: {error:?}", staging.display()),
        )
    })?;
    let source_buckets = source
        .buckets_with_policy()
        .map_err(|error| CliError::new("compare", Class::Io, format!("{error:?}")))?;
    let target_buckets = target.buckets_with_policy().map_err(|error| {
        CliError::new(
            "compare",
            Class::TargetInvalid,
            format!("reading the target catalog: {error:?}"),
        )
    })?;
    if source_buckets != target_buckets {
        return Err(CliError::new(
            "compare",
            Class::VerifyMismatch,
            format!(
                "bucket sets differ: the source has {:?}, the target has {:?}",
                source_buckets, target_buckets
            ),
        ));
    }
    for (name, _) in &source_buckets {
        let mut difference = None;
        source
            .view(name, |txn| {
                target.view(name, |target_txn| {
                    let mut source_iter = txn.iter();
                    let mut target_iter = target_txn.iter();
                    let mut source_key = Vec::new();
                    let mut source_value = Vec::new();
                    let mut target_key = Vec::new();
                    let mut target_value = Vec::new();
                    loop {
                        let has_source = source_iter.next_ref(&mut source_key, &mut source_value);
                        let has_target = target_iter.next_ref(&mut target_key, &mut target_value);
                        if has_source != has_target {
                            difference = Some(format!(
                                "bucket {name:?}: the source and the target hold a different number of records"
                            ));
                            return Ok(());
                        }
                        if !has_source {
                            return Ok(());
                        }
                        if source_key != target_key || source_value != target_value {
                            difference = Some(format!(
                                "bucket {name:?}: key {:?} / value {} bytes differ",
                                String::from_utf8_lossy(&source_key),
                                source_value.len()
                            ));
                            return Ok(());
                        }
                    }
                })
            })
            .map_err(|error| {
                CliError::new(
                    "compare",
                    Class::VerifyMismatch,
                    format!("reading bucket {name:?}: {error}"),
                )
            })?;
        if let Some(detail) = difference {
            return Err(CliError::new("compare", Class::VerifyMismatch, detail));
        }
    }
    Ok(())
}

/// What one logical traversal counts, and everything the estimate is derived from. Every
/// field is a count the traversal actually observed, never a fill-rate guess. Which pages
/// those bytes turn into is the writer's call, so a field is an exact count unless its own
/// note says it is only an upper bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SourceStats {
    pub(crate) buckets: usize,
    pub(crate) records: u64,
    /// key bytes plus value bytes, as the reader returns them: an over-count in a
    /// prefix-encoded bucket, where the writer stores only the key tail — see
    /// [`SourceStats::record`].
    pub(crate) logical_bytes: u64,
    /// Value pages the writer will allocate: `ceil(value / VALUE_PAGE_CONTENT)`.
    /// Exact for buckets that store whole keys, and an upper bound for
    /// prefix-encoded ones — see [`SourceStats::record`].
    pub(crate) value_pages: u64,
    /// Indirect pages for the values that need them, on the same footing as
    /// `value_pages`.
    pub(crate) indirect_pages: u64,
    /// Node content: a slot entry plus the key bytes the reader returns. In a
    /// prefix-encoded bucket the writer stores only the part of the key beyond the
    /// node's prefix, so this is an upper bound there — see [`SourceStats::record`].
    pub(crate) node_content_bytes: u64,
    /// Bucket-name bytes, which the catalog stores beside each entry's slot.
    pub(crate) catalog_name_bytes: u64,
}

impl SourceStats {
    /// Folds one record in, using the writer's own page rules.
    ///
    /// `key_len` is the length of the key as the reader returns it. In a
    /// prefix-encoded bucket the writer does not store that key: it stores the
    /// tail beyond the node's prefix and decides inlining from the tail, so a
    /// record whose tail fits is stored inline even when the full key does not.
    /// The read path exposes no stored form to measure, so this count charges the
    /// full key, which over-counts for exactly the bucket shape prefix encoding
    /// exists to serve. The count is never an under-estimate, and it is exact
    /// whenever whole keys are stored.
    pub(crate) fn record(&mut self, key_len: usize, value_len: usize) {
        use btree_store::{IDS_PER_INDIRECT_PAGE, MAX_INLINE_LEN, SLOT_SIZE, VALUE_PAGE_CONTENT};
        let slot = SLOT_SIZE;
        self.records += 1;
        self.logical_bytes += (key_len + value_len) as u64;
        self.node_content_bytes += (slot + key_len) as u64;
        // The writer stores the key tail beside the slot, not the whole key, so a
        // prefix-encoded bucket makes this an upper bound rather than the exact
        // node content.

        // The writer keeps a value in the slot only while the stored key tail and
        // the value together fit (`value_inline_in_slot`).
        if key_len + value_len > MAX_INLINE_LEN {
            let pages = value_len.div_ceil(VALUE_PAGE_CONTENT) as u64;
            self.value_pages += pages;
            if pages > 5 {
                self.indirect_pages += pages.div_ceil(IDS_PER_INDIRECT_PAGE as u64);
            }
        }
    }

    /// Folds one bucket in. The catalog stores its name beside the entry's slot,
    /// so the name is catalog content rather than node content.
    pub(crate) fn bucket(&mut self, name_len: usize) {
        self.buckets += 1;
        self.catalog_name_bytes += name_len as u64;
    }
}
/// The estimated size of a rebuild, as a range. Neither end is a guarantee: the
/// node page count is a range because page filling is the writer's decision, the
/// value and indirect page counts are upper bounds in a prefix-encoded bucket
/// (see [`SourceStats::record`]), and the allocator list pages a rebuild
/// allocates are not counted at all. The range is a cost estimate for deciding
/// whether to run the command, not a bracket the produced file must land in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SizeEstimate {
    pub(crate) bytes_low: u64,
    pub(crate) bytes_high: u64,
}

const FIXED_PAGES: u64 = 2; // the two metadata slots
/// Bytes one catalog entry costs the writer: a slot, the bucket name stored beside
/// it, and the fixed-width metadata record the entry carries inline.
const BUCKET_METADATA_BYTES: u64 = 8; // root page id and flags

/// Fixed pages every rebuild allocates. Only the two metadata slots are counted:
/// a rebuild starts from an empty allocator, and the allocator list chains grow
/// with the free pages it inherits, which the traversal cannot see. `check`
/// reports the real figure as `allocator_list_pages=`.
fn fixed_pages() -> u64 {
    FIXED_PAGES
}

/// `stats` in, a range out. The low end packs node content perfectly; the high
/// end gives every record its own page.
pub(crate) fn estimate_bytes(stats: &SourceStats) -> SizeEstimate {
    use btree_store::PAGE_SIZE;
    let page = PAGE_SIZE as u64;
    // The writer allocates whole pages for values, so price them as pages.
    let value_bytes = stats.value_pages * page;
    let indirect_bytes = stats.indirect_pages * page;
    // Low end: every node byte packed perfectly. High end: every record's slot and
    // key padded out to a page boundary, which is the writer's worst case per entry.
    let node_low = stats.node_content_bytes.div_ceil(page);
    let node_high = stats.records * 2; // one page for the slot, one for the key
    use btree_store::SLOT_SIZE;
    let catalog_bytes = (stats.buckets as u64) * (SLOT_SIZE as u64 + BUCKET_METADATA_BYTES)
        + stats.catalog_name_bytes;
    let bytes_low =
        (fixed_pages() + node_low) * page + value_bytes + indirect_bytes + catalog_bytes;
    let bytes_high =
        (fixed_pages() + node_high) * page + value_bytes + indirect_bytes + catalog_bytes;
    debug_assert!(
        bytes_low <= bytes_high,
        "the low bound is packed node content, the high bound is one page per record"
    );
    SizeEstimate {
        bytes_low,
        bytes_high,
    }
}

/// The read-only stage: the source is open read-only and stays that way for its
/// whole life, the audit runs **inside that lock**, and the traversal counts what
/// the audit accepted. The order matters — opening after auditing would leave a
/// window in which another process can commit, and the rebuild would then be of a
/// generation nobody audited.
pub(crate) struct ReadStage {
    pub(crate) stats: SourceStats,
    /// Bucket names with their prefix policy, in catalog order. The rebuild walks
    /// this rather than re-listing, so both phases see one catalog.
    pub(crate) buckets: Vec<(String, bool)>,
    pub(crate) source_bytes: u64,
    pub(crate) generation: u64,
    pub(crate) selected_slot: Option<MetaSlot>,
}

/// Maps a source-open failure to its class. The meta-level refusals arrive here,
/// not from the audit: an unreadable, busy or non-current-format file is refused
/// by the open.
fn open_class(error: &btree_store::OpenError) -> Class {
    match error {
        btree_store::OpenError::Io(_) | btree_store::OpenError::ReadOnly => Class::Io,
        btree_store::OpenError::DatabaseBusy { .. } => Class::LockBusy,
        btree_store::OpenError::Corruption(report) => match report.code {
            "UNSUPPORTED_FORMAT_VERSION" => Class::SourceVersionUnsupported,
            "MIXED_FORMAT_VERSIONS" => Class::SourceVersionMixed,
            _ => Class::SourceCorrupt,
        },
        btree_store::OpenError::InvalidOptions(_) => Class::Io,
    }
}

/// Runs the read-only stage. The returned value owns the source handle for as
/// long as the caller needs it: dropping it releases the shared lock, and the
/// audit result must not outlive the lock that produced it.
pub(crate) fn read_stage(source: &std::path::Path) -> Result<(BTree, ReadStage), CliError> {
    read_stage_probing(source, &|_| {})
}

/// [`read_stage`] with a probe called at each step, so a test can observe the
/// order the steps ran in and what was already true when each one ran.
pub(crate) fn read_stage_probing(
    source: &std::path::Path,
    probe: &dyn Fn(&str),
) -> Result<(BTree, ReadStage), CliError> {
    let tree = BTree::open_read_only(source).map_err(|error| {
        let detail = match &error {
            btree_store::OpenError::Corruption(report)
                if report.code == "UNSUPPORTED_FORMAT_VERSION" =>
            {
                format!(
                    "{error}; run `btree-store migrate` to bring this file to the current format"
                )
            }
            _ => error.to_string(),
        };
        CliError::new("source", open_class(&error), detail)
    })?;

    probe("open");

    // Inside the shared lock the open just took: no writer can commit until the
    // handle is dropped, so the audit and the traversal see one generation.
    probe("audit");
    let report = check_path(source)
        .map_err(|error| CliError::new("check", error_class(&error), error.to_string()))?;
    ensure_source_identity(&tree, source, "audit")?;
    if report.status != CheckStatus::Ok {
        let class = status_class(report.status);
        return Err(CliError::new(
            "check",
            class,
            format!(
                "{}: the source is not healthy, refusing to rebuild it",
                report.diagnostics.len()
            ),
        ));
    }

    probe("traverse");
    let mut stats = SourceStats::default();
    let buckets = tree
        .buckets_with_policy()
        .map_err(|error| CliError::new("source", Class::Io, error.to_string()))?;
    for (name, _prefix_encoding) in &buckets {
        // The catalog stores the name beside the entry's slot, so it is catalog
        // content rather than node content.
        stats.bucket(name.len());
        let bucket = name.clone();
        let mut count = SourceStats::default();
        tree.view(&bucket, |txn| {
            let mut iterator = txn.iter();
            let mut key = Vec::new();
            let mut value = Vec::new();
            while iterator.next_ref(&mut key, &mut value) {
                count.record(key.len(), value.len());
            }
            Ok(())
        })
        .map_err(|error| CliError::new("source", Class::Io, error.to_string()))?;
        stats.records += count.records;
        stats.logical_bytes += count.logical_bytes;
        stats.value_pages += count.value_pages;
        stats.indirect_pages += count.indirect_pages;
        stats.node_content_bytes += count.node_content_bytes;
    }

    ensure_source_identity(&tree, source, "traverse")?;

    let source_bytes = std::fs::metadata(source)
        .map_err(|error| CliError::io("source", "stat", source, error))?
        .len();
    Ok((
        tree,
        ReadStage {
            stats,
            buckets,
            source_bytes,
            generation: report.generation.unwrap_or(0),
            selected_slot: report.selected_slot,
        },
    ))
}

fn ensure_source_identity(
    tree: &BTree,
    source: &std::path::Path,
    phase: &'static str,
) -> Result<(), CliError> {
    let same = tree
        .path_is_same_file(source)
        .map_err(|error| CliError::io("source", "check identity", source, error))?;
    if same {
        return Ok(());
    }
    Err(CliError::new(
        "source",
        Class::Io,
        format!("the source path changed during {phase}; refusing to rebuild"),
    ))
}

#[cfg(test)]
mod compare_tests {
    use super::*;
    use std::path::Path;

    fn db(dir: &Path, name: &str, build: impl FnOnce(&BTree)) -> std::path::PathBuf {
        let path = dir.join(name);
        let tree = BTree::open(&path).unwrap();
        build(&tree);
        drop(tree);
        path
    }

    fn plain(tree: &BTree, keys: &[&str]) {
        tree.new_bucket("plain", false).unwrap();
        tree.exec("plain", |txn| {
            for key in keys {
                txn.put(key.as_bytes(), b"v")?;
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn staging_creation_rechecks_destination_replaceability() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.db");
        crate::staging::check_replaceable_destination(&target).unwrap();
        std::fs::create_dir(&target).unwrap();
        let error = create_staging(&target).unwrap_err();
        assert_eq!(error.phase, "target");
        assert_eq!(error.class, Class::Io);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    /// Identical sources compare clean.
    #[test]
    fn equal_sources_compare_equal() {
        let dir = tempfile::tempdir().unwrap();
        let source = db(dir.path(), "s.db", |t| plain(t, &["a", "b"]));
        let target = db(dir.path(), "t.db", |t| plain(t, &["a", "b"]));
        let source = BTree::open_read_only(&source).unwrap();
        assert!(compare_v2(&source, &target).is_ok());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn source_identity_rejects_a_replaced_path() {
        let dir = tempfile::tempdir().unwrap();
        let source = db(dir.path(), "source.db", |tree| plain(tree, &["a"]));
        let held = BTree::open_read_only(&source).unwrap();
        let replacement = db(dir.path(), "replacement.db", |tree| plain(tree, &["b"]));
        assert!(held.path_is_same_file(&source).unwrap());
        let alias = dir.path().join("alias.db");
        std::fs::hard_link(&source, &alias).unwrap();
        assert!(held.path_is_same_file(&alias).unwrap());
        // Moving the old name first also works on Windows without rename-overwrite.
        std::fs::rename(&source, dir.path().join("old.db")).unwrap();
        std::fs::rename(&replacement, &source).unwrap();
        assert!(!held.path_is_same_file(&source).unwrap());
        assert!(ensure_source_identity(&held, &source, "rebuild").is_err());
    }

    /// A target missing a bucket is the failure the catalog check exists for: a
    /// rebuild that silently dropped one would otherwise look healthy.
    #[test]
    fn a_target_missing_a_bucket_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let source = db(dir.path(), "s.db", |t| {
            t.new_bucket("plain", false).unwrap();
            t.new_bucket("other", false).unwrap();
            t.exec("plain", |txn| {
                txn.put(b"a", b"v")?;
                Ok(())
            })
            .unwrap();
        });
        let target = db(dir.path(), "t.db", |t| plain(t, &["a"]));
        let source = BTree::open_read_only(&source).unwrap();
        let error = compare_v2(&source, &target).expect_err("a missing bucket must be caught");
        assert_eq!(error.class, Class::VerifyMismatch, "{error}");
    }

    /// A target holding a different key must be refused: comparing only the
    /// bucket list would not notice.
    #[test]
    fn a_target_with_a_different_key_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let source = db(dir.path(), "s.db", |t| plain(t, &["a", "b"]));
        let target = db(dir.path(), "t.db", |t| plain(t, &["a", "z"]));
        let source = BTree::open_read_only(&source).unwrap();
        let error = compare_v2(&source, &target).expect_err("a different key must be caught");
        assert_eq!(error.class, Class::VerifyMismatch, "{error}");
    }

    /// A target holding a different value must be refused.
    #[test]
    fn a_target_with_a_different_value_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let source = db(dir.path(), "s.db", |t| {
            t.new_bucket("plain", false).unwrap();
            t.exec("plain", |txn| {
                txn.put(b"a", b"one")?;
                Ok(())
            })
            .unwrap();
        });
        let target = db(dir.path(), "t.db", |t| {
            t.new_bucket("plain", false).unwrap();
            t.exec("plain", |txn| {
                txn.put(b"a", b"two")?;
                Ok(())
            })
            .unwrap();
        });
        let source = BTree::open_read_only(&source).unwrap();
        let error = compare_v2(&source, &target).expect_err("a different value must be caught");
        assert_eq!(error.class, Class::VerifyMismatch, "{error}");
    }

    /// A target whose prefix policy was dropped must be refused, even though the
    /// bucket names and records are identical.
    #[test]
    fn a_target_with_a_flipped_prefix_policy_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let source = db(dir.path(), "s.db", |t| {
            t.new_bucket("plain", true).unwrap();
            t.exec("plain", |txn| {
                txn.put(b"aa", b"v")?;
                Ok(())
            })
            .unwrap();
        });
        let target = db(dir.path(), "t.db", |t| plain(t, &["aa"]));
        let source = BTree::open_read_only(&source).unwrap();
        let error = compare_v2(&source, &target).expect_err("a flipped policy must be caught");
        assert_eq!(error.class, Class::VerifyMismatch, "{error}");
    }

    /// Values long enough to leave the slot: `next_ref` reassembles them from
    /// value pages, and past five pages from an indirect chain. Every case above
    /// stores a one-byte inline value, so neither reassembly path ran there.
    mod overflow {
        use super::*;
        use btree_store::VALUE_PAGE_CONTENT;

        fn records() -> Vec<(&'static [u8], Vec<u8>)> {
            let wide: Vec<u8> = (0..=2 * VALUE_PAGE_CONTENT)
                .map(|index| index as u8)
                .collect();
            let chained: Vec<u8> = (0..6 * VALUE_PAGE_CONTENT)
                .map(|index| index as u8)
                .collect();
            vec![(b"a", wide), (b"b", chained)]
        }

        fn store(path: &std::path::Path, records: &[(&'static [u8], Vec<u8>)]) {
            let tree = BTree::open(path).unwrap();
            tree.new_bucket("plain", false).unwrap();
            tree.exec("plain", |txn| {
                for (key, value) in records {
                    txn.put(key, value)?;
                }
                Ok(())
            })
            .unwrap();
        }

        #[test]
        fn an_overflowing_source_compares_equal_to_its_rebuild() {
            let dir = tempfile::tempdir().unwrap();
            let records = records();
            let source = dir.path().join("s.db");
            store(&source, &records);
            let target = dir.path().join("t.db");
            store(&target, &records);
            let source = BTree::open_read_only(&source).unwrap();
            compare_v2(&source, &target).expect("an overflow value must round-trip");

            let mut seen: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            source
                .view("plain", |txn| {
                    let mut iterator = txn.iter();
                    let mut key = Vec::new();
                    let mut value = Vec::new();
                    while iterator.next_ref(&mut key, &mut value) {
                        seen.push((key.clone(), value.clone()));
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                seen,
                records
                    .iter()
                    .map(|(key, value)| (key.to_vec(), value.clone()))
                    .collect::<Vec<_>>(),
                "every overflow byte must come back, page chain and all"
            );
        }

        #[test]
        fn a_byte_changed_in_a_late_chain_page_is_refused() {
            let dir = tempfile::tempdir().unwrap();
            let records = records();
            let source = dir.path().join("s.db");
            store(&source, &records);
            let mut damaged = records.clone();
            damaged[1].1[6 * VALUE_PAGE_CONTENT - 7] ^= 0x01;
            let target = dir.path().join("t.db");
            store(&target, &damaged);
            let source = BTree::open_read_only(&source).unwrap();
            let error = compare_v2(&source, &target)
                .expect_err("a byte inside a late chain page must be caught");
            assert_eq!(error.class, Class::VerifyMismatch, "{error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use btree_store::{IDS_PER_INDIRECT_PAGE, MAX_INLINE_LEN, PAGE_SIZE, VALUE_PAGE_CONTENT};

    /// A summary whose target came out larger must report zero reclaimed bytes,
    /// not a wrapped-around number. No fixture produces that pair of sizes — a
    /// rebuild of a real source is not larger here — so the render is exercised
    /// directly, which is the only place the saturation can be observed.
    #[test]
    fn a_larger_target_reports_zero_reclaimed() {
        fn summary(source_bytes: u64, physical_bytes: u64) -> String {
            Summary {
                mode: Mode::Rebuild,
                source: std::path::PathBuf::from("s.db"),
                source_generation: 7,
                source_selected_slot: Some(MetaSlot::A),
                source_bytes,
                target: std::path::PathBuf::from("t.db"),
                target_generation: 6,
                buckets: 1,
                records: 10,
                logical_bytes: 100,
                batches: 1,
                physical_bytes,
                estimated_low: 1,
                estimated_high: 2,
                verified: true,
            }
            .render()
        }
        let grown = summary(10, 20);
        assert!(
            grown.contains("reclaimed_bytes=0\n"),
            "a target larger than its source reclaims nothing: {grown}"
        );
        assert!(
            !grown.contains("18446744073709551606"),
            "the subtraction must not wrap around: {grown}"
        );
        let shrunk = summary(20, 10);
        assert!(
            shrunk.contains("reclaimed_bytes=10\n"),
            "a smaller target reports the difference: {shrunk}"
        );
    }

    /// The value-page count must be the writer's own formula, not a guess: a value
    /// that exactly fills a page costs one page, one byte more costs two.
    #[test]
    fn value_pages_follow_the_writer_rule() {
        // Each step is checked against the running total, and the value size is
        // chosen so the added page count is unambiguous.
        let mut stats = SourceStats::default();
        stats.record(4, MAX_INLINE_LEN - 4);
        assert_eq!(
            stats.value_pages, 0,
            "a value that fits the slot costs nothing"
        );

        stats.record(4, VALUE_PAGE_CONTENT);
        assert_eq!(stats.value_pages, 1, "a value needing a page costs one");

        stats.record(4, VALUE_PAGE_CONTENT + 1);
        assert_eq!(stats.value_pages, 3, "one byte over costs two more");

        stats.record(4, 4 * VALUE_PAGE_CONTENT);
        assert_eq!(stats.value_pages, 7, "four more pages");
        assert_eq!(
            stats.indirect_pages, 0,
            "a single value of five pages still fits a direct slot"
        );

        // A value needing six pages is the first that needs an indirect chain, so
        // it is counted on its own rather than added to the smaller ones.
        let mut wide = SourceStats::default();
        wide.record(4, 6 * VALUE_PAGE_CONTENT);
        assert_eq!(wide.value_pages, 6);
        assert_eq!(
            wide.indirect_pages, 1,
            "past five value pages the chain is named by an indirect page"
        );
    }

    /// The writer keeps a value in the slot only while the **stored** key tail and
    /// the value together fit `MAX_INLINE_LEN`, so a long key can push a small
    /// value out to a page. The estimator is driven here with the whole key, which
    /// is what it charges in production; see [`SourceStats::record`] for why that
    /// is exact in a plain bucket and only an upper bound in a prefix-encoded one.
    #[test]
    fn a_long_key_pushes_its_value_out_of_the_slot() {
        let value = 200;
        assert!(
            value <= MAX_INLINE_LEN,
            "the value alone still fits the slot limit"
        );
        let mut short_key = SourceStats::default();
        short_key.record(4, value);
        assert_eq!(
            short_key.value_pages, 0,
            "a short key leaves the value inline"
        );

        let mut long_key = SourceStats::default();
        long_key.record(MAX_INLINE_LEN - value + 1, value);
        assert_eq!(
            long_key.value_pages, 1,
            "one byte of key over the limit sends the value to a page"
        );
    }

    /// A single value large enough to need many indirect pages must round the
    /// chain the way `write_index_chain` does.
    #[test]
    fn indirect_pages_round_up_like_the_writer() {
        let pages = IDS_PER_INDIRECT_PAGE as u64 + 1;
        let mut stats = SourceStats::default();
        stats.record(4, (pages * VALUE_PAGE_CONTENT as u64) as usize);
        assert_eq!(stats.value_pages, pages);
        assert_eq!(stats.indirect_pages, 2);
    }

    /// The catalog stores a bucket's name beside its entry slot, so a longer name
    /// must cost more. Without this the catalog term is silently short by exactly
    /// the name bytes.
    #[test]
    fn bucket_names_are_charged_to_the_catalog() {
        let none = SourceStats::default();
        let with_one = {
            let mut stats = SourceStats::default();
            stats.bucket(7);
            stats
        };
        let with_two = {
            let mut stats = SourceStats::default();
            stats.bucket(7);
            stats.bucket(11);
            stats
        };
        assert_eq!(with_one.buckets, 1);
        assert_eq!(with_one.catalog_name_bytes, 7);
        assert_eq!(with_two.buckets, 2);
        assert_eq!(with_two.catalog_name_bytes, 18);

        use btree_store::SLOT_SIZE;
        let empty = estimate_bytes(&none).bytes_low;
        let one = estimate_bytes(&with_one).bytes_low;
        let two = estimate_bytes(&with_two).bytes_low;
        // A bucket costs a slot, the fixed-width metadata record and its name.
        let per_bucket = SLOT_SIZE as u64 + BUCKET_METADATA_BYTES;
        assert_eq!(
            one - empty,
            per_bucket + 7,
            "one bucket costs a slot, its metadata and its name"
        );
        assert_eq!(
            two - one,
            per_bucket + 11,
            "the second bucket adds a slot, its metadata and its own name"
        );
    }

    /// The writer allocates a whole page per value page, so the estimate must price
    /// pages and not the content inside them. A value of one byte over a page costs
    /// two whole pages in the file, and the lower bound has to say so.
    #[test]
    fn value_pages_are_priced_at_whole_pages() {
        let mut stats = SourceStats::default();
        stats.record(4, VALUE_PAGE_CONTENT + 1);
        assert_eq!(stats.value_pages, 2, "one byte over is two pages");

        // The only difference between the two sources is the value page count, so
        // the difference between their estimates is exactly what a value page
        // costs. A total-based bound would also pass if node pages absorbed the
        // gap, so the assertion is on the difference.
        let mut without_value_pages = stats;
        without_value_pages.value_pages = 0;
        let delta =
            estimate_bytes(&stats).bytes_low - estimate_bytes(&without_value_pages).bytes_low;
        let two_whole_pages = 2 * PAGE_SIZE as u64;
        assert_eq!(
            delta, two_whole_pages,
            "a value page costs a whole page, not its {VALUE_PAGE_CONTENT} content bytes"
        );
        assert_eq!(
            two_whole_pages - 2 * VALUE_PAGE_CONTENT as u64,
            8,
            "four trailer bytes on each of the two pages"
        );
    }

    /// The interval must be ordered, and the counts must move it: more records
    /// can only widen it.
    #[test]
    fn the_estimate_is_an_ordered_interval_that_grows_with_the_source() {
        let mut small = SourceStats::default();
        for _ in 0..10 {
            small.record(8, 64);
        }
        let mut large = SourceStats::default();
        for _ in 0..10_000 {
            large.record(8, 64);
        }
        let small = estimate_bytes(&small);
        let large = estimate_bytes(&large);
        // Strict: a real source must produce a real interval. An earlier formula
        // collapsed the two bounds together, and a non-strict assertion could not
        // see that.
        assert!(
            small.bytes_high > small.bytes_low,
            "a real interval has width: {}..{}",
            small.bytes_low,
            small.bytes_high
        );
        assert!(small.bytes_low <= small.bytes_high);
        assert!(large.bytes_low > small.bytes_low);
        assert!(large.bytes_high > small.bytes_high);
        assert!(
            small.bytes_low >= FIXED_PAGES * PAGE_SIZE as u64,
            "the two metadata slots are always there"
        );
    }

    #[test]
    fn the_estimate_is_independent_of_record_order() {
        let mut forward = SourceStats::default();
        let mut reverse = SourceStats::default();
        for index in 0..500 {
            forward.record(4 + index % 16, (index * 37) % 9_000);
        }
        for index in (0..500).rev() {
            reverse.record(4 + index % 16, (index * 37) % 9_000);
        }
        assert_eq!(forward, reverse);
        assert_eq!(estimate_bytes(&forward), estimate_bytes(&reverse));
    }

    /// Builds a small populated database at `path` and then removes its last
    /// page, which leaves a referenced page unreadable. The audit reports that as
    /// a non-`Ok` status rather than as an outright error, which is the shape both
    /// gate tests below need.
    fn drop_last_page(path: &std::path::Path) {
        use btree_store::PAGE_SIZE;
        {
            let tree = BTree::open(path).unwrap();
            tree.new_bucket("plain", false).unwrap();
            tree.exec("plain", |txn| {
                for index in 0..2_000u32 {
                    txn.put(format!("k{index:06}").as_bytes(), vec![b'v'; 180])?;
                }
                Ok(())
            })
            .unwrap();
        }
        let bytes = std::fs::read(path).unwrap();
        std::fs::write(path, &bytes[..bytes.len() - PAGE_SIZE]).unwrap();
    }

    /// A staging file that does not survive the audit is this run's product being
    /// wrong, not the operator's source being damaged, so it must be reported as
    /// an invalid target rather than as an unhealthy database.
    #[test]
    fn a_damaged_staging_file_is_reported_as_an_invalid_target() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intact.db");
        {
            let tree = BTree::open(&path).unwrap();
            tree.new_bucket("plain", false).unwrap();
            tree.exec("plain", |txn| {
                txn.put(b"a", b"v")?;
                Ok(())
            })
            .unwrap();
        }
        assert!(
            verify_target(&path).is_ok(),
            "an intact file passes the staging gate"
        );

        let damaged = dir.path().join("damaged.db");
        drop_last_page(&damaged);
        let error = verify_target(&damaged).expect_err("a damaged staging file must not publish");
        assert_eq!(error.class, Class::TargetInvalid, "{error}");
        assert_eq!(error.phase, "target", "{error}");
    }

    /// The two audits are the same call on different subjects, so they must not
    /// share a class: one damaged file is `failed` as a source and
    /// `target-invalid` as a target. A single shared mapping would tell the
    /// operator their source is broken when that source passed moments earlier.
    #[test]
    fn the_staging_gate_does_not_reuse_the_source_gates_classes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subject.db");
        drop_last_page(&path);

        let report = check_path(&path).expect("a missing page is a status, not an error");
        assert_ne!(report.status, CheckStatus::Ok);
        assert_eq!(
            status_class(report.status),
            Class::CheckFailed,
            "as a source the same damage is still classified against the source"
        );
        assert_eq!(
            verify_target(&path)
                .expect_err("the staging gate refuses it")
                .class,
            Class::TargetInvalid
        );
    }

    /// The audit must run inside the shared lock the open took, and the traversal
    /// after it: auditing before opening would leave a window in which another
    /// process commits, and the rebuild would then be of a generation nobody
    /// audited.
    ///
    /// The probe records, at each step, whether an exclusive lock on the source is
    /// refused yet. That is the state the ordering has to produce, so this test
    /// fails if the real calls are reordered - unlike a test that only compares the
    /// step names the probe was handed, which the function it lives in writes.
    #[test]
    fn the_read_stage_audits_before_it_traverses() {
        use parking_lot::Mutex;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("order.db");
        {
            let tree = BTree::open(&path).unwrap();
            tree.new_bucket("plain", false).unwrap();
            tree.exec("plain", |txn| {
                txn.put(b"a", b"v")?;
                Ok(())
            })
            .unwrap();
        }

        let steps = Mutex::new(Vec::new());
        let (handle, stage) = read_stage_probing(&path, &|step| {
            // A second descriptor: the store's own shared lock is on this path, so
            // an exclusive request from here can only be refused while it is held.
            let held = std::fs::File::open(&path).unwrap().try_lock().is_err();
            steps.lock().push((step.to_string(), held));
        })
        .expect("a healthy source passes the read stage");
        assert_eq!(
            *steps.lock(),
            vec![
                ("open".to_string(), true),
                ("audit".to_string(), true),
                ("traverse".to_string(), true)
            ],
            "every step must run under the shared lock the open took"
        );
        assert_eq!(stage.stats.records, 1);
        drop(handle);
    }
}
