//! The migration flow: checks, staging, rebuild, comparison, publication.
//!
//! Phase 1 validates the source generation before anything is created; phase 2
//! rebuilds it into a private staging file with the normal v2 writer; phase 3
//! reopens that file with a *fresh* instance and compares every key and value
//! byte against the source; phase 4 publishes by replacing the destination.

use crate::cli::{Class, CliError, MigrateArgs, SOURCE_VERSIONS, Session, source_versions_list};
pub(crate) use crate::staging::{
    BATCH_BYTES, Staging, batch_is_full, cleanup_staging, injected_fault, publish,
    removes_staging_after, staging_is_present,
};
use crate::v1::{V1Bucket, V1Entry, V1Error, V1Reader, V1Walk};
use btree_store::BTree;
use std::fs::File;
use std::path::PathBuf;

/// The success summary printed on stdout. `verified` means the key/value
/// comparison passed; it does not claim a separate full-graph scan.
pub(crate) struct Summary {
    pub(crate) source: PathBuf,
    pub(crate) source_version: u32,
    pub(crate) source_generation: u64,
    pub(crate) source_ignored_invalid_candidate: bool,
    pub(crate) target: PathBuf,
    pub(crate) target_generation: u64,
    pub(crate) buckets: usize,
    pub(crate) records: u64,
    /// Logical key+value bytes written.
    pub(crate) logical_bytes: u64,
    /// Target transactions committed: the batch thresholds, observed.
    pub(crate) batches: u64,
    pub(crate) physical_bytes: u64,
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "source={}", self.source.display())?;
        writeln!(f, "source_version={}", self.source_version)?;
        writeln!(f, "source_generation={}", self.source_generation)?;
        writeln!(
            f,
            "source_ignored_invalid_candidate={}",
            self.source_ignored_invalid_candidate
        )?;
        writeln!(f, "target={}", self.target.display())?;
        writeln!(f, "target_version={}", btree_store::FORMAT_VERSION)?;
        writeln!(f, "target_generation={}", self.target_generation)?;
        writeln!(f, "buckets={}", self.buckets)?;
        writeln!(f, "records={}", self.records)?;
        writeln!(f, "logical_bytes={}", self.logical_bytes)?;
        writeln!(f, "batches={}", self.batches)?;
        writeln!(f, "physical_bytes={}", self.physical_bytes)?;
        writeln!(f, "verified=true")
    }
}

pub(crate) fn run(args: MigrateArgs) -> Result<Summary, CliError> {
    let session = Session::open(&args.source, &args.destination)?;

    let source_file = session.source().try_clone().map_err(|error| {
        CliError::new("source", Class::Io, format!("duplicating the fd: {error}"))
    })?;
    let mut reader = open_source(session.source_version, source_file)?;
    let mut walk = V1Walk::default();
    let buckets = reader.catalog(&mut walk).map_err(map_source)?;
    for bucket in &buckets {
        if bucket.root != 0 {
            reader
                .walk(bucket.root, &mut walk, None)
                .map_err(map_source)?;
        }
    }
    reader.check_ownership(&walk).map_err(map_source)?;

    let staging = session.create_staging()?;
    let outcome = (|| -> Result<Summary, CliError> {
        let written = rebuild(&mut reader, &buckets, &staging)?;

        let target_generation = compare(&mut reader, &buckets, &staging)?;

        // Measured before publication, so nothing after the rename can fail and
        // report a published target as an ordinary failure.
        let physical_bytes = std::fs::metadata(&staging.path)
            .map_err(|error| {
                CliError::new(
                    "staging",
                    Class::Io,
                    format!("stat {}: {error}", staging.path.display()),
                )
            })?
            .len();

        // Phase 4: publish by replacing the destination.
        publish(
            &staging,
            &session.destination,
            #[cfg(unix)]
            session.source_mode,
        )?;

        Ok(Summary {
            source: session.source_path.clone(),
            source_version: session.source_version,
            source_generation: reader.meta.seq,
            source_ignored_invalid_candidate: reader.ignored_invalid_candidate,
            target: session.destination.clone(),
            target_generation,
            buckets: buckets.len(),
            records: written.records,
            logical_bytes: written.logical_bytes,
            batches: written.batches,
            physical_bytes,
        })
    })();

    match outcome {
        Ok(summary) => Ok(summary),
        Err(error) => {
            if removes_staging_after(error.class) {
                cleanup_staging(&staging);
            }
            let present = staging_is_present(&staging.path);
            Err(error.with_staging(&staging.path, present))
        }
    }
}

/// Opens the decoder for one accepted source version.
///
/// [`SOURCE_VERSIONS`] decides which versions may arrive here and this function
/// decides which decoder reads them; `every_accepted_source_version_has_a_decoder`
/// walks the list so the two cannot drift apart (a version accepted without a decoder
/// would silently lose its migration path).
fn open_source(version: u32, file: File) -> Result<V1Reader, CliError> {
    match version {
        1 => V1Reader::open(file).map_err(map_source),
        other => {
            debug_assert!(
                !SOURCE_VERSIONS.contains(&other),
                "format version {other} is listed in SOURCE_VERSIONS but has no decoder"
            );
            Err(CliError::new(
                "source",
                Class::SourceVersionUnsupported,
                format!(
                    "format version {other} has no decoder in this build; it migrates {}",
                    source_versions_list()
                ),
            ))
        }
    }
}

/// [`open_source`] for the CLI's own test, which walks `SOURCE_VERSIONS`.
#[cfg(test)]
pub(crate) fn open_source_for_test(version: u32, file: File) -> Result<V1Reader, CliError> {
    open_source(version, file)
}

fn map_source(error: V1Error) -> CliError {
    let where_ = match error.pid {
        Some(pid) => format!("page {pid}"),
        None => "the database".to_string(),
    };
    // reports it as `V1_IO` so it can be told apart from the corruption codes,
    let class = if error.code == "V1_IO" {
        Class::Io
    } else {
        Class::SourceCorrupt
    };
    CliError::new(
        "source",
        class,
        format!(
            "{} ({where_}; check: {}): {}",
            error.code, error.check, error.detail
        ),
    )
}

struct Written {
    records: u64,
    logical_bytes: u64,
    batches: u64,
}

/// Reads the source in batches and writes them through normal v2 transactions.
///
/// A batch is prepared *before* it enters a target transaction, and a record
/// larger than the byte threshold travels alone.
fn rebuild(
    reader: &mut V1Reader,
    buckets: &[V1Bucket],
    staging: &Staging,
) -> Result<Written, CliError> {
    injected_fault("rebuild").map_err(|error| {
        CliError::new("rebuild", Class::Io, format!("staging database: {error}"))
    })?;
    let tree = BTree::open(&staging.path).map_err(|error| {
        CliError::new(
            "rebuild",
            Class::Io,
            format!("opening the staging database: {error:?}"),
        )
    })?;

    for bucket in buckets {
        tree.new_bucket(&bucket.name, bucket.prefix_encoded())
            .map_err(|error| {
                CliError::new(
                    "rebuild",
                    Class::Io,
                    format!("creating bucket {:?}: {error:?}", bucket.name),
                )
            })?;
    }

    let mut written = Written {
        records: 0,
        logical_bytes: 0,
        batches: 0,
    };
    for bucket in buckets {
        if bucket.root == 0 {
            continue;
        }
        let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut batch_bytes = 0u64;
        let mut walk = V1Walk::default();

        let mut write_failure: Option<CliError> = None;
        let mut visit = |reader: &mut V1Reader, entry: &V1Entry| -> Result<(), V1Error> {
            if write_failure.is_some() {
                return Ok(());
            }
            let value = reader.read_value(entry)?;
            let size = entry.key.len() as u64 + value.len() as u64;
            let full = batch_is_full(batch.len(), batch_bytes, size);
            if full
                && let Err(error) = flush(
                    &tree,
                    &bucket.name,
                    &mut batch,
                    &mut batch_bytes,
                    &mut written,
                )
            {
                write_failure = Some(error);
                return Ok(());
            }
            batch_bytes += size;
            batch.push((entry.key.clone(), value));
            if size > BATCH_BYTES
                && let Err(error) = flush(
                    &tree,
                    &bucket.name,
                    &mut batch,
                    &mut batch_bytes,
                    &mut written,
                )
            {
                write_failure = Some(error);
            }
            Ok(())
        };

        reader
            .walk(bucket.root, &mut walk, Some(&mut visit))
            .map_err(map_source)?;
        if let Some(error) = write_failure {
            return Err(error);
        }
        flush(
            &tree,
            &bucket.name,
            &mut batch,
            &mut batch_bytes,
            &mut written,
        )?;
    }

    drop(tree);
    Ok(written)
}

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
    let items = std::mem::take(batch);
    *batch_bytes = 0;
    let count = items.len() as u64;
    let bytes: u64 = items
        .iter()
        .map(|(key, value)| key.len() as u64 + value.len() as u64)
        .sum();
    tree.exec(bucket, |txn| {
        for (key, value) in &items {
            txn.put(key, value)?;
        }
        Ok(())
    })
    .map_err(|error| {
        CliError::new(
            "rebuild",
            Class::Io,
            format!("writing {count} records into {bucket:?}: {error:?}"),
        )
    })?;
    written.records += count;
    written.logical_bytes += bytes;
    written.batches += 1;
    Ok(())
}

fn compare(
    reader: &mut V1Reader,
    buckets: &[V1Bucket],
    staging: &Staging,
) -> Result<u64, CliError> {
    injected_fault("compare").map_err(|error| {
        CliError::new("compare", Class::Io, format!("target comparison: {error}"))
    })?;
    let target = BTree::open(&staging.path).map_err(|error| {
        CliError::new(
            "compare",
            Class::TargetInvalid,
            format!("reopening {}: {error:?}", staging.path.display()),
        )
    })?;

    let policies = target.buckets_with_policy().map_err(|error| {
        CliError::new(
            "compare",
            Class::TargetInvalid,
            format!("reading the target catalog: {error:?}"),
        )
    })?;
    if policies.len() != buckets.len() {
        return Err(mismatch(format!(
            "the target has {} buckets, the source has {}",
            policies.len(),
            buckets.len()
        )));
    }
    for (bucket, (name, prefix)) in buckets.iter().zip(&policies) {
        if &bucket.name != name {
            return Err(mismatch(format!(
                "bucket name mismatch: source {:?}, target {name:?}",
                bucket.name
            )));
        }
        if bucket.prefix_encoded() != *prefix {
            return Err(mismatch(format!(
                "bucket {:?} policy mismatch: source prefix={}, target prefix={prefix}",
                bucket.name,
                bucket.prefix_encoded()
            )));
        }
    }

    for bucket in buckets {
        let mut mismatch_detail: Option<String> = None;
        let mut source_error: Option<V1Error> = None;
        let mut walk = V1Walk::default();
        let mut index = 0u64;

        target
            .view(&bucket.name, |txn| {
                let mut iter = txn.iter();
                let (mut key, mut value) = (Vec::new(), Vec::new());
                let mut visit = |reader: &mut V1Reader, entry: &V1Entry| -> Result<(), V1Error> {
                    if !iter.next_ref(&mut key, &mut value) {
                        mismatch_detail = Some(format!(
                            "bucket {:?} is missing records from index {index}",
                            bucket.name
                        ));
                        return Ok(());
                    }
                    if key != entry.key {
                        mismatch_detail = Some(format!(
                            "bucket {:?} differs at record {index}: source key {:?}, target key {:?}",
                            bucket.name, entry.key, key
                        ));
                        return Ok(());
                    }
                    let expected = reader.read_value(entry)?;
                    if value != expected {
                        mismatch_detail = Some(format!(
                            "bucket {:?} value differs at key {:?} ({} vs {} bytes)",
                            bucket.name,
                            entry.key,
                            expected.len(),
                            value.len()
                        ));
                        return Ok(());
                    }
                    index += 1;
                    Ok(())
                };
                if let Err(error) = reader.walk(bucket.root, &mut walk, Some(&mut visit)) {
                    source_error = Some(error);
                }
                if mismatch_detail.is_none() && iter.next_ref(&mut key, &mut value) {
                    mismatch_detail = Some(format!(
                        "bucket {:?} has extra records starting at index {index}",
                        bucket.name
                    ));
                }
                Ok::<_, btree_store::Error>(())
            })
            .map_err(|error| {
                CliError::new(
                    "compare",
                    Class::TargetInvalid,
                    format!("reading bucket {:?}: {error:?}", bucket.name),
                )
            })?;

        if let Some(error) = source_error {
            return Err(map_source(error));
        }
        if let Some(detail) = mismatch_detail {
            return Err(mismatch(detail));
        }
    }

    Ok(target.current_seq())
}

fn mismatch(detail: String) -> CliError {
    CliError::new("compare", Class::VerifyMismatch, detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::staging::BATCH_RECORDS;

    #[test]
    fn staging_survives_a_publication_stage_failure() {
        assert!(!removes_staging_after(Class::PublishedDurabilityUnknown));
        assert!(removes_staging_after(Class::VerifyMismatch));
        assert!(removes_staging_after(Class::Io));
    }

    #[test]
    fn staging_presence_is_only_reported_absent_when_it_is_proved_absent() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("out.db.migrate-v2.1.0.tmp");
        assert!(!staging_is_present(&path), "a missing name is absent");

        std::fs::write(&path, b"staged").unwrap();
        assert!(
            staging_is_present(&path),
            "an existing staging file is present"
        );
        std::fs::remove_file(&path).unwrap();
        assert!(
            !staging_is_present(&path),
            "a removed staging file is absent"
        );

        #[cfg(unix)]
        {
            let dangling = dir.path().join("dangling.tmp");
            std::os::unix::fs::symlink(dir.path().join("nowhere"), &dangling).unwrap();
            assert!(
                staging_is_present(&dangling),
                "a dangling symlink is present, not removed"
            );
        }
    }

    #[test]
    fn batch_policy_uses_the_byte_and_record_thresholds() {
        // batch never splits, and the batch is flushed once it is in.
        assert!(!batch_is_full(0, 0, BATCH_BYTES + 1));
        assert!(!batch_is_full(0, 0, u64::MAX)); // an empty batch never splits
        assert!(!batch_is_full(1, 0, 0));
        assert!(!batch_is_full(1, BATCH_BYTES - 1, 1)); // exactly at the threshold is fine
        assert!(batch_is_full(1, BATCH_BYTES, 1)); // crossing it flushes first

        assert!(!batch_is_full(BATCH_RECORDS - 1, 1, 1));
        assert!(batch_is_full(BATCH_RECORDS, 1, 1));
    }
}
