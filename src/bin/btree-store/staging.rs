//! The staging file and the publication protocol.
//!
//! `migrate` builds into a private staging file and publishes it by replacing the
//! destination, so the 0600 creation, the failure classification and the publish
//! sequence live here. Moving them out of `cli`/`migrate` is the only
//! reason this module exists: a second copy of this protocol would be a second
//! crash-safety contract. The staging *name* stays the caller's choice, passed in
//! as a suffix.

use crate::cli::{Class, CliError, STAGING_COLLISION_LIMIT};
use btree_store::FORMAT_VERSION;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// The staged output: a private name this run owns until publication. The file is
/// created and written by the caller's writer; publication re-opens it, syncs it
/// and consumes the name in one rename.
#[derive(Debug)]
pub(crate) struct Staging {
    pub(crate) path: PathBuf,
}

impl Staging {
    /// Narrows the output permissions once every writer handle is closed:
    /// `source_mode & 0o600`, never a mode that removes readability entirely.
    #[cfg(unix)]
    pub(crate) fn narrow_permissions(&self, source_mode: u32) -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let narrowed = source_mode & 0o600;
        // A source mode with no owner read (write-only, or nothing at all) must not
        // produce an output its owner cannot read, so owner read is added back.
        let mode = if narrowed & 0o400 == 0 {
            0o400
        } else {
            narrowed
        };
        std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(mode))
    }
}

/// `suffix` is the caller's own middle segment, so this module holds no naming rule
/// of its own: `migrate` passes `staging_suffix()` and `compact` passes its own.
pub(crate) fn create_staging_in(
    parent: &Path,
    destination_name: &std::ffi::OsStr,
    pid: u32,
    suffix: &str,
) -> Result<Staging, CliError> {
    // Source validation can take time; recheck immediately before creating output.
    check_replaceable_destination(&parent.join(destination_name))?;
    for counter in 0..STAGING_COLLISION_LIMIT {
        let mut candidate = destination_name.to_os_string();
        candidate.push(suffix);
        candidate.push(format!(".{pid}.{counter}.tmp"));
        let path = parent.join(candidate);
        match create_private(&path) {
            Ok(file) => {
                // file is ours and not yet published, so an ordinary failure
                let init = injected_fault("staging-init")
                    .map_err(|error| format!("{error:?}"))
                    .and_then(|()| {
                        btree_store::BTree::open(&path).map_err(|error| format!("{error:?}"))
                    });
                let tree = match init {
                    Ok(tree) => tree,
                    Err(error) => {
                        drop(file);
                        let _ = std::fs::remove_file(&path);
                        let present = staging_is_present(&path);
                        return Err(CliError::new(
                            "staging",
                            Class::Io,
                            format!("initializing the staging database: {error}"),
                        )
                        .with_staging(&path, present));
                    }
                };
                drop(tree);
                drop(file);
                return Ok(Staging { path });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(CliError::io("staging", "create", &path, error)),
        }
    }

    Err(CliError::new(
        "staging",
        Class::Io,
        format!("no free staging name after {STAGING_COLLISION_LIMIT} attempts"),
    ))
}

/// The staging name's middle segment for `migrate`, which writes the current
/// format version. `compact` passes its own suffix so the two commands never share
/// one naming rule.
pub(crate) fn staging_suffix() -> String {
    format!(".migrate-v{FORMAT_VERSION}")
}

#[cfg(unix)]
pub(crate) fn create_private(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
pub(crate) fn create_private(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
}

// Reject aliases of the source so publication cannot replace its input.
// Missing paths are diagnosed by the source open or destination checks.
pub(crate) fn refuse_self_destination(destination: &Path, source: &Path) -> Result<(), CliError> {
    let (Ok(destination_meta), Ok(source_meta)) =
        (std::fs::metadata(destination), std::fs::metadata(source))
    else {
        return Ok(());
    };
    #[cfg(unix)]
    let same = {
        use std::os::unix::fs::MetadataExt;
        (destination_meta.dev(), destination_meta.ino()) == (source_meta.dev(), source_meta.ino())
    };
    #[cfg(not(unix))]
    let same = {
        let _ = (destination_meta, source_meta);
        let resolve =
            |path: &Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        resolve(destination) == resolve(source)
    };
    if same {
        return Err(CliError::new(
            "arguments",
            Class::Arg,
            format!(
                "{} is the source itself; the destination must be another file",
                destination.display()
            ),
        ));
    }
    Ok(())
}

/// The destination must be replaceable by a file: its parent must resolve to a
/// directory, and the name itself must not be a directory. Both are properties of
/// the path only, so this runs before the source is read and no target is looked
/// at for its contents.
///
/// The name is inspected without following it, because a rename replaces the name:
/// a symlink is replaceable whatever it points at, and only a real directory is not.
pub(crate) fn check_replaceable_destination(destination: &Path) -> Result<(), CliError> {
    let parent = match destination.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let parent = std::fs::canonicalize(parent)
        .map_err(|error| CliError::io("target", "resolve parent of", destination, error))?;

    if let Ok(metadata) = std::fs::metadata(&parent)
        && !metadata.is_dir()
    {
        return Err(CliError::new(
            "target",
            Class::Io,
            format!("{} is not a directory", parent.display()),
        ));
    }
    if let Ok(metadata) = std::fs::symlink_metadata(destination)
        && metadata.is_dir()
    {
        return Err(CliError::new(
            "target",
            Class::Io,
            format!("{} is a directory, not a file", destination.display()),
        ));
    }
    Ok(())
}

/// Whether the staging file is still there, as far as the filesystem can say.
///
/// Only `NotFound` proves it is gone: a stat that fails for any other reason (an
/// unsearchable parent, a failing device) is not evidence that the file was removed,
/// and reporting `(already removed)` there would send the operator away from a
/// leftover that has to be deleted by hand. `symlink_metadata` is used because the
/// question is about the name, not what it resolves to.
pub(crate) fn staging_is_present(path: &Path) -> bool {
    !matches!(
        std::fs::symlink_metadata(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    )
}

/// Best-effort removal of this run's own staging file. An abort or a signal
/// can still leave it behind; the tool never removes anything it did not
/// create, and never resumes from a leftover.
pub(crate) fn cleanup_staging(staging: &Staging) {
    let _ = std::fs::remove_file(&staging.path);
}

/// Whether a failure may remove this run's staging file.
///
/// The one class that keeps it is the one reported *after* the swap: the rename has
/// already consumed the staged name by then, so there is nothing left to remove
/// either way, and the class says the destination's directory entry is what may not
/// be durable. Every earlier failure leaves the staged file behind for the caller
/// to remove.
pub(crate) fn removes_staging_after(class: Class) -> bool {
    class != Class::PublishedDurabilityUnknown
}

/// Publishes the staged file as the destination, replacing whatever held that name.
///
/// The staged bytes are synced first, then the staged file is renamed onto the
/// destination: on one filesystem that is one atomic step, so the destination is
/// either its old self or the complete verified staged file, never a half-written
/// one, and the staging name is consumed by the same step - there is no cleanup
/// left that could fail. A failure before the rename leaves the destination exactly
/// as it was and the staged file for the caller to remove; a failure after it means
/// the file is in place but its directory entry may not be durable, which is the
/// only `published-*` class this can report.
pub(crate) fn publish(
    staging: &Staging,
    destination: &Path,
    #[cfg(unix)] source_mode: u32,
) -> Result<(), CliError> {
    // Hold a descriptor with an exclusive lock across the final sync. The handle
    // is opened *before* the permissions are narrowed, because once the output is
    // read-only it could no longer be opened for writing.
    let handle = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&staging.path)
        .map_err(|error| {
            CliError::new(
                "publish",
                Class::Io,
                format!("opening {}: {error}", staging.path.display()),
            )
        })?;
    handle.try_lock().map_err(|error| {
        CliError::new(
            "publish",
            Class::Io,
            format!("locking {}: {error}", staging.path.display()),
        )
    })?;

    #[cfg(unix)]
    staging.narrow_permissions(source_mode).map_err(|error| {
        CliError::new(
            "publish",
            Class::Io,
            format!("narrowing {}: {error}", staging.path.display()),
        )
    })?;

    let sync_result = injected_fault("sync").and_then(|()| handle.sync_all());
    sync_result.map_err(|error| {
        CliError::new(
            "publish",
            Class::Io,
            format!("syncing {}: {error}", staging.path.display()),
        )
    })?;

    let swap = injected_fault("rename").and_then(|()| std::fs::rename(&staging.path, destination));
    swap.map_err(|error| {
        CliError::new(
            "publish",
            Class::Io,
            format!(
                "replacing {} with {}: {error}",
                destination.display(),
                staging.path.display()
            ),
        )
    })?;

    let dir_sync = injected_fault("dirsync1").and_then(|()| sync_parent_dir(destination));
    if let Err(error) = dir_sync {
        return Err(CliError::new(
            "publish",
            Class::PublishedDurabilityUnknown,
            format!(
                "{} is in place and may not be durable: {error}",
                destination.display()
            ),
        ));
    }

    drop(handle);
    Ok(())
}

/// Flushes a directory entry of `path`.
///
/// Windows tolerates unsupported directory flushes; other failures report that
/// the published target's directory entry has not been confirmed durable.
fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    sync_dir(parent)
}

pub(crate) const BATCH_BYTES: u64 = 8 << 20;
pub(crate) const BATCH_RECORDS: usize = 4096;

/// Whether the pending batch must be committed before `size` more bytes are
/// added: the byte threshold, the record count, or a single record that is
/// larger than the byte threshold on its own.
pub(crate) fn batch_is_full(records: usize, bytes: u64, size: u64) -> bool {
    records >= BATCH_RECORDS || (records > 0 && bytes + size > BATCH_BYTES)
}

/// Injects a failure at a named CLI-owned boundary.
///
/// Compiled only with the `test-fault-injection` feature, which is never on for a
/// released binary; it exists so the publication failure ladder can be exercised
/// deterministically instead of being argued about in prose.
#[cfg(feature = "test-fault-injection")]
pub(crate) fn injected_fault(point: &str) -> std::io::Result<()> {
    match std::env::var("BTREE_STORE_MIGRATE_FAULT") {
        Ok(requested) if requested == point => Err(std::io::Error::other(format!(
            "injected failure at {point}"
        ))),
        _ => Ok(()),
    }
}

#[cfg(not(feature = "test-fault-injection"))]
#[inline(always)]
pub(crate) fn injected_fault(_point: &str) -> std::io::Result<()> {
    Ok(())
}

fn has_trailing_separator(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().last() == Some(&b'/')
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        // `encode_wide` yields u16 code points, so the separators are the literals
        // 0x2f (`/`) and 0x5c (`\`).
        matches!(
            path.as_os_str().encode_wide().last(),
            Some(0x2f) | Some(0x5c) // '/' or '\\'
        )
    }
    #[cfg(not(any(unix, windows)))]
    {
        path.to_string_lossy().ends_with(std::path::MAIN_SEPARATOR)
    }
}

fn is_path_separator(character: char) -> bool {
    #[cfg(windows)]
    {
        character == '/' || character == '\\'
    }
    #[cfg(not(windows))]
    {
        character == std::path::MAIN_SEPARATOR
    }
}

/// Whether the destination's spelling ends in a `.` segment (after any trailing
/// separators), which names a directory rather than the file to publish.
/// `Path::file_name` normalizes that segment away - it reports `"x"` for `"x/."` - so
/// the spelling itself has to be inspected.
///
/// The lossy rendering is sound for this question: the `.` and the separators around
/// it are ASCII, and a replaced byte is never one of them, so no path is accepted or
/// rejected because of the substitution.
fn ends_with_curdir(path: &Path) -> bool {
    let text = path.to_string_lossy();
    let trimmed = text.trim_end_matches(is_path_separator);
    trimmed == "."
        || trimmed
            .strip_suffix('.')
            .is_some_and(|head| head.ends_with(is_path_separator))
}

/// The destination spelling must name a file, before anything is created.
///
/// `label` is how the operator spelled it - `--output` for `migrate`, the
/// `destination` positional for `compact` - so the message quotes what they typed
/// rather than an option their command does not have.
pub(crate) fn check_output_name(label: &str, destination: &Path) -> Result<(), CliError> {
    if destination.file_name().is_none()
        || has_trailing_separator(destination)
        || ends_with_curdir(destination)
    {
        return Err(CliError::new(
            "arguments",
            Class::Arg,
            format!(
                "{label} {:?} must name a file: it is empty, ends with a path separator, or ends in a `.` segment",
                destination
            ),
        ));
    }
    Ok(())
}

/// Flushes the parent directory entry of a published target.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(windows)]
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_INVALID_HANDLE: i32 = 6;
    const ERROR_INVALID_FUNCTION: i32 = 1;
    const ERROR_NOT_SUPPORTED: i32 = 50;

    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(dir)?;

    // Windows filesystems commonly allow opening a directory but reject
    // FlushFileBuffers on its handle. This is the same platform limitation
    // handled by the store writer: the file itself was already flushed, and
    // these errors mean directory durability cannot be queried here.
    match directory.sync_all() {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(
                    ERROR_ACCESS_DENIED
                        | ERROR_INVALID_HANDLE
                        | ERROR_INVALID_FUNCTION
                        | ERROR_NOT_SUPPORTED
                )
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// A platform that cannot sync a directory reports the error rather than skipping
/// the step: a published target whose directory entry is not durable is exactly
/// what this call exists to prevent.
#[cfg(not(any(unix, windows)))]
fn sync_dir(_dir: &Path) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "this platform cannot sync a directory entry",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The refusal moved out of `run_migrate` when the protocol was shared; for
    /// `migrate` it must still be the message it has always printed, and for
    /// `compact` it must name the positional the operator actually wrote.
    #[test]
    fn the_output_name_refusal_keeps_its_wording() {
        for destination in ["", "out/", "out/.", ".", "out/db/."] {
            let error = check_output_name("--output", Path::new(destination))
                .expect_err("a spelling that does not name a file must be refused");
            assert_eq!(error.phase, "arguments");
            assert_eq!(error.class.as_str(), "arg");
            assert_eq!(
                error.detail,
                format!(
                    "--output {destination:?} must name a file: it is empty, ends with a path separator, or ends in a `.` segment"
                ),
                "{destination:?}"
            );

            let positional = check_output_name("the destination", Path::new(destination))
                .expect_err("the same spelling is refused for compact");
            assert_eq!(
                positional.detail,
                format!(
                    "the destination {destination:?} must name a file: it is empty, ends with a path separator, or ends in a `.` segment"
                ),
                "{destination:?}"
            );
        }
        assert!(check_output_name("--output", Path::new("out.db")).is_ok());
    }

    /// The staged name still records the target format, so a half-published
    /// staging file is never mistaken for a database of another version.
    #[test]
    fn the_staging_name_is_private_and_versioned() {
        let dir = tempfile::TempDir::new().unwrap();
        let staging = create_staging_in(
            dir.path(),
            std::ffi::OsStr::new("out.db"),
            std::process::id(),
            &staging_suffix(),
        )
        .unwrap();
        let name = staging
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(name.starts_with("out.db.migrate-v"), "{name}");
        assert!(name.ends_with(".tmp"), "{name}");
        assert_eq!(staging_suffix(), format!(".migrate-v{FORMAT_VERSION}"));
    }
}
