//! Command surface, error contract, source locking and staging lifecycle.
//!
//! This is the only place that decides the process interface: argument grammar,
//! exit codes, the `error: <phase>: <class>: <detail>` contract and the source
//! session. The command flows live in `migrate`, `check` and `compact`; all three
//! use the error classes and exit codes defined here. The staged output file and
//! the publication protocol are owned by `staging`.

use crate::compact::{CompactArgs, Mode};
use crate::staging::Staging;
use btree_store::{FORMAT_VERSION, MAGIC};
use clap::ArgAction;
use clap::builder::OsStringValueParser;
use clap::{Arg, Command};
use std::ffi::OsString;
use std::fmt;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Argument parse failed: usage goes to stderr, nothing was touched.
pub(crate) const EXIT_USAGE: i32 = 2;
/// A migration/verification/publication failure: reported as an `error:` line.
pub(crate) const EXIT_FAILURE: i32 = 1;

/// Shared-lock acquisition budget, matching the library's `OPEN_LOCK_TIMEOUT`.
const LOCK_TIMEOUT: Duration = Duration::from_secs(1);
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(1);
/// Staging-name collisions to try before giving up
/// (`<dest>.migrate-v<version>.<pid>.<n>.tmp`, the middle segment being `staging_suffix()`).
pub(crate) const STAGING_COLLISION_LIMIT: u32 = 1024;

/// The format versions this build can read as a migration source, oldest first.
///
/// It is the single source of truth: `Session::open` accepts exactly these,
/// `migrate::open_source` dispatches on them, and the refusal message names them.
/// Adding a version means adding its decoder (a module of its own, like `v1`) and
/// one entry here - `every_accepted_source_version_has_a_decoder` below walks this list
/// and fails if a member has no decoder, so a forgotten one cannot degrade into a
/// silent `unsupported`.
pub(crate) const SOURCE_VERSIONS: &[u32] = &[1];
/// Meta discovery prefix: magic u64, seq u64, format version u32, all LE.
/// The fixed-width meta record: the discovery contract shared by every format
/// version, and the only thing readable before the version is known.
const META_RECORD_LEN: usize = 40;
/// Offset of the record's CRC32C, which covers everything before it.
const META_CHECKSUM_OFFSET: usize = 36;
const PAGE_SIZE: u64 = 4096;

/// Error classes of the CLI contract. Tests assert these tokens, never prose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    /// An argument that is syntactically fine but semantically unacceptable: a
    /// `--to` value that is not this build's format version, or a destination that
    /// names no file. Exit code 2, like clap's own usage errors, but with the same
    /// `error: <phase>: <class>: <detail>` shape.
    Arg,
    Io,
    CheckFailed,
    CheckIncomplete,
    LockBusy,
    SourceVersionUnsupported,
    /// Two meta slots that are both valid records but record different format
    /// versions: the file cannot be attributed to one contract, so no decoder
    /// may be chosen for it.
    SourceVersionMixed,
    SourceAlreadyCurrent,
    SourceCorrupt,
    TargetInvalid,
    PublishedDurabilityUnknown,
    VerifyMismatch,
}

impl Class {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Class::Arg => "arg",
            Class::Io => "io",
            Class::CheckFailed => "failed",
            Class::CheckIncomplete => "incomplete",
            Class::LockBusy => "lock-busy",
            Class::SourceVersionUnsupported => "source-version-unsupported",
            Class::SourceVersionMixed => "source-version-mixed",
            Class::SourceAlreadyCurrent => "source-already-current",
            Class::SourceCorrupt => "source-corrupt",
            Class::TargetInvalid => "target-invalid",
            Class::PublishedDurabilityUnknown => "published-durability-unknown",
            Class::VerifyMismatch => "verify-mismatch",
        }
    }
}

#[derive(Debug)]
pub(crate) struct CliError {
    pub(crate) phase: &'static str,
    pub(crate) class: Class,
    pub(crate) detail: String,
}

impl CliError {
    pub(crate) fn new(phase: &'static str, class: Class, detail: impl Into<String>) -> Self {
        Self {
            phase,
            class,
            detail: detail.into(),
        }
    }

    pub(crate) fn io(phase: &'static str, action: &str, path: &Path, error: io::Error) -> Self {
        Self::new(
            phase,
            Class::Io,
            format!("{action} {}: {error}", path.display()),
        )
    }

    /// Appends the staging path, which callers need in order to clean up by hand
    /// when the tool could not, together with whether the file is still there: a run
    /// that removed its own staging before the failure must not send the operator
    /// looking for a name that is already gone.
    /// The path is rendered with `{:?}`: `Display` substitutes U+FFFD for bytes
    /// that are not valid UTF-8, and a lossy name cannot be used to find (or
    /// delete) the leftover it reports.
    pub(crate) fn with_staging(mut self, staging: &Path, present: bool) -> Self {
        let state = if present {
            "present"
        } else {
            "already removed"
        };
        self.detail = format!("{}; staging {staging:?} ({state})", self.detail);
        self
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error: {}: {}: {}",
            self.phase,
            self.class.as_str(),
            self.detail
        )
    }
}

/// Parsed `migrate` invocation.
pub(crate) struct MigrateArgs {
    pub(crate) source: PathBuf,
    pub(crate) destination: PathBuf,
}

fn command() -> Command {
    Command::new("btree-store")
        // The command surface is deliberately minimal: no `--version`, no extra
        // subcommands, no generated help/version surface beyond `--help`/`-h`.
        .disable_version_flag(true)
        .disable_help_subcommand(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("migrate")
                .about("Migrate a database to the current on-disk format")
                .arg(
                    Arg::new("source")
                        .value_name("source")
                        .required(true)
                        .value_parser(OsStringValueParser::new()),
                )
                .arg(
                    Arg::new("output")
                        .long("output")
                        .value_name("destination")
                        .required(true)
                        .value_parser(OsStringValueParser::new()),
                )
                .arg(
                    Arg::new("to")
                        .long("to")
                        .value_name("version")
                        .required(true)
                        .value_parser(clap::value_parser!(u32)),
                ),
        )
        .subcommand(
            Command::new("compact")
                .about(
                    "Rebuild a compact copy of a static database file, replacing the destination if it exists",
                )
                .override_usage(
                    "btree-store compact [rebuild|inplace] <source> [destination] [--info]",
                )
                // `compact rebuild a b` spells the same run as `compact a b`: the
                // mode is a word of its own, and its absence means `rebuild`. It is
                // a subcommand rather than a positional because a leading optional
                // positional cannot be told apart from the source: clap would have
                // to decide whether the first path is a mode word, and it assigns
                // positionals in order, so `compact a b` would be read as a mode.
                .subcommand_negates_reqs(true)
                .args(compact_paths())
                .arg(compact_info())
                .subcommand(
                    Command::new("rebuild")
                        .about("Rebuild into the destination (what omitting the mode word means)")
                        .args(compact_paths()),
                )
                .subcommand(
                    Command::new("inplace")
                        .about("Reserved: page relocation is not implemented in this build")
                        .args(compact_paths()),
                ),
        )
        .subcommand(
            Command::new("check")
                .about("Check a static database file")
                .arg(
                    Arg::new("file")
                        .value_name("file")
                        .required(true)
                        .value_parser(OsStringValueParser::new()),
                )
                .arg(
                    Arg::new("summary")
                        .long("summary")
                        .action(ArgAction::SetTrue)
                        .help("Print the fixed space summary instead of the plain report"),
                )
                .arg(
                    Arg::new("scan")
                        .long("scan")
                        .action(ArgAction::SetTrue)
                        .help("Add per-bucket statistics to the report"),
                ),
        )
}

/// The two path arguments every `compact` spelling takes, default form and mode word
/// alike.
///
/// Shared by construction rather than by convention: `clap` hands each level its own
/// matches, and the two mode subcommands must accept exactly what the default form
/// accepts.
fn compact_paths() -> [Arg; 2] {
    [
        Arg::new("source")
            .value_name("source")
            .required(true)
            .value_parser(OsStringValueParser::new()),
        // Whether a destination is required depends on `--info`, which a mode
        // subcommand cannot ask this level about: `clap` hands each level its own
        // matches, so `compact --info rebuild <source>` would be told it is missing a
        // destination that the run never names. `run_compact` decides it instead,
        // where both levels are visible.
        Arg::new("destination")
            .value_name("destination")
            .value_parser(OsStringValueParser::new()),
    ]
}

/// The one option, global so that where it is written does not matter.
fn compact_info() -> Arg {
    Arg::new("info")
        .long("info")
        .global(true)
        .action(ArgAction::SetTrue)
        .help("Print what the rebuild would cost and write nothing")
}

fn run_compact(matches: &clap::ArgMatches) -> i32 {
    // The mode word owns the paths when it is present; without one they belong to
    // this level, and the mode is the default `rebuild`. Flags are read from both
    // levels because a flag written before the mode word is parsed by this level
    // and one written after it by the subcommand.
    let (mode, paths) = match matches.subcommand() {
        Some(("inplace", sub)) => (Mode::Inplace, sub),
        Some(("rebuild", sub)) => (Mode::Rebuild, sub),
        _ => (Mode::Rebuild, matches),
    };
    let args = CompactArgs {
        source: PathBuf::from(
            paths
                .get_one::<OsString>("source")
                .expect("required by the parser"),
        ),
        destination: paths.get_one::<OsString>("destination").map(PathBuf::from),
        mode,
        info: paths.get_flag("info") || matches.get_flag("info"),
    };
    match crate::compact::run(args) {
        Ok(summary) => {
            print!("{summary}");
            EXIT_OK_SUMMARY
        }
        Err(error) => {
            eprintln!("{error}");
            if error.class == Class::Arg {
                EXIT_USAGE
            } else {
                EXIT_FAILURE
            }
        }
    }
}

fn run_migrate(matches: &clap::ArgMatches) -> i32 {
    let source = PathBuf::from(
        matches
            .get_one::<OsString>("source")
            .expect("required by the parser"),
    );
    let destination = PathBuf::from(
        matches
            .get_one::<OsString>("output")
            .expect("required by the parser"),
    );
    let to = *matches
        .get_one::<u32>("to")
        .expect("required by the parser");

    if to != FORMAT_VERSION {
        eprintln!(
            "{}",
            CliError::new(
                "arguments",
                Class::Arg,
                format!("--to {to} is not this build's format version ({FORMAT_VERSION})"),
            )
        );
        return EXIT_USAGE;
    }

    if let Err(error) = crate::staging::check_output_name("--output", &destination) {
        eprintln!("{error}");
        return EXIT_USAGE;
    }

    match crate::migrate::run(MigrateArgs {
        source,
        destination,
    }) {
        Ok(summary) => {
            print!("{summary}");
            EXIT_OK_SUMMARY
        }
        Err(error) => {
            eprintln!("{error}");
            // A value error inside the run (naming the source as the destination)
            // is a usage error like the ones judged above, not a failed migration.
            if error.class == Class::Arg {
                EXIT_USAGE
            } else {
                EXIT_FAILURE
            }
        }
    }
}

/// The process entry point: returns the exit code.
pub(crate) fn main(args: impl IntoIterator<Item = OsString>) -> i32 {
    let matches = match command().try_get_matches_from(args) {
        Ok(matches) => matches,
        Err(error) => {
            let _ = error.print();
            return if error.use_stderr() { EXIT_USAGE } else { 0 };
        }
    };

    match matches.subcommand() {
        Some(("migrate", migrate_matches)) => run_migrate(migrate_matches),
        Some(("check", check_matches)) => {
            let file = PathBuf::from(
                check_matches
                    .get_one::<OsString>("file")
                    .expect("required by the parser"),
            );
            let options = crate::check::Options {
                summary: check_matches.get_flag("summary"),
                scan: check_matches.get_flag("scan"),
            };
            crate::check::run(&file, options)
        }
        Some(("compact", compact_matches)) => run_compact(compact_matches),
        _ => EXIT_USAGE,
    }
}

/// A command that succeeded: whatever the command chose to print on stdout
/// (a summary for `migrate` and `check`, possibly nothing at all for
/// `compact --info`) is the command's only output.
pub(crate) const EXIT_OK_SUMMARY: i32 = 0;

/// The source file, held open and shared-locked for the whole migration.
///
/// The descriptor is what the v1 reader re-reads from, and the recorded source
/// mode feeds the pre-publication permission narrowing.
pub(crate) struct Session {
    source: File,
    pub(crate) source_path: PathBuf,
    pub(crate) source_version: u32,
    pub(crate) destination: PathBuf,
    #[cfg(unix)]
    pub(crate) source_mode: u32,
}

impl Session {
    /// Opens and locks the source, then rejects anything that cannot be a
    /// migration input. The source is never created, never opened writable and
    /// never re-opened: every later read uses this descriptor.
    pub(crate) fn open(source: &Path, destination: &Path) -> Result<Self, CliError> {
        let path_metadata = std::fs::metadata(source)
            .map_err(|error| CliError::io("source", "stat", source, error))?;
        if !path_metadata.is_file() {
            return Err(CliError::new(
                "source",
                Class::Io,
                format!("{} is not a regular file", source.display()),
            ));
        }

        let file = OpenOptions::new()
            .read(true)
            .open(source)
            .map_err(|error| CliError::io("source", "open", source, error))?;

        let metadata = file
            .metadata()
            .map_err(|error| CliError::io("source", "stat", source, error))?;
        if !metadata.is_file() {
            return Err(CliError::new(
                "source",
                Class::Io,
                format!("{} is not a regular file", source.display()),
            ));
        }

        acquire_shared_lock(&file, source)?;
        // The destination is replaced, so the only questions about it are whether it
        // can hold a file at all and whether it is the source itself.
        crate::staging::check_replaceable_destination(destination)?;
        crate::staging::refuse_self_destination(destination, source)?;
        let source_version = probe_version(&file, source)?;

        if source_version == FORMAT_VERSION {
            return Err(CliError::new(
                "source",
                Class::SourceAlreadyCurrent,
                format!(
                    "{} already uses format version {FORMAT_VERSION}; there is nothing to migrate",
                    source.display()
                ),
            ));
        }
        if !SOURCE_VERSIONS.contains(&source_version) {
            return Err(CliError::new(
                "source",
                Class::SourceVersionUnsupported,
                format!(
                    "{} uses format version {source_version}; this build reads {FORMAT_VERSION} and migrates {}",
                    source.display(),
                    source_versions_list()
                ),
            ));
        }

        Ok(Self {
            source: file,
            source_path: source.to_path_buf(),
            source_version,
            destination: destination.to_path_buf(),
            #[cfg(unix)]
            source_mode: source_mode(&metadata),
        })
    }

    /// Creates the private staging file for the output in the destination's
    /// directory and hands it to the normal v2 writer, so the target is built by
    /// the same code path as any other database (`is_new` is "length is zero").
    pub(crate) fn create_staging(&self) -> Result<Staging, CliError> {
        let destination = &self.destination;
        let parent = match destination.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        let name = destination.file_name().ok_or_else(|| {
            CliError::new(
                "staging",
                Class::Io,
                format!("{} has no file name", destination.display()),
            )
        })?;
        crate::staging::create_staging_in(
            parent,
            name,
            std::process::id(),
            &crate::staging::staging_suffix(),
        )
    }

    /// The read-only descriptor every source read must use.
    pub(crate) fn source(&self) -> &File {
        &self.source
    }
}

/// The accepted source versions, as the refusal message names them.
pub(crate) fn source_versions_list() -> String {
    SOURCE_VERSIONS
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Takes a shared lock, so concurrent readers coexist while any writer that
/// honours this repository's exclusive lock is blocked. A platform that cannot
/// lock a read-only descriptor reports an error instead of degrading.
fn acquire_shared_lock(file: &File, path: &Path) -> Result<(), CliError> {
    let deadline = Instant::now() + LOCK_TIMEOUT;
    loop {
        match file.try_lock_shared() {
            Ok(()) => return Ok(()),
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                std::thread::sleep(LOCK_RETRY_INTERVAL);
            }
            Err(TryLockError::WouldBlock) => {
                return Err(CliError::new(
                    "source",
                    Class::LockBusy,
                    format!("{} is locked by another process", path.display()),
                ));
            }
            Err(TryLockError::Error(error)) => {
                return Err(CliError::new(
                    "source",
                    Class::Io,
                    format!(
                        "cannot lock {} read-only: {error}; this platform does not support it",
                        path.display()
                    ),
                ));
            }
        }
    }
}

/// The permission bits the published target inherits from the source.
#[cfg(unix)]
fn source_mode(metadata: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o7777
}

/// Decides which decoder may read the source, from its meta slots alone.
///
/// The meta record is the one structure every format version has promised to keep
/// at the same place: magic at `[0, 8)`, sequence at `[8, 16)`, format version at
/// `[16, 20)`, the roots, and a CRC32C over the first 36 bytes at `[36, 40)`. It is
/// therefore the only thing this tool may look at before it knows the version.
///
/// A slot is a *valid record* when it carries the magic and its checksum matches;
/// only such a slot votes. A slot that does not vote is torn or absent and is
/// skipped, exactly like the runtime's slot selection. What is refused:
///
/// - no valid record at all: there is nothing to read, and no version to name;
/// - valid records that disagree on the version: the file cannot be attributed to
///   one contract, and picking the slot that happens to name a version this build
///   can decode would silently read a generation the file no longer claims to be
///   the current one - the runtime refuses this file too (`MIXED_FORMAT_VERSIONS`).
///
/// A single valid version outside the supported set is returned to the caller,
/// which refuses it by name.
fn probe_version(file: &File, path: &Path) -> Result<u32, CliError> {
    let mut versions: Vec<u32> = Vec::new();
    for offset in [0u64, PAGE_SIZE] {
        if let Some(version) = read_slot_version(file, offset)?
            && !versions.contains(&version)
        {
            versions.push(version);
        }
    }
    match versions.as_slice() {
        [] => Err(CliError::new(
            "source",
            Class::SourceCorrupt,
            format!(
                "neither meta slot of {} is a valid meta record",
                path.display()
            ),
        )),
        [version] => Ok(*version),
        [first, second, ..] => Err(CliError::new(
            "source",
            Class::SourceVersionMixed,
            format!(
                "{} records format versions {first} and {second} in its meta slots; \
                 refusing to choose which generation to read",
                path.display()
            ),
        )),
    }
}

/// The format version one meta slot records, or `None` when that slot is not a
/// valid record (missing magic, or a record checksum that does not match).
fn read_slot_version(file: &File, offset: u64) -> Result<Option<u32>, CliError> {
    let mut buf = [0u8; META_RECORD_LEN];
    let mut reader = file;
    let read = reader
        .seek(SeekFrom::Start(offset))
        .and_then(|_| reader.read_exact(&mut buf));
    match read {
        Ok(()) => Ok(valid_record_version(&buf)),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(CliError::new(
            "source",
            Class::Io,
            format!("reading the meta record: {error}"),
        )),
    }
}

/// The version token of a complete record, or `None` when `buf` is not one.
fn valid_record_version(buf: &[u8; META_RECORD_LEN]) -> Option<u32> {
    if u64::from_le_bytes(buf[0..8].try_into().expect("8 bytes")) != MAGIC {
        return None;
    }
    let stored = u32::from_le_bytes(buf[META_CHECKSUM_OFFSET..].try_into().expect("4 bytes"));
    if stored != crc32c::crc32c(&buf[..META_CHECKSUM_OFFSET]) {
        return None;
    }
    Some(u32::from_le_bytes(buf[16..20].try_into().expect("4 bytes")))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use crate::staging::create_private;
    use crate::staging::staging_suffix;

    #[test]
    fn class_tokens_match_the_contract() {
        assert_eq!(Class::Arg.as_str(), "arg");
        assert_eq!(Class::Io.as_str(), "io");
        assert_eq!(Class::CheckFailed.as_str(), "failed");
        assert_eq!(Class::CheckIncomplete.as_str(), "incomplete");
        assert_eq!(Class::LockBusy.as_str(), "lock-busy");
        assert_eq!(
            Class::SourceVersionUnsupported.as_str(),
            "source-version-unsupported"
        );
        assert_eq!(Class::SourceVersionMixed.as_str(), "source-version-mixed");
        assert_eq!(
            Class::SourceAlreadyCurrent.as_str(),
            "source-already-current"
        );
        assert_eq!(Class::SourceCorrupt.as_str(), "source-corrupt");
        assert_eq!(Class::TargetInvalid.as_str(), "target-invalid");
        assert_eq!(
            Class::PublishedDurabilityUnknown.as_str(),
            "published-durability-unknown"
        );
        assert_eq!(Class::VerifyMismatch.as_str(), "verify-mismatch");
    }

    #[test]
    fn migration_staging_rechecks_destination_after_source_validation() {
        let dir = tempfile::tempdir().unwrap();
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v1_shapes.v1");
        let target = dir.path().join("target.db");
        let session = Session::open(&source, &target).unwrap();
        std::fs::create_dir(&target).unwrap();
        let error = session.create_staging().unwrap_err();
        assert_eq!(error.phase, "target");
        assert_eq!(error.class, Class::Io);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn error_line_is_phase_class_detail() {
        let staging = Path::new("/tmp/x.migrate-v2.1.0.tmp");
        let line = CliError::new("source", Class::LockBusy, "busy")
            .with_staging(staging, true)
            .to_string();
        assert_eq!(
            line,
            format!("error: source: lock-busy: busy; staging {staging:?} (present)")
        );
        assert!(
            line.contains("(present)"),
            "a surviving staging file must be named as present: {line}"
        );

        // A run that removed its own staging before failing must say so instead of
        // sending the operator after a name that no longer exists.
        let line = CliError::new("publish", Class::PublishedDurabilityUnknown, "gone")
            .with_staging(staging, false)
            .to_string();
        assert!(
            line.contains("(already removed)"),
            "a removed staging file must be named as removed: {line}"
        );
    }

    /// The accepted source versions and the decoders must be the same set: the table
    /// is what `Session::open` accepts and what `migrate::open_source` dispatches on,
    /// so a version added to it without a decoder is a failure here rather than a
    /// migration path lost in the field.
    #[test]
    fn every_accepted_source_version_has_a_decoder() {
        for &version in SOURCE_VERSIONS {
            let fixture = match version {
                1 => concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/v1_shapes.v1"),
                other => panic!(
                    "source version {other} is accepted but has no fixture; add the fixture \
                     with its decoder"
                ),
            };
            let file = File::open(fixture).expect("the fixture must exist");
            let reader = crate::migrate::open_source_for_test(version, file)
                .expect("an accepted version must have a decoder");
            assert_eq!(
                reader.meta.seq, 103,
                "the decoder must actually read the fixture, not default its meta"
            );
        }

        // A version outside the table is refused by name, never decoded.
        let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/v1_shapes.v1");
        let file = File::open(fixture).unwrap();
        let error = crate::migrate::open_source_for_test(999, file)
            .err()
            .expect("an unaccepted version must not be decoded");
        assert_eq!(error.class, Class::SourceVersionUnsupported);
    }

    #[test]
    fn the_staging_name_records_the_target_format() {
        let dir = tempfile::TempDir::new().unwrap();
        let staging = crate::staging::create_staging_in(
            dir.path(),
            std::ffi::OsStr::new("out.db"),
            7,
            &crate::staging::staging_suffix(),
        )
        .unwrap();
        let name = staging
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            name,
            format!("out.db.migrate-v{}.7.0.tmp", FORMAT_VERSION),
            "the staging name is the destination, the format it writes, the pid and the counter"
        );
    }

    #[test]
    fn staging_advances_the_counter_past_taken_names() {
        let dir = tempfile::TempDir::new().unwrap();
        let name = std::ffi::OsStr::new("out.db");
        let taken_name = format!("out.db{}.4242.0.tmp", staging_suffix());
        let taken = dir.path().join(&taken_name);
        std::fs::write(&taken, b"someone else's leftover").unwrap();

        let staging = crate::staging::create_staging_in(
            dir.path(),
            name,
            4242,
            &crate::staging::staging_suffix(),
        )
        .unwrap();
        assert_eq!(
            staging.path.file_name().unwrap(),
            std::ffi::OsStr::new(&format!("out.db{}.4242.1.tmp", staging_suffix())),
            "a taken name must move the counter, not overwrite"
        );
        assert_eq!(
            std::fs::read(&taken).unwrap(),
            b"someone else's leftover",
            "a foreign staging file is never touched"
        );
        assert_ne!(
            std::fs::metadata(&staging.path).unwrap().len(),
            0,
            "the staging file is initialized as a database by the normal writer"
        );
        drop(staging);
    }

    #[test]
    fn staging_reports_io_when_every_candidate_name_is_taken() {
        let dir = tempfile::TempDir::new().unwrap();
        let name = std::ffi::OsStr::new("out.db");
        for counter in 0..STAGING_COLLISION_LIMIT {
            std::fs::create_dir(
                dir.path()
                    .join(format!("out.db{}.4242.{counter}.tmp", staging_suffix())),
            )
            .unwrap();
        }

        let error = crate::staging::create_staging_in(
            dir.path(),
            name,
            4242,
            &crate::staging::staging_suffix(),
        )
        .expect_err("no name is free");
        assert_eq!(error.class, Class::Io);
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect();
        assert!(
            entries
                .iter()
                .all(|entry| entry.file_type().unwrap().is_dir()),
            "a failed staging attempt leaves no file behind"
        );
        assert_eq!(
            entries.len(),
            STAGING_COLLISION_LIMIT as usize,
            "no foreign candidate may be removed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn permissions_narrow_to_the_source_owner_bits() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("staged.db");
        let file = create_private(&path).unwrap();
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600,
            "staging starts private"
        );
        drop(file);
        let staging = Staging { path: path.clone() };
        staging.narrow_permissions(0o644).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        staging.narrow_permissions(0o400).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o400
        );
        staging.narrow_permissions(0).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o400,
            "a zero mask must not produce an unreadable file"
        );
        // A source whose mode leaves the owner without read - write-only, or
        // nothing at all - must still yield an output its owner can read.
        // `narrow_permissions` keeps only `0o600` and adds owner read back when
        // that left none, so group and other bits are irrelevant here.
        for source_mode in [0o200, 0o222, 0o240, 0o300] {
            staging.narrow_permissions(source_mode).unwrap();
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                mode, 0o400,
                "source {source_mode:o} must not lose owner read"
            );
        }
    }
}
