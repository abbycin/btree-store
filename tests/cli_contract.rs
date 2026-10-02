//! CLI contract acceptance tests: the command surface, the staging lifecycle and
//! permissions, and the source-side early failures. Assertions bind exit codes and
//! the `error: <phase>: <class>: <detail>` class token, never prose.

mod common;

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_btree-store");
const V1_FIXTURE: &str = "tests/fixtures/v1_shapes.v1";

fn run<I, S>(args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let mut command: Command = common::child_test_command(Path::new(BIN));
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("the CLI must be runnable")
}

/// Runs the CLI and fails the test if it has not exited within `secs`: a wedged
/// process must be reported as a bug, not hang the suite.
fn run_with_deadline<I, S>(args: I, secs: u64) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let mut child = common::child_test_command(Path::new(BIN))
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the CLI must be spawnable");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    loop {
        match child.try_wait().expect("try_wait") {
            Some(_) => break,
            None if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            None => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("the CLI did not exit within {secs}s (wedged)");
            }
        }
    }
    child.wait_with_output().expect("collect output")
}

fn code(output: &Output) -> i32 {
    output.status.code().expect("the CLI must exit normally")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// `error: <phase>: <class>: <detail>` — returns the class token.
/// A runtime failure must report the class on stderr and keep stdout empty.
fn error_class(output: &Output) -> String {
    assert!(
        stdout(output).is_empty(),
        "a failed run must not print a summary: {:?}",
        stdout(output)
    );
    let line = stderr(output);
    let line = line.lines().next().unwrap_or_default();
    let mut parts = line.splitn(4, ": ");
    assert_eq!(parts.next(), Some("error"), "unexpected error line: {line}");
    let phase = parts.next().unwrap_or_default();
    assert!(
        matches!(
            phase,
            "arguments"
                | "source"
                | "target"
                | "rebuild"
                | "compare"
                | "publish"
                | "check"
                | "staging"
        ),
        "unexpected phase: {line}"
    );
    parts.next().unwrap_or_default().to_string()
}

fn copy_v1_fixture(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    fs::copy(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(V1_FIXTURE),
        &path,
    )
    .unwrap();
    path
}

/// Rewrites one meta slot so it stays a *valid* record of the version it names:
/// the version field moves and the record checksum follows. This is how a
/// version-mixed file is produced - two slots that are both readable, under
/// different contracts. The slot must be a valid record beforehand, so the case
/// tests the version rule rather than a torn slot.
fn retag_meta_slot(path: &Path, slot: u64, version: u32) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let offset = slot * common::PAGE_SIZE as u64;
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut page = [0u8; 4096];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut page).unwrap();
    let mut meta = btree_store::MetaNode::from_slice(&page);
    let mut probe = meta;
    probe.update_checksum();
    assert_eq!(
        meta.magic,
        btree_store::MAGIC,
        "the slot must already carry a valid record: {path:?} slot {slot}"
    );
    assert_eq!(
        meta.checksum, probe.checksum,
        "the slot must already carry a valid record: {path:?} slot {slot}"
    );
    meta.format_version = version;
    meta.update_checksum();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(meta.as_page_slice()).unwrap();
    file.sync_all().unwrap();
}

fn tear_meta_slot(path: &Path, slot: u64) {
    use std::io::{Seek, SeekFrom, Write};
    let offset = slot * common::PAGE_SIZE as u64;
    let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[0u8; 40]).unwrap();
    file.sync_all().unwrap();
}

fn fresh_v2(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    let tree = btree_store::BTree::open(&path).unwrap();
    tree.new_bucket("bucket", false).unwrap();
    tree.exec("bucket", |txn| txn.put(b"key", b"value"))
        .unwrap();
    drop(tree);
    path
}

/// Rewrites both meta slots' format version and recomputes their checksums, i.e.
/// a checksum-valid file from another format version.
fn rewrite_format_version(path: &Path, version: u32) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    for offset in [0u64, 4096] {
        let mut page = [0u8; 4096];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut page).unwrap();
        let mut meta = btree_store::MetaNode::from_slice(&page);
        meta.format_version = version;
        meta.update_checksum();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(meta.as_page_slice()).unwrap();
    }
    file.sync_all().unwrap();
}

#[test]
fn command_surface_reports_usage_errors_with_exit_2() {
    let dir = TempDir::new().unwrap();
    let source = copy_v1_fixture(dir.path(), "source.db");
    let destination = dir.path().join("out.db");
    let destination_arg = destination.as_os_str();

    let cases: Vec<(&str, Vec<OsString>)> = vec![
        ("no arguments", vec![]),
        ("unknown subcommand", vec!["repair".into()]),
        (
            "unknown option",
            vec![
                "migrate".into(),
                source.clone().into(),
                "--output".into(),
                destination_arg.into(),
                "--to".into(),
                "2".into(),
                "--force".into(),
            ],
        ),
        (
            "duplicate option",
            vec![
                "migrate".into(),
                source.clone().into(),
                "--output".into(),
                destination_arg.into(),
                "--output".into(),
                destination_arg.into(),
                "--to".into(),
                "2".into(),
            ],
        ),
        (
            "missing --to",
            vec![
                "migrate".into(),
                source.clone().into(),
                "--output".into(),
                destination_arg.into(),
            ],
        ),
        (
            "missing --output",
            vec![
                "migrate".into(),
                source.clone().into(),
                "--to".into(),
                "2".into(),
            ],
        ),
        (
            "missing source",
            vec!["migrate".into(), "--to".into(), "2".into()],
        ),
    ];

    for (name, args) in &cases {
        let output = run(args.clone());
        assert_eq!(code(&output), 2, "{name}: {}", stderr(&output));
        assert!(
            stdout(&output).is_empty(),
            "{name}: usage errors must not write a summary to stdout"
        );
    }

    for (target, class) in [
        ("1", Some("arg")),
        ("3", Some("arg")),
        ("x", None),
        ("", None),
    ] {
        let output = run(vec![
            OsString::from("migrate"),
            source.clone().into(),
            "--output".into(),
            destination_arg.into(),
            "--to".into(),
            target.into(),
        ]);
        assert_eq!(code(&output), 2, "--to {target:?}: {}", stderr(&output));
        assert!(
            stdout(&output).is_empty(),
            "--to {target:?} must not write to stdout: {}",
            stdout(&output)
        );
        assert!(
            !stderr(&output).is_empty(),
            "--to {target:?} must report on stderr"
        );
        if let Some(class) = class {
            assert_eq!(
                error_class(&output),
                class,
                "--to {target:?}: {}",
                stderr(&output)
            );
        }
        // An argument rejection happens before anything is created.
        assert!(
            !destination.exists(),
            "--to {target:?} must not create the destination"
        );
        assert!(
            fs::read_dir(dir.path()).unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".migrate-")),
            "--to {target:?} must not create a staging file"
        );
    }

    // `--version`/`-V` are not part of the surface.
    for flag in ["--version", "-V"] {
        let output = run(vec![OsString::from(flag)]);
        assert_eq!(code(&output), 2, "{flag} must be rejected");
        assert!(
            stdout(&output).is_empty(),
            "{flag} must not write to stdout: {}",
            stdout(&output)
        );
    }

    let output = run(vec![OsString::from("help")]);
    assert_eq!(code(&output), 2, "`help` must not be a subcommand");

    for flag in ["--help", "-h"] {
        let output = run(vec![OsString::from(flag)]);
        assert_eq!(code(&output), 0);
        assert!(!stdout(&output).is_empty(), "{flag} writes usage to stdout");
        assert!(
            stderr(&output).is_empty(),
            "{flag} must not write to stderr"
        );
    }
}

/// A destination that names no file cannot be published to, and `link(2)` would only
/// say so after the whole rebuild had run. It is an argument the CLI will not honour,
/// so it is refused with the `arg` class before the source is opened.
#[test]
fn a_destination_that_names_no_file_is_a_usage_error() {
    let dir = TempDir::new().unwrap();
    let source = copy_v1_fixture(dir.path(), "source.db");
    let source_before = fs::read(&source).unwrap();

    for destination in ["", "out.db/", "./sub/", "/tmp/", "out.db/.", "./sub/."] {
        let output = run(vec![
            OsString::from("migrate"),
            source.clone().into(),
            "--output".into(),
            destination.into(),
            "--to".into(),
            "2".into(),
        ]);
        assert_eq!(
            code(&output),
            2,
            "--output {destination:?}: {}",
            stderr(&output)
        );
        assert_eq!(
            error_class(&output),
            "arg",
            "--output {destination:?}: {}",
            stderr(&output)
        );
        assert!(
            stdout(&output).is_empty(),
            "--output {destination:?} must not write a summary: {}",
            stdout(&output)
        );
        assert!(
            fs::read_dir(dir.path()).unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".migrate-")),
            "--output {destination:?} must not create a staging file"
        );
    }

    // The refusal happens before the source is touched.
    assert_eq!(fs::read(&source).unwrap(), source_before);

    // A name that merely contains a `.` is a file: the check must not over-reach.
    let trailing_dot = dir.path().join("out.");
    let output = run(vec![
        OsString::from("migrate"),
        source.into(),
        "--output".into(),
        trailing_dot.clone().into(),
        "--to".into(),
        "2".into(),
    ]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    assert!(
        trailing_dot.is_file(),
        "a destination named `out.` must publish"
    );
}

#[test]
fn source_side_failures_leave_no_target_and_do_not_abort() {
    let dir = TempDir::new().unwrap();
    let destination_parent = dir.path().join("out");
    fs::create_dir(&destination_parent).unwrap();

    let destination = destination_parent.join("missing.db");
    let output = run(vec![
        "migrate".as_ref(),
        dir.path().join("absent.db").as_os_str(),
        "--output".as_ref(),
        destination.as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1);
    assert_eq!(error_class(&output), "io");
    assert!(!destination.exists());

    let directory_source = dir.path().join("a-directory");
    fs::create_dir(&directory_source).unwrap();
    let destination = destination_parent.join("dir.db");
    let output = run(vec![
        "migrate".as_ref(),
        directory_source.as_os_str(),
        "--output".as_ref(),
        destination.as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1);
    assert_eq!(error_class(&output), "io");
    assert!(!destination.exists());

    #[cfg(unix)]
    {
        let destination = destination_parent.join("dev-null-out.db");
        let output = run(vec![
            "migrate".as_ref(),
            "/dev/null".as_ref(),
            "--output".as_ref(),
            destination.as_os_str(),
            "--to".as_ref(),
            "2".as_ref(),
        ]);
        assert_eq!(code(&output), 1);
        assert_eq!(
            error_class(&output),
            "io",
            "/dev/null is not a regular file: {}",
            stderr(&output)
        );
        assert!(!destination.exists());

        let fifo = dir.path().join("source.fifo");
        if matches!(Command::new("mkfifo").arg(&fifo).status(), Ok(s) if s.success()) {
            let destination = destination_parent.join("fifo-out.db");
            let output = run_with_deadline(
                vec![
                    "migrate".as_ref(),
                    fifo.as_os_str(),
                    "--output".as_ref(),
                    destination.as_os_str(),
                    "--to".as_ref(),
                    "2".as_ref(),
                ],
                10,
            );
            assert_eq!(code(&output), 1, "{}", stderr(&output));
            assert_eq!(error_class(&output), "io");
            assert!(!destination.exists());
        }
    }

    let garbage = dir.path().join("garbage.db");
    fs::write(&garbage, vec![0xA5u8; 8192]).unwrap();
    let destination = destination_parent.join("garbage-out.db");
    let output = run(vec![
        "migrate".as_ref(),
        garbage.as_os_str(),
        "--output".as_ref(),
        destination.as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1);
    assert_eq!(error_class(&output), "source-corrupt");
    assert!(!destination.exists());

    // 4. source already in the current format
    let current = fresh_v2(dir.path(), "current.db");
    let before = fs::read(&current).unwrap();
    let destination = destination_parent.join("current-out.db");
    let output = run(vec![
        "migrate".as_ref(),
        current.as_os_str(),
        "--output".as_ref(),
        destination.as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1);
    assert_eq!(error_class(&output), "source-already-current");
    assert!(!destination.exists());
    assert_eq!(
        fs::read(&current).unwrap(),
        before,
        "the source is never written"
    );

    // 5. checksum-valid file from a future format version
    let future = fresh_v2(dir.path(), "future.db");
    rewrite_format_version(&future, 99);
    let before = fs::read(&future).unwrap();
    let destination = destination_parent.join("future-out.db");
    let output = run(vec![
        "migrate".as_ref(),
        future.as_os_str(),
        "--output".as_ref(),
        destination.as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1);
    assert_eq!(error_class(&output), "source-version-unsupported");
    assert!(!destination.exists());
    assert_eq!(fs::read(&future).unwrap(), before);

    // An existing destination is not an error: it is replaced. A dangling symlink
    // is a name like any other, and the link itself is what gets replaced.
    let existing = fresh_v2(dir.path(), "existing.db");
    let stale = fs::read(&existing).unwrap();
    let source = copy_v1_fixture(dir.path(), "v1-source.db");
    let output = run(vec![
        "migrate".as_ref(),
        source.as_os_str(),
        "--output".as_ref(),
        existing.as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(
        code(&output),
        0,
        "an existing destination is replaced: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_ne!(
        fs::read(&existing).unwrap(),
        stale,
        "the old bytes are gone"
    );

    #[cfg(unix)]
    {
        let dangling = destination_parent.join("dangling.db");
        std::os::unix::fs::symlink(destination_parent.join("nowhere"), &dangling).unwrap();
        let output = run(vec![
            "migrate".as_ref(),
            source.as_os_str(),
            "--output".as_ref(),
            dangling.as_os_str(),
            "--to".as_ref(),
            "2".as_ref(),
        ]);
        assert_eq!(
            code(&output),
            0,
            "a dangling link is replaced, not followed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            fs::symlink_metadata(&dangling).unwrap().is_file(),
            "the link itself was replaced by the migrated database"
        );
        assert!(!destination_parent.join("nowhere").exists());
    }

    // The source is the one file a destination may not be, whatever spelling names
    // it.
    let before = fs::read(&source).unwrap();
    for destination in [source.clone(), source.canonicalize().unwrap()] {
        let output = run(vec![
            "migrate".as_ref(),
            source.as_os_str(),
            "--output".as_ref(),
            destination.as_os_str(),
            "--to".as_ref(),
            "2".as_ref(),
        ]);
        assert_eq!(code(&output), 2, "{destination:?}");
        assert_eq!(error_class(&output), "arg", "{destination:?}");
        assert_eq!(
            fs::read(&source).unwrap(),
            before,
            "{destination:?}: the source is untouched"
        );
    }

    let output = run(vec![
        "migrate".as_ref(),
        source.as_os_str(),
        "--output".as_ref(),
        dir.path().join("no-such-dir/out.db").as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1);
    assert_eq!(error_class(&output), "io");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let locked = copy_v1_fixture(dir.path(), "unreadable.db");
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
        if fs::File::open(&locked).is_err() {
            let destination = destination_parent.join("unreadable-out.db");
            let output = run(vec![
                "migrate".as_ref(),
                locked.as_os_str(),
                "--output".as_ref(),
                destination.as_os_str(),
                "--to".as_ref(),
                "2".as_ref(),
            ]);
            assert_eq!(code(&output), 1);
            assert_eq!(error_class(&output), "io");
            assert!(!destination.exists());
        }
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o600)).unwrap();
    }

    // 9. a read-only source is the normal case: writing must not be required.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let read_only = copy_v1_fixture(dir.path(), "read-only.db");
        fs::set_permissions(&read_only, fs::Permissions::from_mode(0o400)).unwrap();
        let destination = destination_parent.join("read-only-out.db");
        let output = run(vec![
            "migrate".as_ref(),
            read_only.as_os_str(),
            "--output".as_ref(),
            destination.as_os_str(),
            "--to".as_ref(),
            "2".as_ref(),
        ]);
        assert_eq!(
            code(&output),
            0,
            "a read-only source must be usable: {}",
            stderr(&output)
        );
        assert!(destination.exists());
        fs::set_permissions(&read_only, fs::Permissions::from_mode(0o600)).unwrap();
    }

    let leftovers: Vec<_> = fs::read_dir(&destination_parent)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().contains(".migrate-"))
        .collect();
    assert!(leftovers.is_empty(), "staging leftovers: {leftovers:?}");
}

#[test]
fn a_locked_source_reports_busy() {
    let dir = TempDir::new().unwrap();
    let source = copy_v1_fixture(dir.path(), "locked.db");
    let destination = dir.path().join("locked-out.db");

    // Hold the exclusive lock the library's writer would hold.
    let holder = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&source)
        .unwrap();
    holder.try_lock().unwrap();

    let output = run(vec![
        "migrate".as_ref(),
        source.as_os_str(),
        "--output".as_ref(),
        destination.as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1);
    assert_eq!(error_class(&output), "lock-busy");
    assert!(!destination.exists());
}

/// The source lock is *shared*: a second reader must coexist, while an exclusive
/// holder (the library's writer) is refused. Without this case the lock type is
/// unpinned - swapping `try_lock_shared` for `try_lock` would leave the suite
/// green while turning a read-only migration into a mutex.
#[test]
fn a_shared_source_lock_coexists_and_an_exclusive_one_is_refused() {
    let dir = TempDir::new().unwrap();
    let source = copy_v1_fixture(dir.path(), "shared.db");

    let reader = fs::OpenOptions::new().read(true).open(&source).unwrap();
    reader.try_lock_shared().unwrap();

    let output = run(vec![
        "migrate".as_ref(),
        source.as_os_str(),
        "--output".as_ref(),
        dir.path().join("shared-out.db").as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(
        code(&output),
        0,
        "another reader must not block the run: {}",
        stderr(&output)
    );
    drop(reader);
}

#[test]
fn staging_is_private_and_never_removes_foreign_files() {
    let dir = TempDir::new().unwrap();
    let source = copy_v1_fixture(dir.path(), "v1.db");
    let destination = dir.path().join("staged.db");

    // A leftover from somebody else's run must survive untouched.
    let foreign = dir.path().join("staged.db.migrate-v2.999999.0.tmp");
    fs::write(&foreign, b"not ours").unwrap();
    let foreign_before = fs::read(&foreign).unwrap();

    let output = run(vec![
        "migrate".as_ref(),
        source.as_os_str(),
        "--output".as_ref(),
        destination.as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);

    // A successful migration publishes the target, leaves no staging file of its
    // own and never touches a foreign leftover.
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    assert_eq!(fs::read(&foreign).unwrap(), foreign_before);
    assert!(
        destination.exists(),
        "a successful run publishes the target"
    );
    let leftovers: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().contains(".migrate-"))
        .collect();
    assert_eq!(
        leftovers.len(),
        1,
        "only the foreign leftover may remain: {leftovers:?}"
    );
}


#[test]
fn option_terminator_and_non_utf8_paths_are_accepted() {
    let dir = TempDir::new().unwrap();
    let destination = dir.path().join("out.db");

    // A source whose name starts with a dash, passed after `--`.
    let dashed = copy_v1_fixture(dir.path(), "-source.db");
    let output = run(vec![
        OsString::from("migrate"),
        "--output".into(),
        destination.clone().into_os_string(),
        "--to".into(),
        OsString::from("2"),
        OsString::from("--"),
        dashed.clone().into_os_string(),
    ]);
    assert_eq!(
        code(&output),
        0,
        "a `--`-terminated path must parse and run: {}",
        stderr(&output)
    );
    assert!(destination.exists());

    // A source whose name is not valid UTF-8. macOS refuses to create such a file
    // at all, so the platform's own answer is what is asserted there.
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let name = std::ffi::OsStr::from_bytes(b"v1-\xff-source.db");
        let path = dir.path().join(name);
        #[cfg(target_os = "macos")]
        {
            let refused = fs::write(&path, b"");
            assert!(
                refused.is_err() && !path.exists(),
                "macOS must refuse a name that is not valid UTF-8: {refused:?}"
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            let bytes =
                fs::read(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(V1_FIXTURE)).unwrap();
            fs::write(&path, &bytes).unwrap();
            let output = run(vec![
                "migrate".into(),
                path.clone().into_os_string(),
                "--output".into(),
                dir.path().join("non-utf8-out.db").into_os_string(),
                "--to".into(),
                OsString::from("2"),
            ]);
            assert_eq!(
                code(&output),
                0,
                "a non-UTF-8 source path must be accepted: {}",
                stderr(&output)
            );

            // A destination whose name is not valid UTF-8: the `--output` parser
            // must take an OsString too, not only the positional source.
            let target = dir.path().join(std::ffi::OsStr::from_bytes(b"out-\xfe.db"));
            let output = run(vec![
                "migrate".into(),
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join(V1_FIXTURE)
                    .into_os_string(),
                "--output".into(),
                target.clone().into_os_string(),
                "--to".into(),
                OsString::from("2"),
            ]);
            assert_eq!(
                code(&output),
                0,
                "a non-UTF-8 --output must be accepted: {}",
                stderr(&output)
            );
            assert!(target.exists(), "the non-UTF-8 destination must exist");
        }
    }
}

/// Two readable meta slots under different format versions: the file cannot be
/// attributed to one contract, so the tool must refuse it rather than pick the
/// slot whose version it happens to have a decoder for. A v1 file with a second
/// slot that a newer writer already rewrote (or an in-place upgrade interrupted
/// halfway) is exactly this shape, and reading the older generation would present
/// a stale generation as the source's content.
#[test]
fn a_mixed_version_source_is_refused() {
    let dir = TempDir::new().unwrap();
    let destination_parent = dir.path();

    // Already current, one slot still names the old version.
    let current = fresh_v2(dir.path(), "current.db");
    let control = run(vec![
        "migrate".as_ref(),
        current.as_os_str(),
        "--output".as_ref(),
        destination_parent.join("control.db").as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(
        error_class(&control),
        "source-already-current",
        "the control run must recognize the file as current"
    );
    retag_meta_slot(&current, 1, 1);
    let output = run(vec![
        "migrate".as_ref(),
        current.as_os_str(),
        "--output".as_ref(),
        destination_parent.join("current-out.db").as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1, "{}", stderr(&output));
    assert_eq!(error_class(&output), "source-version-mixed");
    assert!(!destination_parent.join("current-out.db").exists());

    // Still version 1, one slot already names the current version.
    let stale = copy_v1_fixture(dir.path(), "stale.v1");
    retag_meta_slot(&stale, 1, btree_store::FORMAT_VERSION);
    let output = run(vec![
        "migrate".as_ref(),
        stale.as_os_str(),
        "--output".as_ref(),
        destination_parent.join("stale-out.db").as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1, "{}", stderr(&output));
    assert_eq!(error_class(&output), "source-version-mixed");
    assert!(stdout(&output).is_empty(), "no summary on refusal");
    assert!(!destination_parent.join("stale-out.db").exists());
    assert!(
        fs::read_dir(destination_parent).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".migrate-")),
        "a refused source creates no staging file"
    );
}

/// A torn slot is not a version conflict: the readable slot alone decides, which
/// is what an interrupted write leaves behind and what recovery already does.
#[test]
fn a_torn_meta_slot_is_read_from_the_surviving_slot() {
    let dir = TempDir::new().unwrap();
    let destination_parent = dir.path();

    let stale = copy_v1_fixture(dir.path(), "torn.v1");
    tear_meta_slot(&stale, 1);
    let output = run(vec![
        "migrate".as_ref(),
        stale.as_os_str(),
        "--output".as_ref(),
        destination_parent.join("torn-out.db").as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    assert!(
        stdout(&output).contains("source_version=1"),
        "{}",
        stdout(&output)
    );

    let current = fresh_v2(dir.path(), "torn-current.db");
    tear_meta_slot(&current, 1);
    let output = run(vec![
        "migrate".as_ref(),
        current.as_os_str(),
        "--output".as_ref(),
        destination_parent.join("torn-current-out.db").as_os_str(),
        "--to".as_ref(),
        "2".as_ref(),
    ]);
    assert_eq!(code(&output), 1);
    assert_eq!(error_class(&output), "source-already-current");
}

/// The `key=value` lines of a successful `check`, each as (key, value), with
/// the trailing diagnostic lines dropped.
fn check_pairs(args: &[&str]) -> Vec<(String, String)> {
    let output = run(args);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    stdout(&output)
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with("diagnostic: "))
        .map(|line| {
            let (key, value) = line.split_once('=').expect("a key=value line");
            (key.to_string(), value.to_string())
        })
        .collect()
}

fn value_of<'a>(pairs: &'a [(String, String)], key: &str) -> &'a str {
    pairs
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
        .unwrap_or_else(|| panic!("{key} must be reported"))
}

/// A scan is for deciding what a rebuild would cost, and `compact --info`
/// already reports that from the same file. Two commands that answer the same
/// question must agree, so the per-bucket numbers are pinned against it
/// instead of against a baseline this test wrote itself.
#[test]
fn a_scan_totals_agree_with_what_compact_would_copy() {
    let dir = TempDir::new().unwrap();
    let path = fresh_v2(dir.path(), "scanned.db");

    let output = run(["check", path.to_str().unwrap(), "--scan"]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    let mut records = 0u64;
    let mut logical = 0u64;
    let mut buckets = 0;
    for line in stdout(&output).lines() {
        if let Some(value) = line.strip_prefix("bucket.records=") {
            records += value.parse::<u64>().unwrap();
            buckets += 1;
        } else if let Some(value) = line.strip_prefix("bucket.logical_key_bytes=") {
            logical += value.parse::<u64>().unwrap();
        } else if let Some(value) = line.strip_prefix("bucket.logical_value_bytes=") {
            logical += value.parse::<u64>().unwrap();
        }
    }
    assert!(buckets > 0, "the fixture must have a bucket to report");

    let output = run(["compact", "--info", path.to_str().unwrap()]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    let estimate = stderr(&output);
    assert_eq!(
        estimate
            .split_whitespace()
            .find_map(|field| field.strip_prefix("records=")),
        Some(records.to_string().as_str()),
        "scan records must match the rebuild's record count: {estimate}"
    );
    assert_eq!(
        estimate
            .split_whitespace()
            .find_map(|field| field.strip_prefix("logical_bytes=")),
        Some(logical.to_string().as_str()),
        "scan logical bytes must match the rebuild's logical bytes: {estimate}"
    );
}

/// The four spellings differ only in what they print. The check itself must be
/// the same run: same generation, same status, same exit code.
#[test]
fn every_check_spelling_reports_the_same_generation() {
    let dir = TempDir::new().unwrap();
    let path = fresh_v2(dir.path(), "modes.db");
    let file = path.to_str().unwrap();

    let plain = check_pairs(&["check", file]);
    let summary = check_pairs(&["check", file, "--summary"]);
    let scan = check_pairs(&["check", file, "--scan"]);
    let both = check_pairs(&["check", file, "--summary", "--scan"]);

    let keys =
        |pairs: &[(String, String)]| pairs.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
    assert_eq!(
        keys(&plain),
        [
            "status",
            "file",
            "file_len",
            "generation",
            "next_page_id",
            "reachable_pages",
            "reusable_pages",
            "retired_pages",
            "allocator_list_pages",
            "trailing_bytes",
        ],
        "the no-flag protocol must keep exactly its current keys"
    );
    assert_eq!(
        keys(&summary),
        [
            "status",
            "file",
            "source_version",
            "generation",
            "file_len",
            "next_page_id",
            "data_pages",
            "reusable_pages",
            "reusable_bytes",
            "reusable_extents",
            "retired_pages",
            "retired_bytes",
            "retired_extents",
            "allocator_list_pages",
            "reusable_if_all_retired_promoted_bytes",
            "reachable_pages",
            "active_data_pages",
            "trailing_bytes",
            "cost",
        ],
        "the summary field set is fixed"
    );
    assert_eq!(
        keys(&scan)[..plain.len()].to_vec(),
        keys(&plain),
        "a scan keeps the plain report and appends to it"
    );
    assert_eq!(
        &keys(&scan)[plain.len()..plain.len() + 5],
        [
            "source_version",
            "data_pages",
            "active_data_pages",
            "cost",
            "bucket_scan"
        ]
    );
    assert_eq!(
        keys(&both)[..summary.len()].to_vec(),
        keys(&summary),
        "a summary scan prints the summary set, then its own block"
    );
    assert_eq!(
        &keys(&both)[summary.len()..summary.len() + 1],
        ["bucket_scan"]
    );
    for pairs in [&scan, &both] {
        let all = keys(pairs);
        assert_eq!(
            all[all.len() - 7..].to_vec(),
            [
                "bucket.name",
                "bucket.prefix_encoding",
                "bucket.records",
                "bucket.logical_key_bytes",
                "bucket.logical_value_bytes",
                "bucket.tree_height",
                "bucket.reachable_pages",
            ],
            "each bucket is seven consecutive lines"
        );
    }

    // The flags choose keys, not a different reading of the file.
    for pairs in [&plain, &summary, &scan, &both] {
        assert_eq!(value_of(pairs, "status"), "ok");
        assert_eq!(value_of(pairs, "file"), path.display().to_string());
    }
    assert_eq!(
        value_of(&summary, "source_version"),
        btree_store::FORMAT_VERSION.to_string(),
        "source_version renders the build's version"
    );
    assert_eq!(value_of(&summary, "cost"), "full-check");
    assert_eq!(value_of(&scan, "cost"), "full-check");
    assert_eq!(value_of(&both, "cost"), "full-check");
    for pairs in [&summary, &scan, &both] {
        let number = |key| value_of(pairs, key).parse::<u64>().unwrap();
        assert_eq!(number("data_pages"), number("next_page_id") - 2);
        assert_eq!(
            number("active_data_pages"),
            number("reachable_pages") + number("allocator_list_pages")
        );
        assert_eq!(
            number("active_data_pages") + number("reusable_pages") + number("retired_pages"),
            number("data_pages")
        );
    }
    for (pairs, expected) in [
        (&plain, None),
        (&summary, None),
        (&scan, Some("true")),
        (&both, Some("true")),
    ] {
        assert_eq!(
            pairs
                .iter()
                .find(|(key, _)| key == "bucket_scan")
                .map(|(_, value)| value.as_str()),
            expected,
            "bucket_scan belongs to a scan and nothing else"
        );
    }

    // The byte fields are the report's page counts times the page size, which a
    // reader can recompute from the same lines.
    let page = 4096u64;
    for pairs in [&summary, &both] {
        assert_eq!(
            value_of(pairs, "reusable_bytes").parse::<u64>().unwrap(),
            value_of(pairs, "reusable_pages").parse::<u64>().unwrap() * page
        );
        assert_eq!(
            value_of(pairs, "retired_bytes").parse::<u64>().unwrap(),
            value_of(pairs, "retired_pages").parse::<u64>().unwrap() * page
        );
        assert_eq!(
            value_of(pairs, "reusable_if_all_retired_promoted_bytes")
                .parse::<u64>()
                .unwrap(),
            value_of(pairs, "reusable_bytes").parse::<u64>().unwrap()
                + value_of(pairs, "retired_bytes").parse::<u64>().unwrap()
        );
    }

    let generations: Vec<&str> = [&plain, &summary, &scan, &both]
        .iter()
        .map(|pairs| value_of(pairs, "generation"))
        .collect();
    assert!(
        generations.windows(2).all(|pair| pair[0] == pair[1]),
        "one file, one generation: {generations:?}"
    );
}

/// A bucket name is a catalog key, so it may hold the separator this protocol
/// is written in. The scan has to survive that: each bucket stays seven lines,
/// and the name decodes back to the bytes the catalog holds.
#[test]
fn a_bucket_name_cannot_break_the_line_protocol() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("odd-names.db");
    let names = ["a=b", "100%", "two\nlines", "bell\u{7}", "sp ace"];
    {
        let tree = btree_store::BTree::open(&path).unwrap();
        for name in names {
            tree.new_bucket(name, false).unwrap();
            tree.exec(name, |txn| txn.put(b"k", b"v")).unwrap();
        }
    }

    let output = run(["check", path.to_str().unwrap(), "--scan"]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    let printed = stdout(&output);
    let blocks = printed
        .split_once("bucket_scan=true\n\n")
        .expect("one empty line must separate the report and the first bucket")
        .1
        .trim_end_matches('\n')
        .split("\n\n")
        .collect::<Vec<_>>();
    assert_eq!(blocks.len(), names.len(), "one blank line between buckets");
    assert!(blocks.iter().all(|block| block.lines().count() == 7));
    let bucket_lines: Vec<&str> = printed
        .lines()
        .filter(|line| line.starts_with("bucket."))
        .collect();
    assert_eq!(bucket_lines.len(), names.len() * 7);

    // A reader takes seven consecutive lines per bucket, so the block has to be
    // contiguous and has to start with the name: a run that printed every name
    // first would keep the same line count and still break every parser.
    const KEYS: [&str; 7] = [
        "bucket.name",
        "bucket.prefix_encoding",
        "bucket.records",
        "bucket.logical_key_bytes",
        "bucket.logical_value_bytes",
        "bucket.tree_height",
        "bucket.reachable_pages",
    ];
    let mut decoded = Vec::new();
    for block in bucket_lines.chunks(7) {
        for (line, key) in block.iter().zip(KEYS) {
            assert!(
                line.starts_with(&format!("{key}=")),
                "a bucket block must start with its name and keep its field order: {line}"
            );
        }
        let name = block[0].split_once('=').expect("a key=value line").1;
        decoded.push((name.to_string(), percent_decode(name)));
        assert_eq!(block[1], "bucket.prefix_encoding=false");
        assert_eq!(block[2], "bucket.records=1");
        assert_eq!(block[3], "bucket.logical_key_bytes=1");
        assert_eq!(block[4], "bucket.logical_value_bytes=1");
        assert_eq!(block[5], "bucket.tree_height=1");
        assert_eq!(block[6], "bucket.reachable_pages=1");
    }

    // The wire form is fixed: only the unreserved bytes stay literal, and every
    // other byte is an uppercase `%HH`. A decoder that would also accept
    // lowercase hex or an escaped unreserved byte proves nothing here.
    let mut expected: Vec<String> = names.iter().map(|name| encode_expecting(name)).collect();
    expected.sort();
    let mut printed_names: Vec<String> = decoded.iter().map(|(raw, _)| raw.clone()).collect();
    printed_names.sort();
    assert_eq!(printed_names, expected, "the encoding is the fixed one");
    let mut round_tripped: Vec<String> = decoded.into_iter().map(|(_, name)| name).collect();
    round_tripped.sort();
    let mut originals: Vec<String> = names.iter().map(|name| name.to_string()).collect();
    originals.sort();
    assert_eq!(round_tripped, originals, "every name must round-trip");
}

/// The expected wire form of a name, spelled out here rather than borrowed
/// from the encoder under test.
fn encode_expecting(name: &str) -> String {
    let mut out = String::new();
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap();
            out.push(u8::from_str_radix(hex, 16).unwrap());
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

/// Statistics are a reading of a file that passed. A file that did not pass
/// must keep the failure protocol in every spelling, not gain a partial view.
#[test]
fn a_damaged_file_reports_no_statistics_in_any_spelling() {
    let dir = TempDir::new().unwrap();
    let path = fresh_v2(dir.path(), "broken.db");
    let mut bytes = fs::read(&path).unwrap();
    // Damage the catalog of the generation the checker will select, not of
    // whichever slot happens to sit first in the file.
    let a = btree_store::MetaNode::from_slice(&bytes[..40]);
    let b = btree_store::MetaNode::from_slice(&bytes[4096..4096 + 40]);
    let selected = if b.seq > a.seq { b } else { a };
    assert!(
        selected.catalog_root != 0,
        "the fixture must have a catalog"
    );
    bytes[selected.catalog_root as usize * 4096 + 64] ^= 0xFF;
    fs::write(&path, &bytes).unwrap();

    for extra in [
        vec![],
        vec!["--summary"],
        vec!["--scan"],
        vec!["--summary", "--scan"],
    ] {
        let mut args = vec!["check", path.to_str().unwrap()];
        args.extend(extra.iter().copied());
        let output = run(args);
        assert_eq!(code(&output), 1, "a damaged file must fail: {:?}", extra);
        // `error_class` asserts stdout is empty, which is the whole of the
        // contract for a failure: no statistics reach the reader.
        assert_eq!(error_class(&output), "failed");
    }
}

/// `file=` is a path a reader pastes into another command, so it keeps the
/// rendering it has always had. That rendering is lossy for a name that is not
/// UTF-8, which is why the encoding rule covers `bucket.name` only.
#[test]
fn the_file_line_keeps_its_path_rendering() {
    let dir = TempDir::new().unwrap();
    let path = fresh_v2(dir.path(), "utf8.db");
    let output = run(["check", path.to_str().unwrap()]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    let printed = stdout(&output);
    let line = printed
        .lines()
        .find(|line| line.starts_with("file="))
        .unwrap();
    assert_eq!(line, format!("file={}", path.display()));

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let odd = dir.path().join(std::ffi::OsStr::from_bytes(b"na\xffme.db"));
        #[cfg(target_os = "macos")]
        {
            // The filesystem, not the renderer, is what decides here: macOS
            // refuses to create the name, so there is no line to render.
            let refused = fs::write(&odd, b"");
            assert!(
                refused.is_err() && !odd.exists(),
                "macOS must refuse a name that is not valid UTF-8: {refused:?}"
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            fs::write(&odd, fs::read(&path).unwrap()).unwrap();
            let output = run([OsString::from("check"), odd.clone().into_os_string()]);
            assert_eq!(code(&output), 0, "{}", stderr(&output));
            let printed = stdout(&output);
            let line = printed
                .lines()
                .find(|line| line.starts_with("file="))
                .unwrap();
            assert_eq!(line, format!("file={}", odd.display()));
        }
    }
}

/// The rendering path must carry what the checker found, not a shape of it: a
/// prefix-encoded bucket, a value that overflows onto its own pages and a
/// bucket that was never written each have to reach the reader as themselves.
#[test]
fn a_scan_renders_each_buckets_own_numbers() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("shapes.db");
    {
        let tree = btree_store::BTree::open(&path).unwrap();
        tree.new_bucket("plain", false).unwrap();
        tree.new_bucket("prefixed", true).unwrap();
        tree.new_bucket("empty", false).unwrap();
        tree.exec("plain", |txn| txn.put(b"k", b"v")).unwrap();
        tree.exec("prefixed", |txn| {
            txn.put(b"a", vec![0x41; 10])?;
            txn.put(b"b", vec![0x42; 10_000])
        })
        .unwrap();
    }

    let output = run(["check", path.to_str().unwrap(), "--scan"]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    let printed = stdout(&output);
    let blocks: Vec<Vec<&str>> = printed
        .lines()
        .filter(|line| line.starts_with("bucket."))
        .collect::<Vec<_>>()
        .chunks(7)
        .map(|block| block.to_vec())
        .collect();
    assert_eq!(blocks.len(), 3, "one block per bucket: {printed}");

    let named = |name: &str| {
        blocks
            .iter()
            .find(|block| block[0] == format!("bucket.name={name}"))
            .unwrap_or_else(|| panic!("{name} must be reported"))
            .clone()
    };
    assert_eq!(
        named("plain"),
        [
            "bucket.name=plain",
            "bucket.prefix_encoding=false",
            "bucket.records=1",
            "bucket.logical_key_bytes=1",
            "bucket.logical_value_bytes=1",
            "bucket.tree_height=1",
            "bucket.reachable_pages=1",
        ]
    );
    // 10,000 value bytes need ceil(10000 / 4092) = 3 value pages, beside the
    // one node page.
    assert_eq!(
        named("prefixed"),
        [
            "bucket.name=prefixed",
            "bucket.prefix_encoding=true",
            "bucket.records=2",
            "bucket.logical_key_bytes=2",
            "bucket.logical_value_bytes=10010",
            "bucket.tree_height=1",
            "bucket.reachable_pages=4",
        ]
    );
    assert_eq!(
        named("empty"),
        [
            "bucket.name=empty",
            "bucket.prefix_encoding=false",
            "bucket.records=0",
            "bucket.logical_key_bytes=0",
            "bucket.logical_value_bytes=0",
            "bucket.tree_height=0",
            "bucket.reachable_pages=0",
        ]
    );
}

/// A warning does not fail a check, and the report says so after its statistics:
/// the whole key set and every bucket block come first, so a reader that stops
/// at the first unexpected line still has the report.
#[test]
fn warning_lines_follow_the_bucket_blocks() {
    let dir = TempDir::new().unwrap();
    let path = fresh_v2(dir.path(), "warned.db");
    let file = path.to_str().unwrap();
    // A torn second metadata slot is ignored, not fatal, which a passing check
    // reports as a warning.
    tear_meta_slot(&path, 1);

    for extra in [
        vec![],
        vec!["--summary"],
        vec!["--scan"],
        vec!["--summary", "--scan"],
    ] {
        let mut args = vec!["check", file];
        args.extend(extra.iter().copied());
        let output = run(args);
        assert_eq!(code(&output), 0, "{:?}: {}", extra, stderr(&output));
        let printed = stdout(&output);
        assert_eq!(lines_of(&printed)[0], "status=ok", "the report comes first");

        // The report is what a reader consumes line by line, so its terminator
        // matters: a missing final newline drops the last key for every reader
        // that splits on it.
        assert!(
            printed.ends_with('\n'),
            "the output must end with a newline: {printed:?}"
        );

        let lines = lines_of(&printed);
        let first_warning = lines
            .iter()
            .position(|line| line.starts_with("diagnostic: "))
            .unwrap_or_else(|| panic!("the torn slot must warn: {printed}"));
        assert!(
            lines[first_warning..]
                .iter()
                .all(|line| line.starts_with("diagnostic: ")),
            "warnings come last, after the report and after every bucket block: {printed}"
        );

        // The documented separator between the header and the first bucket is
        // the same in both scan spellings.
        if extra.contains(&"--scan") {
            assert!(
                printed.contains("bucket_scan=true\n\nbucket.name="),
                "the first bucket block is separated by one blank line: {printed:?}"
            );
        }
    }
}

fn lines_of(printed: &str) -> Vec<&str> {
    printed.lines().filter(|line| !line.is_empty()).collect()
}

#[test]
fn failed_check_prints_one_bounded_diagnostic_and_no_summary() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("unowned.db");
    let mut meta = btree_store::MetaNode::new();
    meta.next_page_id = 512;
    meta.update_checksum();
    let mut bytes = vec![0; 512 * common::PAGE_SIZE];
    for start in [0, common::PAGE_SIZE] {
        bytes[start..start + 40].copy_from_slice(meta.as_page_slice());
    }
    fs::write(&path, bytes).unwrap();
    let output = run(["check", path.to_str().unwrap()]);
    assert_eq!(code(&output), 1);
    assert!(stdout(&output).is_empty());
    let stderr = stderr(&output);
    let lines: Vec<_> = stderr.lines().collect();
    assert_eq!(lines.len(), 2, "{stderr}");
    assert_eq!(lines[0], "error: check: failed: 1 diagnostic(s)");
    assert_eq!(
        lines[1],
        "diagnostic: severity=error code=OWNERSHIP_UNACCOUNTED check=every physical data page must have exactly one ownership class generation=1 pid=2 path=data-id-space expected=one owner actual=510%20page%28s%29%20with%20no%20owner"
    );
}
