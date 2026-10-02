//! Publication failure-ladder acceptance: the CLI-owned boundaries after the
//! staging file exists.
//!
//! These cases need a deterministic failure before and after the swap that
//! publishes the staged file, so they run against a binary built with the
//! `test-fault-injection` feature (which `cargo test --all-features` enables and a
//! released binary never has). The observable contract under test is the class,
//! the exit code, and what is left on disk: the swap is one rename, so it either
//! did not happen (destination as it was, staged file left for the caller to
//! remove) or it did (destination complete, staged name consumed).

#![cfg(feature = "test-fault-injection")]

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_btree-store");
const FAULT: &str = "BTREE_STORE_MIGRATE_FAULT";

fn repo_file(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn migrate(source: &Path, destination: &Path, fault: &str) -> Output {
    let mut command: Command = common::child_test_command(Path::new(BIN));
    command
        .args(["migrate"])
        .arg(source)
        .arg("--output")
        .arg(destination)
        .args(["--to", "2"])
        .env(FAULT, fault)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("the CLI must run")
}

fn class_of(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let line = stderr.lines().next().unwrap_or_default();
    let mut parts = line.splitn(4, ": ");
    assert_eq!(parts.next(), Some("error"), "unexpected error line: {line}");
    parts.next();
    parts.next().unwrap_or_default().to_string()
}

fn staging_files(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().contains(".migrate-"))
                .unwrap_or(false)
        })
        .collect()
}

/// Sets up a migration that will reach the publication stage.
fn fixture() -> (TempDir, PathBuf, PathBuf) {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source.v1");
    fs::copy(repo_file("tests/fixtures/v1_shapes.v1"), &source).unwrap();
    let destination = dir.path().join("out.db");
    (dir, source, destination)
}

#[test]
fn a_failure_before_the_swap_leaves_no_target_and_removes_the_staging() {
    for fault in ["rebuild", "compare", "sync", "rename"] {
        let (dir, source, destination) = fixture();
        let output = migrate(&source, &destination, fault);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(1),
            "fault {fault}: stderr: {stderr}"
        );
        assert_eq!(class_of(&output), "io", "fault {fault}: {stderr}");
        assert!(
            !destination.exists(),
            "fault {fault} must not publish a target"
        );
        assert!(
            staging_files(dir.path()).is_empty(),
            "fault {fault} must remove its own staging: {:?}",
            staging_files(dir.path())
        );
        assert!(
            stderr.contains("staging") && stderr.contains("(already removed)"),
            "fault {fault} must name the staging file and that it was removed: {stderr}"
        );
    }
}

/// The ladder's one post-swap rung: the rename happened, so the destination holds
/// the complete file and the staging name is gone with the step that consumed it.
#[test]
fn a_failed_directory_sync_after_the_swap_leaves_the_target_in_place() {
    let (dir, source, destination) = fixture();
    let output = migrate(&source, &destination, "dirsync1");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    assert_eq!(
        class_of(&output),
        "published-durability-unknown",
        "{stderr}"
    );
    assert!(
        destination.exists(),
        "the target is in place; only its durability is unconfirmed"
    );
    assert!(
        staging_files(dir.path()).is_empty(),
        "the swap consumed the staging name: {:?}",
        staging_files(dir.path())
    );
    let check = common::child_test_command(Path::new(BIN))
        .args(["check"])
        .arg(&destination)
        .output()
        .expect("the CLI must run");
    assert_eq!(
        check.status.code(),
        Some(0),
        "what reached the destination is complete: {}",
        String::from_utf8_lossy(&check.stderr)
    );
}

/// The staging file exists but holds no database yet: the tool owns it, so the
/// failure has to remove it and still name it rather than leak it.
#[test]
fn a_failed_staging_initialization_leaves_no_staging_file() {
    let (dir, source, destination) = fixture();
    let output = migrate(&source, &destination, "staging-init");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "stderr: {stderr}");
    assert_eq!(class_of(&output), "io", "{stderr}");
    assert!(
        stderr.contains("staging: io: initializing the staging database"),
        "the failure must come from initializing a created staging file: {stderr}"
    );
    assert!(
        stderr.contains("staging") && stderr.contains("(already removed)"),
        "the failure must name the staging file and that it was removed: {stderr}"
    );
    assert!(
        staging_files(dir.path()).is_empty(),
        "a failed initialization must remove its staging file: {:?}",
        staging_files(dir.path())
    );
    assert!(!destination.exists());
}
