mod common;

use btree_store::{BTree, OpenError};
use common::child_test_command;
use std::io::{BufRead, BufReader, Write};
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;

// Keep the subprocess lock test isolated so its parent cannot inherit database
// descriptors opened by unrelated tests in the same integration-test binary.
const LOCK_CHILD_PATH: &str = "BTREE_STORE_LOCK_CHILD_PATH";

fn open_error(path: &std::path::Path, context: &str) -> OpenError {
    match BTree::open(path) {
        Ok(_) => panic!("{context}"),
        Err(error) => error,
    }
}

#[test]
#[ignore = "subprocess target that holds the exclusive database lock"]
fn database_lock_child() {
    let Ok(path) = std::env::var(LOCK_CHILD_PATH) else {
        return;
    };
    let _tree = BTree::open(path).unwrap();
    println!("ready");
    std::io::stdout().flush().unwrap();
    std::thread::sleep(Duration::from_secs(30));
}

#[test]
fn second_process_gets_database_busy_before_handle_publication() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("busy.db");
    let mut child = child_test_command(&std::env::current_exe().unwrap())
        .args(["--exact", "database_lock_child", "--ignored", "--nocapture"])
        .env(LOCK_CHILD_PATH, &path)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let mut reader = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            reader.read_line(&mut line).unwrap(),
            0,
            "lock child exited early"
        );
        if line.contains("ready") {
            break;
        }
    }

    assert!(matches!(
        open_error(&path, "second process must not publish a competing handle"),
        OpenError::DatabaseBusy { .. }
    ));

    child.kill().unwrap();
    let _ = child.wait();
}
