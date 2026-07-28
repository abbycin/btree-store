use btree_store::{
    BTree, BucketError, CorruptionReport, Error, KeyError, OpenError, OpenOptions, OpenResult,
    Result,
};
use tempfile::TempDir;

fn classify_runtime_error(error: Error) -> &'static str {
    match error {
        Error::KeyNotFound => "key-not-found",
        Error::BucketNotFound => "bucket-not-found",
        Error::BucketExists => "bucket-exists",
        Error::InvalidKey(KeyError::Empty | KeyError::TooLarge { .. }) => "invalid-key",
        Error::InvalidBucket(BucketError::Empty | BucketError::TooLarge { .. }) => "invalid-bucket",
        Error::ValueTooLarge { .. } => "value-too-large",
    }
}

fn classify_open_error(error: OpenError) -> &'static str {
    match error {
        OpenError::Io(_) => "io",
        OpenError::Corruption(_) => "corruption",
        OpenError::InvalidOptions(_) => "invalid-options",
        OpenError::DatabaseBusy { .. } => "database-busy",
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CallerError {
    Runtime(Error),
}

#[test]
fn open_and_runtime_error_surfaces_are_distinct() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("surface.db");

    let opened: OpenResult<BTree> = BTree::open(&path);
    let tree = opened.unwrap();
    let reopened: std::result::Result<BTree, OpenError> = OpenOptions::default().open(&path);
    assert!(reopened.is_ok());

    let missing: Result<Vec<u8>> = tree
        .view("missing", |txn| txn.get(b"key"))
        .map_err(|error: Error| error);
    assert_eq!(missing.unwrap_err(), Error::BucketNotFound);
    assert_eq!(classify_runtime_error(Error::KeyNotFound), "key-not-found");
    assert_eq!(classify_runtime_error(Error::BucketExists), "bucket-exists");
    assert_eq!(
        classify_open_error(OpenError::Corruption(CorruptionReport {
            code: "TEST_CORRUPTION",
            generation: None,
            page_kind: "meta",
            pid: None,
            check: "test corruption",
            expected: None,
            actual: None,
        })),
        "corruption"
    );
}

#[test]
fn new_bucket_conflicts_are_reported_as_bucket_exists() {
    let dir = TempDir::new().unwrap();
    let tree = BTree::open(dir.path().join("bucket-exists.db")).unwrap();

    tree.new_bucket("dup", false).unwrap();
    assert_eq!(tree.new_bucket("dup", false), Err(Error::BucketExists));
    assert_eq!(classify_runtime_error(Error::BucketExists), "bucket-exists");
}

#[test]
fn prefix_encoding_flag_survives_writes_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("flag-survives.db");

    {
        let tree = BTree::open(&db_path).unwrap();
        tree.new_bucket("encoded", true).unwrap();
        tree.exec("encoded", |txn| txn.put(b"k1", b"v1")).unwrap();
    }
    {
        let tree = BTree::open(&db_path).unwrap();
        tree.exec("encoded", |txn| txn.put(b"k2", b"v2")).unwrap();
    }
    let tree = BTree::open(&db_path).unwrap();
    tree.view("encoded", |txn| {
        assert_eq!(txn.get(b"k1").unwrap(), b"v1".to_vec());
        assert_eq!(txn.get(b"k2").unwrap(), b"v2".to_vec());
        Ok(())
    })
    .unwrap();
}

#[test]
fn transaction_errors_are_engine_errors_and_can_be_mapped_afterward() {
    let dir = TempDir::new().unwrap();
    let tree = BTree::open(dir.path().join("caller-errors.db")).unwrap();
    tree.new_bucket("bucket", false).unwrap();

    let exec: btree_store::Result<()> = tree.exec("bucket", |txn| {
        txn.put(b"key", b"value")?;
        Err(Error::KeyNotFound)
    });
    let exec = exec.map_err(CallerError::Runtime);
    assert_eq!(exec, Err(CallerError::Runtime(Error::KeyNotFound)));

    tree.exec("bucket", |txn| txn.put(b"key", b"value"))
        .unwrap();
    let view = tree
        .view("bucket", |_| Err::<(), _>(Error::KeyNotFound))
        .map_err(CallerError::Runtime);
    assert_eq!(view, Err(CallerError::Runtime(Error::KeyNotFound)));

    let multi = tree.exec_multi(|multi| {
        multi.exec("bucket", |txn| {
            txn.put(b"inner", b"value")?;
            Err(Error::KeyNotFound)
        })?;
        Ok(())
    });
    let multi = multi.map_err(CallerError::Runtime);
    assert_eq!(multi, Err(CallerError::Runtime(Error::KeyNotFound)));

    let outer: std::result::Result<(), CallerError> = tree
        .exec_multi(|_| Err(Error::KeyNotFound))
        .map_err(CallerError::Runtime);
    assert_eq!(outer, Err(CallerError::Runtime(Error::KeyNotFound)));
}
