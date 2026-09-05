#![no_main]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};
use btree_store::{BTree, Error, OpenResult, Result as DbResult};
use libfuzzer_sys::fuzz_target;
use tempfile::{Builder, TempDir};

mod common;

use common::{BUCKET_NAMES, Bucket, MAX_KEY_LEN, Value, arbitrary_vec, bounded_nonempty_bytes};

const EPOCH_KEY: &[u8] = b"__epoch__";
const MAX_CONCURRENT_OPS: usize = 96;
const MAX_SINGLE_STEPS: usize = 8;
const MAX_MULTI_STEPS: usize = 8;
const MAX_RACE_READERS: usize = 4;
const MAX_CONCURRENT_WRITERS: usize = 4;

const MIXED_WRITER_BUCKETS: [Bucket; MAX_CONCURRENT_WRITERS] = [
    Bucket::new("a"),
    Bucket::new("b"),
    Bucket::new("c"),
    Bucket::new("users"),
];

#[derive(Clone, Debug)]
struct ConcurrentCase {
    ops: Vec<ConcurrentOp>,
}

impl<'a> Arbitrary<'a> for ConcurrentCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        Ok(Self {
            ops: arbitrary_vec(u, MAX_CONCURRENT_OPS)?,
        })
    }
}

#[derive(Clone, Debug)]
enum ConcurrentOp {
    Single {
        bucket: Bucket,
        steps: Vec<SingleStep>,
        readers: Vec<ReaderPlan>,
    },
    Multi {
        steps: Vec<MultiStep>,
        readers: Vec<ReaderPlan>,
    },
    Mixed {
        writers: Vec<WriterPlan>,
        readers: Vec<ReaderPlan>,
    },
    Reopen,
    Validate,
}

impl<'a> Arbitrary<'a> for ConcurrentOp {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=4u8)? {
            0 => Ok(Self::Single {
                bucket: Bucket::arbitrary(u)?,
                steps: arbitrary_vec(u, MAX_SINGLE_STEPS)?,
                readers: arbitrary_vec(u, MAX_RACE_READERS)?,
            }),
            1 => Ok(Self::Multi {
                steps: arbitrary_vec(u, MAX_MULTI_STEPS)?,
                readers: arbitrary_vec(u, MAX_RACE_READERS)?,
            }),
            2 => Ok(Self::Mixed {
                writers: arbitrary_vec(u, MAX_CONCURRENT_WRITERS)?,
                readers: arbitrary_vec(u, MAX_RACE_READERS)?,
            }),
            3 => Ok(Self::Reopen),
            _ => Ok(Self::Validate),
        }
    }
}

#[derive(Clone, Debug)]
enum SingleStep {
    Put(DataKey, Value),
    Update(DataKey, Value),
    Del(DataKey),
    Touch,
}

impl<'a> Arbitrary<'a> for SingleStep {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=3u8)? {
            0 => Ok(Self::Put(DataKey::arbitrary(u)?, Value::arbitrary(u)?)),
            1 => Ok(Self::Update(DataKey::arbitrary(u)?, Value::arbitrary(u)?)),
            2 => Ok(Self::Del(DataKey::arbitrary(u)?)),
            _ => Ok(Self::Touch),
        }
    }
}

#[derive(Clone, Debug)]
struct WriterPlan {
    steps: Vec<SingleStep>,
}

impl<'a> Arbitrary<'a> for WriterPlan {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        Ok(Self {
            steps: arbitrary_vec(u, MAX_SINGLE_STEPS)?,
        })
    }
}

#[derive(Clone, Debug)]
enum MultiStep {
    Put(Bucket, DataKey, Value),
    Update(Bucket, DataKey, Value),
    Del(Bucket, DataKey),
    Touch(Bucket),
}

impl<'a> Arbitrary<'a> for MultiStep {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=3u8)? {
            0 => Ok(Self::Put(
                Bucket::arbitrary(u)?,
                DataKey::arbitrary(u)?,
                Value::arbitrary(u)?,
            )),
            1 => Ok(Self::Update(
                Bucket::arbitrary(u)?,
                DataKey::arbitrary(u)?,
                Value::arbitrary(u)?,
            )),
            2 => Ok(Self::Del(Bucket::arbitrary(u)?, DataKey::arbitrary(u)?)),
            _ => Ok(Self::Touch(Bucket::arbitrary(u)?)),
        }
    }
}

#[derive(Clone, Debug)]
struct ReaderPlan {
    bucket: Bucket,
    mode: ReaderMode,
}

impl<'a> Arbitrary<'a> for ReaderPlan {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        Ok(Self {
            bucket: Bucket::arbitrary(u)?,
            mode: ReaderMode::arbitrary(u)?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReaderMode {
    StaleClone,
    CloneAtRead,
    SamePathOpen,
    /// Uses the harness's original handle so reader and writer share start_seq.
    SharedHandle,
    /// Holds one view open across the writer commits and re-reads from the
    /// same fixed snapshot afterwards (MVCC read/write non-blocking).
    HeldView,
}

impl<'a> Arbitrary<'a> for ReaderMode {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=4u8)? {
            0 => Ok(Self::StaleClone),
            1 => Ok(Self::CloneAtRead),
            2 => Ok(Self::SamePathOpen),
            3 => Ok(Self::HeldView),
            _ => Ok(Self::SharedHandle),
        }
    }
}

enum ReaderHandle<'a> {
    Owned(BTree),
    Shared(&'a BTree),
}

impl ReaderHandle<'_> {
    fn as_ref(&self) -> &BTree {
        match self {
            Self::Owned(tree) => tree,
            Self::Shared(tree) => tree,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DataKey(Vec<u8>);

impl<'a> Arbitrary<'a> for DataKey {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        let mut key = bounded_nonempty_bytes(u, MAX_KEY_LEN)?;
        if key == EPOCH_KEY {
            key = b"epoch-user-key".to_vec();
        }
        Ok(Self(key))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct BucketModel {
    epoch: u64,
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Model {
    buckets: BTreeMap<String, BucketModel>,
}

impl Model {
    fn ensure_bucket(&mut self, bucket: &str) -> &mut BucketModel {
        self.buckets.entry(bucket.to_string()).or_default()
    }

    fn apply_single(&self, bucket: &str, steps: &[SingleStep]) -> DbResult<Self> {
        let mut next = self.clone();
        let bucket_model = next.ensure_bucket(bucket);
        let mut touched = false;
        for step in steps {
            match step {
                SingleStep::Put(key, value) => {
                    bucket_model.entries.insert(key.0.clone(), value.0.clone());
                    touched = true;
                }
                SingleStep::Update(key, value) => {
                    if let Some(current) = bucket_model.entries.get_mut(&key.0) {
                        *current = value.0.clone();
                        touched = true;
                    }
                }
                SingleStep::Del(key) => {
                    if bucket_model.entries.remove(&key.0).is_none() {
                        return Err(Error::KeyNotFound);
                    }
                    touched = true;
                }
                SingleStep::Touch => {}
            }
        }
        if touched {
            bucket_model.epoch = bucket_model.epoch.saturating_add(1);
        }
        Ok(next)
    }

    fn apply_multi(&self, steps: &[MultiStep]) -> DbResult<Self> {
        let mut next = self.clone();
        let mut touched = BTreeSet::new();
        for step in steps {
            match step {
                MultiStep::Put(bucket, key, value) => {
                    let bucket_model = next.ensure_bucket(bucket.as_str());
                    bucket_model.entries.insert(key.0.clone(), value.0.clone());
                    touched.insert(bucket.as_str().to_string());
                }
                MultiStep::Update(bucket, key, value) => {
                    let bucket_model = next.ensure_bucket(bucket.as_str());
                    if let Some(current) = bucket_model.entries.get_mut(&key.0) {
                        *current = value.0.clone();
                        touched.insert(bucket.as_str().to_string());
                    }
                }
                MultiStep::Del(bucket, key) => {
                    let bucket_model = next
                        .buckets
                        .get_mut(bucket.as_str())
                        .ok_or(Error::KeyNotFound)?;
                    if bucket_model.entries.remove(&key.0).is_none() {
                        return Err(Error::KeyNotFound);
                    }
                    touched.insert(bucket.as_str().to_string());
                }
                MultiStep::Touch(bucket) => {
                    next.ensure_bucket(bucket.as_str());
                }
            }
        }
        for bucket in touched {
            let bucket_model = next.ensure_bucket(&bucket);
            bucket_model.epoch = bucket_model.epoch.saturating_add(1);
        }
        Ok(next)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BucketSnapshot {
    Missing,
    Present {
        epoch: u64,
        entries: BTreeMap<Vec<u8>, Vec<u8>>,
    },
}

impl BucketSnapshot {
    fn from_model(model: &Model, bucket: &str) -> Self {
        match model.buckets.get(bucket) {
            Some(bucket_model) => Self::Present {
                epoch: bucket_model.epoch,
                entries: bucket_model.entries.clone(),
            },
            None => Self::Missing,
        }
    }
}

struct ConcurrentHarness {
    _dir: TempDir,
    path: PathBuf,
    db: Option<BTree>,
    model: Model,
}

impl ConcurrentHarness {
    fn new() -> Self {
        let dir = Builder::new()
            .prefix("btree-store-concurrent-fuzz-")
            .tempdir()
            .expect("create fuzz tempdir");
        let path = dir.path().join("db.btree");
        let db = expect_open_ok(BTree::open(&path), "open database");
        let mut model = Model::default();
        // exec/multi now require the bucket to exist, so every bucket the
        // harness can touch is created up front; untouched buckets stay at
        // epoch 0 with no entries.
        for bucket in BUCKET_NAMES {
            expect_db_ok(db.new_bucket(bucket, false), "pre-create bucket");
            model.ensure_bucket(bucket);
        }
        Self {
            _dir: dir,
            path,
            db: Some(db),
            model,
        }
    }

    fn db(&self) -> &BTree {
        self.db
            .as_ref()
            .expect("concurrent harness database should be open")
    }

    fn validate(&self) {
        validate_db(self.db(), &self.model);
    }

    fn reopen(&mut self) {
        let old = self.db.take();
        drop(old);
        self.db = Some(expect_open_ok(
            BTree::open(&self.path),
            "reopen concurrent database",
        ));
        self.validate();
    }

    // Disjoint buckets keep each writer's before/after state independent of
    // the serialized commit order while still exercising concurrent exec calls.
    fn race_mixed(&mut self, writers: &[WriterPlan], readers: &[ReaderPlan]) {
        let before = self.model.clone();
        let expected: Vec<DbResult<Model>> = writers
            .iter()
            .enumerate()
            .map(|(index, writer)| {
                before.apply_single(mixed_writer_bucket(index).as_str(), &writer.steps)
            })
            .collect();
        let stale = self.db().clone();
        let shared_handle = self.db();
        let path = self.path.clone();
        let barrier = Arc::new(Barrier::new(writers.len() + readers.len() + 1));
        let completed = Arc::new(AtomicUsize::new(0));

        thread::scope(|scope| {
            for (index, writer) in writers.iter().enumerate() {
                let writer = writer.clone();
                let db = self.db().clone();
                let barrier = barrier.clone();
                let completed = completed.clone();
                let bucket = mixed_writer_bucket(index);
                let expected_result = expected[index].clone();
                scope.spawn(move || {
                    barrier.wait();
                    let marker_model = expected_result.clone();
                    let actual = db.exec(bucket.as_str(), |txn| {
                        for step in &writer.steps {
                            apply_single_step(txn, step)?;
                        }
                        if let Ok(model) = marker_model.as_ref()
                            && let Some(bucket_model) = model.buckets.get(bucket.as_str())
                        {
                            txn.put(EPOCH_KEY, bucket_model.epoch.to_le_bytes())?;
                        }
                        Ok(())
                    });
                    match expected_result {
                        Ok(_) => {
                            expect_db_ok(actual, "mixed concurrent writer commit");
                        }
                        Err(expected_err) => {
                            expect_db_err(actual, expected_err, "mixed concurrent writer rollback");
                        }
                    }
                    completed.fetch_add(1, Ordering::Release);
                });
            }

            for reader in readers {
                let reader = reader.clone();
                let barrier = barrier.clone();
                let path = path.clone();
                let base = self.db().clone();
                let stale = stale.clone();
                let before_state = BucketSnapshot::from_model(&before, reader.bucket.as_str());
                let after_state = expected
                    .iter()
                    .enumerate()
                    .find(|(index, _)| {
                        mixed_writer_bucket(*index).as_str() == reader.bucket.as_str()
                    })
                    .and_then(|(_, result)| result.as_ref().ok())
                    .map(|model| BucketSnapshot::from_model(model, reader.bucket.as_str()))
                    .unwrap_or_else(|| before_state.clone());
                let completed = completed.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let handle = match reader.mode {
                        ReaderMode::StaleClone => ReaderHandle::Owned(stale),
                        ReaderMode::CloneAtRead => ReaderHandle::Owned(base.clone()),
                        ReaderMode::SamePathOpen => ReaderHandle::Owned(
                            expect_open_ok(BTree::open(&path), "same-path open during mixed race"),
                        ),
                        ReaderMode::HeldView => ReaderHandle::Owned(base.clone()),
                        ReaderMode::SharedHandle => ReaderHandle::Shared(shared_handle),
                    };
                    if reader.mode == ReaderMode::HeldView {
                        let first = expect_db_ok(
                            read_bucket_snapshot_held(
                                handle.as_ref(),
                                reader.bucket.as_str(),
                                &completed,
                                writers.len(),
                            ),
                            "held view read",
                        );
                        assert!(
                            first == before_state || first == after_state,
                            "held view observed unexpected snapshot: actual={first:?} before={before_state:?} after={after_state:?} bucket={:?}",
                            reader.bucket
                        );
                    } else {
                        let actual = expect_db_ok(
                            read_bucket_snapshot(handle.as_ref(), reader.bucket.as_str()),
                            "read mixed bucket",
                        );
                        assert!(
                            actual == before_state || actual == after_state,
                            "reader observed unexpected mixed snapshot: actual={actual:?} before={before_state:?} after={after_state:?} mode={:?} bucket={:?}",
                            reader.mode,
                            reader.bucket
                        );
                    }
                });
            }

            barrier.wait();
        });

        for (index, result) in expected.into_iter().enumerate() {
            if let Ok(next) = result {
                let bucket_name = mixed_writer_bucket(index).as_str().to_string();
                let bucket_model = next
                    .buckets
                    .get(&bucket_name)
                    .cloned()
                    .expect("successful mixed writer must materialize its bucket");
                self.model.buckets.insert(bucket_name, bucket_model);
            }
        }
        self.reopen();
    }

    fn race_single(&mut self, bucket: Bucket, steps: &[SingleStep], readers: &[ReaderPlan]) {
        let before = self.model.clone();
        let expected = before.apply_single(bucket.as_str(), steps);
        let stale = self.db().clone();
        let shared_handle = self.db();
        let path = self.path.clone();
        let barrier = Arc::new(Barrier::new(readers.len() + 1));
        let completed = Arc::new(AtomicUsize::new(0));

        thread::scope(|scope| {
            for reader in readers {
                let reader = reader.clone();
                let barrier = barrier.clone();
                let path = path.clone();
                let base = self.db().clone();
                let stale = stale.clone();
                let before_state = BucketSnapshot::from_model(&before, reader.bucket.as_str());
                let after_state = expected
                    .as_ref()
                    .map(|model| BucketSnapshot::from_model(model, reader.bucket.as_str()))
                    .unwrap_or_else(|_| before_state.clone());
                let completed = completed.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let handle = match reader.mode {
                        ReaderMode::StaleClone => ReaderHandle::Owned(stale),
                        ReaderMode::CloneAtRead => ReaderHandle::Owned(base.clone()),
                        ReaderMode::SamePathOpen => ReaderHandle::Owned(
                            expect_open_ok(BTree::open(&path), "same-path open during race"),
                        ),
                        ReaderMode::HeldView => ReaderHandle::Owned(base.clone()),
                        ReaderMode::SharedHandle => ReaderHandle::Shared(shared_handle),
                    };
                    if reader.mode == ReaderMode::HeldView {
                        let first = expect_db_ok(
                            read_bucket_snapshot_held(
                                handle.as_ref(),
                                reader.bucket.as_str(),
                                &completed,
                                1,
                            ),
                            "held view read",
                        );
                        assert!(
                            first == before_state || first == after_state,
                            "held view observed unexpected snapshot: actual={first:?} before={before_state:?} after={after_state:?} bucket={:?}",
                            reader.bucket
                        );
                    } else {
                        let actual = expect_db_ok(
                            read_bucket_snapshot(handle.as_ref(), reader.bucket.as_str()),
                            "read raced bucket",
                        );
                        assert!(
                            actual == before_state || actual == after_state,
                            "reader observed unexpected bucket snapshot: actual={actual:?} before={before_state:?} after={after_state:?} mode={:?} bucket={:?}",
                            reader.mode,
                            reader.bucket
                        );
                    }
                });
            }

            barrier.wait();
            let actual = self.db().exec(bucket.as_str(), |txn| {
                for step in steps {
                    apply_single_step(txn, step)?;
                }
                if let Ok(model) = expected.as_ref()
                    && let Some(bucket_model) = model.buckets.get(bucket.as_str())
                {
                    txn.put(EPOCH_KEY, bucket_model.epoch.to_le_bytes())?;
                }
                Ok(())
            });
            match &expected {
                Ok(_) => {
                    expect_db_ok(actual, "single-bucket concurrent commit");
                }
                Err(expected_err) => {
                    expect_db_err(
                        actual,
                        expected_err.clone(),
                        "single-bucket concurrent commit",
                    );
                }
            }
            completed.fetch_add(1, Ordering::Release);
        });

        if let Ok(next) = expected {
            self.model = next;
        }
        self.reopen();
    }

    fn race_multi(&mut self, steps: &[MultiStep], readers: &[ReaderPlan]) {
        let before = self.model.clone();
        let expected = before.apply_multi(steps);
        let stale = self.db().clone();
        let shared_handle = self.db();
        let path = self.path.clone();
        let barrier = Arc::new(Barrier::new(readers.len() + 1));
        let completed = Arc::new(AtomicUsize::new(0));

        thread::scope(|scope| {
            for reader in readers {
                let reader = reader.clone();
                let barrier = barrier.clone();
                let path = path.clone();
                let base = self.db().clone();
                let stale = stale.clone();
                let before_state = BucketSnapshot::from_model(&before, reader.bucket.as_str());
                let after_state = expected
                    .as_ref()
                    .map(|model| BucketSnapshot::from_model(model, reader.bucket.as_str()))
                    .unwrap_or_else(|_| before_state.clone());
                let completed = completed.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let handle = match reader.mode {
                        ReaderMode::StaleClone => ReaderHandle::Owned(stale),
                        ReaderMode::CloneAtRead => ReaderHandle::Owned(base.clone()),
                        ReaderMode::SamePathOpen => ReaderHandle::Owned(
                            expect_open_ok(BTree::open(&path), "same-path open during multi race"),
                        ),
                        ReaderMode::HeldView => ReaderHandle::Owned(base.clone()),
                        ReaderMode::SharedHandle => ReaderHandle::Shared(shared_handle),
                    };
                    if reader.mode == ReaderMode::HeldView {
                        let first = expect_db_ok(
                            read_bucket_snapshot_held(
                                handle.as_ref(),
                                reader.bucket.as_str(),
                                &completed,
                                1,
                            ),
                            "held view read",
                        );
                        assert!(
                            first == before_state || first == after_state,
                            "held view observed unexpected snapshot: actual={first:?} before={before_state:?} after={after_state:?} bucket={:?}",
                            reader.bucket
                        );
                    } else {
                        let actual = expect_db_ok(
                            read_bucket_snapshot(handle.as_ref(), reader.bucket.as_str()),
                            "read raced multi bucket",
                        );
                        assert!(
                            actual == before_state || actual == after_state,
                            "reader observed unexpected multi-bucket snapshot: actual={actual:?} before={before_state:?} after={after_state:?} mode={:?} bucket={:?}",
                            reader.mode,
                            reader.bucket
                        );
                    }
                });
            }

            barrier.wait();
            let actual = self.db().exec_multi(|multi| {
                for step in steps {
                    multi.exec(multi_step_bucket(step), |txn| apply_multi_step(txn, step))?;
                }

                let expected_model = expected.as_ref().ok();
                let buckets: BTreeSet<String> = steps
                    .iter()
                    .map(|step| multi_step_bucket(step).to_string())
                    .collect();
                for bucket in buckets {
                    let Some(next_epoch) = expected_model
                        .and_then(|model| model.buckets.get(bucket.as_str()))
                        .map(|bucket_model| bucket_model.epoch)
                    else {
                        continue;
                    };
                    multi.exec(&bucket, |txn| txn.put(EPOCH_KEY, next_epoch.to_le_bytes()))?;
                }
                Ok(())
            });
            match &expected {
                Ok(_) => {
                    expect_db_ok(actual, "multi-bucket concurrent commit");
                }
                Err(expected_err) => {
                    expect_db_err(
                        actual,
                        expected_err.clone(),
                        "multi-bucket concurrent commit",
                    );
                }
            }
            completed.fetch_add(1, Ordering::Release);
        });

        if let Ok(next) = expected {
            self.model = next;
        }
        self.reopen();
    }
}

fn apply_single_step(txn: &mut btree_store::Txn<'_>, step: &SingleStep) -> DbResult<()> {
    match step {
        SingleStep::Put(key, value) => txn.put(&key.0, &value.0),
        SingleStep::Update(key, value) => {
            txn.update(&key.0, &value.0)?;
            Ok(())
        }
        SingleStep::Del(key) => txn.del(&key.0),
        SingleStep::Touch => Ok(()),
    }
}

fn mixed_writer_bucket(index: usize) -> Bucket {
    MIXED_WRITER_BUCKETS[index]
}

fn apply_multi_step(txn: &mut btree_store::Txn<'_>, step: &MultiStep) -> DbResult<()> {
    match step {
        MultiStep::Put(_, key, value) => txn.put(&key.0, &value.0),
        MultiStep::Update(_, key, value) => {
            txn.update(&key.0, &value.0)?;
            Ok(())
        }
        MultiStep::Del(_, key) => txn.del(&key.0),
        MultiStep::Touch(_) => Ok(()),
    }
}

fn multi_step_bucket(step: &MultiStep) -> &str {
    match step {
        MultiStep::Put(bucket, _, _) => bucket.as_str(),
        MultiStep::Update(bucket, _, _) => bucket.as_str(),
        MultiStep::Del(bucket, _) => bucket.as_str(),
        MultiStep::Touch(bucket) => bucket.as_str(),
    }
}

fn snapshot_from_txn(txn: &btree_store::ReadOnlyTxn<'_>) -> BucketSnapshot {
    let mut iter = txn.iter();
    let mut key_buf = Vec::new();
    let mut val_buf = Vec::new();
    let mut epoch = None;
    let mut entries = BTreeMap::new();
    while iter.next_ref(&mut key_buf, &mut val_buf) {
        if key_buf == EPOCH_KEY {
            if val_buf.len() != std::mem::size_of::<u64>() {
                panic!("epoch value has invalid length {}", val_buf.len());
            }
            epoch = Some(u64::from_le_bytes(val_buf.as_slice().try_into().unwrap()));
        } else {
            entries.insert(key_buf.clone(), val_buf.clone());
        }
    }
    BucketSnapshot::Present {
        epoch: epoch.unwrap_or(0),
        entries,
    }
}

fn read_bucket_snapshot(db: &BTree, bucket: &str) -> DbResult<BucketSnapshot> {
    match db.view(bucket, |txn| Ok(snapshot_from_txn(txn))) {
        Ok(snapshot) => Ok(snapshot),
        Err(Error::BucketNotFound) => Ok(BucketSnapshot::Missing),
        Err(err) => Err(err),
    }
}

/// Probe keys read per interleaved pass while a held view spans writer
/// commits: a bounded deterministic spread over the fixed snapshot's key set.
const HELD_VIEW_PROBE_KEYS: usize = 8;
/// Minimum interleaved passes even when the writers finish immediately, so
/// the reader always touches the fixed snapshot more than twice.
const HELD_VIEW_MIN_PROBES: usize = 4;

/// Reads a bucket snapshot, then holds the same view open while writers
/// publish new generations (tracked by `completed`) and repeatedly probes a
/// deterministic key spread: the fixed snapshot must not change at any point,
/// which catches premature page reuse (an epoch-gate violation) far more
/// often than a single before/after comparison. Writers increment `completed`
/// only after their exec, so the final full read is ordered after every
/// commit; the probes land inside the scan-to-publish / promotion windows.
fn read_bucket_snapshot_held(
    db: &BTree,
    bucket: &str,
    completed: &AtomicUsize,
    writer_count: usize,
) -> DbResult<BucketSnapshot> {
    match db.view(bucket, |txn| {
        let first = snapshot_from_txn(txn);
        let probe_pairs: Vec<(Vec<u8>, Vec<u8>)> = match &first {
            BucketSnapshot::Present { entries, .. } => {
                entries.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            }
            BucketSnapshot::Missing => Vec::new(),
        };
        let mut offset = 0usize;
        let mut probes = 0usize;
        while completed.load(Ordering::Acquire) < writer_count || probes < HELD_VIEW_MIN_PROBES {
            if !probe_pairs.is_empty() {
                for _ in 0..HELD_VIEW_PROBE_KEYS {
                    let (key, expected) = &probe_pairs[offset % probe_pairs.len()];
                    offset = offset.wrapping_add(1);
                    let actual = txn
                        .get(key)
                        .unwrap_or_else(|_| panic!("held view lost key {key:?}"));
                    assert_eq!(
                        actual, *expected,
                        "held view must keep its fixed snapshot across the writer commit"
                    );
                }
            }
            probes += 1;
            thread::yield_now();
        }
        let final_read = snapshot_from_txn(txn);
        assert_eq!(
            first, final_read,
            "held view must keep its fixed snapshot across the writer commit"
        );
        Ok(first)
    }) {
        Ok(snapshot) => Ok(snapshot),
        Err(Error::BucketNotFound) => Ok(BucketSnapshot::Missing),
        Err(err) => Err(err),
    }
}

fn validate_db(db: &BTree, model: &Model) {
    let actual_buckets: BTreeSet<String> = expect_db_ok(db.buckets(), "list buckets")
        .into_iter()
        .collect();
    let expected_buckets: BTreeSet<String> = model.buckets.keys().cloned().collect();
    assert_eq!(
        actual_buckets, expected_buckets,
        "concurrent target bucket set mismatch"
    );

    for bucket in &expected_buckets {
        let actual = expect_db_ok(
            read_bucket_snapshot(db, bucket),
            "read bucket snapshot for validation",
        );
        let expected = BucketSnapshot::from_model(model, bucket);
        // A bucket that was touched must carry its epoch marker; losing it
        // would otherwise be silently read back as epoch 0.
        if let BucketSnapshot::Present { epoch, .. } = expected {
            if epoch > 0 {
                let BucketSnapshot::Present {
                    epoch: actual_epoch,
                    ..
                } = &actual
                else {
                    panic!("touched bucket {bucket:?} must be present");
                };
                assert!(
                    *actual_epoch > 0,
                    "touched bucket {bucket:?} lost its epoch marker"
                );
            }
        }
        assert_eq!(
            actual, expected,
            "concurrent target bucket snapshot mismatch for bucket {bucket:?}"
        );
    }
}

fn expect_db_ok<T>(result: DbResult<T>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(err) => panic!("{context} returned unexpected error: {err:?}"),
    }
}

fn expect_open_ok<T>(result: OpenResult<T>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(err) => panic!("{context} returned unexpected open error: {err:?}"),
    }
}

fn expect_db_err<T>(result: DbResult<T>, expected: Error, context: &str) {
    match result {
        Ok(_) => panic!("{context} unexpectedly succeeded, expected {expected:?}"),
        Err(actual) => assert_eq!(actual, expected, "{context} returned wrong error"),
    }
}

fuzz_target!(|case: ConcurrentCase| {
    let mut harness = ConcurrentHarness::new();
    for op in case.ops {
        match op {
            ConcurrentOp::Single {
                bucket,
                steps,
                readers,
            } => harness.race_single(bucket, &steps, &readers),
            ConcurrentOp::Multi { steps, readers } => harness.race_multi(&steps, &readers),
            ConcurrentOp::Mixed { writers, readers } => harness.race_mixed(&writers, &readers),
            ConcurrentOp::Reopen => harness.reopen(),
            ConcurrentOp::Validate => harness.validate(),
        }
    }
    harness.validate();
});
