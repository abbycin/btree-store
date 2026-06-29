#![no_main]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
};

use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};
use btree_store::{BTree, Error, Result as DbResult};
use libfuzzer_sys::fuzz_target;
use tempfile::{Builder, TempDir};

mod common;

use common::{Bucket, MAX_KEY_LEN, Value, arbitrary_vec, bounded_nonempty_bytes};

const EPOCH_KEY: &[u8] = b"__epoch__";
const MAX_CONCURRENT_OPS: usize = 96;
const MAX_SINGLE_STEPS: usize = 8;
const MAX_MULTI_STEPS: usize = 8;
const MAX_RACE_READERS: usize = 4;

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
    Reopen,
    Validate,
}

impl<'a> Arbitrary<'a> for ConcurrentOp {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=3u8)? {
            0 => Ok(Self::Single {
                bucket: Bucket::arbitrary(u)?,
                steps: arbitrary_vec(u, MAX_SINGLE_STEPS)?,
                readers: arbitrary_vec(u, MAX_RACE_READERS)?,
            }),
            1 => Ok(Self::Multi {
                steps: arbitrary_vec(u, MAX_MULTI_STEPS)?,
                readers: arbitrary_vec(u, MAX_RACE_READERS)?,
            }),
            2 => Ok(Self::Reopen),
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

#[derive(Clone, Copy, Debug)]
enum ReaderMode {
    StaleClone,
    CloneAtRead,
    SamePathOpen,
}

impl<'a> Arbitrary<'a> for ReaderMode {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=2u8)? {
            0 => Ok(Self::StaleClone),
            1 => Ok(Self::CloneAtRead),
            _ => Ok(Self::SamePathOpen),
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
                        return Err(Error::NotFound);
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
                        .ok_or(Error::NotFound)?;
                    if bucket_model.entries.remove(&key.0).is_none() {
                        return Err(Error::NotFound);
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
        let db = expect_db_ok(BTree::open(&path), "open database");
        Self {
            _dir: dir,
            path,
            db: Some(db),
            model: Model::default(),
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
        self.db = Some(expect_db_ok(
            BTree::open(&self.path),
            "reopen concurrent database",
        ));
        self.validate();
    }

    fn race_single(&mut self, bucket: Bucket, steps: &[SingleStep], readers: &[ReaderPlan]) {
        let before = self.model.clone();
        let expected = before.apply_single(bucket.as_str(), steps);
        let stale = self.db().clone();
        let path = self.path.clone();
        let barrier = Arc::new(Barrier::new(readers.len() + 1));

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
                scope.spawn(move || {
                    barrier.wait();
                    let handle = match reader.mode {
                        ReaderMode::StaleClone => stale,
                        ReaderMode::CloneAtRead => base.clone(),
                        ReaderMode::SamePathOpen => {
                            expect_db_ok(BTree::open(&path), "same-path open during race")
                        }
                    };
                    let actual =
                        expect_db_ok(read_bucket_snapshot(&handle, reader.bucket.as_str()), "read raced bucket");
                    assert!(
                        actual == before_state || actual == after_state,
                        "reader observed unexpected bucket snapshot: actual={actual:?} before={before_state:?} after={after_state:?} mode={:?} bucket={:?}",
                        reader.mode,
                        reader.bucket
                    );
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
                    expect_db_err(actual, *expected_err, "single-bucket concurrent commit");
                }
            }
        });

        if let Ok(next) = expected {
            self.model = next;
        }
        self.validate();
    }

    fn race_multi(&mut self, steps: &[MultiStep], readers: &[ReaderPlan]) {
        let before = self.model.clone();
        let expected = before.apply_multi(steps);
        let stale = self.db().clone();
        let path = self.path.clone();
        let barrier = Arc::new(Barrier::new(readers.len() + 1));

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
                scope.spawn(move || {
                    barrier.wait();
                    let handle = match reader.mode {
                        ReaderMode::StaleClone => stale,
                        ReaderMode::CloneAtRead => base.clone(),
                        ReaderMode::SamePathOpen => {
                            expect_db_ok(BTree::open(&path), "same-path open during multi race")
                        }
                    };
                    let actual =
                        expect_db_ok(read_bucket_snapshot(&handle, reader.bucket.as_str()), "read raced multi bucket");
                    assert!(
                        actual == before_state || actual == after_state,
                        "reader observed unexpected multi-bucket snapshot: actual={actual:?} before={before_state:?} after={after_state:?} mode={:?} bucket={:?}",
                        reader.mode,
                        reader.bucket
                    );
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
                    expect_db_err(actual, *expected_err, "multi-bucket concurrent commit");
                }
            }
        });

        if let Ok(next) = expected {
            self.model = next;
        }
        self.validate();
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

fn read_bucket_snapshot(db: &BTree, bucket: &str) -> DbResult<BucketSnapshot> {
    match db.view(bucket, |txn| {
        let mut iter = txn.iter();
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        let mut epoch = None;
        let mut entries = BTreeMap::new();
        while iter.next_ref(&mut key_buf, &mut val_buf) {
            if key_buf == EPOCH_KEY {
                if val_buf.len() != std::mem::size_of::<u64>() {
                    return Err(Error::Corruption);
                }
                epoch = Some(u64::from_le_bytes(val_buf.as_slice().try_into().unwrap()));
            } else {
                entries.insert(key_buf.clone(), val_buf.clone());
            }
        }
        Ok(BucketSnapshot::Present {
            epoch: epoch.ok_or(Error::Corruption)?,
            entries,
        })
    }) {
        Ok(snapshot) => Ok(snapshot),
        Err(Error::NotFound) => Ok(BucketSnapshot::Missing),
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
            ConcurrentOp::Reopen => harness.reopen(),
            ConcurrentOp::Validate => harness.validate(),
        }
    }
    harness.validate();
});
