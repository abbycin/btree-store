#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};
use btree_store::{BTree, Error, KeyError, MultiTxn, OpenResult, Result as DbResult};
use tempfile::{Builder, TempDir};

pub const MAX_KEY_LEN: usize = btree_store::MAX_KEY_LEN;
pub const MAX_VALUE_LEN: usize = 64 * 1024;
pub const MAX_KV_OPS: usize = 256;
pub const MAX_MULTI_OPS: usize = 128;
pub const MAX_MULTI_STEPS: usize = 16;
pub const MAX_LIFECYCLE_OPS: usize = 160;

pub const BUCKET_NAMES: [&str; 7] = ["a", "b", "c", "users", "stats", "empty", "zz"];
// The kv-model target uses a dedicated bucket name outside the arbitrary pool;
// every harness pre-creates the union so exec/multi never depends on implicit creation.
const PRE_CREATED_BUCKETS: [&str; 8] = ["a", "b", "c", "users", "stats", "empty", "zz", "kv"];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Bucket(&'static str);

impl Bucket {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub fn as_str(self) -> &'static str {
        self.0
    }
}

impl<'a> Arbitrary<'a> for Bucket {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        let idx = u.int_in_range(0..=BUCKET_NAMES.len() - 1)?;
        Ok(Self(BUCKET_NAMES[idx]))
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Key(pub Vec<u8>);

impl<'a> Arbitrary<'a> for Key {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        Ok(Self(bounded_nonempty_bytes(u, MAX_KEY_LEN)?))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Value(pub Vec<u8>);

impl<'a> Arbitrary<'a> for Value {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        Ok(Self(bounded_bytes(u, MAX_VALUE_LEN)?))
    }
}

#[derive(Clone, Debug)]
pub enum MultiStep {
    Put(Bucket, Key, Value),
    Update(Bucket, Key, Value),
    Del(Bucket, Key),
    Get(Bucket, Key),
    Observe(Bucket, Key),
    Touch(Bucket),
}

impl<'a> Arbitrary<'a> for MultiStep {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=5u8)? {
            0 => Ok(Self::Put(
                Bucket::arbitrary(u)?,
                Key::arbitrary(u)?,
                Value::arbitrary(u)?,
            )),
            1 => Ok(Self::Update(
                Bucket::arbitrary(u)?,
                Key::arbitrary(u)?,
                Value::arbitrary(u)?,
            )),
            2 => Ok(Self::Del(Bucket::arbitrary(u)?, Key::arbitrary(u)?)),
            3 => Ok(Self::Get(Bucket::arbitrary(u)?, Key::arbitrary(u)?)),
            4 => Ok(Self::Observe(Bucket::arbitrary(u)?, Key::arbitrary(u)?)),
            _ => Ok(Self::Touch(Bucket::arbitrary(u)?)),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Model {
    buckets: BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Model {
    fn touch_bucket(&mut self, bucket: &str) {
        self.buckets.entry(bucket.to_string()).or_default();
    }

    fn update_in_txn(&mut self, bucket: &str, key: &[u8], value: &[u8]) -> bool {
        self.touch_bucket(bucket);
        self.update(bucket, key, value)
    }

    fn put(&mut self, bucket: &str, key: Vec<u8>, value: Vec<u8>) {
        self.touch_bucket(bucket);
        self.buckets
            .get_mut(bucket)
            .expect("bucket must exist after touch")
            .insert(key, value);
    }

    fn get(&self, bucket: &str, key: &[u8]) -> DbResult<&Vec<u8>> {
        self.buckets
            .get(bucket)
            .and_then(|entries| entries.get(key))
            .ok_or(Error::KeyNotFound)
    }

    fn del(&mut self, bucket: &str, key: &[u8]) -> DbResult<()> {
        let entries = self.buckets.get_mut(bucket).ok_or(Error::KeyNotFound)?;
        if entries.remove(key).is_some() {
            Ok(())
        } else {
            Err(Error::KeyNotFound)
        }
    }

    fn update(&mut self, bucket: &str, key: &[u8], value: &[u8]) -> bool {
        let Some(entries) = self.buckets.get_mut(bucket) else {
            return false;
        };
        let Some(current) = entries.get_mut(key) else {
            return false;
        };
        *current = value.to_vec();
        true
    }

    fn del_bucket(&mut self, bucket: &str) -> DbResult<()> {
        self.buckets
            .remove(bucket)
            .map(|_| ())
            .ok_or(Error::BucketNotFound)
    }

    fn contains_bucket(&self, bucket: &str) -> bool {
        self.buckets.contains_key(bucket)
    }
}

pub struct Harness {
    _dir: TempDir,
    path: PathBuf,
    db: Option<BTree>,
    model: Model,
}

impl Harness {
    pub fn new() -> Self {
        let dir = Builder::new()
            .prefix("btree-store-fuzz-")
            .tempdir()
            .expect("create fuzz tempdir");
        let path = dir.path().join("db.btree");
        let db = expect_open_ok(BTree::open(&path), "open database");
        let mut model = Model::default();
        for bucket in PRE_CREATED_BUCKETS {
            expect_db_ok(db.new_bucket(bucket, false), "pre-create bucket");
            model.touch_bucket(bucket);
        }
        Self {
            _dir: dir,
            path,
            db: Some(db),
            model,
        }
    }

    /// Recreates a dropped bucket through the catalog API so the harness can
    /// keep operating on it without relying on implicit `exec` creation.
    fn ensure_bucket(&mut self, bucket: Bucket) {
        if !self.model.contains_bucket(bucket.as_str()) {
            expect_db_ok(
                self.db().new_bucket(bucket.as_str(), false),
                "recreate dropped bucket",
            );
            self.model.touch_bucket(bucket.as_str());
        }
    }

    pub fn validate(&self) {
        validate_db(self.db(), &self.model);
    }

    pub fn reopen(&mut self) {
        let old = self.db.take();
        drop(old);
        self.db = Some(expect_open_ok(BTree::open(&self.path), "reopen database"));
        self.validate();
    }

    pub fn validate_clone(&self) {
        let clone = self.db().clone();
        validate_db(&clone, &self.model);
    }

    pub fn validate_same_path_open(&self) {
        let other = expect_open_ok(BTree::open(&self.path), "open same path");
        validate_db(&other, &self.model);
    }

    pub fn exec_put(&mut self, bucket: Bucket, key: &Key, value: &Value) {
        self.ensure_bucket(bucket);
        expect_db_ok(
            self.db()
                .exec(bucket.as_str(), |txn| txn.put(&key.0, &value.0)),
            "exec put",
        );
        self.model
            .put(bucket.as_str(), key.0.clone(), value.0.clone());
        self.validate();
    }

    pub fn exec_del(&mut self, bucket: Bucket, key: &Key) {
        if !self.model.contains_bucket(bucket.as_str()) {
            expect_db_err(
                self.db().exec(bucket.as_str(), |txn| txn.del(&key.0)),
                Error::BucketNotFound,
                "exec del missing bucket",
            );
            self.validate();
            return;
        }
        let mut next_model = self.model.clone();
        let expected = next_model.del(bucket.as_str(), &key.0);
        let actual = self.db().exec(bucket.as_str(), |txn| txn.del(&key.0));
        match expected {
            Ok(()) => {
                expect_db_ok(actual, "exec del");
                self.model = next_model;
            }
            Err(expected_err) => {
                expect_db_err(actual, expected_err, "exec del missing key");
            }
        }
        self.validate();
    }

    pub fn exec_update(&mut self, bucket: Bucket, key: &Key, value: &Value) {
        self.ensure_bucket(bucket);
        let mut next_model = self.model.clone();
        let expected = next_model.update_in_txn(bucket.as_str(), &key.0, &value.0);
        let actual = self
            .db()
            .exec(bucket.as_str(), |txn| txn.update(&key.0, &value.0));
        let actual_updated = expect_db_ok(actual, "exec update");
        assert_eq!(actual_updated, expected, "exec update result mismatch");
        self.model = next_model;
        self.validate();
    }

    pub fn exec_get(&self, bucket: Bucket, key: &Key) {
        let expected = if self.model.contains_bucket(bucket.as_str()) {
            self.model.get(bucket.as_str(), &key.0).cloned()
        } else {
            Err(Error::BucketNotFound)
        };
        let actual = self.db().view(bucket.as_str(), |txn| txn.get(&key.0));
        match expected {
            Ok(expected_value) => {
                let actual_value = expect_db_ok(actual, "view get");
                assert_eq!(actual_value, expected_value, "get value mismatch");
            }
            Err(expected_err) => {
                expect_db_err(actual, expected_err, "view get missing key");
            }
        }
    }

    pub fn exec_touch(&mut self, bucket: Bucket) {
        if self.model.contains_bucket(bucket.as_str()) {
            expect_db_ok(self.db().exec(bucket.as_str(), |_| Ok(())), "exec touch");
        } else {
            expect_db_ok(
                self.db().new_bucket(bucket.as_str(), false),
                "exec touch creates dropped bucket",
            );
        }
        self.model.touch_bucket(bucket.as_str());
        self.validate();
    }

    pub fn exec_observe(&mut self, bucket: Bucket, key: &Key) {
        self.ensure_bucket(bucket);
        let expected = self.model.get(bucket.as_str(), &key.0).cloned();
        expect_db_ok(
            self.db().exec(bucket.as_str(), |txn| {
                match txn.get(&key.0) {
                    Ok(actual_value) => {
                        assert_eq!(
                            Ok(actual_value),
                            expected.clone(),
                            "observed value mismatch"
                        );
                    }
                    Err(Error::KeyNotFound) => {
                        assert_eq!(expected, Err(Error::KeyNotFound), "unexpected missing key");
                    }
                    Err(err) => return Err(err),
                }
                Ok(())
            }),
            "exec observe",
        );
        self.model.touch_bucket(bucket.as_str());
        self.validate();
    }

    pub fn exec_abort_put(&mut self, bucket: Bucket, key: &Key, value: &Value) {
        self.ensure_bucket(bucket);
        expect_db_err(
            self.db().exec(bucket.as_str(), |txn| {
                txn.put(&key.0, &value.0)?;
                Err::<(), Error>(abort_marker())
            }),
            abort_marker(),
            "abort put",
        );
        self.reopen();
    }

    pub fn exec_abort_del(&mut self, bucket: Bucket, key: &Key) {
        self.ensure_bucket(bucket);
        let expected_err = match self.model.get(bucket.as_str(), &key.0) {
            Ok(_) => abort_marker(),
            Err(err) => err,
        };
        expect_db_err(
            self.db().exec(bucket.as_str(), |txn| {
                txn.del(&key.0)?;
                Err::<(), Error>(abort_marker())
            }),
            expected_err,
            "abort del",
        );
        self.reopen();
    }

    pub fn del_bucket(&mut self, bucket: Bucket) {
        let mut next_model = self.model.clone();
        let expected = next_model.del_bucket(bucket.as_str());
        let actual = self.db().del_bucket(bucket.as_str());
        match expected {
            Ok(()) => {
                expect_db_ok(actual, "delete bucket");
                self.model = next_model;
            }
            Err(expected_err) => {
                expect_db_err(actual, expected_err, "delete missing bucket");
            }
        }
        self.validate();
    }

    pub fn expect_bucket_state(&self, bucket: Bucket) {
        if self.model.contains_bucket(bucket.as_str()) {
            self.validate();
            return;
        }

        expect_db_err(
            self.db().view(bucket.as_str(), |txn| {
                let mut iter = txn.iter();
                let mut key_buf = Vec::new();
                let mut val_buf = Vec::new();
                Ok(iter.next_ref(&mut key_buf, &mut val_buf))
            }),
            Error::BucketNotFound,
            "view missing bucket",
        );
    }

    pub fn recreate_bucket(&mut self, bucket: Bucket) {
        let mut next_model = self.model.clone();
        let expected = next_model.del_bucket(bucket.as_str());
        let actual = self.db().del_bucket(bucket.as_str());
        match expected {
            Ok(()) => expect_db_ok(actual, "recreate delete bucket"),
            Err(expected_err) => expect_db_err(actual, expected_err, "recreate missing bucket"),
        }
        self.model = next_model;
        self.exec_touch(bucket);
    }

    pub fn exec_multi(&mut self, steps: &[MultiStep], abort_after: bool) {
        // The multi simulation treats a bucket as existing after the first
        // touch, so every referenced bucket must exist before the multi runs.
        for step in steps {
            let bucket = match step {
                MultiStep::Put(b, _, _)
                | MultiStep::Update(b, _, _)
                | MultiStep::Del(b, _)
                | MultiStep::Get(b, _)
                | MultiStep::Observe(b, _)
                | MultiStep::Touch(b) => *b,
            };
            self.ensure_bucket(bucket);
        }

        let expected = simulate_multi(&self.model, steps, abort_after);
        let mut closure_model = self.model.clone();
        let actual = self.db().exec_multi(|multi| {
            for step in steps {
                apply_multi_engine_step(multi, &mut closure_model, step)?;
            }
            if abort_after {
                return Err(abort_marker());
            }
            Ok(())
        });

        let succeeded = expected.is_ok();
        match expected {
            Ok(next_model) => {
                expect_db_ok(actual, "exec_multi");
                self.model = next_model;
            }
            Err(expected_err) => {
                expect_db_err(actual, expected_err, "exec_multi abort");
            }
        }
        if succeeded {
            self.validate();
        } else {
            self.reopen();
        }
    }

    fn db(&self) -> &BTree {
        self.db.as_ref().expect("database handle must be present")
    }
}

pub fn arbitrary_vec<'a, T>(u: &mut Unstructured<'a>, max_len: usize) -> ArbitraryResult<Vec<T>>
where
    T: Arbitrary<'a>,
{
    let len = u.int_in_range(0..=max_len)?;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(T::arbitrary(u)?);
    }
    Ok(out)
}

pub fn bounded_bytes<'a>(u: &mut Unstructured<'a>, max_len: usize) -> ArbitraryResult<Vec<u8>> {
    let len = u.int_in_range(0..=max_len)?;
    Ok(u.bytes(len)?.to_vec())
}

pub fn bounded_nonempty_bytes<'a>(
    u: &mut Unstructured<'a>,
    max_len: usize,
) -> ArbitraryResult<Vec<u8>> {
    let len = u.int_in_range(1..=max_len)?;
    Ok(u.bytes(len)?.to_vec())
}

fn simulate_multi(model: &Model, steps: &[MultiStep], abort_after: bool) -> DbResult<Model> {
    let mut next_model = model.clone();
    for step in steps {
        apply_multi_model_step(&mut next_model, step)?;
    }
    if abort_after {
        return Err(abort_marker());
    }
    Ok(next_model)
}

fn apply_multi_model_step(model: &mut Model, step: &MultiStep) -> DbResult<()> {
    match step {
        MultiStep::Put(bucket, key, value) => {
            model.put(bucket.as_str(), key.0.clone(), value.0.clone());
            Ok(())
        }
        MultiStep::Update(bucket, key, value) => {
            let _ = model.update_in_txn(bucket.as_str(), &key.0, &value.0);
            Ok(())
        }
        MultiStep::Del(bucket, key) => model.del(bucket.as_str(), &key.0),
        MultiStep::Get(bucket, key) => model.get(bucket.as_str(), &key.0).map(|_| ()),
        MultiStep::Observe(bucket, _) | MultiStep::Touch(bucket) => {
            model.touch_bucket(bucket.as_str());
            Ok(())
        }
    }
}

fn apply_multi_engine_step(
    multi: &mut MultiTxn<'_>,
    model: &mut Model,
    step: &MultiStep,
) -> DbResult<()> {
    match step {
        MultiStep::Put(bucket, key, value) => {
            expect_db_ok(
                multi.exec(bucket.as_str(), |txn| txn.put(&key.0, &value.0)),
                "multi put",
            );
            model.put(bucket.as_str(), key.0.clone(), value.0.clone());
            Ok(())
        }
        MultiStep::Update(bucket, key, value) => {
            let expected = model.update_in_txn(bucket.as_str(), &key.0, &value.0);
            let actual = expect_db_ok(
                multi.exec(bucket.as_str(), |txn| txn.update(&key.0, &value.0)),
                "multi update",
            );
            assert_eq!(actual, expected, "multi update result mismatch");
            Ok(())
        }
        MultiStep::Del(bucket, key) => {
            let mut next_model = model.clone();
            let expected = next_model.del(bucket.as_str(), &key.0);
            let actual = multi.exec(bucket.as_str(), |txn| txn.del(&key.0));
            match expected {
                Ok(()) => {
                    expect_db_ok(actual, "multi del");
                    *model = next_model;
                    Ok(())
                }
                Err(expected_err) => {
                    expect_db_err(actual, expected_err.clone(), "multi del missing key");
                    Err(expected_err)
                }
            }
        }
        MultiStep::Get(bucket, key) => {
            let expected = model.get(bucket.as_str(), &key.0).cloned();
            let actual = multi.exec(bucket.as_str(), |txn| txn.get(&key.0));
            match expected {
                Ok(expected_value) => {
                    let actual_value = expect_db_ok(actual, "multi get");
                    assert_eq!(actual_value, expected_value, "multi get value mismatch");
                    Ok(())
                }
                Err(expected_err) => {
                    expect_db_err(actual, expected_err.clone(), "multi get missing key");
                    Err(expected_err)
                }
            }
        }
        MultiStep::Observe(bucket, key) => {
            let expected = model.get(bucket.as_str(), &key.0).cloned();
            expect_db_ok(
                multi.exec(bucket.as_str(), |txn| {
                    match txn.get(&key.0) {
                        Ok(actual_value) => {
                            assert_eq!(
                                Ok(actual_value),
                                expected.clone(),
                                "multi observed value mismatch"
                            );
                        }
                        Err(Error::KeyNotFound) => {
                            assert_eq!(expected, Err(Error::KeyNotFound), "unexpected missing key");
                        }
                        Err(err) => return Err(err),
                    }
                    Ok(())
                }),
                "multi observe",
            );
            model.touch_bucket(bucket.as_str());
            Ok(())
        }
        MultiStep::Touch(bucket) => {
            expect_db_ok(multi.exec(bucket.as_str(), |_| Ok(())), "multi touch");
            model.touch_bucket(bucket.as_str());
            Ok(())
        }
    }
}

fn validate_db(db: &BTree, model: &Model) {
    let actual_buckets: BTreeSet<String> = expect_db_ok(db.buckets(), "list buckets")
        .into_iter()
        .collect();
    let expected_buckets: BTreeSet<String> = model.buckets.keys().cloned().collect();
    assert_eq!(actual_buckets, expected_buckets, "bucket set mismatch");

    for (bucket, expected_entries) in &model.buckets {
        let actual_entries = expect_db_ok(
            db.view(bucket, |txn| {
                for (key, expected_value) in expected_entries {
                    let actual_value = txn.get(key.as_slice())?;
                    assert_eq!(
                        &actual_value, expected_value,
                        "point get mismatch in bucket {bucket:?}"
                    );
                }

                let mut actual = BTreeMap::new();
                let mut iter = txn.iter();
                let mut key_buf = Vec::new();
                let mut val_buf = Vec::new();
                while iter.next_ref(&mut key_buf, &mut val_buf) {
                    actual.insert(key_buf.clone(), val_buf.clone());
                }
                Ok(actual)
            }),
            "validate bucket view",
        );
        assert_eq!(
            &actual_entries, expected_entries,
            "iterator mismatch in bucket {bucket:?}"
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

fn abort_marker() -> Error {
    Error::InvalidKey(KeyError::Empty)
}

fn expect_db_err<T>(result: DbResult<T>, expected: Error, context: &str) {
    match result {
        Ok(_) => panic!("{context} unexpectedly succeeded, expected {expected:?}"),
        Err(actual) => assert_eq!(actual, expected, "{context} returned wrong error"),
    }
}
