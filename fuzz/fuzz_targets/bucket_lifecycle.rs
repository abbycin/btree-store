#![no_main]

use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};
use libfuzzer_sys::fuzz_target;

mod common;

use common::{Bucket, Harness, Key, MAX_LIFECYCLE_OPS, Value, arbitrary_vec};

#[derive(Clone, Debug)]
struct BucketLifecycleCase {
    ops: Vec<BucketLifecycleOp>,
}

impl<'a> Arbitrary<'a> for BucketLifecycleCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        Ok(Self {
            ops: arbitrary_vec(u, MAX_LIFECYCLE_OPS)?,
        })
    }
}

#[derive(Clone, Debug)]
enum BucketLifecycleOp {
    Touch(Bucket),
    Put(Bucket, Key, Value),
    Del(Bucket, Key),
    Drop(Bucket),
    Recreate(Bucket),
    Observe(Bucket, Key),
    ExpectBucketState(Bucket),
    Reopen,
    Validate,
}

impl<'a> Arbitrary<'a> for BucketLifecycleOp {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=8u8)? {
            0 => Ok(Self::Touch(Bucket::arbitrary(u)?)),
            1 => Ok(Self::Put(
                Bucket::arbitrary(u)?,
                Key::arbitrary(u)?,
                Value::arbitrary(u)?,
            )),
            2 => Ok(Self::Del(Bucket::arbitrary(u)?, Key::arbitrary(u)?)),
            3 => Ok(Self::Drop(Bucket::arbitrary(u)?)),
            4 => Ok(Self::Recreate(Bucket::arbitrary(u)?)),
            5 => Ok(Self::Observe(Bucket::arbitrary(u)?, Key::arbitrary(u)?)),
            6 => Ok(Self::ExpectBucketState(Bucket::arbitrary(u)?)),
            7 => Ok(Self::Reopen),
            _ => Ok(Self::Validate),
        }
    }
}

fuzz_target!(|case: BucketLifecycleCase| {
    let mut harness = Harness::new();
    for op in case.ops {
        match op {
            BucketLifecycleOp::Touch(bucket) => harness.exec_touch(bucket),
            BucketLifecycleOp::Put(bucket, key, value) => harness.exec_put(bucket, &key, &value),
            BucketLifecycleOp::Del(bucket, key) => harness.exec_del(bucket, &key),
            BucketLifecycleOp::Drop(bucket) => harness.del_bucket(bucket),
            BucketLifecycleOp::Recreate(bucket) => harness.recreate_bucket(bucket),
            BucketLifecycleOp::Observe(bucket, key) => harness.exec_observe(bucket, &key),
            BucketLifecycleOp::ExpectBucketState(bucket) => harness.expect_bucket_state(bucket),
            BucketLifecycleOp::Reopen => harness.reopen(),
            BucketLifecycleOp::Validate => harness.validate(),
        }
    }
    harness.validate();
});
