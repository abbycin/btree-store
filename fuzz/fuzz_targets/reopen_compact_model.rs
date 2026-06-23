#![no_main]

use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};
use libfuzzer_sys::fuzz_target;

mod common;

use common::{Bucket, Harness, Key, MAX_REOPEN_COMPACT_OPS, Value, arbitrary_vec};

#[derive(Clone, Debug)]
struct ReopenCompactCase {
    ops: Vec<ReopenCompactOp>,
}

impl<'a> Arbitrary<'a> for ReopenCompactCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        Ok(Self {
            ops: arbitrary_vec(u, MAX_REOPEN_COMPACT_OPS)?,
        })
    }
}

#[derive(Clone, Debug)]
enum ReopenCompactOp {
    Put(Bucket, Key, Value),
    Del(Bucket, Key),
    Touch(Bucket),
    Observe(Bucket, Key),
    Reopen,
    CompactDefault,
    CompactFull,
    CloneRead,
    SamePathOpenRead,
    Validate,
}

impl<'a> Arbitrary<'a> for ReopenCompactOp {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=9u8)? {
            0 => Ok(Self::Put(
                Bucket::arbitrary(u)?,
                Key::arbitrary(u)?,
                Value::arbitrary(u)?,
            )),
            1 => Ok(Self::Del(Bucket::arbitrary(u)?, Key::arbitrary(u)?)),
            2 => Ok(Self::Touch(Bucket::arbitrary(u)?)),
            3 => Ok(Self::Observe(Bucket::arbitrary(u)?, Key::arbitrary(u)?)),
            4 => Ok(Self::Reopen),
            5 => Ok(Self::CompactDefault),
            6 => Ok(Self::CompactFull),
            7 => Ok(Self::CloneRead),
            8 => Ok(Self::SamePathOpenRead),
            _ => Ok(Self::Validate),
        }
    }
}

fuzz_target!(|case: ReopenCompactCase| {
    let mut harness = Harness::new();
    for op in case.ops {
        match op {
            ReopenCompactOp::Put(bucket, key, value) => harness.exec_put(bucket, &key, &value),
            ReopenCompactOp::Del(bucket, key) => harness.exec_del(bucket, &key),
            ReopenCompactOp::Touch(bucket) => harness.exec_touch(bucket),
            ReopenCompactOp::Observe(bucket, key) => harness.exec_observe(bucket, &key),
            ReopenCompactOp::Reopen => harness.reopen(),
            ReopenCompactOp::CompactDefault => harness.compact(0),
            ReopenCompactOp::CompactFull => harness.compact(u64::MAX),
            ReopenCompactOp::CloneRead => harness.validate_clone(),
            ReopenCompactOp::SamePathOpenRead => harness.validate_same_path_open(),
            ReopenCompactOp::Validate => harness.validate(),
        }
    }
    harness.validate();
});
