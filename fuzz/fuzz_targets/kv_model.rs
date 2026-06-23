#![no_main]

use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};
use libfuzzer_sys::fuzz_target;

mod common;

use common::{Bucket, Harness, Key, MAX_KV_OPS, Value, arbitrary_vec};

const KV_BUCKET: Bucket = Bucket::new("kv");

#[derive(Clone, Debug)]
struct KvCase {
    ops: Vec<KvOp>,
}

impl<'a> Arbitrary<'a> for KvCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        Ok(Self {
            ops: arbitrary_vec(u, MAX_KV_OPS)?,
        })
    }
}

#[derive(Clone, Debug)]
enum KvOp {
    Put(Key, Value),
    Del(Key),
    Get(Key),
    Touch,
    Observe(Key),
    AbortPut(Key, Value),
    AbortDel(Key),
    Reopen,
    Validate,
}

impl<'a> Arbitrary<'a> for KvOp {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=8u8)? {
            0 => Ok(Self::Put(Key::arbitrary(u)?, Value::arbitrary(u)?)),
            1 => Ok(Self::Del(Key::arbitrary(u)?)),
            2 => Ok(Self::Get(Key::arbitrary(u)?)),
            3 => Ok(Self::Touch),
            4 => Ok(Self::Observe(Key::arbitrary(u)?)),
            5 => Ok(Self::AbortPut(Key::arbitrary(u)?, Value::arbitrary(u)?)),
            6 => Ok(Self::AbortDel(Key::arbitrary(u)?)),
            7 => Ok(Self::Reopen),
            _ => Ok(Self::Validate),
        }
    }
}

fuzz_target!(|case: KvCase| {
    let mut harness = Harness::new();
    for op in case.ops {
        match op {
            KvOp::Put(key, value) => harness.exec_put(KV_BUCKET, &key, &value),
            KvOp::Del(key) => harness.exec_del(KV_BUCKET, &key),
            KvOp::Get(key) => harness.exec_get(KV_BUCKET, &key),
            KvOp::Touch => harness.exec_touch(KV_BUCKET),
            KvOp::Observe(key) => harness.exec_observe(KV_BUCKET, &key),
            KvOp::AbortPut(key, value) => harness.exec_abort_put(KV_BUCKET, &key, &value),
            KvOp::AbortDel(key) => harness.exec_abort_del(KV_BUCKET, &key),
            KvOp::Reopen => harness.reopen(),
            KvOp::Validate => harness.validate(),
        }
    }
    harness.validate();
});
