#![no_main]

use arbitrary::{Arbitrary, Result as ArbitraryResult, Unstructured};
use libfuzzer_sys::fuzz_target;

mod common;

use common::{Harness, MAX_MULTI_OPS, MAX_MULTI_STEPS, MultiStep, arbitrary_vec};

#[derive(Clone, Debug)]
struct MultiBucketCase {
    ops: Vec<MultiBucketOp>,
}

impl<'a> Arbitrary<'a> for MultiBucketCase {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        Ok(Self {
            ops: arbitrary_vec(u, MAX_MULTI_OPS)?,
        })
    }
}

#[derive(Clone, Debug)]
enum MultiBucketOp {
    Commit(Vec<MultiStep>),
    Abort(Vec<MultiStep>),
    Reopen,
    Validate,
}

impl<'a> Arbitrary<'a> for MultiBucketOp {
    fn arbitrary(u: &mut Unstructured<'a>) -> ArbitraryResult<Self> {
        match u.int_in_range(0..=3u8)? {
            0 => Ok(Self::Commit(arbitrary_vec(u, MAX_MULTI_STEPS)?)),
            1 => Ok(Self::Abort(arbitrary_vec(u, MAX_MULTI_STEPS)?)),
            2 => Ok(Self::Reopen),
            _ => Ok(Self::Validate),
        }
    }
}

fuzz_target!(|case: MultiBucketCase| {
    let mut harness = Harness::new();
    for op in case.ops {
        match op {
            MultiBucketOp::Commit(steps) => harness.exec_multi(&steps, false),
            MultiBucketOp::Abort(steps) => harness.exec_multi(&steps, true),
            MultiBucketOp::Reopen => harness.reopen(),
            MultiBucketOp::Validate => harness.validate(),
        }
    }
    harness.validate();
});
