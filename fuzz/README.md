# btree-store fuzzing

This directory contains `cargo-fuzz` state-machine targets. The inputs are
structured operations, not arbitrary byte blobs.

The harness keeps a `BTreeMap` model beside a temporary `btree-store` database
and checks these invariants after successful operations:

- `buckets()` matches the modeled bucket set.
- Point reads match the model.
- `update()` returns `false` without mutating existing key/value state when the key is missing.
  A successful transaction may still materialize the bucket under normal `exec`/`exec_multi`
  semantics if the bucket was previously missing.
- Iteration returns exactly the modeled key/value pairs.
- Failed transactions and aborted `exec_multi` calls leave the database unchanged.
- Reopen, same-path open, and clone reads preserve the model.
- Concurrent reader fuzzing only asserts snapshot self-consistency for the
  observed epoch; it does not require readers to see the latest writer commit.
  The concurrent target also starts multiple writer threads together, so the
  reader checks run while independent `exec` calls contend on the writer lock.
  Some reader cases use the original `BTree` handle directly, exercising
  concurrent access to the handle's local snapshot state as well as cloned handles.

The model is intentionally limited to the public bucket/key/value contract. It
does not invent a logical page namespace or assert physical PID stability:
mapping/reverse trees and tail-window compaction are no longer part of the
engine. Physical PID validity, MetaNode layout, allocator ownership, and the
monotonic `next_page_id` rule are checked by the Rust storage/recovery tests.
The fuzz targets supplement, but do not replace, the deterministic high-pressure
and in-flight snapshot regressions in `tests/data_corruption.rs`,
`tests/mace_repro.rs`, and `tests/mace_snapshot_regression.rs`.
Every aborted single- or multi-transaction path in the shared harness, and
every concurrent race, is reopened before the next model step so the fuzz
oracle also checks the durable post-rollback state.

All generated database keys are bounded to the public `btree_store::MAX_KEY_LEN`
limit; in the current tree that means `1..=128` bytes, while still preserving
the non-empty key contract.
Bucket names are generated from a fixed non-empty set and are subject to the
same limit because bucket names are stored as catalog keys. Values may be empty
and are bounded to 64 KiB so the target still covers inline and overflow values
without making each fuzz iteration too expensive.

## Targets

- `kv_model`: single-bucket puts, updates, deletes, reads, empty-bucket
  creation, aborts, and reopen checks.
- `multi_bucket_model`: atomic `exec_multi` batches across buckets, including
  repeated same-bucket steps, `update(false)` key/value no-ops, no-op bucket touches,
  missing-key aborts, and explicit aborts.
- `bucket_lifecycle`: bucket creation, updates, deletion, recreation,
  missing-bucket checks, and reopen validation.
- `concurrent_snapshot_model`: one-writer/many-reader and multiple-writer/
  many-reader races where readers use a stale handle, `clone()`, or same-path
  `open()`, or the shared original handle, and must observe either the full pre-commit snapshot or the full
  post-commit snapshot for a bucket. The mixed phase gives each writer a
  distinct bucket, starts all writers and readers behind one barrier, and
  includes `update(false)` key/value no-ops, real writes, deletes, and rollback
  paths.

## Commands

Run individual targets:

```bash
cargo +nightly fuzz run kv_model -- -max_total_time=600
cargo +nightly fuzz run multi_bucket_model -- -max_total_time=600
cargo +nightly fuzz run bucket_lifecycle -- -max_total_time=600
cargo +nightly fuzz run concurrent_snapshot_model -- -max_total_time=600
```

Run the bounded regression set serially:

```bash
./scripts/fuzz_regression.sh
```

`scripts/fuzz_regression.sh` defaults `ASAN_OPTIONS` to `detect_leaks=0`
because LeakSanitizer can fail before running the target under ptrace-like
terminal/sandbox environments. To keep LeakSanitizer enabled, pass your own
`ASAN_OPTIONS` explicitly.

If a target finds a failure, minimize the artifact and convert the minimized
input into a deterministic regression test before claiming the bug is fixed.
