# btree-store fuzzing

This directory contains `cargo-fuzz` state-machine targets. The inputs are
structured operations, not arbitrary byte blobs.

The harness keeps a `BTreeMap` model beside a temporary `btree-store` database
and checks these invariants after successful operations:

- `buckets()` matches the modeled bucket set.
- Point reads match the model.
- Iteration returns exactly the modeled key/value pairs.
- Failed transactions and aborted `exec_multi` calls leave the database unchanged.
- Reopen, same-path open, clone reads, and compaction preserve the model.
- Concurrent reader fuzzing only asserts snapshot self-consistency for the
  observed epoch; it does not require readers to see the latest writer commit.

All generated database keys are bounded to `1..=32` bytes, matching
btree-store's current `MAX_KEY_LEN` limit and its non-empty key contract.
Bucket names are generated from a fixed non-empty set and are subject to the
same limit because bucket names are stored as catalog keys. Values may be empty
and are bounded to 64 KiB so the target still covers inline and overflow values
without making each fuzz iteration too expensive.

## Targets

- `kv_model`: single-bucket puts, deletes, reads, empty-bucket creation, aborts,
  and reopen checks.
- `multi_bucket_model`: atomic `exec_multi` batches across buckets, including
  repeated same-bucket steps, no-op bucket touches, missing-key aborts, and
  explicit aborts.
- `reopen_compact_model`: multi-bucket writes mixed with reopen, same-path open,
  clone read validation, and `compact(0)` / `compact(u64::MAX)`.
- `bucket_lifecycle`: bucket creation, deletion, recreation, missing-bucket
  checks, and reopen validation.
- `concurrent_snapshot_model`: one-writer/many-reader races where readers use a
  stale handle, `clone()`, or same-path `open()` and must observe either the
  full pre-commit snapshot or the full post-commit snapshot for a bucket.

## Commands

Run individual targets:

```bash
cargo +nightly fuzz run kv_model -- -max_total_time=600
cargo +nightly fuzz run multi_bucket_model -- -max_total_time=600
cargo +nightly fuzz run reopen_compact_model -- -max_total_time=600
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
