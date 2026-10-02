# btree-store

[![CI](https://github.com/abbycin/btree-store/actions/workflows/ci.yml/badge.svg)](https://github.com/abbycin/btree-store/actions)
[![Crates.io](https://img.shields.io/crates/v/btree-store.svg)](https://crates.io/crates/btree-store)
[![License](https://img.shields.io/crates/l/btree-store.svg)](./LICENSE)

**btree-store** is a persistent, embedded key-value database written in Rust, built on a Copy-On-Write (COW) B+ Tree for data integrity, crash safety, and efficient concurrent access.

## ⚠️ Upgrading from 1.x to 2.0 — read this before opening a 1.x database

**2.0 is a breaking release. A database file written by any 1.x release will not open, and three Rust
API changes need a code edit.**

1.x wrote on-disk **format version 1**; 2.0 reads and writes **format version 2**. The runtime
refuses a version-1 file by name rather than reading it, so rebuilding the file is mandatory:

```bash
btree-store migrate old.db --output new.db --to 2
```

The source is never modified. The rebuilt file is verified against it bucket by bucket and
published by rename only after it passes. Bump the dependency to `btree-store = "2.0"` at the same
time. Stop your application first — it holds an exclusive lock on the file while it runs.

**[→ Full migration guide](docs/migration.md)** — the complete procedure, how to verify and roll
back, and the three API changes with their replacements.

## Features

*   **Copy-on-Write B+ Tree:** Atomic commits without in-place updates.
*   **Snapshot Transactions:** Closure-based read/write transactions with automatic refresh, rollback, and snapshot-bound iteration.
*   **MVCC Read/Write Non-Blocking:** Views pin an epoch snapshot for their whole lifetime instead of taking the writer lock — a long view never blocks a commit, and a commit never blocks a view's traversal. Writers serialize on a shared mutex; readers traverse lock-free on a fixed snapshot.
*   **Multi-Bucket Atomicity:** Named buckets share one database file; `exec_multi` commits updates across buckets in one generation.
*   **Prefix Encoding:** Optional per-bucket key-prefix compression, persisted as part of the bucket layout policy.
*   **Crash Safety:** Double-buffered metadata publication and recovery from the newest complete generation.
*   **Durable, Reader-Gated Reclamation:** Reusable and quarantined pages are persisted and recovered with the database generation; retired pages are promoted to reusable only while no in-flight reader can still reference them. Long-lived views delay reclamation and grow the file, but writes are never blocked.
*   **Read-Only Opens:** `BTree::open_read_only` opens an existing database without write access. The path is never created, an empty file is not initialised, and the handle writes nothing — not even during open. Mutating calls return `Error::ReadOnly` instead of touching the file.

> **Warning:** Concurrent *write* access from more than one process is not supported. A competing process receives `OpenError::DatabaseBusy` if the exclusive file lock remains held after the bounded open wait. Read-only opens take a shared lock instead, so several processes may read one file at once.
>
> Within a single process, re-opening the same path returns the existing `BTree` instance as a clone. Use `BTree::clone()` to share handles across threads.

## Architecture

See [the design document](docs/design.md) for the complete architecture, transaction, persistence, recovery, and format-evolution model.
See the [migration guide](docs/migration.md) when upgrading across a version bump that changes the
file format or the Rust API.
See [the CLI reference](docs/cli.md) for the `btree-store` commands.


## Basic Example

```rust
use btree_store::BTree;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = BTree::open("data.db")?;

    // Buckets are created explicitly with an optional prefix-encoding flag.
    db.new_bucket("users", false)?;
    db.new_bucket("quote", false)?;

    // Read-write transaction.
    db.exec("users", |txn| {
        txn.put("mo", "ha")?;
        let val = txn.get("mo")?;
        assert_eq!(val, b"ha".to_vec());
        let updated = txn.update("elder", "+1s")?;
        assert!(
            !updated,
            "update only changes an existing key and does not insert a missing key"
        );
        Ok(())
    })?;

    // Read-only view.
    db.view("users", |txn| {
        let val = txn.get("mo")?;
        println!("mo: {:?}", String::from_utf8_lossy(&val));
        Ok(())
    })?;

    // Multi-bucket atomic transaction.
    db.exec_multi(|multi| {
        multi.exec("users", |txn| {
            // Overwrite the existing value.
            txn.put("mo", "+1s")
        })?;
        multi.exec("quote", |txn| txn.put("moha", "naive!"))?;
        Ok(())
    })?;

    Ok(())
}
```

## Benchmarks

Environment:
*   **Date:** 2026-09-24
*   **OS:** openSUSE Tumbleweed, kernel 7.2.4-1-default
*   **CPU:** AMD Ryzen 5 3600, 6C/12T
*   **Command:** `cargo bench --bench btree_bench -- --noplot`

Results (lower is better):
| Benchmark | Estimate |
| --- | --- |
| bucket_ops/create_drop_empty_bucket | 14.960 us |
| bucket_ops/drop_large_bucket_100k | 3.8496 ms |
| concurrent_get/4_threads_random_get | 331.08 ns |
| delete/delete_insert_cycle_1k | 16.121 ms |
| exec_multi/mixed_1k_exec_multi_1k | 315.87 ms |
| get/random_get_100k | 426.26 ns |
| insert/insert_1k_tx | 15.518 ms |

Plain vs. prefix-encoded buckets, using the same workload:
| Workload | Plain | Prefix |
| --- | --- | --- |
| insert | 2.2791 ms | 2.5898 ms |
| point_get | 2.8024 ms | 3.1190 ms |
| update | 5.3343 ms | 4.9033 ms |
| delete | 4.7901 ms | 4.7878 ms |
| iterate | 2.5537 ms | 2.8267 ms |
| mixed | 891.77 us | 938.98 us |

Interpretation:
*   **get**: ~0.43 us/op (random get on 100k keys).
*   **get (4 threads)**: ~0.33 us/op (per get, concurrent reads).
*   **put**: ~15.52 us/op (**single-op transactions**; `insert_1k_tx` measures 1000 separate `exec` calls).
*   **del**: ~16.12 us/op (**single-op transactions** after a prefill).
*   **exec_multi**: ~316 us/exec_multi (`mixed_1k_exec_multi_1k` performs 1000 outer `exec_multi` calls, each with 1000 nested operations).
*   **bucket ops**: empty bucket create+drop ~14.96 us; drop 100k-key bucket ~3.85 ms.
*   **prefix encoding**: the second table compares independent plain and prefix measurements for the same workload. It uses 2000 keys and 64-byte values. Prefix encoding is clearly faster for update (~8%), a tie for delete (0.05%, inside run-to-run noise), and slower for the rest in these measurements: insert ~14%, point get ~11%, iteration ~11%, and mixed ~5%. The 100k random-get and 4-thread random-get benchmarks are only in the standard table and were not run for both layouts.
*   These numbers are machine- and load-dependent; rerun on your hardware for comparable results.


## Limits

*   **Keys and bucket names:** 1..=128 bytes; empty keys and empty bucket names are rejected as invalid input.
*   **Max file size:** ~16 TB with 4 KB pages (32-bit page ids).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
