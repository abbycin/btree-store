# btree-store

[![CI](https://github.com/abbycin/btree-store/actions/workflows/ci.yml/badge.svg)](https://github.com/abbycin/btree-store/actions)
[![Crates.io](https://img.shields.io/crates/v/btree-store.svg)](https://crates.io/crates/btree-store)
[![License](https://img.shields.io/crates/l/btree-store.svg)](./LICENSE)

**btree-store** is a persistent, embedded key-value storage engine written in Rust, built on a Copy-On-Write (COW) B+ Tree for data integrity, crash safety, and efficient concurrent access.

## Features

*   **Copy-on-Write B+ Tree:** Atomic commits without in-place updates.
*   **Snapshot Transactions:** Closure-based read/write transactions with automatic refresh, rollback, and snapshot-bound iteration.
*   **MVCC Read/Write Non-Blocking:** Views pin an epoch snapshot for their whole lifetime instead of taking the writer lock — a long view never blocks a commit, and a commit never blocks a view's traversal. Writers serialize on a shared mutex; readers traverse lock-free on a fixed snapshot.
*   **Multi-Bucket Atomicity:** Named buckets share one database file; `exec_multi` commits updates across buckets in one generation.
*   **Prefix Encoding:** Optional per-bucket key-prefix compression, persisted as part of the bucket layout policy.
*   **Crash Safety:** Double-buffered metadata publication and recovery from the newest complete generation.
*   **Durable, Reader-Gated Reclamation:** Reusable and quarantined pages are persisted and recovered with the database generation; retired pages are promoted to reusable only while no in-flight reader can still reference them. Long-lived views delay reclamation and grow the file, but writes are never blocked.

> **Warning:** Multi-process concurrent access is not supported. A competing process receives `OpenError::DatabaseBusy` if the exclusive file lock remains held after the bounded open wait.
>
> Within a single process, re-opening the same path returns the existing `BTree` instance as a clone. Use `BTree::clone()` to share handles across threads.

## Architecture

See [the design document](docs/design.md) for the complete architecture, transaction, persistence, recovery, and format-evolution model.


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
*   **Date:** 2026-08-08
*   **OS:** openSUSE Tumbleweed, kernel 7.1.3-1-default
*   **CPU:** AMD Ryzen 5 3600, 6C/12T
*   **Command:** `cargo bench --bench btree_bench -- --noplot`
*   **Method:** One Criterion run of the current version; values below are the center estimate from each benchmark

Results (lower is better):
| Benchmark | Estimate |
| --- | --- |
| bucket_ops/create_drop_empty_bucket | 8.7369 us |
| bucket_ops/drop_large_bucket_100k | 2.0382 ms |
| concurrent_get/4_threads_random_get | 366.84 ns |
| delete/delete_insert_cycle_1k | 9.2958 ms |
| exec_multi/mixed_1k_exec_multi_1k | 1.0670 s |
| get/random_get_100k | 425.37 ns |
| insert/insert_1k_tx | 8.8122 ms |

Plain vs. prefix-encoded buckets, using the same workload:
| Workload | Plain | Prefix |
| --- | --- | --- |
| insert | 8.0452 ms | 8.1435 ms |
| point_get | 8.4084 ms | 8.5309 ms |
| update | 19.071 ms | 15.484 ms |
| delete | 17.077 ms | 15.543 ms |
| iterate | 8.1412 ms | 8.2080 ms |
| mixed | 1.5102 ms | 1.5734 ms |

Interpretation:
*   **get**: ~0.43 us/op (random get on 100k keys).
*   **get (4 threads)**: ~0.37 us/op (per get, concurrent reads).
*   **put**: ~8.81 us/op (**single-op transactions**; `insert_1k_tx` measures 1000 separate `exec` calls).
*   **del**: ~9.30 us/op (**single-op transactions** after a prefill).
*   **exec_multi**: ~1067 us/exec_multi (`mixed_1k_exec_multi_1k` performs 1000 outer `exec_multi` calls, each with 1000 nested operations).
*   **bucket ops**: empty bucket create+drop ~8.74 us; drop 100k-key bucket ~2.04 ms.
*   **prefix encoding**: the second table compares independent plain and prefix measurements for the same workload. It uses 2000 keys and 64-byte values; prefix encoding is close to plain layout for insert, point get, and iteration, faster for update/delete, and slightly slower for mixed in this run. The 100k random-get and 4-thread random-get benchmarks are only in the standard table and were not run for both layouts.
*   These numbers are machine- and load-dependent; rerun on your hardware for comparable results.


## Limits

*   **Keys and bucket names:** 1..=128 bytes; empty keys and empty bucket names are rejected as invalid input.
*   **Max file size:** ~16 TB with 4 KB pages (32-bit page ids).
*   **On-disk format:** initial format version 1.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
