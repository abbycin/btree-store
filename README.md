# btree_store

[![CI](https://github.com/abbycin/btree-store/actions/workflows/ci.yml/badge.svg)](https://github.com/abbycin/btree-store/actions)
[![Crates.io](https://img.shields.io/crates/v/btree-store.svg)](https://crates.io/crates/btree-store)
[![License](https://img.shields.io/crates/l/btree-store.svg)](./LICENSE)

**btree_store** is a persistent, embedded key-value storage engine written in Rust. It implements a robust Copy-On-Write (COW) B-Tree architecture to ensure data integrity, crash safety, and efficient concurrent access.

## Features

*   **ACID Compliance:** Atomic commits using COW, Snapshot Isolation, and double-buffered meta pages.
*   **Closure-based Transactions:** Simplified `exec` (read-write) and `view` (read-only) APIs with automatic commit and rollback.
*   **Auto-Refresh:** Every transaction automatically starts from the freshest disk state. No manual snapshot management required.
*   **Conflict Detection:** Built-in "First-Committer-Wins" strategy for concurrent handles.
*   **Batch Operations:** `exec_multi` for atomic updates across multiple buckets with a single disk sync, significantly reducing I/O overhead.
*   **Crash Safety:** Double-write superblock updates and a `.pending` log recovery mechanism ensure zero-leak recovery even from torn writes.
*   **Logical Namespaces:** Direct support for multiple buckets within a single database file.
*   **Zero-Copy Access:** 8-byte aligned memory layouts allow direct pointer-to-reference conversion for maximum performance.
*   **Robust Data Integrity:** Strengthened physical invariant checks and CRC32C checksum validation for every node and metadata page.
*   **Shared Transaction State:** `clone()` creates a new handle that shares the same transaction context, optimized for multi-threaded components.
*   **Manual Compaction:** Best-effort tail compaction to reclaim space when requested.

> **Warning:** Multi-process concurrent access is NOT supported. Only one process should access the database file at a time.

## Architecture

*   **Store (`src/store.rs`):** Low-level page management, sharded LRU caching with mandatory invalidation on sync, and positional I/O.
*   **Node (`src/node.rs`):** 8-byte aligned memory management (`AlignedPage`), zero-copy serialization, and checksumming.
*   **Tree Logic (`src/lib.rs`):** Core B+ Tree algorithms and Transactional Snapshot Isolation logic.

## Usage

Add this to your `Cargo.toml`:

```bash
cargo add btree-store
```

### Basic Example

```rust
use btree_store::{BTree, Error};

fn main() -> Result<(), Error> {
    let db = BTree::open("data.db")?;

    // Read-Write Transaction
    db.exec("users", |txn| {
        txn.put(b"id:100", b"Alice")?;
        let val = txn.get(b"id:100")?;
        assert_eq!(val, b"Alice");
        Ok(())
    })?;

    // Read-Only View
    db.view("users", |txn| {
        let val = txn.get(b"id:100")?;
        println!("User: {:?}", String::from_utf8_lossy(&val));
        Ok(())
    })?;

    // Multi-Bucket Atomic Transaction
    db.exec_multi(|multi| {
        multi.execute("users", |txn| {
            txn.put(b"id:101", b"Bob")
        })?;
        multi.execute("stats", |txn| {
            txn.put(b"total_users", b"2")
        })?;
        Ok(())
    })?;

    Ok(())
}
```

## FFI (C)

Enable the `ffi` feature and build shared + static libraries:

Linux/macOS:
```bash
RUSTFLAGS="-C panic=abort" cargo build --features ffi --release
```

Windows (PowerShell):
```powershell
$env:RUSTFLAGS="-C panic=abort"; cargo build --features ffi --release
```

Build and run the minimal C example (`examples/ffi.c`):

Linux:
```bash
cc -I./include examples/ffi.c -L./target/release -lbtree_store -o ffi
LD_LIBRARY_PATH=./target/release ./ffi
```

macOS:
```bash
cc -I./include examples/ffi.c -L./target/release -lbtree_store -o ffi
DYLD_LIBRARY_PATH=./target/release ./ffi
```

Windows (MSVC):
```bat
cl /I include examples\ffi.c /Fe:ffi.exe /link /LIBPATH:target\release btree_store.dll.lib
copy target\release\btree_store.dll .
ffi.exe
```

Notes:
*   The C ABI is callback-based (`btree_exec`/`btree_view`).
*   Do not call `exec`/`view` inside callbacks.
*   `Txn`/`MultiTxn` handles are valid only during callbacks.
*   `longjmp` across FFI is unsafe.
*   Panics are not caught; use `panic=abort`.
*   `txn_get` returns a Rust-allocated buffer that must be freed with `btree_free`.
*   `btree_last_error` returns a pointer valid until the next FFI call on the same thread or `btree_last_error_clear`.
*   See `ffi.md` for full FFI usage documentation.

## Maintenance

You can trigger a best-effort tail compaction to reclaim space:

```rust
// compact using the default internal ratio
db.compact(0)?;

// compact targeting about 64 MB of tail space
db.compact(64 * 1024 * 1024)?;
```

## Performance Design

The engine is optimized for high-throughput scenarios:
*   **8-Byte Alignment:** Every page is allocated with 8-byte alignment, allowing direct casting of raw bytes to internal structures without memory copies.
*   **Snapshot Isolation (SI):** Readers and writers operate on stable snapshots without blocking each other (non-blocking reads).
*   **Automatic Page Reclamation:** Failed or conflicted transactions automatically trigger page reclamation to prevent database bloat.
*   **Transmute-based lifetime extension:** Iterators return direct references to internal buffers under a read lock, achieving near-zero allocation.

## Limits

*   **Max file size:** ~16 TB with 4 KB pages (32-bit page ids).

## Benchmarks

Environment:
*   **Date:** 2026-02-08
*   **OS:** openSUSE Tumbleweed (20260131), kernel 6.18.7-1-default
*   **CPU:** AMD Ryzen 7 4800H, 8C/16T
*   **Command:** `cargo bench`

Results (lower is better):
| Benchmark | Mean | 95% CI |
| --- | --- | --- |
| bucket_ops/create_drop_empty_bucket | 53.737 us | [53.484, 54.001] us |
| bucket_ops/drop_large_bucket_100k | 37.526 ms | [36.650, 38.906] ms |
| concurrent_get/4_threads_random_get | 438.833 ns | [428.752, 448.772] ns |
| delete/delete_insert_cycle_1k | 71.924 ms | [71.699, 72.222] ms |
| get/random_get_100k | 959.259 ns | [948.173, 971.986] ns |
| insert/insert_1k_tx | 67.990 ms | [67.707, 68.283] ms |

Interpretation:
*   **get**: ~0.96 us/op (random get on 100k keys).
*   **get (4 threads)**: ~0.44 us/op (per get, concurrent reads).
*   **put**: ~68 us/op (**single-op transactions**; `insert_1k_tx` runs 1000 separate `exec` calls).
*   **del**: ~72 us/op (**single-op transactions** after a prefill).
*   **bucket ops**: empty bucket create+drop ~54 us; drop 100k-key bucket ~37.5 ms.
*   These numbers are machine- and load-dependent; rerun on your hardware for comparable results.
*   No cross-DB comparison is provided here because different engines and configurations are not directly comparable.

## Testing

The project includes a comprehensive suite of integration tests:
*   `smo_stress_test`: Structural Modification Operations under heavy load.
*   `crash_safety_tests`: Verifies data integrity across simulated crashes.
*   `concurrency_tests`: Parallel readers and writers with auto-refresh validation.
*   `leak_safety_tests`: Ensures no pages are lost during failed operations.

Run tests with:
```bash
cargo test
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
