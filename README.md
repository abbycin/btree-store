# btree-store

[![CI](https://github.com/abbycin/btree-store/actions/workflows/ci.yml/badge.svg)](https://github.com/abbycin/btree-store/actions)
[![Crates.io](https://img.shields.io/crates/v/btree-store.svg)](https://crates.io/crates/btree-store)
[![License](https://img.shields.io/crates/l/btree-store.svg)](./LICENSE)

**btree_store** is a persistent, embedded key-value storage engine written in Rust. It implements a robust Copy-On-Write (COW) B+ Tree architecture to ensure data integrity, crash safety, and efficient concurrent access.

## Features

*   **ACID Compliance:** Atomic commits using COW, Snapshot Isolation, and double-buffered meta pages.
*   **Closure-based Transactions:** Simplified `exec` (read-write) and `view` (read-only) APIs with automatic commit and rollback.
*   **Auto-Refresh:** Every transaction automatically starts from the freshest disk state. No manual snapshot management required.
*   **Conflict Detection:** Built-in "First-Committer-Wins" strategy for concurrent handles.
*   **Batch Operations:** `exec_multi` for atomic updates across multiple buckets with a single disk sync, significantly reducing I/O overhead.
*   **Crash Safety:** Double-buffered superblock with CRC32C checksums and ordered metadata writes; recovery selects the newest valid meta page.
*   **Logical Namespaces:** Direct support for multiple buckets within a single database file.
*   **Zero-Copy Access:** 8-byte aligned memory layouts allow direct pointer-to-reference conversion for maximum performance.
*   **Robust Data Integrity:** Strengthened physical invariant checks and CRC32C checksum validation for every node and metadata page.
*   **Shared Transaction State:** `clone()` creates a new handle that shares the same transaction context, optimized for multi-threaded components.
*   **Manual Compaction:** Best-effort tail compaction to reclaim space when requested.

> **Warning:** Multi-process concurrent access is NOT supported. Only one process should access the database file at a time.
>
> Within a single process, opening the same database path multiple times will return a clone of the already-opened `BTree` instance. Use `BTree::clone()` to share handles explicitly across threads/components.

## Architecture

*   **Store (`src/store.rs`):** Low-level page management, sharded clock cache with explicit invalidation, and positional I/O.
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
*   **Snapshot Isolation (SI):** Readers use a stable root per transaction; each handle enforces single-writer/multi-reader via an RwLock.
*   **Automatic Page Reclamation:** Failed or conflicted transactions automatically trigger page reclamation to prevent database bloat.
*   **Lock-Free File I/O:** Positional reads (`pread`/`seek_read`) avoid a global file mutex for concurrent access.

## Limits

*   **Keys and bucket names:** 1..=128 bytes; empty keys and empty bucket names are rejected as invalid input.
*   **Max file size:** ~16 TB with 4 KB pages (32-bit page ids).

## Benchmarks

Environment:
*   **Date:** 2026-05-08
*   **OS:** openSUSE Tumbleweed, kernel 6.19.12-1-default
*   **CPU:** AMD Ryzen 5 3600, 6C/12T
*   **Command:** `cargo bench`

Results (lower is better):
| Benchmark | Mean | 95% CI |
| --- | --- | --- |
| bucket_ops/create_drop_empty_bucket | 34.963 us | [34.908, 35.015] us |
| bucket_ops/drop_large_bucket_100k | 35.515 ms | [35.336, 35.685] ms |
| concurrent_get/4_threads_random_get | 325.270 ns | [319.530, 329.860] ns |
| delete/delete_insert_cycle_1k | 54.982 ms | [54.885, 55.088] ms |
| get/random_get_100k | 448.090 ns | [443.860, 452.620] ns |
| insert/insert_1k_tx | 52.708 ms | [52.623, 52.801] ms |

Interpretation:
*   **get**: ~0.45 us/op (random get on 100k keys).
*   **get (4 threads)**: ~0.33 us/op (per get, concurrent reads).
*   **put**: ~53 us/op (**single-op transactions**; `insert_1k_tx` runs 1000 separate `exec` calls).
*   **del**: ~55 us/op (**single-op transactions** after a prefill).
*   **bucket ops**: empty bucket create+drop ~35 us; drop 100k-key bucket ~35.5 ms.
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
