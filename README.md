# btree_store

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

> **Warning:** Multi-process concurrent access is NOT supported. Only one process should access the database file at a time.

## Architecture

*   **Store (`src/store.rs`):** Low-level page management, sharded LRU caching with mandatory invalidation on sync, and positional I/O.
*   **Node (`src/node.rs`):** 8-byte aligned memory management (`AlignedPage`), zero-copy serialization, and checksumming.
*   **Tree Logic (`src/lib.rs`):** Core B+ Tree algorithms and Transactional Snapshot Isolation logic.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
btree-store = "0.1.1"
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

## Performance Design

The engine is optimized for high-throughput scenarios:
*   **8-Byte Alignment:** Every page is allocated with 8-byte alignment, allowing direct casting of raw bytes to internal structures without memory copies.
*   **Snapshot Isolation (SI):** Readers and writers operate on stable snapshots without blocking each other (non-blocking reads).
*   **Automatic Page Reclamation:** Failed or conflicted transactions automatically trigger page reclamation to prevent database bloat.
*   **Transmute-based lifetime extension:** Iterators return direct references to internal buffers under a read lock, achieving near-zero allocation.

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