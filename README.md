# btree_store

**btree_store** is a high-performance, persistent, embedded key-value storage engine written in Rust. It implements a robust Copy-On-Write (COW) B-Tree architecture to ensure data integrity, crash safety, and efficient concurrent access.

## Features

*   **ACID Compliance:** Atomic commits using COW, Snapshot Isolation, and double-buffered meta pages.
*   **Conflict Detection:** Built-in "First-Committer-Wins" strategy ensures data integrity in multi-process environments.
*   **Crash Safety:** A dedicated `.pending` log recovery mechanism reclaims leaked pages from interrupted transactions.
*   **Buckets:** Logical namespaces for data separation within a single database file.
*   **Zero-Allocation Iterators:** High-performance iterators returning `(&[u8], &[u8])` with internal buffer reuse.
*   **Efficient Concurrency:** Shared bucket handles and Snapshot Isolation (SI) using `parking_lot` for scalable access.
*   **Data Integrity:** Full CRC32C checksum validation for every page (nodes, meta, and free lists).
*   **Zero External Dependencies:** Core engine built using native Rust standard library.

## Architecture

*   **Store (`src/store.rs`):** Low-level page management, sharded LRU caching, and positional I/O.
*   **Node (`src/node.rs`):** B-Tree node serialization, splitting, compacting, and checksumming.
*   **Tree (`src/lib.rs`):** Core B+ Tree algorithms and Snapshot Isolation logic.
*   **Bucket (`src/lib.rs`):** High-level API with shared handle caching.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
btree-store = "0.1.0"
```

### Basic Example (with Conflict Handling)

```rust
use btree_store::{BTree, Error};

fn main() -> Result<(), Error> {
    let db = BTree::open("data.db")?;
    let bucket = db.get_bucket("users").or_else(|_| db.new_bucket("users"))?;

    loop {
        // Perform operations
        bucket.put(b"id:100", b"Alice")?;

        // Commit changes
        match db.commit() {
            Ok(_) => break,
            Err(Error::Conflict) => {
                // Another instance committed first, refresh and retry
                db.refresh()?;
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}
```

## Performance Design

The engine is optimized for high-throughput scenarios:
*   **Snapshot Isolation (SI)** allows concurrent readers and writers to operate without blocking each other.
*   **Bucket Handle Caching** ensures all threads share the same metadata locks, preventing structural corruption.
*   **Cursor-based path traversal** avoids redundant tree searches.
*   **RAII CommitContext** ensures zero-leak automatic rollbacks on failure.
*   **Transmute-based lifetime extension** in iterators allows returning direct references to internal buffers under a read lock.

## Testing

The project includes a comprehensive suite of integration tests:
*   `smo_stress_test`: Structural Modification Operations under heavy load.
*   `crash_safety_tests`: Verifies data integrity across simulated crashes.
*   `concurrency_tests`: Parallel readers and writers.
*   `leak_safety_tests`: Ensures no pages are lost during failed operations.

Run tests with:
```bash
cargo test
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
