# btree_store

**btree_store** is a high-performance, persistent, embedded key-value storage engine written in Rust. It implements a robust Copy-On-Write (COW) B-Tree architecture to ensure data integrity, crash safety, and efficient concurrent access.

## Features

*   **ACID Compliance:** Atomic commits using COW and double-buffered meta pages.
*   **Crash Safety:** A dedicated `.pending` log recovery mechanism reclaims leaked pages from interrupted transactions.
*   **Buckets:** Logical namespaces for data separation within a single database file.
*   **Zero-Allocation Iterators:** High-performance iterators returning `(&[u8], &[u8])` with internal buffer reuse to minimize GC pressure and memory overhead.
*   **Efficient Concurrency:** Granular locking using `parking_lot` and sharded node caching.
*   **Data Integrity:** Full CRC32C checksum validation for every page (nodes, meta, and free lists).
*   **Zero External Dependencies:** Core engine built using native Rust standard library for a lightweight footprint.

## Architecture

*   **Store (`src/store.rs`):** Low-level page management, sharded LRU caching, and positional I/O.
*   **Node (`src/node.rs`):** B-Tree node serialization, splitting, compacting, and checksumming.
*   **Tree (`src/lib.rs`):** Core B+ Tree algorithms including COW insertion, deletion with root collapse optimization, and search.
*   **Bucket (`src/lib.rs`):** High-level API for user-facing key-value operations.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
btree-store = "0.1.0"
```

### Basic Example

```rust
use btree_store::BTree;

fn main() -> Result<(), btree_store::Error> {
    // Open or create the database
    let db = BTree::open("data.db")?;

    // Create or retrieve a bucket
    let bucket = db.new_bucket("users")?;

    // Insert data
    bucket.put(b"id:100", b"Alice")?;
    
    // Retrieve data
    if let Some(val) = bucket.get(b"id:100").ok() {
        println!("Found user: {:?}", String::from_utf8_lossy(&val));
    }

    // Commit changes to disk
    db.commit()?;

    // Iterate efficiently
    let mut iter = bucket.iter()?;
    while let Some((k, v)) = iter.next() {
        println!("Key: {:?}, Value: {:?}", k, v);
    }

    Ok(())
}
```

## Performance Design

The engine is optimized for high-throughput scenarios:
*   **Cursor-based path traversal** avoids redundant tree searches.
*   **RAII CommitContext** ensures zero-leak automatic rollbacks on failure.
*   **Transmute-based lifetime extension** in iterators allows returning direct references to internal buffers under a read lock, achieving C-like performance in Rust.

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
