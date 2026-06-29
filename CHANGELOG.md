# Changelog

All notable changes to the **btree-store** project will be documented in this file.

## [0.1.9] - 2026-06-29

### Added
- **Conditional Update API**: Added atomic `Txn::update` and C `txn_update` APIs that update only existing keys and return a boolean flag instead of reporting a missing key as an error.
- **Update Regression Coverage**: Added single-bucket, `exec_multi`, and FFI regression tests plus cargo-fuzz model coverage for successful `update(false)` paths and follow-up writes in the same transaction.

### Changed
- **API Documentation**: README examples, FFI guide, and rustdoc for `Txn` and `ReadOnlyTxn` now document the byte-oriented return values, update semantics, and callback-scoped handle lifetime more accurately.

## [0.1.8] - 2026-06-23

### Fixed
- **Multi-Bucket No-Op Commits**: `exec_multi` now skips catalog updates for touched buckets whose existing root did not change, avoiding unnecessary sequence bumps and snapshot churn.
- **Empty Bucket Creation**: `exec_multi` now distinguishes missing buckets from existing empty buckets, so successful touches of new empty buckets still create catalog entries while existing empty bucket no-ops remain no-op commits.
- **Single-Bucket Empty Bucket No-Op Commits**: `exec` now skips catalog updates when an existing empty bucket's root did not change, avoiding unnecessary sequence bumps and the empty-bucket catalog churn that could surface after repeated `compact` and `reopen` sequences.
- **Snapshot Handle Consistency**: Cloned and same-path reopened handles now use a consistent local metadata snapshot without taking writer locks from active read callbacks, avoiding clone/open self-deadlocks.
- **Shared Cache Stability**: Same-path reopen snapshot sync no longer clears shared bucket caches when only the local handle snapshot needs to be refreshed.
- **Freelist Retirement Across Reopen**: Repeated `compact` + `reopen` cycles now retire only freelist pages that actually left the active freelist, preventing live freelist pages from being reclaimed and later reused as catalog data.
- **Compaction Tail Relocation Walk**: Tail-page compaction now snapshots reverse-index candidates before relocating them, avoiding reverse-tree mutation while iterating the candidate range.

### Added
- **Fuzz Tests**: add cargo-fuzz state-machine targets for kv, multi-bucket, reopen+compact, bucket lifecycle, and concurrent snapshot scenarios



## [0.1.7] - 2026-05-12

### Added
- **MSRV Declaration**: Added an explicit Minimum Supported Rust Version (MSRV) of `1.95` via `Cargo.toml` (`rust-version = "1.95"`).

## [0.1.6] - 2026-05-07

### Changed
- **Open Reuse Semantics**: `BTree::open(path)` reuses an existing in-process instance for the same normalized path and returns a refreshed clone.
- **Read-Path Performance**: Added `lid -> pid` hot cache, optimized branch child-position lookup, and reduced `view` setup overhead with earlier bucket-tree cache hits and root-node fast path.
- **Compaction Default Policy**: Default compaction (`target_bytes == 0`) now uses strict no-growth planning when low-address free pages are insufficient.

### Fixed
- **Reopen Snapshot Freshness**: Reused handles sync to latest in-memory snapshot state, avoiding stale-sequence no-op commit conflicts.

## [0.1.5] - 2026-04-27

### Fixed
- **Fresh Store Creation Durability**: Sync the parent directory after writing and syncing the initial store file so a newly created database file is not lost across a power failure before the directory entry is persisted.

## [0.1.4] - 2026-02-08

### Added
- **C FFI**: Added `ffi` feature with `cdylib`/`staticlib` outputs, public C headers and example, and FFI documentation.

### Changed
- **CI Coverage**: Added FFI build and C example checks across Linux/macOS/Windows (plus FreeBSD).

## [0.1.3] - 2026-02-08

### Added
- **Tail-Window Compaction**: Added `BTree::compact(target_bytes)` with `CompactStats`, best-effort tail relocation/truncation, and new compaction tests.
- **Logical Page Mapping**: Introduced `PageStore`/`LogicalStore` with mapping + reverse indexes to relocate pages during compaction.
- **Read-Path Caches**: Added shared meta snapshots plus bucket root/tree and LID->PID caches to reduce refresh and lookup overhead.

### Changed
- **On-Disk Format v3**: 32-bit page ids and new catalog/mapping/reverse roots (max ~16 TB with 4 KB pages); v1/v2 files now rejected with `Error::Invalid`.
- **Freelist & Commit Pipeline**: Free space is persisted as merged extents in freelist pages; commits stage freelist + superblock then sync (no `.pending` log).
- **Sync Strategy**: Uses `sync_data` unless the file grows, falling back to `sync_all` only on extension.
- **Overflow Layout**: Slots inline up to 5 page ids before spilling to index pages, reducing indirect page traffic.

### Removed
- **Benchmark Report**: Removed `benchmark.md` and its README reference.

## [0.1.2] - 2026-01-28

### Added
- **Performance Benchmarking**: Integrated `criterion` benchmarks to evaluate core engine metrics, covering batch writes, random reads, and concurrent access.
- **Detailed Performance Documentation**: Added `benchmark.md` containing baseline and optimized performance results for transparency.

### Changed
- **Lock-Free Read Path**: Removed the global file `Mutex` in `Store`, enabling true parallel reading across multiple threads by leveraging thread-safe positional I/O (`pread`/`pwrite`).
- **Memory Optimization**: Switched from heap-allocated vectors to stack-allocated arrays for superblock refreshing, reducing allocation overhead in the transaction hot-path.
- **Improved Multi-Core Scalability**: Achieved a ~80% reduction in concurrent read latency (from ~4.5µs to ~860ns per op with 4 threads).

## [0.1.1] - 2026-01-18

### Added
- **Atomic Multi-Bucket Transactions**: Introduced `exec_multi` API and `MultiTxn` handle. This allows performing multiple operations across different buckets in a single atomic transaction with only one disk sync, significantly improving batch performance.
- **Enhanced Data Validation**: Reinforced `Node::validate` with physical invariant checks.
- **Torn Write Detection**: Enhanced `MetaNode::validate` to identify and reject zeroed-out blocks caused by power failures during I/O.
- **8-Byte Memory Alignment**: Guaranteed alignment for zero-copy serialization via `AlignedPage`.

### Changed
- **Closure-based Transaction API**: Introduced `exec` and `view` methods.
- **Unified Reclamation**: Consolidated page release via `Store::free_pages`.
- **Log Management**: Switched to `.pending` log truncation (`set_len(0)`).
- **Auto-Refresh**: Implicit superblock sync at transaction start.
- **API Simplification**: Continued the transition to closure-based APIs across all internal and external logic.
- **Shared Transaction State**: Clones share `pending` containers within the same process.

### Fixed
- **Double-Write Protocol**: Refined the superblock update sequence to guarantee zero-leak and zero-corruption recovery by splitting the commit into two distinct disk-sync phases.
