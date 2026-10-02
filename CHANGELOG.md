# Changelog

All notable changes to the **btree-store** project will be documented in this file.

## [2.0.0] - 2026-10-01

### Breaking
- **On-disk format version 2.** Every database written by a 1.x release is format version 1 and is
  refused by the 2.0 runtime with `OpenError::Corruption` code `UNSUPPORTED_FORMAT_VERSION`; the
  runtime does not read or guess. Rebuild each file offline with
  `btree-store migrate <source> --output <destination> --to 2`. The source is never modified and is
  never itself the destination, the source is fully validated before any output file exists, and the
  destination is replaced by rename only after the rebuilt file passes verification. The
  `btree-store migrate` CLI is the only code that understands version 1. See the
  [migration guide](docs/migration.md).
- **Removed `iter_uncached`.** `Txn::iter_uncached` and `ReadOnlyTxn::iter_uncached` no longer exist;
  every iterator uses the shared node cache.
- **`OpenOptions` gained a `read_only` field.** Struct-literal construction must set it.
  `OpenOptions::new()` and `OpenOptions::default()` are unchanged and default it to `false`.
- **New error variants `Error::ReadOnly` and `OpenError::ReadOnly`.** Neither enum is
  `#[non_exhaustive]`, so a downstream exhaustive `match` needs a new arm.

### Changed
- **Page checksums cover the whole page.** One CRC32C per page, in the node/allocator header or the
  value/indirect trailer, computed over all 4096 bytes with the field read as zero and bound to the
  expected physical page id. A wrong offset, a swapped pair or a wrong id now fails verification
  instead of being read under the wrong identity; a value page carries 4092 bytes, not 4096.
- **Deterministic page bytes.** Node work pages are allocated zeroed and every page class writes what
  it leaves, so a page can neither persist stale process memory nor depend on allocation history. The
  catalog is rewritten in bucket-name order, so the same workload over the same generation history
  writes the same bytes.
- **Frozen metadata discovery record.** Magic, generation, format version, the root references and the
  CRC32C at `[36, 40)` keep their place and meaning in every format version, so a tool can name a
  file's version before interpreting anything else.
- **Three-level node cache.** Branch nodes start HOT and leaf nodes WARM; hits age an entry up, hand
  visits age it down.
- **Destination names validated up front.** A destination that names no file (`--output dir/.`) is an
  argument error before the source is opened, not a failure after the rebuild.

### Added
- **`btree-store check <file> [--summary] [--scan]`** and the `check_path` API: a read-only audit of a
  static v2 file covering metadata, the catalog and bucket trees, value and indirect chains, allocator
  extents, page ownership and checksums, without modifying the source. `--summary` and `--scan` add
  space accounting and a per-bucket block whose totals agree with `compact --info`; neither changes
  the verdict, and nothing is printed for a file that did not pass. See [the CLI reference](docs/cli.md).
- **`btree-store compact [rebuild|inplace] <source> [destination] [--info]`**: rebuilds a compact copy
  through a staging file, keeping the source's bucket prefix policies, and publishes only after the
  staging file passes the same audit and matches the source record for record. The source is never
  modified and is never itself a destination; the rebuild needs room for a second copy, and `inplace`
  is reserved but not implemented.
- **`BTree::take_snapshot`**: writes the current published generation to a standalone file and returns
  the generation it equals. Writers are not blocked and later generations never appear in it. The
  destination is used exactly as given and is refused if it is this store's own file; it is written
  directly rather than staged, so a failed call can leave a partial file.
- **Read-only opens (`BTree::open_read_only`, `OpenOptions::read_only`)**: never create or initialise a
  file and write nothing, not even during open. They take a shared lock, so several coexist and several
  processes may read one file; mutating calls return `Error::ReadOnly` without touching it.
- **Bucket policy and same-file queries**: `BTree::buckets_with_policy` reports each bucket's
  prefix-encoding flag, and `BTree::path_is_same_file` compares a path against this store's own file.

## [1.1.1] - 2026-09-05

### Fixed
- **Published Snapshot Refresh**: Prevented concurrent readers from adopting a writer's unpublished in-memory superblock while a generation is being persisted, which could advance a handle's sequence and trigger `COMMIT_SEQUENCE_CONFLICT` during a valid serialized commit.
- **Monotonic Handle Snapshots**: Prevented a reader holding an older published snapshot from regressing a shared handle after a writer has published a newer generation, eliminating the resulting `COMMIT_SEQUENCE_CONFLICT` on the next commit.
- **Snapshot Enumeration**: `BTree::buckets` now traverses the handle's published metadata snapshot instead of the mutable store cache.
- **Regression Coverage**: Added a test for reader refresh during the unpublished superblock window.

## [1.1.0] - 2026-09-02

### Added
- **Read/Write Non-Blocking Concurrency**: Readers pin an epoch snapshot instead of taking the
  writer lock, so a view never blocks a writer and a writer never blocks a view's traversal.
  Retired pages are promoted to reusable only while no in-flight reader can still reference them;
  deferred pages stay quarantined and are published with the next generation. The on-disk format
  is unchanged and existing databases open as before.

## [1.0.0] - 2026-08-08

### Added
- **Prefix-Encoded Buckets**: Added optional per-bucket shared-prefix encoding for B+ Tree nodes.
- **Durable Generation Reclamation**: Persisted reusable and retired allocator extents across commits and reopen.

### Changed
- **Physical Page Storage**: Reworked the store around direct physical page IDs, COW roots, double-buffered metadata, and two-phase generation publication.
- **Runtime Architecture**: Added shared positional I/O and a sharded physical-node cache with page-ID invalidation.

### Removed
- **Tail Compaction**: Removed tail-window relocation, truncation, and related compaction APIs.
- **Logical Mapping Layer**: Removed the logical page namespace, mapping/reverse trees, and dedicated translation caches.
- **C FFI**: Removed the FFI feature, C headers, examples, and generated library targets.

## [0.1.10] - 2026-07-06

### Added
- **Configurable Runtime Open Options**: Added `OpenOptions`, `SyncMode`, and `BTree::open_with_options` so callers can tune node cache, `lid -> pid` caches, shared bucket root/tree caches, and post-commit sync policy. Reopening the same path in-process with different runtime options now returns `Error::Invalid`.
- **Uncached Iterator APIs**: Added `Txn::iter_uncached` and `ReadOnlyTxn::iter_uncached` for scan-heavy reads that bypass leaf-node and overflow-value-page caching while preserving hot upper-level branch paths.
- **Public Key-Length Limit Helper**: Exported `MAX_KEY_LEN` in Rust and `btree_max_key_len()` in the C API so applications can validate keys and bucket names against the engine limit before issuing operations.

### Changed
- **Bucket Cache Eviction Policy**: Shared bucket root/tree caches now use bounded second-chance eviction instead of unbounded maps, keeping hot buckets resident under churn without adding write-path locking to `view()`.
- **Uncached Scan Cache Behavior**: Uncached iterators now preserve internal `lid -> pid` mapping-cache reuse while avoiding leaf-node cache warming, so repeated scans do not keep large leaf pages hot in the node clock cache.

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
- **Single-Bucket Empty Bucket No-Op Commits**: `exec` now skips catalog updates when an existing empty bucket's root did not change, avoiding unnecessary sequence bumps and empty-bucket catalog churn.
- **Snapshot Handle Consistency**: Cloned and same-path reopened handles now use a consistent local metadata snapshot without taking writer locks from active read callbacks, avoiding clone/open self-deadlocks.
- **Shared Cache Stability**: Same-path reopen snapshot sync no longer clears shared bucket caches when only the local handle snapshot needs to be refreshed.

### Added
- **Fuzz Tests**: add cargo-fuzz state-machine targets for kv, multi-bucket, bucket lifecycle, and concurrent snapshot scenarios



## [0.1.7] - 2026-05-12

### Added
- **MSRV Declaration**: Added an explicit Minimum Supported Rust Version (MSRV) of `1.95` via `Cargo.toml` (`rust-version = "1.95"`).

## [0.1.6] - 2026-05-07

### Changed
- **Open Reuse Semantics**: `BTree::open(path)` reuses an existing in-process instance for the same normalized path and returns a refreshed clone.
- **Read-Path Performance**: Added `lid -> pid` hot cache, optimized branch child-position lookup, and reduced `view` setup overhead with earlier bucket-tree cache hits and root-node fast path.

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
- **Logical Page Mapping**: Introduced `PageStore`/`LogicalStore` with a forward LID-to-PID mapping for logical page access.
- **Read-Path Caches**: Added shared meta snapshots plus bucket root/tree and LID->PID caches to reduce refresh and lookup overhead.

### Changed
- **Page Addressing**: Added 32-bit page ids and catalog/mapping roots (max ~16 TB with 4 KB pages).
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
