# Changelog

All notable changes to the **btree_store** project will be documented in this file.

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
