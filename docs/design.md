# btree_store Architecture and Design Document

## 1. System Overview

**btree_store** is a persistent, embedded key-value storage engine written in Rust. It is designed for reliability, crash safety, and high performance using a B+ Tree data structure with Copy-On-Write (COW) semantics and a closure-based transaction model.

### Key Design Principles
- **Copy-On-Write (COW):** Modifications never overwrite existing data in place. This ensures the database is always in a consistent state.
- **Transactional API:** All operations are wrapped in `exec` or `view` closures, providing automatic ACID guarantees and seamless version management.
- **Auto-Refresh Snapshot Isolation:** Transactions automatically sync to the latest disk version upon start, eliminating manual snapshot management.

---

## 2. Architecture Layers

### 2.1. User API Layer (`src/lib.rs`)
- **`BTree`**: The central coordinator. It manages the Catalog Tree which maps bucket names to Root Page IDs.
    - **`exec<F, R>(bucket, f)`**: Starts a read-write transaction on a single bucket. It acquires a process-level write lock, refreshes to the latest disk state, and executes the closure.
    - **`exec_multi<F, R>(f)`**: Executes multiple operations across different buckets in a single atomic transaction. It caches root updates in memory and performs a single `commit_internal()` at the end, minimizing disk I/O.
    - **`view<F, R>(bucket, f)`**: Starts a read-only transaction. It acquires a process-level read lock.
- **`MultiTxn`**: A specialized handle for `exec_multi`. Its `exec` method allows switching between different buckets within the same transaction context.
- **`Clone` Behavior**: Cloned handles share the same underlying store, writer lock, pending alloc/free trackers, and read caches, while keeping per-handle local snapshot markers (`start_seq`, `start_root_id`).
- **Same-Path Open Reuse**: Within one process, `BTree::open(path)` reuses an already-open instance for the same normalized path and returns a refreshed clone of that live instance, rather than creating an independent second opener.

> **Note:** Concurrent access from multiple processes is not supported. Within a single process, the supported model is one live instance per database path, shared via cloning/open reuse.

### 2.2. Tree Logic Layer (`Tree` struct in `src/lib.rs`)
Implements core B+ Tree algorithms:
- **COW Propagation:** Every modification creates a new path from leaf to root.
- **Transaction Context:** Tracks `freed` and `alloc` pages. 
- **Internal Commit:** Performed at the end of `exec`, involving free list persistence, a double-buffered superblock switch, and storage sync.

### 2.3. Node Layer (`src/node.rs`)
Defines binary layout with strict 8-byte alignment and reinforced validation:
- **Zero-Copy:** Casts raw memory directly to `&NodeHeader` or `&Slot`.
- **Physical Invariants:** `Node::validate` strictly checks `is_leaf` flags, element counts, and data offset boundaries to detect corruption before it propagates.
- **Dirty Management:** All mutation functions (e.g., `shrink_slot`, `expand_slot`) automatically set the `dirty` flag, guaranteeing that `finalize` updates the checksum before disk I/O.
- **Slot Layout:** `Slot` keeps a 32-byte size and stores up to 5 inline page ids (`page_id[5]`) for small overflow values, avoiding index pages for up to 5 pages.

### 2.4. Storage Layer (`src/store.rs`)
- **`Store`**: Manages file I/O and a sharded clock cache.
- **Granular Invalidation:** Cache invalidation is integrated directly into `alloc_pages` and `free_pages`. This ensures that any page ID being reused or released is immediately purged from the memory cache across all threads.
- **Consolidated Reclamation:** A unified `free_pages` method handles all physical page releases, ensuring the on-disk free list remains consistent during commits and rollbacks.
    - The cache uses a sharded clock-style eviction policy for low overhead under concurrency.

---

## 3. Core Mechanisms

### 3.1. Transaction Isolation & Auto-Refresh
The engine implements **Snapshot Isolation (SI)** with an focus on usability:
1.  **Start-Time Sync:** `exec` calls `refresh_internal()` (refreshes `root_current` from the Superblock and clears the `NodeCache`). `view` reads a process-wide shared snapshot `(seq, root)` and clears the cache when `seq` changes.
2.  **Snapshot Stability:** Once a closure starts, its root ID is fixed. Even if another process commits, the current closure's view remains stable.
3.  **Conflict Detection:** `commit_internal()` rejects commits when the caller's `start_seq` is stale and returns `Error::Conflict`. Under the supported single-process model, same-path handles share one writer lock and are normally refreshed before `exec`, so ordinary same-process writers serialize instead of racing to conflict. The conflict path remains the guardrail for stale local snapshots.

### 3.2. Automatic Rollback Logic
When an `exec` closure returns `Err`, the following occurs:
1.  **Catalog Restoration:** The Catalog Tree's root is reset to its pre-transaction state.
2.  **Physical Reclamation:** All pages allocated during the failed transaction are immediately released back to the `Store`'s free list.
3.  **Pending State Clear:** Uncommitted `pending_free` and `pending_alloc` lists are purged.

### 3.3. Double-Write Commit Protocol
Ensures crash-safe metadata updates:
1.  **Write Free List + SB:** Write free list pages and stage the new roots in the double-buffered superblock.
2.  **Sync Data:** Flush data and metadata (`sync_all`).
3.  **Clear Pending State:** Clear pending alloc/free trackers in memory.

---

## 4. File Format

| Page ID | Content | Description |
| :--- | :--- | :--- |
| 0 | `MetaNode` (A) | Superblock Buffer 0 |
| 1 | `MetaNode` (B) | Superblock Buffer 1 |
| 2..N | Nodes / Data | B-Tree nodes, overflow data, or free list pages. |

**Limits**
- **Page ID width:** 32-bit page ids are used on disk (including index pages).
- **Max file size:** ~16 TB with 4 KB pages.

---

## 5. Implementation Details

- **Iterators:** `TreeIterator` captures the root ID at the time of creation (`txn.iter()`), ensuring a stable view even if the transaction performs further writes later.
- **Automatic Sync:** `BTree::open` performs recovery on startup (v2 has no `.pending` log).

---

## 6. Space Management

**Free List Structure**
- Free space is tracked as sorted, merged extents `(page_id, nr_pages)`.
- `freelist_add_extent` inserts in order and coalesces adjacent ranges, keeping the list ordered by `page_id`.
- This ordering enables efficient prefix scans (e.g., counting free pages below a limit).

**Allocation Strategy**
- Normal allocations scan a limited number of low-address extents first, then take from higher extents, and finally extend the file if needed.
- Free list pages themselves are also allocated via the same allocator and stored on disk as linked pages.

**Large Value Storage**
- Leaf values with `key_len + value_len <= 64` are stored inline inside the node page.
- Larger values are stored in overflow data pages: `nr_pages = ceil(value_len / PAGE_SIZE)`.
- If `nr_pages <= 5`, the slot stores the data page ids directly in `page_id[0..nr_pages]`.
- If `nr_pages > 5`, `page_id[0]` points to an indirect index chain; each index page stores page ids plus a next pointer and a checksum.
- On update/delete, overflow data pages and index pages are scheduled into `pending_free` and reclaimed after commit, returning space to the freelist.

---

## 7. Tail-Window Compaction (Current)

**Goal:** Reclaim tail space by moving live pages out of the file tail, then truncating if the tail becomes free.

**Parameters**
- `compact(target_bytes)` uses the requested byte budget to select a tail window.
- `target_bytes == 0` uses the default internal ratio (0.5).
- if total data pages `<= 1024`, compact all data pages.

**Workflow**
1. **Compute tail window:**  
   - `total_pages = next_page_id`, `usable_pages = total_pages - 2`  
   - if `usable_pages <= 1024` then `target_pages = usable_pages`  
   - else if `target_bytes == 0` then `target_pages = ceil(usable_pages * 0.5)`  
   - else `target_pages = ceil(target_bytes / PAGE_SIZE)`  
   - `tail_start = total_pages - target_pages` (clamped to `>= 2`)
2. **Move tail pages by reverse index:**  
   - Iterate `reverse_tree` (pid -> lid) from `tail_start` to `total_pages`.  
   - If low-address free pages below `tail_start` are available, compaction pre-allocates them with `alloc_pages_below`.
   - For default compaction (`target_bytes == 0`), relocation is strict no-growth: if enough low pages are not available for all planned moves, the run returns without moving pages.
   - For explicit `target_bytes`, compaction can still fall back to the normal allocator when low preallocation is unavailable or only partially available.
   - Update mapping (`lid -> new_pid`) and reverse (`new_pid -> lid`), and add old pid to `pending_free`.
3. **Commit:**  
   - `commit_internal()` persists mapping/reverse roots and merges `pending_free` into the freelist.
4. **Tail truncate attempt:**  
   - Compute a safe floor using the max pid from mapping/reverse trees, the reverse index itself, and freelist pages.  
   - Call `try_truncate_tail_with_floor(floor)`; if truncation fails, compaction still succeeds for logical relocation.

**Return Value**
- `CompactStats { moved_pages, remaining_candidates }` where `remaining_candidates` counts tail candidates not moved in this run.

**Notes**
- Low-address relocation improves the chance of freeing a contiguous tail, but truncation is still best-effort because metadata roots and freelist pages can keep the floor above `tail_start`.  
- Default compaction (`target_bytes == 0`) prefers "do not grow the file during compaction" over partial relocation, so it may return `moved_pages = 0` when low free space is insufficient.

---

## 8. Read Path Optimizations (B1/B2/B3/B4)

**B1: Shared Meta Snapshot**
- A per-path `SharedMeta { seq, root }` is stored in a process-wide registry.
- `Store::open` initializes or reuses the shared meta.
- `commit_roots` updates `(root, seq)` atomically.
- `view` uses the shared snapshot and avoids `refresh_sb` on the hot path; it only refreshes when `seq` changes.

**B2: Bucket Root Cache**
- `bucket_root_cache: HashMap<Vec<u8>, (root, seq)>` avoids catalog lookups when `seq` matches.
- `view` reads from cache when possible; otherwise it loads catalog and updates cache.
- `exec`/`exec_multi` refresh cache entries on successful commit.

**B3: Bucket Read-Only Tree Cache**
- `bucket_tree_cache` caches `ReadOnlyTree` instances per bucket, avoiding per-`view` tree construction.
- `view` checks this cache before re-reading bucket metadata; a matching `(bucket, seq)` entry can satisfy the read-only setup path directly.
- `ReadOnlyTree` preloads its root node once, avoiding a repeated root-page load on every lookup.
- Cache entries are invalidated on `seq` changes to prevent stale reads.

**B4: LID->PID Cache**
- `LogicalStore` maintains a two-level cache for `lid -> pid` lookups: a lock-free direct-mapped hot cache in front of the existing sharded cache, reducing mapping tree reads and lock traffic on hot paths.
- The cache is invalidated when the shared `seq` changes or after compaction.

**Consistency Note**
- These optimizations do not change snapshot isolation: caches are keyed by `seq`, and `view` refreshes roots when `seq` changes.
- Multi-process concurrent access is not supported and remains outside the consistency guarantees.
