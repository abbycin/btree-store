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
- **`MultiTxn`**: A specialized handle for `exec_multi`. Its `execute` method allows switching between different buckets within the same transaction context.
- **`Clone` Behavior**: Cloned handles share the same `pending_free` and `pending_alloc` containers. This ensures that multiple threads within the same process operate on a unified transaction view, preventing premature page reclamation.

> **Note:** Concurrent access from multiple processes is not supported.

### 2.2. Tree Logic Layer (`Tree` struct in `src/lib.rs`)
Implements core B+ Tree algorithms:
- **COW Propagation:** Every modification creates a new path from leaf to root.
- **Transaction Context:** Tracks `freed` and `alloc` pages. 
- **Internal Commit:** Performed at the end of `exec`, involving storage sync, WAL update, and a dual-stage Superblock switch.

### 2.3. Node Layer (`src/node.rs`)
Defines binary layout with strict 8-byte alignment and reinforced validation:
- **Zero-Copy:** Casts raw memory directly to `&NodeHeader` or `&Slot`.
- **Physical Invariants:** `Node::validate` strictly checks `is_leaf` flags, element counts, and data offset boundaries to detect corruption before it propagates.
- **Dirty Management:** All mutation functions (e.g., `shrink_slot`, `expand_slot`) automatically set the `dirty` flag, guaranteeing that `finalize` updates the checksum before disk I/O.

### 2.4. Storage Layer (`src/store.rs`)
- **`Store`**: Manages file I/O and sharded LRU cache.
- **Granular Invalidation:** Cache invalidation is integrated directly into `alloc_pages` and `free_pages`. This ensures that any page ID being reused or released is immediately purged from the memory cache across all threads.
- **Consolidated Reclamation:** A unified `free_pages` method handles all physical page releases, ensuring the on-disk free list remains consistent during commits and rollbacks.

---

## 3. Core Mechanisms

### 3.1. Transaction Isolation & Auto-Refresh
The engine implements **Snapshot Isolation (SI)** with an focus on usability:
1.  **Start-Time Sync:** Both `exec` and `view` call `refresh_internal()` before execution. This fetches the latest `root_current` from the Superblock and clears the local `NodeCache`.
2.  **Snapshot Stability:** Once a closure starts, its root ID is fixed. Even if another process commits, the current closure's view remains stable.
3.  **Conflict Detection:** If two handles overlap their `exec` calls, the first to commit wins. The second will detect that the disk's version has moved and return `Error::Conflict`. (Applies to concurrent handles within the same process).

### 3.2. Automatic Rollback Logic
When an `exec` closure returns `Err`, the following occurs:
1.  **Catalog Restoration:** The Catalog Tree's root is reset to its pre-transaction state.
2.  **Physical Reclamation:** All pages allocated during the failed transaction are immediately released back to the `Store`'s free list.
3.  **Pending State Clear:** Uncommitted `pending_free` and `pending_alloc` lists are purged.

### 3.3. Double-Write Commit Protocol
Ensures zero-leak recovery:
1.  **Sync Data:** Flush all new COW pages to disk.
2.  **Write Pending Log:** Log the intended `freed` and `alloc` sets to a `.pending` file.
3.  **Update SB (Phase 1):** Commit the new Root ID.
4.  **Finalize Free List:** Physically update the free list on disk and clear the log (via truncation).
5.  **Update SB (Phase 2):** Finalize the sequence number.

---

## 4. File Format

| Page ID | Content | Description |
| :--- | :--- | :--- |
| 0 | `MetaNode` (A) | Superblock Buffer 0 |
| 1 | `MetaNode` (B) | Superblock Buffer 1 |
| 2..N | Nodes / Data | B-Tree nodes, overflow data, or free list pages. |

---

## 5. Implementation Details

- **Iterators:** `TreeIterator` captures the root ID at the time of creation (`txn.iter()`), ensuring a stable view even if the transaction performs further writes later.
- **Automatic Sync:** Every `BTree::open` automatically performs crash recovery by checking for a `.pending` log from a previous session.