# btree_store Architecture and Design Document

## 1. System Overview

**btree_store** is a persistent, embedded key-value storage engine written in Rust. It is designed for reliability, crash safety, and high performance using a B+ Tree data structure with Copy-On-Write (COW) semantics.

### Key Design Principles
- **Copy-On-Write (COW):** Modifications never overwrite existing data in place. Instead, new pages are allocated, and changes propagate up to a new root. This ensures that the database is always in a consistent state.
- **Crash Safety:** A combination of COW, atomic superblock updates, and a "Pending Log" (Write-Ahead Log for metadata) ensures ACID properties.
- **Page-Based Storage:** The file is divided into fixed-size pages (default 4KB), managed by a central `Store` allocator.

---

## 2. Architecture Layers

The system is layered as follows:

### 2.1. User API Layer (`src/lib.rs`)
- **`BTree`**: The main entry point. It manages a "Catalog Tree" which maps bucket names to their Root Page IDs.
- **`Bucket`**: A handle to a specific named B+ Tree. Users perform `put`, `get`, `del`, and `iter` operations here.

### 2.2. Tree Logic Layer (`Tree` struct in `src/lib.rs`)
Implements the core B+ Tree algorithms.
- **Traversal:** `traverse_to_leaf` moves from root to leaf.
- **Modifications:**
    - **Insert (`put`):** Uses `TxContext` to track allocated pages. If a node is full, it splits (`Node::split`), propagating the split upwards. If the root splits, the tree height increases.
    - **Delete (`del`):** If a node becomes empty, it is removed. If the root becomes a branch with only one child, it "collapses" to reduce height.
- **Transaction Context (`TxContext` & `CommitContext`):**
    - Tracks `freed` (pages to be released) and `alloc` (newly allocated pages) during a logical operation.
    - **RAII Rollback:** If an operation returns `Err`, the `CommitContext` destructor ensures allocated pages are returned to the free list and pending frees are discarded.

### 2.3. Node Layer (`src/node.rs`)
Defines the binary layout of a B-Tree node within a page.
- **Structure:**
    - **Header:** `leaf` (u8), `num_children` (u16), `checksum` (u64).
    - **Slots:** An array of `Slot` structs, growing from the header.
    - **Data Heap:** Grows from the end of the page towards the start.

#### Slot Structure & Polymorphism
The `Slot` struct is interpreted differently based on the node type:

```rust
struct Slot {
    pos: u32,       // Offset in the Data Heap
    klen: u32,      // Key Length
    vlen: u32,      // Value Length (0 for branch nodes)
    page_id: [u64; 3], // Context-dependent usage
}
```

- **Branch Nodes (`is_leaf = 0`):**
    - **Key:** Separator key stored at `pos`.
    - **Value:** `vlen` is always 0.
    - **Page ID:** `page_id[0]` stores the **Child Page ID**. Other `page_id` fields are unused.

- **Leaf Nodes (`is_leaf = 1`):**
    - **Key:** Actual key stored at `pos`.
    - **Value:**
        - **Inline:** If `klen + vlen <= 64`, the value is stored immediately after the key in the heap. `page_id` is all zeros.
        - **Overflow:** If larger, the value is stored in separate overflow pages.
            - `page_id` stores up to 3 direct Page IDs.
            - If more are needed, `page_id[0]` points to an **Indirect Page** (linked list of pages containing Page IDs).

### 2.4. Storage Layer (`src/store.rs`)
Manages the physical file, page cache, and free space.
- **`Store`**: Thread-safe access to the file.
- **`NodeCache`**: A sharded LRU cache to reduce disk reads.
- **`FreeList`**: A linked list of `FreeNode` pages stored within the database file itself, tracking unused pages.
- **Superblock (`MetaNode`)**: Stores the current transaction sequence, root page ID, and free list head.

---

## 3. Core Mechanisms

### 3.1. Crash Safety & Commit Protocol
The system uses a **2-Phase Commit** inspired approach with a helper log (`.pending`) to handle page lifecycle safely.

1.  **Phase 1: Prepare (In-Memory & Data Sync)**
    - All new data and nodes are written to *newly allocated* pages.
    - `fsync` is called to ensure these new pages are durable.
    - **Invariant:** The old tree structure is untouched on disk.

2.  **Phase 2: Write Pending Log**
    - A `.pending` file is written containing:
        - `freed`: List of Page IDs that *will* be freed if commit succeeds.
        - `alloc`: List of Page IDs allocated in this transaction.
    - This log is `fsync`ed.

3.  **Phase 3: Atomic Switch**
    - The **Superblock** is updated to point to the new Root Page ID.
    - This is the "Point of No Return". Once the Superblock is synced, the transaction is committed.

4.  **Phase 4: Post-Commit Cleanup**
    - **Physical Free:** Pages in the `freed` list are added to the on-disk Free List.
    - **Log Clear:** The `.pending` file is deleted.

**Recovery Logic (`recover_pending`):**
- **Undo-Alloc:** If crash happens *before* Superblock update, the transaction is invalid. The `alloc` pages from the log are reclaimed (freed).
- **Redo-Free:** If crash happens *after* Superblock update (but before Post-Commit), the transaction is valid. The `freed` pages from the log are reclaimed.

### 3.2. Concurrency Control
- **RwLock:** The `BTree` uses `RwLock` for the Root Page ID and Free/Alloc lists.
    - Multiple Readers: Allowed (read `root_page_id`).
    - Single Writer: Only one thread can `commit` at a time.
- **Deadlock Prevention:**
    - Strict locking order is enforced: **Lock `pending_free` FIRST, then `pending_alloc`**.

### 3.4. Large Value Storage (Overflow Pages)
Values larger than `MAX_INLINE_LEN` (64 bytes) are stored in separate overflow pages to keep B-Tree nodes compact.

- **Thresholds:**
    - **Inline:** Value is stored directly in the Node's data heap.
    - **Direct Overflow:** If the value fits in up to 3 pages (`NR_INLINE_PAGE`), the Page IDs are stored directly in `Slot.page_id`.
    - **Indirect Overflow:** If more than 3 pages are needed, `Slot.page_id[0]` points to an **Indirect Page Chain**.

- **Indirect Page Chain Structure:**
    - An Indirect Page is a standard 4KB page containing:
        - **Data Page IDs:** Array of `u64` pointing to the actual value content.
        - **Next Pointer:** The last 16 bytes contain `next_indirect_page_id` (8 bytes) and `checksum` (8 bytes).
    - This forms a linked list of index pages, allowing for arbitrarily large values.

- **Allocation & Freeing:**
    - **Allocation:**
        1.  Calculate required data pages.
        2.  Allocate data pages in batch.
        3.  If indirect, allocate necessary index pages.
        4.  Write `next` pointers to link index pages.
    - **Freeing:**
        1.  Read `Slot`.
        2.  If indirect, traverse the linked list of index pages.
        3.  For each index page: verify checksum, free all referenced data pages, then free the index page itself.
        4.  This ensures no leakage of data or index pages.

### 3.5. Crash Recovery Details
The system recovers state using the `.pending` log file found on startup.

#### Recovery Scenarios
The recovery logic compares the **Log Sequence (`Log.seq`)** with the **Superblock Sequence (`SB.seq`)**.

1.  **Redo-Free (Commit Completed, Cleanup Failed)**
    - **Condition:** `Log.seq == SB.seq`
    - **State:** The Superblock was successfully updated to the new root, but the old pages (from the `freed` list) were not yet added to the on-disk free list.
    - **Action:** Iterate through the `freed` list in the log and call `free_pages()` for each. This "replays" the cleanup phase.

2.  **Undo-Alloc (Commit Failed)**
    - **Condition:** `Log.seq == SB.seq + 1`
    - **State:** The transaction prepared new pages and wrote the log, but crashed *before* updating the Superblock. The DB is still pointing to the old root.
    - **Action:** We must reclaim the space used by the "future" pages.
        - **Reused Pages (`pid < SB.next_page_id`):** Do nothing. The Superblock still considers these free (since it wasn't updated), so they are safe.
        - **Appended Pages (`pid >= SB.next_page_id`):** These pages physically exist but are unknown to the Superblock. We call `free_pages()` to explicitly add them to the free list, making them available for future use.

3.  **Invalid/Corrupt Log**
    - **Condition:** Checksum mismatch or incomplete write.
    - **Action:** The log is ignored and deleted. The database starts from the last valid Superblock state (Atomic Rollback).

---

## 4. File Format

The database file consists of a sequence of **4096-byte pages**.

| Page ID | Content | Description |
| :--- | :--- | :--- |
| 0 | `MetaNode` | Primary Superblock. |
| 1 | `MetaNode` | Backup Superblock (Double Buffering). |
| 2..N | Data / Free | B-Tree Nodes or Free List Nodes. |

### 4.1. MetaNode (Superblock)
```rust
struct MetaNode {
    magic: u64,          // 0x636f776274726565 ("cowbtree")
    version: u64,        // 1
    page_size: u64,      // 4096
    root_current: u64,   // Page ID of the current B+ Tree root
    root_backup: u64,    // Page ID of the previous root
    next_page_id: u64,   // High-water mark for file size
    free_list_head: u64, // Head of the FreeNode chain
    nr_free: u64,        // Total number of free pages
    seq: u64,            // Monotonic transaction sequence number
    checksum: u64,       // CRC32C of this struct
}
```

### 4.2. Pending Log Format
The `.pending` file is a temporary WAL used only during commit.
- **Header:** `seq`, `nr_freed`, `nr_alloc`, `checksum`.
- **Body:** Array of `PendingEntry` structs (`page_id`, `nr_pages`).

---

## 5. Implementation Details

- **Iterators:**
    - `TreeIterator` maintains a stack of `(Node, index)` to perform Depth-First Search (DFS).
    - It holds a reference to `Store` to load pages lazily.
- **Checksums:**
    - Every page (Node, Meta, Free) has a CRC32C checksum.
    - Checksums are verified on read and updated on write.
- **Root Collapse:**
    - Optimization: If a root deletion results in a branch node with a single child, that child is promoted to be the new root. This keeps the tree height minimal.
