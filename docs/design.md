# btree-store Design

This document describes the stable architecture, persistence protocol, lifecycle rules, and
format boundaries of `btree-store`. It is a design document rather than an implementation guide:
names of internal functions and individual test cases are intentionally omitted.

## 1. Goals

`btree-store` is a persistent, embedded key-value engine with the following goals:

- atomic updates without a write-ahead log
- snapshot isolation for readers
- crash-safe publication of a complete database generation
- multiple named buckets in one database file
- predictable page-oriented storage and reclamation
- efficient point reads and ordered iteration
- a small, fixed physical format with offline migration for incompatible changes

The engine is deliberately single-writer within a process and does not provide multi-process
concurrent access to one database path.

## 2. System Model

The database consists of one physical file divided into 4096-byte pages. Two pages are metadata
slots; all remaining pages belong to the published data graph or to allocator state.

There are four logical layers:

- public handle and transaction API
  - owns the database lifecycle and exposes bucket, read, write, and multi-bucket operations
- catalog and bucket trees
  - the catalog maps bucket names to bucket metadata
  - each bucket metadata record identifies the bucket root and its layout policy
- page and value storage
  - B+ Tree nodes store keys and either inline values or references to overflow storage
  - allocator pages store reusable and retired physical extents
- runtime services
  - positional file I/O, shared metadata snapshot, page cache, and writer/read locks

The catalog tree and every bucket tree use the same physical page namespace. A published root is
therefore resolved as:

1. metadata slot to catalog root
2. catalog key to bucket metadata
3. bucket metadata to bucket root
4. tree nodes to child nodes and value pages

Runtime caches and transaction state are disposable. The two metadata slots and the pages reachable
from the selected slot are the recovery authority.

## 3. Database And Bucket Lifecycle

### 3.1 Open

Opening a database establishes one live owner for its normalized path. Same-process opens share that
owner and its runtime state; a competing process is rejected by the database file lock.

For a new database, the engine creates the two metadata slots and initializes an empty catalog.
For an existing database, opening validates both metadata candidates, selects the newest valid
generation, and reconstructs allocator state from that generation.

Runtime options such as cache capacity and synchronization policy do not change the file format.
They are fixed for a shared live instance and must not silently disagree between same-process
opens.

### 3.2 Bucket creation

Bucket creation is a catalog transaction. It atomically adds a name and its bucket metadata with an
empty root and a fixed layout policy. A name is visible only after the catalog generation is
published. Creating an existing name fails without replacing its root.

### 3.3 Bucket use

A write transaction operates on an existing bucket. A read-only view also requires an existing
bucket; no operation creates a bucket implicitly. A multi-bucket transaction may switch among
existing buckets while retaining one transaction snapshot and one publication boundary.

Bucket metadata is part of the catalog value space, not a separate metadata file. Its durable
layout policy controls how future nodes in that bucket are encoded.

### 3.4 Close and reuse

Cloning a handle shares the live store, cache, and writer serialization while keeping transaction
snapshots local to each operation. Closing the last live handle releases the file ownership. A
later open reconstructs state from the published file rather than from runtime caches.

## 4. Tree Model

The index is a copy-on-write B+ Tree:

- leaves contain sorted key/value records
- branches contain ordered child references and separator keys
- an empty root represents an empty bucket
- a mutation rewrites the affected path from leaf to root
- split and merge decisions preserve sorted order and equal leaf depth
- old pages are never overwritten by a new tree version

The tree algorithm is independent of the durable store. A transaction supplies the snapshot root,
page reader, page writer, and page ownership journal. A successful operation returns a new root;
the new root becomes durable only through generation publication.

### 4.1 Node layouts

The format supports two node layouts selected per bucket:

- plain layout
  - stores each key in full
  - uses the same representation for leaves and branches, with the node kind and slot metadata
    defining interpretation
- prefix-encoded layout
  - stores one shared key prefix followed by key tails
  - compares a searched key against prefix plus tail without changing key ordering
  - uses a self-describing node kind for encoded leaves and branches

Prefix encoding is a storage policy, not a change to key semantics. A bucket may retain its policy
across reopen, and all nodes reachable from that bucket must be interpreted using the fixed format
contract for their node class.

### 4.2 Values

Small values are stored in the leaf record. Larger values are split into raw 4096-byte overflow
pages. A slot either contains the inline value marker, a bounded set of direct overflow page IDs,
or the root of an indirect page chain for larger values.

Updating or deleting a value creates the replacement references in the new tree version and retires
the old value pages with the old tree path. Value pages are physical storage, not independently
visible records.

### 4.3 Iteration

An iterator is bound to the transaction snapshot that created it. It traverses one fixed root in
key order, and its lifetime prevents that transaction from mutating the same snapshot. It cannot
outlive the transaction that owns the snapshot.

## 5. Transactions And Snapshots

### 5.1 Snapshot isolation

Each operation observes one pair `(generation, catalog root)`:

- a write operation refreshes to the latest published generation before its closure starts
- a read view uses the latest shared snapshot available when it starts
- the root remains fixed for the lifetime of the closure and its iterators
- a concurrent publication does not change an already-started view

There is one process-level writer and multiple readers. The writer lock serializes all changes to
transaction roots and ownership journals. Reader locks allow concurrent snapshot reads while
preventing a writer from publishing against an inconsistent in-memory state.

### 5.2 Transaction state

A write transaction owns a working catalog root, bucket root updates, newly allocated pages, and
pages scheduled for retirement. This state is local to the transaction and is not shared by cloned
handles or other write calls.

Nested operations in a multi-bucket transaction use savepoints. A savepoint records the root and
page-ownership delta at its boundary. Rolling back a savepoint discards only its uncommitted changes
and releases pages allocated solely by that savepoint; pages from the published generation are
never made reusable by a local rollback.

### 5.3 Closure outcomes

A closure-returned error is a normal transaction rollback request. Its key/value and bucket error
is returned to the caller after restoring the pre-transaction logical state. Engine I/O,
corruption, address-space exhaustion, and invariant failures are outside the user error contract
and terminate through the engine's fatal fault boundary.

User panics are also outside the rollback contract. The supported release configuration terminates
the process on panic rather than promising to recover partially-owned transaction state.

## 6. Generation Publication

The metadata slot is the atomic publication switch. A generation is complete only when its metadata
slot and every page named by that slot satisfy the publication protocol.

A successful publication follows this ordering:

1. finish the working catalog and bucket roots
2. determine allocator state for the candidate generation
3. write all replacement nodes, value pages, indirect pages, and allocator-list pages
4. synchronize those dependency pages according to the configured sync policy
5. write the next metadata slot with the new generation, roots, and allocator roots
6. perform an independent publication synchronization
7. expose the new generation to in-process readers and clear transaction-local state

The metadata slot is never allowed to reference a page that has not crossed the dependency
durability boundary. If publication fails before the slot switch is durable, the previous slot
remains the recovery state. In-memory working roots and pending ownership are then discarded or
restored without changing the previously published generation.

A multi-bucket transaction executes the same protocol once. All catalog and bucket root changes are
either named by the new slot together or remain at the old generation.

## 7. Allocator And Reclamation

### 7.1 Ownership classes

For every physical page ID in the allocated file range, a stable published generation has exactly
one ownership class:

- reachable tree or value storage
- reusable extent
- retired extent
- reusable-list page
- retired-list page

No page may be both reachable and reusable, appear in two allocator classes, or be absent from all
ownership classes.

### 7.2 Reusable and retired state

The allocator maintains two separate extent sets:

- reusable pages may be allocated by a later transaction
- retired pages are no longer in the candidate data graph but may still be referenced by the
  previous metadata generation used for crash recovery

Both sets are persisted as linked extent-page chains and are restored together with the metadata
slot. Extents are ordered by page ID and adjacent ranges are merged.

### 7.3 Quarantine rule

A page removed by a COW rewrite is first placed in the candidate generation's retired state. It may
be promoted to reusable only while constructing a later generation whose fallback metadata no longer
references it. Allocator-list pages follow the same rule: list pages named by the old slot are
retired rather than overwritten in place.

Pages allocated and released entirely inside the current transaction may be recycled locally once
they are unreachable from the working roots. They were never reachable from a published metadata
slot and therefore do not need generation quarantine.

### 7.4 Allocation

Allocation consumes reusable extents in ascending page-ID order and extends the file when no suitable
reusable extent remains. Allocator metadata pages use the same allocation mechanism as data pages;
their ownership is included in the candidate generation before the metadata slot is written.

## 8. File Format

The physical file is:

| Page range | Content |
| --- | --- |
| 0 | metadata slot A |
| 1 | metadata slot B |
| 2..N | nodes, overflow pages, indirect pages, or allocator-list pages |

The initial format is version 1. The metadata record contains the magic value, generation number,
format version, catalog root, next page ID, reusable-list root, retired-list root, and checksum.
The two metadata slots are the only database-level format discriminator.

All other persisted records have fixed layouts defined by the version-1 contract. They do not carry
independent runtime version tags. A change to a persisted layout or its interpretation increments
the database format version; the normal reader rejects the incompatible database and an offline
migration produces the new format.

The format contract includes, at minimum:

- 4096-byte pages and 32-bit physical page IDs
- little-endian persisted integers and little-endian target support only
- node headers, slot widths, node-class discriminants, and branch sentinel semantics
- bucket metadata layout and its prefix-encoding policy bit
- inline value threshold, direct overflow reference capacity, and indirect-chain layout
- extent-list header, entry layout, ordering, and chain termination rules

Only metadata slots carry CRC32C. Other page classes rely on fixed structural interpretation and
producer/publication invariants rather than per-page checksums. A malformed page or broken physical
reference discovered while opening or running the engine is corruption, not a user-level key/value
error.

## 9. Recovery And Failure Boundaries

Recovery independently evaluates both metadata slots. A candidate is usable only if its checksum,
magic, format version, generation fields, root references, and allocator chains are valid. Recovery
selects the highest-generation valid candidate and ignores an incomplete or torn newer candidate.

After selecting a slot, recovery reconstructs the reusable and retired sets before exposing the live
handle. The selected catalog root is the sole source for bucket visibility and bucket roots; runtime
caches are rebuilt lazily.

The design has no separate pending log. Crash safety comes from COW page ownership plus the ordering
of dependency synchronization and metadata publication. The fallback metadata slot remains valid
through every pre-publication failure window.

## 10. Runtime Sharing And Caching

The live instance owns a shared published metadata snapshot and a physical-page cache. Cloned
handles share these runtime services but do not share mutable transaction state.

Reads use positional I/O and may run concurrently. The node cache is keyed by physical page ID and
must invalidate an entry before that page ID is reused or released. Cache contents never determine
durability or recovery and can be dropped without changing database meaning.

## 11. API And Error Boundary

The public API separates user outcomes from engine faults:

- user outcomes include missing keys, missing or existing buckets, invalid inputs, and values that
  exceed configured limits
- opening outcomes include file I/O, invalid options, database busy, and detected corruption
- live engine I/O, corruption, exhausted physical address space, and violated internal invariants
  are fatal faults rather than recoverable transaction results

This boundary prevents a damaged durable graph from being interpreted as an ordinary missing key and
keeps transaction rollback semantics limited to caller-requested closure errors.

## 12. Format Evolution

Format version 1 is intentionally fixed. Compatibility is defined at the database level, not per
node or per record. Runtime readers do not accumulate readers for incompatible historical layouts.

When a future change alters persisted bytes or their meaning:

1. define the new complete format contract
2. increment the metadata format version
3. reject the old database in the normal runtime
4. provide an offline migration that reads the old contract and publishes a new database

Changes that affect only runtime policy, such as cache sizing or synchronization mode, do not change
the format version and are not persisted as data semantics.

## 13. Design Invariants

The following invariants define correctness at the architecture boundary:

- a published metadata slot references one complete durable generation
- a reader observes one immutable root for its transaction lifetime
- a writer publishes all bucket changes in one metadata switch
- an old published page is not reused before the fallback generation stops referencing it
- every allocated physical page has exactly one published ownership class
- cache state cannot alter durable meaning
- a format change cannot be silently interpreted as the old format
