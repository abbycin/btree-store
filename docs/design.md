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

The engine is deliberately single-writer per database path: at most one process may open a path for
writing, while read-only opens take a shared lock and several may coexist.

## 2. System Model

The database consists of one physical file divided into 4096-byte pages. Two pages are metadata
slots; all remaining pages belong to the published data graph or to allocator state.

There are four logical layers:

- public handle and transaction API
  - owns the database lifecycle and exposes bucket, read, write, and multi-bucket operations
- catalog and bucket trees
  - the catalog maps bucket names to bucket metadata
  - each bucket metadata record identifies the bucket root and its layout policy
  - the catalog tree is always plain; a layout policy applies to bucket trees only
- page and value storage
  - B+ Tree nodes store keys and either inline values or references to overflow storage
  - allocator pages store reusable and retired physical extents
- runtime services
  - positional file I/O, a writer mutex, a reader epoch registry, one shared page cache, and a
    transaction-private page overlay
  - the writer's private superblock is distinct from the published snapshot, so a reader refreshing
    cannot move shared state
  - only pages a transaction does not own reach the shared cache; the instance also holds the
    in-memory allocator extent sets and a thread-affine registry slot used to probe the epoch registry
    without a lock

The catalog tree and every bucket tree use the same physical page namespace. A published root is
therefore resolved as:

1. metadata slot to catalog root
2. catalog key to bucket metadata
3. bucket metadata to bucket root
4. tree nodes to child nodes and, from a leaf slot, to inline bytes or to value pages

Runtime caches and transaction state are disposable. The two metadata slots and the pages reachable
from the selected slot are the recovery authority.

## 3. Database And Bucket Lifecycle

### 3.1 Open

Opening a database establishes one live owner for its normalized path. A process-global registry
maps each normalized path to that owner and holds it weakly, so same-process opens share its runtime
state and the file lock is released when the last handle drops. A competing process is rejected by
the database file lock, which is exclusive for a read-write open and shared for a read-only one.

For a new database, the engine creates the two metadata slots and initializes an empty catalog.
For an existing database, opening validates both metadata candidates, selects the newest valid
generation, and reconstructs allocator state from that generation.

Runtime options such as cache capacity and synchronization policy do not change the file format.
They are fixed for a shared live instance and must not silently disagree between same-process
opens; a disagreement is refused rather than resolved silently.

A read-only open never creates a file, refuses to initialize an empty one, and writes nothing at any
point, including during open. It is never the same live instance as a read-write open of the same
path, so a live read-write handle and a read-only handle of one path exclude each other even
in-process - the second open is refused as an option mismatch, before the file is even locked - while
several read-only handles coexist and a read-write open from *another* process reports the database as
busy while any of them is held. Every mutating call on a read-only handle is refused as a
recoverable user outcome before it takes the writer lock.

### 3.2 Bucket creation

Bucket creation is a catalog transaction. It atomically adds a name and its bucket metadata with an
empty root and the layout policy the caller chose at creation. That policy is immutable for the
bucket's lifetime - no operation rewrites it - and a name is visible only after the catalog
generation is published. Creating an existing name fails without replacing its root.

### 3.3 Bucket use

A write transaction operates on an existing bucket. A read-only view also requires an existing
bucket; no operation creates a bucket implicitly. A multi-bucket transaction may switch among
existing buckets while retaining one transaction snapshot and one publication boundary.

Bucket metadata is part of the catalog value space, not a separate metadata file. Its durable
layout policy controls how future nodes in that bucket are encoded.

### 3.4 Bucket removal

Bucket removal is a catalog transaction that drops the name and retires every page the bucket owned.
Retiring rather than freeing is the point: those pages return to the reusable set only through the
normal retired-to-reusable promotion, so the fallback metadata slot or an in-flight reader on an
older snapshot can still reach them.

### 3.5 Close and reuse

Cloning a handle shares the live store, cache, and writer serialization while keeping transaction
snapshots local to each operation. Closing the last live handle releases the file ownership. A
later open reconstructs state from the published file rather than from runtime caches.

## 4. Tree Model

The index is a copy-on-write B+ Tree:

- leaves contain sorted key/value records
- branches contain ordered child references and separator keys
- an empty root represents an empty bucket
- a mutation rewrites the affected path from leaf to root
- a node splits only when a compacted page still cannot hold the new entry, and the pivot is chosen
  near the midpoint; deletion never merges or rebalances, so a node may stay arbitrarily under full
  until it empties
- old pages are never overwritten by a new tree version

The tree is not separated from the durable store by an interface. It is driven by a read context
that resolves pages through the shared cache and a write context that carries the page-ownership
delta; a transaction supplies the snapshot root together with those two page contexts, the bucket's
layout policy, and a transaction-private page overlay. A successful operation returns a new root;
the new root becomes durable only through generation publication.

### 4.1 Node layouts

The format supports two node layouts selected per bucket:

- plain layout
  - stores each key in full
  - uses the same representation for leaves and branches, with a class word and slot metadata
    defining interpretation
- prefix-encoded layout
  - stores one shared key prefix followed by key tails
  - compares a searched key against prefix plus tail without changing key ordering
  - uses a self-describing node kind for encoded leaves and branches

One class word at a fixed header offset carries the node class: a plain branch is 0 and a plain leaf
is 1, read as a leaf flag, while an encoded branch is 2 and an encoded leaf is 3, read as a node kind.
A reader selects the layout from that word alone, so no node is interpreted through a hint that could
disagree with its own bytes.

An encoded node is a header, then the shared prefix, then up to three bytes of padding, then the slot
array. That padding is part of the layout rather than slack: the slot array starts at the next
four-byte boundary after the prefix. The prefix is recomputed from the keys present in the node
rather than maintained incrementally, so it is bounded by the maximum key length and by what fits in
the page, and an encoded leaf holding one key stores that whole key as its prefix with an empty tail.

Prefix encoding is a storage policy, not a change to key semantics. A bucket may retain its policy
across reopen, and all nodes reachable from that bucket must be interpreted using the fixed format
contract for their node class.

### 4.2 Values

Small values are stored in the leaf record. The inline budget is 256 bytes shared between the stored
key and the value: in the plain layout the stored key is the full key, in the prefix-encoded layout it
is the key tail, so the same key/value pair can be inline in one layout and overflow in the other.
Larger values are split into overflow pages carrying 4092 value bytes each, with the last four bytes of
the page reserved for its checksum trailer. A slot either contains the inline value marker, up to
five direct overflow page IDs, or the root of an indirect page chain for larger values; an indirect
page indexes 1022 page IDs, then a four-byte next-pointer that terminates the chain with zero, then
the same trailer. The page-ID fields a slot does not use are zero.

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

There is one writer per live database instance and multiple readers. A writer mutex serializes all
changes to transaction roots and ownership journals, and every mutating entry point takes it.
Readers do not take the writer lock: each view pins the current epoch for its whole lifetime, and
the allocator promotes retired pages to reusable only while every in-flight reader pinned at or
after the current epoch.
The epoch advances once per shared snapshot install — a normal publication, or a writer adopting a
disk-newer generation left by a failed publication — after the new generation is exposed to readers.
A reader refresh never installs a disk-newer generation into the shared snapshot; it only advances
its own handle to the shared generation, so a stale reader cannot move the shared state
mid-transaction. A long-lived view delays reclamation and grows the file, but writes are never
blocked.

The reader's own path still synchronizes. It reads the published metadata snapshot under a shared
lock, and it takes a per-shard read lock on the node cache for each page it loads. Those are the two
places a reader can delay a writer: caching a node takes the same shard's write lock, and publishing a
generation takes the metadata snapshot's write lock.
A view whose handle has fallen behind the shared sequence also flushes the node cache of the whole
live instance before adopting it, since its cached entries may belong to a generation the shared state
has moved past. The pin registry itself is bounded: a fixed set of in-memory slots covers the common
case of concurrent views without allocating, and only beyond that bound does pinning fall back to an
allocating path under a short lock.

### 5.2 Transaction state

A write transaction owns a working catalog root, bucket root updates, newly allocated pages, and
pages scheduled for retirement. This state is local to the transaction and is not shared by cloned
handles or other write calls.

Nested operations in a multi-bucket transaction use a savepoint. A savepoint is an internal,
single-depth mechanism rather than a general rollback facility: a second nested operation while one
is active violates an engine invariant. A savepoint records only the page-ownership delta at its
boundary; a failed nested operation leaves per-bucket roots untouched rather than restoring them from
the savepoint. Rolling back a savepoint discards only its uncommitted changes and
releases pages allocated solely by that savepoint; pages from the published generation are
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
3. write all replacement nodes, indirect pages, value pages, and allocator-list pages, plus the page
   that closes the gap when the id space has grown past the live file
4. synchronize those dependency pages according to the configured sync policy
5. write the next metadata slot with the new generation, roots, and allocator roots
6. perform an independent publication synchronization
7. expose the new generation to in-process readers
8. advance the reader epoch after the exposure, so a reader that pins the new epoch observes the
   new generation
9. clear transaction-local state

The metadata slot is never allowed to reference a page that has not crossed the dependency
durability boundary. If publication fails before the slot switch is durable, the previous slot
remains the recovery state. In-memory working roots and pending ownership are then discarded or
restored without changing the previously published generation.

A multi-bucket transaction executes the same protocol once. All catalog and bucket root changes are
either named by the new slot together or remain at the old generation.

A transaction that rolls back still publishes. Its closure error is not the end of the protocol: the
engine restores the pre-transaction roots and then publishes a generation whose roots are unchanged,
carrying the metadata and quarantine state the attempt consumed, because that state is durable work a
later open must see. Only a transaction that changed nothing skips publication entirely.

## 7. Allocator And Reclamation

### 7.1 Ownership classes

For every physical page ID in the data id space - page 0 and 1 are the metadata slots, and bytes past
the selected generation's last page ID are not part of it - a stable published generation has exactly
one ownership class:

- reachable tree or value storage
- reusable extent
- retired extent
- reusable-list page
- retired-list page

No page may be both reachable and reusable, appear in two allocator classes, or be absent from all
ownership classes. The writer maintains that by construction rather than by re-deriving it: the
in-memory extent set absorbs adjacent and duplicate retirements instead of recording them twice, and
allocator sets are checked for disjointness whenever they are read back from disk. Proving that every
page in the id space falls into exactly one class is the job of the offline checker and its tests, not
of a commit.

### 7.2 Reusable and retired state

The allocator maintains two separate extent sets:

- reusable pages may be allocated by a later transaction
- retired pages are no longer in the candidate data graph but may still be referenced by the
  previous metadata generation used for crash recovery or by an in-flight reader on an older
  snapshot

Both sets are persisted as linked extent-page chains and are restored together with the metadata
slot. Extents are ordered by page ID, and adjacent ranges are merged in memory and again on restore;
merging is a normalization rather than a format rule, so a file may carry adjacent extents that are
not merged and have them merged when it is opened.

### 7.3 Quarantine rule

A page removed by a COW rewrite is first placed in the candidate generation's retired state. It may
be promoted to reusable only while constructing a later generation whose fallback metadata no longer
references it, and only when no in-flight reader can still reference it: promotion is additionally
gated on every reader having pinned at or after the current epoch. Deferred pages stay in the
retired set and are published with the next generation. Allocator-list pages follow the same rule:
list pages named by the old slot are retired rather than overwritten in place.

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

The current format is version 2, and the metadata record contains the magic value, generation
number, format version, catalog root, next page ID, reusable-list root, retired-list root, and
checksum. The whole forty-byte record — magic, generation, format version, the root references and the
CRC32C at `[36, 40)` over the first thirty-six bytes — is the *discovery record*, and its layout and
checksum keep the same meaning in every format version: a tool can name a file's version before it
interprets anything else, and a record whose checksum does not match is never taken as a version vote.
The two metadata slots are the only database-level format discriminator.

The record is persisted as a fixed-width field sequence rather than a serialized encoding, so its byte
order is the target's; little-endian is a build requirement of this implementation, not a property of
the writer.

All other persisted records have fixed layouts defined by the version-2 contract. They do not carry
independent runtime version tags. A change to a persisted layout or its interpretation increments
the database format version; the normal reader rejects an incompatible database with an explicit
version refusal, and the offline migration command rebuilds an older database in the current
format.

The format contract includes, at minimum:

- 4096-byte pages and 32-bit physical page IDs
- little-endian persisted integers and little-endian target support only
- node headers, slot widths, node-class discriminants, and branch sentinel semantics
- the four-byte alignment of an encoded node's slot array after its shared prefix, and the rule that
  every payload lies between that slot array and the header's free-space offset
- the canonical form of a branch slot, which carries one child page ID and no value, and of a direct
  or indirect value slot, whose unused page-ID fields are zero
- bucket metadata layout and its prefix-encoding policy bit
- the 256-byte inline budget shared between stored key and value, the five-page direct overflow
  reference capacity, and the indirect-chain layout including its next-pointer
- extent-list header, entry layout, ordering, and chain termination rules

Checksums are one rule for every page class, a CRC32C from the same implementation. A page keeps its
checksum in a single four-byte field - the first field of a node or allocator header, or the trailer of
an indirect (overflow index) or overflow (value) page - and computing it means reading that field as
zero and hashing the whole page: every other byte of the 4096 is covered, including bytes a writer never
wrote, so nothing on a page is outside the check. Each checksum is bound to the physical page id its
caller expects - the id the reader asked for, or the one the allocator handed the writer, never a value
read out of the page - and the hashed stream is the whole page with the checksum field read as zero,
followed by that page id's four little-endian bytes occupying the four bytes immediately after the
field. A verifier reproduces it by zeroing the field and substituting the page id just past it:

- node and allocator pages carry the field as the first field of their header, so the field cannot cover
  itself and no byte of the page is reserved.
- pages without a header keep a four-byte trailer, whose four bytes stand in as zeros while the checksum
  is computed.
- metadata slots keep their own 40-byte record checksum, which binds no page id.

Because the whole page is covered, a class that does not write every byte has to write it
deterministically: node work pages are allocated zeroed, the allocator zeroes the entry slots and tail
it does not fill, and the indirect and value writers zero theirs, so a reused buffer's leftovers can
neither reach the file nor make a page that verified once fail later. Bytes a writer never wrote are
therefore zero on a page built from scratch rather than whatever the allocator handed back — stale
process memory would otherwise be persisted by the page checksum that covers it. A page that releases
space in place keeps that page's own earlier bytes in the released span instead - a deleted slot in
either layout, an inline value replaced by a shorter one in either layout, or a prefix-encoded leaf
rewritten in place by an inline-to-overflow update or a prefix change; the span is never interpreted,
and the checksum covers it as it is. The plain layout rebuilds a value update that grows, crosses the
inline budget, or replaces an overflow value with an inline one into a fresh zeroed page, so that
path's released span stays zero.

A page written to the wrong offset, two pages swapped inside a run, or a reader asking for the wrong id
therefore fail verification instead of being interpreted under the wrong identity; a page copied to the
same id (the consistent-snapshot path) keeps its checksum, while a copy to a different id has to be
resealed. Metadata slots are the exception, because their record binds no page id: the same record may
legitimately be written to either slot unchanged, and a misdirected slot write is caught by the
generation selection rather than by the checksum. A malformed page, a failed page checksum or a broken
physical reference discovered while opening or running the engine is corruption, not a user-level
key/value error.

## 9. Recovery And Failure Boundaries

Recovery independently evaluates both metadata slots. A slot is usable only if its checksum, magic
and format version are valid: a slot whose checksum is valid under an unsupported version is refused
with an explicit version error, never treated as a torn slot, and two slots that disagree on the
version are refused as well. Among usable slots recovery selects the highest generation and ignores
an incomplete or torn newer candidate.

Allocator chains are validated after that selection, together with the catalog and the rest of the
selected generation; a failure there is reported for the generation that was selected, and recovery
does not silently fall back to an older candidate.

After selecting a slot, recovery reconstructs the reusable and retired sets before exposing the live
handle. The selected catalog root is the sole source for bucket visibility and bucket roots; runtime
caches are rebuilt lazily.

The design has no separate pending log. Crash safety comes from COW page ownership plus the ordering
of dependency synchronization and metadata publication. The fallback metadata slot remains valid
through every pre-publication failure window.

## 10. Runtime Sharing And Caching

The live instance owns a shared published metadata snapshot and a physical-page cache. Cloned
handles share these runtime services but do not share mutable transaction state; each handle adopts
its own copy of the snapshot and refreshes it when the shared sequence moves.

Reads use positional I/O and may run concurrently. The node cache is keyed by physical page ID,
sharded so that unrelated pages do not contend, and evicted per shard by a clock hand that ages
entries across hot, warm and cold levels rather than treating every hit alike. It must invalidate an
entry before that page ID is reused or released. Cache contents never determine durability or
recovery and can be dropped without changing database meaning; a capacity of zero disables caching
rather than failing.

## 11. API And Error Boundary

The public API separates user outcomes from engine faults:

- user outcomes include missing keys, missing or existing buckets, invalid inputs, values that
  exceed configured limits, and mutating calls refused because the handle is read-only
- open and snapshot outcomes include file I/O, invalid options, database busy, detected corruption,
  and the same read-only refusal; the type behind them is also the return type of live operations,
  not only of opening
- the offline checker reports its own recoverable outcomes - a non-regular file and a file that
  changed underneath it are its own, while a held lock, no valid metadata record, and an unsupported
  or mixed format version are the same conditions a live open refuses
- live engine I/O, corruption, exhausted physical address space, and violated internal invariants
  are fatal faults rather than recoverable transaction results

This boundary prevents a damaged durable graph from being interpreted as an ordinary missing key and
keeps transaction rollback semantics limited to caller-requested closure errors. A fault is not a
panic: the engine prints one structured diagnostic and terminates the process, in every build
profile, so a caller that must survive a damaged database has to isolate the read in a subprocess.

## 12. Format Evolution

The current format is version 2, and its version number is fixed for this contract. Compatibility
is defined at the database level, not per node or per record. Runtime readers do not accumulate
readers for incompatible historical layouts: the library reads exactly one version, and the
`btree-store migrate` CLI is the only code that understands an older one.

When a change alters persisted bytes or their meaning:

1. define the new complete format contract
2. increment the metadata format version
3. reject the older database in the normal runtime with an explicit version error
4. keep a frozen, read-only decoder for each version the migration must accept, and rebuild the
   database in the current format from any of them in one step

The metadata discovery record is frozen: magic at `[0, 8)`, generation at `[8, 16)`, format version
at `[16, 20)`, the root references, and CRC32C at `[36, 40)` over the first thirty-six bytes. Every
version keeps that layout and that checksum, so the migration can name a source's version before it
interprets anything else: a slot whose checksum does not match never votes and is skipped as torn, a file
with no valid record at all is refused as corrupt, and a version this build does not read is refused by
name. A version that needs a different
record layout or checksum has to extend discovery (for example by keeping a first-generation record
that every later build can read) rather than change the frozen one: a build that changed it could
neither read nor even diagnose any older file.

Changes that affect only runtime policy, such as cache sizing or synchronization mode, do not change
the format version and are not persisted as data semantics.

## 13. Design Invariants

The following invariants define correctness at the architecture boundary:

- a published metadata slot references one complete durable generation
- a reader observes one immutable root for its transaction lifetime
- a writer publishes all bucket changes in one metadata switch
- an old published page is not reused before the fallback generation stops referencing it
- a page is not reused while an in-flight reader can still reference it
- the reader epoch advances exactly once per shared snapshot install, so the promotion gate
  compares against the true generation boundary
- every allocated physical page has exactly one published ownership class
- cache state cannot alter durable meaning
- a format change cannot be silently interpreted as the old format
- the metadata discovery record is frozen, so a reader can name the format version of a file it cannot
  otherwise interpret
- a page whose checksum does not match the expected page ID is never interpreted
- the offline migration publishes only a complete, verified target, replacing whatever held
  the destination name and never the source
