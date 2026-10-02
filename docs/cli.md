# btree-store CLI

`btree-store` is the offline maintenance tool for btree-store. It provides three commands:

```text
btree-store migrate <source> --output <destination> --to <version>
btree-store check <file> [--summary] [--scan]
btree-store compact [rebuild|inplace] <source> [destination] [--info]
```

All three work on files at rest. `migrate` reads an older version and publishes a new file; `check`
inspects a current v2 file read-only.

`compact`'s `inplace` spelling is a reserved value today: it returns an `arg` error, while the rebuild
path is implemented. The destination is a positional argument, and the default spelling - the mode word
omitted - is `rebuild`.

`migrate`'s `--to` takes a version number, and `2` is the only value this build accepts; the synopsis
spells it `<version>` because the constant moves with the format, not with the grammar.

## General conventions

### Command form

```text
btree-store <COMMAND> [OPTIONS]
```

Help:

```bash
btree-store --help
btree-store migrate --help
btree-store check --help
btree-store compact --help
```

### Exit codes

| Exit code | Meaning |
| --- | --- |
| `0` | The command succeeded, or `--help` / `-h` was requested |
| `1` | The run failed, the file is corrupt, a lock conflicted, or the check did not pass |
| `2` | An argument or command usage is wrong |

### Error output

A failed run uses one format:

```text
error: <phase>: <class>: <detail>
```

`<phase>` names the step that failed, and the vocabulary is per command rather than shared:
`arguments`, `source`, `target`, `staging`, `rebuild`, `compare`, `publish` for `migrate` and
`compact`, and `check` for `check`. A destination-shape failure is therefore reported under `target`,
not `arguments`, while a destination naming error raised before any work is `arguments`. A `migrate`
run that got as far as creating its staging file appends the staging file's fate to the detail, as
`; staging <path> (present)` or `; staging <path> (already removed)`.

A command never disguises a failure as a success summary. `check` writes no summary to stdout when it
fails or when the check is incomplete.

## `migrate`

### Usage

```bash
btree-store migrate <source> --output <destination> --to 2
```

Example:

```bash
btree-store migrate old.v1 --output current.db --to 2
```

### Arguments

| Argument | Meaning |
| --- | --- |
| `<source>` | The older-version database file. The migration reader supports v1 today. |
| `--output <destination>` | The destination for the new database; an existing one is replaced, the source never. The published file inherits the source's permission bits narrowed to owner read/write, so a group-readable database is published owner-only. |
| `--to <version>` | The target format version; it must currently be `2`. A number other than `2` is an `arg` error; a non-number is caught by the parser as a usage error and prints no `error:` line. |

### Migration flow

`migrate` works in this order:

1. Opens the source read-only and holds a shared file lock.
2. Reads the source's fixed discovery record and confirms it is a supported older version.
3. Before any target file is created, checks the source's metadata, catalog, bucket trees,
   value/overflow chains, indirect chains and allocator ownership.
4. Rebuilds the database in a private staging file, in batches, with the current v2 writer.
5. Reopens the staging file with a fresh v2 instance and compares bucket by bucket, key by key and
   value by value.
6. Once verification passes, syncs the staged bytes, then renames the staging file onto `--output`
   in one step: whatever held that name is replaced.
7. Syncs the destination directory for durability.

The source is never modified, truncated or replaced, and never becomes the destination. When a
migration fails the destination name is left as it was - with one exception: after `published-durability-unknown`
the rename has already happened, so the new file is in place and merely not known to be durable. The
staging file is removed on every failure except that one, where the rename has already consumed its
name.

### What a migration does not do

- It does not upgrade the source in place;
- It does not use the source itself as the destination (compared by device and inode, or by resolved
  path where there are no inodes; refused with `arg`, except when either path cannot be inspected at
  all, in which case the comparison is skipped rather than failed);
- It does not catch up with writes that follow the source;
- It does not migrate a v2 database as if it were a v1 source;
- It does not run compact or repair.

### Common migration errors

| Error class | Meaning |
| --- | --- |
| `arg` | Wrong argument form, a `--to` version that is not the current one, a destination name that cannot name a file, or a destination that is the source. |
| `io` | A file system operation failed: stat, open, fd copy, staging I/O, permissions, rename, sync, a source or destination that is not a regular file, a destination that exists as a directory, or no free staging name after repeated collisions. A directory destination is reported under `target`, not as `arg`. |
| `lock-busy` | The source is being held exclusively by another process. |
| `source-corrupt` | The source's discovery record, graph structure or ownership check failed. |
| `source-version-unsupported` | The source version is not one this build's migration reader supports. |
| `source-version-mixed` | The two metadata slots declare different versions. |
| `source-already-current` | The source is already in the current format, so there is nothing to migrate. |
| `target-invalid` | The staging file cannot be read back as the current format, so the comparison stops. |
| `verify-mismatch` | The staging file and the source hold different logical data. |
| `published-durability-unknown` | Renamed, but the directory's durability is unconfirmed; the target is complete. |

On Windows a directory handle can be opened but refuses `FlushFileBuffers`, so a refusal of that
one call is treated as "cannot be confirmed" rather than as a failure - the same tolerance the
store writer applies (`src/store.rs`, `sync_dir`). A published target is therefore complete on
every platform, but its directory entry is confirmed durable only where a directory can be
flushed. `published-durability-unknown` is still raised on Windows when the directory cannot be
opened at all, or when the flush fails with any other error. Treat a Windows target as durable only
after the filesystem has successfully flushed it; an interrupted reboot does not establish durability.

## `check`

### Usage

```bash
btree-store check <file> [--summary] [--scan]
```

Example:

```bash
btree-store check current.db
```

The two flags choose what a successful run prints; neither changes what is
checked, and all four spellings run the same full check over the same selected
generation:

- `--summary` prints the fixed space summary instead of the plain report;
- `--scan` keeps the plain report and adds per-bucket statistics, marking them
  with `bucket_scan=true`.

Every flag combination is accepted together. On failure nothing changes: stdout
stays empty and no statistics are printed for a file that did not pass.

`check` inspects the current v2 format only. It does not open a live `BTree` instance, and it does not
create, repair, truncate or rewrite a database.

### File and concurrency boundaries

- The file must be an existing regular file; a directory, device, FIFO and so on are refused with the
  `io` class (the detail is `not a regular file: <path>`);
- The checker opens the file read-only;
- The checker holds a shared lock; a read-write process that follows this project's lock protocol and
  holds an exclusive lock gets `lock-busy`. The lock is retried for up to one second before it is
  declared busy, so a contended check is not an instant failure;
- The check covers the selected published generation;
- At the end the checker re-reads the file identity and length; a file that changed during the check
  returns an error;
- A live database that another process is writing is not a supported subject for a stable check;
- If an external program writes the file directly, bypassing the lock, the static-file assumption does
  not hold.

## What `check` inspects

### 1. Metadata and generation selection

The checker reads both 4096-byte metadata slots and records, for each:

- magic;
- format version;
- generation;
- catalog root;
- `next_page_id`;
- reusable root;
- retired root;
- metadata checksum.

Metadata is verified through the frozen 40-byte discovery record: the checksum covers `[0, 36)`, the
fields sit in `[36, 40)`, and the record binds no physical PID.

Among the valid candidates of the current version the highest generation wins; a tie keeps slot A. When
one slot is torn, has an invalid checksum or is truncated, the other complete valid slot still allows
the check to continue, and the report records the ignored candidate.

These cases fail before a report is produced (the class segment of stderr is the value in
parentheses):

- Neither metadata slot is usable: `source-corrupt` (`neither metadata slot is valid`);
- Only an unsupported version: `source-version-unsupported` (`unsupported format version <version>`);
- The slots do not reduce to one supported version: `source-version-mixed` (`metadata slots disagree
  on format version <version>`). The checker collects the *distinct* unsupported versions the slots
  declare; the class is mixed when there is more than one of them, or when any slot declares the
  current version. Two slots both declaring the same unsupported version are therefore not mixed, and
  `<version>` is the lowest unsupported version present;

### 2. File length and the data id space

The data id space of the current generation is:

```text
[2, next_page_id)
```

The checker requires a complete physical page for every PID in it:

- A whole page missing: `MISSING_PHYSICAL_PAGE`;
- A page present only in part: `TRUNCATED_PAGE`;
- Data outside `[2, next_page_id)` is informational `trailing_bytes` only and takes no part in the
  current generation's ownership conclusion.

A metadata slot itself may hold a truncated candidate, as long as another complete metadata slot is
valid and selected.

### 3. Catalog and bucket metadata

The checker starts at the selected catalog root and inspects:

- Whether a bucket name is non-empty valid UTF-8;
- Whether catalog keys strictly increase and whether any repeat;
- Whether bucket metadata is a fixed-width inline value;
- Whether a metadata root lies in the valid data id space;
- Whether the prefix-encoding policy flag uses defined bits only;
- Whether the layout matches the bucket tree's actual node layout.

### 4. Plain and prefix-encoded trees

The checker walks the catalog and every bucket tree structurally, inspecting:

- Plain and prefix-encoded page kinds;
- A header's element count, offsets and capacity bounds;
- The key/value ranges of slots;
- That a leaf key is non-empty and no longer than `MAX_KEY_LEN`;
- The canonical form of branch sentinels and separators;
- The full key length of a branch separator;
- Key/separator order within one node;
- Global key order in a DFS;
- Equal leaf depth across one tree;
- Branch routing intervals and child-selection semantics;
- Node PID ranges (`[2, next_page_id)`; 0 and 1 are metadata slots), duplicate references and duplicate
  owners;
- That a published branch has at least one child, and that every branch slot's child PID is non-zero.

The checker never hands a damaged node to the live `Node` decoder; a structural error becomes a
locatable diagnostic.

### 5. Values, overflow and indirect chains

For every leaf value the checker inspects:

- That `vlen` does not exceed `MAX_VAL_LEN`;
- That every page ID is zero when `vlen == 0`;
- That an inline value meets the current layout's canonical inline threshold;
- That every page ID of an inline value is zero;
- The page count, PID uniqueness and unused PIDs of a direct overflow;
- The next pointer, entry count, entry PIDs and termination of an indirect chain;
- That an indirect chain does not form a cycle;
- The PID, checksum and page bounds of a value page;
- That the unused logical bytes of the last value page are zero.

Value pages and indirect pages carry no page-kind header of their own; the checker interprets a page by
its role in the reference chain it has already verified.

### 6. Allocator extents and list pages

The checker inspects the reusable and retired extent chains independently:

- List page PID ranges and checksums;
- That a list chain has no cycle;
- That a header count does not exceed the page capacity;
- That an entry is non-zero and lies within the data id space;
- That extents are sorted by PID and do not overlap;
- That adjacent extents are reported as a warning rather than an error: the runtime can open such a
  file and merges them when it reads;
- That an extent covers no allocator list page;
- That reusable and retired do not overlap;
- That the reusable list and the retired list do not overlap.

Free space covered by an extent is treated as an ownership range and is never interpreted as a node, a
value or metadata.

### 7. Ownership

For every physical page in `[2, next_page_id)` the checker builds a compact ownership state and
confirms that it belongs to exactly one category:

```text
reachable node
reachable value
reachable indirect
reusable extent
retired extent
reusable list page
retired list page
```

These cases produce diagnostics:

- A reachable page overlapping an allocator class;
- Reusable overlapping retired;
- An extent covering a list page;
- A duplicate node/value/indirect reference;
- A page with no owner at all;
- An owner conflict across classes.

`reachable_pages`, `reusable_pages`, `retired_pages` and `allocator_list_pages` in the report count the
selected generation only; they are not accumulated over every generation in the file's history.

## `check` report and status

`check` writes a summary to stdout only when the check passes (the first line is `status=ok`). On
failure stdout is empty and the first stderr line is `error: check: <class>: <n> diagnostic(s)`, where
`<class>` is `failed` or `incomplete`.

As soon as one error diagnostic is recorded, the ownership completeness check no longer runs: the
ownership conclusion is incomplete anyway, as it is for both `failed` and `incomplete`. Warnings do not
affect the status.

### `status=ok`

It means:

- No error was found;
- Every required traversal completed;
- The data id space is physically complete;
- Ownership is complete;
- Warnings in the report do not mean the check failed.

With no flag, stdout carries the plain report, for example:

```text
status=ok
file=current.db
file_len=65536
generation=7
next_page_id=16
reachable_pages=9
reusable_pages=2
retired_pages=1
allocator_list_pages=2
trailing_bytes=0
```

`file=` is the checked path as `Path::display()` renders it. That rendering is
lossy for a name that is not valid UTF-8, which is why the encoding rule below
covers `bucket.name` and not this line.

`--summary` prints this fixed set of keys, in this order:

```text
status=ok
file=current.db
source_version=2
generation=7
file_len=65536
next_page_id=16
data_pages=14
reusable_pages=2
reusable_bytes=8192
reusable_extents=1
retired_pages=1
retired_bytes=4096
retired_extents=1
allocator_list_pages=2
reusable_if_all_retired_promoted_bytes=12288
reachable_pages=9
active_data_pages=11
trailing_bytes=0
cost=full-check
```

- `source_version` is this build's format version, not a version inferred from
  the data: `check` only accepts the current one, and any other version is
  refused before a report exists;
- `data_pages` is `next_page_id - 2`: the selected generation's data id space
  excludes the two meta slots. `active_data_pages` is `reachable_pages +
  allocator_list_pages`. For a passing check, `active_data_pages + reusable_pages
  + retired_pages = data_pages`; the checker already verifies this page by page.
  `retired_pages` remain protected, and `trailing_bytes` lie outside this id
  space;
- `reusable_bytes` is `reusable_pages * 4096` - extent pages a writer may
  allocate now;
- `retired_bytes` is `retired_pages * 4096` - extent pages that are retired and
  not allocatable yet: a reader may still hold them, or they were deferred to a
  later generation, so they are **not** space a write can use today;
- `reusable_if_all_retired_promoted_bytes` is exactly the sum of the two. It is
  what the total would become once every retired extent is promoted; it is no
  promise about the next write, and it deliberately leaves out the
  allocator-list pages that hold the extents, which are reported by
  `allocator_list_pages`;
- `reusable_extents` and `retired_extents` are the number of extent entries in
  the two chains as persisted, not the number of pages. Adjacent extents are
  merged by the runtime on the next open;
- `cost=full-check` states that this run walked the whole file, which every
  spelling costs. Only the spellings that carry statistics print the key; the
  bare spelling is frozen to its own key set above.

A `--summary` run without `--scan` prints this key set and nothing else, apart
from any `diagnostic:` warning lines it collected, which follow the set.

`--scan` prints the plain report, then `source_version`, `data_pages`,
`active_data_pages`, `cost=full-check` and `bucket_scan=true`, then one
seven-line block per bucket in catalog order, then any warning lines.
`--summary --scan` prints the summary set, then `bucket_scan=true` and the same
blocks. One blank line separates the header from the first bucket and each
pair of bucket blocks; parsers skip blank lines. A bucket block is:

```text
bucket.name=<percent-encoded-name>
bucket.prefix_encoding=true
bucket.records=100
bucket.logical_key_bytes=1200
bucket.logical_value_bytes=6400
bucket.tree_height=2
bucket.reachable_pages=5
```

- `bucket.name` is the catalog key, percent-encoded byte by byte: only `A-Z`,
  `a-z`, `0-9`, `.`, `_`, `-` and `~` stay as they are, and every other byte -
  including `%`, `=`, newline, carriage return and all ASCII control
  characters - becomes an uppercase `%HH`. A reader splits a line at its first
  `=`, so a name can never end its own line or invent a key;
- `bucket.prefix_encoding` is the bucket's prefix-encoding policy bit;
- `bucket.records` counts the leaf entries, `bucket.logical_key_bytes` and
  `bucket.logical_value_bytes` sum their key and value lengths, and
  `bucket.tree_height` counts the levels from the root to a leaf. A bucket with
  no root page reports zeros for all of them;
- `bucket.reachable_pages` is the node, value and indirect pages that bucket's
  own walk touched. It is a physical count: it is never inferred from the
  logical byte counts, and it never includes catalog or allocator pages.

`--scan` and `compact --info` read the same file for the same question, so
their totals agree: the sum of `bucket.records` equals the `records` of
`compact --info`, and the sum of `bucket.logical_key_bytes +
bucket.logical_value_bytes` equals its `logical_bytes`. Those two tokens are on
`compact --info`'s stderr `estimate` line, not on its stdout, which is empty for
an `--info` run. Neither number is a promise about how much a rebuild would
reclaim.

### `failed`

It means a definite format violation was found:

- A page checksum mismatch (node, allocator list, indirect, value);
- An out-of-range PID: node, value, indirect and allocator list references must all lie in
  `[2, next_page_id)`; 0 and 1 are metadata slots, not data pages;
- A missing or truncated page;
- A violation of a metadata root, `next_page_id` or the data id space;
- A structural violation in the catalog, a tree, a value/indirect chain, an allocator extent or
  ownership.

### `incomplete`

It means a generation was selected but reading a referenced page failed, so this traversal stopped
early; the diagnostics gathered so far are kept. The checks that did not run after the stop are: the
allocator check and **every bucket tree** when the catalog cannot be walked, and the ownership
completeness check in every case. The `code` of such a diagnostic starts with `IO_`, and today there is
only `IO_READ` (reading a physically present page failed, for example on a device error).

`incomplete` is not success: the CLI returns exit code `1` and never treats it as `Ok`.

## `check` output and exit codes

### Success

- stdout: the summary and warning diagnostics;
- stderr: empty;
- Exit code: `0`.

### Failed or incomplete check

- stdout: empty;
- First stderr line: `error: check: failed: <n> diagnostic(s)` or `error: check: incomplete: <n>
  diagnostic(s)`;
- Later stderr lines: the `diagnostic:` details;
- Exit code: `1`.

### Setup errors

These include:

- `io`: the file does not exist, cannot be read, or is not a regular file (the detail is `not a regular
  file: <path>`);
- `lock-busy`;
- `source-corrupt`: neither metadata slot is usable;
- `source-version-unsupported`;
- `source-version-mixed`;
- The file changed during the check.

These errors also write nothing to stdout, and their exit code is `1`.

## Typical triage flow

```bash
btree-store check suspect.db
```

If stderr's class is `incomplete` or `failed`:

1. Note the `generation` in the report;
2. Read the `diagnostic:` lines below it; each carries `severity`, `code`, `check`, `generation`, `pid`, `path`, `expected` and `actual`, with `-` standing in for an absent value:
3. Do not run repair, clear pages or hand-write metadata on the original database;
4. Copy an independent replica first;
5. Continue offline repair or migration experiments on the replica.

```text
diagnostic: severity=<error|warning> code=<CODE> check=<rule text> generation=<seq> pid=<pid|-> path=<reference path|-> expected=<...|-> actual=<...|->
```

`path` is how the checker reached the page, for example `bucket%2Fplain%2Fnode%3D7%2Fchild%3D0` -
it is not a file system path. `expected` and `actual` are rendered as hexadecimal where the rule
compares values.

`severity`, `code`, `check`, `expected`, `generation` and `pid` are the crate's own and are
printed plain. Two fields carry file bytes and are percent-encoded - every byte outside
`[A-Za-z0-9._~-]` becomes `%XX`:

-   `path` embeds a catalog key;
-   `actual` holds the offending key bytes.

A key holding a newline, a space or `=` therefore cannot add a diagnostic line or a field to one;
the separator characters of this protocol, `%2F` and `%3D` among them, read back the same way a
bucket name on the `bucket.name=` lines does. `check` is a sentence and contains spaces, so a line
is read by its `key=` prefixes and never by splitting on whitespace.

`check` never repairs anything by itself. It does now report space and bucket
statistics for a file that passes; controlled repair remains a separate, later
capability.

## `compact`

### Usage

```bash
btree-store compact [rebuild|inplace] <source> [destination] [--info]
```

Rebuilds the logical content of every bucket of the source, from one frozen generation, into a new
file. The mode word is an optional leading positional argument; omitting it means `rebuild`, and
`inplace` is a reserved value that returns an `arg` error today (page relocation is not implemented).
Writing the mode word and omitting it are the same run in the `rebuild` form (`compact a b` ≡
`compact rebuild a b`).

- `<source>` is required; `<destination>` is required for a run that **writes** (there is no default
  destination name) and may be omitted with `--info`. Where `--info` is written does not matter (before
  the mode word, after it, or at the end), because whether a destination is needed is decided by the
  command itself - inside clap a mode subcommand cannot see an `--info` written before the mode word;
- The destination **uses replace semantics**: whether or not it already exists (a regular file, a
  leftover from an earlier run, a symlink) it is replaced by the new file, and the only thing that may
  not be the destination is **the source itself**, with no switch to bypass that. The published file
  inherits the source's permission bits narrowed to owner read/write, so a group-readable database is
  rebuilt owner-only;
- `--info` cares about `<source>` only: it walks the audit and the traversal, prints the estimate to
  stderr and exits 0 without creating anything. It neither judges the destination against the source
  nor checks the destination's spelling - a `<destination>` it was given is treated as if it were not
  there;
- There is no option other than `--info`.

### Behavior

0. Before **reading the source**, a run that writes judges three things that have nothing to do with
   content: that the destination's spelling can name a file, that the destination is not the source
   itself (by inode; on platforms without inodes, by resolved path), and that the destination can be
   replaced by a file (its parent is a directory, and the name is not a directory). All three look at
   names and the file system only, never at what the destination holds - a stale database, a leftover
   and a symlink are all replaced. A name that currently exists as a directory is refused. `--info`
   judges none of the three - it reads the source only;
   The third of those is re-checked when the staging file is created, because the read takes time and
   the file system may have changed underneath the run; a destination that has become a directory in
   the meantime is refused then, after the audit and after the estimate has been printed;
1. The audit and the traversal run inside one **shared lock** on the source (`check_path` after
   `open_read_only`, before the traversal);
2. The estimate line is printed to stderr before any target file is created;
3. The rebuild commits in batches into the staging file, with the same bucket prefix-encoding policy as
   the source;
4. Two gates precede publication: the staging file must pass `check_path`, and it must compare
   **record for record** against the source (bucket set, order, policy, every key and value byte). The
   source's own identity is re-checked after the audit, after the traversal, and once more after both
   gates, so a source replaced or moved mid-run aborts rather than being rebuilt from;
5. Only after both gates pass is it published: one `rename` replaces the target (a single atomic
   replacement within one file system), so the target is either its old self or a complete file that
   passed both gates, and a half-written one is never observable. Failure classes and staging cleanup
   follow the existing rules of the `staging` module.

### Summary (stdout)

```text
mode=rebuild
source=...
source_version=2
source_generation=9
source_selected_slot=A
source_bytes=27447296
target=...
target_version=2
target_generation=7
buckets=3
records=700
logical_bytes=140900
batches=2
physical_bytes=364544
estimated_bytes=37000..5742728
reclaimed_bytes=27082752
verified=true
```

`reclaimed_bytes` is a saturating subtraction: it is 0 when the target is not smaller than the source,
and it never wraps around.

`estimated_bytes` is a **cost estimate range**, not a bracket the produced file must land in. Both ends
are the two metadata slots plus a node-page range plus the value and indirect pages plus a catalog
term that charges every bucket name its bytes: the low end counts the node pages the stored bytes
actually fill, the high end charges two pages per record. The node page count is a range because page
filling is the writer's decision; on buckets that store whole keys the value and indirect page counts
are exact, and on prefix-encoded buckets they are upper bounds; node content bytes are an upper bound
on prefix buckets as well (the writer stores the key tail beyond the node's prefix, and the read path
does not expose the stored form). The allocator list pages a rebuild allocates are not counted. The
measured `physical_bytes` is printed next to it for comparison, and no deviation gate is applied.

The command does not promise to be smaller: when it is not, `reclaimed_bytes` is 0.

### Failures and exit codes

- The source did not pass the audit → `source-corrupt`, or `failed` / `incomplete` from the audit
  itself; see the class table under `## migrate` above
- The source is held exclusively → `lock-busy`
- The source declares a format version this build does not read → `source-version-unsupported`,
  refused while opening, before the audit runs, and the detail names `btree-store migrate` as the way
  forward. A source whose two slots disagree on the version → `source-version-mixed`;
- The source is not a regular file, cannot be opened or changed during the run → `io`;
- The source's path changed during the audit, the traversal, or the rebuild → `io`, refusing to
  rebuild from a file that is no longer the one that was audited;
- A run that writes was given no `<destination>` → `arg` (exit 2), before the audit, and the message
  names `--info` as the way out
- The destination's spelling cannot name a file, the destination is the source itself, or the
  destination cannot be replaced by a file → `arg` / `io`, before the source is read; the last of
  those is repeated when the staging file is created, so a destination that has become a directory in
  the meantime is refused under `target` at that point
- The staging file did not pass the audit → `target` step's `target-invalid` (the product is bad, not
  the source)
- The logical comparison disagrees → `verify-mismatch`, and **nothing is published**
- An existing destination is not an error: it is replaced (both commands use replace semantics);
- Except for `published-durability-unknown`, no failure changes the target file (the replacement
  happens after the gates), and staging is cleaned up according to the existing classes of
  `removes_staging_after`; `published-cleanup-incomplete` was retired with the link protocol (the
  replacement consumes the staging name in one step, so there is no second cleanup that could fail)

### Platform limits

- **Windows directory durability.** Both commands publish through the same `publish`, so this
  limit is shared. When the directory handle opens but refuses `FlushFileBuffers` - the common
  Windows case - that single refusal is tolerated and the sync is skipped, so the class is not
  raised for it. It is still raised when the directory cannot be opened at all, or when the flush
  fails with any other error. The published target is complete either way; its directory entry is
  simply not confirmed durable on Windows.
