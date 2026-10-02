# Migrating btree-store databases and code

This document covers every published version bump that a database file or a Rust program has to
survive. A bump only appears here when it changes the on-disk format or the public API; a release
that is compatible with the files and the code of the version before it is not a migration.

Read [the design document](design.md) for what the format and the API actually are, and
[the CLI reference](cli.md) for the full command surface.

## Which migrations exist

| From | To | Written by | Command |
| --- | --- | --- | --- |
| format 1 | format 2 | `btree-store` 1.x | `btree-store migrate <source> --output <destination> --to 2` |

`migrate` reads only the versions listed here. A file that is already in the current format is
refused rather than rewritten.

---

## 1.x to 2.0

`btree-store` 1.x wrote **format version 1**. Version 2.0.0 reads and writes **format version 2**.

### What changed on disk

Every page carries one CRC32C checksum, stored in the node or allocator header or in the trailer of
a value or indirect page. It is computed over the whole 4096-byte page with its own field read as
zero, so bytes a writer never wrote are covered too, and it is bound to the physical page id the
caller expects. The practical consequences:

- a value page carries 4092 value bytes instead of 4096;
- a page written to the wrong offset, two pages swapped inside a run, or a reader asking for the
  wrong id now fail verification instead of being read under the wrong identity;
- no reader needs to know how many bytes a page used in order to verify it.

The 40-byte metadata discovery record is unchanged and keeps its meaning, which is what lets the
migration name a source's version before it interprets anything else.

### The 2.0 runtime refuses a format-1 file

It does not read one and it does not guess. Opening it fails:

```text
OpenError::Corruption(CorruptionReport {
    code: "UNSUPPORTED_FORMAT_VERSION",
    expected: Some("2"),
    actual: Some("1"),
    ..
})
```

### What changed in the Rust API

Three source-level changes need a code edit.

| Change | What to do |
| --- | --- |
| `Txn::iter_uncached` and `ReadOnlyTxn::iter_uncached` are removed | call `iter()`; every iterator uses the shared node cache |
| `OpenOptions` gained a `read_only: bool` field | add it to any struct literal; `OpenOptions::new()` and `OpenOptions::default()` are unchanged and set it to `false` |
| `Error::ReadOnly` and `OpenError::ReadOnly` are new variants | add an arm; neither enum is `#[non_exhaustive]`, so an exhaustive `match` stops compiling |

2.0 also adds read-only opens, whole-file snapshots, an offline integrity checker and an offline
compact command. They are new surface, not changes to the rules above.

### Procedure

**1. Get the 2.0 CLI.**

```bash
cargo install btree-store --version 2.0.0
```

or, from a checkout of the repository:

```bash
cargo build --release --bin btree-store
```

**2. Stop the application.**

`migrate` takes a shared lock on the source. A running read-write instance holds an exclusive lock,
so the migration fails with `lock-busy` while it is held. The migration also cannot see writes that
follow the source, so a source that is still being written would be rebuilt from an inconsistent
point in time.

**3. Rebuild the file.**

```bash
btree-store migrate old.db --output new.db --to 2
```

Migration is offline and non-destructive:

1. the source is opened read-only and its discovery record confirms the version;
2. the source's metadata, catalog, bucket trees, value and indirect chains, and allocator ownership
   are checked in full **before any output file exists**, so an unusable reference is caught up front
   rather than when its page is read;
3. the database is rebuilt into a private staging file with the current writer;
4. the staging file is reopened with a fresh instance and compared to the source bucket by bucket,
   key by key, value by value;
5. only then is the staging file synced and renamed onto `--output`, replacing whatever held that
   name, and the destination directory is synced.

The source is never modified, truncated or replaced, and is never itself the destination. On failure
the destination name is left as it was, with one exception: if the rename succeeded but the directory
sync did not, the run reports `published-durability-unknown` and the new file *is* in place, merely
not known to survive a power loss. On Windows a refusal of `FlushFileBuffers` on the directory is
tolerated and that sync is skipped, so the class is not raised for that one case; it is still raised
when the directory cannot be opened at all or the flush fails with any other error. The staging file
is removed on every other failure. Give the destination a **new** name rather than the source's, so
the backup is unambiguous.

**4. Verify before you switch over.**

```bash
btree-store check new.db --summary
```

A passing run exits `0` and reports `status=ok`. The check is read-only and does not modify the
file. See [the CLI reference](cli.md) for what `--summary` and `--scan` report.

**5. Point the application at `new.db` and keep `old.db`.**

The rebuilt file is an ordinary 2.0 database: there is no further migration, no compatibility mode
and no version pin to carry forward. Delete the backup only once the application has run against it.

### Rolling back

Rollback is a file swap, because the source is still intact: point the application back at `old.db`.
Doing so requires a 1.x build of the library, since 2.0 will not open the file.

### What migration does not do

- it does not upgrade the source in place;
- it does not use the source as the destination;
- it does not catch up with writes made after the source was read;
- it does not run `compact` or repair a damaged file — a source that fails its checks is refused.

The full command reference, including every error class, is in [the CLI reference](cli.md).