# btree-store C FFI Guide

## Overview
- callback-based C ABI wrapping `exec`/`view`
- single-writer/multi-reader semantics preserved
- ffi is gated behind the `ffi` feature

## Quick Start
1) build libraries
   - linux/macos:
     - RUSTFLAGS="-C panic=abort" cargo build --features ffi --release
   - windows powershell:
     - $env:RUSTFLAGS="-C panic=abort"; cargo build --features ffi --release
2) build and run example
   - see `examples/ffi.c`

## Build & Artifacts
- cdylib:
  - linux: target/release/libbtree_store.so
  - macos: target/release/libbtree_store.dylib
  - windows: target/release/btree_store.dll
- staticlib:
  - linux/macos: target/release/libbtree_store.a
  - windows: target/release/btree_store.lib

## Linking & Runtime
linux:
```
cc -I./include examples/ffi.c -L./target/release -lbtree_store -o ffi
LD_LIBRARY_PATH=./target/release ./ffi
```
macos:
```
cc -I./include examples/ffi.c -L./target/release -lbtree_store -o ffi
DYLD_LIBRARY_PATH=./target/release ./ffi
```
windows (msvc):
```
cl /I include examples\\ffi.c /Fe:ffi.exe /link /LIBPATH:target\\release btree_store.dll.lib
copy target\\release\\btree_store.dll .
ffi.exe
```

## API Overview
open/close:
- btree_open
- btree_default_open_options
- btree_open_with_options
- btree_max_key_len
- btree_close

transactions:
- btree_exec
- btree_view

multi bucket:
- btree_exec_multi
- mtxn_bucket

kv operations:
- txn_get
- txn_put
- txn_update
- txn_del

iteration:
- txn_iter
- txn_iter_uncached
- btree_iter_next
- btree_iter_close

memory:
- btree_free

errors:
- btree_last_error
- btree_last_error_clear

## API Signatures (with parameter notes)
```
int btree_open(const char *path, BTree **out);
```
- path: utf-8 file path
- out: output handle, set to non-null on success
- returns: 0 on success, non-zero on error

```
typedef struct BTreeOpenOptions {
    size_t node_cache_capacity;
    size_t lid_pid_cache_capacity;
    size_t lid_pid_hot_cache_capacity;
    size_t bucket_root_cache_capacity;
    size_t bucket_tree_cache_capacity;
    uint32_t sync_mode;
} BTreeOpenOptions;
```
- runtime-only open options matching the Rust `OpenOptions`
- `sync_mode` must be one of `BTREE_SYNC_ADAPTIVE`, `BTREE_SYNC_DATA`, or `BTREE_SYNC_ALL`

```
int btree_default_open_options(BTreeOpenOptions *out);
```
- out: receives the current default runtime options
- returns: 0 on success, non-zero on error

```
int btree_open_with_options(const char *path, const BTreeOpenOptions *options, BTree **out);
```
- path: utf-8 file path
- options: runtime-only open options
- out: output handle, set to non-null on success
- returns: 0 on success, non-zero on error
- note: same-path reopen within one process must use identical options

```
size_t btree_max_key_len(void);
```
- returns: maximum encoded byte length accepted for keys and bucket names
- note: the current limit is 128 bytes

```
void btree_close(BTree *db);
```
- db: handle returned by btree_open, safe to pass NULL

```
int btree_exec(BTree *db, const char *bucket, int (*fn)(Txn *txn, void *ctx), void *ctx);
```
- db: database handle
- bucket: utf-8 bucket name, encoded length must be 1..=btree_max_key_len() bytes
- fn: callback invoked within a write transaction
- ctx: user context pointer passed to fn
- returns: callback rc on non-zero, or error code on internal failure
- note: callback must not call exec/view

```
int btree_view(BTree *db, const char *bucket, int (*fn)(Txn *txn, void *ctx), void *ctx);
```
- db: database handle
- bucket: utf-8 bucket name, encoded length must be 1..=btree_max_key_len() bytes
- fn: callback invoked within a read transaction
- ctx: user context pointer passed to fn
- returns: callback rc on non-zero, or error code on internal failure
- note: callback must not call exec/view

```
int btree_exec_multi(BTree *db, int (*fn)(MultiTxn *mtxn, void *ctx), void *ctx);
```
- db: database handle
- fn: callback invoked within a multi-bucket transaction
- ctx: user context pointer passed to fn
- returns: callback rc on non-zero, or error code on internal failure

```
int mtxn_bucket(MultiTxn *mtxn, const char *bucket, Txn **out);
```
- mtxn: handle provided to btree_exec_multi callback
- bucket: utf-8 bucket name, encoded length must be 1..=btree_max_key_len() bytes
- out: output txn handle for the bucket
- returns: 0 on success, non-zero on error
- note: out handle valid only during the callback

```
int txn_get(Txn *txn, const uint8_t *key, size_t klen, uint8_t **out, size_t *out_len);
```
- txn: transaction handle
- key/klen: key bytes, length must be 1..=btree_max_key_len() bytes
- out/out_len: output buffer and length, valid on success
- returns: 0 on success, non-zero on error
- note: caller must free out with btree_free

```
int txn_put(Txn *txn, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen);
```
- txn: transaction handle
- key/klen: key bytes, length must be 1..=btree_max_key_len() bytes
- val/vlen: value bytes
- returns: 0 on success, non-zero on error
- note: invalid in read-only view

```
int txn_update(Txn *txn, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen, int *updated);
```
- txn: transaction handle
- key/klen: key bytes, length must be 1..=btree_max_key_len() bytes
- val/vlen: value bytes
- updated: output flag set to 1 when the key existed and was updated, 0 when the key was missing
- returns: 0 on success, non-zero on error
- note: missing key is reported via `updated=0`, not as an error; invalid in read-only view

```
int txn_del(Txn *txn, const uint8_t *key, size_t klen);
```
- txn: transaction handle
- key/klen: key bytes, length must be 1..=btree_max_key_len() bytes
- returns: 0 on success, non-zero on error
- note: invalid in read-only view

```
int txn_iter(Txn *txn, BTreeIter **out);
```
- txn: transaction handle
- out: iterator handle valid only during the surrounding callback
- returns: 0 on success, non-zero on error

```
int txn_iter_uncached(Txn *txn, BTreeIter **out);
```
- txn: transaction handle
- out: iterator handle valid only during the surrounding callback
- returns: 0 on success, non-zero on error
- note: bypasses leaf-node and overflow-value-page caching, but may still reuse cached branch paths

```
int btree_iter_next(BTreeIter *iter, int (*fn)(const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen, void *ctx), void *ctx);
```
- iter: iterator handle returned by `txn_iter` or `txn_iter_uncached`
- fn: callback invoked for the next key/value pair
- ctx: user context pointer passed to fn
- returns: `0` when an item was produced, `1` on end-of-iteration, non-zero error code on failure

```
void btree_iter_close(BTreeIter *iter);
```
- iter: iterator handle returned by `txn_iter` or `txn_iter_uncached`
- note: safe to pass NULL

```
void btree_free(void *p, size_t len);
```
- p/len: buffer returned by txn_get
- note: safe to pass NULL or len=0

```
int btree_last_error(const char **msg, size_t *len);
```
- msg/len: output message pointer and length
- returns: last error code, 0 if none
- note: message pointer valid until next ffi call on same thread or btree_last_error_clear

```
void btree_last_error_clear(void);
```
- clears last error for the current thread

## Error Codes
| code | name |
| --- | --- |
| 0 | BTREE_OK |
| -1 | BTREE_ERR_NOT_FOUND |
| -2 | BTREE_ERR_CORRUPTION |
| -3 | BTREE_ERR_TOO_LARGE |
| -4 | BTREE_ERR_INTERNAL |
| -5 | BTREE_ERR_NO_SPACE |
| -6 | BTREE_ERR_IO |
| -7 | BTREE_ERR_INVALID |
| -8 | BTREE_ERR_DUPLICATE |
| -9 | BTREE_ERR_CONFLICT |
| -1000 | BTREE_ERR_UNKNOWN |

## API Contract
- callback must not call `exec`/`view`
- keys and bucket names are invalid when empty or longer than `btree_max_key_len()` bytes
- `Txn` and `MultiTxn` handles are valid only during the callback
- `BTreeIter` handles are valid only during the callback that created them
- `longjmp` across ffi is unsafe
- panics are not caught; build with `panic=abort`

## Error Handling
- all ffi functions return an `int` error code
- on error, `btree_last_error` returns the last error code and message
- last_error message pointer is valid until the next ffi call on the same thread or `btree_last_error_clear`
- callback return codes are propagated and do not overwrite last_error

## Memory Management
- `txn_get` returns a rust-allocated buffer
- caller must free the buffer using `btree_free`

## Example
See `examples/ffi.c` for a complete example that exercises all public C APIs.
