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
- txn_del

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
void btree_close(BTree *db);
```
- db: handle returned by btree_open, safe to pass NULL

```
int btree_exec(BTree *db, const char *bucket, int (*fn)(Txn *txn, void *ctx), void *ctx);
```
- db: database handle
- bucket: utf-8 bucket name
- fn: callback invoked within a write transaction
- ctx: user context pointer passed to fn
- returns: callback rc on non-zero, or error code on internal failure
- note: callback must not call exec/view

```
int btree_view(BTree *db, const char *bucket, int (*fn)(Txn *txn, void *ctx), void *ctx);
```
- db: database handle
- bucket: utf-8 bucket name
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
- bucket: utf-8 bucket name
- out: output txn handle for the bucket
- returns: 0 on success, non-zero on error
- note: out handle valid only during the callback

```
int txn_get(Txn *txn, const uint8_t *key, size_t klen, uint8_t **out, size_t *out_len);
```
- txn: transaction handle
- key/klen: key bytes
- out/out_len: output buffer and length, valid on success
- returns: 0 on success, non-zero on error
- note: caller must free out with btree_free

```
int txn_put(Txn *txn, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen);
```
- txn: transaction handle
- key/klen: key bytes
- val/vlen: value bytes
- returns: 0 on success, non-zero on error
- note: invalid in read-only view

```
int txn_del(Txn *txn, const uint8_t *key, size_t klen);
```
- txn: transaction handle
- key/klen: key bytes
- returns: 0 on success, non-zero on error
- note: invalid in read-only view

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
- `Txn` and `MultiTxn` handles are valid only during the callback
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
