#ifndef BTREE_STORE_H
#define BTREE_STORE_H

#include <stddef.h>
#include <stdint.h>

// callback must not call exec/view
// txn handles are valid only during callback
// longjmp across ffi is unsafe
// panics abort the process

#ifdef __cplusplus
extern "C" {
#endif

typedef struct BTree BTree;
typedef struct Txn Txn;
typedef struct MultiTxn MultiTxn;
typedef struct BTreeIter BTreeIter;

typedef struct BTreeOpenOptions {
    size_t node_cache_capacity;
    size_t lid_pid_cache_capacity;
    size_t lid_pid_hot_cache_capacity;
    size_t bucket_root_cache_capacity;
    size_t bucket_tree_cache_capacity;
    uint32_t sync_mode;
} BTreeOpenOptions;

#define BTREE_OK 0
#define BTREE_ERR_NOT_FOUND -1
#define BTREE_ERR_CORRUPTION -2
#define BTREE_ERR_TOO_LARGE -3
#define BTREE_ERR_INTERNAL -4
#define BTREE_ERR_NO_SPACE -5
#define BTREE_ERR_IO -6
#define BTREE_ERR_INVALID -7
#define BTREE_ERR_DUPLICATE -8
#define BTREE_ERR_CONFLICT -9
#define BTREE_ERR_UNKNOWN -1000

#define BTREE_SYNC_ADAPTIVE 0
#define BTREE_SYNC_DATA 1
#define BTREE_SYNC_ALL 2

int btree_open(const char *path, BTree **out);
int btree_default_open_options(BTreeOpenOptions *out);
int btree_open_with_options(const char *path, const BTreeOpenOptions *options, BTree **out);
size_t btree_max_key_len(void);
void btree_close(BTree *db);

int btree_exec(BTree *db, const char *bucket, int (*fn)(Txn *txn, void *ctx), void *ctx);
int btree_view(BTree *db, const char *bucket, int (*fn)(Txn *txn, void *ctx), void *ctx);
int btree_exec_multi(BTree *db, int (*fn)(MultiTxn *mtxn, void *ctx), void *ctx);
int mtxn_bucket(MultiTxn *mtxn, const char *bucket, Txn **out);

int txn_get(Txn *txn, const uint8_t *key, size_t klen, uint8_t **out, size_t *out_len);
int txn_put(Txn *txn, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen);
int txn_update(Txn *txn, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen, int *updated);
int txn_del(Txn *txn, const uint8_t *key, size_t klen);
int txn_iter(Txn *txn, BTreeIter **out);
int txn_iter_uncached(Txn *txn, BTreeIter **out);
int btree_iter_next(BTreeIter *iter, int (*fn)(const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen, void *ctx), void *ctx);
void btree_iter_close(BTreeIter *iter);

void btree_free(void *p, size_t len);
// last_error message pointer is valid until next ffi call on the same thread or btree_last_error_clear
int btree_last_error(const char **msg, size_t *len);
void btree_last_error_clear(void);

#ifdef __cplusplus
} // extern "C"
#endif

#endif
