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

int btree_open(const char *path, BTree **out);
void btree_close(BTree *db);

int btree_exec(BTree *db, const char *bucket, int (*fn)(Txn *txn, void *ctx), void *ctx);
int btree_view(BTree *db, const char *bucket, int (*fn)(Txn *txn, void *ctx), void *ctx);
int btree_exec_multi(BTree *db, int (*fn)(MultiTxn *mtxn, void *ctx), void *ctx);
int mtxn_bucket(MultiTxn *mtxn, const char *bucket, Txn **out);

int txn_get(Txn *txn, const uint8_t *key, size_t klen, uint8_t **out, size_t *out_len);
int txn_put(Txn *txn, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen);
int txn_del(Txn *txn, const uint8_t *key, size_t klen);

void btree_free(void *p, size_t len);
// last_error message pointer is valid until next ffi call on the same thread or btree_last_error_clear
int btree_last_error(const char **msg, size_t *len);
void btree_last_error_clear(void);

#ifdef __cplusplus
} // extern "C"
#endif

#endif
