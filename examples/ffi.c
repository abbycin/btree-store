// build library
// linux or macos: RUSTFLAGS="-C panic=abort" cargo build --features ffi --release
// windows powershell: $env:RUSTFLAGS="-C panic=abort"; cargo build --features ffi --release
//
// build example
// linux: cc -I./include examples/ffi.c -L./target/release -lbtree_store -o ffi
// macos: cc -I./include examples/ffi.c -L./target/release -lbtree_store -o ffi
// windows msvc: cl /I include examples\\ffi.c /Fe:ffi.exe /link /LIBPATH:target\\release btree_store.dll.lib
//
// run
// linux: LD_LIBRARY_PATH=./target/release ./ffi
// macos: DYLD_LIBRARY_PATH=./target/release ./ffi
// windows: .\\ffi.exe

#include "btree_store.h"
#include <stdio.h>
#include <string.h>

static void print_last_error(const char *label) {
    const char *msg = NULL;
    size_t len = 0;
    int code = btree_last_error(&msg, &len);
    if (code == 0) {
        return;
    }
    if (msg && len > 0) {
        fprintf(stderr, "%s: %.*s (%d)\n", label, (int)len, msg, code);
    } else {
        fprintf(stderr, "%s: error %d\n", label, code);
    }
}

static int exec_put(Txn *txn, void *ctx) {
    (void)ctx;
    const char *key = "hello";
    const char *val = "world";
    return txn_put(txn, (const uint8_t *)key, strlen(key), (const uint8_t *)val, strlen(val));
}

static int view_get(Txn *txn, void *ctx) {
    (void)ctx;
    uint8_t *out = NULL;
    size_t out_len = 0;
    int rc = txn_get(txn, (const uint8_t *)"hello", 5, &out, &out_len);
    if (rc != 0) {
        return rc;
    }
    if (out && out_len > 0) {
        printf("value: %.*s\n", (int)out_len, out);
        fflush(stdout);
        btree_free(out, out_len);
    }
    return 0;
}

static int exec_update(Txn *txn, void *ctx) {
    (void)ctx;
    int updated = 0;
    int rc = txn_update(
        txn,
        (const uint8_t *)"hello",
        5,
        (const uint8_t *)"rust",
        4,
        &updated
    );
    if (rc != 0) {
        return rc;
    }
    if (!updated) {
        fprintf(stderr, "txn_update did not update an existing key\n");
        return BTREE_ERR_INTERNAL;
    }
    return 0;
}

static int exec_del(Txn *txn, void *ctx) {
    (void)ctx;
    return txn_del(txn, (const uint8_t *)"hello", 5);
}

static int view_missing(Txn *txn, void *ctx) {
    (void)ctx;
    uint8_t *out = NULL;
    size_t out_len = 0;
    int rc = txn_get(txn, (const uint8_t *)"hello", 5, &out, &out_len);
    if (rc == BTREE_ERR_NOT_FOUND) {
        btree_last_error_clear();
        puts("confirmed missing key");
        return 0;
    }
    if (rc != 0) {
        return rc;
    }
    if (out && out_len > 0) {
        printf("unexpected value: %.*s\n", (int)out_len, out);
        btree_free(out, out_len);
    }
    return 0;
}

static int multi_ops(MultiTxn *mtxn, void *ctx) {
    (void)ctx;
    Txn *t1 = NULL;
    Txn *t2 = NULL;
    int rc = mtxn_bucket(mtxn, "bucket1", &t1);
    if (rc != 0) {
        return rc;
    }
    rc = txn_put(t1, (const uint8_t *)"k1", 2, (const uint8_t *)"v1", 2);
    if (rc != 0) {
        return rc;
    }
    rc = mtxn_bucket(mtxn, "bucket2", &t2);
    if (rc != 0) {
        return rc;
    }
    return txn_put(t2, (const uint8_t *)"k2", 2, (const uint8_t *)"v2", 2);
}

static int print_iter_item(
    const uint8_t *key,
    size_t klen,
    const uint8_t *val,
    size_t vlen,
    void *ctx
) {
    (void)ctx;
    printf("iter: %.*s=%.*s\n", (int)klen, key, (int)vlen, val);
    return 0;
}

static int view_iter_uncached(Txn *txn, void *ctx) {
    (void)ctx;
    BTreeIter *iter = NULL;
    int rc = txn_iter_uncached(txn, &iter);
    if (rc != 0) {
        return rc;
    }
    while ((rc = btree_iter_next(iter, print_iter_item, NULL)) == 0) {
    }
    btree_iter_close(iter);
    return rc == 1 ? 0 : rc;
}

int main(void) {
    BTree *db = NULL;
    BTreeOpenOptions options;
    int rc = btree_default_open_options(&options);
    if (rc != 0) {
        print_last_error("btree_default_open_options");
        return 1;
    }
    options.bucket_tree_cache_capacity = 1024;
    options.sync_mode = BTREE_SYNC_ADAPTIVE;

    rc = btree_open_with_options("ffi_example.db", &options, &db);
    if (rc != 0) {
        print_last_error("btree_open_with_options");
        return 1;
    }

    rc = btree_exec(db, "bucket1", exec_put, NULL);
    if (rc != 0) {
        print_last_error("btree_exec");
        btree_close(db);
        return 1;
    }

    rc = btree_view(db, "bucket1", view_get, NULL);
    if (rc != 0) {
        print_last_error("btree_view");
        btree_close(db);
        return 1;
    }

    rc = btree_exec(db, "bucket1", exec_update, NULL);
    if (rc != 0) {
        print_last_error("btree_exec update");
        btree_close(db);
        return 1;
    }

    rc = btree_view(db, "bucket1", view_get, NULL);
    if (rc != 0) {
        print_last_error("btree_view after update");
        btree_close(db);
        return 1;
    }

    rc = btree_view(db, "bucket1", view_iter_uncached, NULL);
    if (rc != 0) {
        print_last_error("btree_view uncached iter");
        btree_close(db);
        return 1;
    }

    rc = btree_exec_multi(db, multi_ops, NULL);
    if (rc != 0) {
        print_last_error("btree_exec_multi");
        btree_close(db);
        return 1;
    }

    rc = btree_exec(db, "bucket1", exec_del, NULL);
    if (rc != 0) {
        print_last_error("btree_exec delete");
        btree_close(db);
        return 1;
    }

    rc = btree_view(db, "bucket1", view_missing, NULL);
    if (rc != 0) {
        print_last_error("btree_view missing");
        btree_close(db);
        return 1;
    }

    btree_close(db);
    return 0;
}
