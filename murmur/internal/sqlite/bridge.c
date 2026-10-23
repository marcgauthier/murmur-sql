// SQLite amalgamation build plus cgo trampolines.
//
// The amalgamation (third_party/sqlite) is compiled with the flags from
// sqlite.go's preamble: session capture, FTS5, column metadata.
//
// The trampolines below exist because cgo cannot pass C function pointers
// or use the SQLITE_TRANSIENT destructor macro directly.
#include "../../third_party/sqlite/sqlite3.c"

#include <stdint.h>

// Go callbacks, implemented in sqlite.go with //export.
extern int murmurGoAuthorizer(void *db, int action, const char *a1,
                              const char *a2, const char *a3, const char *a4);
extern void murmurGoFunc(void *ctx, long long handle, int argc, void *argv);
extern void murmurGoFuncDestroy(void *p);

static int murmurAuthorizerTramp(void *p, int code, const char *s1,
                                 const char *s2, const char *s3,
                                 const char *s4) {
    return murmurGoAuthorizer(p, code, s1, s2, s3, s4);
}

int murmurSetAuthorizer(sqlite3 *db, int enable) {
    return sqlite3_set_authorizer(db, enable ? murmurAuthorizerTramp : 0, db);
}

static void murmurFuncTramp(sqlite3_context *ctx, int argc,
                            sqlite3_value **argv) {
    long long handle =
        (long long)(intptr_t)sqlite3_user_data(ctx);
    murmurGoFunc((void *)ctx, handle, argc, (void *)argv);
}

static void murmurFuncDestroyTramp(void *p) { murmurGoFuncDestroy(p); }

int murmurCreateFunction(sqlite3 *db, const char *name, int nArg, int flags,
                         long long handle) {
    return sqlite3_create_function_v2(db, name, nArg, flags,
                                      (void *)(intptr_t)handle, murmurFuncTramp,
                                      0, 0, murmurFuncDestroyTramp);
}

int murmurBindText(sqlite3_stmt *stmt, int i, const char *s, int n) {
    return sqlite3_bind_text(stmt, i, s, n, SQLITE_TRANSIENT);
}

int murmurBindBlob(sqlite3_stmt *stmt, int i, const void *b, int n) {
    return sqlite3_bind_blob(stmt, i, b, n, SQLITE_TRANSIENT);
}

void murmurResultText(sqlite3_context *ctx, const char *s, int n) {
    sqlite3_result_text(ctx, s, n, SQLITE_TRANSIENT);
}

void murmurResultBlob(sqlite3_context *ctx, const void *b, int n) {
    sqlite3_result_blob(ctx, b, n, SQLITE_TRANSIENT);
}

void murmurResultError(sqlite3_context *ctx, const char *s, int n) {
    sqlite3_result_error(ctx, s, n);
}
