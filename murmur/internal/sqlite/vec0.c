// Brute-force vec0 virtual table module for murmur vector indexes.
//
// This is murmur's own minimal implementation of the sqlite-vec vec0
// interface, not the sqlite-vec project: one BLOB column of
// little-endian float32 values, KNN queries through
// "<column> MATCH '<query>' AND k = <n>", and a REAL "distance" column.
// Rows live in a per-index TEMP shadow table, so all writes stay fully
// transactional (savepoints and rollback propagate for free).
//
// Supported DDL (as emitted by search.go):
//   CREATE VIRTUAL TABLE t USING vec0(<column> float[<dims>] distance_metric=<l1|l2|cosine>)
#include "../../third_party/sqlite/sqlite3.h"

#include <ctype.h>
#include <stdint.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    sqlite3_vtab base;
    sqlite3 *db;
    char *shadow;   /* TEMP shadow table name, unquoted */
    char *column;   /* indexed column name, unquoted */
    int dims;
    int metric;     /* 0=l2 1=l1 2=cosine */
} vec0_vtab;

typedef struct {
    sqlite3_vtab_cursor base;
    int64_t *rowids;
    double *distances;
    unsigned char **vectors;
    int *lengths;
    int count;
    int pos;
} vec0_cursor;

static void vec0_free_results(vec0_cursor *c) {
    if (c->vectors) {
        for (int i = 0; i < c->count; i++) {
            sqlite3_free(c->vectors[i]);
        }
    }
    sqlite3_free(c->rowids);
    sqlite3_free(c->distances);
    sqlite3_free(c->vectors);
    sqlite3_free(c->lengths);
    c->rowids = 0;
    c->distances = 0;
    c->vectors = 0;
    c->lengths = 0;
    c->count = 0;
    c->pos = 0;
}

static float vec0_f32(const unsigned char *p) {
    float f;
    memcpy(&f, p, sizeof(f));
    return f;
}

static double vec0_distance(int metric, const float *q, const float *v, int dims) {
    if (metric == 1) {
        double sum = 0;
        for (int i = 0; i < dims; i++) {
            sum += fabs((double)q[i] - (double)v[i]);
        }
        return sum;
    }
    if (metric == 2) {
        double dot = 0, nq = 0, nv = 0;
        for (int i = 0; i < dims; i++) {
            dot += (double)q[i] * (double)v[i];
            nq += (double)q[i] * (double)q[i];
            nv += (double)v[i] * (double)v[i];
        }
        if (nq <= 0 || nv <= 0) {
            return 1.0;
        }
        return 1.0 - dot / (sqrt(nq) * sqrt(nv));
    }
    double sum = 0;
    for (int i = 0; i < dims; i++) {
        double d = (double)q[i] - (double)v[i];
        sum += d * d;
    }
    return sqrt(sum);
}

/* Parse "[1,0]" (brackets optional) into out; returns count or -1. */
static int vec0_parse_text(const char *s, int n, float *out, int dims) {
    int count = 0;
    int i = 0;
    while (i < n && isspace((unsigned char)s[i])) {
        i++;
    }
    if (i < n && (s[i] == '[' || s[i] == '(')) {
        i++;
    }
    while (i < n) {
        while (i < n && (isspace((unsigned char)s[i]) || s[i] == ',')) {
            i++;
        }
        if (i >= n || s[i] == ']' || s[i] == ')') {
            break;
        }
        char *end = 0;
        double v = strtod(s + i, &end);
        if (end == s + i) {
            return -1;
        }
        if (count >= dims) {
            return -1;
        }
        out[count++] = (float)v;
        i = (int)(end - s);
    }
    return count;
}

static int vec0_exec(sqlite3 *db, const char *sql) {
    char *err = 0;
    int rc = sqlite3_exec(db, sql, 0, 0, &err);
    sqlite3_free(err);
    return rc;
}

/* Parse module arguments. argv[0..2] are module/db/table; the rest form
 * one "<column> float[<dims>] distance_metric=<m>" definition. */
static int vec0_parse_args(vec0_vtab *v, int argc, const char *const *argv) {
    char *joined = sqlite3_mprintf("");
    for (int i = 3; i < argc; i++) {
        char *next = sqlite3_mprintf("%z %s", joined, argv[i]);
        if (!next) {
            sqlite3_free(joined);
            return SQLITE_NOMEM;
        }
        joined = next;
    }
    int dims = -1;
    int metric = 0;
    char *name = 0;
    size_t nameLen = 0;
    char *save = 0;
    /* strtok-style split without locale surprises. */
    char *tok = joined;
    while (*tok) {
        while (*tok && (*tok == ' ' || *tok == '\t' || *tok == '\n' || *tok == '\r' || *tok == ',')) {
            tok++;
        }
        if (!*tok) {
            break;
        }
        char *start = tok;
        while (*tok && *tok != ' ' && *tok != '\t' && *tok != '\n' && *tok != '\r' && *tok != ',') {
            tok++;
        }
        char saved = *tok;
        *tok = 0;
        if (strncmp(start, "float[", 6) == 0) {
            char *end = 0;
            long d = strtol(start + 6, &end, 10);
            if (end == start + 6 || *end != ']' || d < 1 || d > 8192) {
                sqlite3_free(joined);
                sqlite3_free(name);
                return SQLITE_ERROR;
            }
            dims = (int)d;
        } else if (strncmp(start, "distance_metric=", 16) == 0) {
            const char *m = start + 16;
            if (strcmp(m, "l2") == 0) {
                metric = 0;
            } else if (strcmp(m, "l1") == 0) {
                metric = 1;
            } else if (strcmp(m, "cosine") == 0) {
                metric = 2;
            } else {
                sqlite3_free(joined);
                sqlite3_free(name);
                return SQLITE_ERROR;
            }
        } else {
            size_t len = strlen(start);
            char *grown = sqlite3_realloc(name, nameLen + (nameLen ? 1 : 0) + len + 1);
            if (!grown) {
                sqlite3_free(joined);
                sqlite3_free(name);
                return SQLITE_NOMEM;
            }
            name = grown;
            if (nameLen) {
                name[nameLen++] = ' ';
            }
            memcpy(name + nameLen, start, len + 1);
            nameLen += len;
        }
        *tok = saved;
        (void)save;
    }
    sqlite3_free(joined);
    if (dims < 0 || !name || !*name) {
        sqlite3_free(name);
        return SQLITE_ERROR;
    }
    v->column = name;
    v->dims = dims;
    v->metric = metric;
    return SQLITE_OK;
}

static int vec0_create(sqlite3 *db, void *aux, int argc, const char *const *argv,
                       sqlite3_vtab **pp, char **err) {
    (void)aux;
    (void)err;
    vec0_vtab *v = sqlite3_malloc(sizeof(*v));
    if (!v) {
        return SQLITE_NOMEM;
    }
    memset(v, 0, sizeof(*v));
    v->db = db;
    if (vec0_parse_args(v, argc, argv) != SQLITE_OK) {
        sqlite3_free(v);
        return SQLITE_ERROR;
    }
    v->shadow = sqlite3_mprintf("%s_vec0_data", argv[2]);
    if (!v->shadow) {
        sqlite3_free(v->column);
        sqlite3_free(v);
        return SQLITE_NOMEM;
    }
    char *ddl = sqlite3_mprintf("CREATE TABLE x(%w BLOB, \"distance\" REAL, \"k\" INTEGER HIDDEN)", v->column);
    if (!ddl) {
        sqlite3_free(v->shadow);
        sqlite3_free(v->column);
        sqlite3_free(v);
        return SQLITE_NOMEM;
    }
    int rc = sqlite3_declare_vtab(db, ddl);
    sqlite3_free(ddl);
    if (rc != SQLITE_OK) {
        sqlite3_free(v->shadow);
        sqlite3_free(v->column);
        sqlite3_free(v);
        return rc;
    }
    char *shadowDDL = sqlite3_mprintf("CREATE TABLE IF NOT EXISTS temp.%w(rowid INTEGER PRIMARY KEY, embedding BLOB)", v->shadow);
    if (!shadowDDL) {
        sqlite3_free(v->shadow);
        sqlite3_free(v->column);
        sqlite3_free(v);
        return SQLITE_NOMEM;
    }
    rc = vec0_exec(db, shadowDDL);
    sqlite3_free(shadowDDL);
    if (rc != SQLITE_OK) {
        sqlite3_free(v->shadow);
        sqlite3_free(v->column);
        sqlite3_free(v);
        return rc;
    }
    *pp = &v->base;
    return SQLITE_OK;
}

static int vec0_connect(sqlite3 *db, void *aux, int argc, const char *const *argv,
                        sqlite3_vtab **pp, char **err) {
    /* xConnect must not run DDL: reconnects happen on ordinary reads (for
     * example after a rolled-back reconfiguration drops and restores the
     * index), where the authorizer denies TEMP writes. The shadow table
     * already exists whenever the vtab does: xDestroy drops it only when
     * the DROP commits. */
    (void)aux;
    (void)err;
    vec0_vtab *v = sqlite3_malloc(sizeof(*v));
    if (!v) {
        return SQLITE_NOMEM;
    }
    memset(v, 0, sizeof(*v));
    v->db = db;
    if (vec0_parse_args(v, argc, argv) != SQLITE_OK) {
        sqlite3_free(v);
        return SQLITE_ERROR;
    }
    v->shadow = sqlite3_mprintf("%s_vec0_data", argv[2]);
    if (!v->shadow) {
        sqlite3_free(v->column);
        sqlite3_free(v);
        return SQLITE_NOMEM;
    }
    char *ddl = sqlite3_mprintf("CREATE TABLE x(%w BLOB, \"distance\" REAL, \"k\" INTEGER HIDDEN)", v->column);
    if (!ddl) {
        sqlite3_free(v->shadow);
        sqlite3_free(v->column);
        sqlite3_free(v);
        return SQLITE_NOMEM;
    }
    int rc = sqlite3_declare_vtab(db, ddl);
    sqlite3_free(ddl);
    if (rc != SQLITE_OK) {
        sqlite3_free(v->shadow);
        sqlite3_free(v->column);
        sqlite3_free(v);
        return rc;
    }
    *pp = &v->base;
    return SQLITE_OK;
}

static int vec0_disconnect(sqlite3_vtab *p) {
    vec0_vtab *v = (vec0_vtab *)p;
    sqlite3_free(v->shadow);
    sqlite3_free(v->column);
    sqlite3_free(v);
    return SQLITE_OK;
}

static int vec0_destroy(sqlite3_vtab *p) {
    vec0_vtab *v = (vec0_vtab *)p;
    char *sql = sqlite3_mprintf("DROP TABLE IF EXISTS temp.%w", v->shadow);
    if (sql) {
        vec0_exec(v->db, sql);
        sqlite3_free(sql);
    }
    return vec0_disconnect(p);
}

static int vec0_rename(sqlite3_vtab *p, const char *name) {
    vec0_vtab *v = (vec0_vtab *)p;
    char *next = sqlite3_mprintf("%s_vec0_data", name);
    if (!next) {
        return SQLITE_NOMEM;
    }
    char *sql = sqlite3_mprintf("ALTER TABLE temp.%w RENAME TO %w", v->shadow, next);
    if (!sql) {
        sqlite3_free(next);
        return SQLITE_NOMEM;
    }
    int rc = vec0_exec(v->db, sql);
    sqlite3_free(sql);
    if (rc == SQLITE_OK) {
        sqlite3_free(v->shadow);
        v->shadow = next;
    } else {
        sqlite3_free(next);
    }
    return rc;
}

static int vec0_open(sqlite3_vtab *p, sqlite3_vtab_cursor **pp) {
    (void)p;
    vec0_cursor *c = sqlite3_malloc(sizeof(*c));
    if (!c) {
        return SQLITE_NOMEM;
    }
    memset(c, 0, sizeof(*c));
    *pp = &c->base;
    return SQLITE_OK;
}

static int vec0_close(sqlite3_vtab_cursor *p) {
    vec0_free_results((vec0_cursor *)p);
    sqlite3_free(p);
    return SQLITE_OK;
}

static int vec0_bestindex(sqlite3_vtab *p, sqlite3_index_info *info) {
    (void)p;
    /* argv order is canonical: query, k, rowid. */
    int idxNum = 0;
    int nextArg = 1;
    for (int pass = 0; pass < 3; pass++) {
        for (int i = 0; i < info->nConstraint; i++) {
            const struct sqlite3_index_constraint *c = &info->aConstraint[i];
            if (!c->usable || info->aConstraintUsage[i].argvIndex > 0) {
                continue;
            }
            int bit = 0;
            if (pass == 0 && c->iColumn == 0 && c->op == SQLITE_INDEX_CONSTRAINT_MATCH) {
                bit = 1;
            } else if (pass == 1 && c->iColumn == 2 && c->op == SQLITE_INDEX_CONSTRAINT_EQ) {
                bit = 2;
            } else if (pass == 2 && c->iColumn == -1 && c->op == SQLITE_INDEX_CONSTRAINT_EQ) {
                bit = 4;
            }
            if (bit && !(idxNum & bit)) {
                idxNum |= bit;
                info->aConstraintUsage[i].argvIndex = nextArg++;
                info->aConstraintUsage[i].omit = 1;
                break;
            }
        }
    }
    info->idxNum = idxNum;
    info->estimatedCost = (idxNum & 1) ? 10.0 : 1000000.0;
    info->estimatedRows = 1000;
    return SQLITE_OK;
}

typedef struct {
    int64_t rowid;
    double distance;
    unsigned char *vector;
    int length;
} vec0_hit;

static int vec0_hit_compare(const void *a, const void *b) {
    const vec0_hit *x = a, *y = b;
    if (x->distance < y->distance) {
        return -1;
    }
    if (x->distance > y->distance) {
        return 1;
    }
    if (x->rowid < y->rowid) {
        return -1;
    }
    if (x->rowid > y->rowid) {
        return 1;
    }
    return 0;
}

static int vec0_query_vector(vec0_vtab *v, sqlite3_value *val, float *out) {
    if (sqlite3_value_type(val) == SQLITE_BLOB) {
        int n = sqlite3_value_bytes(val);
        if (n != v->dims * 4) {
            return SQLITE_ERROR;
        }
        memcpy(out, sqlite3_value_blob(val), (size_t)n);
        return SQLITE_OK;
    }
    const unsigned char *text = sqlite3_value_text(val);
    if (!text) {
        return SQLITE_ERROR;
    }
    int n = sqlite3_value_bytes(val);
    if (vec0_parse_text((const char *)text, n, out, v->dims) != v->dims) {
        return SQLITE_ERROR;
    }
    return SQLITE_OK;
}

static int vec0_filter(sqlite3_vtab_cursor *p, int idxNum, const char *idxStr,
                       int argc, sqlite3_value **argv) {
    (void)idxStr;
    vec0_vtab *v = (vec0_vtab *)p->pVtab;
    vec0_cursor *c = (vec0_cursor *)p;
    vec0_free_results(c);

    float *query = 0;
    int hasQuery = (idxNum & 1) != 0;
    int arg = 0;
    if (hasQuery) {
        if (arg >= argc) {
            return SQLITE_ERROR;
        }
        query = sqlite3_malloc(sizeof(float) * (size_t)v->dims);
        if (!query) {
            return SQLITE_NOMEM;
        }
        if (vec0_query_vector(v, argv[arg++], query) != SQLITE_OK) {
            sqlite3_free(query);
            return SQLITE_ERROR;
        }
    }
    int64_t k = -1;
    if (idxNum & 2) {
        if (arg >= argc) {
            sqlite3_free(query);
            return SQLITE_ERROR;
        }
        k = sqlite3_value_int64(argv[arg++]);
        if (k < 0) {
            k = 0;
        }
    }
    int rowidOnly = (idxNum & 4) != 0;
    int64_t wantRowid = 0;
    if (rowidOnly) {
        if (arg >= argc) {
            sqlite3_free(query);
            return SQLITE_ERROR;
        }
        wantRowid = sqlite3_value_int64(argv[arg++]);
    }

    char *sql;
    if (rowidOnly) {
        sql = sqlite3_mprintf("SELECT rowid, embedding FROM temp.%w WHERE rowid=?", v->shadow);
    } else {
        sql = sqlite3_mprintf("SELECT rowid, embedding FROM temp.%w", v->shadow);
    }
    if (!sql) {
        sqlite3_free(query);
        return SQLITE_NOMEM;
    }
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, 0);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        sqlite3_free(query);
        return rc;
    }
    if (rowidOnly) {
        sqlite3_bind_int64(stmt, 1, wantRowid);
    }
    vec0_hit *hits = 0;
    int count = 0, cap = 0;
    float *row = sqlite3_malloc(sizeof(float) * (size_t)v->dims);
    if (!row) {
        sqlite3_finalize(stmt);
        sqlite3_free(query);
        return SQLITE_NOMEM;
    }
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(stmt, 0);
        int n = sqlite3_column_bytes(stmt, 1);
        const void *blob = sqlite3_column_blob(stmt, 1);
        if (n != v->dims * 4 || !blob) {
            continue;
        }
        if (count == cap) {
            int next = cap ? cap * 2 : 64;
            vec0_hit *grown = sqlite3_realloc(hits, sizeof(*grown) * (size_t)next);
            if (!grown) {
                rc = SQLITE_NOMEM;
                break;
            }
            hits = grown;
            cap = next;
        }
        unsigned char *copy = sqlite3_malloc((size_t)n);
        if (!copy) {
            rc = SQLITE_NOMEM;
            break;
        }
        memcpy(copy, blob, (size_t)n);
        memcpy(row, copy, (size_t)n);
        double distance = 0;
        if (hasQuery) {
            distance = vec0_distance(v->metric, query, row, v->dims);
        }
        hits[count].rowid = id;
        hits[count].distance = distance;
        hits[count].vector = copy;
        hits[count].length = n;
        count++;
    }
    sqlite3_free(row);
    sqlite3_free(query);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        for (int i = 0; i < count; i++) {
            sqlite3_free(hits[i].vector);
        }
        sqlite3_free(hits);
        return rc;
    }
    if (hasQuery && count > 1) {
        qsort(hits, (size_t)count, sizeof(*hits), vec0_hit_compare);
    }
    if (k >= 0 && count > k) {
        for (int i = (int)k; i < count; i++) {
            sqlite3_free(hits[i].vector);
        }
        count = (int)k;
    }
    c->rowids = sqlite3_malloc(sizeof(*c->rowids) * (size_t)(count ? count : 1));
    c->distances = sqlite3_malloc(sizeof(*c->distances) * (size_t)(count ? count : 1));
    c->vectors = sqlite3_malloc(sizeof(*c->vectors) * (size_t)(count ? count : 1));
    c->lengths = sqlite3_malloc(sizeof(*c->lengths) * (size_t)(count ? count : 1));
    if (!c->rowids || !c->distances || !c->vectors || !c->lengths) {
        for (int i = 0; i < count; i++) {
            sqlite3_free(hits[i].vector);
        }
        sqlite3_free(hits);
        vec0_free_results(c);
        return SQLITE_NOMEM;
    }
    for (int i = 0; i < count; i++) {
        c->rowids[i] = hits[i].rowid;
        c->distances[i] = hits[i].distance;
        c->vectors[i] = hits[i].vector;
        c->lengths[i] = hits[i].length;
    }
    sqlite3_free(hits);
    c->count = count;
    c->pos = 0;
    return SQLITE_OK;
}

static int vec0_eof(sqlite3_vtab_cursor *p) {
    vec0_cursor *c = (vec0_cursor *)p;
    return c->pos >= c->count;
}

static int vec0_next(sqlite3_vtab_cursor *p) {
    ((vec0_cursor *)p)->pos++;
    return SQLITE_OK;
}

static int vec0_column(sqlite3_vtab_cursor *p, sqlite3_context *ctx, int i) {
    vec0_cursor *c = (vec0_cursor *)p;
    if (c->pos < 0 || c->pos >= c->count) {
        return SQLITE_ERROR;
    }
    switch (i) {
    case 0:
        sqlite3_result_blob(ctx, c->vectors[c->pos], c->lengths[c->pos], SQLITE_TRANSIENT);
        break;
    case 1:
        sqlite3_result_double(ctx, c->distances[c->pos]);
        break;
    case 2:
        sqlite3_result_null(ctx);
        break;
    default:
        return SQLITE_ERROR;
    }
    return SQLITE_OK;
}

static int vec0_rowid(sqlite3_vtab_cursor *p, sqlite_int64 *id) {
    vec0_cursor *c = (vec0_cursor *)p;
    if (c->pos < 0 || c->pos >= c->count) {
        return SQLITE_ERROR;
    }
    *id = c->rowids[c->pos];
    return SQLITE_OK;
}

static int vec0_update(sqlite3_vtab *p, int argc, sqlite3_value **argv, sqlite_int64 *rowid) {
    vec0_vtab *v = (vec0_vtab *)p;
    if (argc == 1) {
        /* DELETE */
        char *sql = sqlite3_mprintf("DELETE FROM temp.%w WHERE rowid=?", v->shadow);
        if (!sql) {
            return SQLITE_NOMEM;
        }
        sqlite3_stmt *stmt = 0;
        int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, 0);
        sqlite3_free(sql);
        if (rc != SQLITE_OK) {
            return rc;
        }
        sqlite3_bind_int64(stmt, 1, sqlite3_value_int64(argv[0]));
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        return rc == SQLITE_DONE ? SQLITE_OK : rc;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        /* INSERT */
        if (argc < 3 || sqlite3_value_type(argv[2]) != SQLITE_BLOB) {
            return SQLITE_ERROR;
        }
        int n = sqlite3_value_bytes(argv[2]);
        if (n != v->dims * 4) {
            return SQLITE_ERROR;
        }
        char *sql = sqlite3_mprintf("INSERT INTO temp.%w(rowid, embedding) VALUES(?,?)", v->shadow);
        if (!sql) {
            return SQLITE_NOMEM;
        }
        sqlite3_stmt *stmt = 0;
        int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, 0);
        sqlite3_free(sql);
        if (rc != SQLITE_OK) {
            return rc;
        }
        if (sqlite3_value_type(argv[1]) == SQLITE_NULL) {
            sqlite3_bind_null(stmt, 1);
        } else {
            sqlite3_bind_int64(stmt, 1, sqlite3_value_int64(argv[1]));
        }
        sqlite3_bind_blob(stmt, 2, sqlite3_value_blob(argv[2]), n, SQLITE_TRANSIENT);
        rc = sqlite3_step(stmt);
        if (rc == SQLITE_DONE && rowid) {
            *rowid = sqlite3_last_insert_rowid(v->db);
        }
        sqlite3_finalize(stmt);
        return rc == SQLITE_DONE ? SQLITE_OK : rc;
    }
    /* UPDATE */
    if (argc < 3 || sqlite3_value_type(argv[2]) != SQLITE_BLOB) {
        return SQLITE_ERROR;
    }
    int n = sqlite3_value_bytes(argv[2]);
    if (n != v->dims * 4) {
        return SQLITE_ERROR;
    }
    char *sql = sqlite3_mprintf("UPDATE temp.%w SET rowid=?, embedding=? WHERE rowid=?", v->shadow);
    if (!sql) {
        return SQLITE_NOMEM;
    }
    sqlite3_stmt *stmt = 0;
    int rc = sqlite3_prepare_v2(v->db, sql, -1, &stmt, 0);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) {
        return rc;
    }
    sqlite3_bind_int64(stmt, 1, sqlite3_value_int64(argv[1]));
    sqlite3_bind_blob(stmt, 2, sqlite3_value_blob(argv[2]), n, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 3, sqlite3_value_int64(argv[0]));
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_DONE && rowid) {
        *rowid = sqlite3_value_int64(argv[1]);
    }
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? SQLITE_OK : rc;
}

static sqlite3_module vec0_module = {
    1,              /* iVersion */
    vec0_create,    /* xCreate */
    vec0_connect,   /* xConnect */
    vec0_bestindex, /* xBestIndex */
    vec0_disconnect,
    vec0_destroy,
    vec0_open,
    vec0_close,
    vec0_filter,
    vec0_next,
    vec0_eof,
    vec0_column,
    vec0_rowid,
    vec0_update,
    0, /* xBegin */
    0, /* xSync */
    0, /* xCommit */
    0, /* xRollback */
    0, /* xFindMethod */
    vec0_rename, /* xRename */
    0, /* xSavepoint */
    0, /* xRelease */
    0, /* xRollbackTo */
    0, /* xShadowName */
    0  /* xIntegrity */
};

int murmurRegisterVec0(sqlite3 *db) {
    return sqlite3_create_module(db, "vec0", &vec0_module, 0);
}
