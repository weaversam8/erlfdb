// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

// erlfdb_worker: standalone subprocess that wraps a single libfdb_c network
// thread and communicates with BEAM via {packet, 4}-framed ETF over
// stdin/stdout.
//
// Startup sequence:
//   1. Write {hello, OsPid} to stdout.
//   2. Read {req, 1, init, {ApiVersion, [{Name, BinVal}, ...]}} from stdin.
//   3. Call fdb_select_api_version, fdb_network_set_option for each opt,
//      fdb_setup_network, spawn the FDB network thread.
//   4. Reply {reply, 1, ok}.
//   5. Loop dispatching requests until stdin EOF.
//   6. On EOF: fdb_stop_network(), join the network thread, exit.

#include <ei.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
// FDB_API_VERSION must be set via -DFDB_API_VERSION=xxx at compile time by
// the Makefile. Define a fallback of 0 so IDE analyzers that lack the -D
// flag still produce well-formed C on the #if guards below.
#ifndef FDB_API_VERSION
#define FDB_API_VERSION 0
#endif
#include "fdb.h"
#include "notify.h"
#include "registry.h"
#include <poll.h>

// ---------------------------------------------------------------------------
// Future type tag — determines which fdb_future_get_* function to call
// ---------------------------------------------------------------------------

typedef enum {
    ERLFDB_FUT_VOID  = 0,
    ERLFDB_FUT_VALUE,       // fdb_future_get_value  (transaction_get)
    ERLFDB_FUT_INT64,       // fdb_future_get_int64  (get_read_version, etc.)
    ERLFDB_FUT_KEY,         // fdb_future_get_key
    ERLFDB_FUT_STRING_ARRAY,
    ERLFDB_FUT_KEYVALUE_ARRAY,
    ERLFDB_FUT_MAPPEDKEYVALUE_ARRAY,
    ERLFDB_FUT_KEY_ARRAY,
} erlfdb_future_type;

// Enhanced future struct.  Stored in future_registry; freed after future_get.
typedef struct {
    FDBFuture *future;
    erlfdb_future_type ftype;
    erlang_ref fut_ref;    // the Erlang ref — used in the {ready,...} notification
    erlang_pid owner_pid;  // the Erlang process to notify when ready
    int tx_scoped;         // 1 → include tx_ref in notification
    erlang_ref tx_ref;     // the transaction ref for tx-scoped notifications
} erlfdb_future_t;

// ---------------------------------------------------------------------------
// Enhanced transaction struct — wraps FDBTransaction* with the local flags
// the NIF tracked in ErlFDBTransaction (read_only, writes_allowed, has_watches,
// txid). Stored in tx_registry instead of a raw FDBTransaction*.
// ---------------------------------------------------------------------------

typedef struct {
    FDBTransaction *transaction;
    uint32_t txid;
    int read_only;      // starts 1; cleared to 0 on first write
    int writes_allowed; // starts 1; cleared by disallow_writes option
    int has_watches;    // set to 1 when a watch is registered
} erlfdb_tx_t;

// ---------------------------------------------------------------------------
// Resource registries — one per FDB handle type
// ---------------------------------------------------------------------------

static erlfdb_registry db_registry;
static erlfdb_registry tenant_registry;
static erlfdb_registry tx_registry;
static erlfdb_registry future_registry;

static void registries_init(void) {
    erlfdb_registry_init(&db_registry);
    erlfdb_registry_init(&tenant_registry);
    erlfdb_registry_init(&tx_registry);
    erlfdb_registry_init(&future_registry);
}

static void fdb_database_destroy_cb(void *v) {
    if (v) fdb_database_destroy((FDBDatabase *)v);
}
static void fdb_tenant_destroy_cb(void *v) {
    if (v) fdb_tenant_destroy((FDBTenant *)v);
}
static void erlfdb_tx_destroy_cb(void *v) {
    // v is void* here because erlfdb_registry_destroy takes a generic
    // void (*)(void*) callback — see registry.h. Cast to the real type inside.
    if (!v) return;
    erlfdb_tx_t *t = (erlfdb_tx_t *)v;
    if (t->transaction) fdb_transaction_destroy(t->transaction);
    free(t);
}
static void erlfdb_future_destroy_cb(void *v) {
    if (!v) return;
    erlfdb_future_t *ft = (erlfdb_future_t *)v;
    if (ft->future) fdb_future_destroy(ft->future);
    free(ft);
}

static void registries_destroy(void) {
    erlfdb_registry_destroy(&db_registry, fdb_database_destroy_cb);
    erlfdb_registry_destroy(&tenant_registry, fdb_tenant_destroy_cb);
    erlfdb_registry_destroy(&tx_registry, erlfdb_tx_destroy_cb);
    erlfdb_registry_destroy(&future_registry, erlfdb_future_destroy_cb);
}

// ---------------------------------------------------------------------------
// Global notify queue — FDB network thread enqueues; main thread drains
// ---------------------------------------------------------------------------

static erlfdb_notify_queue g_notify_queue;

// Forward declaration for write_frame defined in the I/O helpers section.
static int write_frame(const uint8_t *buf, uint32_t len);

// Registered with every async FDB future via fdb_future_set_callback.
// Runs on the FDB network thread.
static void fdb_future_completion_cb(FDBFuture *future, void *user_data) {
    (void)future;
    erlfdb_future_t *ft = (erlfdb_future_t *)user_data;
    erlfdb_notify_signal(&g_notify_queue, &ft->fut_ref);
}

// Called from the main thread for each notification dequeued from the pipe.
// Looks up the future and sends the appropriate {ready,...} frame to BEAM.
static void send_ready(const erlang_pid *pid, int tx_scoped,
                       const erlang_ref *tx_ref, const erlang_ref *fut_ref) {
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) return;
    if (tx_scoped) {
        ei_x_encode_tuple_header(&x, 4);
        ei_x_encode_atom(&x, "ready");
        ei_x_encode_pid(&x, pid);
        ei_x_encode_ref(&x, tx_ref);
        ei_x_encode_ref(&x, fut_ref);
    } else {
        ei_x_encode_tuple_header(&x, 3);
        ei_x_encode_atom(&x, "ready");
        ei_x_encode_pid(&x, pid);
        ei_x_encode_ref(&x, fut_ref);
    }
    write_frame((uint8_t *)x.buff, (uint32_t)x.index);
    ei_x_free(&x);
}

static void process_notification(const erlang_ref *fut_ref, void *ctx) {
    (void)ctx;
    erlfdb_future_t *ft =
        (erlfdb_future_t *)erlfdb_registry_get(&future_registry, fut_ref);
    if (!ft) return; // future already freed (e.g. cancel) — ignore
    send_ready(&ft->owner_pid, ft->tx_scoped, &ft->tx_ref, &ft->fut_ref);
}

// ---------------------------------------------------------------------------
// Network thread state
// ---------------------------------------------------------------------------

static pthread_t g_network_tid;
static int g_network_running = 0;

// Used to block dispatch_init until the network thread has entered
// fdb_run_network and is ready.
static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_init_cond = PTHREAD_COND_INITIALIZER;
static int g_network_started = 0;

static void *network_thread_fn(void *arg) {
    (void)arg;
    pthread_mutex_lock(&g_init_mutex);
    g_network_started = 1;
    pthread_cond_signal(&g_init_cond);
    pthread_mutex_unlock(&g_init_mutex);
    fdb_run_network();
    return NULL;
}

// ---------------------------------------------------------------------------
// I/O helpers
// ---------------------------------------------------------------------------

static int read_exact(int fd, void *buf, size_t want) {
    uint8_t *p = (uint8_t *)buf;
    size_t got = 0;
    while (got < want) {
        ssize_t n = read(fd, p + got, want - got);
        if (n == 0) return 0;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        got += (size_t)n;
    }
    return 1;
}

static int write_exact(int fd, const void *buf, size_t want) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t sent = 0;
    while (sent < want) {
        ssize_t n = write(fd, p + sent, want - sent);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

static int read_frame(uint8_t **out, uint32_t *outlen) {
    uint8_t hdr[4];
    int r = read_exact(STDIN_FILENO, hdr, 4);
    if (r <= 0) return r;
    uint32_t len = ((uint32_t)hdr[0] << 24) | ((uint32_t)hdr[1] << 16) |
                   ((uint32_t)hdr[2] << 8) | (uint32_t)hdr[3];
    uint8_t *buf = NULL;
    if (len > 0) {
        buf = (uint8_t *)malloc(len);
        if (!buf) return -1;
        if (read_exact(STDIN_FILENO, buf, len) != 1) {
            free(buf);
            return -1;
        }
    }
    *out = buf;
    *outlen = len;
    return 1;
}

static int write_frame(const uint8_t *buf, uint32_t len) {
    uint8_t hdr[4];
    hdr[0] = (uint8_t)((len >> 24) & 0xFF);
    hdr[1] = (uint8_t)((len >> 16) & 0xFF);
    hdr[2] = (uint8_t)((len >> 8) & 0xFF);
    hdr[3] = (uint8_t)(len & 0xFF);
    if (write_exact(STDOUT_FILENO, hdr, 4) != 0) return -1;
    if (len > 0 && write_exact(STDOUT_FILENO, buf, len) != 0) return -1;
    return 0;
}

// ---------------------------------------------------------------------------
// Reply helpers
// ---------------------------------------------------------------------------

static int send_hello(void) {
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) return -1;
    ei_x_encode_tuple_header(&x, 2);
    ei_x_encode_atom(&x, "hello");
    ei_x_encode_long(&x, (long)getpid());
    int rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
    ei_x_free(&x);
    return rc;
}

static int send_reply_ok(long req_id) {
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) return -1;
    ei_x_encode_tuple_header(&x, 3);
    ei_x_encode_atom(&x, "reply");
    ei_x_encode_long(&x, req_id);
    ei_x_encode_atom(&x, "ok");
    int rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
    ei_x_free(&x);
    return rc;
}

static int send_reply_ok_long(long req_id, long value) {
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) return -1;
    ei_x_encode_tuple_header(&x, 3);
    ei_x_encode_atom(&x, "reply");
    ei_x_encode_long(&x, req_id);
    ei_x_encode_tuple_header(&x, 2);
    ei_x_encode_atom(&x, "ok");
    ei_x_encode_long(&x, value);
    int rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
    ei_x_free(&x);
    return rc;
}

static int send_reply_ok_binary(long req_id, const uint8_t *data, int len) {
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) return -1;
    ei_x_encode_tuple_header(&x, 3);
    ei_x_encode_atom(&x, "reply");
    ei_x_encode_long(&x, req_id);
    ei_x_encode_tuple_header(&x, 2);
    ei_x_encode_atom(&x, "ok");
    ei_x_encode_binary(&x, data, len);
    int rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
    ei_x_free(&x);
    return rc;
}

static int send_reply_ok_double(long req_id, double value) {
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) return -1;
    ei_x_encode_tuple_header(&x, 3);
    ei_x_encode_atom(&x, "reply");
    ei_x_encode_long(&x, req_id);
    ei_x_encode_tuple_header(&x, 2);
    ei_x_encode_atom(&x, "ok");
    ei_x_encode_double(&x, value);
    int rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
    ei_x_free(&x);
    return rc;
}

static int send_reply_not_found(long req_id) {
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) return -1;
    ei_x_encode_tuple_header(&x, 3);
    ei_x_encode_atom(&x, "reply");
    ei_x_encode_long(&x, req_id);
    ei_x_encode_atom(&x, "not_found");
    int rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
    ei_x_free(&x);
    return rc;
}

// Send {reply, ReqId, {error, FdbErrorCode}} where the code is an integer,
// matching {erlfdb_error, Code} semantics on the Erlang side.
static int send_reply_fdb_error(long req_id, fdb_error_t err) {
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) return -1;
    ei_x_encode_tuple_header(&x, 3);
    ei_x_encode_atom(&x, "reply");
    ei_x_encode_long(&x, req_id);
    ei_x_encode_tuple_header(&x, 2);
    ei_x_encode_atom(&x, "error");
    ei_x_encode_long(&x, (long)err);
    int rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
    ei_x_free(&x);
    return rc;
}

static int send_reply_error(long req_id, const char *reason) {
    ei_x_buff x;
    if (ei_x_new_with_version(&x) != 0) return -1;
    ei_x_encode_tuple_header(&x, 3);
    ei_x_encode_atom(&x, "reply");
    ei_x_encode_long(&x, req_id);
    ei_x_encode_tuple_header(&x, 2);
    ei_x_encode_atom(&x, "error");
    ei_x_encode_atom(&x, reason);
    int rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
    ei_x_free(&x);
    return rc;
}

// ---------------------------------------------------------------------------
// Network option name -> FDBNetworkOption lookup
// ---------------------------------------------------------------------------

typedef struct {
    const char *name;
    FDBNetworkOption opt;
} network_option_entry;

static const network_option_entry network_option_map[] = {
    {"local_address",               FDB_NET_OPTION_LOCAL_ADDRESS},
    {"cluster_file",                FDB_NET_OPTION_CLUSTER_FILE},
    {"trace_enable",                FDB_NET_OPTION_TRACE_ENABLE},
    {"trace_format",                FDB_NET_OPTION_TRACE_FORMAT},
    {"trace_roll_size",             FDB_NET_OPTION_TRACE_ROLL_SIZE},
    {"trace_max_logs_size",         FDB_NET_OPTION_TRACE_MAX_LOGS_SIZE},
    {"trace_log_group",             FDB_NET_OPTION_TRACE_LOG_GROUP},
    {"knob",                        FDB_NET_OPTION_KNOB},
    {"tls_plugin",                  FDB_NET_OPTION_TLS_PLUGIN},
    {"tls_cert_bytes",              FDB_NET_OPTION_TLS_CERT_BYTES},
    {"tls_cert_path",               FDB_NET_OPTION_TLS_CERT_PATH},
    {"tls_key_bytes",               FDB_NET_OPTION_TLS_KEY_BYTES},
    {"tls_key_path",                FDB_NET_OPTION_TLS_KEY_PATH},
    {"tls_verify_peers",            FDB_NET_OPTION_TLS_VERIFY_PEERS},
    {"tls_ca_bytes",                FDB_NET_OPTION_TLS_CA_BYTES},
    {"tls_password",                FDB_NET_OPTION_TLS_PASSWORD},
    {"client_buggify_enable",       FDB_NET_OPTION_CLIENT_BUGGIFY_ENABLE},
    {"client_buggify_disable",      FDB_NET_OPTION_CLIENT_BUGGIFY_DISABLE},
    {"client_buggify_section_activated_probability",
                                    FDB_NET_OPTION_CLIENT_BUGGIFY_SECTION_ACTIVATED_PROBABILITY},
    {"client_buggify_section_fired_probability",
                                    FDB_NET_OPTION_CLIENT_BUGGIFY_SECTION_FIRED_PROBABILITY},
    {"disable_multi_version_client_api",
                                    FDB_NET_OPTION_DISABLE_MULTI_VERSION_CLIENT_API},
    {"external_client_library",     FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY},
    {"external_client_directory",   FDB_NET_OPTION_EXTERNAL_CLIENT_DIRECTORY},
    {"disable_local_client",        FDB_NET_OPTION_DISABLE_LOCAL_CLIENT},
    {"disable_client_statistics_logging",
                                    FDB_NET_OPTION_DISABLE_CLIENT_STATISTICS_LOGGING},
    {"enable_slow_task_profiling",  FDB_NET_OPTION_ENABLE_SLOW_TASK_PROFILING},
    {"callbacks_on_external_threads",
                                    FDB_NET_OPTION_CALLBACKS_ON_EXTERNAL_THREADS},
#if FDB_API_VERSION >= 630
    {"enable_run_loop_profiling",   FDB_NET_OPTION_ENABLE_RUN_LOOP_PROFILING},
    {"client_threads_per_version",  FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION},
#endif
#if FDB_API_VERSION >= 730
    {"ignore_external_client_failures",
                                    FDB_NET_OPTION_IGNORE_EXTERNAL_CLIENT_FAILURES},
#endif
};

static int lookup_network_option(const char *name, FDBNetworkOption *out) {
    size_t n = sizeof(network_option_map) / sizeof(network_option_map[0]);
    for (size_t i = 0; i < n; i++) {
        if (strcmp(name, network_option_map[i].name) == 0) {
            *out = network_option_map[i].opt;
            return 1;
        }
    }
    return 0;
}

// ---------------------------------------------------------------------------
// Dispatch: init
// ---------------------------------------------------------------------------

static int dispatch_init(long req_id, const char *buf, int *idx) {
    // Decode {ApiVersion, [{Name, BinVal}, ...]}
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 2) {
        return send_reply_error(req_id, "bad_init_args");
    }

    long api_version = 0;
    if (ei_decode_long(buf, idx, &api_version) != 0) {
        return send_reply_error(req_id, "bad_api_version");
    }

    fdb_error_t err = fdb_select_api_version((int)api_version);
    if (err != 0) {
        return send_reply_error(req_id, "select_api_version_failed");
    }

    // Decode and apply network options
    int list_len = 0;
    if (ei_decode_list_header(buf, idx, &list_len) != 0) {
        return send_reply_error(req_id, "bad_option_list");
    }

    for (int i = 0; i < list_len; i++) {
        int tup_arity = 0;
        if (ei_decode_tuple_header(buf, idx, &tup_arity) != 0 || tup_arity != 2) {
            return send_reply_error(req_id, "bad_option_tuple");
        }

        char opt_name[MAXATOMLEN + 1] = {0};
        if (ei_decode_atom(buf, idx, opt_name) != 0) {
            return send_reply_error(req_id, "bad_option_name");
        }

        // Peek at the value type and length. ei_get_type uses int*; ei_decode_binary
        // uses long* — keep them separate to satisfy both signatures.
        int type = 0;
        int bin_size = 0;
        if (ei_get_type(buf, idx, &type, &bin_size) != 0) {
            return send_reply_error(req_id, "bad_option_value_type");
        }

        // Allocate buffer (1-byte minimum so malloc(0) is never called)
        uint8_t *opt_val = (uint8_t *)malloc((size_t)(bin_size > 0 ? bin_size : 1));
        if (!opt_val) return -1;

        long actual_len = (long)bin_size;
        if (ei_decode_binary(buf, idx, opt_val, &actual_len) != 0) {
            free(opt_val);
            return send_reply_error(req_id, "bad_option_value");
        }

        // Apply the option; unknown names are silently skipped for forward
        // compatibility.
        FDBNetworkOption fdb_opt;
        if (lookup_network_option(opt_name, &fdb_opt)) {
            fdb_network_set_option(fdb_opt, opt_val, (int)actual_len);
        }
        free(opt_val);
    }

    // Consume the list tail (nil)
    if (list_len > 0) {
        int tail = 0;
        ei_decode_list_header(buf, idx, &tail);
    }

    err = fdb_setup_network();
    if (err != 0) {
        return send_reply_error(req_id, "setup_network_failed");
    }

    if (pthread_create(&g_network_tid, NULL, network_thread_fn, NULL) != 0) {
        return send_reply_error(req_id, "network_thread_create_failed");
    }

    // Wait until the network thread signals that it has entered fdb_run_network
    pthread_mutex_lock(&g_init_mutex);
    while (!g_network_started) {
        pthread_cond_wait(&g_init_cond, &g_init_mutex);
    }
    pthread_mutex_unlock(&g_init_mutex);

    g_network_running = 1;
    registries_init();
    erlfdb_notify_init(&g_notify_queue);
    return send_reply_ok(req_id);
}

// ---------------------------------------------------------------------------
// Dispatch: create_database
// ---------------------------------------------------------------------------

static int dispatch_create_database(long req_id, const char *buf, int *idx) {
    // Decode {DbRef, ClusterFile}
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 2) {
        return send_reply_error(req_id, "bad_args");
    }

    erlang_ref db_ref;
    if (ei_decode_ref(buf, idx, &db_ref) != 0) {
        return send_reply_error(req_id, "bad_db_ref");
    }

    // Cluster file arrives as a binary; C side adds its own null terminator.
    int type = 0;
    int bin_size = 0;
    if (ei_get_type(buf, idx, &type, &bin_size) != 0) {
        return send_reply_error(req_id, "bad_cluster_file_type");
    }
    char *cluster_file = (char *)malloc((size_t)bin_size + 1);
    if (!cluster_file) return -1;
    long actual_len = (long)bin_size;
    if (ei_decode_binary(buf, idx, cluster_file, &actual_len) != 0) {
        free(cluster_file);
        return send_reply_error(req_id, "bad_cluster_file");
    }
    cluster_file[bin_size] = '\0';

    FDBDatabase *database = NULL;
    fdb_error_t err = fdb_create_database(cluster_file, &database);
    free(cluster_file);

    if (err != 0) return send_reply_fdb_error(req_id, err);

    if (erlfdb_registry_put(&db_registry, &db_ref, database, NULL) != 0) {
        fdb_database_destroy(database);
        return send_reply_error(req_id, "registry_alloc_failed");
    }

    return send_reply_ok(req_id);
}

// ---------------------------------------------------------------------------
// Dispatch: database_create_transaction
// ---------------------------------------------------------------------------

static int dispatch_database_create_transaction(long req_id, const char *buf,
                                                int *idx) {
    // Decode {DbRef, TxRef}
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 2) {
        return send_reply_error(req_id, "bad_args");
    }

    erlang_ref db_ref;
    if (ei_decode_ref(buf, idx, &db_ref) != 0) {
        return send_reply_error(req_id, "bad_db_ref");
    }

    erlang_ref tx_ref;
    if (ei_decode_ref(buf, idx, &tx_ref) != 0) {
        return send_reply_error(req_id, "bad_tx_ref");
    }

    FDBDatabase *database = (FDBDatabase *)erlfdb_registry_get(&db_registry, &db_ref);
    if (!database) return send_reply_error(req_id, "unknown_db");

    FDBTransaction *transaction = NULL;
    fdb_error_t err = fdb_database_create_transaction(database, &transaction);
    if (err != 0) return send_reply_fdb_error(req_id, err);

    erlfdb_tx_t *tx = (erlfdb_tx_t *)malloc(sizeof(erlfdb_tx_t));
    if (!tx) {
        fdb_transaction_destroy(transaction);
        return -1;
    }
    tx->transaction  = transaction;
    tx->txid         = 0;
    tx->read_only    = 1;
    tx->writes_allowed = 1;
    tx->has_watches  = 0;

    if (erlfdb_registry_put(&tx_registry, &tx_ref, tx, NULL) != 0) {
        erlfdb_tx_destroy_cb(tx);
        return send_reply_error(req_id, "registry_alloc_failed");
    }

    return send_reply_ok(req_id);
}

// ---------------------------------------------------------------------------
// Async dispatch helpers: register a future and set its FDB callback
// ---------------------------------------------------------------------------

// Forward declarations for decode helpers defined in the sync-ops section.
static int decode_tx(const char *buf, int *idx, long req_id,
                     erlfdb_tx_t **out);
static int decode_binary(const char *buf, int *idx, long req_id,
                         uint8_t **valout, int *lenout);

static int register_future(long req_id, FDBFuture *future,
                            erlfdb_future_type ftype,
                            const erlang_ref *fut_ref,
                            const erlang_pid *owner_pid,
                            int tx_scoped,
                            const erlang_ref *tx_ref) {
    erlfdb_future_t *ft = (erlfdb_future_t *)malloc(sizeof(erlfdb_future_t));
    if (!ft) {
        fdb_future_destroy(future);
        return -1;
    }
    ft->future     = future;
    ft->ftype      = ftype;
    ft->fut_ref    = *fut_ref;
    ft->owner_pid  = *owner_pid;
    ft->tx_scoped  = tx_scoped;
    if (tx_scoped && tx_ref) ft->tx_ref = *tx_ref;

    if (erlfdb_registry_put(&future_registry, fut_ref, ft, NULL) != 0) {
        erlfdb_future_destroy_cb(ft);
        return send_reply_error(req_id, "registry_alloc_failed");
    }
    fdb_future_set_callback(future, fdb_future_completion_cb, ft);
    return send_reply_ok(req_id);
}

// ---------------------------------------------------------------------------
// Dispatch: transaction_get
// Args: {TxRef, Key, Snapshot, FutRef, OwnerPid}
// ---------------------------------------------------------------------------

static int dispatch_transaction_get(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 5)
        return send_reply_error(req_id, "bad_args");

    erlang_ref tx_ref;
    if (ei_decode_ref(buf, idx, &tx_ref) != 0)
        return send_reply_error(req_id, "bad_tx_ref");
    erlfdb_tx_t *tx =
        (erlfdb_tx_t *)erlfdb_registry_get(&tx_registry, &tx_ref);
    if (!tx) return send_reply_error(req_id, "unknown_tx");

    uint8_t *key = NULL; int klen = 0;
    int r;
    if ((r = decode_binary(buf, idx, req_id, &key, &klen)) != 0) return r;

    char snap_atom[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom(buf, idx, snap_atom) != 0) {
        free(key);
        return send_reply_error(req_id, "bad_snapshot");
    }
    int snapshot = strcmp(snap_atom, "true") == 0;

    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0) {
        free(key); return send_reply_error(req_id, "bad_fut_ref");
    }
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0) {
        free(key); return send_reply_error(req_id, "bad_owner_pid");
    }

    FDBFuture *future =
        fdb_transaction_get(tx->transaction, key, klen, snapshot);
    free(key);

    return register_future(req_id, future, ERLFDB_FUT_VALUE,
                           &fut_ref, &owner_pid, 1, &tx_ref);
}

// ---------------------------------------------------------------------------
// Dispatch: transaction_commit
// Args: {TxRef, FutRef, OwnerPid}
// ---------------------------------------------------------------------------

static int dispatch_transaction_commit(long req_id, const char *buf,
                                       int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");

    erlang_ref tx_ref;
    if (ei_decode_ref(buf, idx, &tx_ref) != 0)
        return send_reply_error(req_id, "bad_tx_ref");
    erlfdb_tx_t *tx =
        (erlfdb_tx_t *)erlfdb_registry_get(&tx_registry, &tx_ref);
    if (!tx) return send_reply_error(req_id, "unknown_tx");

    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0)
        return send_reply_error(req_id, "bad_owner_pid");

    FDBFuture *future = fdb_transaction_commit(tx->transaction);
    return register_future(req_id, future, ERLFDB_FUT_VOID,
                           &fut_ref, &owner_pid, 1, &tx_ref);
}

// ---------------------------------------------------------------------------
// Dispatch: future_get
// Args: {FutRef}
// Removes and frees the future entry after extracting the result.
// ---------------------------------------------------------------------------

static int dispatch_future_get(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");

    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");

    erlfdb_future_t *ft = (erlfdb_future_t *)erlfdb_registry_remove(
        &future_registry, &fut_ref);
    if (!ft) return send_reply_error(req_id, "unknown_future");

    int rc = 0;
    switch (ft->ftype) {
        case ERLFDB_FUT_VOID: {
            fdb_error_t err = fdb_future_get_error(ft->future);
            rc = (err != 0) ? send_reply_fdb_error(req_id, err)
                            : send_reply_ok(req_id);
            break;
        }
        case ERLFDB_FUT_VALUE: {
            fdb_bool_t present = 0;
            const uint8_t *val = NULL;
            int len = 0;
            fdb_error_t err =
                fdb_future_get_value(ft->future, &present, &val, &len);
            if (err != 0) rc = send_reply_fdb_error(req_id, err);
            else if (!present) rc = send_reply_not_found(req_id);
            else rc = send_reply_ok_binary(req_id, val, len);
            break;
        }
        case ERLFDB_FUT_INT64: {
            int64_t val = 0;
            fdb_error_t err = fdb_future_get_int64(ft->future, &val);
            rc = (err != 0) ? send_reply_fdb_error(req_id, err)
                            : send_reply_ok_long(req_id, (long)val);
            break;
        }
        case ERLFDB_FUT_KEY: {
            const uint8_t *key = NULL;
            int klen = 0;
            fdb_error_t err = fdb_future_get_key(ft->future, &key, &klen);
            rc = (err != 0) ? send_reply_fdb_error(req_id, err)
                            : send_reply_ok_binary(req_id, key, klen);
            break;
        }
        case ERLFDB_FUT_STRING_ARRAY: {
            const char **strings = NULL;
            int count = 0;
            fdb_error_t err = fdb_future_get_string_array(ft->future, &strings, &count);
            if (err != 0) {
                rc = send_reply_fdb_error(req_id, err);
            } else {
                ei_x_buff x;
                if (ei_x_new_with_version(&x) != 0) { rc = -1; break; }
                ei_x_encode_tuple_header(&x, 3);
                ei_x_encode_atom(&x, "reply");
                ei_x_encode_long(&x, req_id);
                ei_x_encode_tuple_header(&x, 2);
                ei_x_encode_atom(&x, "ok");
                if (count > 0) ei_x_encode_list_header(&x, count);
                for (int i = 0; i < count; i++)
                    ei_x_encode_binary(&x, strings[i], (long)strlen(strings[i]));
                ei_x_encode_empty_list(&x);
                rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
                ei_x_free(&x);
            }
            break;
        }
        case ERLFDB_FUT_KEY_ARRAY: {
            FDBKey const *keys = NULL;
            int count = 0;
            fdb_error_t err = fdb_future_get_key_array(ft->future, &keys, &count);
            if (err != 0) {
                rc = send_reply_fdb_error(req_id, err);
            } else {
                ei_x_buff x;
                if (ei_x_new_with_version(&x) != 0) { rc = -1; break; }
                ei_x_encode_tuple_header(&x, 3);
                ei_x_encode_atom(&x, "reply");
                ei_x_encode_long(&x, req_id);
                ei_x_encode_tuple_header(&x, 2);
                ei_x_encode_atom(&x, "ok");
                if (count > 0) ei_x_encode_list_header(&x, count);
                for (int i = 0; i < count; i++)
                    ei_x_encode_binary(&x, keys[i].key, keys[i].key_length);
                ei_x_encode_empty_list(&x);
                rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
                ei_x_free(&x);
            }
            break;
        }
        case ERLFDB_FUT_KEYVALUE_ARRAY: {
            FDBKeyValue const *kvs = NULL;
            fdb_bool_t more = 0;
            int count = 0;
            fdb_error_t err = fdb_future_get_keyvalue_array(ft->future, &kvs, &count, &more);
            if (err != 0) {
                rc = send_reply_fdb_error(req_id, err);
            } else {
                ei_x_buff x;
                if (ei_x_new_with_version(&x) != 0) { rc = -1; break; }
                ei_x_encode_tuple_header(&x, 3);
                ei_x_encode_atom(&x, "reply");
                ei_x_encode_long(&x, req_id);
                ei_x_encode_tuple_header(&x, 2);
                ei_x_encode_atom(&x, "ok");
                ei_x_encode_tuple_header(&x, 3);  /* {KVList, Count, More} */
                if (count > 0) ei_x_encode_list_header(&x, count);
                for (int i = 0; i < count; i++) {
                    ei_x_encode_tuple_header(&x, 2);
                    ei_x_encode_binary(&x, kvs[i].key, kvs[i].key_length);
                    ei_x_encode_binary(&x, kvs[i].value, kvs[i].value_length);
                }
                ei_x_encode_empty_list(&x);
                ei_x_encode_long(&x, (long)count);
                ei_x_encode_atom(&x, more ? "true" : "false");
                rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
                ei_x_free(&x);
            }
            break;
        }
        case ERLFDB_FUT_MAPPEDKEYVALUE_ARRAY: {
#if FDB_API_VERSION >= 730
            FDBMappedKeyValue const *mkvs = NULL;
            fdb_bool_t more = 0;
            int count = 0;
            fdb_error_t err = fdb_future_get_mappedkeyvalue_array(
                ft->future, &mkvs, &count, &more);
            if (err != 0) {
                rc = send_reply_fdb_error(req_id, err);
            } else {
                ei_x_buff x;
                if (ei_x_new_with_version(&x) != 0) { rc = -1; break; }
                ei_x_encode_tuple_header(&x, 3);
                ei_x_encode_atom(&x, "reply");
                ei_x_encode_long(&x, req_id);
                ei_x_encode_tuple_header(&x, 2);
                ei_x_encode_atom(&x, "ok");
                ei_x_encode_tuple_header(&x, 3);  /* {MKVList, Count, More} */
                if (count > 0) ei_x_encode_list_header(&x, count);
                for (int i = 0; i < count; i++) {
                    const FDBMappedKeyValue *mkv = &mkvs[i];
                    const FDBGetRangeReqAndResult *gr = &mkv->getRange;
                    /* {{PQKey, PQVal}, {SQBegin, SQEnd}, [{MK, MV},...]} */
                    ei_x_encode_tuple_header(&x, 3);
                    ei_x_encode_tuple_header(&x, 2);
                    ei_x_encode_binary(&x, mkv->key.key, mkv->key.key_length);
                    ei_x_encode_binary(&x, mkv->value.key, mkv->value.key_length);
                    ei_x_encode_tuple_header(&x, 2);
                    ei_x_encode_binary(&x, gr->begin.key.key, gr->begin.key.key_length);
                    ei_x_encode_binary(&x, gr->end.key.key, gr->end.key.key_length);
                    if (gr->m_size > 0) ei_x_encode_list_header(&x, gr->m_size);
                    for (int j = 0; j < gr->m_size; j++) {
                        ei_x_encode_tuple_header(&x, 2);
                        ei_x_encode_binary(&x, gr->data[j].key, gr->data[j].key_length);
                        ei_x_encode_binary(&x, gr->data[j].value, gr->data[j].value_length);
                    }
                    ei_x_encode_empty_list(&x);
                }
                ei_x_encode_empty_list(&x);
                ei_x_encode_long(&x, (long)count);
                ei_x_encode_atom(&x, more ? "true" : "false");
                rc = write_frame((uint8_t *)x.buff, (uint32_t)x.index);
                ei_x_free(&x);
            }
#else
            rc = send_reply_error(req_id, "not_supported");
#endif
            break;
        }
        default:
            rc = send_reply_error(req_id, "unsupported_future_type");
    }
    erlfdb_future_destroy_cb(ft);
    return rc;
}

// ---------------------------------------------------------------------------
// Transaction option / mutation type / conflict type lookups
// ---------------------------------------------------------------------------

typedef struct { const char *name; FDBTransactionOption opt; } tx_option_entry;
static const tx_option_entry tx_option_map[] = {
    {"causal_write_risky",                   FDB_TR_OPTION_CAUSAL_WRITE_RISKY},
    {"causal_read_risky",                    FDB_TR_OPTION_CAUSAL_READ_RISKY},
    {"causal_read_disable",                  FDB_TR_OPTION_CAUSAL_READ_DISABLE},
    {"include_port_in_address",              FDB_TR_OPTION_INCLUDE_PORT_IN_ADDRESS},
    {"next_write_no_write_conflict_range",   FDB_TR_OPTION_NEXT_WRITE_NO_WRITE_CONFLICT_RANGE},
    {"read_your_writes_disable",             FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE},
    {"read_ahead_disable",                   FDB_TR_OPTION_READ_AHEAD_DISABLE},
    {"durability_datacenter",               FDB_TR_OPTION_DURABILITY_DATACENTER},
    {"durability_risky",                     FDB_TR_OPTION_DURABILITY_RISKY},
    {"durability_dev_null_is_web_scale",     FDB_TR_OPTION_DURABILITY_DEV_NULL_IS_WEB_SCALE},
    {"priority_system_immediate",            FDB_TR_OPTION_PRIORITY_SYSTEM_IMMEDIATE},
    {"priority_batch",                       FDB_TR_OPTION_PRIORITY_BATCH},
    {"initialize_new_database",              FDB_TR_OPTION_INITIALIZE_NEW_DATABASE},
    {"access_system_keys",                   FDB_TR_OPTION_ACCESS_SYSTEM_KEYS},
    {"read_system_keys",                     FDB_TR_OPTION_READ_SYSTEM_KEYS},
    {"debug_retry_logging",                  FDB_TR_OPTION_DEBUG_RETRY_LOGGING},
    {"transaction_logging_enable",           FDB_TR_OPTION_TRANSACTION_LOGGING_ENABLE},
    {"debug_transaction_identifier",         FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER},
    {"log_transaction",                      FDB_TR_OPTION_LOG_TRANSACTION},
    {"transaction_logging_max_field_length", FDB_TR_OPTION_TRANSACTION_LOGGING_MAX_FIELD_LENGTH},
    {"timeout",                              FDB_TR_OPTION_TIMEOUT},
    {"retry_limit",                          FDB_TR_OPTION_RETRY_LIMIT},
    {"max_retry_delay",                      FDB_TR_OPTION_MAX_RETRY_DELAY},
    {"size_limit",                           FDB_TR_OPTION_SIZE_LIMIT},
    {"snapshot_ryw_enable",                  FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE},
    {"snapshot_ryw_disable",                 FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE},
    {"lock_aware",                           FDB_TR_OPTION_LOCK_AWARE},
    {"used_during_commit_protection_disable",FDB_TR_OPTION_USED_DURING_COMMIT_PROTECTION_DISABLE},
    {"read_lock_aware",                      FDB_TR_OPTION_READ_LOCK_AWARE},
    {"use_provisional_proxies",              FDB_TR_OPTION_USE_PROVISIONAL_PROXIES},
#if FDB_API_VERSION > 620
    {"report_conflicting_keys",              FDB_TR_OPTION_REPORT_CONFLICTING_KEYS},
#endif
#if FDB_API_VERSION > 630
    {"special_key_space_enable_writes",      FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES},
#endif
};
static int lookup_tx_option(const char *name, FDBTransactionOption *out) {
    size_t n = sizeof(tx_option_map) / sizeof(tx_option_map[0]);
    for (size_t i = 0; i < n; i++) {
        if (strcmp(name, tx_option_map[i].name) == 0) {
            *out = tx_option_map[i].opt;
            return 1;
        }
    }
    return 0;
}

typedef struct { const char *name; FDBMutationType mtype; } mutation_entry;
static const mutation_entry mutation_map[] = {
    {"add",                      FDB_MUTATION_TYPE_ADD},
    {"bit_and",                  FDB_MUTATION_TYPE_BIT_AND},
    {"bit_or",                   FDB_MUTATION_TYPE_BIT_OR},
    {"bit_xor",                  FDB_MUTATION_TYPE_BIT_XOR},
    {"append_if_fits",           FDB_MUTATION_TYPE_APPEND_IF_FITS},
    {"max",                      FDB_MUTATION_TYPE_MAX},
    {"min",                      FDB_MUTATION_TYPE_MIN},
    {"byte_min",                 FDB_MUTATION_TYPE_BYTE_MIN},
    {"byte_max",                 FDB_MUTATION_TYPE_BYTE_MAX},
    {"set_versionstamped_key",   FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY},
    {"set_versionstamped_value", FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE},
};
static int lookup_mutation_type(const char *name, FDBMutationType *out) {
    size_t n = sizeof(mutation_map) / sizeof(mutation_map[0]);
    for (size_t i = 0; i < n; i++) {
        if (strcmp(name, mutation_map[i].name) == 0) { *out = mutation_map[i].mtype; return 1; }
    }
    return 0;
}

// ---------------------------------------------------------------------------
// decode_tx: decode a TxRef from buf at *idx and look up the transaction.
// On success returns 0 and sets *out. On error sends a reply and returns
// either 0 (error reply sent cleanly) or -1 (fatal write failure).
// Callers distinguish by checking *out == NULL for the logical-error case.
// ---------------------------------------------------------------------------

static int decode_tx(const char *buf, int *idx, long req_id, erlfdb_tx_t **out) {
    *out = NULL;
    erlang_ref tx_ref;
    if (ei_decode_ref(buf, idx, &tx_ref) != 0)
        return send_reply_error(req_id, "bad_tx_ref");
    *out = (erlfdb_tx_t *)erlfdb_registry_get(&tx_registry, &tx_ref);
    if (!*out)
        return send_reply_error(req_id, "unknown_tx");
    return 0;
}

// Helper: decode a binary value at *idx. Caller frees *valout.
static int decode_binary(const char *buf, int *idx, long req_id,
                         uint8_t **valout, int *lenout) {
    int type = 0, bin_size = 0;
    if (ei_get_type(buf, idx, &type, &bin_size) != 0)
        return send_reply_error(req_id, "bad_binary_type");
    *valout = (uint8_t *)malloc((size_t)(bin_size > 0 ? bin_size : 1));
    if (!*valout) return -1;
    long actual = (long)bin_size;
    if (ei_decode_binary(buf, idx, *valout, &actual) != 0) {
        free(*valout); *valout = NULL;
        return send_reply_error(req_id, "bad_binary");
    }
    *lenout = bin_size;
    return 0;
}

// Decode a key_selector tuple {KeyBinary, Atom} or {KeyBinary, Atom, Offset}.
// Returns 0 on success; on error sends a reply and returns non-zero.
// Caller must free *key_out on success.
static int decode_key_selector(const char *buf, int *idx, long req_id,
                                uint8_t **key_out, int *klen_out,
                                fdb_bool_t *or_equal_out, int *offset_out) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || (arity != 2 && arity != 3))
        return send_reply_error(req_id, "bad_key_selector");
    int r = decode_binary(buf, idx, req_id, key_out, klen_out);
    if (r != 0) return r;
    int or_equal = 0, offset = 0;
    int type = 0, sz = 0;
    if (ei_get_type(buf, idx, &type, &sz) != 0) {
        free(*key_out); *key_out = NULL;
        return send_reply_error(req_id, "bad_ks_cmp");
    }
    if (type == ERL_ATOM_EXT || type == ERL_SMALL_ATOM_EXT ||
        type == ERL_ATOM_UTF8_EXT || type == ERL_SMALL_ATOM_UTF8_EXT) {
        char atom[MAXATOMLEN + 1] = {0};
        if (ei_decode_atom(buf, idx, atom) != 0) {
            free(*key_out); *key_out = NULL;
            return send_reply_error(req_id, "bad_ks_atom");
        }
        if      (strcmp(atom, "lt")   == 0) { or_equal = 0; offset = 0; }
        else if (strcmp(atom, "lteq") == 0) { or_equal = 1; offset = 0; }
        else if (strcmp(atom, "gt")   == 0) { or_equal = 1; offset = 1; }
        else if (strcmp(atom, "gteq") == 0) { or_equal = 0; offset = 1; }
        else if (strcmp(atom, "true") == 0) { or_equal = 1; offset = 0; }
        else if (strcmp(atom, "false")== 0) { or_equal = 0; offset = 0; }
        else { free(*key_out); *key_out = NULL; return send_reply_error(req_id, "unknown_ks_cmp"); }
    } else {
        long oe = 0;
        if (ei_decode_long(buf, idx, &oe) != 0) {
            free(*key_out); *key_out = NULL;
            return send_reply_error(req_id, "bad_ks_oe");
        }
        or_equal = oe ? 1 : 0;
    }
    if (arity == 3) {
        long add_off = 0;
        if (ei_decode_long(buf, idx, &add_off) != 0) {
            free(*key_out); *key_out = NULL;
            return send_reply_error(req_id, "bad_ks_offset");
        }
        offset += (int)add_off;
    }
    *or_equal_out = (fdb_bool_t)or_equal;
    *offset_out   = offset;
    return 0;
}

// Decode a streaming mode atom (want_all|iterator|exact|small|medium|large|serial).
// Returns 0 on success; on error sends a reply and returns non-zero.
static int decode_streaming_mode(const char *buf, int *idx, long req_id,
                                  FDBStreamingMode *mode_out) {
    int type = 0, sz = 0;
    if (ei_get_type(buf, idx, &type, &sz) != 0)
        return send_reply_error(req_id, "bad_streaming_mode");
    if (type == ERL_ATOM_EXT || type == ERL_SMALL_ATOM_EXT ||
        type == ERL_ATOM_UTF8_EXT || type == ERL_SMALL_ATOM_UTF8_EXT) {
        char atom[MAXATOMLEN + 1] = {0};
        if (ei_decode_atom(buf, idx, atom) != 0)
            return send_reply_error(req_id, "bad_streaming_mode_atom");
        if      (strcmp(atom, "want_all") == 0) *mode_out = FDB_STREAMING_MODE_WANT_ALL;
        else if (strcmp(atom, "iterator") == 0) *mode_out = FDB_STREAMING_MODE_ITERATOR;
        else if (strcmp(atom, "exact")    == 0) *mode_out = FDB_STREAMING_MODE_EXACT;
        else if (strcmp(atom, "small")    == 0) *mode_out = FDB_STREAMING_MODE_SMALL;
        else if (strcmp(atom, "medium")   == 0) *mode_out = FDB_STREAMING_MODE_MEDIUM;
        else if (strcmp(atom, "large")    == 0) *mode_out = FDB_STREAMING_MODE_LARGE;
        else if (strcmp(atom, "serial")   == 0) *mode_out = FDB_STREAMING_MODE_SERIAL;
        else return send_reply_error(req_id, "unknown_streaming_mode");
    } else {
        long m = 0;
        if (ei_decode_long(buf, idx, &m) != 0)
            return send_reply_error(req_id, "bad_streaming_mode_int");
        *mode_out = (FDBStreamingMode)m;
    }
    return 0;
}

// ---------------------------------------------------------------------------
// Sync transaction dispatchers
// ---------------------------------------------------------------------------

static int dispatch_transaction_set(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *key = NULL, *val = NULL;
    int klen = 0, vlen = 0;
    if ((r = decode_binary(buf, idx, req_id, &key, &klen)) != 0) return r;
    if ((r = decode_binary(buf, idx, req_id, &val, &vlen)) != 0) { free(key); return r; }
    if (!tx->writes_allowed) { free(key); free(val); return send_reply_error(req_id, "writes_not_allowed"); }
    fdb_transaction_set(tx->transaction, key, klen, val, vlen);
    free(key); free(val);
    tx->read_only = 0;
    return send_reply_ok(req_id);
}

static int dispatch_transaction_clear(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 2)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *key = NULL; int klen = 0;
    if ((r = decode_binary(buf, idx, req_id, &key, &klen)) != 0) return r;
    if (!tx->writes_allowed) { free(key); return send_reply_error(req_id, "writes_not_allowed"); }
    fdb_transaction_clear(tx->transaction, key, klen);
    free(key);
    tx->read_only = 0;
    return send_reply_ok(req_id);
}

static int dispatch_transaction_clear_range(long req_id, const char *buf,
                                            int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *skey = NULL, *ekey = NULL; int slen = 0, elen = 0;
    if ((r = decode_binary(buf, idx, req_id, &skey, &slen)) != 0) return r;
    if ((r = decode_binary(buf, idx, req_id, &ekey, &elen)) != 0) { free(skey); return r; }
    if (!tx->writes_allowed) { free(skey); free(ekey); return send_reply_error(req_id, "writes_not_allowed"); }
    fdb_transaction_clear_range(tx->transaction, skey, slen, ekey, elen);
    free(skey); free(ekey);
    tx->read_only = 0;
    return send_reply_ok(req_id);
}

static int dispatch_transaction_atomic_op(long req_id, const char *buf,
                                          int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 4)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *key = NULL, *param = NULL; int klen = 0, plen = 0;
    if ((r = decode_binary(buf, idx, req_id, &key, &klen)) != 0) return r;
    if ((r = decode_binary(buf, idx, req_id, &param, &plen)) != 0) { free(key); return r; }
    char mtype_name[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom(buf, idx, mtype_name) != 0) {
        free(key); free(param);
        return send_reply_error(req_id, "bad_mutation_type");
    }
    FDBMutationType mtype;
    if (!lookup_mutation_type(mtype_name, &mtype)) {
        free(key); free(param);
        return send_reply_error(req_id, "unknown_mutation_type");
    }
    if (!tx->writes_allowed) { free(key); free(param); return send_reply_error(req_id, "writes_not_allowed"); }
    fdb_transaction_atomic_op(tx->transaction, key, klen, param, plen, mtype);
    free(key); free(param);
    tx->read_only = 0;
    return send_reply_ok(req_id);
}

static int dispatch_transaction_set_option(long req_id, const char *buf,
                                           int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    char opt_name[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom(buf, idx, opt_name) != 0)
        return send_reply_error(req_id, "bad_option_name");
    // allow_writes and disallow_writes are local flags, not passed to FDB.
    if (strcmp(opt_name, "allow_writes") == 0) {
        tx->writes_allowed = 1;
        // Consume the (empty) value binary that the Erlang side always sends.
        uint8_t *val = NULL; int vlen = 0;
        decode_binary(buf, idx, req_id, &val, &vlen); free(val);
        return send_reply_ok(req_id);
    }
    if (strcmp(opt_name, "disallow_writes") == 0) {
        if (!tx->read_only) return send_reply_error(req_id, "writes_already_done");
        tx->writes_allowed = 0;
        uint8_t *val = NULL; int vlen = 0;
        decode_binary(buf, idx, req_id, &val, &vlen); free(val);
        return send_reply_ok(req_id);
    }
    uint8_t *val = NULL; int vlen = 0;
    if ((r = decode_binary(buf, idx, req_id, &val, &vlen)) != 0) return r;
    FDBTransactionOption fdb_opt;
    if (lookup_tx_option(opt_name, &fdb_opt)) {
        fdb_error_t err = fdb_transaction_set_option(tx->transaction, fdb_opt,
                                                      val, vlen);
        free(val);
        if (err != 0) return send_reply_fdb_error(req_id, err);
    } else {
        free(val); // unknown options silently ignored (forward compatibility)
    }
    return send_reply_ok(req_id);
}

static int dispatch_transaction_set_read_version(long req_id, const char *buf,
                                                 int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 2)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    long version = 0;
    if (ei_decode_long(buf, idx, &version) != 0)
        return send_reply_error(req_id, "bad_version");
    fdb_transaction_set_read_version(tx->transaction, (int64_t)version);
    return send_reply_ok(req_id);
}

static int dispatch_transaction_reset(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    fdb_transaction_reset(tx->transaction);
    tx->txid      = 0;
    tx->read_only = 1;
    tx->has_watches = 0;
    return send_reply_ok(req_id);
}

static int dispatch_transaction_cancel(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    fdb_transaction_cancel(tx->transaction);
    return send_reply_ok(req_id);
}

static int dispatch_transaction_add_conflict_range(long req_id, const char *buf,
                                                   int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 4)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *skey = NULL, *ekey = NULL; int slen = 0, elen = 0;
    if ((r = decode_binary(buf, idx, req_id, &skey, &slen)) != 0) return r;
    if ((r = decode_binary(buf, idx, req_id, &ekey, &elen)) != 0) { free(skey); return r; }
    char rtype_name[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom(buf, idx, rtype_name) != 0) {
        free(skey); free(ekey);
        return send_reply_error(req_id, "bad_conflict_type");
    }
    FDBConflictRangeType rtype;
    if (strcmp(rtype_name, "read") == 0) rtype = FDB_CONFLICT_RANGE_TYPE_READ;
    else if (strcmp(rtype_name, "write") == 0) rtype = FDB_CONFLICT_RANGE_TYPE_WRITE;
    else { free(skey); free(ekey); return send_reply_error(req_id, "unknown_conflict_type"); }
    fdb_error_t err = fdb_transaction_add_conflict_range(tx->transaction,
                                                          skey, slen,
                                                          ekey, elen, rtype);
    free(skey); free(ekey);
    if (err != 0) return send_reply_fdb_error(req_id, err);
    if (rtype == FDB_CONFLICT_RANGE_TYPE_WRITE) tx->read_only = 0;
    return send_reply_ok(req_id);
}

static int dispatch_transaction_get_committed_version(long req_id,
                                                      const char *buf,
                                                      int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    int64_t version = 0;
    fdb_error_t err = fdb_transaction_get_committed_version(tx->transaction,
                                                             &version);
    if (err != 0) return send_reply_fdb_error(req_id, err);
    return send_reply_ok_long(req_id, (long)version);
}

static int dispatch_transaction_get_next_tx_id(long req_id, const char *buf,
                                               int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    if (tx->txid > 65535) return send_reply_error(req_id, "txid_overflow");
    uint32_t id = tx->txid++;
    return send_reply_ok_long(req_id, (long)id);
}

static int dispatch_transaction_is_read_only(long req_id, const char *buf,
                                             int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    return send_reply_ok_long(req_id, (long)tx->read_only);
}

static int dispatch_transaction_has_watches(long req_id, const char *buf,
                                            int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    return send_reply_ok_long(req_id, (long)tx->has_watches);
}

static int dispatch_transaction_get_writes_allowed(long req_id, const char *buf,
                                                   int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    return send_reply_ok_long(req_id, (long)tx->writes_allowed);
}

// ---------------------------------------------------------------------------
// Database option lookup
// ---------------------------------------------------------------------------

typedef struct { const char *name; FDBDatabaseOption opt; } db_option_entry;
static const db_option_entry db_option_map[] = {
    {"location_cache_size",                    FDB_DB_OPTION_LOCATION_CACHE_SIZE},
    {"max_watches",                            FDB_DB_OPTION_MAX_WATCHES},
    {"machine_id",                             FDB_DB_OPTION_MACHINE_ID},
    {"datacenter_id",                          FDB_DB_OPTION_DATACENTER_ID},
    {"snapshot_ryw_enable",                    FDB_DB_OPTION_SNAPSHOT_RYW_ENABLE},
    {"snapshot_ryw_disable",                   FDB_DB_OPTION_SNAPSHOT_RYW_DISABLE},
    {"transaction_logging_max_field_length",   FDB_DB_OPTION_TRANSACTION_LOGGING_MAX_FIELD_LENGTH},
    {"transaction_timeout",                    FDB_DB_OPTION_TRANSACTION_TIMEOUT},
    {"transaction_retry_limit",                FDB_DB_OPTION_TRANSACTION_RETRY_LIMIT},
    {"transaction_max_retry_delay",            FDB_DB_OPTION_TRANSACTION_MAX_RETRY_DELAY},
    {"transaction_size_limit",                 FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT},
    {"transaction_causal_read_risky",          FDB_DB_OPTION_TRANSACTION_CAUSAL_READ_RISKY},
    {"transaction_include_port_in_address",    FDB_DB_OPTION_TRANSACTION_INCLUDE_PORT_IN_ADDRESS},
    /* Aliases matching erlfdb:set_option/2,3 call sites */
    {"size_limit",                             FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT},
    {"timeout",                                FDB_DB_OPTION_TRANSACTION_TIMEOUT},
    {"retry_limit",                            FDB_DB_OPTION_TRANSACTION_RETRY_LIMIT},
    {"max_retry_delay",                        FDB_DB_OPTION_TRANSACTION_MAX_RETRY_DELAY},
    {"causal_read_risky",                      FDB_DB_OPTION_TRANSACTION_CAUSAL_READ_RISKY},
};
static int lookup_db_option(const char *name, FDBDatabaseOption *out) {
    size_t n = sizeof(db_option_map) / sizeof(db_option_map[0]);
    for (size_t i = 0; i < n; i++) {
        if (strcmp(name, db_option_map[i].name) == 0) { *out = db_option_map[i].opt; return 1; }
    }
    return 0;
}

// ---------------------------------------------------------------------------
// Database operation dispatchers
// ---------------------------------------------------------------------------

static int dispatch_database_set_option(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");
    erlang_ref db_ref;
    if (ei_decode_ref(buf, idx, &db_ref) != 0)
        return send_reply_error(req_id, "bad_db_ref");
    FDBDatabase *db = (FDBDatabase *)erlfdb_registry_get(&db_registry, &db_ref);
    if (!db) return send_reply_error(req_id, "unknown_db");
    char opt_name[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom(buf, idx, opt_name) != 0)
        return send_reply_error(req_id, "bad_option_name");
    uint8_t *val = NULL; int vlen = 0;
    int r = decode_binary(buf, idx, req_id, &val, &vlen);
    if (r != 0) return r;
    FDBDatabaseOption fdb_opt;
    fdb_error_t err = 0;
    if (lookup_db_option(opt_name, &fdb_opt))
        err = fdb_database_set_option(db, fdb_opt, val, vlen);
    free(val);
    if (err != 0) return send_reply_fdb_error(req_id, err);
    return send_reply_ok(req_id);
}

static int dispatch_database_open_tenant(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");
    erlang_ref db_ref;
    if (ei_decode_ref(buf, idx, &db_ref) != 0)
        return send_reply_error(req_id, "bad_db_ref");
    FDBDatabase *db = (FDBDatabase *)erlfdb_registry_get(&db_registry, &db_ref);
    if (!db) return send_reply_error(req_id, "unknown_db");
    erlang_ref tenant_ref;
    if (ei_decode_ref(buf, idx, &tenant_ref) != 0)
        return send_reply_error(req_id, "bad_tenant_ref");
    uint8_t *name = NULL; int nlen = 0;
    int r = decode_binary(buf, idx, req_id, &name, &nlen);
    if (r != 0) return r;
    FDBTenant *tenant = NULL;
    fdb_error_t err = fdb_database_open_tenant(db, name, nlen, &tenant);
    free(name);
    if (err != 0) return send_reply_fdb_error(req_id, err);
    if (erlfdb_registry_put(&tenant_registry, &tenant_ref, tenant, NULL) != 0) {
        fdb_tenant_destroy(tenant);
        return send_reply_error(req_id, "registry_alloc_failed");
    }
    return send_reply_ok(req_id);
}

static int dispatch_tenant_create_transaction(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 2)
        return send_reply_error(req_id, "bad_args");
    erlang_ref tenant_ref;
    if (ei_decode_ref(buf, idx, &tenant_ref) != 0)
        return send_reply_error(req_id, "bad_tenant_ref");
    FDBTenant *tenant = (FDBTenant *)erlfdb_registry_get(&tenant_registry, &tenant_ref);
    if (!tenant) return send_reply_error(req_id, "unknown_tenant");
    erlang_ref tx_ref;
    if (ei_decode_ref(buf, idx, &tx_ref) != 0)
        return send_reply_error(req_id, "bad_tx_ref");
    FDBTransaction *transaction = NULL;
    fdb_error_t err = fdb_tenant_create_transaction(tenant, &transaction);
    if (err != 0) return send_reply_fdb_error(req_id, err);
    erlfdb_tx_t *tx = (erlfdb_tx_t *)malloc(sizeof(erlfdb_tx_t));
    if (!tx) { fdb_transaction_destroy(transaction); return -1; }
    tx->transaction    = transaction;
    tx->txid           = 0;
    tx->read_only      = 1;
    tx->writes_allowed = 1;
    tx->has_watches    = 0;
    if (erlfdb_registry_put(&tx_registry, &tx_ref, tx, NULL) != 0) {
        erlfdb_tx_destroy_cb(tx);
        return send_reply_error(req_id, "registry_alloc_failed");
    }
    return send_reply_ok(req_id);
}

static int dispatch_database_get_main_thread_busyness(long req_id,
                                                       const char *buf,
                                                       int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlang_ref db_ref;
    if (ei_decode_ref(buf, idx, &db_ref) != 0)
        return send_reply_error(req_id, "bad_db_ref");
    FDBDatabase *db = (FDBDatabase *)erlfdb_registry_get(&db_registry, &db_ref);
    if (!db) return send_reply_error(req_id, "unknown_db");
    double busyness = fdb_database_get_main_thread_busyness(db);
    return send_reply_ok_double(req_id, busyness);
}

static int dispatch_database_get_client_status(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");
    erlang_ref db_ref;
    if (ei_decode_ref(buf, idx, &db_ref) != 0)
        return send_reply_error(req_id, "bad_db_ref");
    FDBDatabase *db = (FDBDatabase *)erlfdb_registry_get(&db_registry, &db_ref);
    if (!db) return send_reply_error(req_id, "unknown_db");
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0)
        return send_reply_error(req_id, "bad_owner_pid");
#if FDB_API_VERSION >= 730
    FDBFuture *future = fdb_database_get_client_status(db);
    return register_future(req_id, future, ERLFDB_FUT_VALUE, &fut_ref, &owner_pid, 0, NULL);
#else
    return send_reply_error(req_id, "not_supported");
#endif
}

// ---------------------------------------------------------------------------
// Async transaction dispatchers (futures)
// ---------------------------------------------------------------------------

static int dispatch_transaction_get_read_version(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0)
        return send_reply_error(req_id, "bad_owner_pid");
    FDBFuture *future = fdb_transaction_get_read_version(tx->transaction);
    return register_future(req_id, future, ERLFDB_FUT_INT64, &fut_ref, &owner_pid, 0, NULL);
}

// Args: {TxRef, KeySelector, Snapshot, FutRef, OwnerPid}
static int dispatch_transaction_get_key(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 5)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *key = NULL; int klen = 0; fdb_bool_t or_equal = 0; int offset = 0;
    if ((r = decode_key_selector(buf, idx, req_id, &key, &klen, &or_equal, &offset)) != 0)
        return r;
    char snap_atom[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom(buf, idx, snap_atom) != 0) {
        free(key); return send_reply_error(req_id, "bad_snapshot");
    }
    int snapshot = strcmp(snap_atom, "true") == 0;
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0) {
        free(key); return send_reply_error(req_id, "bad_fut_ref");
    }
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0) {
        free(key); return send_reply_error(req_id, "bad_owner_pid");
    }
    FDBFuture *future = fdb_transaction_get_key(tx->transaction, key, klen,
                                                 or_equal, offset, snapshot);
    free(key);
    return register_future(req_id, future, ERLFDB_FUT_KEY, &fut_ref, &owner_pid, 0, NULL);
}

// Args: {TxRef, StartKey, EndKey, FutRef, OwnerPid}
static int dispatch_transaction_get_estimated_range_size(long req_id,
                                                          const char *buf,
                                                          int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 5)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *skey = NULL, *ekey = NULL; int slen = 0, elen = 0;
    if ((r = decode_binary(buf, idx, req_id, &skey, &slen)) != 0) return r;
    if ((r = decode_binary(buf, idx, req_id, &ekey, &elen)) != 0) { free(skey); return r; }
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_fut_ref");
    }
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_owner_pid");
    }
    FDBFuture *future = fdb_transaction_get_estimated_range_size_bytes(
        tx->transaction, skey, slen, ekey, elen);
    free(skey); free(ekey);
    return register_future(req_id, future, ERLFDB_FUT_INT64, &fut_ref, &owner_pid, 0, NULL);
}

// Args: {TxRef, Key, FutRef, OwnerPid}
static int dispatch_transaction_get_addresses_for_key(long req_id,
                                                       const char *buf,
                                                       int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 4)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *key = NULL; int klen = 0;
    if ((r = decode_binary(buf, idx, req_id, &key, &klen)) != 0) return r;
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0) {
        free(key); return send_reply_error(req_id, "bad_fut_ref");
    }
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0) {
        free(key); return send_reply_error(req_id, "bad_owner_pid");
    }
    FDBFuture *future = fdb_transaction_get_addresses_for_key(tx->transaction, key, klen);
    free(key);
    return register_future(req_id, future, ERLFDB_FUT_STRING_ARRAY,
                           &fut_ref, &owner_pid, 0, NULL);
}

// Args: {TxRef, StartKS, EndKS, Limit, TargetBytes, Mode, Iteration, Snapshot, Reverse, FutRef, OwnerPid}
static int dispatch_transaction_get_range(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 11)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *skey = NULL, *ekey = NULL; int slen = 0, elen = 0;
    fdb_bool_t sor_eq = 0, eor_eq = 0; int soff = 0, eoff = 0;
    if ((r = decode_key_selector(buf, idx, req_id, &skey, &slen, &sor_eq, &soff)) != 0)
        return r;
    if ((r = decode_key_selector(buf, idx, req_id, &ekey, &elen, &eor_eq, &eoff)) != 0) {
        free(skey); return r;
    }
    long limit = 0, target_bytes = 0, iteration = 0;
    if (ei_decode_long(buf, idx, &limit) != 0 ||
        ei_decode_long(buf, idx, &target_bytes) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_limit");
    }
    FDBStreamingMode mode;
    if ((r = decode_streaming_mode(buf, idx, req_id, &mode)) != 0) {
        free(skey); free(ekey); return r;
    }
    if (ei_decode_long(buf, idx, &iteration) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_iteration");
    }
    char snap_atom[MAXATOMLEN + 1] = {0}, rev_atom[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom(buf, idx, snap_atom) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_snapshot");
    }
    long reverse = 0;
    if (ei_decode_long(buf, idx, &reverse) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_reverse");
    }
    (void)rev_atom;
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_fut_ref");
    }
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_owner_pid");
    }
    int snapshot = strcmp(snap_atom, "true") == 0;
    FDBFuture *future = fdb_transaction_get_range(
        tx->transaction,
        skey, slen, sor_eq, soff,
        ekey, elen, eor_eq, eoff,
        (int)limit, (int)target_bytes, mode, (int)iteration, snapshot, (int)reverse);
    free(skey); free(ekey);
    return register_future(req_id, future, ERLFDB_FUT_KEYVALUE_ARRAY,
                           &fut_ref, &owner_pid, 0, NULL);
}

// Args: {TxRef, StartKey, EndKey, ChunkSize, FutRef, OwnerPid}
static int dispatch_transaction_get_range_split_points(long req_id,
                                                        const char *buf,
                                                        int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 6)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *skey = NULL, *ekey = NULL; int slen = 0, elen = 0;
    if ((r = decode_binary(buf, idx, req_id, &skey, &slen)) != 0) return r;
    if ((r = decode_binary(buf, idx, req_id, &ekey, &elen)) != 0) { free(skey); return r; }
    long chunk_size = 0;
    if (ei_decode_long(buf, idx, &chunk_size) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_chunk_size");
    }
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_fut_ref");
    }
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0) {
        free(skey); free(ekey); return send_reply_error(req_id, "bad_owner_pid");
    }
    FDBFuture *future = fdb_transaction_get_range_split_points(
        tx->transaction, skey, slen, ekey, elen, (int64_t)chunk_size);
    free(skey); free(ekey);
    return register_future(req_id, future, ERLFDB_FUT_KEY_ARRAY,
                           &fut_ref, &owner_pid, 0, NULL);
}

// Args: {TxRef, StartKS, EndKS, Mapper, Limit, TargetBytes, Mode, Iteration, Snapshot, Reverse, FutRef, OwnerPid}
static int dispatch_transaction_get_mapped_range(long req_id, const char *buf, int *idx) {
#if FDB_API_VERSION >= 730
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 12)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *skey = NULL, *ekey = NULL; int slen = 0, elen = 0;
    fdb_bool_t sor_eq = 0, eor_eq = 0; int soff = 0, eoff = 0;
    if ((r = decode_key_selector(buf, idx, req_id, &skey, &slen, &sor_eq, &soff)) != 0)
        return r;
    if ((r = decode_key_selector(buf, idx, req_id, &ekey, &elen, &eor_eq, &eoff)) != 0) {
        free(skey); return r;
    }
    uint8_t *mapper = NULL; int mlen = 0;
    if ((r = decode_binary(buf, idx, req_id, &mapper, &mlen)) != 0) {
        free(skey); free(ekey); return r;
    }
    long limit = 0, target_bytes = 0, iteration = 0;
    if (ei_decode_long(buf, idx, &limit) != 0 ||
        ei_decode_long(buf, idx, &target_bytes) != 0) {
        free(skey); free(ekey); free(mapper);
        return send_reply_error(req_id, "bad_limit");
    }
    FDBStreamingMode mode;
    if ((r = decode_streaming_mode(buf, idx, req_id, &mode)) != 0) {
        free(skey); free(ekey); free(mapper); return r;
    }
    if (ei_decode_long(buf, idx, &iteration) != 0) {
        free(skey); free(ekey); free(mapper);
        return send_reply_error(req_id, "bad_iteration");
    }
    char snap_atom[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom(buf, idx, snap_atom) != 0) {
        free(skey); free(ekey); free(mapper);
        return send_reply_error(req_id, "bad_snapshot");
    }
    long reverse = 0;
    if (ei_decode_long(buf, idx, &reverse) != 0) {
        free(skey); free(ekey); free(mapper);
        return send_reply_error(req_id, "bad_reverse");
    }
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0) {
        free(skey); free(ekey); free(mapper);
        return send_reply_error(req_id, "bad_fut_ref");
    }
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0) {
        free(skey); free(ekey); free(mapper);
        return send_reply_error(req_id, "bad_owner_pid");
    }
    int snapshot = strcmp(snap_atom, "true") == 0;
    FDBFuture *future = fdb_transaction_get_mapped_range(
        tx->transaction,
        skey, slen, sor_eq, soff,
        ekey, elen, eor_eq, eoff,
        mapper, mlen, (int)limit, (int)target_bytes, mode,
        (int)iteration, snapshot, (int)reverse);
    free(skey); free(ekey); free(mapper);
    return register_future(req_id, future, ERLFDB_FUT_MAPPEDKEYVALUE_ARRAY,
                           &fut_ref, &owner_pid, 0, NULL);
#else
    (void)buf; (void)idx;
    return send_reply_error(req_id, "not_supported");
#endif
}

// Args: {TxRef, FutRef, OwnerPid}  — not tx-scoped; fires after commit
static int dispatch_transaction_get_versionstamp(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0)
        return send_reply_error(req_id, "bad_owner_pid");
    FDBFuture *future = fdb_transaction_get_versionstamp(tx->transaction);
    return register_future(req_id, future, ERLFDB_FUT_KEY, &fut_ref, &owner_pid, 0, NULL);
}

// Args: {TxRef, FutRef, OwnerPid}
static int dispatch_transaction_get_approximate_size(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 3)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0)
        return send_reply_error(req_id, "bad_owner_pid");
    FDBFuture *future = fdb_transaction_get_approximate_size(tx->transaction);
    return register_future(req_id, future, ERLFDB_FUT_INT64, &fut_ref, &owner_pid, 0, NULL);
}

// Args: {TxRef, ErrorCode, FutRef, OwnerPid}
static int dispatch_transaction_on_error(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 4)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    long err_code = 0;
    if (ei_decode_long(buf, idx, &err_code) != 0)
        return send_reply_error(req_id, "bad_error_code");
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0)
        return send_reply_error(req_id, "bad_owner_pid");
    FDBFuture *future = fdb_transaction_on_error(tx->transaction, (fdb_error_t)err_code);
    return register_future(req_id, future, ERLFDB_FUT_VOID, &fut_ref, &owner_pid, 0, NULL);
}

// Args: {TxRef, Key, FutRef, OwnerPid}  — not tx-scoped; fires when key changes
static int dispatch_transaction_watch(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 4)
        return send_reply_error(req_id, "bad_args");
    erlfdb_tx_t *tx = NULL;
    int r = decode_tx(buf, idx, req_id, &tx);
    if (r != 0 || !tx) return r;
    uint8_t *key = NULL; int klen = 0;
    if ((r = decode_binary(buf, idx, req_id, &key, &klen)) != 0) return r;
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0) {
        free(key); return send_reply_error(req_id, "bad_fut_ref");
    }
    erlang_pid owner_pid;
    if (ei_decode_pid(buf, idx, &owner_pid) != 0) {
        free(key); return send_reply_error(req_id, "bad_owner_pid");
    }
    if (!tx->writes_allowed) {
        free(key); return send_reply_error(req_id, "writes_not_allowed");
    }
    FDBFuture *future = fdb_transaction_watch(tx->transaction, key, klen);
    free(key);
    tx->has_watches = 1;
    return register_future(req_id, future, ERLFDB_FUT_VOID, &fut_ref, &owner_pid, 0, NULL);
}

// ---------------------------------------------------------------------------
// Future management dispatchers
// ---------------------------------------------------------------------------

static int dispatch_future_cancel(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    erlfdb_future_t *ft =
        (erlfdb_future_t *)erlfdb_registry_get(&future_registry, &fut_ref);
    if (!ft) return send_reply_error(req_id, "unknown_future");
    fdb_future_cancel(ft->future);
    return send_reply_ok(req_id);
}

static int dispatch_future_silence(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    (void)fut_ref;
    /* No-op: port futures are fire-once. flush_future_message drains the mailbox. */
    return send_reply_ok(req_id);
}

static int dispatch_future_is_ready(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    erlfdb_future_t *ft =
        (erlfdb_future_t *)erlfdb_registry_get(&future_registry, &fut_ref);
    if (!ft) return send_reply_ok_long(req_id, 0L);
    long ready = fdb_future_is_ready(ft->future) ? 1L : 0L;
    return send_reply_ok_long(req_id, ready);
}

static int dispatch_future_get_error(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    erlang_ref fut_ref;
    if (ei_decode_ref(buf, idx, &fut_ref) != 0)
        return send_reply_error(req_id, "bad_fut_ref");
    erlfdb_future_t *ft =
        (erlfdb_future_t *)erlfdb_registry_get(&future_registry, &fut_ref);
    if (!ft) return send_reply_error(req_id, "unknown_future");
    fdb_error_t err = fdb_future_get_error(ft->future);
    return (err != 0) ? send_reply_fdb_error(req_id, err) : send_reply_ok(req_id);
}

// ---------------------------------------------------------------------------
// Misc: get_error, error_predicate
// ---------------------------------------------------------------------------

static int dispatch_get_error(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 1)
        return send_reply_error(req_id, "bad_args");
    long code = 0;
    if (ei_decode_long(buf, idx, &code) != 0)
        return send_reply_error(req_id, "bad_code");
    const char *msg = fdb_get_error((fdb_error_t)code);
    return send_reply_ok_binary(req_id, (const uint8_t *)msg, (int)strlen(msg));
}

static int dispatch_error_predicate(long req_id, const char *buf, int *idx) {
    int arity = 0;
    if (ei_decode_tuple_header(buf, idx, &arity) != 0 || arity != 2)
        return send_reply_error(req_id, "bad_args");
    char pred_name[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom(buf, idx, pred_name) != 0)
        return send_reply_error(req_id, "bad_predicate");
    long code = 0;
    if (ei_decode_long(buf, idx, &code) != 0)
        return send_reply_error(req_id, "bad_code");
    FDBErrorPredicate pred;
    if      (strcmp(pred_name, "retryable")              == 0) pred = FDB_ERROR_PREDICATE_RETRYABLE;
    else if (strcmp(pred_name, "maybe_committed")        == 0) pred = FDB_ERROR_PREDICATE_MAYBE_COMMITTED;
    else if (strcmp(pred_name, "retryable_not_committed")== 0) pred = FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED;
    else return send_reply_error(req_id, "unknown_predicate");
    int result = fdb_error_predicate(pred, (fdb_error_t)code);
    return send_reply_ok_long(req_id, (long)result);
}

// ---------------------------------------------------------------------------
// Request dispatcher
// ---------------------------------------------------------------------------

static int handle_request(const uint8_t *buf, uint32_t len) {
    (void)len;
    int idx = 0;
    int version = 0;
    if (ei_decode_version((const char *)buf, &idx, &version) != 0) return -1;

    int arity = 0;
    if (ei_decode_tuple_header((const char *)buf, &idx, &arity) != 0) return -1;
    if (arity != 4) return -1;

    char tag[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom((const char *)buf, &idx, tag) != 0) return -1;
    if (strcmp(tag, "req") != 0) return -1;

    long req_id = 0;
    if (ei_decode_long((const char *)buf, &idx, &req_id) != 0) return -1;

    char op[MAXATOMLEN + 1] = {0};
    if (ei_decode_atom((const char *)buf, &idx, op) != 0) return -1;

    if (strcmp(op, "init") == 0) {
        return dispatch_init(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "get_max_api_version") == 0) {
        return send_reply_ok_long(req_id, (long)fdb_get_max_api_version());
    } else if (strcmp(op, "create_database") == 0) {
        return dispatch_create_database(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "database_create_transaction") == 0) {
        return dispatch_database_create_transaction(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_set") == 0) {
        return dispatch_transaction_set(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_clear") == 0) {
        return dispatch_transaction_clear(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_clear_range") == 0) {
        return dispatch_transaction_clear_range(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_atomic_op") == 0) {
        return dispatch_transaction_atomic_op(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_set_option") == 0) {
        return dispatch_transaction_set_option(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_set_read_version") == 0) {
        return dispatch_transaction_set_read_version(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_reset") == 0) {
        return dispatch_transaction_reset(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_cancel") == 0) {
        return dispatch_transaction_cancel(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_add_conflict_range") == 0) {
        return dispatch_transaction_add_conflict_range(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_committed_version") == 0) {
        return dispatch_transaction_get_committed_version(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_next_tx_id") == 0) {
        return dispatch_transaction_get_next_tx_id(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_is_read_only") == 0) {
        return dispatch_transaction_is_read_only(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_has_watches") == 0) {
        return dispatch_transaction_has_watches(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_writes_allowed") == 0) {
        return dispatch_transaction_get_writes_allowed(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get") == 0) {
        return dispatch_transaction_get(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_commit") == 0) {
        return dispatch_transaction_commit(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "future_get") == 0) {
        return dispatch_future_get(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "database_set_option") == 0) {
        return dispatch_database_set_option(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "database_open_tenant") == 0) {
        return dispatch_database_open_tenant(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "tenant_create_transaction") == 0) {
        return dispatch_tenant_create_transaction(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "database_get_main_thread_busyness") == 0) {
        return dispatch_database_get_main_thread_busyness(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "database_get_client_status") == 0) {
        return dispatch_database_get_client_status(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_read_version") == 0) {
        return dispatch_transaction_get_read_version(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_key") == 0) {
        return dispatch_transaction_get_key(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_estimated_range_size") == 0) {
        return dispatch_transaction_get_estimated_range_size(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_addresses_for_key") == 0) {
        return dispatch_transaction_get_addresses_for_key(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_range") == 0) {
        return dispatch_transaction_get_range(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_range_split_points") == 0) {
        return dispatch_transaction_get_range_split_points(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_mapped_range") == 0) {
        return dispatch_transaction_get_mapped_range(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_versionstamp") == 0) {
        return dispatch_transaction_get_versionstamp(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_get_approximate_size") == 0) {
        return dispatch_transaction_get_approximate_size(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_on_error") == 0) {
        return dispatch_transaction_on_error(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "transaction_watch") == 0) {
        return dispatch_transaction_watch(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "future_cancel") == 0) {
        return dispatch_future_cancel(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "future_silence") == 0) {
        return dispatch_future_silence(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "future_is_ready") == 0) {
        return dispatch_future_is_ready(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "future_get_error") == 0) {
        return dispatch_future_get_error(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "get_error") == 0) {
        return dispatch_get_error(req_id, (const char *)buf, &idx);
    } else if (strcmp(op, "error_predicate") == 0) {
        return dispatch_error_predicate(req_id, (const char *)buf, &idx);
    }

    return send_reply_error(req_id, "unknown_op");
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    if (send_hello() != 0) return 1;

    for (;;) {
        // Poll stdin (always) and the notify pipe (once the network is up).
        struct pollfd pfd[2];
        pfd[0].fd      = STDIN_FILENO;
        pfd[0].events  = POLLIN;
        pfd[0].revents = 0;
        int nfds = 1;
        if (g_network_running) {
            pfd[1].fd      = erlfdb_notify_rfd(&g_notify_queue);
            pfd[1].events  = POLLIN;
            pfd[1].revents = 0;
            nfds = 2;
        }

        int n = poll(pfd, nfds, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            return 1;
        }

        // Process future-ready notifications before new requests to minimise
        // the time between a future completing and BEAM receiving {ready,...}.
        if (nfds == 2 && (pfd[1].revents & POLLIN)) {
            erlfdb_notify_drain(&g_notify_queue, process_notification, NULL);
        }

        // Handle stdin: new request from BEAM, or EOF on port close / crash.
        if (pfd[0].revents & (POLLIN | POLLHUP)) {
            uint8_t *frame = NULL;
            uint32_t flen  = 0;
            int r = read_frame(&frame, &flen);
            if (r == 0) {
                // EOF: port closed (clean shutdown or hard crash).
                if (g_network_running) {
                    registries_destroy();
                    erlfdb_notify_destroy(&g_notify_queue);
                    fdb_stop_network();
                    pthread_join(g_network_tid, NULL);
                }
                return 0;
            }
            if (r < 0) return 1;

            int rc = handle_request(frame, flen);
            free(frame);
            if (rc != 0) return 1;
        }
    }
}
