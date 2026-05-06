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
    return send_reply_ok(req_id);
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
        uint8_t *frame = NULL;
        uint32_t flen = 0;
        int r = read_frame(&frame, &flen);
        if (r == 0) {
            // EOF: BEAM closed the port (clean shutdown or hard crash).
            // Stop the FDB network and join the thread before exiting so
            // libfdb_c can flush any in-flight work.
            if (g_network_running) {
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
