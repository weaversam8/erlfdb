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

// Step 3: emit {hello, OsPid} on startup; dispatch {req, ReqId, Op, Args}
// frames to the appropriate handler, which replies {reply, ReqId, Result}.
// Supported ops so far: init, get_max_api_version.
//
// {packet, 4} framing: each message is a 4-byte big-endian length prefix
// followed by N bytes of ETF payload. The BEAM-side port option produces this
// framing automatically; we mirror it here on the worker's stdin/stdout.

#include <ei.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "fdb.h"

static int read_exact(int fd, void *buf, size_t want) {
    uint8_t *p = (uint8_t *)buf;
    size_t got = 0;
    while (got < want) {
        ssize_t n = read(fd, p + got, want - got);
        if (n == 0) {
            return 0; // EOF
        }
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

// Reads the next {packet, 4}-framed message from stdin into *out (caller
// frees). Returns 1 on success, 0 on clean EOF, -1 on error.
static int read_frame(uint8_t **out, uint32_t *outlen) {
    uint8_t hdr[4];
    int r = read_exact(STDIN_FILENO, hdr, 4);
    if (r <= 0) return r;
    uint32_t len = ((uint32_t)hdr[0] << 24) | ((uint32_t)hdr[1] << 16) |
                   ((uint32_t)hdr[2] << 8) | (uint32_t)hdr[3];
    uint8_t *buf = NULL;
    if (len > 0) {
        buf = (uint8_t *)malloc(len);
        if (buf == NULL) return -1;
        if (read_exact(STDIN_FILENO, buf, len) != 1) {
            free(buf);
            return -1;
        }
    }
    *out = buf;
    *outlen = len;
    return 1;
}

// Writes a {packet, 4}-framed message to stdout. Returns 0 on success, -1 on
// error.
static int write_frame(const uint8_t *buf, uint32_t len) {
    uint8_t hdr[4];
    hdr[0] = (uint8_t)((len >> 24) & 0xFF);
    hdr[1] = (uint8_t)((len >> 16) & 0xFF);
    hdr[2] = (uint8_t)((len >> 8) & 0xFF);
    hdr[3] = (uint8_t)(len & 0xFF);
    if (write_exact(STDOUT_FILENO, hdr, 4) != 0) return -1;
    if (len > 0) {
        if (write_exact(STDOUT_FILENO, buf, len) != 0) return -1;
    }
    return 0;
}

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
        return send_reply_ok(req_id);
    } else if (strcmp(op, "get_max_api_version") == 0) {
        return send_reply_ok_long(req_id, (long)fdb_get_max_api_version());
    }

    return send_reply_error(req_id, "unknown_op");
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    if (send_hello() != 0) return 1;

    for (;;) {
        uint8_t *frame = NULL;
        uint32_t flen = 0;
        int r = read_frame(&frame, &flen);
        if (r == 0) {
            // EOF, port closed
            return 0;
        }
        if (r < 0) return 1;

        int rc = handle_request(frame, flen);
        free(frame);
        if (rc != 0) return 1;
    }
}
