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

// Step 1 scaffold: read framed messages from stdin and echo them back.
// {packet, 4} framing: each message is a 4-byte big-endian length prefix
// followed by N bytes of payload. The BEAM-side port option handles the
// framing automatically; we mirror it here.
//
// Subsequent steps will replace this body with an ei-based dispatch loop.

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    for (;;) {
        uint8_t hdr[4];
        int r = read_exact(STDIN_FILENO, hdr, 4);
        if (r == 0) {
            // EOF - port closed
            return 0;
        }
        if (r < 0) {
            return 1;
        }

        uint32_t len = ((uint32_t)hdr[0] << 24) | ((uint32_t)hdr[1] << 16) |
                       ((uint32_t)hdr[2] << 8) | (uint32_t)hdr[3];

        uint8_t *payload = NULL;
        if (len > 0) {
            payload = malloc(len);
            if (!payload) {
                return 2;
            }
            if (read_exact(STDIN_FILENO, payload, len) != 1) {
                free(payload);
                return 1;
            }
        }

        if (write_exact(STDOUT_FILENO, hdr, 4) != 0) {
            free(payload);
            return 1;
        }
        if (len > 0) {
            if (write_exact(STDOUT_FILENO, payload, len) != 0) {
                free(payload);
                return 1;
            }
        }
        free(payload);
    }
}
