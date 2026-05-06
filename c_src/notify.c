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

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "notify.h"

int erlfdb_notify_init(erlfdb_notify_queue *q) {
    int fds[2];
    if (pipe(fds) != 0) return -1;

    // Set the read end non-blocking so drain() can consume all available
    // bytes with a simple loop.
    int flags = fcntl(fds[0], F_GETFL, 0);
    if (flags < 0 || fcntl(fds[0], F_SETFL, flags | O_NONBLOCK) != 0) {
        close(fds[0]);
        close(fds[1]);
        return -1;
    }

    q->rfd = fds[0];
    q->wfd = fds[1];
    q->head = NULL;
    q->tail = NULL;
    pthread_mutex_init(&q->lock, NULL);
    return 0;
}

void erlfdb_notify_signal(erlfdb_notify_queue *q, const erlang_ref *fut_ref) {
    erlfdb_notify_node *node =
        (erlfdb_notify_node *)malloc(sizeof(erlfdb_notify_node));
    if (!node) return; // best-effort; main thread will time out gracefully

    node->fut_ref = *fut_ref;
    node->next = NULL;

    pthread_mutex_lock(&q->lock);
    if (q->tail) {
        q->tail->next = node;
    } else {
        q->head = node;
    }
    q->tail = node;
    pthread_mutex_unlock(&q->lock);

    // Wake the main thread.  One byte per notification; drain() consumes all
    // outstanding bytes before processing the queue, so order doesn't matter.
    char byte = 1;
    ssize_t w;
    do {
        w = write(q->wfd, &byte, 1);
    } while (w < 0 && errno == EINTR);
}

void erlfdb_notify_drain(erlfdb_notify_queue *q,
                         void (*cb)(const erlang_ref *, void *), void *ctx) {
    // Consume all signal bytes from the read end first so we don't leave
    // stale bytes that would cause spurious poll() wakeups.  See the ordering
    // note in notify.h: drain bytes *before* processing the queue so that any
    // new signal arriving between the two steps is still picked up on the
    // next poll() iteration.
    char discard[256];
    ssize_t n;
    do {
        n = read(q->rfd, discard, sizeof(discard));
    } while (n > 0 || (n < 0 && errno == EINTR));
    // Loop exits on EAGAIN/EWOULDBLOCK (pipe empty) or unexpected error.

    // Detach the entire pending list atomically.
    pthread_mutex_lock(&q->lock);
    erlfdb_notify_node *list = q->head;
    q->head = NULL;
    q->tail = NULL;
    pthread_mutex_unlock(&q->lock);

    // Invoke the callback for each node and free it.
    while (list) {
        erlfdb_notify_node *next = list->next;
        cb(&list->fut_ref, ctx);
        free(list);
        list = next;
    }
}

int erlfdb_notify_rfd(const erlfdb_notify_queue *q) { return q->rfd; }

void erlfdb_notify_destroy(erlfdb_notify_queue *q) {
    if (q->rfd >= 0) close(q->rfd);
    if (q->wfd >= 0) close(q->wfd);
    q->rfd = -1;
    q->wfd = -1;

    // Free any unconsumed nodes.
    pthread_mutex_lock(&q->lock);
    erlfdb_notify_node *n = q->head;
    q->head = NULL;
    q->tail = NULL;
    pthread_mutex_unlock(&q->lock);
    while (n) {
        erlfdb_notify_node *next = n->next;
        free(n);
        n = next;
    }
    pthread_mutex_destroy(&q->lock);
}
