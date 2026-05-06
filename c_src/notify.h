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

// Thread-safe FIFO for delivering future-ready notifications from the FDB
// network thread to the worker's main I/O thread.
//
// The FDB network thread (running fdb_run_network) calls
// erlfdb_notify_signal() from a future-completion callback. The main thread
// calls erlfdb_notify_drain() after poll() wakes it on the read end of the
// pipe.  Each signal() writes one byte to the write fd; drain() consumes all
// bytes (non-blocking, O_NONBLOCK was set on init) then dequeues every
// pending node.

#ifndef ERLFDB_NOTIFY_H
#define ERLFDB_NOTIFY_H

#include <ei.h>
#include <pthread.h>

typedef struct erlfdb_notify_node {
    erlang_ref fut_ref;
    struct erlfdb_notify_node *next;
} erlfdb_notify_node;

typedef struct {
    pthread_mutex_t lock;
    erlfdb_notify_node *head;
    erlfdb_notify_node *tail;
    int rfd; // read fd — polled by main thread (O_NONBLOCK)
    int wfd; // write fd — written by FDB callback thread
} erlfdb_notify_queue;

// Create the pipe, set read end O_NONBLOCK, init mutex.  Returns 0 on
// success, -1 on error.
int erlfdb_notify_init(erlfdb_notify_queue *q);

// Called from the FDB callback thread when a future completes.  Appends a
// node carrying fut_ref to the queue then writes one byte to the write fd to
// wake the main thread's poll().
void erlfdb_notify_signal(erlfdb_notify_queue *q, const erlang_ref *fut_ref);

// Called from the main thread after poll() reports the read fd readable.
// Drains all bytes from rfd (non-blocking) then dequeues and calls
// cb(fut_ref, ctx) for each pending notification.
void erlfdb_notify_drain(erlfdb_notify_queue *q,
                         void (*cb)(const erlang_ref *, void *), void *ctx);

// Returns the read fd to pass to poll().
int erlfdb_notify_rfd(const erlfdb_notify_queue *q);

// Close both pipe fds, destroy mutex, free any unconsumed nodes.
void erlfdb_notify_destroy(erlfdb_notify_queue *q);

#endif // ERLFDB_NOTIFY_H
