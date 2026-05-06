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

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "registry.h"

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

// FNV-1a hash over the numeric words of the ref. The node name and creation
// are constant within a single BEAM session, so hashing only the word array
// is sufficient for distribution; the full equality check still compares all
// fields.
static uint32_t ref_hash(const erlang_ref *ref) {
    uint32_t h = 2166136261u;
    for (unsigned i = 0; i < ref->len; i++) {
        uint32_t v = ref->n[i];
        h ^= (uint8_t)(v & 0xFF);
        h *= 16777619u;
        h ^= (uint8_t)((v >> 8) & 0xFF);
        h *= 16777619u;
        h ^= (uint8_t)((v >> 16) & 0xFF);
        h *= 16777619u;
        h ^= (uint8_t)((v >> 24) & 0xFF);
        h *= 16777619u;
    }
    return h & (ERLFDB_REGISTRY_BUCKETS - 1);
}

static int ref_equal(const erlang_ref *a, const erlang_ref *b) {
    if (a->len != b->len) return 0;
    if (a->creation != b->creation) return 0;
    for (unsigned i = 0; i < a->len; i++) {
        if (a->n[i] != b->n[i]) return 0;
    }
    return strcmp(a->node, b->node) == 0;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void erlfdb_registry_init(erlfdb_registry *r) {
    memset(r->buckets, 0, sizeof(r->buckets));
    pthread_rwlock_init(&r->lock, NULL);
    r->count = 0;
}

void erlfdb_registry_destroy(erlfdb_registry *r, void (*free_value)(void *)) {
    pthread_rwlock_wrlock(&r->lock);
    for (int i = 0; i < ERLFDB_REGISTRY_BUCKETS; i++) {
        erlfdb_registry_node *n = r->buckets[i];
        while (n) {
            erlfdb_registry_node *next = n->next;
            if (free_value) free_value(n->value);
            free(n);
            n = next;
        }
        r->buckets[i] = NULL;
    }
    r->count = 0;
    pthread_rwlock_unlock(&r->lock);
    pthread_rwlock_destroy(&r->lock);
}

int erlfdb_registry_put(erlfdb_registry *r, const erlang_ref *key, void *value,
                        void **old_out) {
    uint32_t bucket = ref_hash(key);
    if (old_out) *old_out = NULL;

    pthread_rwlock_wrlock(&r->lock);

    erlfdb_registry_node *n = r->buckets[bucket];
    while (n) {
        if (ref_equal(&n->key, key)) {
            // Replace existing entry.
            if (old_out) *old_out = n->value;
            n->value = value;
            pthread_rwlock_unlock(&r->lock);
            return 0;
        }
        n = n->next;
    }

    // New entry.
    erlfdb_registry_node *node = (erlfdb_registry_node *)malloc(sizeof(*node));
    if (!node) {
        pthread_rwlock_unlock(&r->lock);
        return -1;
    }
    node->key = *key;
    node->value = value;
    node->next = r->buckets[bucket];
    r->buckets[bucket] = node;
    r->count++;

    pthread_rwlock_unlock(&r->lock);
    return 0;
}

void *erlfdb_registry_get(erlfdb_registry *r, const erlang_ref *key) {
    uint32_t bucket = ref_hash(key);
    void *result = NULL;

    pthread_rwlock_rdlock(&r->lock);
    erlfdb_registry_node *n = r->buckets[bucket];
    while (n) {
        if (ref_equal(&n->key, key)) {
            result = n->value;
            break;
        }
        n = n->next;
    }
    pthread_rwlock_unlock(&r->lock);
    return result;
}

void *erlfdb_registry_remove(erlfdb_registry *r, const erlang_ref *key) {
    uint32_t bucket = ref_hash(key);
    void *result = NULL;

    pthread_rwlock_wrlock(&r->lock);
    erlfdb_registry_node **pp = &r->buckets[bucket];
    while (*pp) {
        erlfdb_registry_node *n = *pp;
        if (ref_equal(&n->key, key)) {
            *pp = n->next;
            result = n->value;
            free(n);
            r->count--;
            break;
        }
        pp = &n->next;
    }
    pthread_rwlock_unlock(&r->lock);
    return result;
}

size_t erlfdb_registry_count(erlfdb_registry *r) {
    pthread_rwlock_rdlock(&r->lock);
    size_t c = r->count;
    pthread_rwlock_unlock(&r->lock);
    return c;
}
