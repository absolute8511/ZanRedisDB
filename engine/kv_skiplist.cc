#include "kv_skiplist.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char* copyString(const char* str, size_t sz) {
  char* result = (char*)(malloc(sizeof(char) * sz));
  memcpy(result, str, sizeof(char) * sz);
  return result;
}

void kv_skiplist_remove_node(skiplist_raw* l, kv_node* entry) {
    // Detach `entry` from skiplist.
    skiplist_erase_node(l, &entry->snode);
    // Release `entry`, to free its memory.
    skiplist_release_node(&entry->snode);
    skiplist_wait_for_free(&entry->snode);
    // Free `entry` after it becomes safe.
    skiplist_free_node(&entry->snode);
    if (entry->key) {
        free(entry->key);
    }
    if (entry->value) {
        free(entry->value);
    }
    free(entry);
}

skiplist_raw* kv_skiplist_create() {
    skiplist_raw *slist;
    slist = (skiplist_raw*)malloc(sizeof(skiplist_raw));
    skiplist_init(slist, kv_cmp);
    return slist;
}

void kv_skiplist_destroy(skiplist_raw* slist) {
    // Iterate and free all nodes.
    skiplist_node* cursor = skiplist_begin(slist);
    while (cursor) {
        kv_node* entry = _get_entry(cursor, kv_node, snode);
        // Get next `cursor`.
        cursor = skiplist_next(slist, cursor);
        kv_skiplist_remove_node(slist, entry);
    }
    skiplist_free(slist);
}

kv_node* kv_skiplist_node_create(const char* key, size_t ksz, const char* value, size_t vsz) {
    kv_node *n;
    n = (kv_node*)malloc(sizeof(kv_node));
    skiplist_init_node(&n->snode);
    n->key = copyString(key, ksz);
    n->key_sz = ksz;
    n->value = copyString(value, vsz);
    n->value_sz = vsz;
    return n;
}

int kv_skiplist_insert(skiplist_raw* l, const char* key, size_t ksz, const char* value, size_t vsz) {
    kv_node* n = kv_skiplist_node_create(key, ksz, value, vsz);
    return kv_skiplist_insert_node(l, n);
}

int kv_skiplist_insert_node(skiplist_raw* l, kv_node* n) {
    return skiplist_insert(l, &n->snode);
}

char* kv_skiplist_get(skiplist_raw* l, const char* key, size_t ksz, size_t* vsz) {
    kv_node n;
    n.key = copyString(key, ksz);
    n.key_sz = ksz;
    skiplist_node* cur = skiplist_find(l, &n.snode);
    if (!cur) {
        return NULL;
    }
    kv_node* found = _get_entry(cur, kv_node, snode);
    *vsz = found->value_sz;
    char* v = copyString(found->value, found->value_sz);
    skiplist_release_node(cur);
    return v;
}

char* kv_skiplist_get_node_value(skiplist_node* n, size_t* sz) {
    kv_node* found = _get_entry(n, kv_node, snode);
    *sz = found->value_sz;
    char* v = copyString(found->value, found->value_sz);
    return v;
}

char* kv_skiplist_get_node_key(skiplist_node* n, size_t* sz) {
    kv_node* found = _get_entry(n, kv_node, snode);
    *sz = found->key_sz;
    char* v = copyString(found->key, found->key_sz);
    return v;
}

int kv_skiplist_del(skiplist_raw* l, const char* key, size_t ksz) {
    kv_node n;
    n.key = copyString(key, ksz);
    n.key_sz = ksz;
    skiplist_node* cur = skiplist_find(l, &n.snode);
    if (!cur) {
        return 0;
    }
    kv_node* found = _get_entry(cur, kv_node, snode);
    kv_skiplist_remove_node(l, found);
    return 1;
}

skiplist_node* kv_skiplist_find_ge(skiplist_raw* l, const char* key, size_t ksz) {
    kv_node n;
    n.key = copyString(key, ksz);
    n.key_sz = ksz;
    return skiplist_find_greater_or_equal(l, &n.snode);
}

skiplist_node* kv_skiplist_find_le(skiplist_raw* l, const char* key, size_t ksz) {
    kv_node n;
    n.key = copyString(key, ksz);
    n.key_sz = ksz;
    return skiplist_find_smaller_or_equal(l, &n.snode);
}
