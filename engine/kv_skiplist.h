#ifndef _KV_SKIPLIST_H
#define _KV_SKIPLIST_H (1)

#include "skiplist.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// Define a node that contains key and value pair.
typedef struct {
    // Metadata for skiplist node.
    skiplist_node snode;
    // My data here: {int, int} pair.
    char* key;
    size_t key_sz;
    char* value;
    size_t value_sz;
} kv_node;

static int kv_cmp(skiplist_node* a, skiplist_node* b, void* aux) {
    kv_node *aa, *bb;
    aa = _get_entry(a, kv_node, snode);
    bb = _get_entry(b, kv_node, snode);
    size_t sz;
    sz = aa->key_sz;
    if (bb->key_sz < sz) {
        sz = bb->key_sz;
    }
    int ret = memcmp(aa->key, bb->key, sz);
    if (ret != 0 || aa->key_sz == bb->key_sz) {
        return ret;
    }
    return aa->key_sz < bb->key_sz?-1:1;
}

skiplist_raw* kv_skiplist_create(); 
void kv_skiplist_destroy(skiplist_raw* slist);
kv_node* kv_skiplist_node_create(const char* key, size_t ksz, const char* value, size_t vsz);

int kv_skiplist_insert(skiplist_raw* l, const char* key, size_t ksz, const char* value, size_t vsz);
int kv_skiplist_insert_node(skiplist_raw* l, kv_node* n);

char* kv_skiplist_get(skiplist_raw* l, const char* key, size_t ksz, size_t* vsz);
int kv_skiplist_del(skiplist_raw* l, const char* key, size_t ksz);
skiplist_node* kv_skiplist_find_ge(skiplist_raw* l, const char* key, size_t ksz);
skiplist_node* kv_skiplist_find_le(skiplist_raw* l, const char* key, size_t ksz);

char* kv_skiplist_get_node_key(skiplist_node* n, size_t* sz);
char* kv_skiplist_get_node_value(skiplist_node* n, size_t* sz);

#ifdef __cplusplus
}
#endif

#endif