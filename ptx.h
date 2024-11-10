// https://github.com/tidwall/ptx
//
// Copyright 2024 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

#ifndef PTX_H
#define PTX_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

struct ptx_graph;
struct ptx_node;

struct ptx_graph_opts {
    void*(*malloc)(size_t); // custom allocator
    void(*free)(void*);     // custom allocator
    size_t n;   // bloom filter: number of elements (default 1,000,000)
    double p;   // bloom filter: false positive rate (default 1%)
    int autogc; // automatic gc cycle, set -1 to disable. (default: 1000)
};

// Create a new graph.
// Returns NULL if out of memory.
struct ptx_graph *ptx_graph_new(struct ptx_graph_opts*);

// Free the graph and all child transactions
void ptx_graph_free(struct ptx_graph *graph);

// Begin a new transaction.
// Returns NULL if out of memory.
struct ptx_node *ptx_graph_begin(struct ptx_graph *graph, void *opt);

// Read an item using the item's hash
void ptx_node_read(struct ptx_node *node, uint64_t hash);

// Write an item using the item's hash
void ptx_node_write(struct ptx_node *node, uint64_t hash);

// Rollback a transaction
// The transaction node should not be used again after this call.
void ptx_node_rollback(struct ptx_node *node);

// Commit a transaction. Returns false if failure to serialize
// The transaction node should not be used again after this call.
bool ptx_node_commit(struct ptx_node *node);

// Returns true if last ptx_node_commit() failure was due to out of memory
bool ptx_oom(void);

// Debug: set a label for the transaction
void ptx_node_setlabel(struct ptx_node *node, const char *label);

// Debug: return the transaction label
const char *ptx_node_label(struct ptx_node *node);

// Debug: print the entire graph to stdout
void ptx_graph_print(struct ptx_graph *graph, bool withedges);

// Debug: call a garbage collection cycle now
void ptx_graph_gc(struct ptx_graph *graph);

#endif
