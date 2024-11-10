# ptx

Probabilistic transaction graph for optimistic concurrency control.

A new method for serializing transactions.

Rather than using timestamps or sequence numbers, ptx uses hashes that are
stored in [growable bloom filters](https://github.com/tidwall/rhbloom), where
each transaction is a node in a graph that links to dependent nodes. 

The graph model is inspired by the
[SSI Algorithm](https://wiki.postgresql.org/wiki/Serializable) in Postgres, and
works by looking out for certain wr/ww-dependencies and rw-antidependencies
(aka conflicts) in transaction nodes that will cause anomolies such as read
and write skews.

The main idea is that you create a "graph" which represents a shared data
resource (such as a database, table, collection, etc), from which you then
instantiate transaction nodes using the "begin" operation, followed by providing
"read" and "write" operations to that transaction along with a single hash
representing what item in the shared data you're reading or writing.
These hashed items can represent keys, tuples, ranges, or something else; pretty much whatever you want.
When done with the transaction you call "commit" or "rollback".
The "commit" operation will return true if the transaction can serialize and
false if an anomoly was detected. 

Because ptx uses a bloom filter to store the hashes, it's possible to have 
false positives when detecting anomolies. The probability rate for false
positives is a configurable option, as is the targeted number of elements.
Default is 1,000,000 elements, 1% probability.

This repository provides a working implementation written in C. It's designed
to be small, fast, and easily embeddable. Should compile using any C99 compiler
such as gcc, clang, and tcc. Includes webassembly (Emscripten / emcc) support.

## Example

Here's an example that causes a simple write skew.

```c
// Create a graph
struct ptx_graph *graph = ptx_graph_new(0);

// Create a transaction (T1)
struct ptx_node *t1 = ptx_graph_begin(graph, 0);

// Read an item (using the item's hash) (T1)
ptx_node_read(t1, 0x1F19281);

// Create a second transaction (T2)
struct ptx_node *t2 = ptx_graph_begin(graph, 0);

// Read the same item (T2)
ptx_node_read(t2, 0x1F19281);

// Now write the item (T1)
ptx_node_write(t1, 0x1F19281);

// And commit the transaction (T1)
// This will succeed.
assert(ptx_node_commit(t1) == true);

// Now write the in the other transaction (T2)
ptx_node_write(t2, 0x1F19281);

// And try to commit that transaction (T2)
// This will fail.
assert(ptx_node_commit(t2) == false);

// Finally free the graph to when your are done with it.
ptx_graph_free(graph);
```

## API

```c
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
```

## Links

- https://en.wikipedia.org/wiki/Optimistic_concurrency_control
- https://github.com/tidwall/rhbloom
- https://wiki.postgresql.org/wiki/SSI
- https://drkp.net/papers/ssi-vldb12.pdf
- https://jepsen.io/analyses/postgresql-12.3
- https://wiki.postgresql.org/wiki/Serializable
