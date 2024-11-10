#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#ifndef PTX_EXTERN
#define PTX_EXTERN
#endif

struct ptx_graph;
struct ptx_node;

struct ptx_graph_opts {
    void*(*malloc)(size_t);  // custom allocator
    void(*free)(void*);      // custom allocator
    size_t n;   // number bloom filter elements (default 1,000,000)
    double p;   // false positive rate (default 1%)
    int autogc; // automatic gc cycle, set -1 to disable. (default: 1000)
};

PTX_EXTERN struct ptx_graph *ptx_graph_new(struct ptx_graph_opts*);
PTX_EXTERN void ptx_graph_free(struct ptx_graph *graph);
PTX_EXTERN void ptx_graph_gc(struct ptx_graph *graph);
PTX_EXTERN struct ptx_node *ptx_graph_begin(struct ptx_graph *graph, void *opt);
PTX_EXTERN void ptx_node_setlabel(struct ptx_node *node, const char *label);
PTX_EXTERN const char *ptx_node_label(struct ptx_node *node);
PTX_EXTERN void ptx_node_read(struct ptx_node *node, uint64_t hash);
PTX_EXTERN void ptx_node_write(struct ptx_node *node, uint64_t hash);
PTX_EXTERN void ptx_node_rollback(struct ptx_node *node);
PTX_EXTERN bool ptx_node_commit(struct ptx_node *node);
PTX_EXTERN bool ptx_oom(void);
PTX_EXTERN void ptx_graph_print(struct ptx_graph *graph, bool withedges);
PTX_EXTERN void ptx_graph_print_state(struct ptx_graph *graph, char output[]);

#include <stdio.h>
#include <assert.h>
#include <inttypes.h>
#include <string.h>
#include <math.h>

#define PTX_TRACKINS

#define PTX_DEFAULT_N      1000000
#define PTX_DEFAULT_P      0.01
#define PTC_DEFAULT_AUTOGC 1000

#define PTX_ACTIVE     0
#define PTX_COMMITTED  1
#define PTX_ROLLEDBACK 2
#define PTX_NOMEM      3
#define PTX_RELEASED   4

#define PTX_WR 1
#define PTX_WW 2
#define PTX_RW 4

struct ptx_edge {
    uint16_t dib;          // bucket distance (robinhood hashtable)
    uint16_t kind;         // edge kind: TXWR, TXWW, TXRW
    struct ptx_node *node; // the node
};

struct ptx_edgemap {
    struct ptx_edge *buckets;
    size_t count;
    size_t nbuckets;
};

struct ptx_hashset {
    // hashtable fields
    size_t nbuckets;
    size_t count;
    uint64_t *buckets;
    uint64_t buckets0[4]; // 
    // bloom fields
    size_t k;           // number of bits per key
    size_t m;           // number of bits total
    uint8_t *bits;      // bloom bits
};

struct ptx_node {
    struct ptx_node *prev;
    struct ptx_node *next;
    int state;
    uint64_t ident;
    struct ptx_graph *graph;   // root graph
    bool reached;
    bool nomem;

    bool hasdeps;
    bool hasreads;
    bool haswrites;


    struct ptx_edgemap outs;   // Edges that join this node to another.
#ifdef PTX_TRACKINS
    struct ptx_edgemap ins; // Edges that join this node to another.
#endif
    struct ptx_hashset reads;
    struct ptx_hashset writes;
    char label[32];
};

struct ptx_graph {
    struct ptx_node head;
    struct ptx_node tail;
    uint64_t ident;    // ident counter
    int gccounter;     // gc counter
    int autogc;        //
    void*(*malloc)(size_t);
    void(*free)(void*);
    size_t n;  // number bloom filter elements (default 1,000,000)
    double p;  // false positive rate (default 1%)
};

static __thread bool _ptx_oom = false;

bool ptx_oom(void) {
    return _ptx_oom;
}

static void ptx_hashset_init(struct ptx_hashset *set, size_t n, double p) {
    memset(set, 0, sizeof(struct ptx_hashset));
    // Hashtable
    set->nbuckets = sizeof(set->buckets0)/8;
    set->buckets = set->buckets0;
    // Bloom filter
    if (n < 16) {
        n = 16;
    }
    // Calculate the total number of bits needed
    size_t m = n * log(p) / log(1 / pow(2, log(2)));
    // Calculate the bits per key
    size_t k = round(((double)m / (double)(n)) * log(2));
    // Adjust the number of bit to power of two
    set->m = 2;
    while (set->m < m) {
        set->m *= 2;
    }
    set->k = round((double)m / (double)set->m * (double)k);
}

static void ptx_hashset_free(struct ptx_graph *graph, struct ptx_hashset *set) {
    if (set->buckets != set->buckets0) {
        graph->free(set->buckets);
    }
    if (set->bits) {
        graph->free(set->bits);
    }
}

static uint64_t ptx_hashof(uint64_t x) {
    return x << 8 >> 8;
}
static uint8_t ptx_dibof(uint64_t x) {
    return x >> 56;
}
static uint64_t ptx_sethashdib(uint64_t hash, uint8_t dib) {
    return ptx_hashof(hash) | ((uint64_t)dib << 56);
}

static bool ptx_testadd(struct ptx_hashset *set, uint64_t hash,
    bool add)
{
    // We only want the 56-bit hash in order to match correcly with the
    // robinhood entries, upon upgrade.
    hash = ptx_hashof(hash);
    // Add or check each bit
    size_t i = 0;
    size_t j = hash & (set->m-1);
    while (1) {
        if (add) {
            set->bits[j>>3] |= add<<(j&7);
        } else if (!((set->bits[j>>3]>>(j&7))&1)) {
            return false;
        }
        if (i == set->k-1) {
            break;
        }
        // Pick the next bit. 
        // Use part of the mix13 forumula to help get a more randomized value.
        // https://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html
        hash *= UINT64_C(0x94d049bb133111eb);
        hash ^= hash >> 31;
        j = hash & (set->m-1);
        i++;
    }
    return true;
}

static void ptx_add0(struct ptx_hashset *set, uint64_t hash) {
    hash = ptx_hashof(hash);
    uint8_t dib = 1;
    size_t i = hash & (set->nbuckets-1);
    while (1) {
        if (ptx_dibof(set->buckets[i]) == 0) {
            set->buckets[i] = ptx_sethashdib(hash, dib);
            set->count++;
            return;
        }
        if (ptx_dibof(set->buckets[i]) < dib) {
            uint64_t tmp = set->buckets[i];
            set->buckets[i] = ptx_sethashdib(hash, dib);
            hash = ptx_hashof(tmp);
            dib = ptx_dibof(tmp);
        }
        if (ptx_hashof(set->buckets[i]) == hash) {
            return;
        }
        dib++;
        i = (i + 1) & (set->nbuckets-1);
    }
}

static bool ptx_grow(struct ptx_graph *graph, struct ptx_hashset *set) {
    uint64_t *buckets_old = set->buckets;
    size_t nbuckets_old = set->nbuckets;
    if (set->nbuckets*2*8 >= set->m/8) {
        // Upgrade to bloom filter
        set->bits = graph->malloc(set->m/8);
        if (!set->bits) {
            return false;
        }
        set->nbuckets *= 2;
        memset(set->bits, 0, set->m/8);
        set->count = 0;
        set->nbuckets = 0;
        set->buckets = set->buckets0;
        for (size_t i = 0; i < nbuckets_old; i++) {
            if (ptx_dibof(buckets_old[i])) {
                ptx_testadd(set, buckets_old[i], true);
            }
        }
    } else {
        set->buckets = graph->malloc(set->nbuckets*2*8);
        if (set->buckets) {
            return false;
        }
        set->nbuckets *= 2;
        memset(set->buckets, 0, set->nbuckets * 8);
        for (size_t i = 0; i < nbuckets_old; i++) {
            if (ptx_dibof(buckets_old[i])) {
                ptx_add0(set, buckets_old[i]);
            }
        }
    }
    if (buckets_old != set->buckets0) {
        graph->free(buckets_old);
    }
    return true;
}

static bool ptx_hashset_add(struct ptx_graph *graph, struct ptx_hashset *set,
    uint64_t hash)
{
    do {
        if (set->bits) {
            ptx_testadd(set, hash, true);
        } else if (set->count < set->nbuckets >> 1) {
            ptx_add0(set, hash);
        } else {
            if (!ptx_grow(graph, set)) {
                return false;
            }
            continue;
        }
    } while (0);
    return true;
}

static bool ptx_hashset_test(struct ptx_hashset *set, uint64_t hash) {
    if (set->bits) {
        return ptx_testadd(set, hash, false);
    }
    hash = ptx_hashof(hash);
    uint8_t dib = 1;
    size_t i = hash & (set->nbuckets-1);
    while (1) {
        if (ptx_hashof(set->buckets[i]) == hash) {
            return true;
        }
        if (ptx_dibof(set->buckets[i]) < dib) {
            return false;
        }
        dib++;
        i = (i + 1) & (set->nbuckets-1);
    }
}

// Returns true if edgemap is empty
static bool ptx_hashset_empty(struct ptx_hashset *set) {
    return set->bits == 0 && set->count == 0;
}

// Returns the number of items in edgemap
static size_t ptx_edgemap_count(struct ptx_edgemap *map) {
    return map->count;
}

// Free the edgemap
static void ptx_edgemap_free(struct ptx_graph *graph, struct ptx_edgemap *map) {
    if (map->buckets) {
        graph->free(map->buckets);
    }
}

// Iterate over all edges. Returns NULL when done.
// Example:
//    size_t pidx = 0;
//    struct ptx_edge *edge = ptx_edgemap_iter(map, &pidx);
//    while (edge) {
//         edge = ptx_edgemap_iter(map, &pidx);
//    }
static struct ptx_edge *ptx_edgemap_iter(struct ptx_edgemap *map, size_t *pidx){
    while (1) {
        if (*pidx >= map->nbuckets) {
            return 0;
        }
        struct ptx_edge *edge = &map->buckets[(*pidx)++];
        if (edge->dib > 0) {
            return edge;
        }
    }
}

struct ptx_graph *ptx_graph_new(struct ptx_graph_opts *opts) {
    void*(*_malloc)(size_t) = opts ? opts->malloc : 0;
    void(*_free)(void*) = opts ? opts->free : 0;
    size_t n = opts ? opts->n : 0;
    double p = opts ? opts->p : 0;
    int autogc = opts ? opts->autogc : 0;
    _malloc = _malloc ? _malloc : malloc;
    _free = _free ? _free : free;
    n = n > 0 ? n : PTX_DEFAULT_N;
    p = p > 0 && isfinite(p) ? p : PTX_DEFAULT_P;
    autogc = autogc > 0 ? autogc : PTC_DEFAULT_AUTOGC;
    struct ptx_graph *graph = _malloc(sizeof(struct ptx_graph));
    if (!graph) {
        return 0;
    }
    memset(graph, 0, sizeof(struct ptx_graph));
    graph->malloc = _malloc;
    graph->free = _free;
    graph->n = n;
    graph->p = p;
    graph->head.next = &graph->tail;
    graph->tail.prev = &graph->head;
    return graph;
}

static void ptx_node_unlink(struct ptx_node *node) {
    if (node->prev) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
        node->prev = 0;
        node->next = 0;
    }
    node->graph = 0;
}

static void ptx_node_free(struct ptx_node *node) {
    struct ptx_graph *graph = node->graph;
    ptx_node_unlink(node);
    ptx_hashset_free(graph, &node->reads);
    ptx_hashset_free(graph, &node->writes);
#ifdef PTX_TRACKINS
    ptx_edgemap_free(graph, &node->ins);
#endif
    ptx_edgemap_free(graph, &node->outs);
    graph->free(node);
}

void ptx_graph_free(struct ptx_graph *graph) {
    // Run the garbage collector.
    ptx_graph_gc(graph);
    // Any remaining nodes mush be rolled back.
    while (graph->head.next != &graph->tail) {
        struct ptx_node *node = graph->head.next;
        ptx_node_unlink(node);
        if (node->state == PTX_ACTIVE) {
            node->state = PTX_RELEASED;
        }
    }
    graph->free(graph);
}


static void ptx_node_gcmark(struct ptx_node *node) {
    if (!node->reached) {
        node->reached = true;
        size_t pidx = 0;
        struct ptx_edge *edge = ptx_edgemap_iter(&node->outs, &pidx);
        while (edge) {
            ptx_node_gcmark(edge->node);
            edge = ptx_edgemap_iter(&node->outs, &pidx);
        }
    }
}

void ptx_graph_gc(struct ptx_graph *graph) {
    // Mark. Look for reached nodes.
    struct ptx_node *node = graph->head.next;
    while (node != &graph->tail) {
        if (node->state == PTX_ACTIVE) {
            ptx_node_gcmark(node);
        }
        node = node->next;
    }
    // Sweep. Free unreached nodes.
    node = graph->head.next;
    while (node != &graph->tail) {
        struct ptx_node *next = node->next;
        if (!node->reached) {
            ptx_node_free(node);
        } else {
            node->reached = 0;
        }
        node = next;
    }
}

static void ptx_graph_autogc(struct ptx_graph *graph) {
    if (graph->gccounter >= graph->autogc) {
        graph->gccounter = 0;
        ptx_graph_gc(graph);
    }
}

struct ptx_node *ptx_graph_begin(struct ptx_graph *graph, void *opt) {
    (void)opt; // unused atm
    struct ptx_node *node = graph->malloc(sizeof(struct ptx_node));
    if (!node) {
        return 0;
    }
    memset(node, 0, sizeof(struct ptx_node));
    ptx_hashset_init(&node->reads, graph->n, graph->p);
    ptx_hashset_init(&node->writes, graph->n, graph->p);
    node->state = PTX_ACTIVE;
    node->graph = graph;
    graph->tail.prev->next = node;
    node->prev = graph->tail.prev;
    node->next = &graph->tail;
    graph->tail.prev = node;
    node->ident = ++graph->ident;
    ptx_node_setlabel(node, 0);
    return node;
}

void ptx_node_setlabel(struct ptx_node *node, const char *label) {
    if (label) {
        snprintf(node->label, sizeof(node->label), "%s", label);
    } else {
        snprintf(node->label, sizeof(node->label), "T(%" PRIu64 ")", 
            node->ident);
    }
}

const char *ptx_node_label(struct ptx_node *node) {
    return node->label;
}

static void ptx_node_deactivate(struct ptx_node *node, int state) {
    node->state = state;
    if (node->graph->autogc > 0) {
        node->graph->gccounter++;
        if (ptx_edgemap_count(&node->outs) == 0 && !node->hasdeps) {
            ptx_node_free(node);
        }
        ptx_graph_autogc(node->graph);
    }
}

void ptx_node_rollback(struct ptx_node *node) {
    assert(node->state == PTX_ACTIVE || node->state == PTX_NOMEM);
    ptx_node_deactivate(node, PTX_ROLLEDBACK);
}

bool ptx_node_commit(struct ptx_node *node) {
    assert(node->state == PTX_ACTIVE || node->state == PTX_NOMEM);
    if (node->state == PTX_NOMEM) {
        _ptx_oom = true;
        ptx_node_deactivate(node, PTX_ROLLEDBACK);
        return false;
    }
    _ptx_oom = false;
    bool abort = false;
    size_t pidx = 0;
    struct ptx_edge *edge = ptx_edgemap_iter(&node->outs, &pidx);
    while (edge) {
        if (edge->node->state == PTX_COMMITTED && edge->node->haswrites) {
            abort = true;
            break;
        }
        edge = ptx_edgemap_iter(&node->outs, &pidx);
    }
    if (abort) {
        ptx_node_deactivate(node, PTX_ROLLEDBACK);
        return false;
    } else {
        ptx_node_deactivate(node, PTX_COMMITTED);
        return true;
    }
}

static bool ptx_edge_equal(struct ptx_edge *a, struct ptx_edge *b) {
    return a->node->ident == b->node->ident && a->kind == b->kind;
}


// Add the edge by performing Robin-hood hashing.
// This is an intermediate operation and should not be called directly.
static void ptx_edgemap_add0(struct ptx_edgemap *map, struct ptx_edge edge) {
    edge.dib = 1;
    size_t i = edge.node->ident & (map->nbuckets-1);
    while (1) {
        if (map->buckets[i].dib == 0) {
            map->buckets[i] = edge;
            map->count++;
            break;
        }
        if (ptx_edge_equal(&map->buckets[i], &edge)) {
            return;
        }
        if (map->buckets[i].dib < edge.dib) {
            struct ptx_edge tmp = map->buckets[i];
            map->buckets[i] = edge;
            edge = tmp;
        }
        edge.dib++;
        i = (i + 1) & (map->nbuckets-1);
    }
}

// Double the map capacity.
// Return true on Success, or false on Out of memory.
static bool ptx_edgemap_grow(struct ptx_graph *graph, struct ptx_edgemap *map) {
    struct ptx_edge *buckets0 = map->buckets;
    size_t nbuckets0 = map->nbuckets;
    size_t nbuckets1 = map->nbuckets == 0 ? 2 : map->nbuckets * 2;
    struct ptx_edge *bucket1 = graph->malloc(sizeof(struct ptx_edge)*nbuckets1);
    if (!bucket1) {
        return false;
    }
    memset(bucket1, 0, sizeof(struct ptx_edge)*nbuckets1);
    map->buckets = bucket1;
    map->nbuckets = nbuckets1;
    map->count = 0;
    for (size_t i = 0; i < nbuckets0; i++) {
        if (buckets0[i].dib) {
            ptx_edgemap_add0(map, buckets0[i]);
        }
    }
    if (buckets0) {
        graph->free(buckets0);
    }
    return true;
}


// Adds an edge to the map.
// Return true on Success, or false on Out of memory.
static bool ptx_edgemap_add(struct ptx_edgemap *map, struct ptx_node *node,
    int kind)
{
    if (map->count == map->nbuckets / 2) {
        if (!ptx_edgemap_grow(node->graph, map)) {
            return false;
        }
    }
    struct ptx_edge edge = {
        .kind = (int16_t)kind,
        .node = node,
    };
    ptx_edgemap_add0(map, edge);
    return true;
}

// add an edge dependency from node-a to node-b.
static bool ptx_node_adddep(struct ptx_node *a, struct ptx_node *b, int kind) {
#ifdef PTX_TRACKINS
    if (!ptx_edgemap_add(&b->ins, a, kind)) {
        return false;
    }
#endif
    if (!ptx_edgemap_add(&a->outs, b, kind)) {
        return false;
    }
    b->hasdeps = true;
    return true;
}

void ptx_node_read(struct ptx_node *node, uint64_t hash) {
    // The node can only be in ACTIVE or NOMEM state
    assert(node->state == PTX_ACTIVE || node->state == PTX_NOMEM);
    if (node->state == PTX_NOMEM) {
        return;
    }
    // Add the read to the current node
    if (!ptx_hashset_add(node->graph, &node->reads, hash)) {
        node->state = PTX_NOMEM;
        return;
    }
    node->hasreads = true;
    // Search for nodes that have written the same hash
    struct ptx_node *other = node->graph->head.next;
    while (other != &node->graph->tail) {
        if (other != node) {
            if (ptx_hashset_test(&other->writes, hash)) {
                if (!ptx_node_adddep(other, node, PTX_WR)) {
                    node->state = PTX_NOMEM;
                    return;
                }
            }
        }
        other = other->next;
    }
}

void ptx_node_write(struct ptx_node *node, uint64_t hash) {
    // The node can only be in ACTIVE or NOMEM state
    assert(node->state == PTX_ACTIVE || node->state == PTX_NOMEM);
    if (node->state == PTX_NOMEM) {
        return;
    }
    // Add the write to the current node
    if (!ptx_hashset_add(node->graph, &node->writes, hash)) {
        node->state = PTX_NOMEM;
        return;
    }
    node->haswrites = true;
    // Search for nodes that have read or written the same hash.
    struct ptx_node *other = node->graph->head.next;
    while (other != &node->graph->tail) {
        if (other != node) {
            if (ptx_hashset_test(&other->reads, hash)) {
                if (!ptx_node_adddep(other, node, PTX_RW)) {
                    node->state = PTX_NOMEM;
                    return;
                }
            }
            if (ptx_hashset_test(&other->writes, hash)) {
                if (!ptx_node_adddep(other, node, PTX_WW)) {
                    node->state = PTX_NOMEM;
                    return;
                }
                if (!ptx_node_adddep(node, other, PTX_WW)) {
                    node->state = PTX_NOMEM;
                    return;
                }
            }
        }
        other = other->next;
    }
}

void ptx_graph_print(struct ptx_graph *graph, bool withedges) {
    struct ptx_node *node = graph->head.next;
    char T1[32];
    char T2[32];
    while (node != &graph->tail) {
        snprintf(T1, sizeof(T1), "%s", node->label);
        printf("%s", T1);
        if (node->state == PTX_ACTIVE) {
            printf(" \033[1mACTIVE\033[m      ");
        } else if (node->state == PTX_COMMITTED) {
            printf(" \033[1;32mCOMMIT\033[m   ");
        } else if (node->state == PTX_ROLLEDBACK) {
            printf(" \033[1;31mROLLBACK\033[m ");
        }
#ifdef PTX_TRACKINS
        printf("(%d ins, %d outs)", (int)ptx_edgemap_count(&node->ins),
#else
        printf("(%d outs)", 
#endif
            (int)ptx_edgemap_count(&node->outs));
        if (ptx_hashset_empty(&node->writes)) {
            printf(" \033[2m<READONLY>\033[m");
        }
        printf("\n");
        if (withedges) {
            size_t pidx;
            struct ptx_edge *edge;
            printf("\033[1m");
            pidx = 0;
            edge = ptx_edgemap_iter(&node->outs, &pidx);
            while (edge) {
                snprintf(T2, sizeof(T2), "%s", edge->node->label);
                if (edge->kind == PTX_RW) {
                    printf("  %s ----(rw)---> %s\n", T1, T2);
                }
                if (edge->kind == PTX_WR) {
                    printf("  %s ----(wr)---> %s\n", T1, T2);
                }
                if (edge->kind == PTX_WW) {
                    printf("  %s ----(ww)---> %s\n", T1, T2);
                }
                edge = ptx_edgemap_iter(&node->outs, &pidx);
            }
            printf("\033[m");
#ifdef PTX_TRACKINS
            printf("\033[2m\033[1;30m");
            pidx = 0;
            edge = ptx_edgemap_iter(&node->ins, &pidx);
            while (edge) {
                snprintf(T2, sizeof(T2), "%s", edge->node->label);
                if (edge->kind == PTX_RW) {
                    printf("  %s <---(rw)---- %s\n", T1, T2);
                }
                if (edge->kind == PTX_WR) {
                    printf("  %s <---(wr)---- %s\n", T1, T2);
                }
                if (edge->kind == PTX_WW) {
                    printf("  %s <---(ww)---- %s\n", T1, T2);
                }
                edge = ptx_edgemap_iter(&node->ins, &pidx);
            }
            printf("\033[m");
#endif
        }
        node = node->next;
    }
}

static const char *ptx_strstate(int status) {
    switch (status) {
    case PTX_ACTIVE: return "ACTIVE";
    case PTX_COMMITTED: return "COMMIT";
    case PTX_ROLLEDBACK: return "ROLLBACK";
    case PTX_NOMEM: return "NOMEM";
    case PTX_RELEASED: return "RELEASED";
    default: return "UNKNOWN";
    }
}

void ptx_graph_print_state(struct ptx_graph *graph, char output[]) {
    int i = 0;
    char buf[128] = "";
    output[0] = 0;
    struct ptx_node *node = graph->head.next;
    while (node != &graph->tail) {
        if (i > 0) {
            strcat(output, ", ");
        }
        snprintf(buf, sizeof(buf), "%s %s", node->label, 
            ptx_strstate(node->state));
        strcat(output, buf);
        node = node->next;
        i++;
    }
}
