#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "ptx.h"

void ptx_graph_print_state(struct ptx_graph *graph, char output[]);

static size_t _nallocs = 0;

static size_t xallocs(void) {
    return _nallocs;
}

static void *xmalloc(size_t size) {
    void *ptr = malloc(size); 
    assert(ptr);
    _nallocs++;
    return ptr;
}

static void xfree(void *ptr) {
    assert(ptr);
    free(ptr);
    _nallocs--;
}

// https://github.com/tidwall/th64
static uint64_t th64(const void *data, size_t len, uint64_t seed) {
    uint8_t*p=(uint8_t*)data,*e=p+len;
    uint64_t r=0x14020a57acced8b7,x,h=seed;
    while(p+8<=e)memcpy(&x,p,8),x*=r,p+=8,x=x<<31|x>>33,h=h*r^x,h=h<<31|h>>33;
    while(p<e)h=h*r^*(p++);
    return(h=h*r+len,h^=h>>31,h*=r,h^=h>>31,h*=r,h^=h>>31,h*=r,h);
}

static uint64_t strhash(const char *str) {
    return th64(str, strlen(str), 0);
}

int main(void) {
    int N = 1000000;
    struct ptx_graph_opts opts = {
        .malloc = xmalloc,
        .free = xfree,
        .autogc = -1,
    };

    struct ptx_graph *graph = ptx_graph_new(&opts);

    struct ptx_node **txs = xmalloc(N * sizeof(struct ptx_node*));

    size_t nallocs = xallocs();

    struct ptx_node *T1 = 0, *T2 = 0, *T3 = 0, *T4 = 0, *T5 = 0;
    (void)T1;(void)T2;(void)T3;(void)T4;(void)T5;

#define BEGIN(T) (T)=ptx_graph_begin(graph, 0);ptx_node_setlabel((T), #T);
#define READ(T,K) ptx_node_read((T),strhash((K)))
#define WRITE(T,K) ptx_node_write((T),strhash((K)))
#define COMMIT(T) ptx_node_commit((T));(T)=0
#define ROLLBACK(T) ptx_node_rollback((T));(T)=0
#define TXDO(name, writeedges, func, expect) \
    if (graph) { \
        ptx_graph_gc(graph); \
        if (xallocs() != nallocs) { \
            printf("FAIL: (leaked memory) expected %zu allocations, got %zu\n",\
                nallocs, xallocs()); \
            exit(1); \
        } \
        ptx_graph_free(graph); \
    } \
    graph = ptx_graph_new(&opts); \
    printf("========================\n");\
    printf("===%*s%*s===\n",\
        9+(int)strlen(name)/2,name,9-(int)strlen(name)/2,"");\
    printf("========================\n");\
    func \
    ptx_graph_print(graph, (writeedges));\
    printf("\n"); \
    { \
        char statestr[65000]; \
        ptx_graph_print_state(graph, statestr); \
        if (strcmp(statestr, expect) != 0) { \
            printf("FAIL: expected '%s', got '%s'\n", expect, statestr); \
            exit(1); \
        }; \
    }

TXDO("write-skew-2", 1, {
    BEGIN(T1);
    READ(T1, "doctors");
                                BEGIN(T2);
                                READ(T2, "doctors");
    WRITE(T1, "doctors");
    COMMIT(T1);
                                WRITE(T2, "doctors");
                                COMMIT(T2);
}, "T1 COMMIT, T2 ROLLBACK");

TXDO("write-skew-3", 1, {
    BEGIN(T1);
    READ(T1, "doctors");
                                BEGIN(T2);
                                READ(T2, "doctors");
                                                        BEGIN(T3);
                                                        READ(T3, "doctors");
    WRITE(T1, "doctors");
    COMMIT(T1);
                                WRITE(T2, "doctors");
                                COMMIT(T2);
                                                        WRITE(T3, "doctors");
                                                        COMMIT(T3);
}, "T1 COMMIT, T2 ROLLBACK, T3 ROLLBACK");

TXDO("write-skew-3-alt", 1, {
    BEGIN(T1);
    READ(T1, "doctors");
                                BEGIN(T2);
                                READ(T2, "doctors");
    WRITE(T1, "doctors");
    COMMIT(T1);
                                                        BEGIN(T3);
                                                        READ(T3, "doctors");
                                WRITE(T2, "doctors");
                                COMMIT(T2);
                                                        WRITE(T3, "doctors");
                                                        COMMIT(T3);
}, "T1 COMMIT, T2 ROLLBACK, T3 ROLLBACK");

TXDO("receipts", 1, {
                                BEGIN(T2);
                                READ(T2, "current-batch");
                                                        BEGIN(T3);
                                                        WRITE(T3, "current-batch");
                                                        COMMIT(T3);
    BEGIN(T1);
    READ(T1, "current-batch");
    READ(T1, "receipts");
    COMMIT(T1);
                                WRITE(T2, "receipts");
                                COMMIT(T2);
}, "T2 ROLLBACK, T3 COMMIT, T1 COMMIT");

TXDO("dots-2", 1, {
    BEGIN(T1);
    WRITE(T1, "dots");
                                BEGIN(T2);
                                WRITE(T2, "dots");
                                COMMIT(T2);

                                BEGIN(T2);
                                READ(T2, "dots");
                                COMMIT(T2);
    COMMIT(T1);
    
    BEGIN(T1); 
    WRITE(T1, "dots");
    COMMIT(T1);
}, "T1 ROLLBACK, T2 COMMIT, T2 COMMIT, T1 ROLLBACK");

TXDO("intersecting", 1, {
    BEGIN(T1);
    READ(T1, "mytab");
    WRITE(T1, "mytab");
                                BEGIN(T2);
                                READ(T2, "mytab");
                                WRITE(T2, "mytab");
                                COMMIT(T2);
    COMMIT(T1);
}, "T1 ROLLBACK, T2 COMMIT");

TXDO("overdraft", 1, {
    BEGIN(T1);
    READ(T1, "checking");
    READ(T1, "saving");
    
                                BEGIN(T2);
                                READ(T2, "checking");
                                READ(T2, "saving");
    WRITE(T1, "saving");
                                WRITE(T2, "checking");
    COMMIT(T1);
                                COMMIT(T2);
}, "T1 COMMIT, T2 ROLLBACK");

TXDO("write-write", 1, {
    BEGIN(T1);
    WRITE(T1, "dots");
                                BEGIN(T2);
                                WRITE(T2, "dots");
    COMMIT(T1);
                                COMMIT(T2);
}, "T1 COMMIT, T2 ROLLBACK");

TXDO("write-read", 1, {
    BEGIN(T1);
    WRITE(T1, "dots");
                                BEGIN(T2);
                                READ(T2, "dots");
                                COMMIT(T2);
    COMMIT(T1);
}, "T1 COMMIT, T2 COMMIT");

    xfree(txs);
    ptx_graph_free(graph);

    if (xallocs() != 0) {
        printf("%zu remaining allocations\n", xallocs());
        printf("FAIL\n");
        abort();
    }

    printf("PASSED\n");

    return 0;
}
