/*
 * Frozen-vector oracle for bucket.rs boundary/type regressions.
 * Ceph Tentacle 20.2.4, 7f793731f1b39eb4f465e960113d2363c311b964:
 * https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c
 */
#include "mapper.c"
#include <inttypes.h>

int main(void) {
    int items[] = {-2, -3, -4};
    __u32 weights[] = {0, 0x7fffffff, 0x80000000};
    __u32 sums[] = {0, 0x7fffffff, 0xffffffff};
    __u32 normal_weights[] = {0x10000, 0x20000, 0x30000};
    __u32 nodes[] = {0, 0, 0x7fffffff, 0x7fffffff, 0xffffffff, 0x80000000, 0x80000000, 0};
    struct crush_bucket h = {.id = -1, .type = 2, .hash = 0, .weight = 0xffffffff, .size = 3, .items = items};
    struct crush_bucket_uniform uniform = {.h = h, .item_weight = 0xffffffff};
    struct crush_bucket_list list = {.h = h, .item_weights = weights, .sum_weights = sums};
    struct crush_bucket_tree tree = {.h = h, .num_nodes = 8, .node_weights = nodes};
    struct crush_bucket_straw straw = {.h = h, .item_weights = weights, .straws = weights};
    struct crush_bucket_straw2 straw2 = {.h = h, .item_weights = normal_weights};
    unsigned inputs[][2] = {
        {0, 0}, {0x7fffffff, 0x7fffffff}, {0x80000000, 0},
        {0x80000000, 0x80000000}, {0xffffffff, 1},
        {0xffffffff, 0xffffffff}, {0xffffffff, 0xfffffffe}, {1, 0x80000000}
    };
    for (unsigned i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
        int x = (int)inputs[i][0], r = (int)inputs[i][1];
        __u32 perm[3];
        struct crush_work_bucket work = {.perm = perm};
        printf("%#x %#x: [%d, %d, %d, %d, %d]\n", inputs[i][0], inputs[i][1],
            bucket_uniform_choose(&uniform, &work, x, r), bucket_list_choose(&list, x, r),
            bucket_tree_choose(&tree, x, r), bucket_straw_choose(&straw, x, r),
            bucket_straw2_choose(&straw2, x, r, NULL, 0));
    }
    uint64_t digest = UINT64_C(0xcbf29ce484222325);
    for (unsigned u = 0; u <= 0xffff; ++u)
        digest = (digest ^ crush_ln(u)) * UINT64_C(0x100000001b3);
    printf("ln digest: %#" PRIx64 "\n", digest);
    unsigned edges[] = {0, 1, 0xff, 0x100, 0x7fff, 0x8000, 0xfffe, 0xffff};
    for (unsigned i = 0; i < sizeof(edges)/sizeof(edges[0]); ++i)
        printf("ln %#x = %#" PRIx64 "\n", edges[i], (uint64_t)crush_ln(edges[i]));
    int draw_weights[] = {1, 0x7fffffff, (int)0x80000000, -1};
    for (unsigned i = 0; i < sizeof(draw_weights)/sizeof(draw_weights[0]); ++i)
        printf("draw %d = %" PRId64 "\n", draw_weights[i], (int64_t)generate_exponential_distribution(0, (int)0x80000000, -2, -1, draw_weights[i]));
}
