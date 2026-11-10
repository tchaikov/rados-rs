/* Local integer-boundary probes; include the unmodified pinned Ceph mapper
 * to exercise its static FIRSTN helper. See README.md for reproduction. */
#include "mapper.c"
#include <assert.h>
#include <limits.h>
#include <stdio.h>

int main(void)
{
    int root_items[] = {-2}, host_items[] = {0, 1, 2, 3};
    unsigned root_weights[] = {262144}, host_weights[] = {65536, 65536, 65536, 65536};
    struct crush_bucket_straw2 root = {
        .h = {.id = -1, .type = 2, .alg = 5, .hash = 0, .weight = 262144,
              .size = 1, .items = root_items}, .item_weights = root_weights};
    struct crush_bucket_straw2 host = {
        .h = {.id = -2, .type = 1, .alg = 5, .hash = 0, .weight = 262144,
              .size = 4, .items = host_items}, .item_weights = host_weights};
    struct crush_bucket *buckets[] = {&root.h, &host.h};
    struct crush_rule *rule = calloc(1, crush_rule_size(3));
    assert(rule);
    struct crush_rule *rules[] = {rule};
    rule->type = 3;
    rule->len = 3;
    rule->steps[0] = (struct crush_rule_step){1, -2, 0};
    rule->steps[1] = (struct crush_rule_step){3, 1, 0};
    rule->steps[2] = (struct crush_rule_step){4, 0, 0};
    struct crush_map map = {
        .max_buckets = 2, .max_rules = 1, .max_devices = 4,
        .buckets = buckets, .rules = rules, .choose_total_tries = UINT_MAX};
    map.working_size = sizeof(struct crush_work)
        + 2 * sizeof(struct crush_work_bucket *)
        + 2 * sizeof(struct crush_work_bucket) + 5 * sizeof(unsigned);
    void *work = calloc(1, map.working_size + 3 * sizeof(int));
    assert(work);
    crush_init_workspace(&map, work);
    const int seeds[] = {0, 1, INT_MAX, INT_MIN, -1};
    const int parents[] = {INT_MIN, -1};
    for (unsigned p = 0; p < 2; ++p) {
        printf("parent %d:", parents[p]);
        for (unsigned x = 0; x < 5; ++x) {
            int out = -99, leaf = -99;
            int n = crush_choose_firstn(&map, work, &root.h, host_weights, 4,
                seeds[x], 1, 1, &out, 0, 1, 1, 1, 0, 0, 1, 2, 1, &leaf, parents[p], NULL);
            assert(n == 1);
            printf(" %d", leaf);
        }
        puts("");
    }
    int out = -99;
    int n = crush_do_rule(&map, 0, 0, &out, 1, host_weights, 4, work, NULL);
    printf("wrapped INDEP: %d %d\n", n, out);
    unsigned out_weights[] = {0, 0, 0, 0};
    n = crush_choose_firstn(&map, work, &host.h, out_weights, 4,
        0, 1, 0, &out, 0, 1, 1, 1, 0, UINT_MAX, 0, 0, 0, NULL, 0, NULL);
    printf("wrapped fallback: %d\n", n);
    free(work);
    free(rule);
}
