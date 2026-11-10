/* Local differential scenarios, not upstream test ports. See README.md.
 * Link only the unmodified mapper.c/hash.c from a pinned Ceph release.
 */
#include "mapper.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
    assert(argc == 2);
    int scenario = atoi(argv[1]);
    assert(scenario >= 0 && scenario <= 24);
#ifdef QUINCY
    assert(scenario == 0 || scenario == 1 || scenario == 2 || scenario == 4 ||
           (scenario >= 11 && scenario <= 24));
#endif
    int root_items[] = {-2, -3}, host0_items[] = {0, 1}, host1_items[] = {2, 3};
    unsigned root_weights[] = {131072, 131072}, host_weights[] = {65536, 65536};
    struct crush_bucket_straw2 root = {
        .h = {.id = -1, .type = 2, .alg = 5, .hash = 0, .weight = 262144,
              .size = 2, .items = root_items}, .item_weights = root_weights};
    struct crush_bucket_straw2 host0 = {
        .h = {.id = -2, .type = 1, .alg = 5, .hash = 0, .weight = 131072,
              .size = 2, .items = host0_items}, .item_weights = host_weights};
    struct crush_bucket_straw2 host1 = {
        .h = {.id = -3, .type = 1, .alg = 5, .hash = 0, .weight = 131072,
              .size = 2, .items = host1_items}, .item_weights = host_weights};
    struct crush_bucket *buckets[] = {&root.h, &host0.h, &host1.h};
    struct crush_rule *rule = calloc(1, crush_rule_size(8));
    assert(rule);
    struct crush_rule *rules[] = {rule};
    struct crush_map map = {
        .max_buckets = 3, .max_rules = 1, .max_devices = 4,
        .buckets = buckets, .rules = rules, .choose_total_tries = 50,
        .chooseleaf_descend_once = 1, .chooseleaf_vary_r = 1,
        .chooseleaf_stable = 1};
#ifndef QUINCY
    map.msr_descents = 100;
    map.msr_collision_tries = 100;
#endif
    /* Numeric rule codes also let Quincy exercise the unknown CHOOSE_MSR. */
    rule->type = 3;
    rule->len = 4;
    rule->steps[0] = (struct crush_rule_step){1, -1, 0};
    rule->steps[1] = (struct crush_rule_step){3, 3, 1};
    rule->steps[2] = (struct crush_rule_step){3, 1, 0};
    rule->steps[3] = (struct crush_rule_step){4, 0, 0};
    int count = 3;
    switch (scenario) {
    case 0: /* Chained INDEP with fewer hosts than requested. */
        break;
    case 1: /* Explicit leaf retries override descend_once for FIRSTN. */
        map.choose_total_tries = 0;
        rule->steps[0] = (struct crush_rule_step){9, 10, 0};
        rule->steps[1] = (struct crush_rule_step){1, -1, 0};
        rule->steps[2] = (struct crush_rule_step){6, 1, 1};
        count = 1;
        break;
    case 2: /* CHOOSE_MSR in a conventional rule is ignored. */
        rule->len = 3;
        rule->steps[1] = (struct crush_rule_step){16, 2, 0};
        rule->steps[2] = (struct crush_rule_step){4, 0, 0};
        break;
    case 3: /* Conventional CHOOSE in an MSR rule is invalid. */
        rule->type = 5;
        rule->len = 3;
        rule->steps[1] = (struct crush_rule_step){2, 2, 0};
        rule->steps[2] = (struct crush_rule_step){4, 0, 0};
        break;
    case 4: /* EMIT clears the working set. */
        rule->len = 3;
        rule->steps[0] = (struct crush_rule_step){1, 0, 0};
        rule->steps[1] = rule->steps[2] = (struct crush_rule_step){4, 0, 0};
        break;
    case 5: /* Truncate fanout 2 x 2 to three results. */
    case 6:
        rule->type = scenario == 5 ? 4 : 5;
        rule->steps[1] = (struct crush_rule_step){16, 2, 1};
        rule->steps[2] = (struct crush_rule_step){16, 2, 0};
        break;
    case 7: /* Missing TAKE. */
        rule->type = 5;
        rule->len = 1;
        rule->steps[0] = (struct crush_rule_step){4, 0, 0};
        break;
    case 8: /* Missing EMIT. */
        rule->type = 5;
        rule->len = 2;
        rule->steps[1] = (struct crush_rule_step){16, 2, 0};
        break;
    case 9: /* Invalid second block discards the earlier output. */
        rule->type = 5;
        rule->steps[1] = (struct crush_rule_step){16, 1, 0};
        rule->steps[2] = (struct crush_rule_step){4, 0, 0};
        break;
    case 10: /* Device TAKE cannot be followed by CHOOSE_MSR. */
        rule->type = 5;
        rule->len = 3;
        rule->steps[0] = (struct crush_rule_step){1, 0, 0};
        rule->steps[1] = (struct crush_rule_step){16, 1, 0};
        rule->steps[2] = (struct crush_rule_step){4, 0, 0};
        break;
    case 11: /* Nonpositive choose tries leave the positive override intact. */
        rule->len = 6;
        rule->steps[0] = (struct crush_rule_step){8, 1, 0};
        rule->steps[1] = (struct crush_rule_step){8, 0, 0};
        rule->steps[2] = (struct crush_rule_step){8, -1, 0};
        rule->steps[3] = (struct crush_rule_step){1, -1, 0};
        rule->steps[4] = (struct crush_rule_step){6, 2, 1};
        rule->steps[5] = (struct crush_rule_step){4, 0, 0};
        count = 2;
        break;
    case 12: /* Nonpositive leaf tries leave the positive override intact. */
        map.choose_total_tries = 0;
        rule->len = 6;
        rule->steps[0] = (struct crush_rule_step){9, 2, 0};
        rule->steps[1] = (struct crush_rule_step){9, 0, 0};
        rule->steps[2] = (struct crush_rule_step){9, -1, 0};
        rule->steps[3] = (struct crush_rule_step){1, -1, 0};
        rule->steps[4] = (struct crush_rule_step){6, 2, 1};
        rule->steps[5] = (struct crush_rule_step){4, 0, 0};
        count = 2;
        break;
    case 13: /* Positive local retries override map tunables. */
        rule->len = 5;
        rule->steps[0] = (struct crush_rule_step){10, 1, 0};
        rule->steps[1] = (struct crush_rule_step){10, -1, 0};
        rule->steps[2] = (struct crush_rule_step){1, -1, 0};
        rule->steps[3] = (struct crush_rule_step){6, 2, 1};
        rule->steps[4] = (struct crush_rule_step){4, 0, 0};
        count = 2;
        break;
    case 14: /* Zero local retries override, and negative leaves zero intact. */
        rule->len = 6;
        rule->steps[0] = (struct crush_rule_step){10, 1, 0};
        rule->steps[1] = (struct crush_rule_step){10, 0, 0};
        rule->steps[2] = (struct crush_rule_step){10, -1, 0};
        rule->steps[3] = (struct crush_rule_step){1, -1, 0};
        rule->steps[4] = (struct crush_rule_step){6, 2, 1};
        rule->steps[5] = (struct crush_rule_step){4, 0, 0};
        count = 2;
        break;
    case 15: /* Positive local fallback retries override map tunables. */
        rule->len = 5;
        rule->steps[0] = (struct crush_rule_step){11, 1, 0};
        rule->steps[1] = (struct crush_rule_step){11, -1, 0};
        rule->steps[2] = (struct crush_rule_step){1, -1, 0};
        rule->steps[3] = (struct crush_rule_step){6, 2, 1};
        rule->steps[4] = (struct crush_rule_step){4, 0, 0};
        count = 2;
        break;
    case 16: /* Zero fallback retries override, and negative leaves zero intact. */
        rule->len = 6;
        rule->steps[0] = (struct crush_rule_step){11, 1, 0};
        rule->steps[1] = (struct crush_rule_step){11, 0, 0};
        rule->steps[2] = (struct crush_rule_step){11, -1, 0};
        rule->steps[3] = (struct crush_rule_step){1, -1, 0};
        rule->steps[4] = (struct crush_rule_step){6, 2, 1};
        rule->steps[5] = (struct crush_rule_step){4, 0, 0};
        count = 2;
        break;
    case 17: /* Positive vary_r is retained when a negative repeat is ignored. */
        rule->len = 5;
        rule->steps[0] = (struct crush_rule_step){12, 2, 0};
        rule->steps[1] = (struct crush_rule_step){12, -1, 0};
        rule->steps[2] = (struct crush_rule_step){1, -1, 0};
        rule->steps[3] = (struct crush_rule_step){6, 2, 1};
        rule->steps[4] = (struct crush_rule_step){4, 0, 0};
        count = 2;
        break;
    case 18: /* vary_r zero is retained after a negative repeat. */
        rule->len = 6;
        rule->steps[0] = (struct crush_rule_step){12, 2, 0};
        rule->steps[1] = (struct crush_rule_step){12, 0, 0};
        rule->steps[2] = (struct crush_rule_step){12, -1, 0};
        rule->steps[3] = (struct crush_rule_step){1, -1, 0};
        rule->steps[4] = (struct crush_rule_step){6, 2, 1};
        rule->steps[5] = (struct crush_rule_step){4, 0, 0};
        count = 2;
        break;
    case 19: /* stable zero is retained after a negative repeat. */
        rule->len = 6;
        rule->steps[0] = (struct crush_rule_step){13, 1, 0};
        rule->steps[1] = (struct crush_rule_step){13, 0, 0};
        rule->steps[2] = (struct crush_rule_step){13, -1, 0};
        rule->steps[3] = (struct crush_rule_step){1, -1, 0};
        rule->steps[4] = (struct crush_rule_step){6, 2, 1};
        rule->steps[5] = (struct crush_rule_step){4, 0, 0};
        count = 2;
        break;
    case 20: /* Direct FIRSTN also ignores nonpositive choose-tries repeats. */
        rule->len = 6;
        rule->steps[0] = (struct crush_rule_step){8, 1, 0};
        rule->steps[1] = (struct crush_rule_step){8, 0, 0};
        rule->steps[2] = (struct crush_rule_step){8, -1, 0};
        rule->steps[3] = (struct crush_rule_step){1, -1, 0};
        rule->steps[4] = (struct crush_rule_step){2, 1, 0};
        rule->steps[5] = (struct crush_rule_step){4, 0, 0};
        count = 1;
        break;
    case 21: /* Direct FIRSTN: positive local retries change collision handling. */
        rule->len = 5;
        rule->steps[0] = (struct crush_rule_step){10, 1, 0};
        rule->steps[1] = (struct crush_rule_step){10, -1, 0};
        rule->steps[2] = (struct crush_rule_step){1, -1, 0};
        rule->steps[3] = (struct crush_rule_step){2, 3, 0};
        rule->steps[4] = (struct crush_rule_step){4, 0, 0};
        break;
    case 22: /* Direct FIRSTN: zero local retries survive a negative repeat. */
        rule->len = 6;
        rule->steps[0] = (struct crush_rule_step){10, 1, 0};
        rule->steps[1] = (struct crush_rule_step){10, 0, 0};
        rule->steps[2] = (struct crush_rule_step){10, -1, 0};
        rule->steps[3] = (struct crush_rule_step){1, -1, 0};
        rule->steps[4] = (struct crush_rule_step){2, 3, 0};
        rule->steps[5] = (struct crush_rule_step){4, 0, 0};
        break;
    case 23: /* Direct FIRSTN: local fallback handles an out child. */
        rule->len = 5;
        rule->steps[0] = (struct crush_rule_step){11, 1, 0};
        rule->steps[1] = (struct crush_rule_step){11, -1, 0};
        rule->steps[2] = (struct crush_rule_step){1, -1, 0};
        rule->steps[3] = (struct crush_rule_step){2, 3, 0};
        rule->steps[4] = (struct crush_rule_step){4, 0, 0};
        break;
    case 24: /* Direct FIRSTN: zero fallback survives a negative repeat. */
        rule->len = 6;
        rule->steps[0] = (struct crush_rule_step){11, 1, 0};
        rule->steps[1] = (struct crush_rule_step){11, 0, 0};
        rule->steps[2] = (struct crush_rule_step){11, -1, 0};
        rule->steps[3] = (struct crush_rule_step){1, -1, 0};
        rule->steps[4] = (struct crush_rule_step){2, 3, 0};
        rule->steps[5] = (struct crush_rule_step){4, 0, 0};
        break;
    }
    map.working_size = sizeof(struct crush_work)
        + 3 * sizeof(struct crush_work_bucket *)
        + 3 * sizeof(struct crush_work_bucket) + 6 * sizeof(unsigned);
    /* Every scenario needs at most three result vectors. */
    void *work = calloc(1, map.working_size + 3 * count * sizeof(int));
    assert(work);
    crush_init_workspace(&map, work);
    unsigned mask_start = (scenario >= 13 && scenario <= 16) ||
                          (scenario >= 23 && scenario <= 24) ? 1 : 0;
    unsigned masks = scenario >= 11 ? mask_start + 1 : 16;
    for (unsigned mask = mask_start; mask < masks; ++mask) {
        unsigned weights[4];
        for (int i = 0; i < 4; ++i)
            weights[i] = mask & (1u << i) ? 0 : 65536;
        for (int x = 0; x < 100; ++x) {
            int out[3];
            int n = crush_do_rule(&map, 0, x, out, count, weights, 4, work, NULL);
            printf("%d %u %d %d", scenario, mask, x, n);
            for (int i = 0; i < n; ++i) printf(" %d", out[i]);
            puts("");
        }
    }
    free(work);
    free(rule);
}
