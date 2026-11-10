/* Audit runner for the MSR, distribution and bad-mappings functional ports.
 * Link unmodified pinned Ceph mapper/hash/builder/crush.c; see verify-reference.py.
 * This reproduces test inputs, not the mapper algorithm.
 */
#include "mapper.h"
#include "builder.h"
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define W 65536u
static struct crush_map map;
static struct crush_bucket_straw2 buckets[12];
static struct crush_bucket *bucket_ptrs[12];
static int items[12][24];
static unsigned bucket_weights[12][24];
static struct crush_rule *rule;
static void *work;

static int add_bucket(int type, int size, unsigned weight)
{
    int index = map.max_buckets++;
    buckets[index] = (struct crush_bucket_straw2){
        .h = {.id = -1-index, .type = type, .alg = 5, .hash = 0,
              .size = size, .weight = size*weight, .items = items[index]},
        .item_weights = bucket_weights[index]};
    bucket_ptrs[index] = &buckets[index].h;
    for (int i = 0; i < size; ++i) bucket_weights[index][i] = weight;
    return index;
}

static void init_work(int count)
{
    map.buckets = bucket_ptrs;
    map.max_rules = 1;
    map.rules = &rule;
    map.working_size = sizeof(struct crush_work)
        + map.max_buckets * (sizeof(struct crush_work_bucket *)
                            + sizeof(struct crush_work_bucket));
    for (int i = 0; i < map.max_buckets; ++i)
        map.working_size += buckets[i].h.size * sizeof(unsigned);
    work = calloc(1, crush_work_size(&map, count));
    assert(work);
    crush_init_workspace(&map, work);
}

#ifndef QUINCY
static void place(unsigned x, int count, const unsigned *weights, int *out)
{
    int n = crush_do_rule(&map, 0, x, out, count, weights, map.max_devices, work, NULL);
    printf("ORACLE devices=%d x=%u count=%d weights=[", map.max_devices, x, count);
    for (int i = 0; i < map.max_devices; ++i) printf("%s%u", i ? ", " : "", weights[i]);
    printf("] out=[");
    for (int i = 0; i < n; ++i) printf("%s%d", i ? ", " : "", out[i]);
    puts("]");
    assert(n == count);
}

static void host_out(unsigned *weights, int osd, int per_host)
{
    int first = osd/per_host*per_host;
    for (int i = first; i < first+per_host; ++i) weights[i] = 0;
}

static void msr(int roots, int hosts, int osds, int choose_hosts, int choose_osds, int count)
{
    map = (struct crush_map){.max_devices = roots*hosts*osds,
        .choose_total_tries = 50, .chooseleaf_descend_once = 1,
        .chooseleaf_vary_r = 1, .chooseleaf_stable = 1,
        .msr_descents = 100, .msr_collision_tries = 100};
    rule = calloc(1, crush_rule_size(roots*4));
    assert(rule);
    rule->type = 5;
    rule->len = roots*4;
    for (int root = 0; root < roots; ++root) {
        int r = add_bucket(2, hosts, osds*W);
        for (int host = 0; host < hosts; ++host) {
            int h = add_bucket(1, osds, W);
            items[r][host] = -1-h;
            for (int i = 0; i < osds; ++i) items[h][i] = (root*hosts+host)*osds+i;
        }
        rule->steps[root*4] = (struct crush_rule_step){1, -1-r, 0};
        rule->steps[root*4+1] = (struct crush_rule_step){16, choose_hosts, 1};
        rule->steps[root*4+2] = (struct crush_rule_step){16, choose_osds, 0};
        rule->steps[root*4+3] = (struct crush_rule_step){4, 0, 0};
    }
    init_work(count);
    for (unsigned x = 0; x < (roots == 2 ? 1000 : 1); ++x) {
        unsigned weights[24];
        int before[24], after[24];
        for (int i = 0; i < map.max_devices; ++i) weights[i] = W;
        place(x, count, weights, before);
        if (roots == 2) {
            weights[before[1]] = weights[before[5]] = 0;
            place(x, count, weights, after);
            for (int i = 0; i < map.max_devices; ++i) weights[i] = W;
            host_out(weights, before[2], osds);
            host_out(weights, before[6], osds);
        } else {
            host_out(weights, before[0], osds);
        }
        place(x, count, weights, after);
        if (roots == 1 && hosts == 4) {
            for (int i = 0; i < map.max_devices; ++i) weights[i] = W;
            weights[before[0]] = 0;
            place(x, count, weights, after);
        }
    }
    free(work);
    free(rule);
}
#endif

static void distribution(int count)
{
    map = (struct crush_map){.max_devices = 5, .choose_local_tries = 2,
        .choose_local_fallback_tries = 5, .choose_total_tries = 19};
    add_bucket(1, 5, W);
    buckets[0].h.weight = 41*W;
    for (int i = 0; i < 5; ++i) {
        items[0][i] = i;
        bucket_weights[0][i] = (i == 4 ? 1 : 10)*W;
    }
    rule = calloc(1, crush_rule_size(3));
    assert(rule);
    rule->type = 1;
    rule->len = 3;
    rule->steps[0] = (struct crush_rule_step){1, -1, 0};
    rule->steps[1] = (struct crush_rule_step){2, 0, 0};
    rule->steps[2] = (struct crush_rule_step){4, 0, 0};
    init_work(count);
    unsigned weights[] = {W, W, W, W, W};
    long counts[5] = {0};
    for (int x = 1; x <= 1000000; ++x) {
        int out[3];
        int n = crush_do_rule(&map, 0, x, out, count, weights, 5, work, NULL);
        for (int i = 0; i < n; ++i) { assert(out[i] >= 0 && out[i] < 5); ++counts[out[i]]; }
    }
    printf("COUNTS replicas=%d [%ld, %ld, %ld, %ld, %ld]\n", count,
           counts[0], counts[1], counts[2], counts[3], counts[4]);
    free(work);
    free(rule);
}

static void bad_mappings(void)
{
    /* Original bad-mappings.crushmap.txt, with CrushCompiler's legacy defaults. */
    struct crush_map *m = crush_create();
    assert(m);
    m->choose_local_tries = 2;
    m->choose_local_fallback_tries = 5;
    m->choose_total_tries = 19;
    m->chooseleaf_descend_once = 0;
    m->chooseleaf_vary_r = 0;
    m->chooseleaf_stable = 0;
    m->straw_calc_version = 0;
    int devices[] = {0, 1, 2, 3, 4};
    int item_weights[] = {W, W, W, W, W};
    struct crush_bucket_straw *root = crush_make_straw_bucket(m, 0, 1, 5,
                                                             devices, item_weights);
    assert(root);
    for (int i = 0; i < 5; ++i) assert(root->straws[i] == W);
    assert(crush_add_bucket(m, -1, &root->h, NULL) == 0);
    for (int id = 0; id < 2; ++id) {
        struct crush_rule *r = crush_make_rule(3, id == 0 ? 1 : 3);
        assert(r);
        crush_rule_set_step(r, 0, CRUSH_RULE_TAKE, -1, 0);
        crush_rule_set_step(r, 1, id == 0 ? CRUSH_RULE_CHOOSE_FIRSTN : CRUSH_RULE_CHOOSE_INDEP, 0, 0);
        crush_rule_set_step(r, 2, CRUSH_RULE_EMIT, 0, 0);
        assert(crush_add_rule(m, r, id) == id);
    }
    crush_finalize(m);
    void *workspace = calloc(1, crush_work_size(m, 10));
    assert(workspace);
    crush_init_workspace(m, workspace);
    unsigned weights[] = {W, W, W, W, W};
    for (int id = 0; id < 2; ++id) {
        int out[10];
        int n = crush_do_rule(m, id, 1, out, 10, weights, 5, workspace, NULL);
        printf("BAD_MAPPINGS rule=%d out=[", id);
        for (int i = 0; i < n; ++i) printf("%s%d", i ? ", " : "", out[i]);
        puts("]");
    }
    free(workspace);
    crush_destroy(m);
}

static uint64_t digest(uint64_t hash, int n, const int *out)
{
    hash ^= (unsigned)n;
    hash *= UINT64_C(1099511628211);
    for (int i = 0; i < n; ++i) {
        hash ^= (unsigned)out[i];
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static struct crush_map *weight_map(int devices)
{
    struct crush_map *m = crush_create();
    assert(m);
    m->max_devices = devices;
    m->choose_local_tries = 0;
    m->choose_local_fallback_tries = 0;
    m->choose_total_tries = 50;
    m->chooseleaf_descend_once = 1;
    m->chooseleaf_vary_r = 1;
    m->chooseleaf_stable = 1;
    m->straw_calc_version = 1;
    return m;
}

static int add_weight_root(struct crush_map *m, int algorithm, int type, int size,
                           int *device_ids, int *weights)
{
    struct crush_bucket *root = crush_make_bucket(m, algorithm, 0, type, size,
                                                   device_ids, weights);
    assert(root);
    int id;
    assert(crush_add_bucket(m, 0, root, &id) == 0);
    return id;
}

static void add_weight_rule(struct crush_map *m, int id, int root)
{
    struct crush_rule *r = crush_make_rule(3, 1);
    assert(r);
    crush_rule_set_step(r, 0, CRUSH_RULE_TAKE, root, 0);
    crush_rule_set_step(r, 1, CRUSH_RULE_CHOOSE_FIRSTN, 0, 0);
    crush_rule_set_step(r, 2, CRUSH_RULE_EMIT, 0, 0);
    assert(crush_add_rule(m, r, id) == id);
}

static void weight_setup(void)
{
    int ids[15], zero_weights[5], same0[10], same1[10];
    for (int i = 0; i < 15; ++i) ids[i] = i;
    for (int i = 0; i < 5; ++i) zero_weights[i] = (4-i)*W;
    for (int i = 0; i < 10; ++i) {
        same0[i] = W*((i+1)/2+1);
        same1[i] = same0[i] + (i%2)*100;
    }

    struct crush_map *m = weight_map(5);
    int root0 = add_weight_root(m, CRUSH_BUCKET_STRAW, 1, 5, ids, zero_weights);
    int root1 = add_weight_root(m, CRUSH_BUCKET_STRAW, 1, 4, ids, zero_weights);
    add_weight_rule(m, 0, root0);
    add_weight_rule(m, 1, root1);
    crush_finalize(m);
    struct crush_bucket_straw *zero0 = (struct crush_bucket_straw *)m->buckets[-1-root0];
    struct crush_bucket_straw *zero1 = (struct crush_bucket_straw *)m->buckets[-1-root1];
    printf("STRAW zero0 weights=");
    for (int i = 0; i < 5; ++i) printf("%s%u", i ? "," : "", zero0->item_weights[i]);
    printf(" straws=");
    for (int i = 0; i < 5; ++i) printf("%s%u", i ? "," : "", zero0->straws[i]);
    printf(" zero1 weights=");
    for (int i = 0; i < 4; ++i) printf("%s%u", i ? "," : "", zero1->item_weights[i]);
    printf(" straws=");
    for (int i = 0; i < 4; ++i) printf("%s%u", i ? "," : "", zero1->straws[i]);
    puts("");
    void *workspace = calloc(1, crush_work_size(m, 1));
    crush_init_workspace(m, workspace);
    unsigned reweight[5] = {W, W, W, W, W};
    uint64_t hash = UINT64_C(1469598103934665603);
    for (unsigned x = 0; x < 10000; ++x) {
        int first[1], second[1];
        int n0 = crush_do_rule(m, 0, x, first, 1, reweight, 5, workspace, NULL);
        int n1 = crush_do_rule(m, 1, x, second, 1, reweight, 5, workspace, NULL);
        assert(n0 == 1 && n1 == 1 && first[0] == second[0]);
        hash = digest(hash, n0, first);
        hash = digest(hash, n1, second);
    }
    printf("WEIGHTS straw_zero digest=%016llx\n", (unsigned long long)hash);
    free(workspace);
    crush_destroy(m);

    m = weight_map(10);
    root0 = add_weight_root(m, CRUSH_BUCKET_STRAW, 1, 10, ids, same0);
    root1 = add_weight_root(m, CRUSH_BUCKET_STRAW, 1, 10, ids, same1);
    add_weight_rule(m, 0, root0);
    add_weight_rule(m, 1, root1);
    crush_finalize(m);
    struct crush_bucket_straw *straw0 = (struct crush_bucket_straw *)m->buckets[-1-root0];
    struct crush_bucket_straw *straw1 = (struct crush_bucket_straw *)m->buckets[-1-root1];
    printf("STRAW same0 weights=");
    for (int i = 0; i < 10; ++i) printf("%s%u", i ? "," : "", straw0->item_weights[i]);
    printf(" straws=");
    for (int i = 0; i < 10; ++i) printf("%s%u", i ? "," : "", straw0->straws[i]);
    printf(" same1 weights=");
    for (int i = 0; i < 10; ++i) printf("%s%u", i ? "," : "", straw1->item_weights[i]);
    printf(" straws=");
    for (int i = 0; i < 10; ++i) printf("%s%u", i ? "," : "", straw1->straws[i]);
    puts("");
    workspace = calloc(1, crush_work_size(m, 1));
    crush_init_workspace(m, workspace);
    unsigned reweight10[10];
    for (int i = 0; i < 10; ++i) reweight10[i] = W;
    hash = UINT64_C(1469598103934665603);
    int different = 0;
    for (unsigned x = 0; x < 100000; ++x) {
        int first[1], second[1];
        int n0 = crush_do_rule(m, 0, x, first, 1, reweight10, 10, workspace, NULL);
        int n1 = crush_do_rule(m, 1, x, second, 1, reweight10, 10, workspace, NULL);
        assert(n0 == 1 && n1 == 1);
        different += first[0] != second[0];
        hash = digest(hash, n0, first);
        hash = digest(hash, n1, second);
    }
    printf("WEIGHTS straw_same different=%d digest=%016llx\n", different, (unsigned long long)hash);
    free(workspace);
    crush_destroy(m);

    int reweight_weights[] = {W, W, 2*W, 2*W, 3*W, 5*W, W/2, 2*W,
                              W, W, 2*W, W, W, 2*W, 48*W};
    m = weight_map(15);
    root0 = add_weight_root(m, CRUSH_BUCKET_STRAW2, 1, 15, ids, reweight_weights);
    int random = rand() % 10;
    reweight_weights[1] = W / 10 * random;
    root1 = add_weight_root(m, CRUSH_BUCKET_STRAW2, 1, 15, ids, reweight_weights);
    add_weight_rule(m, 0, root0);
    add_weight_rule(m, 1, root1);
    crush_finalize(m);
    workspace = calloc(1, crush_work_size(m, 1));
    crush_init_workspace(m, workspace);
    unsigned reweight15[15];
    for (int i = 0; i < 15; ++i) reweight15[i] = W;
    hash = UINT64_C(1469598103934665603);
    for (unsigned x = 0; x < 1000000; ++x) {
        int first[1], second[1];
        int n0 = crush_do_rule(m, 0, x, first, 1, reweight15, 15, workspace, NULL);
        int n1 = crush_do_rule(m, 1, x, second, 1, reweight15, 15, workspace, NULL);
        assert(n0 == 1 && n1 == 1);
        assert(first[0] == 1 || first[0] == second[0]);
        hash = digest(hash, n0, first);
        hash = digest(hash, n1, second);
    }
    printf("WEIGHTS straw2_reweight rand_mod_10=%d changed_weight=%d digest=%016llx\n",
           random, reweight_weights[1], (unsigned long long)hash);
    free(workspace);
    crush_destroy(m);
}

static struct crush_map *indep_map(int racks, int hosts, int osds, int rule_type)
{
    int rack_ids[3], next = -2;
    for (int rack = 0; rack < racks; ++rack) {
        for (int host = 0; host < hosts; ++host) {
            next--;
            if (host == 0) rack_ids[rack] = next--;
        }
    }
    struct crush_map *m = weight_map(racks * hosts * osds);
    int root = add_weight_root(m, CRUSH_BUCKET_STRAW, 5, racks, rack_ids,
                               (int[3]) {hosts*osds*W, hosts*osds*W, hosts*osds*W});
    assert(root == -1);
    int device = 0;
    for (int rack = 0; rack < racks; ++rack) {
        int host_ids[3], rack_id = 0;
        for (int host = 0; host < hosts; ++host) {
            int devices[3];
            for (int osd = 0; osd < osds; ++osd) devices[osd] = device++;
            host_ids[host] = add_weight_root(m, CRUSH_BUCKET_STRAW2, 1, osds, devices,
                                              (int[3]) {W, W, W});
            if (host == 0) rack_id = add_weight_root(m, CRUSH_BUCKET_STRAW2, 3, hosts, host_ids,
                                                      (int[3]) {osds*W, osds*W, osds*W});
        }
        assert(rack_id == rack_ids[rack]);
        /* Later host IDs are appended after their rack; root already owns IDs. */
        struct crush_bucket *bucket = m->buckets[-1-rack_id];
        for (int host = 0; host < hosts; ++host) bucket->items[host] = host_ids[host];
    }
    struct crush_rule *r = crush_make_rule(4, rule_type);
    assert(r);
    crush_rule_set_step(r, 0, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 10, 0);
    crush_rule_set_step(r, 1, CRUSH_RULE_TAKE, root, 0);
    crush_rule_set_step(r, 2, CRUSH_RULE_CHOOSELEAF_INDEP, 0, 1);
    crush_rule_set_step(r, 3, CRUSH_RULE_EMIT, 0, 0);
    assert(crush_add_rule(m, r, 0) == 0);
    crush_finalize(m);
    return m;
}

static void indep_case(const char *name, int scenario)
{
    int racks = scenario == 0 ? 1 : 3;
    int hosts = 3, osds = scenario == 0 ? 1 : 3;
    struct crush_map *raw = indep_map(racks, hosts, osds, 123);
    struct crush_map *mapped = indep_map(racks, hosts, osds, 3);
    if (scenario >= 1) raw->choose_total_tries = mapped->choose_total_tries = 100;
    void *raw_work = calloc(1, crush_work_size(raw, 9));
    void *mapped_work = calloc(1, crush_work_size(mapped, 9));
    crush_init_workspace(raw, raw_work);
    crush_init_workspace(mapped, mapped_work);
    uint64_t hash = UINT64_C(1469598103934665603);
    unsigned vectors = 0;
    for (unsigned x = scenario == 4 ? 1 : 0; x < (scenario == 4 ? 5 : 100); ++x) {
        unsigned weights[27];
        for (int i = 0; i < raw->max_devices; ++i) weights[i] = W;
        if (scenario == 2) for (int i = 0; i < raw->max_devices / 2; ++i) weights[i*2] = 0;
        if (scenario == 3) for (int i = 0; i < raw->max_devices / 3; ++i) weights[i] = 0;
        int rounds = scenario == 4 ? raw->max_devices : 1;
        for (int i = 0; i < rounds; ++i) {
            int first[9], second[9];
            int count = scenario == 0 ? 5 : scenario == 2 ? 9 : scenario >= 3 ? 7 : 5;
            int n0 = crush_do_rule(raw, 0, x, first, count, weights, raw->max_devices, raw_work, NULL);
            int n1 = crush_do_rule(mapped, 0, x, second, count, weights, mapped->max_devices, mapped_work, NULL);
            assert(n0 == n1 && !memcmp(first, second, n0 * sizeof(*first)));
            hash = digest(hash, n0, first);
            ++vectors;
            if (scenario == 4) weights[i] = 0;
        }
    }
    printf("INDEP %s vectors=%u digest=%016llx\n", name, vectors, (unsigned long long)hash);
    free(raw_work);
    free(mapped_work);
    crush_destroy(raw);
    crush_destroy(mapped);
}

int main(void)
{
#ifndef QUINCY
    msr(1, 4, 3, 3, 1, 3);
    msr(1, 3, 2, 2, 2, 3);
    msr(1, 5, 4, 4, 4, 14);
    msr(2, 4, 3, 2, 2, 8);
#endif
    distribution(1);
    distribution(3);
    bad_mappings();
    weight_setup();
    indep_case("toosmall", 0);
    indep_case("basic", 1);
    indep_case("out_alt", 2);
    indep_case("out_contig", 3);
    indep_case("out_progressive", 4);
}
