/* Pinned Ceph hash.c consumer for generate-osdmap-primary-affinity-reference.py. */
#include "mapper.h"
#include "hash.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static uint32_t stable_mod(uint32_t value, uint32_t count) {
  uint32_t mask = count - 1;
  uint32_t masked = value & mask;
  return masked < count ? masked : value & (mask >> 1);
}

static uint32_t pps(unsigned pool, uint32_t seed) {
  uint32_t value = stable_mod(seed, 64);
  return pool == 2 ? crush_hash32_2(CRUSH_HASH_RJENKINS1, value, pool) : value + pool;
}

static int raw(unsigned pool, uint32_t seed, int *out) {
  static int root_items[] = {-3};
  static int host_items[] = {0, 1, 2, 3, 4, 5};
  static int rack_items[] = {-2};
  static unsigned root_weights[] = {393216};
  static unsigned host_weights[] = {65536, 65536, 65536, 65536, 65536, 65536};
  static unsigned rack_weights[] = {393216};
  static struct crush_bucket_straw2 root = {.h = {.id=-1,.type=11,.alg=5,.hash=0,.weight=393216,.size=1,.items=root_items},.item_weights=root_weights};
  static struct crush_bucket_straw2 host = {.h = {.id=-2,.type=1,.alg=5,.hash=0,.weight=393216,.size=6,.items=host_items},.item_weights=host_weights};
  static struct crush_bucket_straw2 rack = {.h = {.id=-3,.type=3,.alg=5,.hash=0,.weight=393216,.size=1,.items=rack_items},.item_weights=rack_weights};
  static struct crush_bucket *buckets[] = {&root.h, &host.h, &rack.h};
  static struct crush_rule *rules[2];
  static struct crush_map map = {.max_buckets=3,.max_rules=2,.max_devices=6,.buckets=buckets,.rules=rules,.choose_total_tries=50,.chooseleaf_descend_once=1,.chooseleaf_vary_r=1,.chooseleaf_stable=1};
  static void *work;
  if (!work) {
    rules[0] = calloc(1, crush_rule_size(3));
    rules[0]->type = 1; rules[0]->len = 3;
    rules[0]->steps[0] = (struct crush_rule_step){1,-1,0};
    rules[0]->steps[1] = (struct crush_rule_step){2,0,0};
    rules[0]->steps[2] = (struct crush_rule_step){4,0,0};
    rules[1] = calloc(1, crush_rule_size(5));
    rules[1]->type = 3; rules[1]->len = 5;
    rules[1]->steps[0] = (struct crush_rule_step){9,5,0};
    rules[1]->steps[1] = (struct crush_rule_step){8,100,0};
    rules[1]->steps[2] = (struct crush_rule_step){1,-1,0};
    rules[1]->steps[3] = (struct crush_rule_step){3,0,0};
    rules[1]->steps[4] = (struct crush_rule_step){4,0,0};
    map.working_size = sizeof(struct crush_work) + 3*sizeof(struct crush_work_bucket *) + 3*sizeof(struct crush_work_bucket) + 8*sizeof(unsigned);
    work = calloc(1, map.working_size + 9*sizeof(int));
    crush_init_workspace(&map, work);
  }
  unsigned weights[] = {65536, 65536, 65536, 65536, 65536, 65536};
  return crush_do_rule(&map, pool == 1 ? 1 : 0, pps(pool, seed), out, 3, weights, 6, work, NULL);
}

static void write_u32(uint32_t value) {
  unsigned char bytes[] = {value >> 24, value >> 16, value >> 8, value};
  if (fwrite(bytes, 1, sizeof(bytes), stdout) != sizeof(bytes)) abort();
}

static int primary(unsigned pool, unsigned state, uint32_t seed, int *osds, unsigned count) {
  int fallback = -1;
  for (unsigned i = 0; i < count; ++i) {
    unsigned affinity = osds[i] == 0 && state == 2 ? 0x8000 :
      (osds[i] < 2 && state != 0 ? 0 : 0x10000);
    if (affinity < 0x10000 &&
        (crush_hash32_2(CRUSH_HASH_RJENKINS1, pps(pool, seed), osds[i]) >> 16) >= affinity) {
      if (fallback < 0) fallback = i;
    } else {
      return i;
    }
  }
  return fallback;
}

int main(int argc, char **argv) {
  if (argc == 4 && argv[1][0] == 'p') {
    printf("%u\n", pps((unsigned)strtoul(argv[2], NULL, 10),
                         (uint32_t)strtoul(argv[3], NULL, 10)));
    return 0;
  }
  if (argc == 4 && argv[1][0] == 'r') {
    int osds[3];
    int count = raw((unsigned)strtoul(argv[2], NULL, 10), (uint32_t)strtoul(argv[3], NULL, 10), osds);
    printf("%d", count);
    for (int i = 0; i < count; ++i) printf(" %d", osds[i]);
    puts("");
    return 0;
  }
  unsigned pool, state, count;
  uint32_t seed;
  while (scanf("%u %u %u %u", &pool, &state, &seed, &count) == 4) {
    int osds[32];
    for (unsigned i = 0; i < count; ++i) if (scanf("%d", &osds[i]) != 1) return 2;
    int index = primary(pool, state, seed, osds, count);
    int selected_primary = index >= 0 ? osds[index] : -1;
    if (pool == 2 && index > 0) {
      int selected = osds[index];
      for (int i = index; i > 0; --i) osds[i] = osds[i - 1];
      osds[0] = selected;
    }
    write_u32(count);
    for (unsigned i = 0; i < count; ++i) write_u32((uint32_t)osds[i]);
    write_u32(count);
    for (unsigned i = 0; i < count; ++i) write_u32((uint32_t)osds[i]);
    write_u32((uint32_t)selected_primary);
    write_u32((uint32_t)selected_primary);
  }
}
