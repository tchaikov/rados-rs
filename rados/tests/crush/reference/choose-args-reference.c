/* Local choose-argument differential generator. Link unmodified Ceph mapper.c/hash.c. */
#include "mapper.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
  assert(argc == 2);
  int scenario = atoi(argv[1]);
#ifdef QUINCY
  assert(scenario < 5);
#else
  assert(scenario < 7);
#endif
  int root_items[] = {-2, -3}, host0_items[] = {0, 1}, host1_items[] = {2, 3};
  unsigned root_weights[] = {131072, 131072}, host_weights[] = {65536, 65536};
  unsigned root_pos0[] = {131072, 65536}, root_pos1[] = {65536, 131072};
  unsigned host0_pos0[] = {65536, 0}, host0_pos1[] = {0, 65536};
  unsigned host1_pos0[] = {65536, 0}, host1_pos1[] = {0, 65536};
  int root_ids[] = {-20, 30}, host0_ids[] = {-450, 30}, host1_ids[] = {-20, -25};
  struct crush_bucket_straw2 root = {.h = {.id=-1,.type=2,.alg=5,.hash=0,.weight=262144,.size=2,.items=root_items},.item_weights=root_weights};
  struct crush_bucket_straw2 host0 = {.h = {.id=-2,.type=1,.alg=5,.hash=0,.weight=131072,.size=2,.items=host0_items},.item_weights=host_weights};
  struct crush_bucket_straw2 host1 = {.h = {.id=-3,.type=1,.alg=5,.hash=0,.weight=131072,.size=2,.items=host1_items},.item_weights=host_weights};
  struct crush_bucket *buckets[] = {&root.h,&host0.h,&host1.h};
  struct crush_rule *rule = calloc(1, crush_rule_size(4));
  struct crush_rule *rules[] = {rule};
  struct crush_map map = {.max_buckets=3,.max_rules=1,.max_devices=4,.buckets=buckets,.rules=rules,.choose_total_tries=50,.chooseleaf_descend_once=1,.chooseleaf_vary_r=1,.chooseleaf_stable=1};
#ifndef QUINCY
  map.msr_descents = 100; map.msr_collision_tries = 100;
#endif
  struct crush_weight_set root_sets[] = {{root_pos0,2},{root_pos1,2}};
  struct crush_weight_set host0_sets[] = {{host0_pos0,2},{host0_pos1,2}};
  struct crush_weight_set host1_sets[] = {{host1_pos0,2},{host1_pos1,2}};
  struct crush_choose_arg args[3] = {
    {.ids=root_ids,.ids_size=2,.weight_set=root_sets,.weight_set_positions=2},
    {.ids=host0_ids,.ids_size=2,.weight_set=host0_sets,.weight_set_positions=2},
    {.ids=host1_ids,.ids_size=2,.weight_set=host1_sets,.weight_set_positions=2},
  };
  struct crush_choose_arg empty_args[3] = {{0}};
  rule->type = 3; rule->len = 3;
  rule->steps[0] = (struct crush_rule_step){1,-1,0};
  rule->steps[1] = (struct crush_rule_step){2,3,0};
  rule->steps[2] = (struct crush_rule_step){4,0,0};
  if (scenario == 1) rule->steps[1] = (struct crush_rule_step){3,3,0};
  if (scenario == 2) rule->steps[1] = (struct crush_rule_step){6,3,1};
  if (scenario == 3) rule->steps[1] = (struct crush_rule_step){7,3,1};
  if (scenario == 4) {
    rule->len = 4;
    rule->steps[1] = (struct crush_rule_step){3,2,1};
    rule->steps[2] = (struct crush_rule_step){3,1,0};
    rule->steps[3] = (struct crush_rule_step){4,0,0};
  }
#ifndef QUINCY
  if (scenario == 5 || scenario == 6) {
    rule->type = scenario == 5 ? 5 : 4; rule->len = 4;
    rule->steps[1] = (struct crush_rule_step){16,2,1};
    rule->steps[2] = (struct crush_rule_step){16,2,0};
    rule->steps[3] = (struct crush_rule_step){4,0,0};
  }
#endif
  map.working_size = sizeof(struct crush_work) + 3*sizeof(struct crush_work_bucket *) + 3*sizeof(struct crush_work_bucket) + 6*sizeof(unsigned);
  void *work = calloc(1, map.working_size + 9*sizeof(int)); assert(work);
  crush_init_workspace(&map, work);
  for (int mode=0; mode<3; ++mode) for (int x=0; x<100; ++x) {
    unsigned weights[] = {65536,scenario >= 5 ? 0 : 65536,65536,65536}; int out[4];
    const struct crush_choose_arg *arg = mode == 0 ? NULL : mode == 1 ? empty_args : args;
    int n = crush_do_rule(&map,0,x,out,3,weights,4,work,arg);
    printf("%d %d %d %d",scenario,mode,x,n);
    for (int i=0;i<n;++i) printf(" %d",out[i]); puts("");
  }
  free(work); free(rule);
}
