use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use crush::{
    BucketAlgorithm, BucketData, CrushBucket, CrushMap, CrushRule, CrushRuleStep, RuleOp, RuleType,
};
use osdclient::osdmap::{OSDMap, PgId, PgPool};
use osdclient::types::PoolFlags;

fn create_test_crush_map(num_osds: usize, replica_count: usize) -> CrushMap {
    let mut map = CrushMap::new();
    map.max_devices = num_osds as i32;
    map.max_buckets = 1;

    let bucket = CrushBucket {
        id: -1,
        bucket_type: 1,
        alg: BucketAlgorithm::Straw2,
        hash: 0,
        weight: (0x10000 * num_osds) as u32,
        size: num_osds as u32,
        items: (0..num_osds as i32).collect(),
        data: BucketData::Straw2 {
            item_weights: vec![0x10000u32; num_osds],
        },
    };
    map.buckets = vec![Some(bucket)];

    let rule = CrushRule {
        rule_id: 0,
        rule_type: RuleType::Replicated,
        steps: vec![
            CrushRuleStep {
                op: RuleOp::Take,
                arg1: -1,
                arg2: 0,
            },
            CrushRuleStep {
                op: RuleOp::ChooseLeafFirstN,
                arg1: replica_count as i32,
                arg2: 0,
            },
            CrushRuleStep {
                op: RuleOp::Emit,
                arg1: 0,
                arg2: 0,
            },
        ],
    };
    map.rules = vec![Some(rule)];
    map
}

fn create_test_osdmap(num_osds: usize, replica_count: usize) -> OSDMap {
    let mut osdmap = OSDMap::new();
    osdmap.max_osd = num_osds as i32;
    osdmap.osd_weight = vec![0x10000u32; num_osds];
    osdmap.crush = Some(create_test_crush_map(num_osds, replica_count));

    let pool = PgPool {
        size: replica_count as u8,
        crush_rule: 0,
        pg_num: 256,
        flags: PoolFlags::HASHPSPOOL.bits(),
        ..Default::default()
    };
    osdmap.pools.insert(1, pool);
    osdmap
}

fn bench_pg_to_osds_cache_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("placement_cache");
    let osdmap = create_test_osdmap(100, 3);
    let pg = PgId { pool: 1, seed: 42 };

    osdmap.pg_to_osds(&pg).unwrap();

    group.bench_function("pg_to_osds_hit", |b| {
        b.iter(|| {
            let osds = osdmap.pg_to_osds(black_box(&pg)).unwrap();
            black_box(osds);
        });
    });

    group.finish();
}

fn bench_pg_to_osds_cold_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("placement_cache");
    let base = create_test_osdmap(100, 3);

    group.bench_function("pg_to_osds_cold_batch_128", |b| {
        b.iter_batched(
            || base.clone(),
            |osdmap| {
                for seed in 0..128 {
                    let pg = PgId { pool: 1, seed };
                    let osds = osdmap.pg_to_osds(black_box(&pg)).unwrap();
                    black_box(osds);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_object_to_osds_warm_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_placement");
    let osdmap = create_test_osdmap(100, 3);
    let object_names: Vec<String> = (0..128).map(|i| format!("object-{i}")).collect();

    for name in &object_names {
        osdmap.object_to_osds(1, name).unwrap();
    }

    group.bench_function("object_to_osds_warm_batch_128", |b| {
        b.iter(|| {
            for name in &object_names {
                let osds = osdmap.object_to_osds(1, black_box(name)).unwrap();
                black_box(osds);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_pg_to_osds_cache_hit,
    bench_pg_to_osds_cold_batch,
    bench_object_to_osds_warm_batch,
);
criterion_main!(benches);
