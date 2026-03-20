use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rados::crush::{
    BucketAlgorithm, BucketData, CrushBucket, CrushMap, CrushRule, CrushRuleStep, PgId, RuleOp,
    RuleType, pg_to_osds,
};

/// Create a simple test CRUSH map with the specified number of OSDs
fn create_test_crush_map(num_osds: usize, replica_count: usize) -> CrushMap {
    let mut map = CrushMap::new();
    map.max_devices = num_osds as i32;
    map.max_buckets = 1;

    // Create item weights (all equal)
    let item_weights = vec![0x10000u32; num_osds];

    // Create a single root bucket containing all OSDs
    let bucket = CrushBucket {
        id: -1,
        bucket_type: 1, // Type 1 = host
        alg: BucketAlgorithm::Straw2,
        hash: 0, // CRUSH_HASH_RJENKINS1
        weight: (0x10000 * num_osds) as u32,
        size: num_osds as u32,
        items: (0..num_osds as i32).collect(),
        data: BucketData::Straw2 { item_weights },
    };

    map.buckets = vec![Some(bucket)];

    // Create a replicated rule: TAKE -1, CHOOSELEAF_FIRSTN N type 0, EMIT
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
                arg2: 0, // Type 0 = device
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

fn bench_single_placement(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_placement");

    for replica_count in [1, 3, 5, 7] {
        let map = create_test_crush_map(100, replica_count);
        let weights = vec![0x10000u32; 100];
        let pg = PgId::new(1, 42);

        group.bench_with_input(
            BenchmarkId::new("replicas", replica_count),
            &replica_count,
            |b, &_replica_count| {
                b.iter(|| {
                    let result = pg_to_osds(
                        black_box(&map),
                        black_box(pg),
                        black_box(0),
                        black_box(&weights),
                        black_box(replica_count),
                        black_box(true),
                    )
                    .unwrap();
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

fn bench_batch_placement(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_placement");

    for replica_count in [1, 3, 5, 7] {
        let map = create_test_crush_map(100, replica_count);
        let weights = vec![0x10000u32; 100];

        group.bench_with_input(
            BenchmarkId::new("replicas", replica_count),
            &replica_count,
            |b, &_replica_count| {
                b.iter(|| {
                    // Map 100 PGs to OSDs
                    for i in 0..100 {
                        let pg = PgId::new(1, i);
                        let result = pg_to_osds(
                            black_box(&map),
                            black_box(pg),
                            black_box(0),
                            black_box(&weights),
                            black_box(replica_count),
                            black_box(true),
                        )
                        .unwrap();
                        black_box(result);
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_single_placement, bench_batch_placement);
criterion_main!(benches);
