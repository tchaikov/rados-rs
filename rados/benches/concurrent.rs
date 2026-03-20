use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rados::osdclient::MOSDOp;
use rados::osdclient::types::{OSDOp, ObjectId, RequestId, StripedPgId};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_ops");

    let rt = Runtime::new().unwrap();

    for num_threads in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("submit", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    rt.block_on(async {
                        let handles: Vec<_> = (0..num_threads)
                            .map(|i| {
                                tokio::spawn(async move {
                                    for _ in 0..100 {
                                        let object = ObjectId::new(1, &format!("obj_{}", i));
                                        let pgid = StripedPgId::from_pg(1, 0x12345678);
                                        let ops = vec![OSDOp::read(0, 4096)];
                                        let reqid = RequestId::new("client.0", 1, i);
                                        let mosdop =
                                            MOSDOp::new(1, 1, 0, object, pgid, ops, reqid, 0);
                                        black_box(Arc::new(mosdop));
                                    }
                                })
                            })
                            .collect();

                        for handle in handles {
                            handle.await.unwrap();
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_concurrent_operations);
criterion_main!(benches);
