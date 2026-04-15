//! Concurrent throughput A/B: N-at-a-time write_full / read at 1 MiB
//! for both libraries.
//!
//! Run with a live vstart cluster:
//!   CEPH_CONF=... CEPH_LIB=... cargo bench --features bench-librados \
//!     --bench ab_concurrent

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::try_join_all;
use tokio::runtime::Runtime;

use common::adapter_librados::LibradosClient;
use common::adapter_rados_rs::RadosRsClient;
use common::client::BenchClient;
use common::fixture::{BenchEnv, PayloadCache, PoolFixture, SIZES, seed_write};

const CONCURRENCY_POINTS: &[usize] = &[1, 8, 32, 128];
const BENCH_SIZE: usize = 1024 * 1024;

struct BenchHarness {
    runtime: Runtime,
    payload: Bytes,
    clients: Vec<Arc<dyn BenchClient>>,
    _pool: PoolFixture,
}

impl BenchHarness {
    fn build() -> Self {
        let env = BenchEnv::from_env().expect("BenchEnv");
        let runtime = Runtime::new().expect("tokio runtime");
        let (pool, rados_rs, librados) = runtime.block_on(async {
            let pool = PoolFixture::new(&env).await.expect("PoolFixture");
            let rados_rs = RadosRsClient::connect(&env, pool.pool_name())
                .await
                .expect("rados-rs adapter");
            let librados = LibradosClient::connect(&env, &pool)
                .await
                .expect("librados adapter");
            (pool, rados_rs, librados)
        });
        let payloads = PayloadCache::new(SIZES);
        let harness = Self {
            runtime,
            payload: payloads.get(BENCH_SIZE),
            clients: vec![Arc::new(rados_rs), Arc::new(librados)],
            _pool: pool,
        };
        // Cluster warm-up: hit BOTH clients with a burst of 4 MiB writes
        // before measurement so neither library inherits the cold-cache
        // penalty (BlueStore deferred-IO settling, RocksDB compactions).
        // See `single_op.rs` for the full rationale.
        const WARMUP_ITERS: usize = 32;
        let warmup_payload = payloads.get(4 * 1024 * 1024);
        harness.runtime.block_on(async {
            for client in &harness.clients {
                let name = client.name();
                for i in 0..WARMUP_ITERS {
                    let oid = format!("cwarmup-{name}-{i}");
                    client
                        .write_full(&oid, warmup_payload.clone())
                        .await
                        .expect("warmup write_full");
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        harness
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);
fn next_oid(prefix: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{n}")
}

fn bench_concurrent_write(c: &mut Criterion, h: &BenchHarness) {
    let mut group = c.benchmark_group("concurrent_write_1MiB");
    for &concurrency in CONCURRENCY_POINTS {
        group.throughput(Throughput::Bytes((BENCH_SIZE * concurrency) as u64));
        for client in &h.clients {
            let id = BenchmarkId::new(client.name(), concurrency);
            let payload = h.payload.clone();
            let client = Arc::clone(client);
            group.bench_function(id, |b| {
                b.to_async(&h.runtime).iter_custom(|iters| {
                    let payload = payload.clone();
                    let client = Arc::clone(&client);
                    async move {
                        let start = Instant::now();
                        for _ in 0..iters {
                            let mut futs = Vec::with_capacity(concurrency);
                            for _ in 0..concurrency {
                                let oid = next_oid("cwf");
                                let p = payload.clone();
                                let c = Arc::clone(&client);
                                futs.push(async move { c.write_full(&oid, p).await });
                            }
                            try_join_all(futs).await.expect("concurrent write");
                        }
                        start.elapsed()
                    }
                });
            });
        }
    }
    group.finish();
}

fn bench_concurrent_read(c: &mut Criterion, h: &BenchHarness) {
    let mut group = c.benchmark_group("concurrent_read_1MiB");
    // Seed a handful of objects per client and rotate through them.
    const SEED_OBJECTS: usize = 16;
    for client in &h.clients {
        for i in 0..SEED_OBJECTS {
            let oid = format!("cread-seed-{}-{i}", client.name());
            h.runtime.block_on(seed_write(
                client.as_ref(),
                &oid,
                h.payload.clone(),
                "concurrent_read",
            ));
        }
    }

    for &concurrency in CONCURRENCY_POINTS {
        group.throughput(Throughput::Bytes((BENCH_SIZE * concurrency) as u64));
        for client in &h.clients {
            let id = BenchmarkId::new(client.name(), concurrency);
            let client = Arc::clone(client);
            let client_name = client.name().to_string();
            group.bench_function(id, |b| {
                b.to_async(&h.runtime).iter_custom(|iters| {
                    let client = Arc::clone(&client);
                    let client_name = client_name.clone();
                    async move {
                        let start = Instant::now();
                        for _ in 0..iters {
                            let mut futs = Vec::with_capacity(concurrency);
                            for i in 0..concurrency {
                                let oid = format!("cread-seed-{client_name}-{}", i % SEED_OBJECTS);
                                let c = Arc::clone(&client);
                                futs.push(async move { c.read(&oid, BENCH_SIZE).await });
                            }
                            try_join_all(futs).await.expect("concurrent read");
                        }
                        start.elapsed()
                    }
                });
            });
        }
    }
    group.finish();
}

fn bench_all(c: &mut Criterion) {
    let harness = BenchHarness::build();
    bench_concurrent_write(c, &harness);
    bench_concurrent_read(c, &harness);
}

criterion_group!(benches, bench_all);
criterion_main!(benches);
