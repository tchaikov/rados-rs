//! Single-op latency A/B: write_full, read, stat, remove across
//! 4 KiB / 64 KiB / 1 MiB / 4 MiB for both libraries.
//!
//! Run with a live vstart cluster:
//!   CEPH_CONF=... CEPH_LIB=... cargo bench --features bench-librados \
//!     --bench ab_single_op

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use common::adapter_librados::LibradosClient;
use common::adapter_rados_rs::RadosRsClient;
use common::client::BenchClient;
use common::fixture::{BenchEnv, PayloadCache, PoolFixture, SIZES, seed_write};

struct BenchHarness {
    runtime: Runtime,
    payloads: PayloadCache,
    clients: Vec<Arc<dyn BenchClient>>,
    _pool: PoolFixture,
}

impl BenchHarness {
    fn build() -> Self {
        // Enable tracing output for rados-rs internals so io_loop errors,
        // session state transitions etc. are visible when running under
        // criterion (which otherwise swallows them).
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .try_init();

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
        let harness = Self {
            runtime,
            payloads: PayloadCache::new(SIZES),
            clients: vec![Arc::new(rados_rs), Arc::new(librados)],
            _pool: pool,
        };
        // Cluster warm-up: do a burst of 4 MiB writes via BOTH clients so that
        // BlueStore deferred-IO, RocksDB compactions, and the OSD page cache
        // are settled before criterion starts measuring. Without this, the
        // FIRST library benchmarked eats every cold-cache stall and looks
        // 20-40% slower on write_full at 4 MiB. With it, both libraries
        // measure against an equally warm cluster.
        const WARMUP_ITERS: usize = 32;
        let warmup_payload: Bytes = harness.payloads.get(4 * 1024 * 1024);
        harness.runtime.block_on(async {
            for client in &harness.clients {
                let name = client.name();
                for i in 0..WARMUP_ITERS {
                    let oid = format!("warmup-{name}-{i}");
                    client
                        .write_full(&oid, warmup_payload.clone())
                        .await
                        .expect("warmup write_full");
                }
            }
            // Let BlueStore deferred-IO and RocksDB compactions drain
            // before measurement starts. Without this delay the warmup
            // writes leave the cluster mid-commit and the first
            // measurement window inherits the backlog.
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

fn bench_write_full(c: &mut Criterion, h: &BenchHarness) {
    // Rotate through a small oid pool. Each iteration overwrites an existing
    // object, so we measure write latency instead of object-creation plus
    // PG/metadata churn. With a pool of 64 we still avoid write-after-write
    // coalescing on the OSD while keeping total objects bounded.
    const OID_POOL: usize = 64;

    let mut group = c.benchmark_group("write_full");
    // Big payloads: each sample has a few iters, so 30 samples ≈ 600 MB of
    // wire traffic at 4 MiB. Enough for a stable estimate without running all
    // night.
    group.sample_size(30);

    for &size in SIZES {
        group.throughput(Throughput::Bytes(size as u64));
        for client in &h.clients {
            let id = BenchmarkId::new(client.name(), size);
            let payload: Bytes = h.payloads.get(size);
            let client_name = client.name();
            let client = Arc::clone(client);

            // Pre-seed all oids so the first iteration isn't a create.
            for i in 0..OID_POOL {
                let oid = format!("wf-{client_name}-{size}-{i}");
                h.runtime.block_on(seed_write(
                    client.as_ref(),
                    &oid,
                    payload.clone(),
                    "write_full",
                ));
            }

            group.bench_function(id, |b| {
                b.to_async(&h.runtime).iter_custom(|iters| {
                    let payload = payload.clone();
                    let client = Arc::clone(&client);
                    async move {
                        let start = Instant::now();
                        for i in 0..iters {
                            let oid =
                                format!("wf-{client_name}-{size}-{}", (i as usize) % OID_POOL);
                            client
                                .write_full(&oid, payload.clone())
                                .await
                                .expect("write_full");
                        }
                        start.elapsed()
                    }
                });
            });
        }
    }
    group.finish();
}

fn bench_read(c: &mut Criterion, h: &BenchHarness) {
    let mut group = c.benchmark_group("read");
    group.sample_size(30);
    for &size in SIZES {
        group.throughput(Throughput::Bytes(size as u64));
        for client in &h.clients {
            // Seed one object to read repeatedly.
            let oid = format!("read-seed-{}-{size}", client.name());
            let payload: Bytes = h.payloads.get(size);
            h.runtime
                .block_on(seed_write(client.as_ref(), &oid, payload.clone(), "read"));

            let id = BenchmarkId::new(client.name(), size);
            let oid_cloned = oid.clone();
            let client = Arc::clone(client);
            group.bench_function(id, |b| {
                b.to_async(&h.runtime).iter_custom(|iters| {
                    let oid = oid_cloned.clone();
                    let client = Arc::clone(&client);
                    async move {
                        let start = Instant::now();
                        for _ in 0..iters {
                            let _ = client.read(&oid, size).await.expect("read");
                        }
                        start.elapsed()
                    }
                });
            });
        }
    }
    group.finish();
}

fn bench_stat(c: &mut Criterion, h: &BenchHarness) {
    let mut group = c.benchmark_group("stat");
    for &size in &[4 * 1024usize, 4 * 1024 * 1024] {
        for client in &h.clients {
            let oid = format!("stat-seed-{}-{size}", client.name());
            let payload: Bytes = h.payloads.get(size);
            h.runtime
                .block_on(seed_write(client.as_ref(), &oid, payload.clone(), "stat"));

            let id = BenchmarkId::new(client.name(), size);
            let oid_cloned = oid.clone();
            let client = Arc::clone(client);
            group.bench_function(id, |b| {
                b.to_async(&h.runtime).iter_custom(|iters| {
                    let oid = oid_cloned.clone();
                    let client = Arc::clone(&client);
                    async move {
                        let start = Instant::now();
                        for _ in 0..iters {
                            let _ = client.stat(&oid).await.expect("stat");
                        }
                        start.elapsed()
                    }
                });
            });
        }
    }
    group.finish();
}

fn bench_remove(c: &mut Criterion, h: &BenchHarness) {
    let mut group = c.benchmark_group("remove");
    group.sample_size(20);
    for &size in &[4 * 1024usize, 4 * 1024 * 1024] {
        for client in &h.clients {
            let id = BenchmarkId::new(client.name(), size);
            let payload: Bytes = h.payloads.get(size);
            let client = Arc::clone(client);
            group.bench_function(id, |b| {
                b.to_async(&h.runtime).iter_custom(|iters| {
                    let payload = payload.clone();
                    let client = Arc::clone(&client);
                    async move {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iters {
                            let oid = next_oid("rm");
                            client
                                .write_full(&oid, payload.clone())
                                .await
                                .expect("seed");
                            let start = Instant::now();
                            client.remove(&oid).await.expect("remove");
                            elapsed += start.elapsed();
                        }
                        elapsed
                    }
                });
            });
        }
    }
    group.finish();
}

fn bench_all(c: &mut Criterion) {
    let harness = BenchHarness::build();
    bench_write_full(c, &harness);
    bench_read(c, &harness);
    bench_stat(c, &harness);
    bench_remove(c, &harness);
}

criterion_group!(benches, bench_all);
criterion_main!(benches);
