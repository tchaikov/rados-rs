//! CPU + peak-RSS harness for the `rados` crate.
//!
//! A/B bench tool that runs a defined workload (write_full + read) against
//! a live Ceph cluster with ONE library per process, so glibc malloc does
//! not smear RSS between them. Run once per library and compare the two
//! reports:
//!
//! ```text
//! cargo run --release --features bench-librados --example bench_resources -- \
//!     resources --library rados-rs --size 4194304 --iters 200
//! cargo run --release --features bench-librados --example bench_resources -- \
//!     resources --library librados --size 4194304 --iters 200
//! ```

#[path = "../benches/common/mod.rs"]
mod common;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use std::sync::Arc;
use tracing::{info, warn};

use common::adapter_librados::LibradosClient;
use common::adapter_rados_rs::RadosRsClient;
use common::client::BenchClient;
use common::fixture::{BenchEnv, PayloadCache, PoolFixture, SIZES};
use common::resource::ResourceSampler;

#[derive(Parser, Debug)]
#[command(name = "rados-bench", about = "A/B benchmark for rados-rs vs librados")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// One iteration of each op on each library. Catches wiring bugs before
    /// spending minutes in criterion.
    Smoke {
        #[arg(long, value_enum, default_value_t = Library::Both)]
        library: Library,
    },
    /// Measure CPU + peak RSS of a defined workload for ONE library.
    /// Run once per library and compare the outputs; using one library
    /// per process guarantees clean RSS attribution (the glibc allocator
    /// does not fully return freed heap to the OS between runs, so
    /// mixing both libraries in one process smears the RSS deltas).
    Resources {
        #[arg(long, value_enum)]
        library: Library,
        /// Payload size in bytes
        #[arg(long, default_value_t = 4 * 1024 * 1024)]
        size: usize,
        /// Number of write_full + read op pairs
        #[arg(long, default_value_t = 200)]
        iters: usize,
    },
    /// Delete any pool matching bench-ab-* (recovery for panicked runs).
    Cleanup,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Library {
    RadosRs,
    Librados,
    Both,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,rados_bench=info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Smoke { library } => smoke(library).await,
        Commands::Resources {
            library,
            size,
            iters,
        } => resources(library, size, iters).await,
        Commands::Cleanup => {
            warn!(
                "cleanup mode not yet implemented — please drop bench-ab-* pools via `ceph osd pool rm`"
            );
            Ok(())
        }
    }
}

async fn smoke(library: Library) -> Result<()> {
    let env = BenchEnv::from_env()?;
    info!(
        "bench environment: conf={} user={} pool={}",
        env.ceph_conf, env.ceph_user, env.pool_name
    );

    let pool = PoolFixture::new(&env).await.context("pool fixture setup")?;
    info!("pool '{}' ready", pool.pool_name());

    let payloads = PayloadCache::new(SIZES);

    let mut clients: Vec<Arc<dyn BenchClient>> = Vec::new();
    if matches!(library, Library::RadosRs | Library::Both) {
        let c = RadosRsClient::connect(&env, pool.pool_name())
            .await
            .context("rados-rs adapter connect")?;
        clients.push(Arc::new(c));
    }
    if matches!(library, Library::Librados | Library::Both) {
        let c = LibradosClient::connect(&env, &pool)
            .await
            .context("librados adapter connect")?;
        clients.push(Arc::new(c));
    }

    for client in &clients {
        info!("--- smoke: {}", client.name());
        for &size in SIZES {
            run_one(client.as_ref(), &payloads, size).await?;
        }
    }

    info!("smoke test passed");
    Ok(())
}

async fn run_one(client: &dyn BenchClient, payloads: &PayloadCache, size: usize) -> Result<()> {
    let oid = format!("smoke-{}-{size}", client.name());
    let payload: Bytes = payloads.get(size);

    let sampler = ResourceSampler::start();

    client
        .write_full(&oid, payload.clone())
        .await
        .with_context(|| format!("{} write_full {size}", client.name()))?;
    let read = client
        .read(&oid, size)
        .await
        .with_context(|| format!("{} read {size}", client.name()))?;
    anyhow::ensure!(
        read.len() == size,
        "{} read returned {} bytes, expected {size}",
        client.name(),
        read.len()
    );
    let stat = client
        .stat(&oid)
        .await
        .with_context(|| format!("{} stat {size}", client.name()))?;
    anyhow::ensure!(
        stat.size as usize == size,
        "{} stat reported size {}, expected {size}",
        client.name(),
        stat.size
    );
    client
        .remove(&oid)
        .await
        .with_context(|| format!("{} remove {size}", client.name()))?;

    let delta = sampler.stop().await;
    info!(
        "  {:>9} size {:>8} ok: user_cpu={:?} sys_cpu={:?} peak_rss_Δ={} KiB net_rss_Δ={} KiB",
        client.name(),
        size,
        delta.user_cpu,
        delta.sys_cpu,
        delta.peak_rss_delta / 1024,
        delta.net_rss_delta / 1024,
    );

    Ok(())
}

async fn resources(library: Library, size: usize, iters: usize) -> Result<()> {
    if matches!(library, Library::Both) {
        anyhow::bail!(
            "--library=both is not supported here: run once per library so glibc allocator \
             doesn't smear RSS between them"
        );
    }

    let env = BenchEnv::from_env()?;
    let pool = PoolFixture::new(&env).await.context("pool fixture setup")?;
    info!("pool '{}' ready", pool.pool_name());

    let payloads = PayloadCache::new(SIZES);
    let payload: Bytes = payloads.get(size);

    let client: Arc<dyn BenchClient> = match library {
        Library::RadosRs => Arc::new(
            RadosRsClient::connect(&env, pool.pool_name())
                .await
                .context("rados-rs adapter connect")?,
        ),
        Library::Librados => Arc::new(
            LibradosClient::connect(&env, &pool)
                .await
                .context("librados adapter connect")?,
        ),
        Library::Both => unreachable!(),
    };

    // Warm up so the first-iter cold path doesn't skew CPU accounting.
    const WARMUP: usize = 32;
    info!("warming up: {} × write_full @ {} bytes", WARMUP, size);
    for i in 0..WARMUP {
        let oid = format!("resource-warmup-{i}");
        client.write_full(&oid, payload.clone()).await?;
    }
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Measurement window: `iters` × (write_full + read).
    info!(
        "measuring {} library: {} iters × (write_full + read) @ {} bytes",
        client.name(),
        iters,
        size
    );

    let wall_start = std::time::Instant::now();
    let sampler = ResourceSampler::start();
    for i in 0..iters {
        let oid = format!("resource-op-{i}");
        client
            .write_full(&oid, payload.clone())
            .await
            .with_context(|| format!("write_full {i}"))?;
        let read = client
            .read(&oid, size)
            .await
            .with_context(|| format!("read {i}"))?;
        anyhow::ensure!(read.len() == size, "short read at iter {i}");
    }
    let delta = sampler.stop().await;
    let wall = wall_start.elapsed();

    // Cleanup: remove the measurement objects so the next run starts clean.
    for i in 0..iters {
        let oid = format!("resource-op-{i}");
        let _ = client.remove(&oid).await;
    }
    for i in 0..WARMUP {
        let oid = format!("resource-warmup-{i}");
        let _ = client.remove(&oid).await;
    }

    let ops = iters * 2; // write_full + read per iter
    let bytes_transferred = (iters as u64) * (size as u64) * 2;
    let wall_secs = wall.as_secs_f64();
    let cpu_total = delta.user_cpu + delta.sys_cpu;
    let cpu_per_op_us = cpu_total.as_micros() as f64 / ops as f64;
    let cpu_util_pct = cpu_total.as_secs_f64() / wall_secs * 100.0;

    println!();
    println!("=== {} resource report ===", client.name());
    println!("workload     : {iters} iters × (write_full + read) @ {size} B");
    println!("ops          : {ops}");
    println!("bytes        : {} MiB", bytes_transferred / (1024 * 1024));
    println!("wall clock   : {wall:?}");
    println!("user cpu     : {:?}", delta.user_cpu);
    println!("sys  cpu     : {:?}", delta.sys_cpu);
    println!("total cpu    : {cpu_total:?}");
    println!("cpu / op     : {cpu_per_op_us:.1} µs");
    println!("cpu util     : {cpu_util_pct:.1}% of wall");
    println!(
        "peak rss Δ   : {} KiB  ({:.1} MiB)",
        delta.peak_rss_delta / 1024,
        delta.peak_rss_delta as f64 / (1024.0 * 1024.0)
    );
    println!(
        "net  rss Δ   : {} KiB  ({:.1} MiB)",
        delta.net_rss_delta / 1024,
        delta.net_rss_delta as f64 / (1024.0 * 1024.0)
    );
    println!();

    Ok(())
}
