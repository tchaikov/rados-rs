use anyhow::{Context, Result};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::env;
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::librados::{IoCtx, Rados};

pub const SIZES: &[usize] = &[
    4 * 1024,        // 4 KiB
    64 * 1024,       // 64 KiB
    1024 * 1024,     // 1 MiB
    4 * 1024 * 1024, // 4 MiB
];

#[derive(Debug, Clone)]
pub struct BenchEnv {
    pub ceph_conf: String,
    pub ceph_user: String,
    pub pool_name: String,
}

impl BenchEnv {
    pub fn from_env() -> Result<Self> {
        let ceph_conf = env::var("CEPH_CONF")
            .context("CEPH_CONF must be set to the path of a ceph.conf file")?;
        let ceph_user = env::var("CEPH_USER").unwrap_or_else(|_| "client.admin".to_string());
        let pool_name =
            env::var("BENCH_POOL").unwrap_or_else(|_| format!("bench-ab-{}", process::id()));

        Ok(Self {
            ceph_conf,
            ceph_user,
            pool_name,
        })
    }
}

/// Pre-allocated payload per size. Shared across iterations and libraries
/// so we never measure allocator noise.
pub struct PayloadCache {
    payloads: BTreeMap<usize, Bytes>,
}

impl PayloadCache {
    pub fn new(sizes: &[usize]) -> Self {
        let payloads = sizes
            .iter()
            .map(|&size| (size, Bytes::from(vec![0x5Au8; size])))
            .collect();
        Self { payloads }
    }

    pub fn get(&self, size: usize) -> Bytes {
        self.payloads
            .get(&size)
            .cloned()
            .unwrap_or_else(|| panic!("no cached payload for size {size}"))
    }
}

/// Owns the benchmark pool. Creates it on `new` and deletes it on `drop`
/// (best effort).
pub struct PoolFixture {
    rados: Arc<Rados>,
    pool_name: String,
}

impl PoolFixture {
    pub async fn new(env: &BenchEnv) -> Result<Self> {
        // rados_create wants the short id (e.g. "admin"), not "client.admin".
        let short_user = env
            .ceph_user
            .strip_prefix("client.")
            .unwrap_or(&env.ceph_user);
        let mut rados =
            Rados::new(short_user).with_context(|| format!("rados_create({short_user})"))?;
        rados
            .read_conf_file(&env.ceph_conf)
            .with_context(|| format!("rados_conf_read_file({})", env.ceph_conf))?;
        let rados = rados.connect().await.context("rados_connect failed")?;

        // Idempotent: ignore EEXIST so reruns against the same pool work.
        if let Err(e) = rados.create_pool(&env.pool_name) {
            let msg = e.to_string();
            if !msg.contains("File exists") && !msg.contains("EEXIST") {
                return Err(anyhow::anyhow!(
                    "rados_pool_create({}) failed: {e}",
                    env.pool_name
                ));
            }
        }

        // Stabilise the pool layout before any measured I/O.  The default
        // `rados_pool_create` path lets the mgr autoscaler choose pg_num /
        // pgp_num and then gradually raise pgp_num toward the target.  During
        // that ramp, acting sets for individual PGs shift as peering completes,
        // which produces spurious ENOENT on write-then-remove sequences and
        // skews latency noise upward.  Pin pg_num explicitly and turn the
        // autoscaler off so the placement is frozen for the duration of the
        // run.  Best-effort: if the CLI isn't on PATH, fall through and hope
        // for the best.
        pin_pool_placement(&env.ceph_conf, &env.pool_name);

        let rados = Arc::new(rados);

        // Wait for all PGs in the new pool to become active.
        // Pool creation is acknowledged by the monitor immediately, but the
        // OSDs need a few seconds to peer the new PGs.  Seeding writes that
        // arrive before peering completes will block until the PG activates
        // (up to the client's 30 s operation timeout).  We avoid that by
        // probing with a tiny write until the pool accepts I/O.
        wait_for_pool_active(&rados, &env.pool_name).await;

        Ok(Self {
            rados,
            pool_name: env.pool_name.clone(),
        })
    }

    pub fn rados(&self) -> &Arc<Rados> {
        &self.rados
    }

    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }
}

/// Seed a single object, retrying on timeout or connection errors.
///
/// Seeding writes happen outside the timed path but must succeed before the
/// bench group starts.  Transient PG peering gaps (particularly after pool
/// creation or OSD restarts) can make individual writes time out or fail with
/// a connection error.  Retrying a handful of times eliminates the noise without
/// making setup brittle.
pub async fn seed_write<C>(client: &C, oid: &str, payload: bytes::Bytes, label: &str)
where
    C: super::client::BenchClient + ?Sized,
{
    const MAX_ATTEMPTS: u32 = 8;
    const RETRY_DELAY: Duration = Duration::from_secs(2);

    for attempt in 1..=MAX_ATTEMPTS {
        match client.write_full(oid, payload.clone()).await {
            Ok(()) => return,
            Err(e) => {
                if attempt == MAX_ATTEMPTS {
                    panic!("seed write {label} {oid}: all {MAX_ATTEMPTS} attempts failed: {e:#}");
                }
                eprintln!(
                    "warn: seed write {label} {oid} attempt {attempt}/{MAX_ATTEMPTS} \
                     failed ({e:#}); retrying in {RETRY_DELAY:?}"
                );
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

/// Freeze pool placement: pin pg_num/pgp_num and disable the autoscaler.
///
/// Runs the `ceph` CLI against the same ceph.conf the bench connects to.
/// Best-effort — the bench still runs if the CLI is missing, it just may
/// see more noise from background PG splits.
fn pin_pool_placement(ceph_conf: &str, pool_name: &str) {
    const PG_NUM: &str = "32";

    let ceph = env::var("CEPH_BIN").unwrap_or_else(|_| "ceph".to_string());
    let settings: &[(&str, &str)] = &[
        ("pg_autoscale_mode", "off"),
        ("pg_num", PG_NUM),
        ("pgp_num", PG_NUM),
    ];
    for (key, value) in settings {
        let status = std::process::Command::new(&ceph)
            .args([
                "--conf", ceph_conf, "osd", "pool", "set", pool_name, key, value,
            ])
            .status();
        match status {
            Ok(s) if s.success() => {}
            Ok(s) => eprintln!(
                "warn: `{ceph} osd pool set {pool_name} {key} {value}` exited {s}; \
                 pool may reshuffle mid-bench"
            ),
            Err(e) => eprintln!(
                "warn: failed to exec `{ceph}`: {e}; pool layout not pinned, \
                 expect noise from autoscaler"
            ),
        }
    }
}

/// Poll the pool with a tiny write+remove until the PGs activate, or give up
/// after 60 seconds and let the bench attempt writes anyway.
async fn wait_for_pool_active(rados: &Arc<Rados>, pool_name: &str) {
    const SENTINEL: &str = "_bench_pool_ready";
    const POLL_INTERVAL: Duration = Duration::from_secs(1);
    const TIMEOUT: Duration = Duration::from_secs(60);

    let ioctx = match IoCtx::from_rados(Arc::clone(rados), pool_name) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("wait_for_pool_active: could not open IoCtx: {e}");
            return;
        }
    };

    let deadline = Instant::now() + TIMEOUT;
    loop {
        match ioctx.write_full(SENTINEL, &[]).await {
            Ok(()) => {
                let _ = ioctx.remove(SENTINEL).await;
                return;
            }
            Err(e) => {
                if Instant::now() >= deadline {
                    eprintln!(
                        "warn: pool {pool_name} PGs not active after {TIMEOUT:?}: {e}; \
                         proceeding anyway"
                    );
                    return;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    }
}

impl Drop for PoolFixture {
    fn drop(&mut self) {
        if let Err(e) = self.rados.delete_pool(&self.pool_name) {
            eprintln!(
                "warning: failed to delete bench pool {}: {e}",
                self.pool_name
            );
        }
    }
}
