//! Shared helpers for integration tests.
//!
//! All integration tests share the same setup: build a [`rados::Client`] from
//! the environment's `CEPH_CONF`, honour a handful of test-only env overrides
//! (`CEPH_KEYRING`, `CEPH_TEST_POOL`), and open a pool. Centralising the logic
//! here keeps each test file focused on assertions rather than cluster plumbing.
//!
//! The `#[allow(dead_code)]` attributes silence warnings in test binaries
//! that only use a subset of the helpers — cargo compiles this module fresh
//! for each test file.
//!
//! # Running against a rooket cluster
//!
//! The cluster suites, `rados-cls`'s included, need only `CEPH_CONF` and,
//! for the two that call the C++ CLI (`object_locator_routing` and
//! `osdclient_split_merge`), `CEPH_EXEC`. [rooket] provides both for a
//! Rook-managed cluster, which is how the aes256k cluster tests are run.
//!
//! In any directory, write two files:
//!
//! ```text
//! config.yaml                    profiles: [host-network]
//! values/rook-ceph-cluster.yaml  cephImage: {tag: v19.2.6}
//! ```
//!
//! Host networking puts the mons and OSDs on addresses the host can reach;
//! `rooket ceph-config` refuses a cluster without it. The pinned image is
//! the Ceph release under test. Then:
//!
//! ```text
//! rooket up --rook-version v1.20.7 --workers 1 --config-dir <dir> --wait
//! rooket ceph-config --out <conf-dir>
//! export CEPH_CONF=<conf-dir>/ceph.conf
//! export CEPH_EXEC="rooket k -n rook-ceph exec -i deploy/rook-ceph-tools --"
//! ```
//!
//! `ceph-config` writes `ceph.conf` and the admin keyring it names; re-run
//! it whenever the cluster is recreated, which makes a new admin key.
//! `CEPH_EXEC` runs the cluster's `ceph` and `rados` in Rook's toolbox. Its
//! default in the two CLI suites is compose's `docker exec -i ceph-mon`, so
//! on a rooket cluster it must be set, and `-i` is required because `rados
//! put` reads stdin.
//!
//! No pool setting is needed: rooket's one-worker base sets
//! `osd_pool_default_size = 1`, and Rook allows pool deletion and size-one
//! pools by default.
//!
//! Run the suites as `.github/workflows/test-with-ceph.yml` lists them.
//! `osdclient_integration_test` creates `test-pool`, and every later suite,
//! the `cephx_*` ones included, opens it without creating it, so it runs
//! first. `cephx_policy` changes the cluster's auth policy and runs only as
//! its own docs say.
//!
//! Compose sets `mon_max_pg_per_osd = 1000`, while Rook's single OSD keeps
//! Squid's 250. The suites fit within it, so a PG-limit refusal would be a
//! limit of this setup, not a regression. Nothing else here is
//! compose-specific.
//!
//! [rooket]: https://github.com/jhoblitt/rooket

#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

/// Install a `tracing_subscriber` that routes logs through the cargo test
/// writer. Safe to call from multiple tests in the same binary — `try_init`
/// silently returns `Err` if a subscriber is already set.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
}

/// Pool name for tests, overridable via `CEPH_TEST_POOL`.
pub fn test_pool_name() -> String {
    std::env::var("CEPH_TEST_POOL").unwrap_or_else(|_| "test-pool".to_owned())
}

/// A [`rados::ClientBuilder`] configured for the test cluster.
///
/// Resolution order, highest precedence first:
/// 1. `CEPH_CONF` env var (required — the test is skipped if absent/missing).
/// 2. `CEPH_KEYRING` env var (overrides `keyring` in ceph.conf).
/// 3. Everything else comes from ceph.conf via [`rados::Client::builder`].
pub fn test_client_builder() -> Result<rados::ClientBuilder, Box<dyn std::error::Error>> {
    let ceph_conf = std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".to_owned());
    if !Path::new(&ceph_conf).exists() {
        return Err(format!("ceph.conf not found at: {ceph_conf}").into());
    }

    let mut builder = rados::Client::builder()
        .config_file(&ceph_conf)
        .monmap_timeout(Duration::from_secs(5))
        .osdmap_timeout(Duration::from_secs(5));

    if let Ok(keyring) = std::env::var("CEPH_KEYRING") {
        builder = builder.keyring(keyring);
    }

    Ok(builder)
}

/// Build a [`rados::Client`] from [`test_client_builder`].
pub async fn build_test_client() -> Result<rados::Client, Box<dyn std::error::Error>> {
    Ok(test_client_builder()?.build().await?)
}

/// Build a client and open the configured test pool in a single call — the
/// common case for integration tests that only need one pool handle.
///
/// Note: the returned [`rados::IoCtx`] holds an `Arc<OSDClient>` internally,
/// so the temporary `Client` can be dropped immediately; the connection
/// stays alive via the `Arc`.
pub async fn create_ioctx() -> Result<rados::IoCtx, Box<dyn std::error::Error>> {
    let client = build_test_client().await?;
    let pool = test_pool_name();
    Ok(client.open_pool(&pool).await?)
}
