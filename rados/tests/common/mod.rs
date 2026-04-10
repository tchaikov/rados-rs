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

/// Build a [`rados::Client`] using the test cluster configuration.
///
/// Resolution order, highest precedence first:
/// 1. `CEPH_CONF` env var (required — the test is skipped if absent/missing).
/// 2. `CEPH_KEYRING` env var (overrides `keyring` in ceph.conf).
/// 3. Everything else comes from ceph.conf via [`rados::Client::builder`].
pub async fn build_test_client() -> Result<rados::Client, Box<dyn std::error::Error>> {
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

    Ok(builder.build().await?)
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
