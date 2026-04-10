//! High-level RADOS client.
//!
//! [`Client`] is the blessed entry point into the crate. It owns a
//! [`MonClient`] + [`OSDClient`] pair, handles the full connection dance
//! (ceph.conf parsing, keyring loading, msgr2 channel wiring, waiting for the
//! first MonMap and OSDMap), and hands out [`IoCtx`] values by pool name.
//!
//! ```no_run
//! # async fn run() -> Result<(), rados::ClientError> {
//! // One-liner for the common case:
//! let client = rados::connect("/etc/ceph/ceph.conf").await?;
//! let ioctx = client.open_pool("my-pool").await?;
//! # drop(ioctx);
//! # Ok(())
//! # }
//! ```
//!
//! Or with per-field customization via the builder:
//!
//! ```no_run
//! use std::time::Duration;
//! # async fn run() -> Result<(), rados::ClientError> {
//! let client = rados::Client::builder()
//!     .entity_name("client.admin")
//!     .config_file("/etc/ceph/ceph.conf")
//!     .operation_timeout(Duration::from_secs(30))
//!     .build()
//!     .await?;
//! # drop(client);
//! # Ok(())
//! # }
//! ```
//!
//! The submodule types ([`MonClient`], [`OSDClient`], their configs) remain
//! public as an advanced escape hatch, but most code should never need to
//! touch them directly.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::cephconfig::{CephConfig, ConfigError};
use crate::monclient::{AuthConfig, MOSDMap, MonClient, MonClientConfig, MonClientError};
use crate::msgr2::map_channel;
use crate::osdclient::client::default_client_inc;
use crate::osdclient::tracker::TrackerConfig;
use crate::osdclient::{IoCtx, OSDClient, OSDClientConfig, OSDClientError};

/// Depth of the internal msgr2 map channel used to hand OSDMap updates from
/// the MonClient receive task to the OSDClient drain task. Sized to absorb a
/// burst of incremental OSDMaps without stalling the receiver. Not exposed as
/// a builder setter — internal plumbing that no external consumer should need
/// to tune.
const MAP_CHANNEL_BUFFER: usize = 64;

/// One-liner convenience for the common case: parse `ceph.conf` and connect.
///
/// Equivalent to `Client::builder().config_file(path).build().await`. Named
/// after the pattern used by [`tokio::net::TcpStream::connect`] — a free
/// `async fn` for the happy path that sits alongside the full builder for
/// the cases where setters are actually needed.
pub async fn connect(config_file: impl Into<PathBuf>) -> Result<Client, ClientError> {
    Client::builder().config_file(config_file).build().await
}

/// High-level RADOS client.
///
/// Owns the shared [`MonClient`] + [`OSDClient`] pair. A single `Client`
/// handles the cluster connection once; each pool lookup via [`open_pool`]
/// is a cheap [`IoCtx`] construction on the shared [`OSDClient`].
///
/// Cheap to clone — wraps `Arc`s internally.
///
/// [`open_pool`]: Client::open_pool
#[derive(Clone)]
#[must_use = "Client holds a live cluster connection; dropping it without using it tears everything down"]
pub struct Client {
    mon_client: Arc<MonClient>,
    osd_client: Arc<OSDClient>,
}

impl Client {
    /// Start a new [`ClientBuilder`] with default settings.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Open a pool by name, returning an [`IoCtx`] for object operations.
    ///
    /// Tries the currently cached OSDMap first (near-free). On cache miss,
    /// refreshes the OSDMap once — this handles the "create pool then open
    /// immediately" race at the cost of one monitor round-trip. Returns
    /// [`OSDClientError::PoolNameNotFound`] if the pool does not exist after
    /// the refresh.
    pub async fn open_pool(&self, name: &str) -> Result<IoCtx, OSDClientError> {
        // Fast path: consult the cached OSDMap.
        if let Some(pool_id) = self.osd_client.get_osdmap().await?.pool_id_by_name(name) {
            return IoCtx::new(self.osd_client.clone(), pool_id).await;
        }

        // Slow path: the cached map may be stale (e.g. the pool was created
        // after the last refresh). Ask the monitor for the latest, then retry
        // exactly once. A single bounded refresh is enough — a loop would
        // waste monitor round-trips on genuinely missing pools.
        self.osd_client
            .wait_for_latest_osdmap(Duration::from_secs(5))
            .await?;
        let pool_id = self
            .osd_client
            .get_osdmap()
            .await?
            .pool_id_by_name(name)
            .ok_or_else(|| OSDClientError::PoolNameNotFound(name.to_owned()))?;
        IoCtx::new(self.osd_client.clone(), pool_id).await
    }

    /// Advanced: access the underlying [`MonClient`] for operations the
    /// high-level API does not yet cover (mon commands, service subscriptions).
    pub fn mon_client(&self) -> &Arc<MonClient> {
        &self.mon_client
    }

    /// Advanced: access the underlying [`OSDClient`] for operations the
    /// high-level API does not yet cover (cross-pool ops, raw OSDMap access).
    pub fn osd_client(&self) -> &Arc<OSDClient> {
        &self.osd_client
    }

    /// Gracefully shut down both the OSDClient and the MonClient.
    ///
    /// Cancels every background task spawned by the two sub-clients (per-OSD
    /// session I/O loops, the OSDMap drain task, the timeout tracker, the
    /// MonClient hunter / drain / tick loops, every MonConnection I/O task),
    /// waits for them to finish, and returns once the connection is torn
    /// down. Idempotent — calling twice is a no-op on the second call.
    ///
    /// After `shutdown().await` returns, any in-flight [`IoCtx`] operation
    /// will fail with a connection error. It is safe to drop the `Client`
    /// without calling `shutdown()` — the `Drop` impls of the sub-clients
    /// cancel the same tokens — but the explicit call is the only way to
    /// *await* completion of the background tasks, so tests and any code
    /// that cares about ordered teardown should prefer it.
    pub async fn shutdown(&self) -> Result<(), ClientError> {
        // Order matters: stop the OSD side first so its sessions don't try
        // to send keepalives or commands to a MonClient that's already
        // mid-teardown. The OSDClient shutdown is infallible; only the
        // MonClient shutdown can surface an error.
        self.osd_client.shutdown().await;
        self.mon_client.shutdown().await?;
        Ok(())
    }
}

/// Builder for [`Client`].
///
/// All fields are optional. `config_file` is the most common entry point and
/// will populate `mon_addrs`, the keyring path, and the DNS SRV name from
/// ceph.conf. Explicit setters override values from the file when both are
/// provided.
#[derive(Debug, Clone)]
#[must_use = "ClientBuilder does nothing until build() is awaited"]
pub struct ClientBuilder {
    entity_name: String,
    config_file: Option<PathBuf>,
    mon_addrs: Vec<String>,
    keyring_path: Option<PathBuf>,
    dns_srv_name: Option<String>,
    operation_timeout: Duration,
    monmap_wait_timeout: Duration,
    osdmap_wait_timeout: Duration,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            entity_name: "client.admin".to_owned(),
            config_file: None,
            mon_addrs: Vec::new(),
            keyring_path: None,
            dns_srv_name: None,
            operation_timeout: Duration::from_secs(30),
            monmap_wait_timeout: Duration::from_secs(10),
            osdmap_wait_timeout: Duration::from_secs(10),
        }
    }
}

impl ClientBuilder {
    /// Ceph entity name (defaults to `"client.admin"`).
    pub fn entity_name(mut self, name: impl Into<String>) -> Self {
        self.entity_name = name.into();
        self
    }

    /// Path to `ceph.conf`. When set, `mon_addrs`, keyring path, and
    /// `mon_dns_srv_name` are parsed from the file; explicit setters
    /// override the parsed values.
    pub fn config_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_file = Some(path.into());
        self
    }

    /// Override the monitor addresses that would otherwise come from
    /// `ceph.conf`'s `mon_host`.
    pub fn mon_addrs<I, S>(mut self, addrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.mon_addrs = addrs.into_iter().map(Into::into).collect();
        self
    }

    /// Override the keyring path that would otherwise come from `ceph.conf`.
    pub fn keyring(mut self, path: impl Into<PathBuf>) -> Self {
        self.keyring_path = Some(path.into());
        self
    }

    /// Override the DNS SRV name used for monitor discovery.
    pub fn dns_srv_name(mut self, name: impl Into<String>) -> Self {
        self.dns_srv_name = Some(name.into());
        self
    }

    /// Per-operation timeout for OSD requests (default: 30s).
    pub fn operation_timeout(mut self, dur: Duration) -> Self {
        self.operation_timeout = dur;
        self
    }

    /// Time to wait for the first MonMap during [`build`](Self::build)
    /// before giving up (default: 10s).
    pub fn monmap_timeout(mut self, dur: Duration) -> Self {
        self.monmap_wait_timeout = dur;
        self
    }

    /// Time to wait for the first OSDMap during [`build`](Self::build)
    /// before giving up (default: 10s).
    pub fn osdmap_timeout(mut self, dur: Duration) -> Self {
        self.osdmap_wait_timeout = dur;
        self
    }

    /// Connect to the cluster and produce a ready-to-use [`Client`].
    ///
    /// Steps: parse `ceph.conf`, resolve mon addrs / keyring / DNS SRV name,
    /// build `AuthConfig`, wire the msgr2 map channel, start the `MonClient`,
    /// wait for the first MonMap, start the `OSDClient` with a freshly
    /// derived `client_inc`, and wait for the first OSDMap.
    pub async fn build(self) -> Result<Client, ClientError> {
        // Step 1: parse ceph.conf if provided.
        let ceph_config = self
            .config_file
            .as_deref()
            .map(CephConfig::from_file)
            .transpose()?;

        // Step 2: resolve mon addrs, keyring, dns SRV name with explicit
        // setters winning over ceph.conf values.
        let mon_addrs = if self.mon_addrs.is_empty() {
            ceph_config
                .as_ref()
                .and_then(|c| c.mon_addrs().ok())
                .unwrap_or_default()
        } else {
            self.mon_addrs
        };

        let dns_srv_name = self
            .dns_srv_name
            .or_else(|| ceph_config.as_ref().map(|c| c.mon_dns_srv_name()))
            .unwrap_or_default();

        // Step 3: decide auth method. Clusters with `auth_client_required = none`
        // don't need a keyring at all; attempting to read one would just fail.
        // When `ceph.conf` is available, honour its `auth_client_required` /
        // `auth_supported` setting; otherwise fall back to CephX (the common case).
        let cephx_required = ceph_config
            .as_ref()
            .map(|c| {
                c.get_auth_client_required()
                    .contains(&crate::auth::protocol::CEPH_AUTH_CEPHX)
            })
            .unwrap_or(true);

        let auth = if cephx_required {
            let keyring_path: PathBuf = self
                .keyring_path
                .or_else(|| {
                    ceph_config
                        .as_ref()
                        .and_then(|c| c.keyring().ok())
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from("/etc/ceph/keyring"));

            // Validate UTF-8 up front so the spawn_blocking closure can be
            // infallible about the path.
            if keyring_path.to_str().is_none() {
                return Err(ClientError::Config(format!(
                    "keyring path is not valid UTF-8: {}",
                    keyring_path.display()
                )));
            }

            // `AuthConfig::from_keyring` does synchronous file I/O (open +
            // parse). Running it directly on the async runtime stalls the
            // whole executor on slow storage (NFS, network mounts). Hand it
            // off to `spawn_blocking`, matching tokio's own guidance for
            // blocking file work; see tokio/src/fs/mod.rs, which internally
            // does the same thing for `tokio::fs::read` et al.
            let entity_name = self.entity_name;
            tokio::task::spawn_blocking(move || {
                let keyring_str = keyring_path
                    .to_str()
                    .expect("UTF-8 validated before spawn_blocking");
                AuthConfig::from_keyring(entity_name, keyring_str)
            })
            .await
            .map_err(|join_err| {
                ClientError::Config(format!("keyring load task panicked: {join_err}"))
            })??
        } else {
            AuthConfig::no_auth(self.entity_name)
        };

        let (osdmap_tx, osdmap_rx) = map_channel::<MOSDMap>(MAP_CHANNEL_BUFFER);

        // Step 4: spin up MonClient and wait deterministically for the first MonMap.
        let mon_config = MonClientConfig {
            mon_addrs,
            auth: Some(auth),
            dns_srv_name,
            ..Default::default()
        };
        let mon_client = MonClient::new(mon_config, Some(osdmap_tx.clone())).await?;
        mon_client.init().await?;

        // When CephX is in play, service tickets are fetched after the initial
        // hello/auth handshake. Waiting here ensures the OSDClient created below
        // can immediately sign its first message — without this the first OSD
        // op can race the ticket fetch and hit a transient auth failure.
        if cephx_required {
            mon_client
                .wait_for_auth(self.monmap_wait_timeout)
                .await
                .map_err(|e| match e {
                    MonClientError::AuthenticationTimeout => ClientError::Timeout {
                        what: "CephX authentication",
                        timeout: self.monmap_wait_timeout,
                    },
                    other => ClientError::MonClient(other),
                })?;
        }

        mon_client
            .wait_for_monmap(self.monmap_wait_timeout)
            .await
            .map_err(|e| match e {
                MonClientError::Timeout => ClientError::Timeout {
                    what: "first MonMap",
                    timeout: self.monmap_wait_timeout,
                },
                other => ClientError::MonClient(other),
            })?;
        let fsid = mon_client.get_fsid().await;

        // Step 5: spin up OSDClient with an automatically-derived client_inc
        // and wait for the first OSDMap.
        let osd_config = OSDClientConfig {
            client_inc: default_client_inc(),
            tracker_config: TrackerConfig {
                operation_timeout: self.operation_timeout,
            },
            ..Default::default()
        };
        let osd_client =
            OSDClient::new(osd_config, fsid, mon_client.clone(), osdmap_tx, osdmap_rx).await?;
        osd_client
            .wait_for_latest_osdmap(self.osdmap_wait_timeout)
            .await?;

        Ok(Client {
            mon_client,
            osd_client,
        })
    }
}

/// Errors returned by [`ClientBuilder::build`] and related high-level
/// operations.
///
/// Marked `#[non_exhaustive]` so new failure modes can be added without a
/// semver break. Callers that need to branch on the error should match
/// explicit variants and leave a catch-all arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// Caller-supplied configuration was invalid (non-UTF-8 paths, etc.).
    #[error("configuration error: {0}")]
    Config(String),

    /// Failed to parse `ceph.conf` or one of its fields.
    #[error(transparent)]
    CephConfig(#[from] ConfigError),

    /// The underlying [`MonClient`] reported an error during setup.
    #[error(transparent)]
    MonClient(#[from] MonClientError),

    /// The underlying [`OSDClient`] reported an error during setup or
    /// from [`Client::open_pool`].
    #[error(transparent)]
    OsdClient(#[from] OSDClientError),

    /// A bounded wait (for first MonMap or first OSDMap) timed out.
    #[error("timed out waiting for {what} after {timeout:?}")]
    Timeout {
        what: &'static str,
        timeout: Duration,
    },
}

impl From<ClientError> for io::Error {
    fn from(err: ClientError) -> Self {
        match err {
            // Delegate to the OSD error mapping so NotFound / PermissionDenied /
            // TimedOut classifications survive the round trip through ClientError.
            ClientError::OsdClient(e) => e.into(),
            ClientError::MonClient(MonClientError::IoError(io_err)) => io_err,
            ClientError::Timeout { .. } => io::Error::new(io::ErrorKind::TimedOut, err),
            other => io::Error::other(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults_are_sensible() {
        let b = ClientBuilder::default();
        assert_eq!(b.entity_name, "client.admin");
        assert!(b.config_file.is_none());
        assert!(b.mon_addrs.is_empty());
        assert_eq!(b.operation_timeout, Duration::from_secs(30));
        assert_eq!(b.monmap_wait_timeout, Duration::from_secs(10));
        assert_eq!(b.osdmap_wait_timeout, Duration::from_secs(10));
    }

    #[test]
    fn builder_setters_thread_through() {
        let b = ClientBuilder::default()
            .entity_name("client.test")
            .config_file("/tmp/ceph.conf")
            .mon_addrs(["v2:127.0.0.1:3300"])
            .keyring("/tmp/keyring")
            .dns_srv_name("ceph-test")
            .operation_timeout(Duration::from_secs(5))
            .monmap_timeout(Duration::from_secs(2))
            .osdmap_timeout(Duration::from_secs(3));
        assert_eq!(b.entity_name, "client.test");
        assert_eq!(
            b.config_file.as_deref(),
            Some(std::path::Path::new("/tmp/ceph.conf"))
        );
        assert_eq!(b.mon_addrs, vec!["v2:127.0.0.1:3300".to_owned()]);
        assert_eq!(
            b.keyring_path.as_deref(),
            Some(std::path::Path::new("/tmp/keyring"))
        );
        assert_eq!(b.dns_srv_name.as_deref(), Some("ceph-test"));
        assert_eq!(b.operation_timeout, Duration::from_secs(5));
        assert_eq!(b.monmap_wait_timeout, Duration::from_secs(2));
        assert_eq!(b.osdmap_wait_timeout, Duration::from_secs(3));
    }

    #[test]
    fn client_error_preserves_not_found_kind_through_io_error() {
        // A PoolNotFound wrapped in ClientError::OsdClient must surface as
        // io::ErrorKind::NotFound — critical for consumers whose Driver trait
        // speaks io::Result and matches on ErrorKind::NotFound for HEAD / GET.
        let err = ClientError::OsdClient(OSDClientError::PoolNameNotFound("metadata".into()));
        let io_err: io::Error = err.into();
        assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn client_error_timeout_maps_to_timed_out_kind() {
        let err = ClientError::Timeout {
            what: "first MonMap",
            timeout: Duration::from_secs(10),
        };
        let io_err: io::Error = err.into();
        assert_eq!(io_err.kind(), io::ErrorKind::TimedOut);
    }
}
