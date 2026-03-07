//! Monitor Client for Ceph
//!
//! This crate provides a client for communicating with Ceph monitor daemons.
//! It handles connection management, authentication, subscriptions to cluster maps,
//! and command execution.
//!
//! # Example
//!
//! ```no_run
//! use monclient::{AuthConfig, MonClient, MonClientConfig, MonService};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create auth config from ceph.conf
//!     let auth = AuthConfig::from_ceph_conf("/etc/ceph/ceph.conf")?;
//!
//!     let config = MonClientConfig {
//!         mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
//!         auth: Some(auth),
//!         connect_timeout: Duration::from_secs(30),
//!         command_timeout: Duration::from_secs(60),
//!         hunt_interval: Duration::from_secs(3),
//!         hunt_parallel: 3,
//!         ..Default::default()
//!     };
//!
//!     let client = MonClient::new(config, None).await?;
//!     client.init().await?;
//!
//!     // Subscribe to osdmap
//!     client.subscribe(MonService::OsdMap, 0, 0).await?;
//!
//!     // Get version
//!     let (newest, oldest) = client.get_version(MonService::OsdMap).await?;
//!     println!("OSDMap version: {} (oldest: {})", newest, oldest);
//!
//!     Ok(())
//! }
//! ```

pub mod auth_config;
pub mod client;
pub(crate) mod connection;
pub mod defaults;
pub mod dns_srv;
pub mod error;
pub mod messages;
pub mod monmap;
pub(crate) mod paxos_service_message;
pub(crate) mod subscription;
pub mod types;
pub(crate) mod wait_helper;

pub use auth_config::AuthConfig;
pub use client::{MonClient, MonClientConfig, PoolOpResult};
pub use dns_srv::{resolve_mon_addrs_via_dns_srv, DEFAULT_MON_DNS_SRV_NAME};
pub use error::{MonClientError, Result};
pub use messages::{
    MAuth, MAuthReply, MConfig, MMonCommand, MMonCommandAck, MMonGetVersion, MMonGetVersionReply,
    MMonMap, MMonSubscribe, MMonSubscribeAck, MOSDMap, MPoolOp, MPoolOpReply,
};
pub use monmap::{MonInfo, MonMapState};
pub use subscription::{MonService, MonSub, SubscribeItem, CEPH_SUBSCRIBE_ONETIME};
pub use types::CommandResult;
