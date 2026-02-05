//! Monitor Client for Ceph
//!
//! This crate provides a client for communicating with Ceph monitor daemons.
//! It handles connection management, authentication, subscriptions to cluster maps,
//! and command execution.
//!
//! # Example
//!
//! ```no_run
//! use monclient::{MonClient, MonClientConfig};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = MonClientConfig {
//!         entity_name: "client.admin".to_string(),
//!         mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
//!         keyring_path: "/etc/ceph/ceph.client.admin.keyring".to_string(),
//!         connect_timeout: Duration::from_secs(30),
//!         command_timeout: Duration::from_secs(60),
//!         hunt_interval: Duration::from_secs(3),
//!         hunt_parallel: 3,
//!         ..Default::default()
//!     };
//!
//!     let client = MonClient::new(config).await?;
//!     client.init().await?;
//!
//!     // Subscribe to osdmap
//!     client.subscribe("osdmap", 0, 0).await?;
//!
//!     // Get version
//!     let (newest, oldest) = client.get_version("osdmap").await?;
//!     println!("OSDMap version: {} (oldest: {})", newest, oldest);
//!
//!     Ok(())
//! }
//! ```

pub mod ceph_message_impl;
pub mod client;
pub mod connection;
pub mod error;
pub mod messages;
pub mod monmap;
pub mod paxos_service_message;
pub mod subscription;
pub mod types;
pub mod wait_helper;

pub use client::{MonClient, MonClientConfig, PoolOpResult};
pub use error::{MonClientError, Result};
pub use messages::{
    MAuth, MAuthReply, MMonCommand, MMonCommandAck, MMonGetVersion, MMonGetVersionReply, MMonMap,
    MMonSubscribe, MMonSubscribeAck, MOSDMap, MPoolOp, MPoolOpReply,
};
pub use monmap::{MonInfo, MonMap};
pub use subscription::{MonSub, SubscribeItem, CEPH_SUBSCRIBE_ONETIME};
pub use types::{CommandResult, EntityName};
