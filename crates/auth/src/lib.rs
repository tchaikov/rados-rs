//! CephX Authentication Protocol Implementation
//!
//! This crate provides a complete implementation of the CephX authentication protocol
//! used by Ceph storage clusters. It supports both client and server-side authentication
//! flows, ticket management, and cryptographic operations.
//!
//! # Features
//!
//! - CephX challenge-response authentication
//! - Ticket-based authorization
//! - Session key management
//! - Service ticket handling
//! - Cryptographic signing and verification
//!
//! # Example
//!
//! ```rust,no_run
//! use auth::{CephXClientHandler, AuthMode};
//!
//! // Create a client handler
//! let handler = CephXClientHandler::new("client.admin".to_string(), AuthMode::Mon)?;
//!
//! // Build initial authentication request
//! let auth_request = handler.build_initial_request(0)?;
//! # Ok::<(), auth::CephXError>(())
//! ```

pub mod client;
pub mod error;
pub mod keyring;
pub mod protocol;
pub mod provider;
pub mod server;
pub mod types;

pub use client::*;
pub use error::*;
pub use keyring::*;
pub use protocol::*;
pub use provider::*;
pub use server::*;
pub use types::{
    entity_type, service_id, AuthCapsInfo, AuthTicket, CephXAuthenticator, CephXServiceTicketInfo,
    CephXSession, CephXTicketBlob, CryptoKey, EntityName, TicketHandler, CEPH_CRYPTO_AES,
    CEPH_CRYPTO_NONE,
};
