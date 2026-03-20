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
//! use crate::auth::{CephXClientHandler, AuthMode};
//!
//! // Create a client handler
//! let handler = CephXClientHandler::new("client.admin", AuthMode::Mon)?;
//!
//! // Build initial authentication request
//! let auth_request = handler.build_initial_request(0)?;
//! # Ok::<(), crate::auth::CephXError>(())
//! ```

pub mod client;
pub mod error;
pub mod keyring;
pub mod protocol;
pub mod provider;
pub mod server;
pub mod types;

// Client-facing auth handler and result
pub use client::{AuthResult, CephXClientHandler};

// Error types
pub use error::{CephXError, Result};

// Keyring management
pub use keyring::Keyring;

// Auth provider traits and built-in providers
pub use provider::{AuthProvider, MonitorAuthProvider, ServiceAuthProvider};

// Server-side auth handler
pub use server::CephXServerHandler;

// Common auth types
pub use types::{
    AuthCapsInfo, AuthTicket, CEPH_CRYPTO_AES, CephXServiceTicketInfo, CephXSession,
    CephXTicketBlob, CryptoKey, EntityType, TicketHandler,
};
