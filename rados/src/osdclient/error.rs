//! Error types for OSD client operations

use std::io;
use std::time::Duration;
use thiserror::Error;

// Standard errno constants (as negative values, matching Ceph conventions)
pub const ENOENT: i32 = -2; // No such file or directory
pub const ENXIO: i32 = -6; // No such device or address (misdirected op: sent to wrong OSD)
pub const EAGAIN: i32 = -11; // Try again
pub const EACCES: i32 = -13; // Permission denied
pub const EEXIST: i32 = -17; // File exists
pub const EINVAL: i32 = -22; // Invalid argument
pub const ENOSPC: i32 = -28; // No space left on device

/// Errors that can occur during OSD client operations.
///
/// Marked `#[non_exhaustive]` because the RADOS protocol surfaces a large and
/// growing set of failure modes (new OSD error codes, new backoff states,
/// future protocol extensions). Tokio avoids this annotation by keeping each
/// error enum tiny, but `OSDClientError` already has too many variants for
/// that discipline to scale; consumers should match with a catch-all arm or
/// use the [`is_not_found`](Self::is_not_found) / [`is_timeout`](Self::is_timeout)
/// / [`is_blocklisted`](Self::is_blocklisted) classifiers.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OSDClientError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("OSD error {code}: {message}")]
    OSDError { code: i32, message: String },

    #[error("Operation timeout after {0:?}")]
    Timeout(Duration),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Pool not found: {0}")]
    PoolNotFound(u64),

    #[error("Pool not found: '{0}'")]
    PoolNameNotFound(String),

    #[error("No OSDs available")]
    NoOSDs,

    #[error("Client is blocklisted (fenced) by the cluster")]
    Blocklisted,

    #[error("{0}")]
    Other(String),

    #[error("Msgr2 error: {0}")]
    Msgr2(#[from] crate::msgr2::Msgr2Error),

    #[error("Denc error: {0}")]
    Denc(#[from] crate::RadosError),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("MonClient error: {0}")]
    MonClient(#[from] crate::monclient::MonClientError),

    #[error("CRUSH error: {0}")]
    Crush(String),

    #[error("OSD backoff: {0}")]
    Backoff(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for OSD client operations
pub type Result<T> = std::result::Result<T, OSDClientError>;

impl From<OSDClientError> for crate::RadosError {
    fn from(e: OSDClientError) -> Self {
        match e {
            OSDClientError::Denc(error) => error,
            OSDClientError::MonClient(error) => error.into(),
            other => crate::RadosError::Protocol(format!("OSDClient error: {}", other)),
        }
    }
}

/// Error category for retry decision making
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Can retry immediately
    Transient,
    /// Should not retry
    Permanent,
    /// Wait for new OSDMap
    NeedsMapUpdate,
    /// Retry after delay
    RetriableWithBackoff,
}

impl OSDClientError {
    /// Categorize error for retry decisions
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Timeout(_) | Self::Connection(_) => ErrorCategory::Transient,
            Self::ObjectNotFound(_)
            | Self::PoolNotFound(_)
            | Self::PoolNameNotFound(_)
            | Self::Auth(_)
            | Self::Blocklisted => ErrorCategory::Permanent,
            Self::Backoff(_) => ErrorCategory::RetriableWithBackoff,
            Self::OSDError { code, .. } => match *code {
                ENOENT => ErrorCategory::Permanent,
                // ENXIO: OSD rejected op as misdirected (we sent to a replica, not primary).
                // Mirrors Objecter handling: fetch a newer OSDMap and retry.
                ENXIO => ErrorCategory::NeedsMapUpdate,
                EAGAIN => ErrorCategory::NeedsMapUpdate,
                ENOSPC => ErrorCategory::RetriableWithBackoff,
                _ => ErrorCategory::Transient,
            },
            _ => ErrorCategory::Permanent,
        }
    }

    /// Check if error is retriable
    pub fn is_retriable(&self) -> bool {
        !matches!(self.category(), ErrorCategory::Permanent)
    }

    /// True for any variant that semantically means "the thing you asked for doesn't exist".
    ///
    /// Consumers can match on this instead of picking apart individual variants, and the
    /// classification is the same one used by [`OSDClientError::io_error_kind`] for
    /// [`io::ErrorKind::NotFound`].
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::ObjectNotFound(_)
                | Self::PoolNotFound(_)
                | Self::PoolNameNotFound(_)
                | Self::OSDError { code: ENOENT, .. }
        )
    }

    /// True for any variant that represents a timeout expiry. Matches the same
    /// classification used by `io_error_kind()` for [`io::ErrorKind::TimedOut`].
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout(_))
    }

    /// True when the cluster has explicitly fenced this client (the OSDs have
    /// added it to their blocklist). Unlike most failures, a blocklisted client
    /// cannot recover without restarting the `OSDClient` — callers typically
    /// want to bail out rather than retry.
    pub fn is_blocklisted(&self) -> bool {
        matches!(self, Self::Blocklisted)
    }

    /// Map this error to the most specific [`io::ErrorKind`] available.
    ///
    /// Used by `impl From<OSDClientError> for io::Error` so that consumers speaking
    /// `io::Result` (e.g. object-storage drivers) get correct `NotFound` / `TimedOut` /
    /// `PermissionDenied` classification without having to re-implement the mapping.
    pub fn io_error_kind(&self) -> io::ErrorKind {
        use io::ErrorKind as K;
        match self {
            Self::ObjectNotFound(_) | Self::PoolNotFound(_) | Self::PoolNameNotFound(_) => {
                K::NotFound
            }
            Self::Timeout(_) => K::TimedOut,
            Self::Blocklisted | Self::Auth(_) => K::PermissionDenied,
            Self::Connection(_) | Self::Msgr2(_) | Self::MonClient(_) => K::ConnectionAborted,
            Self::NoOSDs => K::NotConnected,
            Self::InvalidOperation(_) => K::InvalidInput,
            Self::Denc(_) | Self::Encoding(_) => K::InvalidData,
            Self::Backoff(_) => K::WouldBlock,
            Self::OSDError { code, .. } => match *code {
                ENOENT => K::NotFound,
                EEXIST => K::AlreadyExists,
                EACCES => K::PermissionDenied,
                EINVAL => K::InvalidInput,
                _ => K::Other,
            },
            Self::Other(_) | Self::Crush(_) | Self::Internal(_) => K::Other,
        }
    }
}

impl From<OSDClientError> for io::Error {
    fn from(err: OSDClientError) -> Self {
        io::Error::new(err.io_error_kind(), err)
    }
}

#[cfg(test)]
mod tests {
    use super::OSDClientError;
    use std::io;
    use std::time::Duration;

    #[test]
    fn converting_osdclient_denc_error_preserves_variant() {
        let error = OSDClientError::Denc(crate::RadosError::InvalidData("bad pg".into()));

        let converted: crate::RadosError = error.into();

        assert!(
            matches!(converted, crate::RadosError::InvalidData(message) if message == "bad pg")
        );
    }

    #[test]
    fn converting_osdclient_monclient_error_preserves_nested_rados_error() {
        let error = OSDClientError::MonClient(crate::monclient::MonClientError::RadosError(
            crate::RadosError::InvalidData("bad mon command".into()),
        ));

        let converted: crate::RadosError = error.into();

        assert!(
            matches!(converted, crate::RadosError::InvalidData(message) if message == "bad mon command")
        );
    }

    #[test]
    fn is_timeout_only_matches_timeout_variant() {
        assert!(OSDClientError::Timeout(Duration::from_secs(1)).is_timeout());
        assert!(!OSDClientError::ObjectNotFound("foo".into()).is_timeout());
        assert!(!OSDClientError::Blocklisted.is_timeout());
    }

    #[test]
    fn is_blocklisted_only_matches_blocklisted_variant() {
        assert!(OSDClientError::Blocklisted.is_blocklisted());
        assert!(!OSDClientError::ObjectNotFound("foo".into()).is_blocklisted());
        assert!(!OSDClientError::Auth("bad key".into()).is_blocklisted());
    }

    #[test]
    fn is_not_found_matches_every_not_found_variant() {
        assert!(OSDClientError::ObjectNotFound("foo".into()).is_not_found());
        assert!(OSDClientError::PoolNotFound(42).is_not_found());
        assert!(OSDClientError::PoolNameNotFound("rbd".into()).is_not_found());
        assert!(
            OSDClientError::OSDError {
                code: super::ENOENT,
                message: "enoent".into(),
            }
            .is_not_found()
        );

        // Negative cases — anything else must not be classified as not-found.
        assert!(!OSDClientError::Timeout(Duration::from_secs(1)).is_not_found());
        assert!(!OSDClientError::Blocklisted.is_not_found());
        assert!(
            !OSDClientError::OSDError {
                code: super::EINVAL,
                message: "einval".into(),
            }
            .is_not_found()
        );
    }

    #[test]
    fn io_error_kind_maps_structural_variants() {
        use io::ErrorKind as K;
        assert_eq!(
            OSDClientError::ObjectNotFound("k".into()).io_error_kind(),
            K::NotFound
        );
        assert_eq!(OSDClientError::PoolNotFound(7).io_error_kind(), K::NotFound);
        assert_eq!(
            OSDClientError::PoolNameNotFound("rbd".into()).io_error_kind(),
            K::NotFound
        );
        assert_eq!(
            OSDClientError::Timeout(Duration::from_secs(1)).io_error_kind(),
            K::TimedOut
        );
        assert_eq!(
            OSDClientError::Blocklisted.io_error_kind(),
            K::PermissionDenied
        );
        assert_eq!(
            OSDClientError::Auth("bad key".into()).io_error_kind(),
            K::PermissionDenied
        );
        assert_eq!(
            OSDClientError::Connection("socket closed".into()).io_error_kind(),
            K::ConnectionAborted
        );
        assert_eq!(OSDClientError::NoOSDs.io_error_kind(), K::NotConnected);
        assert_eq!(
            OSDClientError::InvalidOperation("bad op".into()).io_error_kind(),
            K::InvalidInput
        );
        assert_eq!(
            OSDClientError::Encoding("short buf".into()).io_error_kind(),
            K::InvalidData
        );
        assert_eq!(
            OSDClientError::Backoff("pg backoff".into()).io_error_kind(),
            K::WouldBlock
        );
        assert_eq!(
            OSDClientError::Other("misc".into()).io_error_kind(),
            K::Other
        );
    }

    #[test]
    fn io_error_kind_maps_osd_numeric_codes() {
        use io::ErrorKind as K;
        let cases = [
            (super::ENOENT, K::NotFound),
            (super::EEXIST, K::AlreadyExists),
            (super::EACCES, K::PermissionDenied),
            (super::EINVAL, K::InvalidInput),
            (-99, K::Other), // unmapped → Other
        ];
        for (code, expected) in cases {
            let err = OSDClientError::OSDError {
                code,
                message: format!("code {code}"),
            };
            assert_eq!(err.io_error_kind(), expected, "code {code}");
        }
    }

    #[test]
    fn io_error_round_trip_preserves_not_found_kind_and_source() {
        let err = OSDClientError::PoolNameNotFound("metadata".into());
        let io_err: io::Error = err.into();
        assert_eq!(io_err.kind(), io::ErrorKind::NotFound);

        // The original error survives as the source chain so downstream `Display`/`source`
        // walks still see "Pool not found: 'metadata'".
        let source = io_err
            .get_ref()
            .expect("source must be preserved")
            .to_string();
        assert!(source.contains("metadata"), "got {source:?}");
    }
}
