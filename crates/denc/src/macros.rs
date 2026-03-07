//! Macros for reducing code duplication in encoding/decoding

/// Check minimum supported version for decoding
///
/// This macro enforces minimum version requirements for types that no longer
/// support pre-Octopus (v15) Ceph releases within the current project
/// compatibility boundary. It provides a consistent error
/// message format across all types.
///
/// # Examples
///
/// ```ignore
/// use denc::check_min_version;
///
/// fn decode_content<B: Buf>(
///     buf: &mut B,
///     features: u64,
///     version: u8,
///     _compat_version: u8,
/// ) -> Result<Self, RadosError> {
///     // Reject versions outside the supported floor (Octopus v15+)
///     check_min_version!(version, 5, "ObjectLocator", "Octopus v15+");
///
///     // Continue with decoding...
/// }
/// ```
#[macro_export]
macro_rules! check_min_version {
    ($version:expr, $min:expr, $type_name:expr, $ceph_release:expr) => {
        if $version < $min {
            return Err($crate::RadosError::Protocol(format!(
                "{} version {} not supported (requires Ceph {}, minimum version {})",
                $type_name, $version, $ceph_release, $min
            )));
        }
    };
}

/// Decode a field conditionally based on version
///
/// This macro reduces the repetitive pattern of:
/// ```ignore
/// if version >= MIN_VERSION {
///     Type::decode(buf, features)?
/// } else {
///     default_value
/// }
/// ```
///
/// # Examples
///
/// ```ignore
/// use denc::decode_if_version;
///
/// // Decode u32 if version >= 5, otherwise use 0
/// let value = decode_if_version!(buf, features, version, >= 5, u32, 0);
///
/// // Decode String if version >= 10, otherwise use empty string
/// let name = decode_if_version!(buf, features, version, >= 10, String, String::new());
/// ```
#[macro_export]
macro_rules! decode_if_version {
    ($buf:expr, $features:expr, $version:expr, >= $min:expr, $type:ty, $default:expr) => {
        if $version >= $min {
            <$type as $crate::Denc>::decode($buf, $features)?
        } else {
            $default
        }
    };
}

/// Implement CephMessagePayload trait for a message type
///
/// This macro reduces the boilerplate of implementing CephMessagePayload
/// for message types that follow the standard pattern.
///
/// # Examples
///
/// ```ignore
/// use denc::impl_ceph_message_payload;
///
/// impl_ceph_message_payload!(MAuth, CEPH_MSG_AUTH, 1);
/// impl_ceph_message_payload!(MAuthReply, CEPH_MSG_AUTH_REPLY, 1);
/// ```
#[macro_export]
macro_rules! impl_ceph_message_payload {
    ($type:ty, $msg_type:expr, $version:expr) => {
        impl msgr2::CephMessagePayload for $type {
            fn msg_type() -> u16 {
                $msg_type
            }

            fn msg_version(_features: u64) -> u16 {
                $version
            }

            fn encode_payload(&self, _features: u64) -> Result<bytes::Bytes, msgr2::Error> {
                self.encode().map_err(|_| msgr2::Error::Serialization)
            }

            fn decode_payload(
                _h: &msgr2::CephMsgHeader,
                front: &[u8],
                _m: &[u8],
                _d: &[u8],
            ) -> Result<Self, msgr2::Error> {
                Self::decode(front)
                    .map_err(|_| msgr2::Error::Deserialization(stringify!($type).into()))
            }
        }
    };
}
