//! Macros for reducing code duplication in encoding/decoding

/// Check minimum supported version for decoding
///
/// This macro enforces minimum version requirements for types that no longer
/// support pre-Quincy (v17) Ceph releases within the current project
/// compatibility boundary. It provides a consistent error
/// message format across all types.
///
/// # Examples
///
/// ```ignore
/// use crate::denc::check_min_version;
///
/// fn decode_content<B: Buf>(
///     buf: &mut B,
///     features: u64,
///     version: u8,
///     _compat_version: u8,
/// ) -> Result<Self, RadosError> {
///     // Reject versions outside the supported floor (Quincy v17+)
///     check_min_version!(version, 5, "ObjectLocator", "Quincy v17+");
///
///     // Continue with decoding...
/// }
/// ```
#[macro_export]
macro_rules! check_min_version {
    ($version:expr, $min:expr, $type_name:expr, $ceph_release:expr) => {
        if $version < $min {
            return Err($crate::RadosError::Codec(
                $crate::CodecError::VersionTooOld {
                    got: $version,
                    min: $min,
                    type_name: $type_name,
                    ceph_release: $ceph_release,
                },
            ));
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
/// use crate::denc::decode_if_version;
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
