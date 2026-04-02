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

/// Implement `Denc` and `FixedSize` for a `#[repr(u8)]` enum that has a `From<u8>` impl.
///
/// The enum must be `Copy` and castable to `u8` via `as u8`.
///
/// # Example
///
/// ```ignore
/// #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// #[repr(u8)]
/// enum MyEnum {
///     #[default]
///     A = 0,
///     B = 1,
/// }
///
/// impl From<u8> for MyEnum {
///     fn from(v: u8) -> Self {
///         match v {
///             1 => MyEnum::B,
///             _ => MyEnum::A,
///         }
///     }
/// }
///
/// impl_denc_u8_enum!(MyEnum);
/// ```
#[macro_export]
macro_rules! impl_denc_u8_enum {
    ($type:ty) => {
        impl $crate::Denc for $type {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                features: u64,
            ) -> Result<(), $crate::RadosError> {
                (*self as u8).encode(buf, features)
            }

            fn decode<B: bytes::Buf>(
                buf: &mut B,
                features: u64,
            ) -> Result<Self, $crate::RadosError> {
                Ok(Self::from(u8::decode(buf, features)?))
            }

            fn encoded_size(&self, _features: u64) -> Option<usize> {
                Some(1)
            }
        }

        impl $crate::denc::codec::FixedSize for $type {
            const SIZE: usize = 1;
        }
    };
}
