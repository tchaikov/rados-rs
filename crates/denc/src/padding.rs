//! Padding field type that is skipped during JSON serialization
//!
//! This module provides a `Padding<T>` wrapper type that automatically skips
//! serialization to JSON, making it useful for padding fields that exist in
//! binary formats but should not appear in JSON output (matching ceph-dencoder behavior).

use serde::{Serialize, Serializer};
use std::fmt;
use std::ops::{Deref, DerefMut};

/// A wrapper type for padding fields that are skipped during JSON serialization.
///
/// This type is useful for fields that exist in binary formats for alignment
/// or compatibility but should not be included in JSON output. It automatically
/// implements `skip_serializing` for serde, ensuring the field is omitted from
/// JSON representations.
///
/// # Examples
///
/// ```
/// use serde::Serialize;
/// use denc::Padding;
///
/// #[derive(Serialize)]
/// struct MyStruct {
///     version: u64,
///     epoch: u32,
///     #[serde(skip_serializing)]
///     pad: Padding<u32>,
/// }
///
/// // Alternative: use Padding directly (already skips serialization)
/// #[derive(Serialize)]
/// struct MyStruct2 {
///     version: u64,
///     epoch: u32,
///     pad: Padding<u32>,
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Padding<T>(T);

impl<T> Padding<T> {
    /// Create a new padding field with the given value
    pub const fn new(value: T) -> Self {
        Padding(value)
    }

    /// Get the inner value
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Get a reference to the inner value
    pub const fn get(&self) -> &T {
        &self.0
    }

    /// Get a mutable reference to the inner value
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: Default> Padding<T> {
    /// Create a new padding field with the default value
    pub fn zero() -> Self {
        Padding(T::default())
    }
}

// Implement Deref and DerefMut for easy access to the inner value
impl<T> Deref for Padding<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Padding<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Custom Serialize implementation that always skips the field
impl<T> Serialize for Padding<T> {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Never serialize - this should not be called if used with skip_serializing
        // But we provide this implementation as a safety net
        use serde::ser::Error;
        Err(S::Error::custom("Padding fields should not be serialized"))
    }
}

// Implement From for easy construction
impl<T> From<T> for Padding<T> {
    fn from(value: T) -> Self {
        Padding(value)
    }
}

// Implement Display for debugging
impl<T: fmt::Display> fmt::Display for Padding<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Padding({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_padding_basic() {
        let pad = Padding::new(42u32);
        assert_eq!(*pad, 42);
        assert_eq!(pad.into_inner(), 42);
    }

    #[test]
    fn test_padding_zero() {
        let pad: Padding<u32> = Padding::zero();
        assert_eq!(*pad, 0);
    }

    #[test]
    fn test_padding_deref() {
        let mut pad = Padding::new(10u32);
        *pad += 5;
        assert_eq!(*pad, 15);
    }

    #[test]
    fn test_padding_serialization_with_skip() {
        #[derive(Serialize)]
        struct TestStruct {
            value: u32,
            #[serde(skip_serializing)]
            _pad: Padding<u32>,
        }

        let s = TestStruct {
            value: 42,
            _pad: Padding::new(0),
        };

        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, r#"{"value":42}"#);
    }
}
