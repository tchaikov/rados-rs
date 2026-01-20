/// Type registry for dencoder
///
/// This module maintains a registry of all types that can be encoded/decoded
/// for corpus validation.
/// List all registered types
pub fn list_types() -> Vec<&'static str> {
    // Start with an empty list
    // As we implement types in future commits, we'll add them here
    vec![
        // Primitives will be added in Commit 3
        // Collections will be added in Commit 4
        // Ceph types will be added in later commits
    ]
}

type DecoderFn = fn(&[u8]) -> Result<(), Box<dyn std::error::Error>>;

/// Get decoder for a type
///
/// TODO: This will be implemented as we add more types
#[allow(dead_code)]
pub fn get_decoder(_type_name: &str) -> Option<DecoderFn> {
    None
}

/// Get encoder for a type
///
/// TODO: This will be implemented as we add more types
#[allow(dead_code)]
pub fn get_encoder(_type_name: &str) -> Option<fn() -> Vec<u8>> {
    None
}
