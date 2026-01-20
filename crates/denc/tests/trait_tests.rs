use denc::{Denc, FixedSize};

#[test]
fn test_trait_exists() {
    // This test verifies that the Denc trait can be used as a trait bound
    fn _requires_denc<T: Denc>() {}

    // If this compiles, the trait is properly defined
    // We'll implement actual types in later commits
}

#[test]
fn test_fixed_size_trait_exists() {
    // This test verifies that the FixedSize trait exists and can be used
    fn _requires_fixed_size<T: FixedSize>() {}

    // If this compiles, the trait is properly defined
}
