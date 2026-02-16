---
applyTo: "**/tests/**/*.rs"
---

## Test File Guidelines

When writing or modifying test files in this repository, follow these guidelines:

### Test Organization

1. **Unit tests** - Colocated with the code they test using `#[cfg(test)]` modules
2. **Integration tests** - Located in `crates/*/tests/` directories
3. **Ignored tests** - Tests requiring external setup (Ceph cluster, corpus files) are marked with `#[ignore]`

### Integration Test Requirements

Integration tests that require external dependencies should be marked with `#[ignore]`:

```rust
#[tokio::test]
#[ignore = "requires ceph cluster"]
async fn test_connect_to_monitor() {
    // Test implementation
}
```

### Corpus Tests

When writing corpus comparison tests:

1. Use the `ceph-object-corpus` repository for test data
2. Environment variables:
   - `CORPUS_ROOT`: Path to ceph-object-corpus clone (e.g., `/tmp/ceph-object-corpus`)
   - `CORPUS_VERSION`: Version to test against (e.g., `19.2.0-404-g78ddc7f9027`)
3. Compare JSON outputs between ceph-dencoder and our implementation
4. Mark tests with `#[ignore]` since they require external corpus data

### Running Tests

```bash
# Run only unit tests (not ignored)
cargo test --workspace --lib

# Run integration tests (requires setup)
cargo test --workspace --tests -- --ignored --nocapture

# Run specific integration test
cargo test -p denc --tests corpus_test -- --ignored --nocapture
```

### Test Best Practices

1. **Use descriptive test names** that explain what is being tested
2. **Include failure messages** with helpful context using `.expect()` or custom assertions
3. **Test roundtrip encoding** - For any Denc implementation, test encode → decode → encode cycle
4. **Use `--nocapture` flag** when debugging to see print statements and logs
5. **Set `RUST_LOG`** environment variable for detailed logging:
   ```bash
   RUST_LOG=debug cargo test -p msgr2 --tests -- --ignored --nocapture
   ```

### Async Test Patterns

Use `#[tokio::test]` for async tests:

```rust
#[tokio::test]
async fn test_async_operation() {
    let result = some_async_function().await;
    assert!(result.is_ok());
}
```

### Common Assertions

- Use `assert_eq!` for equality checks with helpful diff output
- Use `assert!` for boolean conditions
- Use `.unwrap()` sparingly - prefer `.expect()` with descriptive messages
- For Result types, consider using `?` and returning `Result<(), Box<dyn Error>>` from test functions
