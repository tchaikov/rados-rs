# msgr2 - Ceph Messenger v2 Protocol Implementation

This crate implements the Ceph messenger v2 protocol in Rust.

## Features

### Compression Statistics

The msgr2 implementation provides detailed statistics for compression operations. This is useful for monitoring compression effectiveness and debugging performance issues.

```rust
use msgr2::compression::{CompressionContext, CompressionAlgorithm};

// Create a compression context
let ctx = CompressionContext::new(CompressionAlgorithm::Snappy);

// Perform compression/decompression operations
let data = b"Hello, World!";
let compressed = ctx.compress(data)?;
let decompressed = ctx.decompress(&compressed, data.len())?;

// Get statistics
let stats = ctx.stats();
println!("Algorithm: {:?}", stats.algorithm());
println!("Initial size: {} bytes", stats.initial_size());
println!("Compressed size: {} bytes", stats.compressed_size());
println!("Compression ratio: {:.2}%", stats.ratio() * 100.0);
println!("Compression count: {}", stats.compression_count());
println!("Decompression count: {}", stats.decompression_count());
```

Statistics are tracked using atomic operations and are thread-safe. The `ratio()` method returns the compression ratio (compressed size / initial size), where values less than 1.0 indicate effective compression.

## Testing

### Unit Tests

Run the standard unit tests with:

```bash
cargo test --lib
```

### Integration Tests

The integration tests in `tests/connection_tests.rs` require a running Ceph cluster. **These tests will fail if the prerequisites are not met.**

#### Prerequisites

1. A running Ceph cluster (local or remote)
2. The `CEPH_CONF` environment variable set to the path of the ceph.conf file

#### Running Integration Tests

```bash
# Set the ceph config path
export CEPH_CONF=/path/to/ceph.conf

# Run the integration tests
cargo test --test connection_tests -- --nocapture
```

#### Configuration

The integration tests will load configuration from the ceph.conf file specified by `CEPH_CONF`. The configuration file should include:
- Monitor addresses in the `mon host` option
- Keyring path in the `keyring` option (or it will use the default path)

## CI/CD

**Important**: The integration tests require a running Ceph cluster and will fail in CI if `CEPH_CONF` is not set. Make sure your CI environment either:
1. Sets up a Ceph cluster and provides the `CEPH_CONF` environment variable, or
2. Explicitly excludes the `connection_tests` from the test run

To exclude integration tests in CI:
```bash
cargo test --workspace --lib --bins
```
