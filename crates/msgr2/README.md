# msgr2 - Ceph Messenger v2 Protocol Implementation

This crate implements the Ceph messenger v2 protocol in Rust.

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
2. The `CEPH_MON_ADDR` environment variable set to the monitor address

#### Running Integration Tests

```bash
# Set the monitor address
export CEPH_MON_ADDR=<monitor_ip>:40390

# Optionally set the keyring path (defaults to /etc/ceph/ceph.client.admin.keyring)
export CEPH_KEYRING=/path/to/ceph/build/keyring

# Run the integration tests
cargo test --test connection_tests -- --nocapture
```

#### Keyring Configuration

The authentication system will:
1. First try to load the keyring from the path specified in the `CEPH_KEYRING` environment variable
2. If not set, try the default path `/etc/ceph/ceph.client.admin.keyring`
3. If the keyring file is not found or cannot be loaded, fall back to a hardcoded key (which may not work with your cluster)

For best results, always set `CEPH_KEYRING` to point to your cluster's keyring file.

## CI/CD

**Important**: The integration tests require a running Ceph cluster and will fail in CI if `CEPH_MON_ADDR` is not set. Make sure your CI environment either:
1. Sets up a Ceph cluster and provides the `CEPH_MON_ADDR` environment variable, or
2. Explicitly excludes the `connection_tests` from the test run

To exclude integration tests in CI:
```bash
cargo test --workspace --lib --bins
```
