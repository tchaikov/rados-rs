# Suggested Commands for RADOS-RS Development

## Build Commands
- `cargo build` - Build the project
- `cargo build --release` - Release build
- `cargo test` - Run tests
- `cargo clippy` - Lint code
- `cargo fmt` - Format code

## Testing Commands
- `cargo run --bin rados-client ceph-test` - Test connection to local Ceph cluster
- `RUST_LOG=debug cargo run --bin rados-client ceph-test` - Test with debug logging
- `cargo run --bin test_runner` - Run test runner
- `cargo run --bin test_runner test-auth` - Run auth tests

## Official Ceph Client (for reference/debugging)
- `LD_PRELOAD=/usr/lib/libasan.so.8 /home/kefu/dev/ceph/build/bin/ceph --conf "/home/kefu/dev/rust-app-ceres/docker/ceph-config/ceph.conf" -s 2>/dev/null` - Run official Ceph client

## Debugging Commands
- `tcpdump` commands for packet capture
- `tshark` for packet analysis
- Check container logs: `podman logs ceph-mon`