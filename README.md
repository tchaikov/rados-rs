# rados-rs

A native Rust implementation of the RADOS (Reliable Autonomic Distributed Object Store) client library for Ceph, built on Tokio for async/await I/O without any C library dependencies.

## Features

- **No FFI**: Pure Rust — no `librados` or `libceph` dependency
- **Async/Await**: Tokio-based async I/O throughout
- **msgr2**: Complete Ceph messenger protocol v2 with AES-128-GCM encryption and CRC32C
- **CephX**: Full authentication and service ticket handling
- **OSD operations**: Write, read, stat, delete on RADOS objects
- **Ceph Octopus+**: Supports Octopus (v15, March 2020) and all later releases

## Architecture

The workspace contains the following crates:

| Crate | Description |
|---|---|
| `denc` | Ceph DENC encoding/decoding trait and primitive types |
| `denc-macros` | Derive macros (`ZeroCopyDencode`) for zero-copy POD types |
| `auth` | CephX authentication protocol, keyring parsing, service tickets |
| `cephconfig` | `ceph.conf` parser |
| `crush` | CRUSH map decoding and object placement |
| `msgr2` | Messenger protocol v2 — framing, crypto, session negotiation |
| `monclient` | Monitor client — MonMap, OSDMap, cluster commands |
| `osdclient` | OSD client — object read/write/stat/delete |
| `dencoder` | CLI tool: decode and inspect Ceph binary corpus files |
| `rados` | CLI tool: RADOS object operations (put/get/stat/rm/ls) |

## Quick Start

### `rados` CLI

```bash
# Write an object from a local file
cargo run -p rados -- -p mypool put myobject /path/to/file

# Read an object to stdout
cargo run -p rados -- -p mypool get myobject -

# Stat an object
cargo run -p rados -- -p mypool stat myobject

# Remove an object
cargo run -p rados -- -p mypool rm myobject

# List objects in a pool
cargo run -p rados -- -p mypool ls
```

By default the `rados` binary reads `CEPH_CONF` (or `/etc/ceph/ceph.conf`). Pass `-c /path/to/ceph.conf` to override.

### `dencoder` CLI

A Rust reimplementation of `ceph-dencoder` for inspecting binary corpus files:

```bash
# Decode a binary corpus file and print as JSON
cargo run -p dencoder -- type OSDMap import /path/to/corpus/file decode dump_json

# List all supported types
cargo run -p dencoder -- list_types
```

## Integration Tests

Integration tests require a running Ceph cluster. Point `CEPH_CONF` at your cluster config:

```bash
export CEPH_CONF=/path/to/ceph.conf

cargo test -p monclient --tests -- --ignored --nocapture
cargo test -p osdclient  --tests -- --ignored --nocapture
cargo test -p msgr2      --tests -- --ignored --nocapture
```

A Docker-based single-node Ceph cluster for local development is available under `docker/`:

```bash
cd docker
docker compose -f docker-compose.ceph.yml up -d
```

## Compatibility

**Minimum Ceph Version**: Octopus (v15, March 2020)

Nautilus and older releases are outside the supported compatibility boundary.

| Release | Version | Date |
|---|---|---|
| Octopus | v15 | March 2020 |
| Pacific | v16 | March 2021 |
| Quincy | v17 | April 2022 |
| Reef | v18 | September 2023 |
| Squid | v19 | September 2024 |

## Reference

- [Ceph msgr2 protocol](https://github.com/ceph/ceph/blob/main/doc/dev/msgr2.rst)
- [librados API](https://docs.ceph.com/en/latest/rados/api/librados/)
- [rust-ceph](https://github.com/ceph/ceph-rust) — official FFI-based Rust bindings

## License

MIT