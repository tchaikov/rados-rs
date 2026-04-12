# rados-rs

A native Rust implementation of the RADOS (Reliable Autonomic Distributed Object Store) client library for Ceph, built on Tokio for async/await I/O without any C library dependencies.

## Features

- **No FFI**: Pure Rust — no `librados` or `libceph` dependency
- **Async/Await**: Tokio-based async I/O throughout
- **msgr2**: Complete Ceph messenger protocol v2 with AES-128-GCM encryption and CRC32C
- **CephX**: Full authentication and service ticket handling
- **OSD operations**: Write, read, stat, delete on RADOS objects
- **Ceph Quincy+**: Supports Quincy (v17, April 2022) and all later releases

## Workspace layout

The workspace contains two published crates plus a root examples package:

| Package | Description |
|---|---|
| `rados` | Main library crate (`denc`, `auth`, `cephconfig`, `crush`, `msgr2`, `monclient`, `osdclient` modules, plus the `dencoder` binary). |
| `rados-denc-macros` | Proc-macro crate providing DENC derive macros. |
| `examples` | Non-published root examples package, including the `rados` CLI example. |

Within `rados`, the main internal modules are:

- `rados::denc`
- `rados::auth`
- `rados::cephconfig`
- `rados::crush`
- `rados::msgr2`
- `rados::monclient`
- `rados::osdclient`

## Quick start

### `rados` library

```toml
[dependencies]
rados = { path = "rados" }
```

Start with `OSDClient`, `IoCtx`, `RadosObject`, and `list_objects_stream`, then drop down into `rados::monclient` or `rados::msgr2` when you need lower-level control.

### `rados` CLI example

The CLI moved out of the workspace crates into the root `examples` package:

```bash
# Write an object from a local file
cargo run -p examples --example rados -- -p mypool put myobject /path/to/file

# Read an object to stdout
cargo run -p examples --example rados -- -p mypool get myobject -

# Stat an object
cargo run -p examples --example rados -- -p mypool stat myobject

# Remove an object
cargo run -p examples --example rados -- -p mypool rm myobject

# List objects in a pool
cargo run -p examples --example rados -- -p mypool ls
```

By default the example reads `CEPH_CONF` (or `/etc/ceph/ceph.conf`). Pass `-c /path/to/ceph.conf` to override.

### `dencoder` internal tool

A Rust reimplementation of `ceph-dencoder` now lives inside the `rados` package:

```bash
# Decode a binary corpus file and print as JSON
cargo run -p rados --bin dencoder -- type OSDMap import /path/to/corpus/file decode dump_json

# List all supported types
cargo run -p rados --bin dencoder -- list_types
```

## Integration tests

Integration tests require a running Ceph cluster. Point `CEPH_CONF` at your cluster config:

```bash
export CEPH_CONF=/path/to/ceph.conf

# Run all integration tests
cargo test -p rados --tests -- --ignored --nocapture

# Or a single suite
cargo test -p rados --test osdclient_object_io_test -- --ignored --nocapture
```

A Docker-based single-node Ceph cluster for local development is available under `docker/`:

```bash
cd docker
docker compose -f docker-compose.ceph.yml up -d
```

## Compatibility

**Minimum Ceph Version**: Quincy (v17, April 2022)

Pacific and older releases are outside the supported compatibility boundary.

| Release | Version | Date |
|---|---|---|
| Quincy | v17 | April 2022 |
| Reef | v18 | September 2023 |
| Squid | v19 | September 2024 |

## Reference

- [Ceph msgr2 protocol](https://github.com/ceph/ceph/blob/main/doc/dev/msgr2.rst)
- [librados API](https://docs.ceph.com/en/latest/rados/api/librados/)
- [rust-ceph](https://github.com/ceph/ceph-rust) — official FFI-based Rust bindings

## License

MIT
