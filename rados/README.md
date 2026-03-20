# rados

The main public Rust client library for RADOS object operations on Ceph.

This crate now contains the former support crates as internal modules:

- `rados::denc`
- `rados::auth`
- `rados::cephconfig`
- `rados::crush`
- `rados::msgr2`
- `rados::monclient`
- `rados::osdclient`

At the crate root it re-exports the high-level client surface, including:

- `OSDClient` / `OSDClientConfig`
- `IoCtx`
- `RadosObject`
- `list_objects_stream`
- object operation builders and result types

The `dencoder` binary also lives in this package as an internal tool:

```bash
cargo run -p rados --bin dencoder -- list_types
```
