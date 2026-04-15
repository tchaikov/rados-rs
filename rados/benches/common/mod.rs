// Each bench file in `benches/` is a separate compilation unit that
// `mod common;`s this module in. Any individual bench binary only
// consumes a subset of the toolbox (client trait, fixture, one or two
// adapters, etc.), so the other items show up as dead-code warnings
// inside that compilation unit. Suppress wholesale — it's the same
// pattern `tests/common/mod.rs` uses.
#![allow(dead_code)]

//! Shared code for the live-cluster A/B benchmarks against librados.
//!
//! This module is compiled only when one of the `bench-librados` bench or
//! example targets asks for it: those targets carry
//! `required-features = ["bench-librados"]` in `rados/Cargo.toml`, so a
//! plain `cargo build` / `cargo test` for regular users never touches any
//! of these files, never links against `librados.so`, and never picks up
//! the FFI build-script dep.
//!
//! File layout:
//! - `client` — `BenchClient` trait that both adapters implement.
//! - `fixture` — environment parsing, `PoolFixture`, payload cache,
//!   pool teardown via RAII.
//! - `resource` — `getrusage` + `/proc/self/status` sampler.
//! - `librados/` — the in-tree librados FFI binding used only by the
//!   bench harness, so the main crate never takes a build-script
//!   dependency on `librados.so`.
//! - `adapter_rados_rs` — adapts `rados::IoCtx` to `BenchClient`.
//! - `adapter_librados` — adapts the in-tree librados binding to
//!   `BenchClient`.

pub mod client;
pub mod fixture;
pub mod librados;
pub mod resource;

pub mod adapter_librados;
pub mod adapter_rados_rs;
