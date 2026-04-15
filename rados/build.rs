//! Build script for the `rados` crate.
//!
//! No-op for the default build. Only the `bench-librados` feature
//! triggers any work here, which gates the in-tree librados FFI binding
//! used by the A/B benchmark harness. Without that feature the script
//! returns immediately — the main library has zero build-time
//! dependency on `librados.so`.
//!
//! When `bench-librados` is enabled the script locates `librados.so`
//! via (in order): `LIBRADOS_LIB_DIR`, `CEPH_LIB`,
//! `$HOME/dev/ceph/build/lib`, and a short list of system default
//! paths. It then emits link and rpath directives so the benches and
//! example bind at run time without requiring `LD_LIBRARY_PATH`.

use std::path::PathBuf;

fn main() {
    // Gate all work on the bench feature — Cargo sets this env var when
    // the feature is active. Without it, produce zero output so the
    // default build never searches for or links against librados.
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_BENCH_LIBRADOS");
    if std::env::var_os("CARGO_FEATURE_BENCH_LIBRADOS").is_none() {
        return;
    }

    if !cfg!(target_os = "linux") {
        return;
    }

    println!("cargo:rerun-if-env-changed=LIBRADOS_LIB_DIR");
    println!("cargo:rerun-if-env-changed=CEPH_LIB");

    let lib_dir = find_lib_dir().expect(
        "librados.so not found. Set CEPH_LIB or LIBRADOS_LIB_DIR to the directory that contains it.",
    );

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=rados");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=m");
    println!("cargo:rustc-link-lib=rt");
}

fn find_lib_dir() -> Option<PathBuf> {
    for var in ["CEPH_LIB", "LIBRADOS_LIB_DIR"] {
        if let Ok(dir) = std::env::var(var) {
            let candidate = PathBuf::from(&dir);
            if candidate.join("librados.so").exists() {
                return Some(candidate);
            }
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let dev = PathBuf::from(home).join("dev/ceph/build/lib");
        if dev.join("librados.so").exists() {
            return Some(dev);
        }
    }

    for path in [
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib64",
        "/usr/lib",
        "/usr/local/lib",
    ] {
        let candidate = PathBuf::from(path);
        if candidate.join("librados.so").exists() {
            return Some(candidate);
        }
    }

    None
}
