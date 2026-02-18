use std::env;

fn main() {
    // Get Ceph library path from environment or use default
    let ceph_lib = env::var("CEPH_LIB").unwrap_or_else(|_| {
        // Default to kefu's Ceph development build location
        // Users can override with CEPH_LIB environment variable
        "/home/kefu/dev/ceph/build/lib".to_string()
    });

    // Link with libceph-common for CRC32C implementation
    println!("cargo:rustc-link-search=native={}", ceph_lib);
    println!("cargo:rustc-link-lib=dylib=ceph-common");

    // Set rpath so the library can be found at runtime
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", ceph_lib);

    // Rerun if environment variables change
    println!("cargo:rerun-if-env-changed=CEPH_LIB");
    println!("cargo:rerun-if-env-changed=HOME");
}
