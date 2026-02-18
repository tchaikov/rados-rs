use std::env;

fn main() {
    // Get Ceph library path from environment or use default
    let ceph_lib = env::var("CEPH_LIB").unwrap_or_else(|_| {
        // Try standard Ceph development build location
        let home = env::var("HOME").expect("HOME environment variable not set");
        format!("{}/dev/ceph/build/lib", home)
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
