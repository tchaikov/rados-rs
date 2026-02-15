//! Integration test to compare ceph-dencoder and our dencoder outputs
//!
//! This test validates that our Rust implementation of dencoding matches the official
//! C++ ceph-dencoder tool from the Ceph project.
//!
//! Following Ceph's readable.sh pattern, this test verifies:
//! 1. Decode correctness: Rust decode matches Ceph decode
//! 2. Ceph roundtrip consistency: Ceph's decode == decode→encode→decode
//! 3. Rust roundtrip consistency: Rust's decode == decode→encode→decode
//! 4. Cross-implementation interoperability:
//!    - Rust-encoded binary can be decoded by Ceph
//!    - Ceph-encoded binary can be decoded by Rust
//!
//! This matches Ceph's readable.sh which tests that for each implementation,
//! the roundtrip (decode→encode→decode) produces the same result as just decode.
//! Additionally, we test full interoperability by verifying that binaries
//! encoded by one implementation can be decoded by the other.
//!
//! Requirements:
//! - ceph-dencoder binary must be in PATH or specified via CEPH_DENCODER
//! - ceph-object-corpus repository will be cloned automatically if not present
//!
//! Environment variables:
//! - CEPH_DENCODER: Path to ceph-dencoder binary (default: search in PATH)
//! - CORPUS_VERSION: Version to test (default: test 18.2.0 and 19.2.0)
//! - CORPUS_ROOT: Root directory of corpus (default: /tmp/ceph-object-corpus)
//! - CORPUS_TYPE: Specific type to test (default: test all types)
//!
//! To run this test:
//!
//! ```bash
//! # Test with custom ceph-dencoder path
//! CEPH_DENCODER=/path/to/ceph-dencoder cargo test -p dencoder --test corpus_comparison_test -- --ignored --nocapture
//!
//! # Test with specific version
//! CORPUS_VERSION=18.2.0 cargo test -p dencoder --test corpus_comparison_test -- --ignored --nocapture
//!
//! # Test with specific type
//! CORPUS_TYPE=pg_t cargo test -p dencoder --test corpus_comparison_test -- --ignored --nocapture
//!
//! # Test all versions (default)
//! cargo test -p dencoder --test corpus_comparison_test -- --ignored --nocapture
//! ```

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Get the corpus root directory from environment or default location
fn get_corpus_root() -> PathBuf {
    // Check environment variable first
    if let Ok(corpus_root) = env::var("CORPUS_ROOT") {
        return PathBuf::from(corpus_root);
    }

    // Default to /tmp location
    PathBuf::from("/tmp/ceph-object-corpus")
}

/// Ensure corpus is available, cloning if necessary
fn ensure_corpus_available() -> Result<PathBuf, String> {
    let corpus_root = get_corpus_root();

    if !corpus_root.exists() {
        eprintln!("Corpus not found at {:?}", corpus_root);
        eprintln!("Attempting to clone ceph-object-corpus...");

        let output = Command::new("git")
            .args([
                "clone",
                "--depth=1",
                "https://github.com/ceph/ceph-object-corpus.git",
                corpus_root.to_str().unwrap(),
            ])
            .output()
            .map_err(|e| format!("Failed to run git clone: {}", e))?;

        if !output.status.success() {
            return Err(format!(
                "Failed to clone corpus: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        eprintln!("Successfully cloned corpus to {:?}", corpus_root);
    }

    Ok(corpus_root)
}

/// Get available corpus versions
fn get_available_versions(corpus_root: &Path) -> Result<Vec<String>, String> {
    let archive_dir = corpus_root.join("archive");

    if !archive_dir.exists() {
        return Err(format!(
            "Archive directory not found at {:?}. Clone ceph-object-corpus repository.",
            archive_dir
        ));
    }

    let mut versions = Vec::new();
    for entry in fs::read_dir(&archive_dir)
        .map_err(|e| format!("Failed to read archive directory: {}", e))?
        .flatten()
    {
        if entry.path().is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                versions.push(name.to_string());
            }
        }
    }

    versions.sort();
    Ok(versions)
}

/// Get corpus path for a specific version
fn get_corpus_path(corpus_root: &Path, version: &str) -> Result<PathBuf, String> {
    let version_path = corpus_root.join("archive").join(version).join("objects");

    if !version_path.exists() {
        return Err(format!(
            "Corpus version {} not found at {:?}",
            version, version_path
        ));
    }

    Ok(version_path)
}

/// Check if ceph-dencoder is available
fn check_ceph_dencoder() -> Result<PathBuf, String> {
    // Check if path is provided via environment variable
    if let Ok(path_str) = env::var("CEPH_DENCODER") {
        let path = PathBuf::from(path_str);
        if path.exists() {
            return Ok(path);
        } else {
            return Err(format!(
                "CEPH_DENCODER path does not exist: {}",
                path.display()
            ));
        }
    }

    // Otherwise, search in PATH
    let output = Command::new("which")
        .arg("ceph-dencoder")
        .output()
        .map_err(|e| format!("Failed to check for ceph-dencoder: {}", e))?;

    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(PathBuf::from(path))
    } else {
        Err("ceph-dencoder not found. Install with: sudo apt-get install ceph-common\nOr specify path with: CEPH_DENCODER=/path/to/ceph-dencoder".to_string())
    }
}

/// Get path to our Rust dencoder
fn get_rust_dencoder() -> Result<PathBuf, String> {
    // Try to find the dencoder binary
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let debug_path = manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target/debug/dencoder");

    if debug_path.exists() {
        return Ok(debug_path);
    }

    // Try to build it
    eprintln!("Building dencoder...");
    let output = Command::new("cargo")
        .args(["build", "--bin", "dencoder"])
        .current_dir(manifest_dir.parent().unwrap().parent().unwrap())
        .output()
        .map_err(|e| format!("Failed to build dencoder: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "Failed to build dencoder: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    if debug_path.exists() {
        Ok(debug_path)
    } else {
        Err("dencoder binary not found after build".to_string())
    }
}

/// Run ceph-dencoder on a corpus file and get JSON output
fn run_ceph_dencoder(
    ceph_dencoder: &Path,
    type_name: &str,
    corpus_file: &Path,
    features: Option<u64>,
) -> Result<String, String> {
    run_ceph_dencoder_with_ops(ceph_dencoder, type_name, corpus_file, features, false)
}

/// Run ceph-dencoder with optional roundtrip (encode before final decode)
/// Following Ceph's readable.sh pattern:
/// - Normal: import decode dump_json
/// - Roundtrip: import decode encode decode dump_json
fn run_ceph_dencoder_with_ops(
    ceph_dencoder: &Path,
    type_name: &str,
    corpus_file: &Path,
    features: Option<u64>,
    roundtrip: bool,
) -> Result<String, String> {
    let mut cmd = Command::new(ceph_dencoder);
    cmd.arg("type").arg(type_name);

    if let Some(f) = features {
        cmd.arg("set_features").arg(format!("0x{:x}", f));
    }

    cmd.arg("import").arg(corpus_file).arg("decode");

    if roundtrip {
        cmd.arg("encode").arg("decode");
    }

    cmd.arg("dump_json");

    let output = cmd
        .output()
        .map_err(|e| format!("Failed to run ceph-dencoder: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "ceph-dencoder failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Run our Rust dencoder on a corpus file and get JSON output
fn run_rust_dencoder(
    rust_dencoder: &Path,
    type_name: &str,
    corpus_file: &Path,
    features: Option<u64>,
) -> Result<String, String> {
    run_rust_dencoder_with_ops(rust_dencoder, type_name, corpus_file, features, false)
}

/// Run our Rust dencoder with optional roundtrip (encode before final decode)
fn run_rust_dencoder_with_ops(
    rust_dencoder: &Path,
    type_name: &str,
    corpus_file: &Path,
    features: Option<u64>,
    roundtrip: bool,
) -> Result<String, String> {
    let mut cmd = Command::new(rust_dencoder);
    cmd.arg("type").arg(type_name);

    if let Some(f) = features {
        cmd.arg("set_features").arg(format!("0x{:x}", f));
    }

    cmd.arg("import").arg(corpus_file).arg("decode");

    if roundtrip {
        cmd.arg("encode").arg("decode");
    }

    cmd.arg("dump_json");

    let output = cmd
        .output()
        .map_err(|e| format!("Failed to run rust dencoder: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "rust dencoder failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let full_output = String::from_utf8_lossy(&output.stdout).to_string();

    // Extract JSON from output - it starts with '{' or '['
    // Our dencoder prints status messages before the JSON
    let json_start = full_output
        .find(['{', '['])
        .ok_or_else(|| format!("No JSON found in output: {}", full_output))?;

    Ok(full_output[json_start..].trim().to_string())
}

/// Compare two JSON outputs
/// Performs strict comparison and reports differences
fn compare_json_outputs(
    ceph_json: &str,
    rust_json: &str,
    type_name: &str,
    file_name: &str,
    is_exception: bool,
) -> Result<(), String> {
    let ceph_value: serde_json::Value =
        serde_json::from_str(ceph_json).map_err(|e| format!("Failed to parse ceph JSON: {}", e))?;

    let rust_value: serde_json::Value =
        serde_json::from_str(rust_json).map_err(|e| format!("Failed to parse rust JSON: {}", e))?;

    // Perform strict comparison
    if ceph_value != rust_value {
        // Format both for easy comparison
        let ceph_pretty = serde_json::to_string_pretty(&ceph_value).unwrap();
        let rust_pretty = serde_json::to_string_pretty(&rust_value).unwrap();

        let error_msg = format!(
            "JSON output mismatch for {} in {}:\n\nCeph output:\n{}\n\nRust output:\n{}\n",
            type_name, file_name, ceph_pretty, rust_pretty
        );

        // For exception types (like pg_pool_t), we report but don't fail
        if is_exception {
            return Err(error_msg);
        }

        return Err(error_msg);
    }

    Ok(())
}

/// Export encoded binary from a dencoder to a temporary file
/// Returns the path to the temporary file
fn export_encoded_binary(
    dencoder: &Path,
    type_name: &str,
    corpus_file: &Path,
    features: Option<u64>,
) -> Result<PathBuf, String> {
    use std::env;

    // Create a unique temporary file
    let temp_dir = env::temp_dir();
    let temp_file = temp_dir.join(format!(
        "dencoder_export_{}_{}.bin",
        type_name,
        std::process::id()
    ));

    let mut cmd = Command::new(dencoder);
    cmd.arg("type").arg(type_name);

    if let Some(f) = features {
        cmd.arg("set_features").arg(format!("0x{:x}", f));
    }

    cmd.arg("import")
        .arg(corpus_file)
        .arg("decode")
        .arg("encode")
        .arg("export")
        .arg(&temp_file);

    let output = cmd
        .output()
        .map_err(|e| format!("Failed to run dencoder export: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "dencoder export failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    if !temp_file.exists() {
        return Err(format!("Export file was not created: {:?}", temp_file));
    }

    Ok(temp_file)
}

/// Test cross-implementation decode: Rust encodes → Ceph decodes
/// Returns Ok(()) if successful, Err with details if failed
fn test_rust_encode_ceph_decode(
    rust_dencoder: &Path,
    ceph_dencoder: &Path,
    type_name: &str,
    corpus_file: &Path,
    features: Option<u64>,
    original_rust_json: &str,
    is_exception: bool,
) -> Result<(), String> {
    // Export Rust-encoded binary
    let rust_encoded_file = export_encoded_binary(rust_dencoder, type_name, corpus_file, features)
        .map_err(|e| format!("Rust export failed: {}", e))?;

    // Decode with Ceph
    let ceph_decoded_json =
        run_ceph_dencoder(ceph_dencoder, type_name, &rust_encoded_file, features).map_err(|e| {
            let _ = fs::remove_file(&rust_encoded_file);
            format!("Ceph decode of Rust-encoded failed: {}", e)
        })?;

    // Clean up temp file
    let _ = fs::remove_file(&rust_encoded_file);

    // Compare: Ceph's decode of Rust-encoded should match original Rust decode
    compare_json_outputs(
        original_rust_json,
        &ceph_decoded_json,
        type_name,
        "rust→ceph",
        is_exception,
    )
}

/// Test cross-implementation decode: Ceph encodes → Rust decodes
/// Returns Ok(()) if successful, Err with details if failed
fn test_ceph_encode_rust_decode(
    ceph_dencoder: &Path,
    rust_dencoder: &Path,
    type_name: &str,
    corpus_file: &Path,
    features: Option<u64>,
    original_ceph_json: &str,
    is_exception: bool,
) -> Result<(), String> {
    // Export Ceph-encoded binary
    let ceph_encoded_file = export_encoded_binary(ceph_dencoder, type_name, corpus_file, features)
        .map_err(|e| format!("Ceph export failed: {}", e))?;

    // Decode with Rust
    let rust_decoded_json =
        run_rust_dencoder(rust_dencoder, type_name, &ceph_encoded_file, features).map_err(|e| {
            let _ = fs::remove_file(&ceph_encoded_file);
            format!("Rust decode of Ceph-encoded failed: {}", e)
        })?;

    // Clean up temp file
    let _ = fs::remove_file(&ceph_encoded_file);

    // Compare: Rust's decode of Ceph-encoded should match original Ceph decode
    compare_json_outputs(
        original_ceph_json,
        &rust_decoded_json,
        type_name,
        "ceph→rust",
        is_exception,
    )
}

/// Test a single type across all its corpus samples
fn test_type(
    type_name: &str,
    corpus_base: &Path,
    ceph_dencoder: &Path,
    rust_dencoder: &Path,
    features: Option<u64>,
    is_exception: bool,
) -> Result<(usize, usize, usize, usize), String> {
    let type_dir = corpus_base.join(type_name);

    if !type_dir.exists() {
        eprintln!("  ⚠ No corpus directory found for {}", type_name);
        return Ok((0, 0, 0, 0));
    }

    let entries: Vec<_> = fs::read_dir(&type_dir)
        .map_err(|e| format!("Failed to read directory {}: {}", type_dir.display(), e))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .collect();

    if entries.is_empty() {
        eprintln!("  ⚠ No corpus files found in {}", type_dir.display());
        return Ok((0, 0, 0, 0));
    }

    let mut matched = 0;
    let mut total = 0;
    let mut both_decoded = 0;
    let mut format_mismatch = 0;

    for entry in entries {
        let corpus_file = entry.path();
        let file_name = corpus_file.file_name().unwrap().to_string_lossy();

        total += 1;

        // Run both dencoders
        let ceph_json = match run_ceph_dencoder(ceph_dencoder, type_name, &corpus_file, features) {
            Ok(json) => json,
            Err(e) => {
                eprintln!("    ✗ ceph-dencoder failed for {}: {}", file_name, e);
                continue;
            }
        };

        let rust_json = match run_rust_dencoder(rust_dencoder, type_name, &corpus_file, features) {
            Ok(json) => json,
            Err(e) => {
                eprintln!("    ✗ rust dencoder failed for {}: {}", file_name, e);
                continue;
            }
        };

        // Both decoders succeeded
        both_decoded += 1;

        // Test 1: Compare decode outputs (decode correctness)
        let decode_matches =
            compare_json_outputs(&ceph_json, &rust_json, type_name, &file_name, is_exception)
                .is_ok();

        // Test 2: Roundtrip consistency (following Ceph's readable.sh)
        // Check if decode == decode→encode→decode for each implementation
        let (ceph_roundtrip_consistent, rust_roundtrip_consistent) = match (
            run_ceph_dencoder_with_ops(ceph_dencoder, type_name, &corpus_file, features, true),
            run_rust_dencoder_with_ops(rust_dencoder, type_name, &corpus_file, features, true),
        ) {
            (Ok(ceph_roundtrip_json), Ok(rust_roundtrip_json)) => {
                // Check Ceph's roundtrip consistency
                let ceph_consistent = compare_json_outputs(
                    &ceph_json,
                    &ceph_roundtrip_json,
                    type_name,
                    &file_name,
                    is_exception,
                )
                .is_ok();

                // Check Rust's roundtrip consistency
                let rust_consistent = compare_json_outputs(
                    &rust_json,
                    &rust_roundtrip_json,
                    type_name,
                    &file_name,
                    is_exception,
                )
                .is_ok();

                (ceph_consistent, rust_consistent)
            }
            _ => {
                // Roundtrip failed for one or both decoders
                (false, false)
            }
        };

        // Test 3: Cross-implementation decode (interoperability)
        // Check if Rust-encoded can be decoded by Ceph, and vice versa
        let rust_to_ceph_works = test_rust_encode_ceph_decode(
            rust_dencoder,
            ceph_dencoder,
            type_name,
            &corpus_file,
            features,
            &rust_json,
            is_exception,
        )
        .is_ok();

        let ceph_to_rust_works = test_ceph_encode_rust_decode(
            ceph_dencoder,
            rust_dencoder,
            type_name,
            &corpus_file,
            features,
            &ceph_json,
            is_exception,
        )
        .is_ok();

        // Overall success requires all tests to pass
        if decode_matches
            && ceph_roundtrip_consistent
            && rust_roundtrip_consistent
            && rust_to_ceph_works
            && ceph_to_rust_works
        {
            eprintln!("    ✓ {} (decode + roundtrip + cross-decode)", file_name);
            matched += 1;
        } else {
            // Format mismatch - both decoded but outputs differ
            format_mismatch += 1;
            // Only show first mismatch details to avoid spam
            if format_mismatch == 1 {
                let marker = "⚠";
                let mut reasons = Vec::new();
                if !decode_matches {
                    reasons.push("decode mismatch");
                }
                if !ceph_roundtrip_consistent {
                    reasons.push("ceph roundtrip inconsistent");
                }
                if !rust_roundtrip_consistent {
                    reasons.push("rust roundtrip inconsistent");
                }
                if !rust_to_ceph_works {
                    reasons.push("rust→ceph interop failed");
                }
                if !ceph_to_rust_works {
                    reasons.push("ceph→rust interop failed");
                }
                eprintln!(
                    "    {} {} - {} (showing first only)",
                    marker,
                    file_name,
                    reasons.join(", ")
                );

                // Show decode mismatch if that's the issue
                if !decode_matches {
                    if let Err(e) = compare_json_outputs(
                        &ceph_json,
                        &rust_json,
                        type_name,
                        &file_name,
                        is_exception,
                    ) {
                        eprintln!("      Decode: {}", e);
                    }
                }

                // Show roundtrip issues
                if !ceph_roundtrip_consistent || !rust_roundtrip_consistent {
                    if let (Ok(ceph_rt), Ok(rust_rt)) = (
                        run_ceph_dencoder_with_ops(
                            ceph_dencoder,
                            type_name,
                            &corpus_file,
                            features,
                            true,
                        ),
                        run_rust_dencoder_with_ops(
                            rust_dencoder,
                            type_name,
                            &corpus_file,
                            features,
                            true,
                        ),
                    ) {
                        if !ceph_roundtrip_consistent {
                            if let Err(e) = compare_json_outputs(
                                &ceph_json,
                                &ceph_rt,
                                type_name,
                                &file_name,
                                is_exception,
                            ) {
                                eprintln!("      Ceph roundtrip: {}", e);
                            }
                        }
                        if !rust_roundtrip_consistent {
                            if let Err(e) = compare_json_outputs(
                                &rust_json,
                                &rust_rt,
                                type_name,
                                &file_name,
                                is_exception,
                            ) {
                                eprintln!("      Rust roundtrip: {}", e);
                            }
                        }
                    }
                }

                // Show cross-decode issues
                if !rust_to_ceph_works {
                    if let Err(e) = test_rust_encode_ceph_decode(
                        rust_dencoder,
                        ceph_dencoder,
                        type_name,
                        &corpus_file,
                        features,
                        &rust_json,
                        is_exception,
                    ) {
                        eprintln!("      Rust→Ceph: {}", e);
                    }
                }
                if !ceph_to_rust_works {
                    if let Err(e) = test_ceph_encode_rust_decode(
                        ceph_dencoder,
                        rust_dencoder,
                        type_name,
                        &corpus_file,
                        features,
                        &ceph_json,
                        is_exception,
                    ) {
                        eprintln!("      Ceph→Rust: {}", e);
                    }
                }
            }
        }
    }

    Ok((matched, total, both_decoded, format_mismatch))
}

/// Main integration test
#[test]
#[ignore] // Requires ceph-dencoder and corpus to be installed
fn test_corpus_comparison() {
    // Get configuration from environment
    let corpus_root = match ensure_corpus_available() {
        Ok(root) => {
            eprintln!("✓ Found corpus root at: {:?}", root);
            root
        }
        Err(e) => {
            panic!("❌ {}", e);
        }
    };

    let requested_version = env::var("CORPUS_VERSION").ok();
    let requested_type = env::var("CORPUS_TYPE").ok();

    let versions_to_test = if let Some(version) = &requested_version {
        vec![version.clone()]
    } else {
        // Test both 18.2.0 and 19.2.0 if available
        match get_available_versions(&corpus_root) {
            Ok(all_versions) => {
                let mut versions = Vec::new();
                for v in &all_versions {
                    if v.starts_with("18.2.0") || v.starts_with("19.2.0") {
                        versions.push(v.clone());
                    }
                }
                if versions.is_empty() {
                    eprintln!(
                        "⚠ No 18.2.0 or 19.2.0 versions found, testing all available versions"
                    );
                    all_versions
                } else {
                    versions
                }
            }
            Err(e) => {
                panic!("❌ Failed to get available versions: {}", e);
            }
        }
    };

    eprintln!("===========================================");
    eprintln!("Corpus Comparison Test");
    eprintln!("===========================================");
    eprintln!();

    // Check prerequisites
    let ceph_dencoder = match check_ceph_dencoder() {
        Ok(path) => {
            eprintln!("✓ Found ceph-dencoder at: {:?}", path);
            path
        }
        Err(e) => {
            panic!("❌ {}", e);
        }
    };

    let rust_dencoder = match get_rust_dencoder() {
        Ok(path) => {
            eprintln!("✓ Found rust dencoder at: {:?}", path);
            path
        }
        Err(e) => {
            panic!("❌ {}", e);
        }
    };

    eprintln!();
    if let Some(ref version) = requested_version {
        eprintln!("Testing specific version: {}", version);
    } else {
        eprintln!("Testing versions: {:?}", versions_to_test);
    }
    if let Some(ref type_name) = requested_type {
        eprintln!("Testing specific type: {}", type_name);
    }
    eprintln!();

    // Define types to test with their feature requirements and exception status
    let all_types: Vec<(&str, Option<u64>, bool)> = vec![
        // Level 1: Primitive types
        ("pg_t", None, false),
        ("eversion_t", None, false),
        ("utime_t", None, false),
        ("uuid_d", None, false),
        ("osd_info_t", None, false),
        // Level 2: Types depending on Level 1
        ("entity_addr_t", Some(0x40000000000000), false), // MSG_ADDR2 feature
        ("pool_snap_info_t", None, false),
        ("osd_xinfo_t", None, false),
        // Level 3: Complex types
        ("pg_merge_meta_t", None, false),
        ("pg_pool_t", None, true), // Exception: ceph-dencoder adds computed fields
        ("mon_info_t", Some(u64::MAX), true), // Exception: different JSON format
        ("MonMap", Some(u64::MAX), true), // Exception: different JSON format
    ];

    let types_to_test: Vec<(&str, Option<u64>, bool)> = if let Some(ref type_name) = requested_type
    {
        // Filter to specific type
        all_types
            .into_iter()
            .filter(|(name, _, _)| *name == type_name)
            .collect()
    } else {
        all_types
    };

    if types_to_test.is_empty() {
        panic!("❌ No types matched filter: {:?}", requested_type);
    }

    let mut version_results = Vec::new();

    for version in &versions_to_test {
        eprintln!("===========================================");
        eprintln!("Testing Version: {}", version);
        eprintln!("===========================================");
        eprintln!();

        let corpus_base = match get_corpus_path(&corpus_root, version) {
            Ok(path) => {
                eprintln!("✓ Found corpus at: {:?}", path);
                path
            }
            Err(e) => {
                eprintln!("⚠ Skipping version {}: {}", version, e);
                continue;
            }
        };

        let mut overall_matched = 0;
        let mut overall_total = 0;
        let mut overall_both_decoded = 0;
        let mut overall_format_mismatch = 0;
        let mut exception_format_mismatch = 0;
        let mut types_with_format_differences = Vec::new();
        let mut exception_types_with_differences = Vec::new();
        let mut types_with_decode_failures = Vec::new();

        for (type_name, features, is_exception) in &types_to_test {
            let marker = if *is_exception { " (exception)" } else { "" };
            eprintln!("Testing type: {}{}", type_name, marker);
            if let Some(f) = features {
                eprintln!("  Features: 0x{:x}", f);
            }

            match test_type(
                type_name,
                &corpus_base,
                &ceph_dencoder,
                &rust_dencoder,
                *features,
                *is_exception,
            ) {
                Ok((matched, total, both_decoded, format_mismatch)) => {
                    overall_matched += matched;
                    overall_total += total;
                    overall_both_decoded += both_decoded;
                    overall_format_mismatch += format_mismatch;

                    if *is_exception && format_mismatch > 0 {
                        exception_format_mismatch += format_mismatch;
                    }

                    if total > 0 {
                        eprintln!(
                            "  Result: {}/{} exact match, {} format differences, {} decode failures",
                            matched,
                            total,
                            format_mismatch,
                            total - both_decoded
                        );

                        if format_mismatch > 0 {
                            if *is_exception {
                                exception_types_with_differences.push(*type_name);
                            } else {
                                types_with_format_differences.push(*type_name);
                            }
                        }
                        if both_decoded < total {
                            types_with_decode_failures.push(*type_name);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  ✗ Error testing {}: {}", type_name, e);
                    types_with_decode_failures.push(*type_name);
                }
            }
            eprintln!();
        }

        eprintln!("-------------------------------------------");
        eprintln!("Results for Version: {}", version);
        eprintln!("-------------------------------------------");
        eprintln!("Total samples: {}", overall_total);
        if overall_total > 0 {
            eprintln!(
                "  Exact match: {}/{} ({:.1}%)",
                overall_matched,
                overall_total,
                (overall_matched as f64 / overall_total as f64) * 100.0
            );
            eprintln!(
                "  Format differences: {}/{} ({:.1}%)",
                overall_format_mismatch,
                overall_total,
                (overall_format_mismatch as f64 / overall_total as f64) * 100.0
            );
            eprintln!("    - Exception types: {}", exception_format_mismatch);
            eprintln!(
                "    - Non-exception types: {}",
                overall_format_mismatch - exception_format_mismatch
            );
            eprintln!(
                "  Decode failures: {}/{} ({:.1}%)",
                overall_total - overall_both_decoded,
                overall_total,
                ((overall_total - overall_both_decoded) as f64 / overall_total as f64) * 100.0
            );
        }
        eprintln!();

        version_results.push((
            version.clone(),
            overall_matched,
            overall_total,
            overall_both_decoded,
            overall_format_mismatch,
            exception_format_mismatch,
            types_with_format_differences.clone(),
            types_with_decode_failures.clone(),
        ));
    }

    // Overall summary across all versions
    eprintln!("===========================================");
    eprintln!("Overall Summary Across All Versions");
    eprintln!("===========================================");

    let mut total_passed_versions = 0;
    for (
        version,
        matched,
        total,
        both_decoded,
        format_mismatch,
        exception_mismatch,
        format_diffs,
        decode_fails,
    ) in &version_results
    {
        eprintln!("Version: {}", version);
        if *total > 0 {
            let non_exception_mismatch = format_mismatch - exception_mismatch;
            let passed = non_exception_mismatch == 0 && *total == *both_decoded;

            eprintln!(
                "  Status: {} - {}/{} exact match, {} non-exception format diffs, {} decode failures",
                if passed { "✓ PASS" } else { "✗ FAIL" },
                matched,
                total,
                non_exception_mismatch,
                total - both_decoded
            );

            if passed {
                total_passed_versions += 1;
            }

            if !format_diffs.is_empty() {
                eprintln!("  Format differences: {:?}", format_diffs);
            }
            if !decode_fails.is_empty() {
                eprintln!("  Decode failures: {:?}", decode_fails);
            }
        }
    }
    eprintln!();
    eprintln!(
        "Passed versions: {}/{}",
        total_passed_versions,
        version_results.len()
    );

    // Fail if any non-exception types have issues
    for (
        version,
        _matched,
        total,
        both_decoded,
        format_mismatch,
        exception_mismatch,
        format_diffs,
        decode_fails,
    ) in &version_results
    {
        if *total == 0 {
            continue;
        }

        let non_exception_mismatch = format_mismatch - exception_mismatch;

        assert!(
            non_exception_mismatch == 0,
            "Version {}: Format mismatches in non-exception types! Types: {:?}",
            version,
            format_diffs
        );

        assert!(
            *total == *both_decoded,
            "Version {}: Decode failures detected! Types: {:?}",
            version,
            decode_fails
        );
    }

    assert!(
        !version_results.is_empty(),
        "No corpus versions were tested!"
    );
}
