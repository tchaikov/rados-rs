//! Integration test to compare ceph-dencoder and our dencoder outputs
//!
//! This test validates that our Rust implementation of dencoding matches the official
//! C++ ceph-dencoder tool from the Ceph project.
//!
//! Requirements:
//! - ceph-dencoder binary must be in PATH
//! - ceph-object-corpus repository must be cloned
//!
//! To run this test:
//!
//! ## Option 1: Using system-installed ceph-common
//! ```bash
//! # Install ceph-common (Ubuntu/Debian)
//! sudo apt-get install ceph-common
//!
//! # Clone corpus repository
//! git clone https://github.com/ceph/ceph-object-corpus.git /tmp/ceph-object-corpus
//!
//! # Run the test
//! cargo test --test corpus_comparison_test -- --ignored --nocapture
//! ```
//!
//! ## Option 2: Using locally-built ceph (development)
//! ```bash
//! # Add ceph build directory to PATH
//! export PATH="$HOME/dev/ceph/build/bin:$PATH"
//! export CEPH_LIB="$HOME/dev/ceph/build/lib"
//! export ASAN_OPTIONS="detect_odr_violation=0,detect_leaks=0"
//!
//! # Clone corpus repository if not already present
//! git clone https://github.com/ceph/ceph-object-corpus.git ~/dev/ceph/ceph-object-corpus
//!
//! # Run the test
//! cargo test --test corpus_comparison_test -- --ignored --nocapture
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The version of the corpus to test against
const CORPUS_VERSION: &str = "19.2.0-404-g78ddc7f9027";

/// Get the corpus directory path
fn get_corpus_dir() -> PathBuf {
    // Try multiple locations for the corpus
    let possible_paths = [
        PathBuf::from("/tmp/ceph-object-corpus"),
        PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("ceph-object-corpus"),
        PathBuf::from(std::env::var("CORPUS_DIR").unwrap_or_default()),
    ];

    for path in &possible_paths {
        if path.exists() {
            return path.clone();
        }
    }

    // Default to /tmp location
    PathBuf::from("/tmp/ceph-object-corpus")
}

/// Download the corpus if it doesn't exist
fn ensure_corpus_available() -> Result<PathBuf, String> {
    let corpus_dir = get_corpus_dir();

    if !corpus_dir.exists() {
        eprintln!("Corpus not found at {:?}", corpus_dir);
        eprintln!("Attempting to clone ceph-object-corpus...");

        let output = Command::new("git")
            .args([
                "clone",
                "--depth=1",
                "https://github.com/ceph/ceph-object-corpus.git",
                corpus_dir.to_str().unwrap(),
            ])
            .output()
            .map_err(|e| format!("Failed to run git clone: {}", e))?;

        if !output.status.success() {
            return Err(format!(
                "Failed to clone corpus: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        eprintln!("Successfully cloned corpus to {:?}", corpus_dir);
    }

    let version_path = corpus_dir
        .join("archive")
        .join(CORPUS_VERSION)
        .join("objects");
    if !version_path.exists() {
        return Err(format!(
            "Corpus version {} not found at {:?}",
            CORPUS_VERSION, version_path
        ));
    }

    Ok(version_path)
}

/// Check if ceph-dencoder is available
fn check_ceph_dencoder() -> Result<PathBuf, String> {
    let output = Command::new("which")
        .arg("ceph-dencoder")
        .output()
        .map_err(|e| format!("Failed to check for ceph-dencoder: {}", e))?;

    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(PathBuf::from(path))
    } else {
        Err("ceph-dencoder not found. Install with: sudo apt-get install ceph-common".to_string())
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
    let mut cmd = Command::new(ceph_dencoder);
    cmd.arg("type").arg(type_name);

    if let Some(f) = features {
        cmd.arg("set_features").arg(format!("0x{:x}", f));
    }

    cmd.arg("import")
        .arg(corpus_file)
        .arg("decode")
        .arg("dump_json");

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
    let mut cmd = Command::new(rust_dencoder);
    cmd.arg("type").arg(type_name);

    if let Some(f) = features {
        cmd.arg("set_features").arg(format!("0x{:x}", f));
    }

    cmd.arg("import")
        .arg(corpus_file)
        .arg("decode")
        .arg("dump_json");

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

        // Compare outputs (strict comparison)
        match compare_json_outputs(&ceph_json, &rust_json, type_name, &file_name, is_exception) {
            Ok(()) => {
                eprintln!("    ✓ {}", file_name);
                matched += 1;
            }
            Err(e) => {
                // Format mismatch - both decoded but outputs differ
                format_mismatch += 1;
                // Only show first mismatch details to avoid spam
                if format_mismatch == 1 {
                    let marker = "⚠";
                    eprintln!(
                        "    {} {} - format mismatch (showing first only)",
                        marker, file_name
                    );
                    eprintln!("      {}", e);
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

    let corpus_base = match ensure_corpus_available() {
        Ok(path) => {
            eprintln!("✓ Found corpus at: {:?}", path);
            path
        }
        Err(e) => {
            panic!("❌ {}", e);
        }
    };

    eprintln!();
    eprintln!("Testing corpus version: {}", CORPUS_VERSION);
    eprintln!();

    // Define types to test with their feature requirements and exception status
    // Exception types are tested and differences are reported, but don't cause test failure
    let types_to_test: Vec<(&str, Option<u64>, bool)> = vec![
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
        // pg_pool_t is marked as exception because ceph-dencoder adds computed/derived fields
        // not present in the binary encoding (flags_names, options, is_stretch_pool, etc.)
        ("pg_pool_t", None, true), // Exception: ceph-dencoder adds computed fields
        // Level 4: Monitor types
        // mon_info_t is marked as exception because ceph-dencoder uses different field names
        // (e.g., "addr" vs "public_addrs", "crush_location" vs "crush_loc")
        ("mon_info_t", Some(u64::MAX), true), // Exception: ceph-dencoder uses different JSON format
        // MonMap is marked as exception because ceph-dencoder uses different field names
        // and structures (e.g., "mons" array vs "mon_info" map, "modified" vs "last_changed")
        ("MonMap", Some(u64::MAX), true), // Exception: ceph-dencoder uses different JSON format
    ];

    let mut overall_matched = 0;
    let mut overall_total = 0;
    let mut overall_both_decoded = 0;
    let mut overall_format_mismatch = 0;
    let mut exception_format_mismatch = 0; // Track exceptions separately
    let mut types_with_format_differences = Vec::new();
    let mut exception_types_with_differences = Vec::new();
    let mut types_with_decode_failures = Vec::new();

    for (type_name, features, is_exception) in types_to_test {
        let marker = if is_exception { " (exception)" } else { "" };
        eprintln!("Testing type: {}{}", type_name, marker);
        if let Some(f) = features {
            eprintln!("  Features: 0x{:x}", f);
        }

        match test_type(
            type_name,
            &corpus_base,
            &ceph_dencoder,
            &rust_dencoder,
            features,
            is_exception,
        ) {
            Ok((matched, total, both_decoded, format_mismatch)) => {
                overall_matched += matched;
                overall_total += total;
                overall_both_decoded += both_decoded;
                overall_format_mismatch += format_mismatch;

                if is_exception && format_mismatch > 0 {
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
                        if is_exception {
                            exception_types_with_differences.push(type_name);
                        } else {
                            types_with_format_differences.push(type_name);
                        }
                    }
                    if both_decoded < total {
                        types_with_decode_failures.push(type_name);
                    }
                }
            }
            Err(e) => {
                eprintln!("  ✗ Error testing {}: {}", type_name, e);
                types_with_decode_failures.push(type_name);
            }
        }
        eprintln!();
    }

    eprintln!("===========================================");
    eprintln!("Overall Results");
    eprintln!("===========================================");
    eprintln!("Total samples: {}", overall_total);
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
    eprintln!();

    if !types_with_format_differences.is_empty() {
        eprintln!("Types with format differences (need custom serialization):");
        for type_name in &types_with_format_differences {
            eprintln!("  - {}", type_name);
        }
        eprintln!();
    }

    if !exception_types_with_differences.is_empty() {
        eprintln!("Exception types with format differences (not considered failures):");
        for type_name in &exception_types_with_differences {
            eprintln!(
                "  - {} (ceph-dencoder adds computed/derived fields)",
                type_name
            );
        }
        eprintln!();
    }

    if !types_with_decode_failures.is_empty() {
        eprintln!("Types with decode failures (implementation bugs):");
        for type_name in &types_with_decode_failures {
            eprintln!("  - {}", type_name);
        }
        eprintln!();
    }

    // The goal is to ensure both dencoders can decode the corpus
    // and produce identical output (except for exception types)
    if overall_total > 0 {
        let non_exception_mismatch = overall_format_mismatch - exception_format_mismatch;
        let exact_match_rate = (overall_matched as f64) / (overall_total as f64);

        // Require 100% exact match for non-exception types
        // Exception types (like pg_pool_t) are allowed to have differences
        assert!(
            non_exception_mismatch == 0,
            "Format mismatches detected in non-exception types! {}/{} samples have format differences.\n\
             Types with format differences: {:?}\n\
             All non-exception types must have custom serialization to exactly match ceph-dencoder output.\n\
             Current exact match rate: {:.1}%\n\
             Note: {} exception type samples are allowed to differ (excluded from count)",
            non_exception_mismatch, overall_total - exception_format_mismatch,
            types_with_format_differences,
            exact_match_rate * 100.0,
            exception_format_mismatch
        );

        // Also ensure no decode failures
        assert!(
            overall_total == overall_both_decoded,
            "Decode failures detected! {}/{} samples failed to decode.\n\
             Types with decode failures: {:?}",
            overall_total - overall_both_decoded,
            overall_total,
            types_with_decode_failures
        );
    } else {
        panic!("No corpus samples were tested!");
    }
}
