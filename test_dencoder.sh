#!/bin/bash
#
# Systematic testing script for rados-rs dencoder
# Tests types in dependency order: Level 1 → Level 2 → Level 3
#
# CRITICAL: Each level must pass 100% before moving to the next level
#

set -e  # Exit on first error

CORPUS_DIR=~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects
DENCODER=./target/debug/dencoder
CPP_DENCODER="env ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 CEPH_LIB=$HOME/dev/ceph/build/lib ~/dev/ceph/build/bin/ceph-dencoder"

echo "========================================="
echo "SYSTEMATIC DENCODER VALIDATION"
echo "========================================="
echo ""
echo "Testing types in dependency order:"
echo "  Level 1 → Level 2 → Level 3"
echo ""

# Function to test a single type
test_type() {
    local type_name=$1
    local level=$2

    echo "=== Testing $type_name (Level $level) ==="

    if [ ! -d "$CORPUS_DIR/$type_name" ]; then
        echo "  ⚠ No corpus files found for $type_name"
        return 0
    fi

    local total=0
    local passed=0
    local failed=0

    for corpus_file in "$CORPUS_DIR/$type_name"/*; do
        total=$((total + 1))
        local filename=$(basename "$corpus_file")

        # CRITICAL CHECK: Verify 0 bytes remaining
        output=$($DENCODER type $type_name import "$corpus_file" decode 2>&1 || true)

        if echo "$output" | grep -q "0 bytes remaining"; then
            passed=$((passed + 1))
            echo "  ✓ $filename - Complete consumption"
        else
            failed=$((failed + 1))
            echo "  ✗ $filename - FAILED!"
            echo "    Output: $output"
            echo ""
            echo "STOPPING: Fix $type_name decode logic before continuing!"
            exit 1
        fi
    done

    echo "  Summary: $passed/$total passed"
    echo ""

    if [ $failed -gt 0 ]; then
        echo "❌ Level $level FAILED - Fix errors before proceeding!"
        exit 1
    fi
}

# ========================================
# LEVEL 1: PRIMITIVE TYPES
# These MUST pass 100% before testing Level 2
# ========================================

echo "###############################################"
echo "# LEVEL 1: PRIMITIVE TYPES (Foundation)"
echo "# Test these FIRST - no Denc dependencies"
echo "###############################################"
echo ""

test_type "pg_t" 1
test_type "eversion_t" 1
test_type "utime_t" 1
test_type "uuid_d" 1
test_type "osd_info_t" 1

echo "✅ Level 1 COMPLETE - All primitive types validated!"
echo ""

# ========================================
# LEVEL 2: TYPES DEPENDING ON LEVEL 1
# Only test if Level 1 is 100% validated
# ========================================

echo "###############################################"
echo "# LEVEL 2: TYPES DEPENDING ON LEVEL 1"
echo "# These types contain Level 1 types as fields"
echo "###############################################"
echo ""

test_type "entity_addr_t" 2
test_type "pool_snap_info_t" 2
test_type "osd_xinfo_t" 2

echo "✅ Level 2 COMPLETE - All Level 2 types validated!"
echo ""

# ========================================
# LEVEL 3: COMPLEX TYPES
# Only test if Level 1 & 2 are validated
# ========================================

echo "###############################################"
echo "# LEVEL 3: COMPLEX TYPES"
echo "# These depend on Level 2 or have multiple nested deps"
echo "###############################################"
echo ""

test_type "pg_merge_meta_t" 3
test_type "pg_pool_t" 3

echo "✅ Level 3 COMPLETE - All complex types validated!"
echo ""

# ========================================
# FINAL SUMMARY
# ========================================

echo "========================================="
echo "✅ ALL LEVELS PASSED!"
echo "========================================="
echo ""
echo "All types validated with:"
echo "  ✓ Zero remaining bytes after decode"
echo "  ✓ Complete consumption of corpus files"
echo ""
echo "Next steps:"
echo "  1. Compare JSON output with C++ dencoder"
echo "  2. Test round-trip encoding (decode → encode → decode)"
echo "  3. Verify C++ can decode Rust-encoded data"
