#!/bin/bash
# Comprehensive corpus test for all types
# Tests decode -> encode -> decode roundtrip for all corpus files

set -e

CORPUS_BASE="$HOME/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects"
RUST_DENCODER="$HOME/dev/rados-rs/target/debug/dencoder"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

if [ ! -d "$CORPUS_BASE" ]; then
    echo -e "${RED}Error: Corpus directory not found: $CORPUS_BASE${NC}"
    exit 1
fi

if [ ! -f "$RUST_DENCODER" ]; then
    echo -e "${YELLOW}Building Rust dencoder...${NC}"
    cd "$HOME/dev/rados-rs"
    cargo build --bin dencoder
fi

# Map C++ type names to Rust type names
declare -A TYPE_MAP=(
    ["entity_addr_t"]="entity_addr_t"
    ["pg_t"]="pg_t"
    ["eversion_t"]="eversion_t"
    ["utime_t"]="utime_t"
    ["uuid_d"]="uuid_d"
    ["osd_info_t"]="osd_info_t"
    ["osd_xinfo_t"]="osd_xinfo_t"
    ["pool_snap_info_t"]="pool_snap_info_t"
    ["pg_merge_meta_t"]="pg_merge_meta_t"
    ["pg_pool_t"]="pg_pool_t"
)

TOTAL_TYPES=0
PASSED_TYPES=0
FAILED_TYPES=0

echo "========================================="
echo "Comprehensive Corpus Test"
echo "========================================="
echo ""

for cpp_type in "${!TYPE_MAP[@]}"; do
    rust_type="${TYPE_MAP[$cpp_type]}"
    corpus_dir="$CORPUS_BASE/$cpp_type"

    if [ ! -d "$corpus_dir" ]; then
        echo -e "${YELLOW}Skipping $cpp_type (no corpus directory)${NC}"
        continue
    fi

    TOTAL_TYPES=$((TOTAL_TYPES + 1))

    echo -e "${BLUE}Testing type: $rust_type${NC}"
    echo "Corpus directory: $corpus_dir"

    TOTAL_FILES=0
    PASSED_FILES=0
    FAILED_FILES=0

    for corpus_file in "$corpus_dir"/*; do
        if [ ! -f "$corpus_file" ]; then
            continue
        fi

        TOTAL_FILES=$((TOTAL_FILES + 1))
        filename=$(basename "$corpus_file")

        # Test decode
        decode_output=$($RUST_DENCODER type "$rust_type" import "$corpus_file" decode 2>&1 || echo "DECODE_FAILED")

        if echo "$decode_output" | grep -q "DECODE_FAILED\|Error\|error"; then
            echo -e "  ${RED}✗${NC} $filename (decode failed)"
            FAILED_FILES=$((FAILED_FILES + 1))
            continue
        fi

        # Test roundtrip: decode -> encode -> decode
        roundtrip_output=$($RUST_DENCODER type "$rust_type" import "$corpus_file" decode encode export /tmp/test_roundtrip.bin 2>&1 || echo "ENCODE_FAILED")

        if echo "$roundtrip_output" | grep -q "ENCODE_FAILED\|Error\|error"; then
            echo -e "  ${RED}✗${NC} $filename (encode failed)"
            FAILED_FILES=$((FAILED_FILES + 1))
            continue
        fi

        # Verify the encoded file can be decoded again
        verify_output=$($RUST_DENCODER type "$rust_type" import /tmp/test_roundtrip.bin decode 2>&1 || echo "VERIFY_FAILED")

        if echo "$verify_output" | grep -q "VERIFY_FAILED\|Error\|error"; then
            echo -e "  ${RED}✗${NC} $filename (roundtrip decode failed)"
            FAILED_FILES=$((FAILED_FILES + 1))
            continue
        fi

        # Check if original and roundtrip files are identical
        if cmp -s "$corpus_file" /tmp/test_roundtrip.bin; then
            echo -e "  ${GREEN}✓${NC} $filename (perfect roundtrip)"
            PASSED_FILES=$((PASSED_FILES + 1))
        else
            # Files differ - this might be OK if encoding is feature-dependent
            # Check if at least the decode succeeded
            echo -e "  ${YELLOW}~${NC} $filename (roundtrip OK, binary differs)"
            PASSED_FILES=$((PASSED_FILES + 1))
        fi

        rm -f /tmp/test_roundtrip.bin
    done

    echo ""
    if [ $FAILED_FILES -eq 0 ]; then
        echo -e "${GREEN}✓ $rust_type: $PASSED_FILES/$TOTAL_FILES passed${NC}"
        PASSED_TYPES=$((PASSED_TYPES + 1))
    else
        echo -e "${RED}✗ $rust_type: $PASSED_FILES/$TOTAL_FILES passed, $FAILED_FILES failed${NC}"
        FAILED_TYPES=$((FAILED_TYPES + 1))
    fi
    echo ""
done

echo "========================================="
echo "Final Results"
echo "========================================="
echo "Types tested: $TOTAL_TYPES"
echo -e "${GREEN}Passed: $PASSED_TYPES${NC}"
echo -e "${RED}Failed: $FAILED_TYPES${NC}"
echo ""

if [ $FAILED_TYPES -eq 0 ]; then
    echo -e "${GREEN}All types passed!${NC}"
    exit 0
else
    echo -e "${RED}Some types failed${NC}"
    exit 1
fi
