#!/bin/bash
# Test EntityAddr DencMut implementation against C++ ceph-dencoder using corpus data

set -e

CORPUS_DIR="$HOME/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/entity_addr_t"
CEPH_DENCODER="$HOME/dev/ceph/build/bin/ceph-dencoder"
RUST_DENCODER="$HOME/dev/rados-rs/target/debug/dencoder"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

if [ ! -d "$CORPUS_DIR" ]; then
    echo -e "${RED}Error: Corpus directory not found: $CORPUS_DIR${NC}"
    exit 1
fi

if [ ! -f "$CEPH_DENCODER" ]; then
    echo -e "${RED}Error: C++ ceph-dencoder not found: $CEPH_DENCODER${NC}"
    exit 1
fi

if [ ! -f "$RUST_DENCODER" ]; then
    echo -e "${YELLOW}Building Rust dencoder...${NC}"
    cd "$HOME/dev/rados-rs"
    cargo build --bin dencoder
fi

echo "Testing EntityAddr with corpus data..."
echo "Corpus directory: $CORPUS_DIR"
echo ""

TOTAL=0
PASSED=0
FAILED=0

for corpus_file in "$CORPUS_DIR"/*; do
    if [ ! -f "$corpus_file" ]; then
        continue
    fi

    TOTAL=$((TOTAL + 1))
    filename=$(basename "$corpus_file")

    echo -n "Testing $filename... "

    # Get C++ output
    cpp_output=$($CEPH_DENCODER type entity_addr_t import "$corpus_file" decode dump_json 2>&1 || echo "DECODE_FAILED")

    # Get Rust output
    rust_output=$($RUST_DENCODER type entity_addr_t import "$corpus_file" decode dump_json 2>&1 || echo "DECODE_FAILED")

    # Compare outputs
    if [ "$cpp_output" = "DECODE_FAILED" ] && [ "$rust_output" = "DECODE_FAILED" ]; then
        echo -e "${YELLOW}SKIP (both failed to decode)${NC}"
        continue
    elif [ "$cpp_output" = "DECODE_FAILED" ]; then
        echo -e "${RED}FAIL (C++ failed, Rust succeeded)${NC}"
        FAILED=$((FAILED + 1))
        continue
    elif [ "$rust_output" = "DECODE_FAILED" ]; then
        echo -e "${RED}FAIL (Rust failed, C++ succeeded)${NC}"
        FAILED=$((FAILED + 1))
        echo "C++ output:"
        echo "$cpp_output"
        continue
    fi

    # Both succeeded, compare JSON
    if [ "$cpp_output" = "$rust_output" ]; then
        echo -e "${GREEN}PASS${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}FAIL (output mismatch)${NC}"
        FAILED=$((FAILED + 1))
        echo "C++ output:"
        echo "$cpp_output"
        echo ""
        echo "Rust output:"
        echo "$rust_output"
        echo ""
    fi
done

echo ""
echo "========================================="
echo "Results: $PASSED/$TOTAL passed, $FAILED failed"
echo "========================================="

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed${NC}"
    exit 1
fi
