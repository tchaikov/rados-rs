#!/bin/bash
# Test script for RADOS operations to diagnose truncation and delete issues

set -e

POOL="testpool"
CEPH_CONF="/home/kefu/dev/ceph/build/ceph.conf"
KEYRING="/home/kefu/dev/ceph/build/keyring"
MON_HOST="v2:127.0.0.1:3300"
RADOS_BIN="./target/release/rados"
CEPH_RADOS="/home/kefu/dev/ceph/build/bin/rados"

echo "=== RADOS Object Operations Test ==="
echo

# Test object names
TESTS=(
    "test_short:ShortData"
    "test_cli_obj:12CharName"
    "test_cli_object:15CharName"
    "test_really_long_object_name_with_many_chars:VeryLongName"
)

for test in "${TESTS[@]}"; do
    OBJ_NAME="${test%%:*}"
    DATA="${test##*:}"

    echo "-------------------------------------------"
    echo "Test Object: '$OBJ_NAME' (${#OBJ_NAME} chars)"
    echo "Test Data: '$DATA'"
    echo

    # Clean up first
    echo "Cleaning up any existing object..."
    $CEPH_RADOS --conf "$CEPH_CONF" -p "$POOL" rm "$OBJ_NAME" 2>/dev/null || true

    # Write using our rados CLI (with debug output)
    echo "1. Writing with Rust rados CLI..."
    echo "$DATA" | RUST_LOG=debug $RADOS_BIN --pool "$POOL" --mon-host "$MON_HOST" \
        --keyring "$KEYRING" --name client.admin --debug put "$OBJ_NAME" - 2>&1 | \
        grep -E "(Encoding|Object|opcode)" || true
    echo

    # List objects to see what was actually created
    echo "2. Listing objects in pool (looking for pattern '$OBJ_NAME')..."
    $CEPH_RADOS --conf "$CEPH_CONF" -p "$POOL" ls | grep -E "^test_" | while read obj; do
        if [[ "$obj" == "$OBJ_NAME"* ]] || [[ "$OBJ_NAME" == "$obj"* ]]; then
            echo "   Found: '$obj' (${#obj} chars)"
            if [[ "$obj" != "$OBJ_NAME" ]]; then
                echo "   ⚠️  NAME MISMATCH! Expected '$OBJ_NAME' but got '$obj'"
                echo "   Missing chars: $((${#OBJ_NAME} - ${#obj}))"
            fi
        fi
    done
    echo

    # Stat using official client
    echo "3. Stat with official ceph rados..."
    if $CEPH_RADOS --conf "$CEPH_CONF" -p "$POOL" stat "$OBJ_NAME" 2>/dev/null; then
        echo "   ✓ Object found with exact name"
    else
        echo "   ⚠️  Object NOT found with exact name!"
        echo "   Trying to find similar objects..."
        $CEPH_RADOS --conf "$CEPH_CONF" -p "$POOL" ls | grep -E "^${OBJ_NAME:0:10}" || echo "   No similar objects found"
    fi
    echo

    # Try to read back
    echo "4. Reading back with official client..."
    READ_DATA=$($CEPH_RADOS --conf "$CEPH_CONF" -p "$POOL" get "$OBJ_NAME" - 2>/dev/null || echo "FAILED")
    if [[ "$READ_DATA" == "FAILED" ]]; then
        echo "   ⚠️  Failed to read object"
    else
        echo "   Read data: '$READ_DATA'"
        if [[ "$READ_DATA" != "$DATA" ]]; then
            echo "   ⚠️  DATA MISMATCH!"
        else
            echo "   ✓ Data matches"
        fi
    fi
    echo

    # Test delete with our CLI
    echo "5. Deleting with Rust rados CLI..."
    RUST_LOG=debug $RADOS_BIN --pool "$POOL" --mon-host "$MON_HOST" \
        --keyring "$KEYRING" --name client.admin --debug rm "$OBJ_NAME" 2>&1 | \
        grep -E "(Encoding|Object|opcode|Delete)" || true
    echo

    # Verify deletion
    echo "6. Verifying deletion..."
    sleep 0.5
    if $CEPH_RADOS --conf "$CEPH_CONF" -p "$POOL" stat "$OBJ_NAME" 2>/dev/null; then
        echo "   ⚠️  OBJECT STILL EXISTS AFTER DELETE!"
    else
        echo "   ✓ Object successfully deleted"
    fi
    echo
done

echo "=== Test Complete ==="
