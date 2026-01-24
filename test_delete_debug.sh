#!/bin/bash
set -e

POOL="testpool"
MON_HOST="v2:192.168.1.37:40511"
KEYRING="$HOME/dev/ceph/build/keyring"
RADOS_BIN="$HOME/dev/rados-rs/target/release/rados"
CEPH_RADOS="$HOME/dev/ceph/build/bin/rados"
CEPH_CONF="$HOME/dev/ceph/build/ceph.conf"

echo "=== Testing Delete Operation ==="
echo

# Create test object with official client
echo "1. Creating test object with official Ceph client..."
echo "test data" | $CEPH_RADOS -c $CEPH_CONF -p $POOL put test_delete_obj -
$CEPH_RADOS -c $CEPH_CONF -p $POOL stat test_delete_obj
echo

# Try to delete with our client
echo "2. Deleting with our Rust client..."
$RADOS_BIN -p $POOL --mon-host "$MON_HOST" --keyring "$KEYRING" --name client.admin rm test_delete_obj
echo

# Check if object still exists
echo "3. Checking if object still exists..."
if $CEPH_RADOS -c $CEPH_CONF -p $POOL stat test_delete_obj 2>&1 | grep -q "No such file"; then
    echo "✓ Object was successfully deleted"
else
    echo "✗ Object still exists after delete!"
    $CEPH_RADOS -c $CEPH_CONF -p $POOL stat test_delete_obj
fi
echo

# Create another object and delete with official client for comparison
echo "4. Creating another object and deleting with official client..."
echo "test data 2" | $CEPH_RADOS -c $CEPH_CONF -p $POOL put test_delete_obj2 -
$CEPH_RADOS -c $CEPH_CONF -p $POOL rm test_delete_obj2
if $CEPH_RADOS -c $CEPH_CONF -p $POOL stat test_delete_obj2 2>&1 | grep -q "No such file"; then
    echo "✓ Official client delete works correctly"
else
    echo "✗ Official client delete also failed (unexpected)"
fi
