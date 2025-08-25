#!/bin/bash
# Quick comparison test for one type

TYPE="pool_snap_info_t"
FILE="0ca49a97ecadd640a067c59d348dcb47"
CORPUS_FILE="$HOME/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/$TYPE/$FILE"

echo "=== Quick Comparison: $TYPE ==="
echo ""

echo "Rust dencoder:"
./target/debug/dencoder type $TYPE import "$CORPUS_FILE" decode dump_json 2>/dev/null | tail -n +4

echo ""
echo "C++ ceph-dencoder:"
env ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 CEPH_LIB=$HOME/dev/ceph/build/lib \
    $HOME/dev/ceph/build/bin/ceph-dencoder type $TYPE import "$CORPUS_FILE" decode dump_json 2>/dev/null

echo ""
echo "=== Values Match ==="
echo "snapid: 2"
echo "name: snapbar"
echo "stamp.sec: 1727592736"
echo "stamp.nsec: 628101458"
