#!/usr/bin/env bash
# Test-only, bounded MON/three-OSD topology for generate-mon-classes-reference.py.
set -euo pipefail
umask 077
live_dir=$(mktemp -d /tmp/rados-mon-classes.XXXXXX)
osds=${CRUSH_PREFLIGHT_OSDS:-3}
create_rbd=${CRUSH_PREFLIGHT_RBD:-1}
test "$osds" -ge 1 && test "$osds" -le 3
trap 'status=$?; for pid in $(jobs -pr); do kill "$pid" 2>/dev/null || true; done; wait || true; rm -rf "$live_dir"; exit "$status"' EXIT
export CEPH_CONF="$live_dir/ceph.conf"
cat > "$CEPH_CONF" <<EOF
[global]
fsid = $(uuidgen)
mon host = [v2:127.0.0.1:7131,v1:127.0.0.1:7130]
auth cluster required = none
auth service required = none
auth client required = none
run dir = $live_dir
admin socket = $live_dir/\$name.asok
log file = $live_dir/\$name.log
osd pool default size = 3
osd pool default min size = 1
osd crush chooseleaf type = 0
osd class update on start = false
osd pool default pg autoscale mode = off
mon allow pool delete = true
mon allow pool size one = true
mon data avail crit = 1
mon data avail warn = 2
paxos propose interval = 0.1
[mon.a]
mon data = $live_dir/mon
[osd]
public addr = 127.0.0.1
osd objectstore = memstore
osd data = $live_dir/osd.\$id
osd numa node = -1
enable experimental unrecoverable data corrupting features = memstore
EOF
mkdir "$live_dir/mon"
ceph-mon -i a --mkfs
ceph-mon -i a -f > "$live_dir/mon.stdout.log" 2>&1 &
for attempt in $(seq 1 30); do
  if timeout 3 ceph mon stat >/dev/null 2>&1; then break; fi
  [ "$attempt" -ne 30 ]
  sleep 1
done
for id in $(seq 0 $((osds - 1))); do
  mkdir "$live_dir/osd.$id"
  osd_uuid=$(uuidgen)
  test "$(ceph osd new "$osd_uuid")" = "$id"
  ceph-osd -i "$id" --mkfs --osd-uuid "$osd_uuid"
  ceph-osd -i "$id" -f > "$live_dir/osd.$id.stdout.log" 2>&1 &
done
for attempt in $(seq 1 60); do
  if ceph osd dump -f json 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); assert len(d["osds"]) == int(sys.argv[1]) and all(x["up"] and x["in"] for x in d["osds"])' "$osds" 2>/dev/null; then break; fi
  [ "$attempt" -ne 60 ]
  sleep 1
done
if test "$create_rbd" = 1; then
  ceph osd pool delete rbd rbd --yes-i-really-really-mean-it
  ceph osd pool create rbd 4
  rbd pool init rbd
  ceph osd map rbd SOMETHING -f json
  printf 'ABCDEF\n' > "$live_dir/payload"
  timeout 60 rados --pool rbd put SOMETHING "$live_dir/payload"
fi
echo "LIVE PREFLIGHT PASS: one MON, $osds OSDs"
sleep "${CRUSH_PREFLIGHT_HOLD:-0}"
