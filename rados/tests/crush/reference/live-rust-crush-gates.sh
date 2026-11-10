#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  quincy)
    release=quincy
    version=17.2.7
    revision=b12291d110049b2f35e32e0de30d70e9a4c060d2
    image='quay.io/ceph/ceph@sha256:a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada'
    ;;
  tentacle)
    release=tentacle
    version=20.2.4
    revision=7f793731f1b39eb4f465e960113d2363c311b964
    image='quay.io/ceph/ceph@sha256:6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa'
    ;;
  *)
    echo "usage: $0 {quincy|tentacle} [all|class-pool|reweight|mon-classes|erasure]" >&2
    exit 2
    ;;
esac

root=$(git rev-parse --show-toplevel)
preflight="$root/rados/tests/crush/reference/live-mon-classes-preflight.sh"
command -v docker >/dev/null || { echo 'missing prerequisite: docker' >&2; exit 1; }
test -f "$preflight" || { echo "missing prerequisite: $preflight" >&2; exit 1; }
docker image inspect "$image" >/dev/null || { echo "missing prerequisite: $image" >&2; exit 1; }
identity=$(docker run --rm --network none --entrypoint ceph "$image" --version)
case "$identity" in
  *"ceph version $version ($revision)"*) ;;
  *) echo "pinned Ceph identity mismatch: $identity" >&2; exit 1 ;;
esac
echo "LIVE CRUSH RUNNER release=$release version=$version revision=$revision image=$image"
echo "LIVE CRUSH PIN PASS release=$release identity=$identity"

name=''
pid=''
work=''
gate=''
cleanup() {
  local completed_name=$name completed_gate=$gate
  test -z "$name" || docker stop --timeout 5 "$name" >/dev/null 2>&1 || true
  test -z "$pid" || wait "$pid" 2>/dev/null || true
  test -z "$work" || rm -rf "$work"
  if test -n "$completed_name"; then
    test -z "$(docker ps -aq --filter "name=^/${completed_name}$")" || {
      echo "LIVE CRUSH CLEANUP FAIL release=$release gate=$completed_gate container=$completed_name" >&2
      return 1
    }
    echo "LIVE CRUSH CLEANUP PASS release=$release gate=$completed_gate container=$completed_name"
  fi
  name=''
  pid=''
  work=''
  gate=''
}
trap cleanup EXIT

run_gate() {
  local test_name=$1 osds=$2 create_rbd=$3 config
  gate=$test_name
  name="rados-live-crush-${release}-${test_name}-$$"
  work=$(mktemp -d "${TMPDIR:-/tmp}/rados-live-crush.XXXXXX")
  echo "LIVE CRUSH GATE START release=$release gate=$gate osds=$osds rbd=$create_rbd container=$name"

  cat > "$work/ceph.conf" <<'EOF'
[global]
mon host = [v2:127.0.0.1:7131,v1:127.0.0.1:7130]
auth cluster required = none
auth service required = none
auth client required = none
EOF

  docker run --rm --name "$name" --network host --user ceph -e CRUSH_PREFLIGHT_HOLD=300 \
  -e CRUSH_PREFLIGHT_OSDS="$osds" -e CRUSH_PREFLIGHT_RBD="$create_rbd" \
  -v "$preflight:/preflight.sh:ro" "$image" bash /preflight.sh >"$work/cluster.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 150); do
    grep -q 'LIVE PREFLIGHT PASS' "$work/cluster.log" && break
    kill -0 "$pid" 2>/dev/null || { cat "$work/cluster.log" >&2; cleanup; return 1; }
    sleep 1
  done
  grep -q 'LIVE PREFLIGHT PASS' "$work/cluster.log" || { cat "$work/cluster.log" >&2; cleanup; return 1; }
  echo "LIVE CRUSH GATE READY release=$release gate=$gate"
  config=$(docker exec --user ceph "$name" sh -c 'find /tmp -path "/tmp/rados-mon-classes.*/ceph.conf" -print -quit')
  test -n "$config" || { echo 'missing fixture ceph.conf' >&2; cat "$work/cluster.log" >&2; cleanup; return 1; }

  if ! CEPH_CONF="$work/ceph.conf" CEPH_LIVE_CONTAINER="$name" CEPH_LIVE_CONFIG="$config" \
    cargo test -p rados --test osdclient_live_crush_gates --offline -- \
    --ignored --exact "$test_name" --test-threads=1 --nocapture; then
    cleanup
    return 1
  fi
  echo "LIVE CRUSH GATE PASS release=$release gate=$gate"
  cleanup
}

case "${2:-all}" in
  all)
    run_gate test_original_crush_class_and_pool_rule_gates 3 1
    run_gate test_reweight_vs_classes_consumes_live_pre_and_post_maps 3 0
    run_gate test_mon_classes_writes_before_lifecycle_mutations 3 1
    run_gate test_crush_rule_create_erasure_consumes_default_and_explicit_rules 1 0
    ;;
  class-pool) run_gate test_original_crush_class_and_pool_rule_gates 3 1 ;;
  reweight) run_gate test_reweight_vs_classes_consumes_live_pre_and_post_maps 3 0 ;;
  mon-classes) run_gate test_mon_classes_writes_before_lifecycle_mutations 3 1 ;;
  erasure) run_gate test_crush_rule_create_erasure_consumes_default_and_explicit_rules 1 0 ;;
  *)
    echo "usage: $0 {quincy|tentacle} [all|class-pool|reweight|mon-classes|erasure]" >&2
    exit 2
    ;;
esac
