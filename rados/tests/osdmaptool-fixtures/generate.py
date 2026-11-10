#!/usr/bin/env python3
"""Capture OSDMap tool fixtures from the pinned Quincy and Tentacle images.

Run: python3 rados/tests/osdmaptool-fixtures/generate.py ../ceph [--check]
"""

from pathlib import Path
import hashlib
import re
import subprocess as sp
import sys
import tempfile


root = Path(__file__).resolve().parent
ceph = Path(sys.argv[1]).resolve()
check = sys.argv[2:] == ["--check"]
if len(sys.argv) != 2 + check:
    raise SystemExit(__doc__)

releases = (
    ("quincy", "17.2.7", "b12291d110049b2f35e32e0de30d70e9a4c060d2",
     "a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada"),
    ("tentacle", "20.2.4", "7f793731f1b39eb4f465e960113d2363c311b964",
     "6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"),
)
paths = (
    "src/test/cli/osdmaptool/ceph.conf.withracks",
    "src/test/cli/osdmaptool/create-racks.t",
    "src/test/cli/osdmaptool/test-map-pgs.t",
    "src/test/cli/osdmaptool/crush.t",
    "src/test/cli/osdmaptool/create-print.t",
)


def source(revision, path):
    return sp.check_output(["git", "-C", str(ceph), "show", f"{revision}:{path}"])


def image(digest):
    return "quay.io/ceph/ceph@sha256:" + digest


def capture(container, directory):
    command = """
set -e
osdmaptool --create-from-conf /work/create-racks.osdmap -c /work/ceph.conf.withracks --with-default-pool >/dev/null
osdmaptool --test-map-pg 0.0 /work/create-racks.osdmap >/work/create-racks-pg0.txt
osdmaptool --createsimple 3 /work/create-print.osdmap --with-default-pool >/dev/null
osdmaptool --createsimple 3 /work/crush.osdmap --with-default-pool >/dev/null
osdmaptool --export-crush /work/crush.bin /work/crush.osdmap >/dev/null
osdmaptool --import-crush /work/crush.bin /work/crush.osdmap >/dev/null
osdmaptool --osd_pool_default_size 3 --pg_bits 4 --createsimple 500 /work/test-map-pgs.osdmap --with-default-pool >/dev/null
crushtool --outfn /work/test-map-pgs.crush --build --num_osds 500 node straw 10 rack straw 10 root straw 0 >/dev/null
osdmaptool --import-crush /work/test-map-pgs.crush /work/test-map-pgs.osdmap >/dev/null
osdmaptool --mark-up-in --test-map-pgs /work/test-map-pgs.osdmap >/work/test-map-pgs-results.txt
"""
    sp.run([
        "docker", "run", "--rm", "--network", "none", "-v", f"{directory}:/work",
        "-w", "/work", "--entrypoint", "/bin/bash", container, "-ec", command,
    ], check=True)


def fixture_hashes():
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (
            sorted(root.glob("*.osdmap"))
            + sorted(root.glob("*.txt"))
            + [root / "ceph.conf.withracks"]
        )
    }


def validate_output(directory, release):
    pg0 = (directory / "create-racks-pg0.txt").read_text()
    assert "0.0 raw ([], p-1) up ([], p-1) acting ([], p-1)" in pg0, pg0
    workload = (directory / "test-map-pgs-results.txt").read_text()
    assert "pool 1 pg_num 8000" in workload, workload
    assert re.search(r"size 3\s+8000", workload), workload
    if check:
        expected = (root / f"test-map-pgs-results-{release}.txt").read_text()
        assert workload == expected, f"{release} test-map-pgs result differs from fixture"


inputs = {release: {path: source(revision, path) for path in paths}
          for release, _, revision, _ in releases}
assert inputs["quincy"][paths[0]] == inputs["tentacle"][paths[0]]

for release, version, revision, digest in releases:
    identity = sp.check_output(["docker", "run", "--rm", "--network", "none", image(digest), "ceph", "--version"], text=True)
    assert f"ceph version {version} ({revision})" in identity, identity
    with tempfile.TemporaryDirectory(prefix=f".osdmaptool-{release}-", dir=root) as temporary:
        temporary = Path(temporary)
        (temporary / "ceph.conf.withracks").write_bytes(inputs[release][paths[0]])
        capture(image(digest), temporary)
        validate_output(temporary, release)
        if not check:
            for name in (
                "create-racks.osdmap", "create-print.osdmap", "crush.osdmap",
                "test-map-pgs.osdmap", "create-racks-pg0.txt", "test-map-pgs-results.txt",
            ):
                (root / f"{name.removesuffix('.osdmap')}-{release}.osdmap" if name.endswith(".osdmap")
                 else root / f"{name.removesuffix('.txt')}-{release}.txt").write_bytes((temporary / name).read_bytes())
            if release == "quincy":
                (root / "ceph.conf.withracks").write_bytes(inputs[release][paths[0]].rstrip(b"\n") + b"\n")

if check:
    checksums = dict(
        line.split(" ", 1) for line in (root / "SHA256SUMS").read_text().splitlines()
        if line and not line.startswith("#")
    )
    expected = {name: digest for name, digest in checksums.items() if not name.startswith("source/")}
    expected_source = {name.removeprefix("source/"): digest for name, digest in checksums.items()
                       if name.startswith("source/")}
    actual_source = {
        f"{release}/{Path(path).name}": hashlib.sha256(data).hexdigest()
        for release, files in inputs.items()
        for path, data in files.items()
    }
    actual = fixture_hashes()
    assert expected_source == actual_source, (expected_source, actual_source)
    assert expected == actual, (expected, actual)
else:
    source_hashes = {
        f"{release}/{Path(path).name}": hashlib.sha256(data).hexdigest()
        for release, files in inputs.items()
        for path, data in files.items()
    }
    lines = ["# Fixture SHA-256 values generated by generate.py."]
    lines += [f"source/{name} {digest}" for name, digest in sorted(source_hashes.items())]
    lines += [f"{name} {digest}" for name, digest in fixture_hashes().items()]
    (root / "SHA256SUMS").write_text("\n".join(lines) + "\n")
