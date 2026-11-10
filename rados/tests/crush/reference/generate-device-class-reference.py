"""Generate and verify pinned device-class CRUSH fixtures.

Run: python3 rados/tests/crush/reference/generate-device-class-reference.py ../ceph [--check]
Requires the pinned ARM64 Docker images documented in README.md.
"""
from pathlib import Path
import hashlib
import subprocess as sp
import sys
import tempfile

reference = Path(__file__).resolve().parent
fixtures = reference.parent / "fixtures"
ceph = Path(sys.argv[1]).resolve()
check = sys.argv[2:] == ["--check"]
releases = (
    ("quincy", "17.2.7", "b12291d110049b2f35e32e0de30d70e9a4c060d2",
     "a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada"),
    ("tentacle", "20.2.4", "7f793731f1b39eb4f465e960113d2363c311b964",
     "6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"),
)
docker = ["docker", "run", "--rm", "--network", "none", "-i", "--entrypoint"]
source = fixtures / "device-class.crush"
transcript = fixtures / "device-class.t"
outputs = {}
vectors = {}
for release, version, revision, digest in releases:
    image = "quay.io/ceph/ceph@sha256:" + digest
    assert source.read_bytes() == sp.check_output([
        "git", "-C", str(ceph), "show", f"{revision}:src/test/cli/crushtool/device-class.crush"])
    assert transcript.read_bytes() == sp.check_output([
        "git", "-C", str(ceph), "show", f"{revision}:src/test/cli/crushtool/device-class.t"])
    identity = sp.check_output(docker + ["ceph", image, "--version"], text=True)
    assert f"ceph version {version} ({revision})" in identity, identity
    compiled = sp.check_output(docker + ["crushtool", image, "-c", "/dev/stdin", "-o", "/dev/stdout"], input=source.read_bytes())
    with tempfile.TemporaryDirectory(prefix=".device-class-", dir=reference) as temporary:
        work = Path(temporary)
        (work / "compiled").write_bytes(compiled)
        sp.run([
            "docker", "run", "--rm", "--network", "none", "-v", f"{work}:/work",
            "--entrypoint", "/bin/sh", image, "-ec",
            "crushtool -d /work/compiled -o /work/decompiled; "
            "crushtool -c /work/decompiled -o /work/recompiled; "
            "cmp /work/compiled /work/recompiled",
        ], check=True)
        assert (work / "decompiled").read_bytes() == source.read_bytes(), release
        vectors[release] = b"".join(sp.check_output([
            "docker", "run", "--rm", "--network", "none", "-v", f"{work}:/work",
            "--entrypoint", "crushtool", image, "-i", "/work/compiled", "--test",
            "--rule", str(rule), "--num-rep", "3", "--min-x", "0", "--max-x", "19",
            "--show-mappings",
        ]) for rule in (1, 2))
    outputs[release] = compiled

assert vectors["quincy"] == vectors["tentacle"]
generated = {f"device-class-{release}.crushmap": data for release, data in outputs.items()}
generated["device-class-vectors.txt"] = vectors["quincy"]
for filename, data in generated.items():
    path = reference / filename
    if check:
        assert path.read_bytes() == data, f"stale {path}"
    else:
        path.write_bytes(data)
    print(f"{filename}: {len(data)} bytes, SHA256 {hashlib.sha256(data).hexdigest()}")
