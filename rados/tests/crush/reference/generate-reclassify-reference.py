"""Capture pinned C reclassification maps and ordered mapping digests.

Run: python3 rados/tests/crush/reference/generate-reclassify-reference.py ../ceph [--check]
Requires the pinned ARM64 Docker images documented in README.md.
"""
from pathlib import Path
import hashlib
import struct
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
cases = (
    ("a", ("--set-subtree-class", "default", "hdd", "--reclassify", "--reclassify-bucket", "%-ssd", "ssd", "default", "--reclassify-bucket", "ssd", "ssd", "default", "--reclassify-root", "default", "hdd"), (0, 1), (0, 0)),
    ("d", ("--set-subtree-class", "default", "hdd", "--reclassify", "--reclassify-bucket", "%-ssd", "ssd", "default", "--reclassify-bucket", "ssd", "ssd", "default", "--reclassify-root", "default", "hdd"), (0, 1), (0, 0)),
    ("e", ("--reclassify", "--reclassify-bucket", "ceph-osd-ssd-%", "ssd", "default", "--reclassify-bucket", "ssd-root", "ssd", "default", "--reclassify-root", "default", "hdd"), (0, 1), (6540, 8417)),
    ("c", ("--reclassify", "--reclassify-bucket", "%-SSD", "ssd", "default", "--reclassify-bucket", "ssd", "ssd", "default", "--reclassify-root", "default", "hdd"), (0, 1, 2), (158, 138, 0)),
    ("beesly", ("--set-subtree-class", "0513-R-0060", "hdd", "--set-subtree-class", "0513-R-0050", "hdd", "--reclassify", "--reclassify-root", "0513-R-0050", "hdd", "--reclassify-root", "0513-R-0060", "hdd"), (0, 1, 2, 4), (0, 0, 0, 0)),
    ("flax", ("--reclassify", "--reclassify-root", "default", "hdd"), (0,), (0,)),
    ("gabe2", ("--reclassify", "--reclassify-root", "default", "hdd"), (0, 1), (627, 652)),
    ("b", ("--reclassify", "--reclassify-bucket", "%-hdd", "hdd", "default", "--reclassify-bucket", "%-ssd", "ssd", "default", "--reclassify-bucket", "ssd", "ssd", "default", "--reclassify-bucket", "hdd", "hdd", "default"), (0, 1), (0, 0)),
    ("f", ("--reclassify", "--reclassify-root", "default", "hdd"), (0, 1), (627, 652)),
    ("g", ("--reclassify", "--reclassify-bucket", "sata-%", "hdd-sata", "default", "--reclassify-bucket", "sas-%", "hdd-sas", "default", "--reclassify-bucket", "sas", "hdd-sas", "default", "--reclassify-bucket", "sata", "hdd-sata", "default"), (0, 1), (0, 0)),
)
docker = ["docker", "run", "--rm", "--network", "none", "-i", "--entrypoint"]


def run(image, *args, input=None):
    return sp.check_output(docker + ["crushtool", image, *args], input=input)


def reclassify(image, arguments, original):
    with tempfile.TemporaryDirectory(prefix=".reclassify-", dir=reference) as temporary:
        work = Path(temporary)
        (work / "input").write_bytes(original)
        sp.run([
            "docker", "run", "--rm", "--network", "none", "-v", f"{work}:/work",
            "--entrypoint", "crushtool", image, "-i", "/work/input", *arguments,
            "-o", "/work/output",
        ], check=True, stdout=sp.DEVNULL)
        return (work / "output").read_bytes()


def vectors(image, data, rule, replica):
    output = run(image, "-i", "-", "--test", "--rule", str(rule), "--num-rep", str(replica),
                 "--min-x", "0", "--max-x", "1023", "--show-mappings", input=data).decode().splitlines()
    assert len(output) == 1024, (rule, replica, len(output))
    rows = []
    for x, line in enumerate(output):
        prefix = f"CRUSH rule {rule} x {x} "
        assert line.startswith(prefix) and line.endswith("]"), line
        items = line[len(prefix):].strip("[]").split(",")
        rows.append([int(item) for item in items if item])
    return rows


def digest(rows):
    value = hashlib.sha256()
    for row in rows:
        value.update(struct.pack(">I", len(row)))
        for item in row:
            value.update(struct.pack(">i", item))
    return value.hexdigest()


generated = {}
generated_fixture = {}
manifest = []
for release, version, revision, image_digest in releases:
    image = "quay.io/ceph/ceph@sha256:" + image_digest
    identity = sp.check_output(docker + ["ceph", image, "--version"], text=True)
    assert f"ceph version {version} ({revision})" in identity, identity
    transcript = sp.check_output(["git", "-C", str(ceph), "show", f"{revision}:src/test/cli/crushtool/reclassify.t"])
    previous = generated_fixture.setdefault("reclassify.t", transcript)
    assert previous == transcript, (release, "reclassify.t")
    for name, arguments, rules, expected_mismatches in cases:
        original = sp.check_output(["git", "-C", str(ceph), "show", f"{revision}:src/test/cli/crushtool/crush-classes/{name}"])
        previous = generated_fixture.setdefault(f"reclassify/{name}", original)
        assert previous == original, (release, name)
        after = reclassify(image, arguments, original)
        state_rows = {}
        for state, data in (("before", original), ("after", after)):
            filename = f"reclassify-{name}-{state}-{release}.crushmap"
            generated[filename] = data
            for rule in rules:
                all_rows = []
                for replica in range(1, 11):
                    rows = vectors(image, data, rule, replica)
                    all_rows.extend(rows)
                    manifest.append(f"{release} {name} {state} {rule} {replica} {len(rows)} {digest(rows)}")
                assert len(all_rows) == 10240
                state_rows[state, rule] = all_rows
        for rule, expected in zip(rules, expected_mismatches):
            before = state_rows["before", rule]
            after_rows = state_rows["after", rule]
            assert len(before) == len(after_rows) == 10240
            assert sum(left != right for left, right in zip(before, after_rows)) == expected, (release, name, rule)

for filename, data in generated_fixture.items():
    path = fixtures / filename
    if check:
        assert path.read_bytes() == data, f"stale {path}"
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    print(f"fixtures/{filename}: {len(data)} bytes SHA256 {hashlib.sha256(data).hexdigest()}")

for filename, data in generated.items():
    path = reference / filename
    if check:
        assert path.read_bytes() == data, f"stale {path}"
    else:
        path.write_bytes(data)
    print(f"{filename}: {len(data)} bytes SHA256 {hashlib.sha256(data).hexdigest()}")
manifest_path = reference / "reclassify-vectors.txt"
manifest_data = ("\n".join(manifest) + "\n").encode()
if check:
    assert manifest_path.read_bytes() == manifest_data, f"stale {manifest_path}"
else:
    manifest_path.write_bytes(manifest_data)
print(f"reclassify-vectors.txt: {len(manifest)} digests SHA256 {hashlib.sha256(manifest_data).hexdigest()}")
