"""Generate pinned-C Uniform, List, and TREE CRUSH placement fixtures.

Run: python3 rados/tests/crush/reference/generate-legacy-bucket-reference.py ../ceph [--check]
Requires the pinned ARM64 Docker images documented in README.md.
"""
from pathlib import Path
import hashlib
import re
import subprocess as sp
import sys
import tempfile

reference = Path(__file__).resolve().parent
ceph = Path(sys.argv[1]).resolve()
check = sys.argv[2:] == ["--check"]
releases = (
    ("quincy", "17.2.7", "b12291d110049b2f35e32e0de30d70e9a4c060d2",
     "a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada"),
    ("tentacle", "20.2.4", "7f793731f1b39eb4f465e960113d2363c311b964",
     "6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"),
)
docker = ["docker", "run", "--rm", "--network", "none", "-i", "--entrypoint"]
tree_paths = (
    "src/test/cli/crushtool/add-item-in-tree.t",
    "src/test/cli/crushtool/tree.template",
    "src/test/cli/crushtool/tree.template.final",
)


def source(revision, path):
    return sp.check_output(["git", "-C", str(ceph), "show", f"{revision}:{path}"])


def run(image, *args, input=None):
    return sp.check_output(docker + ["crushtool", image, *args], input=input)


def mapping_rows(work, image, path, algorithm, case, rule, replicas, weights):
    output = sp.check_output([
        "docker", "run", "--rm", "--network", "none", "-v", f"{work}:/work",
        "--entrypoint", "crushtool", image, "-i", f"/work/{path}", "--test", "--rule", str(rule),
        "--num-rep", str(replicas), "--min-x", "0", "--max-x", "7",
        "--show-mappings", *sum((["--weight", str(device), str(weight)] for device, weight in weights), []),
    ]).decode()
    rows = []
    for line in output.splitlines():
        match = re.search(r"x (\d+) \[([^]]*)\]$", line)
        assert match, line
        items = match.group(2).replace(" ", "").replace("2147483647", "NONE")
        items = items or ""
        rows.append(f"{algorithm}|{case}|{rule}|{match.group(1)}|{replicas}|{len(items.split(',')) if items else 0}|{items}")
    assert len(rows) == 8, output
    return rows


generated = {}
vectors = {}
for release, version, revision, digest in releases:
    image = "quay.io/ceph/ceph@sha256:" + digest
    identity = sp.check_output(docker + ["ceph", image, "--version"], text=True)
    assert f"ceph version {version} ({revision})" in identity, identity
    source_bytes = {path: source(revision, path) for path in tree_paths}
    assert source_bytes[tree_paths[0]] == source(releases[0][2], tree_paths[0])
    assert source_bytes[tree_paths[1]] == source(releases[0][2], tree_paths[1])
    assert source_bytes[tree_paths[2]] == source(releases[0][2], tree_paths[2])
    with tempfile.TemporaryDirectory(prefix=".legacy-bucket-", dir=reference) as temporary:
        work = Path(temporary)
        for path, data in source_bytes.items():
            (work / Path(path).name).write_bytes(data)
        for algorithm in ("uniform", "list"):
            compiled = run(image, "-c", "/dev/stdin", "-o", "/dev/stdout", input=(reference / f"{algorithm}-local.crush").read_bytes())
            (work / algorithm).write_bytes(compiled)
            generated[f"{algorithm}-local-{release}.crushmap"] = compiled
        sp.run([
            "docker", "run", "--rm", "--network", "none", "-v", f"{work}:/work",
            "--entrypoint", "/bin/sh", image, "-ec",
            "crushtool -i /work/tree.template --add-item 0 1.0 device0 --loc host host0 --loc cluster cluster0 -o /work/one; "
            "crushtool -i /work/one --add-item 1 1.0 device1 --loc host host0 --loc cluster cluster0 -o /work/two; "
            "crushtool -i /work/two --add-item 2 1.0 device2 --loc host host0 --loc cluster cluster0 -o /work/three; "
            "crushtool -i /work/three --add-item 3 1.0 device3 --loc host host0 --loc cluster cluster0 -o /work/four; "
            "crushtool -i /work/four --add-item 4 1.0 device4 --loc host host0 --loc cluster cluster0 -o /work/five; "
            "crushtool -i /work/five --add-item 5 1.0 device5 --loc host host0 --loc cluster cluster0 -o /work/six; "
            "crushtool -i /work/six --add-item 6 1.0 device6 --loc host host0 --loc cluster cluster0 -o /work/seven; "
            "crushtool -i /work/seven --add-item 7 1.0 device7 --loc host host0 --loc cluster cluster0 -o /work/eight; "
            "crushtool -d /work/eight -o /work/final; cmp /work/final /work/tree.template.final",
        ], check=True)
        generated[f"tree-add-item-{release}.crushmap"] = (work / "eight").read_bytes()
        rows = []
        for rule in range(3):
            rows += mapping_rows(work, image, "eight", "tree", "complete", rule, 1, [])
            rows += mapping_rows(work, image, "eight", "tree", "collision", rule, 3, [])
        for algorithm in ("uniform", "list"):
            for case, replicas, weights in (
                ("complete", 3, []),
                ("collision", 4, []),
                ("retry", 3, [(1, 0)]),
                ("hole", 3, [(1, 0), (2, 0)]),
            ):
                for rule in range(2):
                    rows += mapping_rows(work, image, algorithm, algorithm, case, rule, replicas, weights)
        vectors[release] = rows

generated["legacy-bucket-vectors.txt"] = (
    "# Generated by generate-legacy-bucket-reference.py from pinned crushtool.\n"
    "# fields: release|algorithm|case|rule|x|replicas|length|ordered-items; NONE=2147483647.\n"
    + "\n".join(f"{release}|{row}" for release, rows in vectors.items() for row in rows) + "\n"
).encode()
for filename, data in generated.items():
    path = reference / filename
    if check:
        assert path.read_bytes() == data, f"stale {path}"
    else:
        path.write_bytes(data)
    print(f"{filename}: {len(data)} bytes, SHA256 {hashlib.sha256(data).hexdigest()}")
