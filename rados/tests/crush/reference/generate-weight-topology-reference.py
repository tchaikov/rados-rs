"""Capture retained CRUSH weight/topology consumers from both pinned Ceph releases.

Run: python3 rados/tests/crush/reference/generate-weight-topology-reference.py ../ceph [--check]
Requires the pinned ARM64 Docker images documented in README.md.
"""
from pathlib import Path
import hashlib
import subprocess as sp
import sys
import tempfile
import time

reference = Path(__file__).resolve().parent
fixtures = reference.parent / "fixtures"
ceph = Path(sys.argv[1]).resolve()
check = sys.argv[2:] == ["--check"]
preflight = reference / "live-mon-classes-preflight.sh"
releases = (
    ("quincy", "17.2.7", "b12291d110049b2f35e32e0de30d70e9a4c060d2",
     "a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada"),
    ("tentacle", "20.2.4", "7f793731f1b39eb4f465e960113d2363c311b964",
     "6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"),
)
docker = ["docker", "run", "--rm", "--network", "none", "-i", "--entrypoint"]
upstream = (
    "src/test/cli/crushtool/reweight.t",
    "src/test/cli/crushtool/reweight_multiple.t",
    "src/test/cli/crushtool/multitype.before",
    "src/test/cli/crushtool/multitype.after",
    "src/test/cli/crushtool/simple.template.multitree",
    "src/test/cli/crushtool/simple.template.multitree.reweighted",
)


def run(image, *args, input=None):
    return sp.check_output(docker + ["crushtool", image, *args], input=input)


def source(revision, path):
    return sp.check_output(["git", "-C", str(ceph), "show", f"{revision}:{path}"])


def compile_map(image, text):
    return run(image, "-c", "/dev/stdin", "-o", "/dev/stdout", input=text.encode())


def topology(weights):
    devices = "\n".join(f"device {index} osd{index}" for index in range(len(weights)))
    hosts = "\n".join(
        f"""host host{index} {{
 id -{index + 2}
 alg straw
 hash 0
 item osd{index} weight {weight:.5f}
}}"""
        for index, weight in enumerate(weights)
    )
    root_items = "\n".join(f" item host{index} weight {weight:.5f}" for index, weight in enumerate(weights))
    return f"""# begin crush map
# devices
{devices}
# types
type 0 osd
type 1 host
type 2 root
# buckets
{hosts}
root default {{
 id -1
 alg straw
 hash 0
{root_items}
}}
# rules
rule data {{
 id 0
 type replicated
 step take default
 step chooseleaf firstn 0 type host
 step emit
}}
# end crush map
"""


def shared(after):
    host0 = (2.0, 1.0) if after == "item" else ((2.0, 2.0) if after == "subtree" else (1.0, 1.0))
    fake = (2.0, 2.0) if after == "item" else (1.0, 1.0)
    return f"""# begin crush map
# devices
device 0 osd.0
device 1 osd.1
# types
type 0 osd
type 1 host
type 2 root
# buckets
host host0 {{
 id -2
 alg straw
 hash 0
 item osd.0 weight {host0[0]:.5f}
 item osd.1 weight {host0[1]:.5f}
}}
host fake {{
 id -3
 alg straw
 hash 0
 item osd.0 weight {fake[0]:.5f}
 item osd.1 weight {fake[1]:.5f}
}}
root default {{
 id -1
 alg straw
 hash 0
 item host0 weight {sum(host0):.5f}
 item fake weight {sum(fake):.5f}
}}
# rules
rule data {{
 id 0
 type replicated
 step take default
 step chooseleaf firstn 0 type host
 step emit
}}
# end crush map
"""


def rows(image, data, replicas, limit=127):
    output = run(image, "-i", "-", "--test", "--rule", "0", "--num-rep", str(replicas),
                 "--min-x", "0", "--max-x", str(limit), "--show-mappings", input=data).decode().splitlines()
    assert len(output) == limit + 1, len(output)
    parsed = []
    for x, line in enumerate(output):
        prefix = f"CRUSH rule 0 x {x} "
        assert line.startswith(prefix) and line.endswith("]"), line
        parsed.append(tuple(int(item) for item in line[len(prefix):].strip("[]").split(",") if item))
    return parsed


def live_crush_bucket(release, image):
    name = f"crush-weight-topology-{release}-{int(time.time())}"
    log = Path(tempfile.mkstemp(prefix=f".weight-topology-{release}-", suffix=".log")[1])
    with log.open("w") as output:
        process = sp.Popen([
            "docker", "run", "--rm", "--name", name, "--network", "host", "--user", "ceph",
            "-e", "CRUSH_PREFLIGHT_HOLD=300", "-v", f"{preflight}:/preflight.sh:ro", image,
            "bash", "/preflight.sh",
        ], stdout=output, stderr=sp.STDOUT)
        try:
            deadline = time.monotonic() + 150
            while "LIVE PREFLIGHT PASS" not in log.read_text():
                if process.poll() is not None:
                    raise RuntimeError(log.read_text())
                if time.monotonic() > deadline:
                    raise TimeoutError(f"{release} cluster startup")
                time.sleep(1)
            command = "; ".join([
                "conf=$(echo /tmp/rados-mon-classes.*/ceph.conf)",
                "ceph --conf \"$conf\" osd getcrushmap -o /tmp/map1",
                "crushtool -d /tmp/map1 -o /tmp/map1.txt",
                "var=$(ceph --conf \"$conf\" osd crush dump | grep -w id | grep '-' | grep -Eo '[0-9]+' | sort | uniq | sed -n '$p')",
                "id=$(expr \"$var\" + 1)",
                "item=$(sed -n '/^root/,/}/p' /tmp/map1.txt | grep item | head -1)",
                "weight=$(sed -n '/^root/,/}/p' /tmp/map1.txt | grep item | head -1 | awk '{print $4}')",
                "bucket=\"host test {\\\\n id -$id\\\\n # weight $weight\\\\n alg straw \\\\n hash 0  # rjenkins1 \\\\n $item\\\\n}\\\\n\"",
                "sed -i \"/# buckets/a\\ $bucket\" /tmp/map1.txt",
                "crushtool -c /tmp/map1.txt -o /tmp/map1.bin 2>/tmp/rev",
                "test ! -s /tmp/rev",
            ])
            sp.run(["docker", "exec", name, "bash", "-ec", command], check=True)
            with tempfile.TemporaryDirectory(prefix=".weight-topology-live-", dir=reference) as temporary:
                captured = Path(temporary) / "added-straw.crushmap"
                sp.run(["docker", "cp", f"{name}:/tmp/map1.bin", str(captured)], check=True, stdout=sp.DEVNULL)
                return captured.read_bytes()
        finally:
            sp.run(["docker", "stop", "--time", "5", name], stdout=sp.DEVNULL, stderr=sp.DEVNULL, check=False)
            process.wait(timeout=15)
            assert not sp.check_output(["docker", "ps", "-q", "--filter", f"name={name}"]).strip()
            log.unlink(missing_ok=True)


generated = {}
copied = {}
manifest = []
for release, version, revision, image_digest in releases:
    image = "quay.io/ceph/ceph@sha256:" + image_digest
    identity = sp.check_output(docker + ["ceph", image, "--version"], text=True)
    assert f"ceph version {version} ({revision})" in identity, identity
    shell = source(revision, "src/test/test_crush_bucket.sh").decode()
    start = shell.index("function TEST_crush_bucket()")
    block = shell[start:shell.index("\n}\n", start) + 2] + "\n"
    assert hashlib.sha256(block.encode()).hexdigest() == "46099b31dd2735d7b6cdb658e91ef94bee0c7ca32b8f77a77e92ae50e84c2406"
    for path in upstream:
        data = source(revision, path)
        copied.setdefault(Path(path).name, data)
        assert copied[Path(path).name] == data, (release, path)
    multitype = compile_map(image, copied["multitype.after"].decode())
    multitree = compile_map(image, copied["simple.template.multitree.reweighted"].decode())
    maps = {
        "adjust-before": compile_map(image, shared("before")),
        "adjust-item-after": compile_map(image, shared("item")),
        "adjust-subtree-after": compile_map(image, shared("subtree")),
        "multitype-after": multitype,
        "multitree-after": multitree,
        "crushdiff-before": compile_map(image, topology([1.0] * 6)),
        "crushdiff-after": compile_map(image, topology([3.0] + [1.0] * 5)),
        "added-straw": live_crush_bucket(release, image),
    }
    for name, data in maps.items():
        generated[f"weight-topology-{name}-{release}.crushmap"] = data
    for name in ("adjust-before", "adjust-item-after", "adjust-subtree-after", "multitype-after", "multitree-after", "added-straw"):
        for x, row in enumerate(rows(image, maps[name], 1)):
            manifest.append(f"{release}|{name}|{x}|{','.join(map(str, row))}")
    before = rows(image, maps["crushdiff-before"], 3, 999)
    after = rows(image, maps["crushdiff-after"], 3, 999)
    moved = sum(left != right for left, right in zip(before, after))
    assert moved > 0, moved
    for name, values in (("crushdiff-before", before), ("crushdiff-after", after)):
        for x, row in enumerate(values):
            manifest.append(f"{release}|{name}|{x}|{','.join(map(str, row))}")
    manifest.append(f"movement|{release}|crushdiff|{moved}")

for filename, data in copied.items():
    path = fixtures / filename
    if check:
        assert path.read_bytes() == data, f"stale {path}"
    else:
        path.write_bytes(data)
    print(f"fixtures/{filename}: {len(data)} bytes SHA256 {hashlib.sha256(data).hexdigest()}")
for filename, data in generated.items():
    path = reference / filename
    if check:
        assert path.read_bytes() == data, f"stale {path}"
    else:
        path.write_bytes(data)
    print(f"{filename}: {len(data)} bytes SHA256 {hashlib.sha256(data).hexdigest()}")
data = ("# Generated by generate-weight-topology-reference.py from pinned crushtool.\n"
        "# mapping fields: release|case|x|ordered-items; movement: movement|release|crushdiff|changed-rows.\n"
        + "\n".join(manifest) + "\n").encode()
path = reference / "weight-topology-vectors.txt"
if check:
    assert path.read_bytes() == data, f"stale {path}"
else:
    path.write_bytes(data)
print(f"weight-topology-vectors.txt: {len(manifest)} rows SHA256 {hashlib.sha256(data).hexdigest()}")
