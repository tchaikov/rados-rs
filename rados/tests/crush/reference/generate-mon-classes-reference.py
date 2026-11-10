"""Capture retained TEST_mon_classes map states from pinned isolated clusters.

Run: python3 rados/tests/crush/reference/generate-mon-classes-reference.py
Requires the pinned ARM64 Docker images and the adjacent bounded topology
script live-mon-classes-preflight.sh.
"""
import hashlib
from pathlib import Path
import re
import subprocess as sp
import sys
import tempfile
import time

reference = Path(__file__).resolve().parent
preflight = reference / "live-mon-classes-preflight.sh"
arguments = sys.argv[1:]
check = arguments[-1:] == ["--check"]
ceph = Path(arguments[0]).resolve() if arguments and not arguments[0].startswith("--") else Path("../ceph").resolve()
releases = (
    ("quincy", "17.2.7", "b12291d110049b2f35e32e0de30d70e9a4c060d2",
     "a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada"),
    ("tentacle", "20.2.4", "7f793731f1b39eb4f465e960113d2363c311b964",
     "6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"),
)
state_rules = {"asdf": (1,), "abc": (1, 2), "class2": (1, 2, 3)}
docker = ["docker", "run", "--rm", "--network", "none", "-i", "--entrypoint"]
source_prefixes = (
    "ceph osd crush class create ",
    "ceph osd crush class rename ",
    "ceph osd crush class rm ",
    "ceph osd erasure-code-profile ",
    "ceph osd crush set-device-class ",
    "ceph osd crush rm-device-class ",
    "ceph osd crush move ",
    "ceph osd crush rule create-replicated ",
)


def command(name, script):
    script = "conf=$(echo /tmp/rados-mon-classes.*/ceph.conf); " + script.replace("ceph ", "ceph --conf \"$conf\" ")
    sp.run(["docker", "exec", name, "bash", "-ec", script], check=True, stdout=sp.DEVNULL)


def lifecycle_commands(block):
    commands = []
    for line in block.splitlines():
        failure = line.lstrip().startswith("expect_failure ")
        match = re.search(r"\bceph osd (?:crush|erasure-code-profile) .*?(?= \|\||$)", line)
        if match and match.group().startswith(source_prefixes):
            commands.append(("! " if failure else "") + match.group())
    assert len(commands) == 30, commands
    return commands


def capture(name, state, output):
    command(name, f"ceph osd getcrushmap -o /tmp/{state}.crushmap")
    sp.run(["docker", "cp", f"{name}:/tmp/{state}.crushmap", str(output)], check=True, stdout=sp.DEVNULL)
    return output.read_bytes()


def vectors(image, data, rule):
    output = sp.check_output(docker + ["crushtool", image, "-i", "-", "--test", "--rule", str(rule),
                                       "--num-rep", "3", "--min-x", "0", "--max-x", "19", "--show-mappings"], input=data)
    rows = output.decode().splitlines()
    assert len(rows) == 20, (rule, len(rows))
    return [line.split("[", 1)[1].rstrip("]").replace(",", " ") for line in rows]


generated = {}
manifest = []
for release, version, revision, image_digest in releases:
    image = "quay.io/ceph/ceph@sha256:" + image_digest
    identity = sp.check_output(docker + ["ceph", image, "--version"], text=True)
    assert f"ceph version {version} ({revision})" in identity, identity
    source = sp.check_output(["git", "-C", str(ceph), "show", f"{revision}:qa/standalone/crush/crush-classes.sh"], text=True)
    block = source[source.index("function TEST_mon_classes()") : source.index("\n}\n", source.index("function TEST_mon_classes()")) + 2]
    assert hashlib.sha256((block + "\n").encode()).hexdigest() == "2a9140cf59fe452de815aa7f61cff56f17c1435a0a3346a2d663eed05be05639"
    commands = lifecycle_commands(block)
    name = f"crush-mon-classes-{release}-{int(time.time())}"
    log = Path(tempfile.mkstemp(prefix=f".mon-classes-{release}-", suffix=".log")[1])
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
            with tempfile.TemporaryDirectory(prefix=".mon-classes-", dir=reference) as temporary:
                work = Path(temporary)
                command(name, "; ".join(commands[:15]))
                generated[release, "removed"] = capture(name, "removed", work / "removed.crushmap")
                command(name, "; ".join(commands[15:18]))
                generated[release, "asdf"] = capture(name, "asdf", work / "asdf.crushmap")
                command(name, "; ".join(commands[18:25]))
                generated[release, "abc"] = capture(name, "abc", work / "abc.crushmap")
                command(name, "; ".join(commands[25:]))
                generated[release, "class2"] = capture(name, "class2", work / "class2.crushmap")
        finally:
            sp.run(["docker", "stop", "--time", "5", name], stdout=sp.DEVNULL, stderr=sp.DEVNULL, check=False)
            process.wait(timeout=15)
            assert not sp.check_output(["docker", "ps", "-q", "--filter", f"name={name}"]).strip()
            log.unlink(missing_ok=True)
    for state, rules in state_rules.items():
        data = generated[release, state]
        for rule in rules:
            for x, row in enumerate(vectors(image, data, rule)):
                manifest.append(f"{release} {state} {rule} {x} [{row}]")

for (release, state), data in generated.items():
    path = reference / f"mon-classes-{state}-{release}.crushmap"
    if check:
        assert path.read_bytes() == data, f"stale {path}"
    else:
        path.write_bytes(data)
    print(f"{path.name}: {len(data)} bytes")
manifest_path = reference / "mon-classes-vectors.txt"
manifest_data = ("\n".join(manifest) + "\n").encode()
if check:
    assert manifest_path.read_bytes() == manifest_data, f"stale {manifest_path}"
else:
    manifest_path.write_bytes(manifest_data)
print(f"mon-classes-vectors.txt: {len(manifest)} rows")
