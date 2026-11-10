"""Compile upstream maps and verify their transcripts with pinned crushtool.

Run: python3 rados/tests/crush/reference/prepare-cli.py ../ceph
Requires the two ARM64 Docker images documented in README.md.
Writes generated .crushmap files here only after every comparison passes.
"""
from pathlib import Path
import hashlib
import shlex
import subprocess as sp
import sys

reference = Path(__file__).resolve().parent
fixtures = reference.parent / "fixtures"
ceph = Path(sys.argv[1]).resolve()
releases = (
    ("quincy", "17.2.7", "b12291d110049b2f35e32e0de30d70e9a4c060d2",
     "a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada"),
    ("tentacle", "20.2.4", "7f793731f1b39eb4f465e960113d2363c311b964",
     "6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"),
)
cases = (
    ("bad-mappings", "bad-mappings.crushmap.txt", 2),
    ("test-map-firstn-indep", "test-map-firstn-indep.txt", 2),
    ("set-choose", "set-choose.crushmap.txt", 3),
    ("show-choose-tries", "show-choose-tries.txt", 2),
)
generated = {}
for release, version, revision, digest in releases:
    image = "quay.io/ceph/ceph@sha256:" + digest
    docker = ["docker", "run", "--rm", "--network", "none", "-i", "--entrypoint"]
    identity = sp.check_output(docker + ["ceph", image, "--version"], text=True)
    assert f"ceph version {version} ({revision})" in identity, identity
    packages = sp.check_output(docker + ["rpm", image, "-qf", "/usr/bin/crushtool",
                                        "/usr/bin/ceph"], text=True).splitlines()
    assert len(packages) == 2 and packages[0].startswith(f"ceph-base-{version}-"), packages
    assert packages[0].removeprefix("ceph-base-") == packages[1].removeprefix("ceph-common-"), packages
    for name, source_name, test_count in cases:
        for filename in (source_name, name + ".t"):
            upstream = sp.check_output(["git", "-C", str(ceph), "show",
                                       f"{revision}:src/test/cli/crushtool/{filename}"])
            assert (fixtures / filename).read_bytes() == upstream, (release, filename)
        data = sp.check_output(docker + ["crushtool", image, "-c", "/dev/stdin",
                                        "-o", "/dev/stdout"],
                               input=(fixtures / source_name).read_bytes())
        assert data[:4] == b"\x00\x00\x01\x00", (release, name, "CRUSH magic")
        checked = 0
        variables = {}
        for block in (fixtures / (name + ".t")).read_text().split("  $ ")[1:]:
            command, output = block.split("\n", 1)
            if "=" in command and " " not in command:
                key, value = command.split("=", 1)
                variables[key] = value
                continue
            args = shlex.split(command)
            if args[0] != "crushtool" or "--test" not in args:
                continue
            args = [variables.get(arg[1:], arg) if arg.startswith("$") else arg for arg in args]
            # Only the input transport changes; every testing option is retained.
            args[args.index("-i") + 1] = "-"
            expected = []
            for line in output.splitlines():
                assert line.startswith("  "), (name, line)
                line = line[2:]
                if line.endswith(" (esc)"):
                    line = line.removesuffix(" (esc)").replace(r"\t", "\t")
                expected.append(line)
            actual = sp.check_output(docker + [args[0], image] + args[1:],
                                     input=data).decode().splitlines()
            assert actual == expected, (release, name, command, "transcript mismatch")
            checked += 1
        assert checked == test_count, (release, name, checked)
        generated[f"{name}-{release}.crushmap"] = data
        print(f"{release}: {name}: {checked} complete command outputs match", flush=True)

for filename, data in generated.items():
    (reference / filename).write_bytes(data)
    print(f"{filename}: {len(data)} bytes, SHA256 {hashlib.sha256(data).hexdigest()}")
