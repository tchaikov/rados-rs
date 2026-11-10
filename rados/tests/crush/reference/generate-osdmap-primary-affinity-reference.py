"""Generate pinned-Ceph PrimaryAffinity placement digests.

Run: python3 rados/tests/crush/reference/generate-osdmap-primary-affinity-reference.py ../ceph [--check]
The source fixture is TestOSDMap's build_simple(6) with its explicit
osd_crush_chooseleaf_type=0 and add_simple_rule("erasure", ..., "indep").
"""
from pathlib import Path
import hashlib
import subprocess as sp
import sys
import tempfile

reference = Path(__file__).resolve().parent
ceph = Path(sys.argv[1]).resolve()
check = sys.argv[2:] == ["--check"]
fixture = reference / "osdmap-primary-affinity.crush"
manifest = reference / "osdmap-primary-affinity-vectors.txt"
runner_source = reference / "osdmap-primary-affinity-reference.c"
releases = (
    ("quincy", "17.2.7", "b12291d110049b2f35e32e0de30d70e9a4c060d2",
     "a70ccb2d8a0e814aa1c009e7541289b99bf039521727b6f44611499e9ca3fada"),
    ("tentacle", "20.2.4", "7f793731f1b39eb4f465e960113d2363c311b964",
     "6e6bc7b28fa1b334108a3646af5533dfb50db508efdf5b358eb7dd0dd37a48aa"),
)
docker = ["docker", "run", "--rm", "--network", "none", "-i", "--entrypoint"]
source_files = (
    "crush/mapper.c", "crush/mapper.h", "crush/hash.c", "crush/hash.h",
    "crush/crush.h", "crush/crush_ln_table.h", "crush/crush_compat.h", "include/int_types.h",
)


def image(digest):
    return "quay.io/ceph/ceph@sha256:" + digest


def run_crushtool(container, *args, input=None):
    return sp.check_output(docker + ["crushtool", container, *args], input=input)


def build_source_fixture(container, directory):
    directory.mkdir()
    command = [
        "docker", "run", "--rm", "--network", "none", "-v", f"{directory}:/work",
        "-w", "/work", "--entrypoint", "/bin/bash", container, "-ec",
        "CEPH_ARGS='--osd_crush_chooseleaf_type 0' osdmaptool map --createsimple 6 --clobber --pg-bits 6 >/dev/null; "
        "CEPH_ARGS='--osd_crush_chooseleaf_type 0' osdmaptool map --export-crush map.bin >/dev/null; "
        "crushtool -d map.bin -o map.crush",
    ]
    sp.run(command, check=True, stdout=sp.DEVNULL)
    text = (directory / "map.crush").read_text()
    assert "step choose firstn 0 type osd" in text
    rule = """rule erasure {
\tid 1
\ttype erasure
\tstep set_chooseleaf_tries 5
\tstep set_choose_tries 100
\tstep take default
\tstep choose indep 0 type osd
\tstep emit
}
"""
    return text.replace("# end crush map\n", rule + "# end crush map\n")


def compile_hash_runner(revision, directory):
    (directory / "crush").mkdir(parents=True)
    (directory / "include").mkdir()
    for source in source_files:
        destination = directory / source
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(sp.check_output(["git", "-C", str(ceph), "show", f"{revision}:src/{source}"]))
    (directory / "acconfig.h").touch()
    runner = directory / "run"
    sp.run([
        "clang", "-std=gnu99", "-O2", f"-I{directory}", f"-I{directory / 'crush'}",
        str(runner_source), str(directory / "crush/mapper.c"), str(directory / "crush/hash.c"),
        "-lm", "-o", str(runner),
    ], check=True)
    return runner


def compile_fixture(container, data, directory):
    directory.mkdir()
    (directory / "input.crush").write_bytes(data)
    sp.run([
        "docker", "run", "--rm", "--network", "none", "-v", f"{directory}:/work",
        "--entrypoint", "crushtool", container, "-c", "/work/input.crush", "-o", "/work/map.bin",
    ], check=True, stdout=sp.DEVNULL)
    return (directory / "map.bin").read_bytes()


def pps(runner, pool, seed):
    return int(sp.check_output([runner, "pps", str(pool), str(seed)]))


def raw_rows(container, data, runner, pool, rule):
    rows = []
    for seed in range(64):
        row = [int(item) for item in sp.check_output([runner, "raw", str(pool), str(seed)]).split()[1:]]
        if pool == 1:
            value = pps(runner, pool, seed)
            line = run_crushtool(
                container, "-i", "-", "--test", "--rule", str(rule), "--min_x", str(value),
                "--max_x", str(value), "--num-rep", "3", "--show-mappings", input=data,
            ).decode().strip()
            prefix = f"CRUSH rule {rule} x {value} "
            assert line.startswith(prefix) and line.endswith("]"), line
            assert row == [int(item) for item in line[len(prefix):].strip("[]").split(",") if item]
        # `crushtool` parses `--min_x` as signed, so high-bit HASHPSPOOL
        # values cannot be requested one at a time. The same pinned mapper.c
        # and hash.c runner supplies those 64 replicated raw rows.
        rows.append(row)
    return rows


def digest_placements(runner, pool, state, rows):
    raw_input = "".join(
        f"{pool} {state} {seed} {len(rows[seed % 64])} "
        + " ".join(map(str, rows[seed % 64])) + "\n"
        for seed in range(10_000)
    ).encode()
    output = sp.check_output([runner], input=raw_input)
    return hashlib.sha256(output).hexdigest(), len(output)


generated = None
lines = [
    "# SHA-256 of 10,000 canonical PgPlacement rows per pinned release/pool/state.",
    "# Row bytes: up length/u32 then i32 OSDs, acting length/u32 then i32 OSDs, up_primary/i32, acting_primary/i32; all big endian.",
    "# fields: release pool affinity-state count sha256",
]
with tempfile.TemporaryDirectory(prefix=".osdmap-primary-affinity-", dir=reference) as temporary:
    temporary = Path(temporary)
    for release, version, revision, digest in releases:
        container = image(digest)
        identity = sp.check_output(docker + ["ceph", container, "--version"], text=True)
        assert f"ceph version {version} ({revision})" in identity, identity
        source = build_source_fixture(container, temporary / release)
        if generated is None:
            generated = source
        else:
            assert generated == source, f"{release} fixture differs"
        fixture_data = compile_fixture(container, source.encode(), temporary / f"{release}-fixture")
        runner = compile_hash_runner(revision, temporary / f"{release}-hash")
        for pool, rule in ((1, 1), (2, 0)):
            rows = raw_rows(container, fixture_data, runner, pool, rule)
            for state, name in enumerate(("default", "zero", "half")):
                value, size = digest_placements(runner, pool, state, rows)
                assert size > 0
                lines.append(f"{release} {pool} {name} 10000 {value}")

assert generated is not None
if check:
    assert fixture.read_text() == generated, f"stale {fixture}"
else:
    fixture.write_text(generated)
data = ("\n".join(lines) + "\n")
if check:
    assert manifest.read_text() == data, f"stale {manifest}"
else:
    manifest.write_text(data)
print(f"osdmap-primary-affinity.crush SHA256 {hashlib.sha256(generated.encode()).hexdigest()}")
print(f"osdmap-primary-affinity-vectors.txt: {len(lines) - 3} digests SHA256 {hashlib.sha256(data.encode()).hexdigest()}")
