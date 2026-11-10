"""Audit functional ports against pinned C sources; not required by Cargo tests.

Run from the repository root after cargo test -p rados --test crush:
    python3 rados/tests/crush/reference/verify-reference.py ../ceph
Requires git, clang, rustc and the two commits in the Ceph checkout.
"""
from pathlib import Path
import os
import re
import subprocess as sp
import sys
import tempfile

ceph = Path(sys.argv[1]).resolve()
reference = Path(__file__).resolve().parent
repo = reference.parents[3]
refs = {
    "quincy": "b12291d110049b2f35e32e0de30d70e9a4c060d2",
    "tentacle": "7f793731f1b39eb4f465e960113d2363c311b964",
}

with tempfile.TemporaryDirectory(prefix="crush-functional-reference-") as temporary:
    root = Path(temporary)
    outputs = {}
    for release, revision in refs.items():
        dest = root / release
        (dest / "include").mkdir(parents=True)
        (dest / "crush").mkdir()
        for name in ("crush/mapper.c", "crush/mapper.h", "crush/hash.c", "crush/hash.h",
                     "crush/crush.h", "crush/crush_ln_table.h", "crush/crush_compat.h",
                     "crush/builder.c", "crush/builder.h", "crush/crush.c", "include/int_types.h"):
            data = sp.check_output(["git", "-C", str(ceph), "show",
                                    f"{revision}:src/{name}"])
            (dest / name).write_bytes(data)
        (dest / "acconfig.h").touch()
        command = ["clang", "-std=gnu99", "-O2", f"-I{dest}", f"-I{dest / 'crush'}",
                   str(reference / "reference-check.c"), str(dest / "crush/mapper.c"),
                   str(dest / "crush/hash.c"), str(dest / "crush/builder.c"),
                   str(dest / "crush/crush.c"), "-lm", "-o", str(dest / "run")]
        if release == "quincy":
            command.append("-DQUINCY")
        sp.run(command, check=True)
        outputs[release] = sp.check_output([str(dest / "run")], text=True).splitlines()

    # Instrument a temporary copy so the checked-in ports remain assertion-only.
    source = (repo / "rados/tests/crush/functional.rs").read_text()
    source = source.replace("    out\n}",
        '    println!("ORACLE devices={} x={x} count={count} weights={weights:?} out={out:?}", map.max_devices);\n    out\n}', 1)
    source = source.replace("    // bc scale=5", '    println!("COUNTS replicas={replicas} {counts:?}");\n    // bc scale=5', 1)
    source = source.replace('        assert_eq!(out, expected, "rule {rule_id}");',
        '        println!("BAD_MAPPINGS rule={rule_id} out={out:?}");\n        assert_eq!(out, expected, "rule {rule_id}");', 1)
    (root / "functional.rs").write_text(source)
    deps = repo / "target/debug/deps"
    rlib = max(deps.glob("librados-*.rlib"), key=lambda p: p.stat().st_mtime)
    sp.run(["rustc", "--edition=2024", "--test", str(root / "functional.rs"),
            "-L", f"dependency={deps}", "--extern", f"rados={rlib}",
            "-o", str(root / "rust-tests")], check=True)
    actual = []
    for selection in ("msr_", "weight_distribution", "bad_mappings"):
        output = sp.check_output([str(root / "rust-tests"), selection,
                                  "--nocapture", "--test-threads=1"], text=True)
        for line in output.splitlines():
            for marker in ("ORACLE ", "COUNTS ", "BAD_MAPPINGS "):
                if marker in line:
                    actual.append(marker + line.split(marker, 1)[1])
    assert sum(line.startswith("ORACLE ") for line in actual) == 3007
    assert sum(line.startswith("BAD_MAPPINGS ") for line in actual) == 2
    mapper_outputs = [line for line in outputs["tentacle"]
                      if line.startswith(("ORACLE ", "COUNTS ", "BAD_MAPPINGS "))]
    assert sorted(actual) == sorted(mapper_outputs), "Rust/Tentacle mismatch"
    shared = sorted(line for line in actual if not line.startswith("ORACLE "))
    quincy_shared = [line for line in outputs["quincy"]
                     if line.startswith(("COUNTS ", "BAD_MAPPINGS "))]
    assert shared == sorted(quincy_shared), "Quincy/Tentacle mismatch"

    weight_source = root / "weights.rs"
    weight_source.write_text((repo / "rados/tests/crush/weights.rs").read_text())
    sp.run(["rustc", "--edition=2024", "--test", str(weight_source),
            "-L", f"dependency={deps}", "--extern", f"rados={rlib}",
            "-o", str(root / "rust-weights")], check=True)
    environment = os.environ | {"CRUSH_REFERENCE_AUDIT": "1"}
    output = sp.check_output([str(root / "rust-weights"), "--nocapture", "--test-threads=1"],
                             text=True, env=environment)
    rust_weights = sorted("WEIGHTS " + line.split("WEIGHTS ", 1)[1]
                          for line in output.splitlines() if "WEIGHTS " in line)
    rust_straws = {
        match.group(1): match.group(2)
        for line in output.splitlines()
        if (match := re.search(r"STRAW_RUST (zero[01]|same[01]) straws=([0-9,]+)", line))
    }
    c_weights = sorted(line for line in outputs["tentacle"] if line.startswith("WEIGHTS "))
    c_straws = {
        name: straws
        for line in outputs["tentacle"] if line.startswith("STRAW ")
        for name, straws in re.findall(r"(zero[01]|same[01]) weights=[0-9,]+ straws=([0-9,]+)", line)
    }
    assert rust_weights == c_weights, f"Rust/Tentacle weight mapping mismatch: {rust_weights!r} != {c_weights!r}"
    assert rust_straws == c_straws, f"Rust/Tentacle STRAW lengths mismatch: {rust_straws!r} != {c_straws!r}"
    assert c_weights == sorted(line for line in outputs["quincy"] if line.startswith("WEIGHTS "))
    assert [line for line in outputs["quincy"] if line.startswith("STRAW ")] == \
        [line for line in outputs["tentacle"] if line.startswith("STRAW ")]
    quincy_indep = [line for line in outputs["quincy"] if line.startswith("INDEP ")]
    tentacle_indep = [line for line in outputs["tentacle"] if line.startswith("INDEP ")]
    assert quincy_indep == tentacle_indep
    source = source.replace("    out\n}\n\nfn assert_no_duplicates",
        '    println!("INDEP_RUST out={out:?}");\n    out\n}\n\nfn assert_no_duplicates', 1)
    (root / "functional-indep.rs").write_text(source)
    sp.run(["rustc", "--edition=2024", "--test", str(root / "functional-indep.rs"),
            "-L", f"dependency={deps}", "--extern", f"rados={rlib}",
            "-o", str(root / "rust-indep")], check=True)
    def word_digest(lines):
        value = 1469598103934665603
        for line in lines:
            values = line.split("out=", 1)[1].strip()[1:-1]
            items = [] if not values else [int(item.strip()) for item in values.split(",")]
            for item in [len(items), *(item & 0xffffffff for item in items)]:
                value = (value ^ item) * 1099511628211 & 0xffffffffffffffff
        return f"{value:016x}"
    expected_indep = {line.split()[1]: line.rsplit("=", 1)[1] for line in quincy_indep}
    for case in ("toosmall", "basic", "out_alt", "out_contig", "out_progressive"):
        output = sp.check_output([str(root / "rust-indep"), f"indep_{case}_normal",
                                  "--nocapture", "--test-threads=1"], text=True)
        rust_lines = [line for line in output.splitlines() if "INDEP_RUST " in line]
        assert word_digest(rust_lines) == expected_indep[case], \
            f"Rust/{case} mismatch: {word_digest(rust_lines)} != {expected_indep[case]} ({len(rust_lines)} vectors)"
    print("All 3,007 MSR vectors match Tentacle; all five device counts match both releases.")
    print("Both bad-mappings vectors and the STRAW builder setup match both releases.")
    print("All STRAW/STRAW2 output digests match both releases; unseeded rand()%10 is 7.")
    print("Pinned C STRAW lengths match the executing Rust fixtures.")
    print("Raw rule type 123 and Erasure type 3 match exactly for every Quincy INDEP vector.")
    print("\n".join(quincy_indep))
    print("\n".join(line for line in outputs["quincy"] if line.startswith("STRAW ")))
    print("\n".join(shared))
