"""Write source-shaped intermediate states from crush-choose-args.sh assertions."""
from pathlib import Path

root = Path(__file__).resolve().parent
default = "18446744073709551615"

def single(name, base, alt, index=default, ids="", positions=None, root_ids=""):
    positions = positions or [alt]
    host_sets = "\n".join("  [ " + " ".join(f"{weight}.00000" for weight in row) + " ]" for row in positions)
    root_sets = "\n".join(f"  [ {sum(row)}.00000 ]" for row in positions)
    devices = "\n".join(f"device {i} osd.{i}" for i in range(len(base)))
    items = "\n".join(f" item osd.{i} weight {weight}.00000" for i, weight in enumerate(base))
    (root / f"qa-{name}.crush").write_text(f'''# Local source-shaped state from crush-choose-args.sh::{name}.
# Published canonical/compat HOST totals: {sum(base)}/{sum(alt)}.
# begin crush map
tunable choose_local_tries 0
tunable choose_local_fallback_tries 0
tunable choose_total_tries 50
tunable chooseleaf_descend_once 1
tunable chooseleaf_vary_r 1
tunable chooseleaf_stable 1
# devices
{devices}
# types
type 0 osd
type 1 host
type 2 root
# buckets
host HOST {{
 id -2
 alg straw2
 hash 0
{items}
}}
root default {{
 id -1
 alg straw2
 hash 0
 item HOST weight {sum(base)}.00000
}}
# rules
rule replicated_rule {{
 id 0
 type replicated
 step take default
 step choose firstn 0 type osd
 step emit
}}
# choose_args
choose_args {index} {{
 {{ bucket_id -1 weight_set [
{root_sets}
 ] {root_ids} }}
 {{ bucket_id -2 weight_set [
{host_sets}
 ] {ids} }}
}}
# end crush map
''')

single("update-pre", [3], [2], "0", "ids [ -20 ]", [[2], [2]], "ids [ -10 ]")
single("update-remove", [3], [2], "0", "ids [ -20 ]", [[2], [2]], "ids [ -10 ]")
single("no-update-pre", [3], [2], "0", "ids [ -20 ]", [[2], [1]], "ids [ -10 ]")
single("no-update-remove", [3], [2], "0", "ids [ -20 ]", [[2], [1]], "ids [ -10 ]")
single("reweight-create", [3, 3], [3, 3])
single("reweight-compat-osd0", [3, 3], [2, 3])
single("reweight-add-osd2", [3, 3, 3], [2, 3, 0])
single("reweight-canonical-osd2", [3, 3, 4], [2, 3, 0])

def move(name, root_items, rack_items, host_items, foo_items, root_alt, rack_alt, host_alt, foo_alt):
    def bucket(kind, label, ident, items):
        text = f"{kind} {label} {{\n id {ident}\n alg straw2\n hash 0\n"
        text += "\n".join(f" item {item} weight {weight}.00000" for item, weight in items)
        return text + "\n}\n"
    devices = "device 0 osd.0\ndevice 1 osd.1"
    buckets = bucket("host", "HOST", -4, host_items) + bucket("host", "FOO", -3, foo_items)
    buckets += bucket("rack", "RACK", -2, rack_items) + bucket("root", "default", -1, root_items)
    args = []
    for ident, weights in [(-1, root_alt), (-2, rack_alt), (-3, foo_alt), (-4, host_alt)]:
        if weights:
            args.append(f" {{ bucket_id {ident} weight_set [ [ {' '.join(f'{w}.00000' for w in weights)} ] ] }}")
    (root / f"qa-{name}.crush").write_text(f'''# Local source-shaped state from crush-choose-args.sh::{name}.
# begin crush map
tunable choose_local_tries 0
tunable choose_local_fallback_tries 0
tunable choose_total_tries 50
tunable chooseleaf_descend_once 1
tunable chooseleaf_vary_r 1
tunable chooseleaf_stable 1
# devices
{devices}
# types
type 0 osd
type 1 host
type 2 rack
type 3 root
# buckets
{buckets}# rules
rule replicated_rule {{
 id 0
 type replicated
 step take default
 step choose firstn 0 type osd
 step emit
}}
# choose_args
choose_args {default} {{
{chr(10).join(args)}
}}
# end crush map
''')

move("move-create", [("HOST", 6)], [], [("osd.0", 3), ("osd.1", 3)], [], [4], [], [2, 2], [])
move("move-rack", [("RACK", 6)], [("HOST", 6)], [("osd.0", 3), ("osd.1", 3)], [], [4], [4], [2, 2], [])
move("move-compat-osd0", [("RACK", 6)], [("HOST", 6)], [("osd.0", 3), ("osd.1", 3)], [], [3], [3], [1, 2], [])
move("move-osd0-to-foo", [("RACK", 3), ("FOO", 3)], [("HOST", 3)], [("osd.1", 3)], [("osd.0", 3)], [2, 3], [2], [2], [3])
