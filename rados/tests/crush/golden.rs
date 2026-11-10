//! Replay Ceph's committed crushtool mappings through the public Rust API.
//! Fixture provenance, checksums and adaptations: fixtures/README.md.

use bytes::Bytes;
use rados::crush::{CrushMap, mapper::crush_do_rule};
use std::collections::BTreeMap;
use std::ops::RangeInclusive;

fn decode(data: &'static [u8]) -> CrushMap {
    let mut bytes = Bytes::from_static(data);
    let map = CrushMap::decode(&mut bytes).expect("decode Ceph reference map");
    assert!(bytes.is_empty(), "unconsumed CRUSH map bytes");
    map
}

fn check_mappings(
    map: CrushMap,
    transcript: &str,
    rule: u32,
    replicas: RangeInclusive<usize>,
    out_devices: &[usize],
) {
    let mut weights = vec![0x10000; map.max_devices as usize];
    for &device in out_devices {
        weights[device] = 0;
    }
    check_weighted_mappings(&map, transcript, rule..=rule, replicas, &weights);
}

fn check_weighted_mappings(
    map: &CrushMap,
    transcript: &str,
    rules: RangeInclusive<u32>,
    replicas: RangeInclusive<usize>,
    weights: &[u32],
) {
    let replica_count = replicas.clone().count();
    let mut fixture_sizes = BTreeMap::<(u32, usize, usize), usize>::new();
    let mut expected_sizes = BTreeMap::new();
    let mut mappings = Vec::new();
    // Validate the entire transcript before the first mapper assertion, so
    // an early placement failure cannot hide missing seeds or statistics.
    for (line_number, line) in transcript.lines().enumerate() {
        let line = line.trim();
        if let Some(mapping) = line.strip_prefix("CRUSH rule ") {
            let (input, output) = mapping.split_once(" [").expect("mapping vector");
            let (source_rule, seed) = input.split_once(" x ").expect("mapping input");
            let rule = rules.start() + (mappings.len() / (1024 * replica_count)) as u32;
            assert!(rules.contains(&rule), "extra rule in fixture");
            assert_eq!(source_rule.parse::<u32>().unwrap(), rule);
            let x = seed.parse::<u32>().unwrap();
            assert_eq!(x, (mappings.len() % 1024) as u32, "missing/reordered seed");
            let numrep = replicas.start() + mappings.len() / 1024 % replica_count;
            let items = output.strip_suffix(']').expect("complete mapping vector");
            let expected: Vec<i32> = if items.is_empty() {
                Vec::new()
            } else {
                items.split(',').map(|item| item.parse().unwrap()).collect()
            };
            *fixture_sizes
                .entry((rule, numrep, expected.len()))
                .or_default() += 1;
            mappings.push((line_number + 1, rule, x, numrep, expected));
        } else if let Some((prefix, statistic)) = line.split_once(" num_rep ") {
            let rule = prefix
                .split_whitespace()
                .nth(1)
                .unwrap()
                .parse::<u32>()
                .unwrap();
            assert!(rules.contains(&rule));
            assert_eq!(prefix, format!("rule {rule} ({})", map.rule_names[&rule]));
            let (numrep, statistic) = statistic.split_once(" result size == ").unwrap();
            let (size, count) = statistic.split_once(":\\t").unwrap();
            let count = count.strip_suffix("/1024 (esc)").unwrap();
            let key = (
                rule,
                numrep.parse::<usize>().unwrap(),
                size.parse::<usize>().unwrap(),
            );
            assert!(
                expected_sizes
                    .insert(key, count.parse::<usize>().unwrap())
                    .is_none()
            );
        } else {
            // CLI-only command, range banner and optional map-modified advisory.
            assert!(
                line.starts_with("$ crushtool ")
                    || rules.clone().any(|rule| line
                        == format!(
                            "rule {rule} ({name}), x = 0..1023, numrep = {min}..{max}",
                            name = map.rule_names.get(&rule).unwrap(),
                            min = replicas.start(),
                            max = replicas.end()
                        ))
                    || line
                        == "crushtool successfully built or modified map.  Use '-o <file>' to write it out.",
                "unrecognized fixture line {}: {line}",
                line_number + 1
            );
        }
    }
    assert_eq!(
        mappings.len(),
        rules.count() * replica_count * 1024,
        "incomplete fixture"
    );
    assert_eq!(
        fixture_sizes, expected_sizes,
        "inconsistent fixture statistics"
    );

    let mut actual = Vec::new();
    let mut actual_sizes = BTreeMap::<(u32, usize, usize), usize>::new();
    for (line, rule, x, numrep, expected) in mappings {
        crush_do_rule(map, rule, x, &mut actual, numrep, weights).unwrap_or_else(|error| {
            panic!("line {line}, rule {rule}, x {x}, replicas {numrep}: {error}")
        });
        assert_eq!(
            actual, expected,
            "line {line}, rule {rule}, x {x}, replicas {numrep}"
        );
        *actual_sizes
            .entry((rule, numrep, actual.len()))
            .or_default() += 1;
    }
    assert_eq!(actual_sizes, expected_sizes, "result-size histograms");
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-bobtail-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-bobtail-tunables.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-bobtail-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-bobtail-tunables.t#L1
#[test]
fn bobtail_tunables() {
    let mut map = decode(include_bytes!("fixtures/test-map-a.crushmap"));
    map.choose_local_tries = 0;
    map.choose_local_fallback_tries = 0;
    map.choose_total_tries = 50;
    map.chooseleaf_descend_once = 1;
    check_mappings(
        map,
        include_str!("fixtures/test-map-bobtail-tunables.t"),
        0,
        1..=10,
        &[],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-firefly-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-firefly-tunables.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-firefly-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-firefly-tunables.t#L1
#[test]
fn firefly_tunables() {
    let mut map = decode(include_bytes!("fixtures/test-map-vary-r.crushmap"));
    map.choose_local_tries = 0;
    map.choose_local_fallback_tries = 0;
    map.choose_total_tries = 50;
    map.chooseleaf_descend_once = 1;
    map.chooseleaf_vary_r = 1;
    check_mappings(
        map,
        include_str!("fixtures/test-map-firefly-tunables.t"),
        0,
        1..=10,
        &[12, 20, 30],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-hammer-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-hammer-tunables.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-hammer-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-hammer-tunables.t#L1
#[test]
fn hammer_tunables() {
    let map = decode(include_bytes!("fixtures/test-map-hammer-tunables.crushmap"));
    check_mappings(
        map,
        include_str!("fixtures/test-map-hammer-tunables.t"),
        0,
        1..=10,
        &[12, 20, 30],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-indep.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-indep.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-indep.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-indep.t#L1
#[test]
fn indep() {
    let mut map = decode(include_bytes!("fixtures/test-map-indep.crushmap"));
    map.choose_local_tries = 0;
    map.choose_local_fallback_tries = 0;
    map.choose_total_tries = 50;
    map.chooseleaf_descend_once = 2;
    check_mappings(
        map,
        include_str!("fixtures/test-map-indep.t"),
        1,
        1..=10,
        &[],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-jewel-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-jewel-tunables.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-jewel-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-jewel-tunables.t#L1
#[test]
fn jewel_tunables() {
    let map = decode(include_bytes!("fixtures/test-map-jewel-tunables.crushmap"));
    check_mappings(
        map,
        include_str!("fixtures/test-map-jewel-tunables.t"),
        0,
        1..=10,
        &[12, 20, 30],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-legacy-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-legacy-tunables.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-legacy-tunables.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-legacy-tunables.t#L1
#[test]
fn legacy_tunables() {
    let map = decode(include_bytes!("fixtures/test-map-a.crushmap"));
    check_mappings(
        map,
        include_str!("fixtures/test-map-legacy-tunables.t"),
        0,
        1..=10,
        &[],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-tries-vs-retries.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-tries-vs-retries.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-tries-vs-retries.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-tries-vs-retries.t#L1
#[test]
fn tries_vs_retries() {
    let map = decode(include_bytes!(
        "fixtures/test-map-tries-vs-retries.crushmap"
    ));
    check_mappings(
        map,
        include_str!("fixtures/test-map-tries-vs-retries.t"),
        0,
        1..=10,
        &[0, 8],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-vary-r-0.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-0.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-vary-r-0.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-0.t#L1
#[test]
fn vary_r_0() {
    let mut map = decode(include_bytes!("fixtures/test-map-vary-r.crushmap"));
    map.chooseleaf_vary_r = 0;
    check_mappings(
        map,
        include_str!("fixtures/test-map-vary-r-0.t"),
        3,
        2..=4,
        &[0, 4, 9],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-vary-r-1.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-1.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-vary-r-1.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-1.t#L1
#[test]
fn vary_r_1() {
    let mut map = decode(include_bytes!("fixtures/test-map-vary-r.crushmap"));
    map.chooseleaf_vary_r = 1;
    check_mappings(
        map,
        include_str!("fixtures/test-map-vary-r-1.t"),
        3,
        2..=4,
        &[0, 4, 9],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-vary-r-2.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-2.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-vary-r-2.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-2.t#L1
#[test]
fn vary_r_2() {
    let mut map = decode(include_bytes!("fixtures/test-map-vary-r.crushmap"));
    map.chooseleaf_vary_r = 2;
    check_mappings(
        map,
        include_str!("fixtures/test-map-vary-r-2.t"),
        3,
        2..=4,
        &[0, 4, 9],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-vary-r-3.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-3.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-vary-r-3.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-3.t#L1
#[test]
fn vary_r_3() {
    let mut map = decode(include_bytes!("fixtures/test-map-vary-r.crushmap"));
    map.chooseleaf_vary_r = 3;
    check_mappings(
        map,
        include_str!("fixtures/test-map-vary-r-3.t"),
        3,
        2..=4,
        &[0, 4, 9],
    );
}

// Upstream (cmd-01 is locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-vary-r-4.t::cmd-01
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-vary-r-4.t#L1
// v20.2.4/src/test/cli/crushtool/test-map-vary-r-4.t::cmd-01
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-vary-r-4.t#L1
#[test]
fn vary_r_4() {
    let mut map = decode(include_bytes!("fixtures/test-map-vary-r.crushmap"));
    map.chooseleaf_vary_r = 4;
    check_mappings(
        map,
        include_str!("fixtures/test-map-vary-r-4.t"),
        3,
        2..=4,
        &[0, 4, 9],
    );
}

fn check_bad_mappings(map: &CrushMap, transcript: &str, replicas: RangeInclusive<usize>) {
    let expected: Vec<_> = transcript
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("bad mapping "))
        .collect();
    let weights = vec![0x10000; map.max_devices as usize];
    let mut actual = Vec::new();
    for rule in 0..2 {
        for count in replicas.clone() {
            let mut out = Vec::new();
            crush_do_rule(map, rule, 1, &mut out, count, &weights).unwrap();
            // CrushTester reports short vectors and any vector containing NONE.
            // Comparing the complete report also checks the unreported successes.
            if out.len() != count || out.contains(&0x7fff_ffff) {
                let items = out.iter().map(i32::to_string).collect::<Vec<_>>().join(",");
                actual.push(format!(
                    "bad mapping rule {rule} x 1 num_rep {count} result [{items}]"
                ));
            }
        }
    }
    assert_eq!(actual, expected);
}

// Upstream (cmd-02/cmd-03 are locally assigned):
// v17.2.7/src/test/cli/crushtool/bad-mappings.t::cmd-02,cmd-03
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/bad-mappings.t#L2
// v20.2.4/src/test/cli/crushtool/bad-mappings.t::cmd-02,cmd-03
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/bad-mappings.t#L2
#[test]
fn bad_mappings_compiled() {
    for (release, bytes) in [
        (
            "v17.2.7",
            &include_bytes!("reference/bad-mappings-quincy.crushmap")[..],
        ),
        (
            "v20.2.4",
            &include_bytes!("reference/bad-mappings-tentacle.crushmap")[..],
        ),
    ] {
        eprintln!("bad-mappings: {release}");
        check_bad_mappings(
            &decode(bytes),
            include_str!("fixtures/bad-mappings.t"),
            10..=10,
        );
    }
}

// Upstream (cmd-02/cmd-03 are locally assigned):
// v17.2.7/src/test/cli/crushtool/test-map-firstn-indep.t::cmd-02,cmd-03
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/test-map-firstn-indep.t#L2
// v20.2.4/src/test/cli/crushtool/test-map-firstn-indep.t::cmd-02,cmd-03
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/test-map-firstn-indep.t#L2
#[test]
fn firstn_indep_compiled() {
    for (release, bytes) in [
        (
            "v17.2.7",
            &include_bytes!("reference/test-map-firstn-indep-quincy.crushmap")[..],
        ),
        (
            "v20.2.4",
            &include_bytes!("reference/test-map-firstn-indep-tentacle.crushmap")[..],
        ),
    ] {
        eprintln!("firstn-indep: {release}");
        check_bad_mappings(
            &decode(bytes),
            include_str!("fixtures/test-map-firstn-indep.t"),
            1..=10,
        );
    }
}

fn check_set_choose(profile: usize, weights: &[u32; 9]) {
    let commands: Vec<_> = include_str!("fixtures/set-choose.t")
        .split("  $ ")
        .collect();
    assert_eq!(commands.len(), 5, "compile command and three test commands");
    let (_, output) = commands[profile + 2].split_once('\n').unwrap();
    for (release, bytes) in [
        (
            "v17.2.7",
            &include_bytes!("reference/set-choose-quincy.crushmap")[..],
        ),
        (
            "v20.2.4",
            &include_bytes!("reference/set-choose-tentacle.crushmap")[..],
        ),
    ] {
        eprintln!("set-choose: {release}, profile {profile}");
        let map = decode(bytes);
        assert_eq!(map.max_devices, 9);
        assert_eq!(map.max_rules, 6);
        check_weighted_mappings(&map, output, 0..=5, 2..=3, weights);
    }
}

// Upstream (cmd-02 is locally assigned):
// v17.2.7/src/test/cli/crushtool/set-choose.t::cmd-02
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/set-choose.t#L2
// v20.2.4/src/test/cli/crushtool/set-choose.t::cmd-02
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/set-choose.t#L2
#[test]
fn set_choose_all_in() {
    check_set_choose(0, &[0x10000; 9]);
}

// Upstream (cmd-03 is locally assigned):
// v17.2.7/src/test/cli/crushtool/set-choose.t::cmd-03
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/set-choose.t#L12310
// v20.2.4/src/test/cli/crushtool/set-choose.t::cmd-03
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/set-choose.t#L12310
#[test]
fn set_choose_out_devices() {
    check_set_choose(
        1,
        &[0, 0, 0x10000, 0, 0, 0x10000, 0x10000, 0x10000, 0x10000],
    );
}

// Upstream (cmd-04 is locally assigned):
// v17.2.7/src/test/cli/crushtool/set-choose.t::cmd-04
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/set-choose.t#L24623
// v20.2.4/src/test/cli/crushtool/set-choose.t::cmd-04
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/set-choose.t#L24623
#[test]
fn set_choose_partial_weights() {
    check_set_choose(
        2,
        &[
            0,
            0x10000,
            0x10000,
            0,
            0x10000 / 2,
            0,
            0x10000 / 10,
            0,
            0x10000,
        ],
    );
}
