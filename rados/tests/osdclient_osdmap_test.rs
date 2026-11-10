use bytes::Bytes;
use rados::crush::PgId;
use rados::denc::VersionedEncode;
use rados::osdclient::{OSDMap, OSDMapIncremental};
use std::fs;
use std::path::{Path, PathBuf};

type ReleaseFixture = (
    &'static str,
    &'static [u8],
    &'static [u8],
    &'static [u8],
    &'static [u8],
    &'static str,
    &'static str,
);

const RELEASES: [ReleaseFixture; 2] = [
    (
        "quincy",
        include_bytes!("osdmaptool-fixtures/create-racks-quincy.osdmap"),
        include_bytes!("osdmaptool-fixtures/test-map-pgs-quincy.osdmap"),
        include_bytes!("osdmaptool-fixtures/crush-quincy.osdmap"),
        include_bytes!("osdmaptool-fixtures/create-print-quincy.osdmap"),
        include_str!("osdmaptool-fixtures/test-map-pgs-results-quincy.txt"),
        include_str!("osdmaptool-fixtures/create-racks-pg0-quincy.txt"),
    ),
    (
        "tentacle",
        include_bytes!("osdmaptool-fixtures/create-racks-tentacle.osdmap"),
        include_bytes!("osdmaptool-fixtures/test-map-pgs-tentacle.osdmap"),
        include_bytes!("osdmaptool-fixtures/crush-tentacle.osdmap"),
        include_bytes!("osdmaptool-fixtures/create-print-tentacle.osdmap"),
        include_str!("osdmaptool-fixtures/test-map-pgs-results-tentacle.txt"),
        include_str!("osdmaptool-fixtures/create-racks-pg0-tentacle.txt"),
    ),
];

fn decode(bytes: &[u8]) -> OSDMap {
    let mut bytes = Bytes::copy_from_slice(bytes);
    let map = OSDMap::decode_versioned(&mut bytes, 0).expect("decode pinned OSDMap fixture");
    assert!(
        bytes.is_empty(),
        "fixture decoder left {} bytes",
        bytes.len()
    );
    map
}

fn corpus_dir(variable: &str) -> PathBuf {
    let path = std::env::var_os(variable)
        .unwrap_or_else(|| panic!("{variable} must name the v19.2.x corpus directory"));
    let path = PathBuf::from(path);
    assert!(
        path.is_dir(),
        "{variable} is not a directory: {}",
        path.display()
    );
    path
}

fn corpus_files(directory: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(directory)
        .expect("read external corpus directory")
        .map(|entry| entry.expect("read external corpus entry").path())
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "external corpus directory is empty");
    paths
}

fn source_counts(output: &str) -> Vec<(usize, usize, usize)> {
    let mut counts = vec![(0, 0, 0); 500];
    let mut seen = 0;
    for line in output.lines().filter(|line| line.starts_with("osd.")) {
        let fields: Vec<_> = line.split_whitespace().collect();
        assert_eq!(fields.len(), 6, "unexpected source row: {line}");
        let osd: usize = fields[0].strip_prefix("osd.").unwrap().parse().unwrap();
        counts[osd] = (
            fields[1].parse().unwrap(),
            fields[2].parse().unwrap(),
            fields[3].parse().unwrap(),
        );
        seen += 1;
    }
    assert_eq!(seen, 500, "source output must list every OSD");
    counts
}

// Upstream: v17.2.7/v20.2.4 src/test/cli/osdmaptool/create-racks.t::PG0.0
// Sources: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/osdmaptool/create-racks.t
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/osdmaptool/create-racks.t
// Empty missing-pool placement: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/osd/OSDMap.cc#L2797
#[test]
fn osdmaptool_create_racks_retains_pg0_source_result() {
    for (release, racks, _, _, _, _, pg0_output) in RELEASES {
        let map = decode(racks);
        assert_eq!(map.max_osd, 239, "{release}");
        assert_eq!(map.pools[&1].size, 3, "{release}");
        assert_eq!(map.pools[&1].pg_num, 15_296, "{release}");
        assert!(
            pg0_output.contains("0.0 raw ([], p-1) up ([], p-1) acting ([], p-1)"),
            "{release}"
        );
        let empty = rados::osdclient::PgPlacement {
            raw: vec![],
            up: vec![],
            acting: vec![],
            up_primary: -1,
            acting_primary: -1,
        };
        assert_eq!(
            map.pg_to_placement(&PgId { pool: 0, seed: 0 }).unwrap(),
            empty,
            "{release}"
        );
    }
}

// Upstream: v17.2.7/v20.2.4 src/test/cli/osdmaptool/test-map-pgs.t::500-OSD STRAW workload
// Sources: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/osdmaptool/test-map-pgs.t
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/osdmaptool/test-map-pgs.t
#[test]
fn osdmaptool_test_map_pgs_replays_all_source_pgs() {
    for (release, _, pgs, _, _, tool_output, _) in RELEASES {
        assert!(tool_output.contains("pool 1 pg_num 8000"), "{release}");
        assert!(tool_output.contains("size 3\t8000"), "{release}");

        let mut map = decode(pgs);
        assert_eq!(map.max_osd, 500, "{release}");
        assert_eq!(map.pools[&1].size, 3, "{release}");
        assert_eq!(map.pools[&1].pg_num, 8_000, "{release}");
        map.osd_state = vec![0x3; 500]; // `osdmaptool --mark-up-in`
        map.osd_weight = vec![0x1_0000; 500];

        let expected = source_counts(tool_output);
        let mut actual = vec![(0, 0, 0); 500];
        for seed in 0..8_000 {
            let placement = map.pg_to_placement(&PgId { pool: 1, seed }).unwrap();
            assert_eq!(placement.raw.len(), 3, "{release} PG 1.{seed:x}");
            assert_eq!(placement.up.len(), 3, "{release} PG 1.{seed:x}");
            assert_eq!(placement.acting.len(), 3, "{release} PG 1.{seed:x}");
            for osd in &placement.acting {
                actual[*osd as usize].0 += 1;
            }
            actual[placement.acting[0] as usize].1 += 1;
            actual[placement.acting_primary as usize].2 += 1;
        }
        assert_eq!(actual, expected, "{release}");
    }
}

// Upstream: v17.2.7/v20.2.4 src/test/cli/osdmaptool/crush.t::create_export_import;
// src/test/cli/osdmaptool/create-print.t::createsimple_3 (locally assigned decode subsets).
// Sources: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/osdmaptool/crush.t
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/osdmaptool/crush.t
// https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/osdmaptool/create-print.t
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/osdmaptool/create-print.t
#[test]
fn osdmaptool_crush_and_create_print_maps_decode() {
    for (release, _, _, crush, create_print, _, _) in RELEASES {
        let crush = decode(crush);
        assert_eq!(crush.max_osd, 3, "{release}");
        assert_eq!(crush.pools[&1].size, 3, "{release}");
        assert!(crush.get_crush_map().is_some(), "{release}");

        let create_print = decode(create_print);
        assert_eq!(create_print.max_osd, 3, "{release}");
        assert_eq!(create_print.pools[&1].size, 3, "{release}");
        assert_eq!(create_print.pools[&1].pg_num, 192, "{release}");
        assert_eq!(create_print.pools[&1].pgp_num, 192, "{release}");
        assert!(create_print.get_crush_map().is_some(), "{release}");
    }
}

/// Supplemental external corpus gate; it is not a pinned-release fixture.
#[test]
#[ignore = "requires RADOS_OSDMAP_CORPUS_DIR with the v19.2.x OSDMap corpus"]
fn v19_osdmap_corpus_files_all_decode() {
    for path in corpus_files(&corpus_dir("RADOS_OSDMAP_CORPUS_DIR")) {
        let mut bytes = Bytes::from(fs::read(&path).expect("read external OSDMap corpus file"));
        OSDMap::decode_versioned(&mut bytes, 0)
            .unwrap_or_else(|error| panic!("decode {}: {error:?}", path.display()));
        assert!(
            bytes.is_empty(),
            "{} left {} bytes",
            path.display(),
            bytes.len()
        );
    }
}

/// Supplemental external corpus gate; it is not a pinned-release fixture.
#[test]
#[ignore = "requires RADOS_OSDMAP_INCREMENTAL_CORPUS_DIR with the v19.2.x incremental corpus"]
fn v19_osdmap_incremental_corpus_files_all_decode() {
    for path in corpus_files(&corpus_dir("RADOS_OSDMAP_INCREMENTAL_CORPUS_DIR")) {
        let mut bytes =
            Bytes::from(fs::read(&path).expect("read external incremental corpus file"));
        OSDMapIncremental::decode_versioned(&mut bytes, 0)
            .unwrap_or_else(|error| panic!("decode {}: {error:?}", path.display()));
        assert!(
            bytes.is_empty(),
            "{} left {} bytes",
            path.display(),
            bytes.len()
        );
    }
}
