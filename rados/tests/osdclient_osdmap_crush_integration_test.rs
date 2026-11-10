use bytes::Bytes;
use rados::crush::PgId;
use rados::denc::VersionedEncode;
use rados::osdclient::OSDMap;
use std::fs;
use std::path::PathBuf;

/// Supplemental external corpus gate; it is not a pinned-release fixture.
#[test]
#[ignore = "requires RADOS_OSDMAP_CORPUS_DIR with the v19.2.x OSDMap corpus"]
fn v19_osdmap_corpus_crush_rules_all_decode() {
    let directory = PathBuf::from(
        std::env::var_os("RADOS_OSDMAP_CORPUS_DIR")
            .expect("RADOS_OSDMAP_CORPUS_DIR must name the v19.2.x OSDMap corpus directory"),
    );
    assert!(
        directory.is_dir(),
        "not a corpus directory: {}",
        directory.display()
    );

    let mut paths: Vec<_> = fs::read_dir(&directory)
        .expect("read external corpus directory")
        .map(|entry| entry.expect("read external corpus entry").path())
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "external corpus directory is empty");

    let mut decoded = 0;
    let mut crush_maps = 0;
    for path in paths {
        let mut bytes = Bytes::from(fs::read(&path).expect("read external OSDMap corpus file"));
        let map = OSDMap::decode_versioned(&mut bytes, 0)
            .unwrap_or_else(|error| panic!("decode {}: {error:?}", path.display()));
        assert!(
            bytes.is_empty(),
            "{} left {} bytes",
            path.display(),
            bytes.len()
        );
        decoded += 1;

        if let Some(crush) = map.get_crush_map() {
            crush_maps += 1;
            for pool_id in map.pools.keys() {
                if let Some(rule_id) = map.get_pool_crush_rule(*pool_id) {
                    crush.get_rule(rule_id as u32).unwrap_or_else(|error| {
                        panic!(
                            "{} pool {pool_id} rule {rule_id}: {error:?}",
                            path.display()
                        )
                    });
                }
            }
        }
    }
    assert!(decoded > 0);
    assert!(crush_maps > 0, "external corpus had no decoded CRUSH map");
}

#[test]
fn osdmap_helper_methods_without_crush_fail() {
    let osdmap = OSDMap::new();
    assert!(osdmap.get_crush_map().is_none());
    assert!(osdmap.get_pool(1).is_none());
    assert!(osdmap.get_pool_name(1).is_none());
    assert!(osdmap.get_pool_crush_rule(1).is_none());
    assert!(osdmap.pg_to_osds(&PgId { pool: 1, seed: 0 }).is_err());
}
