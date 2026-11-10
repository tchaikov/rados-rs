use bytes::Bytes;
use rados::crush::PgId;
use rados::denc::VersionedEncode;
use rados::osdclient::OSDMap;
use std::fs;
use std::path::PathBuf;

fn corpus_paths() -> Vec<PathBuf> {
    let directory = PathBuf::from(
        std::env::var_os("RADOS_OSDMAP_CORPUS_DIR")
            .expect("RADOS_OSDMAP_CORPUS_DIR must name the v19.2.x OSDMap corpus directory"),
    );
    assert!(
        directory.is_dir(),
        "not a corpus directory: {}",
        directory.display()
    );
    let mut paths: Vec<_> = fs::read_dir(directory)
        .expect("read external corpus directory")
        .map(|entry| entry.expect("read external corpus entry").path())
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "external corpus directory is empty");
    paths
}

fn decode(path: &PathBuf) -> OSDMap {
    let mut bytes = Bytes::from(fs::read(path).expect("read external OSDMap corpus file"));
    let map = OSDMap::decode_versioned(&mut bytes, 0)
        .unwrap_or_else(|error| panic!("decode {}: {error:?}", path.display()));
    assert!(
        bytes.is_empty(),
        "{} left {} bytes",
        path.display(),
        bytes.len()
    );
    map
}

/// Supplemental external corpus gate; it is not a pinned-release fixture.
#[test]
#[ignore = "requires RADOS_OSDMAP_CORPUS_DIR with the v19.2.x OSDMap corpus"]
fn v19_object_placement_pipeline() {
    let mut placements = 0;
    for path in corpus_paths() {
        let map = decode(&path);
        if map.get_crush_map().is_none() || map.pools.is_empty() {
            continue;
        }

        for (pool_id, pool) in &map.pools {
            if pool.pg_num == 0 {
                continue;
            }
            for object in ["test_object", "foo", "bar", "object.1"] {
                let pg = map.object_to_pg(*pool_id, object).unwrap_or_else(|error| {
                    panic!(
                        "{} pool {pool_id} object_to_pg({object}): {error:?}",
                        path.display()
                    )
                });
                assert_eq!(pg.pool, *pool_id);
                assert!(pg.seed < pool.pg_num);
                let osds = map.pg_to_osds(&pg).unwrap_or_else(|error| {
                    panic!("{} PG {}.{:x}: {error:?}", path.display(), pg.pool, pg.seed)
                });
                assert!(osds.len() <= pool.size as usize);
                assert_eq!(map.object_to_osds(*pool_id, object).unwrap(), osds);
                placements += 1;
            }
        }
    }
    assert!(
        placements > 0,
        "external corpus had no eligible object placements"
    );
}

/// Supplemental external corpus gate; it is not a pinned-release fixture.
#[test]
#[ignore = "requires RADOS_OSDMAP_CORPUS_DIR with the v19.2.x OSDMap corpus"]
fn v19_object_to_pg_is_deterministic() {
    for path in corpus_paths() {
        let map = decode(&path);
        for (pool_id, pool) in &map.pools {
            if pool.pg_num == 0 {
                continue;
            }
            let object = "deterministic_test_object";
            let first = map.object_to_pg(*pool_id, object).unwrap();
            assert_eq!(first, map.object_to_pg(*pool_id, object).unwrap());
            assert_eq!(first, map.object_to_pg(*pool_id, object).unwrap());
            return;
        }
    }
    panic!("external corpus had no pool with PGs");
}

#[test]
fn pg_to_osds_without_crush_fails() {
    assert!(
        OSDMap::new()
            .pg_to_osds(&PgId { pool: 1, seed: 0 })
            .is_err()
    );
}

#[test]
fn object_to_pg_without_pool_fails() {
    assert!(OSDMap::new().object_to_pg(1, "test_object").is_err());
}
