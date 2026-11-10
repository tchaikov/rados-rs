use super::*;
use crate::crush::{CrushRule, CrushRuleStep, RuleOp, RuleType};

fn placement_map(pool_type: u8, size: u8) -> (OSDMap, PgId) {
    let mut map = OSDMap::new();
    map.max_osd = 3;
    map.osd_state = vec![CEPH_OSD_EXISTS | CEPH_OSD_UP; 3];
    map.osd_weight = vec![0x10000; 3];
    map.pools.insert(
        1,
        PgPool {
            pool_type,
            size,
            pg_num: 1,
            pgp_num: 1,
            ..Default::default()
        },
    );
    let mut crush = CrushMap::new();
    crush.max_devices = 3;
    crush.rules.push(Some(CrushRule {
        rule_id: 0,
        rule_type: if pool_type == PgPool::TYPE_REPLICATED {
            RuleType::Replicated
        } else {
            RuleType::Erasure
        },
        steps: [0, 1]
            .into_iter()
            .flat_map(|osd| {
                [
                    CrushRuleStep {
                        op: RuleOp::Take,
                        arg1: osd,
                        arg2: 0,
                    },
                    CrushRuleStep {
                        op: RuleOp::Emit,
                        arg1: 0,
                        arg2: 0,
                    },
                ]
            })
            .collect(),
    }));
    map.crush = Some(crush);
    (map, PgId::new(1, 0))
}

#[test]
fn incremental_applies_up_after_state_and_resets_destroyed_osds() {
    let (mut map, pg) = placement_map(PgPool::TYPE_REPLICATED, 2);
    map.osd_state[0] = 0;
    let mut up = OSDMapIncremental::new(Epoch::new(1));
    up.new_state.insert(0, 0);
    up.new_up_client.insert(0, EntityAddrvec::default());
    up.apply_to(&mut map).unwrap();
    assert_eq!(map.osd_state[0], CEPH_OSD_EXISTS | CEPH_OSD_UP);
    assert_eq!(map.pg_to_placement(&pg).unwrap().up, [0, 1]);

    map.osd_primary_affinity = vec![0; 3];
    map.osd_uuid = vec![UuidD::from_bytes([1; 16]); 3];
    let addrs = EntityAddrvec::with_addr(EntityAddr::default());
    map.osd_addrs_client = vec![addrs.clone(); 3];
    map.osd_addrs_cluster = vec![addrs.clone(); 3];
    map.osd_addrs_hb_front = vec![addrs.clone(); 3];
    map.osd_addrs_hb_back = vec![addrs; 3];
    map.osd_info = vec![
        OsdInfo {
            up_from: Epoch::new(1),
            ..Default::default()
        };
        3
    ];
    map.osd_xinfo = vec![
        OsdXInfo {
            old_weight: 123,
            ..Default::default()
        };
        3
    ];
    map.pg_temp.insert(pg, vec![0, 1]);
    let mut destroy = OSDMapIncremental::new(Epoch::new(2));
    destroy.new_state.insert(0, CEPH_OSD_EXISTS);
    destroy.new_primary_affinity.insert(0, 123);
    destroy.apply_to(&mut map).unwrap();
    assert_eq!(map.osd_state[0], 0);
    assert!(!map.exists(0));
    assert!(!map.is_up(0));
    assert_eq!(
        map.osd_primary_affinity[0],
        CEPH_OSD_DEFAULT_PRIMARY_AFFINITY
    );
    assert_eq!(map.osd_uuid[0], UuidD::default());
    assert_eq!(map.osd_info[0].up_from, Epoch::default());
    assert_eq!(map.osd_xinfo[0].old_weight, 0);
    for addrs in [
        &map.osd_addrs_client,
        &map.osd_addrs_cluster,
        &map.osd_addrs_hb_front,
        &map.osd_addrs_hb_back,
    ] {
        assert!(addrs[0].addrs.is_empty());
    }
    assert_eq!(map.pg_to_placement(&pg).unwrap().acting, [1]);

    // is_up must also reject inconsistent state vectors and IDs beyond max_osd.
    map.osd_state[0] = CEPH_OSD_UP;
    assert!(!map.is_up(0));
    map.max_osd = 1;
    assert!(!map.exists(1));
    assert!(!map.is_up(1));
}

#[test]
fn invalid_incremental_crush_leaves_base_and_caches_unchanged() {
    for epoch in [0, 1] {
        let (mut map, pg) = placement_map(PgPool::TYPE_REPLICATED, 2);
        map.epoch = Epoch::new(epoch);
        let placement = map.pg_to_placement(&pg).unwrap();
        let before = serde_json::to_value(&map).unwrap();
        let mut inc = OSDMapIncremental::new(Epoch::new(epoch + 1));
        if epoch == 0 {
            inc.fsid = UuidD::from_bytes([42; 16]);
        }
        inc.crush = Bytes::from_static(&[0; 4]);
        inc.new_weight.insert(0, 0);
        inc.old_pools.push(1);
        inc.new_pg_temp.insert(pg, vec![2]);
        let error = inc.apply_to(&mut map).unwrap_err();
        assert!(matches!(error, RadosError::Crush(_)));
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(serde_json::to_value(&map).unwrap(), before);
        let crush = map.crush.as_ref().unwrap();
        assert_eq!(crush.max_devices, 3);
        assert_eq!(crush.rules[0].as_ref().unwrap().steps[0].arg1, 0);
        assert_eq!(
            map.lock_acting_cache().unwrap().peek(&(1, 0)),
            Some(&placement)
        );
        assert_eq!(
            map.lock_crush_cache().unwrap().peek(&(1, 0)),
            Some(&placement.raw)
        );
    }
}

#[test]
fn full_and_incremental_maps_validate_embedded_crush() {
    for fixture in [
        include_bytes!("../../../tests/osdmaptool-fixtures/crush-quincy.osdmap").as_slice(),
        include_bytes!("../../../tests/osdmaptool-fixtures/crush-tentacle.osdmap").as_slice(),
    ] {
        let map = OSDMap::decode_versioned(&mut Bytes::copy_from_slice(fixture), 0).unwrap();
        let crush = map.crush.unwrap();
        let header: Vec<_> = [
            0x10000,
            crush.max_buckets as u32,
            crush.rules.len() as u32,
            crush.max_devices as u32,
        ]
        .into_iter()
        .flat_map(u32::to_le_bytes)
        .collect();
        let offset = fixture
            .windows(header.len())
            .position(|bytes| bytes == header)
            .unwrap();
        let mut damaged = fixture.to_vec();
        damaged[offset..offset + 4].fill(0);
        assert!(matches!(
            OSDMap::decode_versioned(&mut Bytes::from(damaged), 0),
            Err(RadosError::Crush(_))
        ));

        let length = u32::from_le_bytes(fixture[offset - 4..offset].try_into().unwrap()) as usize;
        let (mut base, pg) = placement_map(PgPool::TYPE_REPLICATED, 2);
        base.pg_to_placement(&pg).unwrap();
        let mut inc = OSDMapIncremental::new(Epoch::new(1));
        inc.crush = Bytes::copy_from_slice(&fixture[offset..offset + length]);
        inc.apply_to(&mut base).unwrap();
        assert_eq!(base.epoch, Epoch::new(1));
        let updated = base.crush.as_ref().unwrap();
        assert_eq!(updated.max_devices, crush.max_devices);
        assert_eq!(updated.names, crush.names);
        assert_eq!(updated.buckets.len(), crush.buckets.len());
        assert_eq!(updated.rules.len(), crush.rules.len());
        assert!(base.lock_acting_cache().unwrap().is_empty());
        assert!(base.lock_crush_cache().unwrap().is_empty());
    }
}

#[test]
fn placement_removes_nonexistent_sources_before_upmap() {
    for pool_type in [PgPool::TYPE_REPLICATED, PgPool::TYPE_ERASURE] {
        let (mut map, pg) = placement_map(pool_type, 2);
        map.osd_state[0] = 0;
        map.osd_weight[0] = 0;
        map.pg_upmap_items.insert(pg, vec![(0, 2)]);
        let placement = map.pg_to_placement(&pg).unwrap();
        assert_eq!(placement.raw, [0, 1]);
        assert_eq!(
            placement.up,
            if pool_type == PgPool::TYPE_REPLICATED {
                vec![1]
            } else {
                vec![CRUSH_ITEM_NONE, 1]
            }
        );
        assert_eq!(placement.acting, placement.up);
        assert_eq!(placement.up_primary, 1);

        // A down but existing source remains eligible for an upmap replacement.
        map.osd_state[0] = CEPH_OSD_EXISTS;
        map.lock_acting_cache().unwrap().clear();
        assert_eq!(map.pg_to_placement(&pg).unwrap().up, [2, 1]);
    }
}

#[test]
fn optimized_ec_temp_supports_all_128_shards_and_rejects_invalid_layouts() {
    let (mut map, pg) = placement_map(PgPool::TYPE_ERASURE, 128);
    map.max_osd = 128;
    map.osd_state = vec![CEPH_OSD_EXISTS | CEPH_OSD_UP; 128];
    let pool = map.pools.get_mut(&1).unwrap();
    pool.flags = PgPool::FLAG_EC_OPTIMIZATIONS;
    for shard in [1, 63, 127] {
        pool.nonprimary_shards.insert(ShardId::new(shard));
    }
    let original: Vec<_> = (0..128).rev().collect();
    let encoded = pool.pgtemp_primaryfirst_vec(&original);
    assert_eq!(
        pool.pgtemp_undo_primaryfirst_vec(true, &encoded).unwrap(),
        original
    );
    assert!(
        pool.pgtemp_undo_primaryfirst_vec(true, &encoded[..127])
            .is_err()
    );
    pool.size = 129;
    assert!(
        pool.pgtemp_undo_primaryfirst_vec(true, &vec![0; 129])
            .is_err()
    );
    pool.size = 127;
    assert!(
        pool.pgtemp_undo_primaryfirst_vec(true, &encoded[..127])
            .is_err()
    );
    pool.size = 128;

    map.pg_temp.insert(pg, encoded);
    assert_eq!(map.pg_to_placement(&pg).unwrap().acting, original);
    map.lock_acting_cache().unwrap().clear();
    map.pg_temp.get_mut(&pg).unwrap().pop();
    assert!(map.pg_to_placement(&pg).is_err());
    map.pg_temp.insert(pg, Vec::new());
    let placement = map.pg_to_placement(&pg).unwrap();
    assert_eq!(placement.acting, placement.up);
}
