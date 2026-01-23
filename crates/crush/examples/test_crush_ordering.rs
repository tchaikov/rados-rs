// Test to verify CRUSH ordering fix
// Run with: cargo run --example test_crush_ordering

use bytes::Bytes;
use crush::{pg_to_osds, CrushMap, PgId};
use std::fs;

fn main() {
    // Read the CRUSH map
    let crushmap_data = fs::read("/tmp/crushmap").expect("Failed to read crushmap");

    // Decode it
    let mut bytes = Bytes::from(crushmap_data);
    let crush_map = CrushMap::decode(&mut bytes).expect("Failed to decode crushmap");

    // Test PG 2.a (pool 2, seed 10)
    let pg = PgId::new(2, 10);

    // OSD weights represent in/out status: 0x10000 means fully in
    // (This is different from CRUSH bucket weights which are in the map)
    let weights = vec![0x10000, 0x10000, 0x10000];

    // Debug: compute the x value that will be passed to CRUSH
    use crush::hash::crush_hash32_2;
    let x = crush_hash32_2(pg.seed, pg.pool as u32);
    println!(
        "DEBUG: pg.seed={}, pg.pool={}, x={} (0x{:x})",
        pg.seed, pg.pool, x, x
    );

    // Use rule 0, size 3, with hashpspool enabled
    let result = pg_to_osds(
        &crush_map, pg, 0, // rule_id
        &weights, 3,    // result_max (pool size)
        true, // hashpspool
    )
    .expect("Failed to map PG to OSDs");

    println!("PG {} maps to OSDs: {:?}", pg, result);
    println!("Expected: [1, 0, 2]");

    if result == vec![1, 0, 2] {
        println!("✓ SUCCESS: Ordering matches Ceph!");
    } else {
        println!("✗ FAILURE: Ordering does not match");
    }
}
