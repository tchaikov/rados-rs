use bytes::{Bytes, BytesMut, BufMut};
use denc::{OSDMap, VersionedEncode, Denc, UuidD, UTime};

fn main() {
    // Create a minimal OSDMap for testing
    println!("Testing OSDMap encoder/decoder");
    
    // Test 1: Verify that decode_versioned works without panicking
    let mut test_buf = BytesMut::new();
    
    // Write a minimal versioned OSDMap structure
    // Version header
    test_buf.put_u8(8); // version
    test_buf.put_u8(7); // compat version
    
    // We need to create the content first to know the length
    let mut content = BytesMut::new();
    
    // Client data section
    content.put_u8(10); // client_v
    content.put_u8(1);  // client_compat
    
    // Client data content
    let mut client_content = BytesMut::new();
    
    // fsid (UuidD - 16 bytes)
    client_content.put_slice(&[0u8; 16]);
    
    // epoch (u32)
    client_content.put_u32_le(1);
    
    // created (UTime - 8 bytes)
    client_content.put_u32_le(0);
    client_content.put_u32_le(0);
    
    // modified (UTime - 8 bytes) 
    client_content.put_u32_le(0);
    client_content.put_u32_le(0);
    
    // pools (empty BTreeMap - just length)
    client_content.put_u32_le(0);
    
    // pool_name (empty BTreeMap - just length)
    client_content.put_u32_le(0);
    
    // pool_max
    client_content.put_i64_le(0);
    
    // flags
    client_content.put_u32_le(0);
    
    // max_osd
    client_content.put_i32_le(0);
    
    // osd_state (empty vec)
    client_content.put_u32_le(0);
    
    // osd_weight (empty vec)
    client_content.put_u32_le(0);
    
    // osd_addrs_client (empty vec)
    client_content.put_u32_le(0);
    
    // pg_temp (empty map)
    client_content.put_u32_le(0);
    
    // primary_temp (empty map)
    client_content.put_u32_le(0);
    
    // crush (empty bytes)
    client_content.put_u32_le(0);
    
    // erasure_code_profiles (empty map)
    client_content.put_u32_le(0);
    
    // pg_upmap (client_v >= 4)
    client_content.put_u32_le(0);
    
    // pg_upmap_items
    client_content.put_u32_le(0);
    
    // crush_version (client_v >= 6)
    client_content.put_i32_le(1);
    
    // new_removed_snaps (client_v >= 7)
    client_content.put_u32_le(0);
    
    // new_purged_snaps
    client_content.put_u32_le(0);
    
    // last_up_change (client_v >= 9)
    client_content.put_u32_le(0);
    client_content.put_u32_le(0);
    
    // last_in_change
    client_content.put_u32_le(0);
    client_content.put_u32_le(0);
    
    // pg_upmap_primaries (client_v >= 10)
    client_content.put_u32_le(0);
    
    // Write client data length and content
    content.put_u32_le(client_content.len() as u32);
    content.put_slice(&client_content);
    
    // OSD data section
    content.put_u8(12); // osd_v
    content.put_u8(1);  // osd_compat
    
    // OSD data content
    let mut osd_content = BytesMut::new();
    
    // osd_addrs_hb_back (empty vec)
    osd_content.put_u32_le(0);
    
    // osd_info (empty map)
    osd_content.put_u32_le(0);
    
    // blocklist (empty map)
    osd_content.put_u32_le(0);
    
    // osd_addrs_cluster (empty vec)
    osd_content.put_u32_le(0);
    
    // cluster_snapshot_epoch
    osd_content.put_u32_le(0);
    
    // cluster_snapshot (empty string)
    osd_content.put_u32_le(0);
    
    // osd_uuid (empty map)
    osd_content.put_u32_le(0);
    
    // osd_xinfo (empty map)
    osd_content.put_u32_le(0);
    
    // osd_addrs_hb_front (empty vec)
    osd_content.put_u32_le(0);
    
    // nearfull_ratio, full_ratio, backfillfull_ratio (osd_v >= 2)
    osd_content.put_u32_le(0); // 0.0 as f32
    osd_content.put_u32_le(0);
    osd_content.put_u32_le(0);
    
    // require_min_compat_client, require_osd_release (osd_v >= 5)
    osd_content.put_u8(0);
    osd_content.put_u8(0);
    
    // removed_snaps_queue (osd_v >= 6)
    osd_content.put_u32_le(0);
    
    // crush_node_flags (osd_v >= 8)
    osd_content.put_u32_le(0);
    
    // device_class_flags (osd_v >= 9)
    osd_content.put_u32_le(0);
    
    // stretch mode fields (osd_v >= 10)
    osd_content.put_u8(0); // stretch_mode_enabled
    osd_content.put_u32_le(0); // stretch_bucket_count
    osd_content.put_u32_le(0); // degraded_stretch_mode
    osd_content.put_u32_le(0); // recovering_stretch_mode
    osd_content.put_i32_le(0); // stretch_mode_bucket
    
    // range_blocklist (osd_v >= 11)
    osd_content.put_u32_le(0);
    
    // allow_crimson (osd_v >= 12)
    osd_content.put_u8(0);
    
    // Write OSD data length and content
    content.put_u32_le(osd_content.len() as u32);
    content.put_slice(&osd_content);
    
    // Write total content length
    test_buf.put_u32_le(content.len() as u32);
    test_buf.put_slice(&content);
    
    let mut test_bytes = test_buf.freeze();
    
    println!("Created test OSDMap data: {} bytes", test_bytes.len());
    
    // Try to decode
    match OSDMap::decode_versioned(&mut test_bytes, 0) {
        Ok(osdmap) => {
            println!("✓ Successfully decoded OSDMap!");
            println!("  Epoch: {}", osdmap.epoch);
            println!("  Max OSD: {}", osdmap.max_osd);
            println!("  Pools: {}", osdmap.pools.len());
            println!("  Remaining bytes: {}", test_bytes.len());
            
            if test_bytes.len() == 0 {
                println!("✓ Perfect decode - no remaining bytes");
            } else {
                println!("⚠ Warning: {} bytes remaining", test_bytes.len());
            }
        }
        Err(e) => {
            println!("✗ Failed to decode OSDMap: {}", e);
            std::process::exit(1);
        }
    }
}
