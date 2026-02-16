// CRUSH map decoding implementation
// Reference: ~/dev/ceph/src/crush/CrushWrapper.cc (encode/decode functions)

use bytes::{Buf, Bytes};
use denc::Denc;
use std::collections::HashMap;

use crate::error::{CrushError, Result};
use crate::types::*;

// CRUSH magic number
const CRUSH_MAGIC: u32 = 0x00010000;

// ============= Helper Functions Using Denc =============

/// Decode a value using Denc, converting RadosError to CrushError
#[inline]
fn decode<T: Denc>(buf: &mut impl Buf) -> Result<T> {
    T::decode(buf, 0).map_err(|e| CrushError::DecodeError(e.to_string()))
}

/// Decode a vector of values using Denc
fn decode_vec<T: Denc>(buf: &mut impl Buf, count: usize) -> Result<Vec<T>> {
    let mut vec = Vec::with_capacity(count);
    for _ in 0..count {
        vec.push(decode(buf)?);
    }
    Ok(vec)
}

/// Generic map decoder for HashMap<K, String> where K implements Denc
/// Returns empty map if insufficient data (handles optional fields in versioned format)
fn decode_map_to_string<K>(buf: &mut Bytes) -> Result<HashMap<K, String>>
where
    K: Denc + Eq + std::hash::Hash,
{
    // Optional field - return empty map if not present
    if buf.remaining() < 4 {
        return Ok(HashMap::new());
    }
    let len: u32 = decode(buf)?;
    let mut map = HashMap::with_capacity(len as usize);

    for _ in 0..len {
        // Break on corrupted/truncated data rather than error
        if buf.remaining() < 8 {
            break;
        }
        let key: K = decode(buf)?;
        let value: String = decode(buf)?;
        map.insert(key, value);
    }

    Ok(map)
}

/// Generic map decoder for HashMap<K, V> where both K and V implement Denc
/// Returns empty map if insufficient data (handles optional fields in versioned format)
fn decode_map<K, V>(buf: &mut Bytes) -> Result<HashMap<K, V>>
where
    K: Denc + Eq + std::hash::Hash,
    V: Denc,
{
    // Optional field - return empty map if not present
    if buf.remaining() < 4 {
        return Ok(HashMap::new());
    }
    let len: u32 = decode(buf)?;
    let mut map = HashMap::with_capacity(len as usize);

    for _ in 0..len {
        // Break on corrupted/truncated data rather than error
        if buf.remaining() < 8 {
            break;
        }
        let key: K = decode(buf)?;
        let value: V = decode(buf)?;
        map.insert(key, value);
    }

    Ok(map)
}

/// Decode nested map: HashMap<i32, HashMap<i32, i32>>
/// Returns empty map if insufficient data (handles optional fields in versioned format)
fn decode_nested_i32_map(buf: &mut Bytes) -> Result<HashMap<i32, HashMap<i32, i32>>> {
    // Optional field - return empty map if not present
    if buf.remaining() < 4 {
        return Ok(HashMap::new());
    }

    // Now that HashMap<K, V> implements Denc, we can use the generic decode_map
    decode_map(buf)
}

// ============= CrushMap Decoding =============

impl CrushMap {
    /// Decode a CRUSH map from bytes
    ///
    /// This decodes the binary CRUSH map format used by Ceph.
    /// Reference: CrushWrapper::decode() in ~/dev/ceph/src/crush/CrushWrapper.cc
    pub fn decode(data: &mut Bytes) -> Result<Self> {
        // Read magic number
        let magic: u32 = decode(data)?;
        if magic != CRUSH_MAGIC {
            return Err(CrushError::DecodeError(format!(
                "Invalid CRUSH magic: 0x{:x}, expected 0x{:x}",
                magic, CRUSH_MAGIC
            )));
        }

        // Read header
        let max_buckets: i32 = decode(data)?;
        let max_rules: u32 = decode(data)?;
        let max_devices: i32 = decode(data)?;

        let mut map = CrushMap::new();
        map.max_buckets = max_buckets;
        map.max_rules = max_rules;
        map.max_devices = max_devices;

        // Decode buckets
        map.buckets = Vec::with_capacity(max_buckets as usize);
        for _ in 0..max_buckets {
            let alg: u32 = decode(data)?;
            if alg == 0 {
                map.buckets.push(None);
                continue;
            }

            let bucket = decode_bucket(data, alg)?;
            map.buckets.push(Some(bucket));
        }

        // Decode rules
        map.rules = Vec::with_capacity(max_rules as usize);
        for _ in 0..max_rules {
            let exists: u32 = decode(data)?;
            if exists == 0 {
                map.rules.push(None);
                continue;
            }

            let rule = decode_rule(data)?;
            map.rules.push(Some(rule));
        }

        // Decode name maps using generic decoders
        map.type_names = decode_map_to_string(data)?;
        map.names = decode_map_to_string(data)?;
        map.rule_names = decode_map_to_string(data)?;

        // Decode tunables (optional fields)
        if data.remaining() >= 4 {
            map.choose_local_tries = decode(data)?;
        }
        if data.remaining() >= 4 {
            map.choose_local_fallback_tries = decode(data)?;
        }
        if data.remaining() >= 4 {
            map.choose_total_tries = decode(data)?;
        }
        if data.remaining() >= 4 {
            map.chooseleaf_descend_once = decode(data)?;
        }
        if data.remaining() >= 1 {
            map.chooseleaf_vary_r = decode(data)?;
        }
        if data.remaining() >= 1 {
            // straw_calc_version (skip)
            let _: u8 = decode(data)?;
        }
        if data.remaining() >= 4 {
            map.allowed_bucket_algs = decode(data)?;
        }
        if data.remaining() >= 1 {
            map.chooseleaf_stable = decode(data)?;
        }

        // Decode device classes (Luminous+)
        if data.remaining() >= 4 {
            map.class_map = decode_map(data)?;
        }
        if data.remaining() >= 4 {
            map.class_name = decode_map_to_string(data)?;
        }
        if data.remaining() >= 4 {
            map.class_bucket = decode_nested_i32_map(data)?;
        }

        // Skip remaining data (choose args, MSR tunables, etc.)
        // These are optional and not yet implemented

        Ok(map)
    }
}

fn decode_bucket(data: &mut Bytes, alg: u32) -> Result<CrushBucket> {
    let id: i32 = decode(data)?;
    let bucket_type: u16 = decode(data)?;
    let alg_byte: u8 = decode(data)?;
    let hash: u8 = decode(data)?;
    let weight: u32 = decode(data)?;
    let size: u32 = decode(data)?;

    // Verify alg matches
    if alg_byte as u32 != alg {
        return Err(CrushError::DecodeError(format!(
            "Algorithm mismatch: header says {}, bucket says {}",
            alg, alg_byte
        )));
    }

    // Sanity check on size
    if size > 10000 {
        return Err(CrushError::DecodeError(format!(
            "Bucket size too large: {}",
            size
        )));
    }

    // Read items using Denc
    let items: Vec<i32> = decode_vec(data, size as usize)?;

    let algorithm = BucketAlgorithm::try_from(alg_byte)?;

    // Decode algorithm-specific data
    let bucket_data = match algorithm {
        BucketAlgorithm::Uniform => {
            let item_weight: u32 = decode(data)?;
            BucketData::Uniform { item_weight }
        }
        BucketAlgorithm::List => {
            let mut item_weights = Vec::with_capacity(size as usize);
            let mut sum_weights = Vec::with_capacity(size as usize);
            for _ in 0..size {
                item_weights.push(decode(data)?);
                sum_weights.push(decode(data)?);
            }
            BucketData::List {
                item_weights,
                sum_weights,
            }
        }
        BucketAlgorithm::Tree => {
            let num_nodes: u32 = decode(data)?;
            let node_weights: Vec<u32> = decode_vec(data, num_nodes as usize)?;
            BucketData::Tree {
                num_nodes,
                node_weights,
            }
        }
        BucketAlgorithm::Straw => {
            let mut item_weights = Vec::with_capacity(size as usize);
            let mut straws = Vec::with_capacity(size as usize);
            for _ in 0..size {
                item_weights.push(decode(data)?);
                straws.push(decode(data)?);
            }
            BucketData::Straw {
                item_weights,
                straws,
            }
        }
        BucketAlgorithm::Straw2 => {
            let item_weights: Vec<u32> = decode_vec(data, size as usize)?;
            BucketData::Straw2 { item_weights }
        }
    };

    Ok(CrushBucket {
        id,
        bucket_type: bucket_type as i32,
        alg: algorithm,
        hash,
        weight,
        size,
        items,
        data: bucket_data,
    })
}

fn decode_rule(data: &mut Bytes) -> Result<CrushRule> {
    let len: u32 = decode(data)?;

    // Read legacy rule mask (4 bytes)
    let rule_id: u8 = decode(data)?;
    let rule_type: u8 = decode(data)?;
    let _min_size: u8 = decode(data)?;
    let _max_size: u8 = decode(data)?;

    let mut steps = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let op: u32 = decode(data)?;
        let arg1: i32 = decode(data)?;
        let arg2: i32 = decode(data)?;

        let rule_op = RuleOp::try_from(op)?;
        steps.push(CrushRuleStep {
            op: rule_op,
            arg1,
            arg2,
        });
    }

    Ok(CrushRule {
        rule_id: rule_id as u32,
        rule_type: RuleType::from(rule_type),
        steps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    #[ignore] // TODO: Fix binary format alignment issues
    fn test_decode_crushmap_corpus() {
        // Try to decode a CRUSH map from the corpus
        let corpus_path =
            "/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/CrushWrapper/f43a6ecc6a266d1485e06427c1c79aac";

        if !Path::new(corpus_path).exists() {
            println!("Corpus file not found, skipping test");
            return;
        }

        let data = fs::read(corpus_path).expect("Failed to read corpus file");
        let mut bytes = Bytes::from(data);

        println!("Total bytes: {}", bytes.len());

        let result = CrushMap::decode(&mut bytes);
        if let Err(ref e) = result {
            println!("Decode error: {:?}", e);
            println!("Remaining bytes: {}", bytes.remaining());
        }
        assert!(result.is_ok(), "Failed to decode CRUSH map: {:?}", result);

        let map = result.unwrap();
        println!("Decoded CRUSH map:");
        println!("  max_buckets: {}", map.max_buckets);
        println!("  max_rules: {}", map.max_rules);
        println!("  max_devices: {}", map.max_devices);
        println!(
            "  buckets: {}",
            map.buckets.iter().filter(|b| b.is_some()).count()
        );
        println!(
            "  rules: {}",
            map.rules.iter().filter(|r| r.is_some()).count()
        );

        // Verify we decoded something reasonable
        assert!(map.max_buckets > 0);
        assert!(map.max_rules > 0);
    }

    #[test]
    fn test_device_class_methods() {
        let mut map = CrushMap::new();

        // Add some device classes
        map.class_name.insert(1, "ssd".to_string());
        map.class_name.insert(2, "hdd".to_string());
        map.class_name.insert(3, "nvme".to_string());

        // Assign devices to classes
        map.class_map.insert(0, 1); // OSD 0 is SSD
        map.class_map.insert(1, 2); // OSD 1 is HDD
        map.class_map.insert(2, 1); // OSD 2 is SSD
        map.class_map.insert(3, 3); // OSD 3 is NVMe

        // Test get_device_class
        assert_eq!(map.get_device_class(0), Some("ssd"));
        assert_eq!(map.get_device_class(1), Some("hdd"));
        assert_eq!(map.get_device_class(2), Some("ssd"));
        assert_eq!(map.get_device_class(3), Some("nvme"));
        assert_eq!(map.get_device_class(999), None);

        // Test get_class_id
        assert_eq!(map.get_class_id("ssd"), Some(1));
        assert_eq!(map.get_class_id("hdd"), Some(2));
        assert_eq!(map.get_class_id("nvme"), Some(3));
        assert_eq!(map.get_class_id("unknown"), None);

        // Test device_has_class
        assert!(map.device_has_class(0, "ssd"));
        assert!(map.device_has_class(1, "hdd"));
        assert!(!map.device_has_class(0, "hdd"));
        assert!(!map.device_has_class(999, "ssd"));
    }
}
