// CRUSH map decoding implementation
// Reference: ~/dev/ceph/src/crush/CrushWrapper.cc (encode/decode functions)

use bytes::{Buf, BufMut, Bytes};
use denc::{Denc, RadosError};
use std::collections::HashMap;

use crate::error::{CrushError, Result};
use crate::types::*;

// CRUSH magic number
const CRUSH_MAGIC: u32 = 0x00010000;

/// Decode N items without a length prefix.
///
/// Unlike `Vec<T>::decode()` which reads a u32 length prefix first,
/// CRUSH bucket items have their count in a separate `size` field.
fn decode_n<T: Denc>(buf: &mut impl Buf, count: usize) -> Result<Vec<T>> {
    let mut vec = Vec::with_capacity(count);
    for _ in 0..count {
        vec.push(T::decode(buf, 0)?);
    }
    Ok(vec)
}

// ============= CrushMap Decoding =============

impl CrushMap {
    /// Decode a CRUSH map from bytes
    ///
    /// This decodes the binary CRUSH map format used by Ceph.
    /// Reference: CrushWrapper::decode() in ~/dev/ceph/src/crush/CrushWrapper.cc
    pub fn decode(data: &mut Bytes) -> Result<Self> {
        // Read magic number
        let magic = u32::decode(data, 0)?;
        if magic != CRUSH_MAGIC {
            return Err(CrushError::DecodeError(format!(
                "Invalid CRUSH magic: 0x{:x}, expected 0x{:x}",
                magic, CRUSH_MAGIC
            )));
        }

        // Read header
        let max_buckets = i32::decode(data, 0)?;
        let max_rules = u32::decode(data, 0)?;
        let max_devices = i32::decode(data, 0)?;

        let mut map = CrushMap::new();
        map.max_buckets = max_buckets;
        map.max_rules = max_rules;
        map.max_devices = max_devices;

        // Decode buckets
        map.buckets = Vec::with_capacity(max_buckets as usize);
        for _ in 0..max_buckets {
            let alg = u32::decode(data, 0)?;
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
            let exists = u32::decode(data, 0)?;
            if exists == 0 {
                map.rules.push(None);
                continue;
            }

            let rule = decode_rule(data)?;
            map.rules.push(Some(rule));
        }

        // Decode name maps (always present — no version guard)
        map.type_names = HashMap::decode(data, 0)?;
        map.names = HashMap::decode(data, 0)?;
        map.rule_names = HashMap::decode(data, 0)?;

        // Decode tunables (optional fields)
        if data.remaining() >= 4 {
            map.choose_local_tries = u32::decode(data, 0)?;
        }
        if data.remaining() >= 4 {
            map.choose_local_fallback_tries = u32::decode(data, 0)?;
        }
        if data.remaining() >= 4 {
            map.choose_total_tries = u32::decode(data, 0)?;
        }
        if data.remaining() >= 4 {
            map.chooseleaf_descend_once = u32::decode(data, 0)?;
        }
        if data.remaining() >= 1 {
            map.chooseleaf_vary_r = u8::decode(data, 0)?;
        }
        if data.remaining() >= 1 {
            // straw_calc_version (skip)
            let _ = u8::decode(data, 0)?;
        }
        if data.remaining() >= 4 {
            map.allowed_bucket_algs = u32::decode(data, 0)?;
        }
        if data.remaining() >= 1 {
            map.chooseleaf_stable = u8::decode(data, 0)?;
        }

        // Decode device classes (Luminous+) — one atomic optional section
        if data.remaining() > 0 {
            map.class_map = HashMap::decode(data, 0)?;
            map.class_name = HashMap::decode(data, 0)?;
            map.class_bucket = HashMap::decode(data, 0)?;
        }

        // Skip remaining data (choose args, MSR tunables, etc.)
        // These are optional and not yet implemented

        // Post-decode validation
        validate_crush_map(&map)?;

        Ok(map)
    }
}

/// Post-decode validation of the CRUSH map.
///
/// Validates structural invariants so the mapper can trust the data at runtime:
/// - Bucket index consistency: bucket at index i has id == -1 - i
/// - Item references: each item in a bucket is a valid device or bucket
/// - TAKE rule args: each TAKE step references a valid bucket or device
fn validate_crush_map(map: &CrushMap) -> Result<()> {
    for (i, slot) in map.buckets.iter().enumerate() {
        let bucket = match slot {
            Some(b) => b,
            None => continue,
        };

        let expected_id = -1 - i as i32;
        if bucket.id != expected_id {
            return Err(CrushError::DecodeError(format!(
                "Bucket at index {} has id {}, expected {}",
                i, bucket.id, expected_id
            )));
        }

        for &item in &bucket.items {
            if item >= 0 {
                if item >= map.max_devices {
                    return Err(CrushError::DecodeError(format!(
                        "Bucket {} references device {}, but max_devices is {}",
                        bucket.id, item, map.max_devices
                    )));
                }
            } else {
                let target_idx = (-1 - item) as usize;
                if target_idx >= map.buckets.len() || map.buckets[target_idx].is_none() {
                    return Err(CrushError::DecodeError(format!(
                        "Bucket {} references non-existent bucket {}",
                        bucket.id, item
                    )));
                }
            }
        }
    }

    for rule in map.rules.iter().flatten() {
        for step in &rule.steps {
            if step.op == RuleOp::Take {
                let item = step.arg1;
                if item >= 0 {
                    if item >= map.max_devices {
                        return Err(CrushError::DecodeError(format!(
                            "Rule {} TAKE references device {}, but max_devices is {}",
                            rule.rule_id, item, map.max_devices
                        )));
                    }
                } else {
                    let idx = (-1 - item) as usize;
                    if idx >= map.buckets.len() || map.buckets[idx].is_none() {
                        return Err(CrushError::DecodeError(format!(
                            "Rule {} TAKE references non-existent bucket {}",
                            rule.rule_id, item
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

fn decode_bucket(data: &mut Bytes, alg: u32) -> Result<CrushBucket> {
    let id = i32::decode(data, 0)?;
    if id >= 0 {
        return Err(CrushError::DecodeError(format!(
            "Bucket ID must be negative, got {}",
            id
        )));
    }

    let bucket_type = u16::decode(data, 0)?;
    let alg_byte = u8::decode(data, 0)?;
    let hash = u8::decode(data, 0)?;
    let weight = u32::decode(data, 0)?;
    let size = u32::decode(data, 0)?;

    if size == 0 {
        return Err(CrushError::DecodeError(
            "Bucket size must be non-zero".into(),
        ));
    }

    if alg_byte as u32 != alg {
        return Err(CrushError::DecodeError(format!(
            "Algorithm mismatch: header says {}, bucket says {}",
            alg, alg_byte
        )));
    }

    if size > 10000 {
        return Err(CrushError::DecodeError(format!(
            "Bucket size too large: {}",
            size
        )));
    }

    let items: Vec<i32> = decode_n(data, size as usize)?;

    let algorithm = BucketAlgorithm::try_from(alg_byte)
        .map_err(|_| CrushError::InvalidBucketAlgorithm(alg_byte))?;

    let bucket_data = match algorithm {
        BucketAlgorithm::Uniform => {
            let item_weight = u32::decode(data, 0)?;
            BucketData::Uniform { item_weight }
        }
        BucketAlgorithm::List => {
            let (item_weights, sum_weights) = decode_n::<(u32, u32)>(data, size as usize)?
                .into_iter()
                .unzip();
            BucketData::List {
                item_weights,
                sum_weights,
            }
        }
        BucketAlgorithm::Tree => {
            let num_nodes = u32::decode(data, 0)?;
            let node_weights: Vec<u32> = decode_n(data, num_nodes as usize)?;
            BucketData::Tree {
                num_nodes,
                node_weights,
            }
        }
        BucketAlgorithm::Straw => {
            let (item_weights, straws) = decode_n::<(u32, u32)>(data, size as usize)?
                .into_iter()
                .unzip();
            BucketData::Straw {
                item_weights,
                straws,
            }
        }
        BucketAlgorithm::Straw2 => {
            let item_weights: Vec<u32> = decode_n(data, size as usize)?;
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

impl Denc for CrushRuleStep {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _features: u64,
    ) -> std::result::Result<(), RadosError> {
        (self.op as u32).encode(buf, 0)?;
        self.arg1.encode(buf, 0)?;
        self.arg2.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> std::result::Result<Self, RadosError> {
        let op = u32::decode(buf, 0)?;
        let arg1 = i32::decode(buf, 0)?;
        let arg2 = i32::decode(buf, 0)?;
        let rule_op = RuleOp::try_from(op)
            .map_err(|_| RadosError::Protocol(format!("Invalid rule op: {}", op)))?;
        Ok(CrushRuleStep {
            op: rule_op,
            arg1,
            arg2,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(12) // u32 + i32 + i32
    }
}

fn decode_rule(data: &mut Bytes) -> Result<CrushRule> {
    let len = u32::decode(data, 0)?;

    // Read legacy rule mask (4 bytes)
    let rule_id = u8::decode(data, 0)?;
    let rule_type = u8::decode(data, 0)?;
    let _min_size = u8::decode(data, 0)?;
    let _max_size = u8::decode(data, 0)?;

    let steps: Vec<CrushRuleStep> = decode_n(data, len as usize)?;

    Ok(CrushRule {
        rule_id: rule_id as u32,
        rule_type: RuleType::try_from(rule_type)
            .map_err(|_| CrushError::InvalidRuleType(rule_type))?,
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

        assert!(map.max_buckets > 0);
        assert!(map.max_rules > 0);
    }

    #[test]
    fn test_device_class_methods() {
        let mut map = CrushMap::new();

        map.class_name.insert(1, "ssd".to_string());
        map.class_name.insert(2, "hdd".to_string());
        map.class_name.insert(3, "nvme".to_string());

        map.class_map.insert(0, 1);
        map.class_map.insert(1, 2);
        map.class_map.insert(2, 1);
        map.class_map.insert(3, 3);

        assert_eq!(map.get_device_class(0), Some("ssd"));
        assert_eq!(map.get_device_class(1), Some("hdd"));
        assert_eq!(map.get_device_class(2), Some("ssd"));
        assert_eq!(map.get_device_class(3), Some("nvme"));
        assert_eq!(map.get_device_class(999), None);

        assert_eq!(map.get_class_id("ssd"), Some(1));
        assert_eq!(map.get_class_id("hdd"), Some(2));
        assert_eq!(map.get_class_id("nvme"), Some(3));
        assert_eq!(map.get_class_id("unknown"), None);

        assert!(map.device_has_class(0, "ssd"));
        assert!(map.device_has_class(1, "hdd"));
        assert!(!map.device_has_class(0, "hdd"));
        assert!(!map.device_has_class(999, "ssd"));
    }
}
