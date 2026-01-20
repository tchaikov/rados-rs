// CRUSH map decoding implementation
// Reference: ~/dev/ceph/src/crush/CrushWrapper.cc (encode/decode functions)

use bytes::{Buf, Bytes};
use std::collections::HashMap;

use crate::error::{CrushError, Result};
use crate::types::*;

// CRUSH magic number
const CRUSH_MAGIC: u32 = 0x00010000;

impl CrushMap {
    /// Decode a CRUSH map from bytes
    ///
    /// This decodes the binary CRUSH map format used by Ceph.
    /// Reference: CrushWrapper::decode() in ~/dev/ceph/src/crush/CrushWrapper.cc
    pub fn decode(data: &mut Bytes) -> Result<Self> {
        // Read magic number
        if data.remaining() < 4 {
            return Err(CrushError::DecodeError(
                "Not enough data for magic number".to_string(),
            ));
        }
        let magic = data.get_u32_le();
        if magic != CRUSH_MAGIC {
            return Err(CrushError::DecodeError(format!(
                "Invalid CRUSH magic: 0x{:x}, expected 0x{:x}",
                magic, CRUSH_MAGIC
            )));
        }

        // Read header
        let max_buckets = data.get_i32_le();
        let max_rules = data.get_u32_le();
        let max_devices = data.get_i32_le();

        let mut map = CrushMap::new();
        map.max_buckets = max_buckets;
        map.max_rules = max_rules;
        map.max_devices = max_devices;

        // Decode buckets
        map.buckets = Vec::with_capacity(max_buckets as usize);
        for i in 0..max_buckets {
            if data.remaining() < 4 {
                return Err(CrushError::DecodeError(format!(
                    "Not enough data for bucket {} algorithm",
                    i
                )));
            }
            let alg = data.get_u32_le();
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
            let exists = data.get_u32_le();
            if exists == 0 {
                map.rules.push(None);
                continue;
            }

            let rule = decode_rule(data)?;
            map.rules.push(Some(rule));
        }

        // Decode name maps
        map.type_names = decode_string_vec(data)?;
        let name_map = decode_i32_string_map(data)?;
        map.names = name_map;
        let rule_name_map = decode_u32_string_map(data)?;
        map.rule_names = rule_name_map;

        // Decode tunables
        if data.remaining() >= 4 {
            map.choose_local_tries = data.get_u32_le();
        }
        if data.remaining() >= 4 {
            map.choose_local_fallback_tries = data.get_u32_le();
        }
        if data.remaining() >= 4 {
            map.choose_total_tries = data.get_u32_le();
        }
        if data.remaining() >= 4 {
            map.chooseleaf_descend_once = data.get_u32_le();
        }
        if data.remaining() >= 1 {
            map.chooseleaf_vary_r = data.get_u8();
        }
        if data.remaining() >= 1 {
            // straw_calc_version (skip)
            let _ = data.get_u8();
        }
        if data.remaining() >= 4 {
            map.allowed_bucket_algs = data.get_u32_le();
        }
        if data.remaining() >= 1 {
            map.chooseleaf_stable = data.get_u8();
        }

        // Skip remaining data (device classes, choose args, etc.)
        // These are optional and not needed for basic functionality

        Ok(map)
    }
}

fn decode_bucket(data: &mut Bytes, alg: u32) -> Result<CrushBucket> {
    if data.remaining() < 18 {
        // Need at least: id(4) + type(4) + alg(1) + hash(1) + weight(4) + size(4)
        return Err(CrushError::DecodeError(format!(
            "Not enough data for bucket header: need 18, have {}",
            data.remaining()
        )));
    }

    let id = data.get_i32_le();
    let bucket_type = data.get_i32_le();
    let alg_byte = data.get_u8();
    let _hash = data.get_u8();
    let weight = data.get_u32_le();
    let size = data.get_u32_le();

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

    // Read items
    let items_bytes = size.checked_mul(4).ok_or_else(|| {
        CrushError::DecodeError(format!("Bucket size overflow: {}", size))
    })?;

    if data.remaining() < items_bytes as usize {
        return Err(CrushError::DecodeError(format!(
            "Not enough data for bucket items: need {}, have {}",
            items_bytes,
            data.remaining()
        )));
    }

    let mut items = Vec::with_capacity(size as usize);
    for _ in 0..size {
        items.push(data.get_i32_le());
    }

    let algorithm = BucketAlgorithm::try_from(alg_byte)?;

    // Decode algorithm-specific data
    let bucket_data = match algorithm {
        BucketAlgorithm::Uniform => {
            let item_weight = data.get_u32_le();
            BucketData::Uniform { item_weight }
        }
        BucketAlgorithm::List => {
            let mut item_weights = Vec::with_capacity(size as usize);
            let mut sum_weights = Vec::with_capacity(size as usize);
            for _ in 0..size {
                item_weights.push(data.get_u32_le());
                sum_weights.push(data.get_u32_le());
            }
            BucketData::List {
                item_weights,
                sum_weights,
            }
        }
        BucketAlgorithm::Tree => {
            let num_nodes = data.get_u32_le();
            let mut node_weights = Vec::with_capacity(num_nodes as usize);
            for _ in 0..num_nodes {
                node_weights.push(data.get_u32_le());
            }
            BucketData::Tree {
                num_nodes,
                node_weights,
            }
        }
        BucketAlgorithm::Straw => {
            let mut item_weights = Vec::with_capacity(size as usize);
            let mut straws = Vec::with_capacity(size as usize);
            for _ in 0..size {
                item_weights.push(data.get_u32_le());
                straws.push(data.get_u32_le());
            }
            BucketData::Straw {
                item_weights,
                straws,
            }
        }
        BucketAlgorithm::Straw2 => {
            let mut item_weights = Vec::with_capacity(size as usize);
            for _ in 0..size {
                item_weights.push(data.get_u32_le());
            }
            BucketData::Straw2 { item_weights }
        }
    };

    Ok(CrushBucket {
        id,
        bucket_type,
        alg: algorithm,
        weight,
        size,
        items,
        data: bucket_data,
    })
}

fn decode_rule(data: &mut Bytes) -> Result<CrushRule> {
    let len = data.get_u32_le();

    // Read legacy rule mask (4 bytes)
    let rule_id = data.get_u8() as u32; // ruleset (now rule_id)
    let rule_type = data.get_u8();
    let _min_size = data.get_u8(); // deprecated
    let _max_size = data.get_u8(); // deprecated

    let mut steps = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let op = data.get_u32_le();
        let arg1 = data.get_i32_le();
        let arg2 = data.get_i32_le();

        let rule_op = RuleOp::try_from(op)?;
        steps.push(CrushRuleStep {
            op: rule_op,
            arg1,
            arg2,
        });
    }

    Ok(CrushRule {
        rule_id,
        rule_type: RuleType::from(rule_type),
        steps,
    })
}

fn decode_string_vec(data: &mut Bytes) -> Result<Vec<String>> {
    if data.remaining() < 4 {
        return Ok(Vec::new());
    }
    let len = data.get_u32_le();
    let mut vec = Vec::with_capacity(len as usize);

    for _ in 0..len {
        if data.remaining() < 4 {
            break;
        }
        let str_len = data.get_u32_le();
        if str_len > 0 {
            if data.remaining() < str_len as usize {
                return Err(CrushError::DecodeError(format!(
                    "Not enough data for string: need {}, have {}",
                    str_len,
                    data.remaining()
                )));
            }
            let mut bytes = vec![0u8; str_len as usize];
            data.copy_to_slice(&mut bytes);
            let s = String::from_utf8(bytes)
                .map_err(|e| CrushError::DecodeError(format!("Invalid UTF-8: {}", e)))?;
            vec.push(s);
        } else {
            vec.push(String::new());
        }
    }

    Ok(vec)
}

fn decode_i32_string_map(data: &mut Bytes) -> Result<HashMap<i32, String>> {
    if data.remaining() < 4 {
        return Ok(HashMap::new());
    }
    let len = data.get_u32_le();
    let mut map = HashMap::with_capacity(len as usize);

    for _ in 0..len {
        if data.remaining() < 8 {
            break;
        }
        let key = data.get_i32_le();
        let str_len = data.get_u32_le();
        if data.remaining() < str_len as usize {
            return Err(CrushError::DecodeError(format!(
                "Not enough data for string: need {}, have {}",
                str_len,
                data.remaining()
            )));
        }
        let mut bytes = vec![0u8; str_len as usize];
        data.copy_to_slice(&mut bytes);
        let value = String::from_utf8(bytes)
            .map_err(|e| CrushError::DecodeError(format!("Invalid UTF-8: {}", e)))?;
        map.insert(key, value);
    }

    Ok(map)
}

fn decode_u32_string_map(data: &mut Bytes) -> Result<HashMap<u32, String>> {
    if data.remaining() < 4 {
        return Ok(HashMap::new());
    }
    let len = data.get_u32_le();
    let mut map = HashMap::with_capacity(len as usize);

    for _ in 0..len {
        if data.remaining() < 8 {
            break;
        }
        let key = data.get_u32_le();
        let str_len = data.get_u32_le();
        if data.remaining() < str_len as usize {
            return Err(CrushError::DecodeError(format!(
                "Not enough data for string: need {}, have {}",
                str_len,
                data.remaining()
            )));
        }
        let mut bytes = vec![0u8; str_len as usize];
        data.copy_to_slice(&mut bytes);
        let value = String::from_utf8(bytes)
            .map_err(|e| CrushError::DecodeError(format!("Invalid UTF-8: {}", e)))?;
        map.insert(key, value);
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
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
        println!("  buckets: {}", map.buckets.iter().filter(|b| b.is_some()).count());
        println!("  rules: {}", map.rules.iter().filter(|r| r.is_some()).count());

        // Verify we decoded something reasonable
        assert!(map.max_buckets > 0);
        assert!(map.max_rules > 0);
    }
}
