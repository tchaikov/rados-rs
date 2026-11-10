// CRUSH map decoding implementation
// Reference: ~/dev/ceph/src/crush/CrushWrapper.cc (encode/decode functions)

use crate::denc::{Denc, RadosError};
use bytes::{Buf, BufMut, Bytes};
use std::collections::HashMap;

use crate::crush::error::{CrushError, Result};
use crate::crush::types::*;

// CRUSH magic number
const CRUSH_MAGIC: u32 = 0x00010000;

/// Decode N items without a length prefix.
///
/// Unlike `Vec<T>::decode()` which reads a u32 length prefix first,
/// CRUSH bucket items have their count in a separate `size` field.
/// `item_size` is the fixed encoded size, not the in-memory size of `T`.
fn decode_n<T: Denc>(buf: &mut impl Buf, count: usize, item_size: usize) -> Result<Vec<T>> {
    if count > buf.remaining() / item_size {
        return Err(CrushError::DecodeError(format!(
            "Item count {count} exceeds remaining {} bytes",
            buf.remaining()
        )));
    }
    let mut vec = Vec::with_capacity(count);
    for _ in 0..count {
        vec.push(T::decode(buf, 0)?);
    }
    Ok(vec)
}

impl CrushMap {
    /// Decode a CRUSH map from bytes
    ///
    /// This decodes the binary CRUSH map format used by Ceph.
    /// Reference: CrushWrapper::decode() in ~/dev/ceph/src/crush/CrushWrapper.cc
    pub fn decode(data: &mut Bytes) -> Result<Self> {
        let magic = u32::decode(data, 0)?;
        if magic != CRUSH_MAGIC {
            return Err(CrushError::DecodeError(format!(
                "Invalid CRUSH magic: 0x{magic:x}, expected 0x{CRUSH_MAGIC:x}"
            )));
        }

        let max_buckets = i32::decode(data, 0)?;
        let max_rules = u32::decode(data, 0)?;
        let max_devices = i32::decode(data, 0)?;

        if max_buckets < 0 || max_buckets as usize > data.remaining() / 4 {
            return Err(CrushError::DecodeError(format!(
                "Invalid bucket count: {max_buckets}"
            )));
        }
        if max_devices < 0 {
            return Err(CrushError::DecodeError(format!(
                "Invalid device count: {max_devices}"
            )));
        }

        let mut map = CrushMap::new();
        map.max_buckets = max_buckets;
        map.max_rules = max_rules;
        map.max_devices = max_devices;

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

        if max_rules as usize > data.remaining() / 4 {
            return Err(CrushError::DecodeError(format!(
                "Invalid rule count: {max_rules}"
            )));
        }
        map.rules = Vec::with_capacity(max_rules as usize);
        for rule_index in 0..max_rules {
            let exists = u32::decode(data, 0)?;
            if exists == 0 {
                map.rules.push(None);
                continue;
            }

            let rule = decode_rule(data)?;
            // NOTE: Ceph compares the u8 legacy ruleset with the full rule index.
            // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/CrushWrapper.cc#L3307-L3311
            if rule.rule_id != rule_index {
                return Err(CrushError::DecodeError(format!(
                    "Rule at index {rule_index} has ruleset ID {}",
                    rule.rule_id
                )));
            }
            map.rules.push(Some(rule));
        }

        // Decode name maps.
        //
        // We intentionally require the fixed int32_t encoding here. Ceph switched
        // CrushWrapper::encode() to int32_t in 2012, and Quincy+ monitors only
        // send that corrected form in live OSDMaps. The only incompatible inputs
        // are historical serialized CRUSH blobs produced by pre-fix encoders on
        // affected architectures; those artifacts are outside this project's
        // supported compatibility scope.
        map.type_names = HashMap::decode(data, 0)?;
        map.names = HashMap::decode(data, 0)?;
        map.rule_names = HashMap::decode(data, 0)?;

        // NOTE: Ceph requires each optional section to be complete once present.
        // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/CrushWrapper.cc#L3328-L3348
        if data.has_remaining() {
            map.choose_local_tries = u32::decode(data, 0)?;
            map.choose_local_fallback_tries = u32::decode(data, 0)?;
            map.choose_total_tries = u32::decode(data, 0)?;
        }
        if data.has_remaining() {
            map.chooseleaf_descend_once = u32::decode(data, 0)?;
        }
        if data.has_remaining() {
            map.chooseleaf_vary_r = u8::decode(data, 0)?;
        }
        if data.has_remaining() {
            // straw_calc_version (skip)
            let _ = u8::decode(data, 0)?;
        }
        if data.has_remaining() {
            map.allowed_bucket_algs = u32::decode(data, 0)?;
        }
        if data.has_remaining() {
            map.chooseleaf_stable = u8::decode(data, 0)?;
        }

        // Decode device classes (Luminous+) — one atomic optional section
        if data.remaining() > 0 {
            map.class_map = HashMap::decode(data, 0)?;
            map.class_name = HashMap::decode(data, 0)?;
            map.class_rname = map
                .class_name
                .iter()
                .map(|(&id, name)| (name.clone(), id))
                .collect();
            map.class_bucket = HashMap::decode(data, 0)?;
        }

        // Choose args are producer-provided CRUSH alternatives. Rust consumes
        // these fixtures; it does not implement a CRUSH map encoder.
        if data.remaining() > 0 {
            let choose_args_size = u32::decode(data, 0)?;
            if choose_args_size as usize > data.remaining() / 12 {
                return Err(CrushError::DecodeError(format!(
                    "Too many choose-argument sets: {choose_args_size}"
                )));
            }
            for _ in 0..choose_args_size {
                let choose_args_index = i64::decode(data, 0)?;
                let size = u32::decode(data, 0)?;
                if size > map.buckets.len() as u32 || size as usize > data.remaining() / 12 {
                    return Err(CrushError::DecodeError(format!(
                        "Too many choose-argument buckets: {size}"
                    )));
                }
                let mut args = vec![None; map.buckets.len()];
                for _ in 0..size {
                    let bucket_index = u32::decode(data, 0)? as usize;
                    let bucket = map.buckets.get(bucket_index).ok_or_else(|| {
                        CrushError::DecodeError(format!(
                            "Choose argument references invalid bucket index {bucket_index}"
                        ))
                    })?;
                    let weight_set_positions = u32::decode(data, 0)?;
                    if weight_set_positions as usize > data.remaining() / 4 {
                        return Err(CrushError::DecodeError(format!(
                            "Too many choose-argument positions: {weight_set_positions}"
                        )));
                    }
                    let mut weight_set = Vec::with_capacity(weight_set_positions as usize);
                    for _ in 0..weight_set_positions {
                        let ws_size = u32::decode(data, 0)?;
                        weight_set.push(decode_n(data, ws_size as usize, 4)?);
                    }
                    let ids_size = u32::decode(data, 0)?;
                    if ids_size != 0 && bucket.as_ref().is_none_or(|bucket| ids_size != bucket.size)
                    {
                        return Err(CrushError::DecodeError(format!(
                            "Choose argument ID length {ids_size} does not match bucket index {bucket_index}"
                        )));
                    }
                    let ids = decode_n(data, ids_size as usize, 4)?;
                    args[bucket_index] = Some(CrushChooseArg { weight_set, ids });
                }
                // NOTE: Ceph infers the position count before removing stale
                // arguments and skips normalization when position counts differ.
                // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/CrushWrapper.cc#L442-L513
                let positions = args
                    .iter()
                    .flatten()
                    .map(|arg| arg.weight_set.len())
                    .find(|&positions| positions != 0)
                    .unwrap_or(1);
                for (arg, bucket) in args.iter_mut().zip(&map.buckets) {
                    let Some(bucket) = bucket
                        .as_ref()
                        .filter(|bucket| bucket.alg == BucketAlgorithm::Straw2)
                    else {
                        *arg = None;
                        continue;
                    };
                    let Some(arg) = arg else {
                        continue;
                    };
                    let normalize = arg.weight_set.len() == positions;
                    for weights in &mut arg.weight_set {
                        if normalize {
                            weights.resize(bucket.size as usize, 0);
                        } else if weights.len() != bucket.size as usize {
                            return Err(CrushError::DecodeError(format!(
                                "Choose argument weight length {} does not match bucket {} size {}",
                                weights.len(),
                                bucket.id,
                                bucket.size
                            )));
                        }
                    }
                }
                map.choose_args.insert(choose_args_index, args);
            }
        }

        // Decode MSR tunables (Reef+) — comes after choose_args on the wire.
        // C++ default (set_default_msr_tunables): 100/100.
        if data.has_remaining() {
            map.msr_descents = u32::decode(data, 0)?;
            map.msr_collision_tries = u32::decode(data, 0)?;
        }

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
/// - Bucket references are acyclic
fn validate_crush_map(map: &CrushMap) -> Result<()> {
    // Shared check: item must be a valid device index or an occupied bucket slot.
    let validate_item_ref = |item: i32, context: &str| -> Result<()> {
        if item >= 0 {
            if item >= map.max_devices {
                return Err(CrushError::DecodeError(format!(
                    "{} references device {}, but max_devices is {}",
                    context, item, map.max_devices
                )));
            }
        } else {
            let idx = (-1 - item) as usize;
            if idx >= map.buckets.len() || map.buckets[idx].is_none() {
                return Err(CrushError::DecodeError(format!(
                    "{context} references non-existent bucket {item}"
                )));
            }
        }
        Ok(())
    };

    for (i, slot) in map.buckets.iter().enumerate() {
        let Some(bucket) = slot else {
            continue;
        };

        let expected_id = -1 - i as i32;
        if bucket.id != expected_id {
            return Err(CrushError::DecodeError(format!(
                "Bucket at index {} has id {}, expected {}",
                i, bucket.id, expected_id
            )));
        }

        for &item in &bucket.items {
            validate_item_ref(item, &format!("Bucket {}", bucket.id))?;
        }
    }

    for rule in map.rules.iter().flatten() {
        for step in &rule.steps {
            if step.op == RuleOp::Take {
                validate_item_ref(step.arg1, &format!("Rule {} TAKE", rule.rule_id))?;
            }
        }
    }

    // Iterative DFS avoids overflowing the call stack on deep hierarchies.
    let mut state = vec![0u8; map.buckets.len()];
    let mut stack = Vec::new();
    for root in 0..map.buckets.len() {
        if state[root] != 0 || map.buckets[root].is_none() {
            continue;
        }
        state[root] = 1;
        stack.push((root, 0));
        while let Some((index, next_item)) = stack.last_mut() {
            let bucket = map.buckets[*index].as_ref().unwrap();
            let Some(&item) = bucket.items.get(*next_item) else {
                state[*index] = 2;
                stack.pop();
                continue;
            };
            *next_item += 1;
            if item >= 0 {
                continue;
            }
            let child = (-1 - item) as usize;
            match state[child] {
                1 => {
                    return Err(CrushError::DecodeError(format!(
                        "Bucket hierarchy contains a cycle through {item}"
                    )));
                }
                0 => {
                    state[child] = 1;
                    stack.push((child, 0));
                }
                _ => {}
            }
        }
    }

    Ok(())
}

fn decode_bucket(data: &mut Bytes, alg: u32) -> Result<CrushBucket> {
    let id = i32::decode(data, 0)?;
    if id >= 0 {
        return Err(CrushError::DecodeError(format!(
            "Bucket ID must be negative, got {id}"
        )));
    }

    let bucket_type = u16::decode(data, 0)?;
    let alg_byte = u8::decode(data, 0)?;
    let hash = u8::decode(data, 0)?;
    let weight = u32::decode(data, 0)?;
    let size = u32::decode(data, 0)?;

    if alg_byte as u32 != alg {
        return Err(CrushError::DecodeError(format!(
            "Algorithm mismatch: header says {alg}, bucket says {alg_byte}"
        )));
    }

    if hash != 0 {
        return Err(CrushError::DecodeError(format!(
            "Unsupported bucket hash type: {hash}"
        )));
    }

    let items: Vec<i32> = decode_n(data, size as usize, 4)?;

    let algorithm = BucketAlgorithm::try_from(alg_byte)
        .map_err(|_| CrushError::InvalidBucketAlgorithm(alg_byte))?;

    let bucket_data = match algorithm {
        BucketAlgorithm::Uniform => {
            let item_weight = u32::decode(data, 0)?;
            BucketData::Uniform { item_weight }
        }
        BucketAlgorithm::List => {
            let (item_weights, sum_weights) = decode_n::<(u32, u32)>(data, size as usize, 8)?
                .into_iter()
                .unzip();
            BucketData::List {
                item_weights,
                sum_weights,
            }
        }
        BucketAlgorithm::Tree => {
            let num_nodes = u32::from(u8::decode(data, 0)?);
            let node_weights: Vec<u32> = decode_n(data, num_nodes as usize, 4)?;
            BucketData::Tree {
                num_nodes,
                node_weights,
            }
        }
        BucketAlgorithm::Straw => {
            let (item_weights, straws) = decode_n::<(u32, u32)>(data, size as usize, 8)?
                .into_iter()
                .unzip();
            BucketData::Straw {
                item_weights,
                straws,
            }
        }
        BucketAlgorithm::Straw2 => {
            let item_weights: Vec<u32> = decode_n(data, size as usize, 4)?;
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
            .map_err(|_| RadosError::Protocol(format!("Invalid rule op: {op}")))?;
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

    // Legacy rule mask (4 bytes)
    let rule_id = u8::decode(data, 0)?;
    let rule_type = u8::decode(data, 0)?;
    let _min_size = u8::decode(data, 0)?;
    let _max_size = u8::decode(data, 0)?;

    let steps: Vec<CrushRuleStep> = decode_n(data, len as usize, 12)?;

    Ok(CrushRule {
        rule_id: rule_id as u32,
        rule_type: RuleType::try_from(rule_type)
            .map_err(|_| CrushError::InvalidRuleType(rule_type))?,
        steps,
    })
}

#[cfg(test)]
#[path = "tests/decode_regressions.rs"]
mod decode_regressions;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    #[ignore = "requires local ceph-object-corpus checkout"]
    fn test_decode_crushmap_corpus() {
        let corpus_path = "/home/kefu/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/CrushWrapper/f43a6ecc6a266d1485e06427c1c79aac";

        if !Path::new(corpus_path).exists() {
            println!("Corpus file not found, skipping test");
            return;
        }

        let data = fs::read(corpus_path).expect("Failed to read corpus file");
        let mut bytes = Bytes::from(data);

        println!("Total bytes: {}", bytes.len());

        let result = CrushMap::decode(&mut bytes);
        if let Err(ref e) = result {
            println!("Decode error: {e:?}");
            println!("Remaining bytes: {}", bytes.remaining());
        }
        assert!(result.is_ok(), "Failed to decode CRUSH map: {result:?}");

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
        map.class_rname.insert("ssd".to_string(), 1);
        map.class_rname.insert("hdd".to_string(), 2);
        map.class_rname.insert("nvme".to_string(), 3);

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
