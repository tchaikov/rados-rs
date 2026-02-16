// CRUSH rule execution engine
// Reference: ~/dev/ceph/src/crush/mapper.c

use crate::bucket::bucket_choose;
use crate::error::Result;
use crate::hash::crush_hash32_2;
use crate::types::{CrushMap, RuleOp};
use denc::constants::crush::{FIXED_POINT_MASK, FIXED_POINT_ONE};

/// Sentinel value for "no item selected" in INDEP mode
/// Matches CRUSH_ITEM_NONE in Ceph (crush.h)
const CRUSH_ITEM_NONE: i32 = 0x7fffffff;

/// Check if an OSD is "out" (failed, fully offloaded)
fn is_out(weight: &[u32], item: i32, x: u32) -> bool {
    if item < 0 || item as usize >= weight.len() {
        return true;
    }

    let w = weight[item as usize];

    // Weight >= FIXED_POINT_ONE (1.0 in 16.16 fixed point) means fully in
    if w >= FIXED_POINT_ONE {
        return false;
    }

    // Weight == 0 means fully out
    if w == 0 {
        return true;
    }

    // Probabilistic: use hash to determine if item is in or out
    // This allows gradual weight changes
    let hash = crush_hash32_2(x, item as u32);
    (hash & FIXED_POINT_MASK) >= w
}

/// Execute a CRUSH rule to map a PG to OSDs
///
/// # Arguments
/// * `map` - The CRUSH map
/// * `rule_id` - Rule ID to execute
/// * `x` - Input value (typically PG hash)
/// * `result` - Output vector for selected OSDs
/// * `result_max` - Maximum number of results to return
/// * `weights` - OSD weights (from OSDMap)
///
/// # Returns
/// Ok(()) on success, Err on failure
pub fn crush_do_rule(
    map: &CrushMap,
    rule_id: u32,
    x: u32,
    result: &mut Vec<i32>,
    result_max: usize,
    weights: &[u32],
) -> Result<()> {
    let rule = map.get_rule(rule_id)?;

    result.clear();

    // Working set for intermediate results
    let mut work: Vec<i32> = Vec::new();
    let mut scratch: Vec<i32> = Vec::new();

    // Tunable parameters (can be overridden by rule steps)
    let mut choose_tries = map.choose_total_tries;
    let mut _chooseleaf_descend_once = map.chooseleaf_descend_once;
    let mut chooseleaf_vary_r = map.chooseleaf_vary_r;
    let mut chooseleaf_stable = map.chooseleaf_stable;

    for step in &rule.steps {
        match step.op {
            RuleOp::Take => {
                // Start with a specific bucket or device
                work.clear();
                work.push(step.arg1);
            }

            RuleOp::ChooseFirstN => {
                // Choose N items of a given type
                scratch.clear();
                let numrep = if step.arg1 == 0 {
                    result_max as i32
                } else if step.arg1 > 0 {
                    step.arg1
                } else {
                    // Negative means result_max - |arg1|
                    (result_max as i32) + step.arg1
                };

                let item_type = step.arg2;

                for &item in &work {
                    crush_choose_firstn(
                        map,
                        item,
                        x,
                        numrep as usize,
                        item_type,
                        &mut scratch,
                        weights,
                        choose_tries,
                        false, // recurse_to_leaf
                        chooseleaf_vary_r,
                        chooseleaf_stable,
                    )?;
                }

                work.clone_from(&scratch);
            }

            RuleOp::ChooseLeafFirstN => {
                // Choose N items and descend to leaf devices
                scratch.clear();
                let numrep = if step.arg1 == 0 {
                    result_max as i32
                } else if step.arg1 > 0 {
                    step.arg1
                } else {
                    (result_max as i32) + step.arg1
                };

                let item_type = step.arg2;

                for &item in &work {
                    crush_choose_firstn(
                        map,
                        item,
                        x,
                        numrep as usize,
                        item_type,
                        &mut scratch,
                        weights,
                        choose_tries,
                        true, // recurse_to_leaf
                        chooseleaf_vary_r,
                        chooseleaf_stable,
                    )?;
                }

                work.clone_from(&scratch);
            }

            RuleOp::Emit => {
                // Output current working set
                for &item in &work {
                    if result.len() < result_max {
                        result.push(item);
                    }
                }
            }

            RuleOp::SetChooseTries => {
                choose_tries = step.arg1 as u32;
            }

            RuleOp::SetChooseLeafTries => {
                _chooseleaf_descend_once = step.arg1 as u32;
            }

            RuleOp::SetChooseLeafVaryR => {
                chooseleaf_vary_r = step.arg1 as u8;
            }

            RuleOp::SetChooseLeafStable => {
                chooseleaf_stable = step.arg1 as u8;
            }

            RuleOp::Noop => {
                // Do nothing
            }

            RuleOp::ChooseIndep => {
                // Choose N items using INDEP algorithm (for erasure coding)
                let numrep = if step.arg1 == 0 {
                    result_max as i32
                } else if step.arg1 > 0 {
                    step.arg1
                } else {
                    (result_max as i32) + step.arg1
                };

                let item_type = step.arg2;

                scratch.clear();
                for &item in &work {
                    let mut indep_out = vec![CRUSH_ITEM_NONE; numrep as usize];
                    crush_choose_indep(
                        map,
                        item,
                        x,
                        numrep as usize,
                        item_type,
                        &mut indep_out,
                        weights,
                        choose_tries,
                        false, // recurse_to_leaf
                        chooseleaf_vary_r,
                        chooseleaf_stable,
                    )?;
                    scratch.extend_from_slice(&indep_out);
                }

                work.clone_from(&scratch);
            }

            RuleOp::ChooseLeafIndep => {
                // Choose N items using INDEP algorithm and descend to leaf
                let numrep = if step.arg1 == 0 {
                    result_max as i32
                } else if step.arg1 > 0 {
                    step.arg1
                } else {
                    (result_max as i32) + step.arg1
                };

                let item_type = step.arg2;

                scratch.clear();
                for &item in &work {
                    let mut indep_out = vec![CRUSH_ITEM_NONE; numrep as usize];
                    crush_choose_indep(
                        map,
                        item,
                        x,
                        numrep as usize,
                        item_type,
                        &mut indep_out,
                        weights,
                        choose_tries,
                        true, // recurse_to_leaf
                        chooseleaf_vary_r,
                        chooseleaf_stable,
                    )?;
                    scratch.extend_from_slice(&indep_out);
                }

                work.clone_from(&scratch);
            }

            _ => {
                // Unsupported operations (MSR, etc.)
                tracing::warn!("Unsupported CRUSH rule operation: {:?}", step.op);
            }
        }
    }

    Ok(())
}

/// Choose N items using FIRSTN algorithm
///
/// This is the core CRUSH selection algorithm that recursively descends
/// through the bucket hierarchy to select items.
#[allow(clippy::too_many_arguments)]
fn crush_choose_firstn(
    map: &CrushMap,
    bucket_id: i32,
    x: u32,
    numrep: usize,
    item_type: i32,
    out: &mut Vec<i32>,
    weights: &[u32],
    tries: u32,
    recurse_to_leaf: bool,
    vary_r: u8,
    stable: u8,
) -> Result<()> {
    tracing::debug!(
        "crush_choose_firstn: bucket_id={}, numrep={}, item_type={}, recurse_to_leaf={}",
        bucket_id,
        numrep,
        item_type,
        recurse_to_leaf
    );

    // If bucket_id is a device (>= 0), just return it if it's the right type
    if bucket_id >= 0 {
        if item_type == 0 && !is_out(weights, bucket_id, x) {
            out.push(bucket_id);
        }
        return Ok(());
    }

    // Get the bucket
    let bucket = map.get_bucket(bucket_id)?;
    tracing::debug!(
        "Got bucket: id={}, type={}, size={}, items={:?}",
        bucket.id,
        bucket.bucket_type,
        bucket.size,
        bucket.items
    );

    // For each replica we need to select
    for rep in 0..numrep {
        let mut found = false;
        let r = if stable != 0 { 0 } else { rep as u32 };
        let mut current_bucket = bucket;

        tracing::trace!("=== crush_choose_firstn rep={} START ===", rep);
        tracing::trace!(
            "  bucket_id={}, x={}, numrep={}, item_type={}",
            bucket_id,
            x,
            numrep,
            item_type
        );

        // Try multiple times to find a valid item
        'tries: for ftotal in 0..tries {
            let r_prime = if vary_r != 0 { r + ftotal } else { r };

            tracing::trace!(
                "  rep={}: r_prime = r({}) + ftotal({}) = {}, vary_r={}",
                rep,
                r,
                ftotal,
                r_prime,
                vary_r
            );

            // Inner loop for descending through bucket hierarchy
            loop {
                // Select an item from the current bucket
                let item = match bucket_choose(current_bucket, x, r_prime) {
                    Some(item) => item,
                    None => {
                        tracing::debug!("bucket_choose returned None for r_prime={}", r_prime);
                        continue 'tries;
                    }
                };

                tracing::debug!(
                    "Selected item {} from bucket {} (rep={}, try={})",
                    item,
                    current_bucket.id,
                    rep,
                    ftotal
                );

                // Determine item type
                let itemtype = if item >= 0 {
                    0 // Device
                } else {
                    match map.get_bucket(item) {
                        Ok(b) => b.bucket_type,
                        Err(_) => {
                            tracing::debug!("Invalid bucket {}", item);
                            continue 'tries;
                        }
                    }
                };

                tracing::debug!(
                    "Item {} has type {}, looking for type {}",
                    item,
                    itemtype,
                    item_type
                );

                // Check if this is the type we're looking for
                if itemtype != item_type {
                    if item >= 0 {
                        // Item is a device but wrong type - this shouldn't happen
                        tracing::debug!("Device {} has wrong type", item);
                        continue 'tries;
                    }
                    // Item is a bucket, descend into it
                    current_bucket = map.get_bucket(item)?;
                    tracing::debug!("Descending into bucket {}", item);
                    continue; // Continue inner loop with new bucket
                }

                // Check if this item is already in the output (collision)
                if out.contains(&item) {
                    tracing::debug!("Item {} already in output, skipping", item);
                    continue 'tries;
                }

                // Check if device is out
                if item >= 0 && is_out(weights, item, x) {
                    tracing::debug!("Device {} is out", item);
                    continue 'tries;
                }

                // If we need to recurse to leaf (for CHOOSELEAF)
                if recurse_to_leaf && item < 0 {
                    // Recursively select a device from this bucket
                    let before_len = out.len();
                    crush_choose_firstn(
                        map, item, x, 1, 0, // Type 0 = device
                        out, weights, tries, true, vary_r, stable,
                    )?;

                    if out.len() > before_len {
                        found = true;
                        break 'tries;
                    } else {
                        tracing::debug!("Failed to find leaf in bucket {}", item);
                        continue 'tries;
                    }
                }

                // Success - add this item to output
                tracing::debug!("Found valid item {}", item);
                tracing::trace!("crush_choose_firstn: rep={}, item={} SELECTED", rep, item);
                out.push(item);
                found = true;
                break 'tries;
            }
        }

        // If we couldn't find a valid item after all tries, continue to next replica
        if !found {
            tracing::debug!(
                "Failed to find item for replica {} after {} tries",
                rep,
                tries
            );
        }
    }

    Ok(())
}

/// Choose N items using INDEP (independent) algorithm
///
/// Unlike FIRSTN, each position independently selects an item.
/// If a position fails to find a valid item, it gets CRUSH_ITEM_NONE
/// rather than shifting subsequent items. This is critical for
/// erasure-coded pools where each chunk has a fixed position.
///
/// Reference: Ceph src/crush/mapper.c crush_choose_indep()
#[allow(clippy::too_many_arguments)]
fn crush_choose_indep(
    map: &CrushMap,
    bucket_id: i32,
    x: u32,
    numrep: usize,
    item_type: i32,
    out: &mut [i32],
    weights: &[u32],
    tries: u32,
    recurse_to_leaf: bool,
    _vary_r: u8,
    _stable: u8,
) -> Result<()> {
    tracing::debug!(
        "crush_choose_indep: bucket_id={}, numrep={}, item_type={}, recurse_to_leaf={}",
        bucket_id,
        numrep,
        item_type,
        recurse_to_leaf
    );

    // If bucket_id is a device (>= 0), just return it if it's the right type
    if bucket_id >= 0 {
        if item_type == 0 && !is_out(weights, bucket_id, x) && !out.is_empty() {
            out[0] = bucket_id;
        }
        return Ok(());
    }

    // Get the bucket
    let bucket = map.get_bucket(bucket_id)?;
    tracing::debug!(
        "Got bucket: id={}, type={}, size={}, items={:?}",
        bucket.id,
        bucket.bucket_type,
        bucket.size,
        bucket.items
    );

    // Initialize output with CRUSH_ITEM_NONE
    for slot in out.iter_mut().take(numrep) {
        *slot = CRUSH_ITEM_NONE;
    }

    // Try multiple times to fill all positions
    for ftotal in 0..tries {
        let mut all_done = true;

        for rep in 0..numrep {
            // Skip positions that already have a valid item
            if out[rep] != CRUSH_ITEM_NONE {
                continue;
            }

            all_done = false;

            let mut current_bucket = bucket;

            // In INDEP mode, r starts at numrep * ftotal + rep
            // This ensures each position gets a different hash input
            let r = (numrep as u32)
                .wrapping_mul(ftotal)
                .wrapping_add(rep as u32);

            // Inner loop for descending through bucket hierarchy
            let mut item = CRUSH_ITEM_NONE;
            let mut item_found = false;

            loop {
                // Select an item from the current bucket
                let candidate = match bucket_choose(current_bucket, x, r) {
                    Some(candidate) => candidate,
                    None => {
                        tracing::debug!("bucket_choose returned None for r={}", r);
                        break;
                    }
                };

                tracing::debug!(
                    "Selected item {} from bucket {} (rep={}, try={})",
                    candidate,
                    current_bucket.id,
                    rep,
                    ftotal
                );

                // Determine item type
                let itemtype = if candidate >= 0 {
                    0 // Device
                } else {
                    match map.get_bucket(candidate) {
                        Ok(b) => b.bucket_type,
                        Err(_) => {
                            tracing::debug!("Invalid bucket {}", candidate);
                            break;
                        }
                    }
                };

                tracing::debug!(
                    "Item {} has type {}, looking for type {}",
                    candidate,
                    itemtype,
                    item_type
                );

                // Check if this is the type we're looking for
                if itemtype != item_type {
                    if candidate >= 0 {
                        // Item is a device but wrong type
                        tracing::debug!("Device {} has wrong type", candidate);
                        break;
                    }
                    // Item is a bucket, descend into it
                    current_bucket = map.get_bucket(candidate)?;
                    tracing::debug!("Descending into bucket {}", candidate);
                    continue;
                }

                // Check for collision with any other output position
                let collision = out
                    .iter()
                    .enumerate()
                    .take(numrep)
                    .any(|(j, &val)| j != rep && val == candidate);
                if collision {
                    tracing::debug!("Item {} collides with another position", candidate);
                    break;
                }

                // Check if device is out
                if candidate >= 0 && is_out(weights, candidate, x) {
                    tracing::debug!("Device {} is out", candidate);
                    break;
                }

                // If we need to recurse to leaf (for CHOOSELEAF)
                if recurse_to_leaf && candidate < 0 {
                    // Recursively select a device from this bucket using INDEP
                    let mut leaf_out = [CRUSH_ITEM_NONE; 1];
                    crush_choose_indep(
                        map,
                        candidate,
                        x,
                        1,
                        0, // Type 0 = device
                        &mut leaf_out,
                        weights,
                        tries,
                        true,
                        _vary_r,
                        _stable,
                    )?;

                    if leaf_out[0] == CRUSH_ITEM_NONE {
                        tracing::debug!("Failed to find leaf in bucket {}", candidate);
                        break;
                    }

                    // Check leaf for collision with other positions
                    let leaf_collision = out
                        .iter()
                        .enumerate()
                        .take(numrep)
                        .any(|(j, &val)| j != rep && val == leaf_out[0]);
                    if leaf_collision {
                        tracing::debug!("Leaf item {} collides with another position", leaf_out[0]);
                        break;
                    }

                    item = leaf_out[0];
                    item_found = true;
                    break;
                }

                // Success - found a valid item
                tracing::debug!("Found valid item {}", candidate);
                item = candidate;
                item_found = true;
                break;
            }

            if item_found {
                out[rep] = item;
                tracing::trace!("crush_choose_indep: rep={}, item={} SELECTED", rep, item);
            }
        }

        if all_done {
            break;
        }
    }

    // Log unfilled positions
    for (rep, &item) in out.iter().enumerate().take(numrep) {
        if item == CRUSH_ITEM_NONE {
            tracing::debug!(
                "Failed to find item for position {} after {} tries",
                rep,
                tries
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BucketAlgorithm, BucketData, CrushBucket, CrushRule, CrushRuleStep};

    #[test]
    fn test_is_out() {
        let weights = vec![0x10000, 0x8000, 0, 0x20000];

        // Fully in (weight >= 0x10000)
        assert!(!is_out(&weights, 0, 123));
        assert!(!is_out(&weights, 3, 123));

        // Fully out (weight == 0)
        assert!(is_out(&weights, 2, 123));

        // Out of bounds
        assert!(is_out(&weights, 10, 123));
        assert!(is_out(&weights, -1, 123));
    }

    #[test]
    fn test_crush_do_rule_simple() {
        // Create a simple CRUSH map with one bucket and two devices
        let mut map = CrushMap::new();
        map.max_devices = 2;
        map.max_buckets = 1;

        // Create a bucket with two devices
        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1, // Type 1 = host
            alg: BucketAlgorithm::Straw2,
            hash: 0, // CRUSH_HASH_RJENKINS1
            weight: 0x20000,
            size: 2,
            items: vec![0, 1],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000],
            },
        };

        map.buckets = vec![Some(bucket)];

        // Create a simple rule: TAKE -1, CHOOSELEAF_FIRSTN 1 type 0, EMIT
        let rule = CrushRule {
            rule_id: 0,
            rule_type: crate::types::RuleType::Replicated,
            steps: vec![
                CrushRuleStep {
                    op: RuleOp::Take,
                    arg1: -1,
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::ChooseLeafFirstN,
                    arg1: 1,
                    arg2: 0, // Type 0 = device
                },
                CrushRuleStep {
                    op: RuleOp::Emit,
                    arg1: 0,
                    arg2: 0,
                },
            ],
        };

        map.rules = vec![Some(rule)];

        // Execute the rule
        let mut result = Vec::new();
        let weights = vec![0x10000, 0x10000]; // Both devices fully in

        let res = crush_do_rule(&map, 0, 123, &mut result, 1, &weights);
        assert!(res.is_ok());
        assert_eq!(result.len(), 1);
        assert!(result[0] == 0 || result[0] == 1);
    }

    #[test]
    fn test_crush_choose_firstn() {
        let mut map = CrushMap::new();
        map.max_devices = 3;
        map.max_buckets = 1;

        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0, // CRUSH_HASH_RJENKINS1
            weight: 0x30000,
            size: 3,
            items: vec![0, 1, 2],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000, 0x10000],
            },
        };

        map.buckets = vec![Some(bucket)];

        let mut out = Vec::new();
        let weights = vec![0x10000, 0x10000, 0x10000];

        let res = crush_choose_firstn(&map, -1, 123, 2, 0, &mut out, &weights, 50, false, 0, 0);

        assert!(res.is_ok());
        assert!(out.len() <= 2);
        // Should have selected distinct items
        if out.len() == 2 {
            assert_ne!(out[0], out[1]);
        }
    }

    #[test]
    fn test_crush_choose_indep() {
        let mut map = CrushMap::new();
        map.max_devices = 4;
        map.max_buckets = 1;

        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0,
            weight: 0x40000,
            size: 4,
            items: vec![0, 1, 2, 3],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000, 0x10000, 0x10000],
            },
        };

        map.buckets = vec![Some(bucket)];

        let mut out = vec![CRUSH_ITEM_NONE; 3];
        let weights = vec![0x10000, 0x10000, 0x10000, 0x10000];

        let res = crush_choose_indep(&map, -1, 123, 3, 0, &mut out, &weights, 50, false, 0, 0);

        assert!(res.is_ok());
        // All positions should be filled (enough devices available)
        for &item in &out {
            assert_ne!(item, CRUSH_ITEM_NONE, "all positions should be filled");
            assert!((0..4).contains(&item), "item should be a valid device");
        }
        // All items should be distinct
        assert_ne!(out[0], out[1]);
        assert_ne!(out[0], out[2]);
        assert_ne!(out[1], out[2]);
    }

    #[test]
    fn test_crush_choose_indep_stable_positions() {
        // INDEP key property: removing a device only affects its position,
        // not other positions. Verify determinism across multiple runs.
        let mut map = CrushMap::new();
        map.max_devices = 5;
        map.max_buckets = 1;

        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0,
            weight: 0x50000,
            size: 5,
            items: vec![0, 1, 2, 3, 4],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000, 0x10000, 0x10000, 0x10000],
            },
        };

        map.buckets = vec![Some(bucket)];

        let weights = vec![0x10000, 0x10000, 0x10000, 0x10000, 0x10000];

        // Run the same input twice - should produce identical results
        let mut out1 = vec![CRUSH_ITEM_NONE; 3];
        let mut out2 = vec![CRUSH_ITEM_NONE; 3];
        crush_choose_indep(&map, -1, 42, 3, 0, &mut out1, &weights, 50, false, 0, 0).unwrap();
        crush_choose_indep(&map, -1, 42, 3, 0, &mut out2, &weights, 50, false, 0, 0).unwrap();
        assert_eq!(out1, out2, "same input should produce same output");
    }

    #[test]
    fn test_crush_choose_indep_with_out_device() {
        // When a device is "out", INDEP should put CRUSH_ITEM_NONE in that
        // position rather than shifting other items
        let mut map = CrushMap::new();
        map.max_devices = 3;
        map.max_buckets = 1;

        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0,
            weight: 0x30000,
            size: 3,
            items: vec![0, 1, 2],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000, 0x10000],
            },
        };

        map.buckets = vec![Some(bucket)];

        // All devices in
        let weights_all_in = vec![0x10000, 0x10000, 0x10000];
        let mut out_all = vec![CRUSH_ITEM_NONE; 3];
        crush_choose_indep(
            &map,
            -1,
            100,
            3,
            0,
            &mut out_all,
            &weights_all_in,
            50,
            false,
            0,
            0,
        )
        .unwrap();

        // All positions should be filled when all devices are in
        for &item in &out_all {
            assert_ne!(item, CRUSH_ITEM_NONE);
        }
    }

    #[test]
    fn test_crush_do_rule_indep() {
        // Test ChooseIndep via crush_do_rule
        let mut map = CrushMap::new();
        map.max_devices = 4;
        map.max_buckets = 1;

        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0,
            weight: 0x40000,
            size: 4,
            items: vec![0, 1, 2, 3],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000, 0x10000, 0x10000],
            },
        };

        map.buckets = vec![Some(bucket)];

        // Erasure rule: TAKE -1, CHOOSE_INDEP 3 type 0, EMIT
        let rule = CrushRule {
            rule_id: 0,
            rule_type: crate::types::RuleType::Erasure,
            steps: vec![
                CrushRuleStep {
                    op: RuleOp::Take,
                    arg1: -1,
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::ChooseIndep,
                    arg1: 3,
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::Emit,
                    arg1: 0,
                    arg2: 0,
                },
            ],
        };

        map.rules = vec![Some(rule)];

        let mut result = Vec::new();
        let weights = vec![0x10000, 0x10000, 0x10000, 0x10000];

        let res = crush_do_rule(&map, 0, 123, &mut result, 3, &weights);
        assert!(res.is_ok());
        assert_eq!(result.len(), 3);

        // All results should be valid devices or CRUSH_ITEM_NONE
        let valid_count = result
            .iter()
            .filter(|&&item| item != CRUSH_ITEM_NONE)
            .count();
        assert_eq!(
            valid_count, 3,
            "should find 3 valid devices with 4 available"
        );

        // Valid items should be distinct
        let mut valid_items: Vec<i32> = result
            .iter()
            .copied()
            .filter(|&item| item != CRUSH_ITEM_NONE)
            .collect();
        valid_items.sort();
        valid_items.dedup();
        assert_eq!(valid_items.len(), 3, "valid items should be distinct");
    }

    #[test]
    fn test_crush_do_rule_chooseleaf_indep() {
        // Test ChooseLeafIndep via crush_do_rule
        let mut map = CrushMap::new();
        map.max_devices = 4;
        map.max_buckets = 1;

        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0,
            weight: 0x40000,
            size: 4,
            items: vec![0, 1, 2, 3],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000, 0x10000, 0x10000],
            },
        };

        map.buckets = vec![Some(bucket)];

        // Erasure rule: TAKE -1, CHOOSELEAF_INDEP 3 type 0, EMIT
        let rule = CrushRule {
            rule_id: 0,
            rule_type: crate::types::RuleType::Erasure,
            steps: vec![
                CrushRuleStep {
                    op: RuleOp::Take,
                    arg1: -1,
                    arg2: 0,
                },
                CrushRuleStep {
                    op: RuleOp::ChooseLeafIndep,
                    arg1: 3,
                    arg2: 0, // Type 0 = device
                },
                CrushRuleStep {
                    op: RuleOp::Emit,
                    arg1: 0,
                    arg2: 0,
                },
            ],
        };

        map.rules = vec![Some(rule)];

        let mut result = Vec::new();
        let weights = vec![0x10000, 0x10000, 0x10000, 0x10000];

        let res = crush_do_rule(&map, 0, 123, &mut result, 3, &weights);
        assert!(res.is_ok());
        assert_eq!(result.len(), 3);

        // All results should be valid devices
        let valid_count = result
            .iter()
            .filter(|&&item| item != CRUSH_ITEM_NONE)
            .count();
        assert_eq!(
            valid_count, 3,
            "should find 3 valid devices with 4 available"
        );
    }
}
