// CRUSH rule execution engine
// Reference: ~/dev/ceph/src/crush/mapper.c

use crate::bucket::bucket_choose;
use crate::error::Result;
use crate::hash::crush_hash32_2;
use crate::types::{CrushMap, RuleOp};

/// Check if an OSD is "out" (failed, fully offloaded)
fn is_out(weight: &[u32], item: i32, x: u32) -> bool {
    if item < 0 || item as usize >= weight.len() {
        return true;
    }

    let w = weight[item as usize];

    // Weight >= 0x10000 (1.0 in 16.16 fixed point) means fully in
    if w >= 0x10000 {
        return false;
    }

    // Weight == 0 means fully out
    if w == 0 {
        return true;
    }

    // Probabilistic: use hash to determine if item is in or out
    // This allows gradual weight changes
    let hash = crush_hash32_2(x, item as u32);
    (hash & 0xffff) >= w
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

            _ => {
                // Unsupported operations (INDEP, MSR, etc.)
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

        eprintln!("RUST_CRUSH: === crush_choose_firstn rep={} START ===", rep);
        eprintln!(
            "RUST_CRUSH:   bucket_id={}, x={}, numrep={}, item_type={}",
            bucket_id, x, numrep, item_type
        );

        // Try multiple times to find a valid item
        'tries: for ftotal in 0..tries {
            let r_prime = if vary_r != 0 { r + ftotal } else { r };

            eprintln!(
                "RUST_CRUSH:   rep={}: r_prime = r({}) + ftotal({}) = {}, vary_r={}",
                rep, r, ftotal, r_prime, vary_r
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
                eprintln!(
                    "RUST_CRUSH: crush_choose_firstn: rep={}, item={} SELECTED",
                    rep, item
                );
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
}
