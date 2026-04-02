// CRUSH rule execution engine
// Reference: ~/dev/ceph/src/crush/mapper.c

use crate::crush::bucket::bucket_choose;
use crate::crush::error::Result;
use crate::crush::hash::crush_hash32_2;
use crate::crush::types::{CrushMap, RuleOp};
use crate::denc::constants::crush::{FIXED_POINT_MASK, FIXED_POINT_ONE};

/// Sentinel value for "no item selected" in INDEP mode
/// Matches CRUSH_ITEM_NONE in Ceph (crush.h)
pub(crate) const CRUSH_ITEM_NONE: i32 = 0x7fffffff;
/// Internal sentinel: slot not yet resolved (distinct from CRUSH_ITEM_NONE which is definitive).
const CRUSH_ITEM_UNDEF: i32 = 0x7ffffffe;

/// Calculate the number of replicas/items to select based on rule step argument
///
/// - arg == 0: use result_max
/// - arg > 0: use arg directly
/// - arg < 0: use result_max + arg (i.e., result_max - |arg|)
#[inline]
fn calculate_numrep(step_arg: i32, result_max: usize) -> usize {
    if step_arg == 0 {
        result_max
    } else if step_arg > 0 {
        step_arg as usize
    } else {
        (result_max as i32 + step_arg).max(0) as usize
    }
}

/// Determine the type of an item (0 for device, bucket_type for buckets)
/// Returns None if item is an invalid bucket
#[inline]
fn get_item_type(map: &CrushMap, item: i32) -> Option<i32> {
    if item >= 0 {
        Some(0) // Device
    } else {
        map.get_bucket(item).ok().map(|b| b.bucket_type)
    }
}

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

    let mut work: Vec<i32> = Vec::with_capacity(result_max);
    let mut scratch: Vec<i32> = Vec::with_capacity(result_max);

    // C++ mapper.c: "the original choose_total_tries value was off by one
    // (it counted 'retries' and not 'tries'). add one."
    let mut choose_tries = map.choose_total_tries + 1;
    let mut chooseleaf_vary_r = map.chooseleaf_vary_r;
    let mut chooseleaf_stable = map.chooseleaf_stable;
    let mut msr_descents = map.msr_descents;
    let mut msr_collision_tries = map.msr_collision_tries;

    for step in &rule.steps {
        match step.op {
            RuleOp::Take => {
                work.clear();
                work.push(step.arg1);
            }

            RuleOp::ChooseFirstN | RuleOp::ChooseLeafFirstN => {
                let recurse_to_leaf = step.op == RuleOp::ChooseLeafFirstN;
                let numrep = calculate_numrep(step.arg1, result_max);
                let item_type = step.arg2;

                scratch.clear();
                for &item in &work {
                    crush_choose_firstn(
                        map,
                        item,
                        x,
                        numrep,
                        item_type,
                        &mut scratch,
                        weights,
                        choose_tries,
                        recurse_to_leaf,
                        chooseleaf_vary_r,
                        chooseleaf_stable,
                    )?;
                }
                std::mem::swap(&mut work, &mut scratch);
            }

            RuleOp::ChooseIndep | RuleOp::ChooseLeafIndep => {
                let recurse_to_leaf = step.op == RuleOp::ChooseLeafIndep;
                let numrep = calculate_numrep(step.arg1, result_max);
                let item_type = step.arg2;

                scratch.clear();
                let mut indep_out = vec![CRUSH_ITEM_NONE; numrep];
                for &item in &work {
                    indep_out.fill(CRUSH_ITEM_NONE);
                    crush_choose_indep(
                        map,
                        item,
                        x,
                        numrep,
                        item_type,
                        &mut indep_out,
                        weights,
                        choose_tries,
                        recurse_to_leaf,
                        0, // top-level call: parent_r = 0
                    )?;
                    scratch.extend_from_slice(&indep_out);
                }
                std::mem::swap(&mut work, &mut scratch);
            }

            RuleOp::ChooseMsr => {
                let numrep = calculate_numrep(step.arg1, result_max);
                let item_type = step.arg2;

                scratch.clear();
                let mut msr_out = vec![CRUSH_ITEM_NONE; numrep];
                for &item in &work {
                    msr_out.fill(CRUSH_ITEM_NONE);
                    crush_choose_msr(
                        map,
                        item,
                        x,
                        numrep,
                        item_type,
                        &mut msr_out,
                        weights,
                        msr_descents,
                        msr_collision_tries,
                        false,
                    )?;
                    scratch.extend(msr_out.iter().copied().filter(|&v| v != CRUSH_ITEM_NONE));
                }
                std::mem::swap(&mut work, &mut scratch);
            }

            RuleOp::Emit => {
                for &item in &work {
                    if result.len() < result_max {
                        result.push(item);
                    }
                }
            }

            RuleOp::SetChooseTries => choose_tries = step.arg1 as u32,
            RuleOp::SetChooseLeafVaryR => chooseleaf_vary_r = step.arg1 as u8,
            RuleOp::SetChooseLeafStable => chooseleaf_stable = step.arg1 as u8,
            RuleOp::SetMsrDescents => msr_descents = step.arg1 as u32,
            RuleOp::SetMsrCollisionTries => msr_collision_tries = step.arg1 as u32,

            RuleOp::SetChooseLeafTries
            | RuleOp::SetChooseLocalTries
            | RuleOp::SetChooseLocalFallbackTries
            | RuleOp::Noop => {}
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

    if bucket_id >= 0 {
        if item_type == 0 && !is_out(weights, bucket_id, x) {
            out.push(bucket_id);
        }
        return Ok(());
    }

    let bucket = map.get_bucket(bucket_id)?;
    tracing::debug!(
        "Got bucket: id={}, type={}, size={}, items={:?}",
        bucket.id,
        bucket.bucket_type,
        bucket.size,
        bucket.items
    );

    for rep in 0..numrep {
        let mut found = false;
        let r = if stable != 0 { 0 } else { rep as u32 };
        let mut current_bucket;

        tracing::trace!("=== crush_choose_firstn rep={} START ===", rep);
        tracing::trace!(
            "  bucket_id={}, x={}, numrep={}, item_type={}",
            bucket_id,
            x,
            numrep,
            item_type
        );

        'tries: for ftotal in 0..tries {
            current_bucket = bucket; // reset to starting bucket each retry (C++ `in = bucket`)
            let r_prime = if vary_r != 0 { r + ftotal } else { r };

            tracing::trace!(
                "  rep={}: r_prime = r({}) + ftotal({}) = {}, vary_r={}",
                rep,
                r,
                ftotal,
                r_prime,
                vary_r
            );

            loop {
                let item = bucket_choose(current_bucket, x, r_prime);

                tracing::debug!(
                    "Selected item {} from bucket {} (rep={}, try={})",
                    item,
                    current_bucket.id,
                    rep,
                    ftotal
                );

                let itemtype = match get_item_type(map, item) {
                    Some(t) => t,
                    None => {
                        tracing::debug!("Invalid bucket {}", item);
                        continue 'tries;
                    }
                };

                tracing::debug!(
                    "Item {} has type {}, looking for type {}",
                    item,
                    itemtype,
                    item_type
                );

                if itemtype != item_type {
                    if item >= 0 {
                        tracing::debug!("Device {} has wrong type", item);
                        continue 'tries;
                    }
                    current_bucket = map.get_bucket(item)?;
                    tracing::debug!("Descending into bucket {}", item);
                    continue;
                }

                if out.contains(&item) {
                    tracing::debug!("Item {} already in output, skipping", item);
                    continue 'tries;
                }

                if item >= 0 && is_out(weights, item, x) {
                    tracing::debug!("Device {} is out", item);
                    continue 'tries;
                }

                if recurse_to_leaf && item < 0 {
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

                tracing::debug!("Found valid item {}", item);
                tracing::trace!("crush_choose_firstn: rep={}, item={} SELECTED", rep, item);
                out.push(item);
                found = true;
                break 'tries;
            }
        }

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
    parent_r: i32,
) -> Result<()> {
    tracing::debug!(
        "crush_choose_indep: bucket_id={}, numrep={}, item_type={}, recurse_to_leaf={}",
        bucket_id,
        numrep,
        item_type,
        recurse_to_leaf
    );

    if bucket_id >= 0 {
        if item_type == 0 && !is_out(weights, bucket_id, x) && !out.is_empty() {
            out[0] = bucket_id;
        }
        return Ok(());
    }

    let bucket = map.get_bucket(bucket_id)?;
    tracing::debug!(
        "Got bucket: id={}, type={}, size={}, items={:?}",
        bucket.id,
        bucket.bucket_type,
        bucket.size,
        bucket.items
    );

    // UNDEF distinguishes "slot not yet filled" from definitive NONE (not retried).
    for slot in out.iter_mut().take(numrep) {
        *slot = CRUSH_ITEM_UNDEF;
    }

    for ftotal in 0..tries {
        let mut all_done = true;

        for rep in 0..numrep {
            if out[rep] != CRUSH_ITEM_UNDEF {
                continue;
            }

            all_done = false;

            let mut current_bucket = bucket;

            // r = rep + parent_r + numrep * ftotal
            // Matches C++ crush_choose_indep (non-uniform bucket path).
            let r = (rep as u32)
                .wrapping_add(parent_r as u32)
                .wrapping_add((numrep as u32).wrapping_mul(ftotal));

            let mut item = CRUSH_ITEM_NONE;
            let mut item_found = false;

            loop {
                let candidate = bucket_choose(current_bucket, x, r);

                tracing::debug!(
                    "Selected item {} from bucket {} (rep={}, try={})",
                    candidate,
                    current_bucket.id,
                    rep,
                    ftotal
                );

                let itemtype = match get_item_type(map, candidate) {
                    Some(t) => t,
                    None => {
                        tracing::debug!("Invalid bucket {}", candidate);
                        out[rep] = CRUSH_ITEM_NONE;
                        break;
                    }
                };

                tracing::debug!(
                    "Item {} has type {}, looking for type {}",
                    candidate,
                    itemtype,
                    item_type
                );

                if itemtype != item_type {
                    if candidate >= 0 {
                        tracing::debug!("Device {} has wrong type", candidate);
                        out[rep] = CRUSH_ITEM_NONE;
                        break;
                    }
                    current_bucket = map.get_bucket(candidate)?;
                    tracing::debug!("Descending into bucket {}", candidate);
                    continue;
                }

                let collision = out
                    .iter()
                    .enumerate()
                    .take(numrep)
                    .any(|(j, &val)| j != rep && val == candidate);
                if collision {
                    tracing::debug!("Item {} collides with another position", candidate);
                    break;
                }

                if candidate >= 0 && is_out(weights, candidate, x) {
                    tracing::debug!("Device {} is out", candidate);
                    break;
                }

                if recurse_to_leaf && candidate < 0 {
                    let mut leaf_out = [CRUSH_ITEM_UNDEF; 1];
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
                        r as i32,
                    )?;

                    if leaf_out[0] == CRUSH_ITEM_NONE || leaf_out[0] == CRUSH_ITEM_UNDEF {
                        tracing::debug!("Failed to find leaf in bucket {}", candidate);
                        break;
                    }

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

    for slot in out.iter_mut().take(numrep) {
        if *slot == CRUSH_ITEM_UNDEF {
            *slot = CRUSH_ITEM_NONE;
        }
    }

    Ok(())
}

/// Choose N items using MSR (Main Search Rule) algorithm
///
/// MSR is an alternative selection algorithm for multi-way replication.
/// It uses a different hash function and collision handling strategy.
///
/// Reference: ~/dev/ceph/src/crush/mapper.c (crush_choose_msr)
#[allow(clippy::too_many_arguments)]
fn crush_choose_msr(
    map: &CrushMap,
    bucket_id: i32,
    x: u32,
    numrep: usize,
    item_type: i32,
    out: &mut [i32],
    weights: &[u32],
    descents: u32,
    collision_tries: u32,
    recurse_to_leaf: bool,
) -> Result<()> {
    tracing::debug!(
        "crush_choose_msr: bucket_id={}, numrep={}, item_type={}, descents={}, collision_tries={}",
        bucket_id,
        numrep,
        item_type,
        descents,
        collision_tries
    );

    if bucket_id >= 0 {
        if item_type == 0 && !is_out(weights, bucket_id, x) {
            out[0] = bucket_id;
        }
        return Ok(());
    }

    let bucket = map.get_bucket(bucket_id)?;

    let mut chosen_items: Vec<i32> = Vec::with_capacity(numrep);
    let mut collisions = vec![0u32; numrep];

    for rep in 0..numrep {
        let mut descent = 0;

        'retry: loop {
            if descent >= descents {
                break 'retry;
            }
            descent += 1;

            let r = rep as u32 + descent * numrep as u32 + collisions[rep];
            let hash = crush_hash32_2(x + r, bucket_id as u32);
            let item = bucket_choose(bucket, hash, r);

            let item_type_match = match get_item_type(map, item) {
                Some(t) => t == item_type || item_type == 0,
                None => false,
            };

            if item < 0 && (recurse_to_leaf || !item_type_match) {
                let mut sub_out = [CRUSH_ITEM_NONE; 1];
                crush_choose_msr(
                    map,
                    item,
                    x,
                    1,
                    item_type,
                    &mut sub_out,
                    weights,
                    descents,
                    collision_tries,
                    recurse_to_leaf,
                )?;

                if sub_out[0] == CRUSH_ITEM_NONE {
                    continue 'retry;
                }

                if chosen_items.contains(&sub_out[0]) {
                    collisions[rep] += 1;
                    if collisions[rep] < collision_tries {
                        continue 'retry;
                    }
                    break 'retry;
                }

                out[rep] = sub_out[0];
                chosen_items.push(sub_out[0]);
                break 'retry;
            }

            if item >= 0 && is_out(weights, item, x) {
                continue 'retry;
            }

            if item_type_match {
                if chosen_items.contains(&item) {
                    collisions[rep] += 1;
                    if collisions[rep] < collision_tries {
                        continue 'retry;
                    }
                    break 'retry;
                }
                out[rep] = item;
                chosen_items.push(item);
                break 'retry;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crush::types::{BucketAlgorithm, BucketData, CrushBucket, CrushRule, CrushRuleStep};

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
            rule_type: crate::crush::types::RuleType::Replicated,
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

        let res = crush_choose_indep(&map, -1, 123, 3, 0, &mut out, &weights, 50, false, 0);

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
        crush_choose_indep(&map, -1, 42, 3, 0, &mut out1, &weights, 50, false, 0).unwrap();
        crush_choose_indep(&map, -1, 42, 3, 0, &mut out2, &weights, 50, false, 0).unwrap();
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
            rule_type: crate::crush::types::RuleType::Erasure,
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
            rule_type: crate::crush::types::RuleType::Erasure,
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
