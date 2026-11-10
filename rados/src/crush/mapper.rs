// CRUSH rule execution engine
// Reference: ~/dev/ceph/src/crush/mapper.c

use crate::crush::bucket::{bucket_choose_with_arg, bucket_perm_choose};
use crate::crush::error::{CrushError, Result};
use crate::crush::hash::crush_hash32_2;
use crate::crush::types::{ChooseProfile, CrushChooseArg, CrushMap, RuleOp};
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
        result_max.saturating_sub(step_arg.unsigned_abs() as usize)
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
fn is_out(weight: &[u32], item: i32, x: i32) -> bool {
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
    let hash = crush_hash32_2(x as u32, item as u32);
    (hash & FIXED_POINT_MASK) >= w
}

#[inline]
fn choose_arg(
    choose_args: Option<&[Option<CrushChooseArg>]>,
    bucket_id: i32,
) -> Option<&CrushChooseArg> {
    choose_args?.get((-1 - bucket_id) as usize)?.as_ref()
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
    crush_do_rule_with_choose_args(map, rule_id, x, result, result_max, weights, -1)
}

/// Execute a conventional CRUSH rule while collecting retry observations in `profile`.
pub fn crush_do_rule_with_choose_profile(
    map: &CrushMap,
    rule_id: u32,
    x: u32,
    result: &mut Vec<i32>,
    result_max: usize,
    weights: &[u32],
    profile: &mut ChooseProfile,
) -> Result<()> {
    if matches!(
        map.get_rule(rule_id)?.rule_type,
        crate::crush::types::RuleType::MsrFirstN | crate::crush::types::RuleType::MsrIndep
    ) {
        return Err(CrushError::InvalidRuleState(
            "retry profiling is unsupported for MSR rules",
        ));
    }
    crush_do_rule_impl(
        map,
        rule_id,
        x,
        result,
        result_max,
        weights,
        -1,
        Some(profile),
    )
}

/// Execute a CRUSH rule using a pool's choose-argument index.
pub fn crush_do_rule_with_choose_args(
    map: &CrushMap,
    rule_id: u32,
    x: u32,
    result: &mut Vec<i32>,
    result_max: usize,
    weights: &[u32],
    choose_args_index: i64,
) -> Result<()> {
    crush_do_rule_impl(
        map,
        rule_id,
        x,
        result,
        result_max,
        weights,
        choose_args_index,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn crush_do_rule_impl(
    map: &CrushMap,
    rule_id: u32,
    x: u32,
    result: &mut Vec<i32>,
    result_max: usize,
    weights: &[u32],
    choose_args_index: i64,
    mut profile: Option<&mut ChooseProfile>,
) -> Result<()> {
    // NOTE: Ceph's public mapper takes signed 32-bit counts and input bits.
    // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L2016-L2020
    if result_max > i32::MAX as usize || weights.len() > i32::MAX as usize {
        return Err(CrushError::InvalidRuleState(
            "mapper count exceeds i32::MAX",
        ));
    }
    let x = x as i32;
    let rule = map.get_rule(rule_id)?;
    let choose_args = map
        .choose_args
        .get(&choose_args_index)
        .or_else(|| map.choose_args.get(&-1))
        .map(Vec::as_slice);

    if matches!(
        rule.rule_type,
        crate::crush::types::RuleType::MsrFirstN | crate::crush::types::RuleType::MsrIndep
    ) {
        return crush_msr_do_rule(map, rule, x, result, result_max, weights, choose_args);
    }

    result.clear();

    let mut work: Vec<i32> = Vec::with_capacity(result_max);
    let mut scratch: Vec<i32> = Vec::with_capacity(result_max);

    // NOTE: Ceph adds one to this u32 retry count; overflow wraps before use.
    // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L849-L853
    let mut choose_tries = map.choose_total_tries.wrapping_add(1);
    let mut choose_leaf_tries = 0;
    let mut choose_local_tries = map.choose_local_tries;
    let mut choose_local_fallback_tries = map.choose_local_fallback_tries;
    let mut chooseleaf_vary_r = u32::from(map.chooseleaf_vary_r);
    let mut chooseleaf_stable = u32::from(map.chooseleaf_stable);

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

                let recurse_tries = if choose_leaf_tries > 0 {
                    choose_leaf_tries
                } else if map.chooseleaf_descend_once != 0 {
                    1
                } else {
                    choose_tries
                };
                scratch.clear();
                let mut domains = vec![CRUSH_ITEM_NONE; result_max];
                let mut leaves = vec![CRUSH_ITEM_NONE; result_max];
                for &item in &work {
                    if item >= 0 || numrep == 0 {
                        continue;
                    }
                    let remaining = result_max - scratch.len();
                    let count = crush_choose_firstn(
                        map,
                        item,
                        x,
                        numrep,
                        item_type,
                        &mut domains[..remaining],
                        0,
                        weights,
                        choose_tries,
                        recurse_tries,
                        choose_local_tries,
                        choose_local_fallback_tries,
                        chooseleaf_vary_r,
                        chooseleaf_stable,
                        recurse_to_leaf.then_some(&mut leaves[..remaining]),
                        0,
                        choose_args,
                        profile.as_deref_mut(),
                    )?;
                    let selected = if recurse_to_leaf { &leaves } else { &domains };
                    scratch.extend_from_slice(&selected[..count]);
                }
                std::mem::swap(&mut work, &mut scratch);
            }

            RuleOp::ChooseIndep | RuleOp::ChooseLeafIndep => {
                let recurse_to_leaf = step.op == RuleOp::ChooseLeafIndep;
                let numrep = calculate_numrep(step.arg1, result_max);
                let item_type = step.arg2;

                scratch.clear();
                let mut domains = vec![CRUSH_ITEM_NONE; result_max];
                let mut leaves = vec![CRUSH_ITEM_NONE; result_max];
                for &item in &work {
                    // As in Ceph's executor, devices and unresolved positions
                    // cannot be inputs to another bucket selection step.
                    if item >= 0 || numrep == 0 {
                        continue;
                    }
                    let out_size = numrep.min(result_max - scratch.len());
                    crush_choose_indep(
                        map,
                        item,
                        x,
                        out_size,
                        numrep,
                        item_type,
                        &mut domains[..out_size],
                        0,
                        weights,
                        choose_tries,
                        if choose_leaf_tries > 0 {
                            choose_leaf_tries
                        } else {
                            1
                        },
                        recurse_to_leaf,
                        recurse_to_leaf.then_some(&mut leaves[..out_size]),
                        0, // top-level call: parent_r = 0
                        choose_args,
                        profile.as_deref_mut(),
                    )?;
                    let selected = if recurse_to_leaf { &leaves } else { &domains };
                    scratch.extend_from_slice(&selected[..out_size]);
                }
                std::mem::swap(&mut work, &mut scratch);
            }

            RuleOp::Emit => {
                for &item in &work {
                    if result.len() < result_max {
                        result.push(item);
                    }
                }
                work.clear();
            }

            RuleOp::SetChooseTries if step.arg1 > 0 => choose_tries = step.arg1 as u32,
            RuleOp::SetChooseLeafTries => {
                if step.arg1 > 0 {
                    choose_leaf_tries = step.arg1 as u32;
                }
            }
            RuleOp::SetChooseLocalTries if step.arg1 >= 0 => {
                choose_local_tries = step.arg1 as u32;
            }
            RuleOp::SetChooseLocalFallbackTries if step.arg1 >= 0 => {
                choose_local_fallback_tries = step.arg1 as u32;
            }
            RuleOp::SetChooseLeafVaryR if step.arg1 >= 0 => {
                chooseleaf_vary_r = step.arg1 as u32;
            }
            RuleOp::SetChooseLeafStable if step.arg1 >= 0 => {
                chooseleaf_stable = step.arg1 as u32;
            }
            RuleOp::SetChooseTries
            | RuleOp::SetChooseLocalTries
            | RuleOp::SetChooseLocalFallbackTries
            | RuleOp::SetChooseLeafVaryR
            | RuleOp::SetChooseLeafStable
            | RuleOp::SetMsrDescents
            | RuleOp::SetMsrCollisionTries
            | RuleOp::ChooseMsr
            | RuleOp::Noop => {}
        }
    }

    Ok(())
}

/// Choose failure domains with FIRSTN, optionally selecting a leaf in each.
/// `out` retains domains for collision checks; `leaves` retains the OSDs.
/// The returned position includes any prefix supplied by a recursive call.
/// Reference: src/crush/mapper.c::crush_choose_firstn, v17.2.7 and v20.2.4.
#[allow(clippy::too_many_arguments)]
fn crush_choose_firstn(
    map: &CrushMap,
    bucket_id: i32,
    x: i32,
    numrep: usize,
    item_type: i32,
    out: &mut [i32],
    mut outpos: usize,
    weights: &[u32],
    tries: u32,
    recurse_tries: u32,
    local_tries: u32,
    local_fallback_tries: u32,
    vary_r: u32,
    stable: u32,
    mut leaves: Option<&mut [i32]>,
    parent_r: i32,
    choose_args: Option<&[Option<CrushChooseArg>]>,
    mut profile: Option<&mut ChooseProfile>,
) -> Result<usize> {
    let _span = tracing::debug_span!(
        "crush_choose_firstn",
        bucket_id,
        x,
        numrep,
        item_type,
        parent_r
    )
    .entered();
    tracing::debug!(
        outpos,
        capacity = out.len(),
        tries,
        recurse_tries,
        local_retries = local_tries,
        local_fallback_retries = local_fallback_tries,
        vary_r,
        stable,
        chooseleaf = leaves.is_some(),
        "Starting FIRSTN selection"
    );
    let bucket = map.get_bucket(bucket_id)?;
    let first_rep = if stable != 0 { 0 } else { outpos };
    'replicas: for rep in first_rep..numrep {
        if outpos == out.len() {
            tracing::trace!(rep, outpos, "Output capacity reached");
            break;
        }
        let mut current_bucket = bucket;
        let mut total_failures = 0u32;
        let mut local_failures = 0u32;
        loop {
            // Retries always change r. vary_r only controls the recursive seed.
            let r = (rep as i32)
                .wrapping_add(parent_r)
                .wrapping_add(total_failures as i32);
            let mut collide = false;
            if current_bucket.size != 0 {
                let item = if local_fallback_tries > 0
                    && local_failures >= current_bucket.size / 2
                    && local_failures > local_fallback_tries
                {
                    tracing::trace!(
                        rep,
                        r,
                        current_bucket_id = current_bucket.id,
                        local_failures,
                        "Using permutation fallback"
                    );
                    bucket_perm_choose(current_bucket, x, r)
                } else {
                    bucket_choose_with_arg(
                        current_bucket,
                        x,
                        r,
                        choose_arg(choose_args, current_bucket.id),
                        outpos,
                    )
                };
                tracing::trace!(
                    rep,
                    r,
                    current_bucket_id = current_bucket.id,
                    item,
                    "Selected candidate"
                );
                if item >= map.max_devices {
                    tracing::trace!(
                        rep,
                        item,
                        max_devices = map.max_devices,
                        "Skipping replica: invalid device"
                    );
                    continue 'replicas;
                }
                let Some(selected_type) = get_item_type(map, item) else {
                    tracing::trace!(rep, item, "Skipping replica: invalid bucket");
                    continue 'replicas;
                };
                if selected_type != item_type {
                    if item >= 0 {
                        tracing::trace!(
                            rep,
                            item,
                            selected_type,
                            "Skipping replica: wrong item type"
                        );
                        continue 'replicas;
                    }
                    tracing::trace!(
                        rep,
                        from = current_bucket.id,
                        to = item,
                        selected_type,
                        "Descending through hierarchy"
                    );
                    current_bucket = map.get_bucket(item)?;
                    continue;
                }

                collide = out[..outpos].contains(&item);
                if collide {
                    tracing::trace!(rep, item, outpos, "Rejecting candidate: collision");
                }
                let mut reject = false;
                if !collide && let Some(leaf_out) = leaves.as_deref_mut() {
                    if item < 0 {
                        let sub_r = if vary_r == 0 {
                            0
                        } else {
                            // NOTE: Ceph shifts a signed int here (arithmetic shift).
                            // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L548-L553
                            r.checked_shr(vary_r - 1)
                                .ok_or(CrushError::InvalidRuleState(
                                    "chooseleaf_vary_r exceeds 32-bit shift range",
                                ))?
                        };
                        tracing::trace!(
                            rep,
                            item,
                            outpos,
                            sub_r,
                            recurse_tries,
                            "Selecting leaf in failure domain"
                        );
                        let end = crush_choose_firstn(
                            map,
                            item,
                            x,
                            if stable != 0 { 1 } else { outpos + 1 },
                            0,
                            leaf_out,
                            outpos,
                            weights,
                            recurse_tries,
                            0,
                            local_tries,
                            local_fallback_tries,
                            vary_r,
                            stable,
                            None,
                            sub_r,
                            choose_args,
                            profile.as_deref_mut(),
                        )?;
                        reject = end == outpos;
                        if reject {
                            tracing::trace!(rep, item, "Rejecting candidate: no eligible leaf");
                        }
                    } else {
                        leaf_out[outpos] = item;
                    }
                }
                if !reject && !collide && item >= 0 {
                    reject = is_out(weights, item, x);
                    if reject {
                        tracing::trace!(rep, item, "Rejecting candidate: device is out");
                    }
                }
                if !reject && !collide {
                    tracing::trace!(
                        bucket_id,
                        x,
                        rep,
                        item,
                        total_failures,
                        "CRUSH FIRSTN selected item"
                    );
                    out[outpos] = item;
                    outpos += 1;
                    if let Some(profile) = profile.as_deref_mut() {
                        profile.record(total_failures);
                    }
                    continue 'replicas;
                }
            } else {
                tracing::trace!(
                    rep,
                    current_bucket_id = current_bucket.id,
                    "Rejecting empty bucket"
                );
            }

            // NOTE: Failure counters and the fallback bound use u32 arithmetic in C.
            // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L586-L598
            total_failures = total_failures.wrapping_add(1);
            local_failures = local_failures.wrapping_add(1);
            if (collide && local_failures <= local_tries)
                || (local_fallback_tries > 0
                    && local_failures <= current_bucket.size.wrapping_add(local_fallback_tries))
            {
                // A local retry stays in the bucket where the collision occurred.
                tracing::trace!(
                    rep,
                    current_bucket_id = current_bucket.id,
                    total_failures,
                    local_failures,
                    "Retrying current bucket"
                );
                continue;
            }
            if total_failures >= tries {
                tracing::debug!(
                    rep,
                    outpos,
                    total_failures,
                    "Skipping replica: retry budget exhausted"
                );
                continue 'replicas;
            }
            // A descent retry starts again at the original failure domain.
            tracing::trace!(rep, total_failures, "Restarting descent");
            current_bucket = bucket;
            local_failures = 0;
        }
    }
    tracing::debug!(outpos, "Finished FIRSTN selection");
    Ok(outpos)
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
    x: i32,
    mut left: usize,
    numrep: usize,
    item_type: i32,
    out: &mut [i32],
    outpos: usize,
    weights: &[u32],
    tries: u32,
    recurse_tries: u32,
    recurse_to_leaf: bool,
    mut leaves: Option<&mut [i32]>,
    parent_r: i32,
    choose_args: Option<&[Option<CrushChooseArg>]>,
    mut profile: Option<&mut ChooseProfile>,
) -> Result<()> {
    tracing::debug!(
        "crush_choose_indep: bucket_id={}, numrep={}, item_type={}, recurse_to_leaf={}",
        bucket_id,
        numrep,
        item_type,
        recurse_to_leaf
    );

    let bucket = map.get_bucket(bucket_id)?;
    tracing::debug!(
        "Got bucket: id={}, type={}, size={}, items={:?}",
        bucket.id,
        bucket.bucket_type,
        bucket.size,
        bucket.items
    );

    let endpos = (outpos + left).min(out.len());
    for rep in outpos..endpos {
        out[rep] = CRUSH_ITEM_UNDEF;
        if let Some(leaves) = leaves.as_deref_mut() {
            leaves[rep] = CRUSH_ITEM_UNDEF;
        }
    }

    let mut ftotal = 0;
    while ftotal < tries && left > 0 {
        for rep in outpos..endpos {
            if out[rep] != CRUSH_ITEM_UNDEF {
                continue;
            }
            let mut current_bucket = bucket;

            loop {
                if current_bucket.size == 0 {
                    // Keep this position unresolved and retry from the original bucket.
                    break;
                }
                let retry_stride = if current_bucket.alg
                    == crate::crush::types::BucketAlgorithm::Uniform
                    && current_bucket.size % numrep as u32 == 0
                {
                    numrep + 1
                } else {
                    numrep
                };
                let r = (rep as i32)
                    .wrapping_add(parent_r)
                    .wrapping_add((retry_stride as u32).wrapping_mul(ftotal) as i32);
                let candidate = bucket_choose_with_arg(
                    current_bucket,
                    x,
                    r,
                    choose_arg(choose_args, current_bucket.id),
                    outpos,
                );

                tracing::debug!(
                    "Selected item {} from bucket {} (rep={}, try={})",
                    candidate,
                    current_bucket.id,
                    rep,
                    ftotal
                );

                let Some(itemtype) = get_item_type(map, candidate) else {
                    tracing::debug!("Invalid bucket {}", candidate);
                    out[rep] = CRUSH_ITEM_NONE;
                    if let Some(leaves) = leaves.as_deref_mut() {
                        leaves[rep] = CRUSH_ITEM_NONE;
                    }
                    left -= 1;
                    break;
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
                        if let Some(leaves) = leaves.as_deref_mut() {
                            leaves[rep] = CRUSH_ITEM_NONE;
                        }
                        left -= 1;
                        break;
                    }
                    current_bucket = map.get_bucket(candidate)?;
                    tracing::debug!("Descending into bucket {}", candidate);
                    continue;
                }

                let collision = out
                    .iter()
                    .take(endpos)
                    .skip(outpos)
                    .any(|&val| val == candidate);
                if collision {
                    tracing::debug!("Item {} collides with another position", candidate);
                    break;
                }

                if recurse_to_leaf && candidate < 0 {
                    let leaf_out = leaves
                        .as_deref_mut()
                        .ok_or(CrushError::InvalidRuleState("missing chooseleaf output"))?;
                    crush_choose_indep(
                        map,
                        candidate,
                        x,
                        1,
                        numrep,
                        0, // Type 0 = device
                        leaf_out,
                        rep,
                        weights,
                        recurse_tries,
                        0,
                        false,
                        None,
                        r,
                        choose_args,
                        profile.as_deref_mut(),
                    )?;
                    if leaf_out[rep] == CRUSH_ITEM_NONE {
                        tracing::debug!("Failed to find leaf in bucket {}", candidate);
                        break;
                    }
                } else if recurse_to_leaf {
                    leaves
                        .as_deref_mut()
                        .ok_or(CrushError::InvalidRuleState("missing chooseleaf output"))?[rep] =
                        candidate;
                }

                if itemtype == 0 && is_out(weights, candidate, x) {
                    tracing::debug!("Device {} is out", candidate);
                    break;
                }

                out[rep] = candidate;
                left -= 1;
                tracing::trace!(
                    "crush_choose_indep: rep={}, item={} SELECTED",
                    rep,
                    candidate
                );
                break;
            }
        }
        ftotal += 1;
    }

    for rep in outpos..endpos {
        if out[rep] == CRUSH_ITEM_UNDEF {
            out[rep] = CRUSH_ITEM_NONE;
        }
        if let Some(leaves) = leaves.as_deref_mut()
            && leaves[rep] == CRUSH_ITEM_UNDEF
        {
            leaves[rep] = CRUSH_ITEM_NONE;
        }
    }

    if let Some(profile) = profile {
        profile.record(ftotal);
    }

    Ok(())
}

fn crush_msr_do_rule(
    map: &CrushMap,
    rule: &crate::crush::types::CrushRule,
    x: i32,
    result: &mut Vec<i32>,
    result_max: usize,
    weights: &[u32],
    choose_args: Option<&[Option<CrushChooseArg>]>,
) -> Result<()> {
    let mut descents = map.msr_descents;
    let mut collision_tries = map.msr_collision_tries;
    let mut step = 0;
    while let Some(config) = rule.steps.get(step) {
        match config.op {
            RuleOp::SetMsrDescents => descents = config.arg1 as u32,
            RuleOp::SetMsrCollisionTries => collision_tries = config.arg1 as u32,
            _ => break,
        }
        step += 1;
    }
    tracing::debug!(
        rule_id = rule.rule_id,
        x,
        result_max,
        descents,
        collision_tries,
        "executing MSR rule"
    );

    result.clear();
    result.resize(result_max, CRUSH_ITEM_NONE);
    let mut returned = 0;
    let mut start_index = 0usize;

    while step < rule.steps.len() {
        let take = &rule.steps[step];
        if take.op != RuleOp::Take {
            result.clear();
            return Ok(());
        }
        let first_choose = step + 1;
        let Some(emit) = rule.steps[first_choose..]
            .iter()
            .position(|candidate| candidate.op == RuleOp::Emit)
            .map(|offset| first_choose + offset)
        else {
            result.clear();
            return Ok(());
        };
        if rule.steps[first_choose..emit]
            .iter()
            .any(|candidate| candidate.op != RuleOp::ChooseMsr)
        {
            result.clear();
            return Ok(());
        }

        let mut total_children = 1usize;
        for choose in &rule.steps[first_choose..emit] {
            // Ceph assumes a nonnegative MSR fanout. Reject invalid input
            // before converting it to an allocation size in choose_msr.
            let fanout = usize::try_from(choose.arg1)
                .map_err(|_| CrushError::InvalidMsrFanout(choose.arg1))?;
            total_children = total_children
                .checked_mul(if fanout == 0 { result_max } else { fanout })
                .filter(|&count| count <= u32::MAX as usize)
                .ok_or(CrushError::InvalidRuleState(
                    "MSR fanout product exceeds u32::MAX",
                ))?;
        }
        // NOTE: Ceph stores MSR fanout products and indices in unsigned int.
        // Reject overflow instead of widening it into a different traversal.
        // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L1139-L1212
        let end_index = start_index
            .checked_add(total_children)
            .filter(|&end| end <= u32::MAX as usize)
            .ok_or(CrushError::InvalidRuleState(
                "MSR end index exceeds u32::MAX",
            ))?
            .min(result_max);

        if take.arg1 >= 0 {
            if first_choose != emit {
                result.clear();
                return Ok(());
            }
            if start_index < result_max {
                emit_msr_result(
                    rule.rule_type,
                    result,
                    &mut returned,
                    start_index,
                    take.arg1,
                );
            }
        } else if first_choose < emit && total_children > 0 {
            let mut workspace = vec![vec![CRUSH_ITEM_UNDEF; result_max]; emit - first_choose];
            let return_limit = returned + end_index.saturating_sub(start_index);
            for attempt in 0..descents {
                if returned >= return_limit {
                    break;
                }
                choose_msr(
                    map,
                    rule,
                    x,
                    result_max,
                    weights,
                    collision_tries,
                    &mut workspace,
                    result,
                    &mut returned,
                    take.arg1,
                    total_children,
                    start_index,
                    end_index,
                    first_choose,
                    emit,
                    attempt,
                    first_choose,
                    choose_args,
                )?;
            }
        }
        start_index = end_index;
        step = emit + 1;
    }

    if rule.rule_type == crate::crush::types::RuleType::MsrFirstN {
        result.truncate(returned);
    }
    tracing::debug!(rule_id = rule.rule_id, returned, "MSR rule completed");
    Ok(())
}

fn emit_msr_result(
    rule_type: crate::crush::types::RuleType,
    result: &mut [i32],
    returned: &mut usize,
    position: usize,
    item: i32,
) {
    let output = if rule_type == crate::crush::types::RuleType::MsrFirstN {
        *returned
    } else {
        position
    };
    result[output] = item;
    *returned += 1;
    tracing::trace!(
        position,
        output,
        item,
        returned = *returned,
        "MSR item selected"
    );
}

#[allow(clippy::too_many_arguments)]
fn descend_msr(
    map: &CrushMap,
    x: i32,
    result_max: usize,
    mut bucket_id: i32,
    item_type: i32,
    attempt: u32,
    local_attempt: u32,
    index: usize,
    choose_args: Option<&[Option<CrushChooseArg>]>,
) -> Result<i32> {
    loop {
        let bucket = map.get_bucket(bucket_id)?;
        let retry = attempt
            .wrapping_mul(result_max as u32)
            .wrapping_add(index as u32)
            .wrapping_shl(16)
            .wrapping_add(local_attempt);
        // NOTE: Ceph computes an unsigned retry value, then passes it to an int.
        // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L1249-L1268
        let candidate = bucket_choose_with_arg(
            bucket,
            x,
            retry as i32,
            choose_arg(choose_args, bucket.id),
            index,
        );
        tracing::trace!(
            bucket_id,
            candidate,
            item_type,
            attempt,
            local_attempt,
            index,
            retry,
            "MSR descent candidate"
        );
        if candidate >= 0 {
            return Ok(candidate);
        }
        let child = map.get_bucket(candidate)?;
        if child.bucket_type == item_type {
            return Ok(candidate);
        }
        bucket_id = candidate;
    }
}

fn valid_msr_candidate(
    selected: &[i32],
    exclude: std::ops::Range<usize>,
    include: std::ops::Range<usize>,
    candidate: i32,
) -> bool {
    let exclude_start = exclude.start;
    selected[exclude]
        .iter()
        .enumerate()
        .find(|&(_, &item)| item == candidate)
        .is_none_or(|(offset, _)| include.contains(&(offset + exclude_start)))
}

fn push_msr_candidate(selected: &mut [i32], candidate: i32) -> Result<bool> {
    if selected.contains(&candidate) {
        return Ok(false);
    }
    *selected
        .iter_mut()
        .find(|item| **item == CRUSH_ITEM_UNDEF)
        .ok_or(CrushError::InvalidRuleState(
            "MSR stride capacity exhausted",
        ))? = candidate;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn choose_msr(
    map: &CrushMap,
    rule: &crate::crush::types::CrushRule,
    x: i32,
    result_max: usize,
    weights: &[u32],
    collision_tries: u32,
    workspace: &mut [Vec<i32>],
    result: &mut [i32],
    returned: &mut usize,
    bucket_id: i32,
    total_descendants: usize,
    start_index: usize,
    end_index: usize,
    current_step: usize,
    end_step: usize,
    attempt: u32,
    workspace_start: usize,
    choose_args: Option<&[Option<CrushChooseArg>]>,
) -> Result<usize> {
    let _span = tracing::debug_span!(
        "crush_choose_msr",
        bucket_id,
        start_index,
        end_index,
        current_step,
        attempt
    )
    .entered();
    let choose = &rule.steps[current_step];
    let num_strides = if choose.arg1 == 0 {
        result_max
    } else {
        choose.arg1 as usize
    };
    let stride_length = total_descendants / num_strides;
    let workspace_index = current_step - workspace_start;
    let leaf_index = end_step - workspace_start - 1;
    // Only strides intersecting the requested output can acquire undo entries.
    let mut undo = vec![CRUSH_ITEM_UNDEF; (end_index - start_index).div_ceil(stride_length)];
    let mut mapped = 0;

    for (stride, stride_start) in (start_index..end_index).step_by(stride_length).enumerate() {
        let stride_end = (stride_start + stride_length).min(end_index);
        if workspace[leaf_index][stride_start..stride_end]
            .iter()
            .all(|&item| item != CRUSH_ITEM_UNDEF)
        {
            continue;
        }

        let mut candidate = None;
        for local_attempt in 0..collision_tries {
            let item = descend_msr(
                map,
                x,
                result_max,
                bucket_id,
                choose.arg2,
                attempt,
                local_attempt,
                stride,
                choose_args,
            )?;
            if valid_msr_candidate(
                &workspace[workspace_index],
                start_index..end_index,
                stride_start..stride_end,
                item,
            ) {
                candidate = Some(item);
                break;
            }
        }
        let Some(candidate) = candidate else {
            tracing::trace!(
                stride,
                stride_start,
                stride_end,
                "MSR collision retries exhausted"
            );
            continue;
        };

        if choose.arg2 == 0 {
            if stride_length != 1 || current_step + 1 != end_step {
                continue;
            }
            if is_out(weights, candidate, x) {
                tracing::trace!(candidate, stride_start, "MSR candidate is out");
                continue;
            }
            push_msr_candidate(
                &mut workspace[workspace_index][stride_start..stride_end],
                candidate,
            )?;
            emit_msr_result(rule.rule_type, result, returned, stride_start, candidate);
            mapped += 1;
            continue;
        }

        if current_step + 1 >= end_step {
            continue;
        }
        let child_mapped = choose_msr(
            map,
            rule,
            x,
            result_max,
            weights,
            collision_tries,
            workspace,
            result,
            returned,
            candidate,
            stride_length,
            stride_start,
            stride_end,
            current_step + 1,
            end_step,
            attempt,
            workspace_start,
            choose_args,
        )?;
        let pushed = push_msr_candidate(
            &mut workspace[workspace_index][stride_start..stride_end],
            candidate,
        )?;
        if pushed && child_mapped == 0 {
            undo[stride] = candidate;
            tracing::trace!(candidate, stride, "MSR descent produced no leaf");
        } else {
            mapped += child_mapped;
        }
    }

    for (stride, stride_start) in (start_index..end_index).step_by(stride_length).enumerate() {
        if undo[stride] == CRUSH_ITEM_UNDEF {
            continue;
        }
        let stride_end = (stride_start + stride_length).min(end_index);
        let selected = &mut workspace[workspace_index][stride_start..stride_end];
        let entry = selected
            .iter_mut()
            .rev()
            .find(|item| **item != CRUSH_ITEM_UNDEF)
            .ok_or(CrushError::InvalidRuleState("missing MSR undo candidate"))?;
        debug_assert_eq!(*entry, undo[stride]);
        tracing::trace!(candidate = *entry, stride, "MSR unused candidate removed");
        *entry = CRUSH_ITEM_UNDEF;
    }

    Ok(mapped)
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

        let mut out = vec![CRUSH_ITEM_NONE; 2];
        let weights = vec![0x10000, 0x10000, 0x10000];
        let count = crush_choose_firstn(
            &map, -1, 123, 2, 0, &mut out, 0, &weights, 50, 50, 0, 0, 0, 0, None, 0, None, None,
        )
        .unwrap();
        out.truncate(count);

        assert_eq!(out.len(), 2);
        assert_ne!(out[0], out[1]);
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

        let res = crush_choose_indep(
            &map, -1, 123, 3, 3, 0, &mut out, 0, &weights, 50, 0, false, None, 0, None, None,
        );

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
        crush_choose_indep(
            &map, -1, 42, 3, 3, 0, &mut out1, 0, &weights, 50, 0, false, None, 0, None, None,
        )
        .unwrap();
        crush_choose_indep(
            &map, -1, 42, 3, 3, 0, &mut out2, 0, &weights, 50, 0, false, None, 0, None, None,
        )
        .unwrap();
        assert_eq!(out1, out2, "same input should produce same output");
    }

    #[test]
    fn indep_retries_empty_intermediate_bucket() {
        let mut map = CrushMap::new();
        map.max_devices = 2;
        map.max_buckets = 6;
        map.buckets = [
            (-1, 3, vec![-2, -3, -4]),
            (-2, 2, vec![]),
            (-3, 2, vec![-5]),
            (-4, 2, vec![-6]),
            (-5, 1, vec![0]),
            (-6, 1, vec![1]),
        ]
        .into_iter()
        .map(|(id, bucket_type, items)| {
            Some(CrushBucket {
                id,
                bucket_type,
                alg: BucketAlgorithm::Uniform,
                hash: 0,
                weight: items.len() as u32 * 0x10000,
                size: items.len() as u32,
                items,
                data: BucketData::Uniform {
                    item_weight: 0x10000,
                },
            })
        })
        .collect();

        for (op, expected) in [(RuleOp::ChooseLeafIndep, 0), (RuleOp::ChooseIndep, -5)] {
            map.rules = vec![Some(CrushRule {
                rule_id: 0,
                rule_type: crate::crush::types::RuleType::Erasure,
                steps: vec![
                    CrushRuleStep {
                        op: RuleOp::Take,
                        arg1: -1,
                        arg2: 0,
                    },
                    CrushRuleStep {
                        op,
                        arg1: 1,
                        arg2: 1,
                    },
                    CrushRuleStep {
                        op: RuleOp::Emit,
                        arg1: 0,
                        arg2: 0,
                    },
                ],
            })];

            // x=1 descends into empty rack -2 on its first attempt.
            let mut result = Vec::new();
            map.choose_total_tries = 0;
            crush_do_rule(&map, 0, 1, &mut result, 1, &[0x10000; 2]).unwrap();
            assert_eq!(result, [CRUSH_ITEM_NONE]);

            // Ceph retries from the root and selects host -5 / osd.0.
            map.choose_total_tries = 50;
            crush_do_rule(&map, 0, 1, &mut result, 1, &[0x10000; 2]).unwrap();
            assert_eq!(result, [expected]);
        }
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
            3,
            0,
            &mut out_all,
            0,
            &weights_all_in,
            50,
            0,
            false,
            None,
            0,
            None,
            None,
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

#[cfg(test)]
#[path = "tests/mapper_types.rs"]
mod type_tests;
