//! Per-group Raft timing profiles (ADR 0075 section 3.4, roadmap G-01 stage
//! G-c groundwork).
//!
//! A [`RaftCore`](crate::raft::RaftCore) used to carry one hard-coded LAN
//! timing pair (election base 150 ms, heartbeat 50 ms). A group whose replicas
//! span more than one region (a "stretch" group) needs an election timeout well
//! above the inter-region round trip, or every vote round trip outlives the
//! timeouts it races against and the group churns. This module is the **pure**
//! half: given a replica set, the replicated member labels and the configured
//! `max_region_rtt`, pick a [`TimingProfile`] and turn it into the
//! `(election_base, heartbeat_interval)` pair
//! [`RaftCore::set_timing`](crate::raft::RaftCore::set_timing) installs.
//!
//! **Nothing here is replicated or wire-visible** (ADR 0075 section 8): the
//! profile is a node-local function of existing `Metadata` (`Member.labels`)
//! plus an additive node-local config value, so no gate is needed.
//!
//! # The formula (normative; the ADR 0075 2026-10-04 amendment restates it)
//!
//! ```text
//! LAN                 = (election 150 ms, heartbeat 50 ms)      // the old constants
//! Wan{max_region_rtt} :
//!   election  = max(150 ms, 5 * max_region_rtt)   // = 10x the one-way RTT
//!   heartbeat = max(50 ms,  election / 10)
//! ```
//!
//! With the default `max_region_rtt` of 150 ms that is election 750 ms
//! (randomized `[750, 1500)` ms) and heartbeat 75 ms. Both LAN values are
//! **floors**, so a WAN profile can never be faster than LAN, and a tiny
//! configured RTT degrades to exactly LAN.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_env::NodeId;

/// The node-label key whose value defines a node's "region" for MRSC (ADR 0075
/// section 3.1/7): the one `animus_placement` constant (G-01 stage G-a),
/// re-exported so `animus_control::timing::REGION_LABEL` keeps resolving.
pub use animus_placement::REGION_LABEL;

/// The LAN election-timeout base (the low end of the randomized
/// `[base, 2*base)` range) — what every group used before profiles existed.
pub const LAN_ELECTION_BASE: Duration = Duration::from_millis(150);

/// The LAN heartbeat interval — what every group used before profiles existed.
pub const LAN_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

/// The default `max_region_rtt` (ADR 0075 section 3.4): the configured upper
/// bound on the round trip between any two regions of the cluster.
pub const DEFAULT_MAX_REGION_RTT: Duration = Duration::from_millis(150);

/// A group's Raft timing profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimingProfile {
    /// Every replica in one region (or no region labels at all): the
    /// historical constants.
    Lan,
    /// Replicas span more than one region: timeouts sized to
    /// `max_region_rtt`.
    Wan {
        /// Upper bound on the round trip between any two regions.
        max_region_rtt: Duration,
    },
}

impl TimingProfile {
    /// This profile's `(election_base, heartbeat_interval)` — see the module
    /// doc for the formula. Pure and total (saturating arithmetic).
    #[must_use]
    pub fn durations(self) -> (Duration, Duration) {
        match self {
            TimingProfile::Lan => (LAN_ELECTION_BASE, LAN_HEARTBEAT_INTERVAL),
            TimingProfile::Wan { max_region_rtt } => {
                let election = max_region_rtt.saturating_mul(5).max(LAN_ELECTION_BASE);
                let heartbeat = (election / 10).max(LAN_HEARTBEAT_INTERVAL);
                (election, heartbeat)
            }
        }
    }

    /// Whether this is the WAN profile.
    #[must_use]
    pub fn is_wan(self) -> bool {
        matches!(self, TimingProfile::Wan { .. })
    }
}

/// The number of distinct region label values among `replicas`, looked up in
/// `regions` (member id -> that member's `topology.kubernetes.io/region`
/// value). A replica with no entry is **unlabelled** and contributes nothing —
/// it can neither create nor hide a region.
#[must_use]
pub fn distinct_regions<'a>(
    replicas: impl IntoIterator<Item = &'a NodeId>,
    regions: &BTreeMap<NodeId, String>,
) -> usize {
    let mut seen: Vec<&str> = Vec::new();
    for r in replicas {
        if let Some(region) = regions.get(r)
            && !seen.contains(&region.as_str())
        {
            seen.push(region.as_str());
        }
    }
    seen.len()
}

/// The profile for a group whose replica set (voters **and** learners) is
/// `replicas`: [`TimingProfile::Wan`] iff the replicas carry region labels with
/// more than one distinct value, else [`TimingProfile::Lan`]. An unlabelled
/// cluster is therefore always LAN — no behaviour change.
#[must_use]
pub fn profile_for_replicas<'a>(
    replicas: impl IntoIterator<Item = &'a NodeId>,
    regions: &BTreeMap<NodeId, String>,
    max_region_rtt: Duration,
) -> TimingProfile {
    if distinct_regions(replicas, regions) > 1 {
        TimingProfile::Wan { max_region_rtt }
    } else {
        TimingProfile::Lan
    }
}

/// Project a member table's labels onto the region key: member id -> region.
/// Members without the label are omitted.
#[must_use]
pub fn region_map<'a>(
    members: impl IntoIterator<Item = (&'a NodeId, &'a BTreeMap<String, String>)>,
) -> BTreeMap<NodeId, String> {
    members
        .into_iter()
        .filter_map(|(id, labels)| labels.get(REGION_LABEL).map(|r| (id.clone(), r.clone())))
        .collect()
}

/// A control-voter set that would put a strict majority of voters in one region
/// (ADR 0075 section 3.1: losing that region would lose the control quorum).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionMajority {
    /// The region holding the majority.
    pub region: String,
    /// How many voters it holds.
    pub in_region: usize,
    /// The total voter count.
    pub total: usize,
}

impl std::fmt::Display for RegionMajority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} of {} control voters would be in region {:?}: losing that region would lose \
             the control quorum (ADR 0075 section 3.1 wants voters spread across regions, \
             never a majority in one)",
            self.in_region, self.total, self.region
        )
    }
}

/// The region-aware control-voter placement check (ADR 0075 sections 3.1/3.4).
///
/// `region_of` gives a voter's region (`None` = unlabelled / unknown — a
/// control-only node has no `Member` row, so its labels are not in `Metadata`).
/// `cluster_regions` is the number of distinct region values the cluster's
/// members carry (computed over **all** members plus any candidate's supplied
/// labels). Returns `Some` only when the cluster carries labels from more than
/// one region **and** a single region holds a strict majority of `voters`.
/// Unlabelled voters count toward the total (they dilute a majority) but
/// belong to no region. An unlabelled or single-region cluster is never
/// refused — no behaviour change.
#[must_use]
pub fn region_majority_violation<'a>(
    voters: impl IntoIterator<Item = &'a NodeId>,
    region_of: impl Fn(&NodeId) -> Option<String>,
    cluster_regions: usize,
) -> Option<RegionMajority> {
    if cluster_regions < 2 {
        return None;
    }
    let mut per_region: BTreeMap<String, usize> = BTreeMap::new();
    let mut total = 0usize;
    for v in voters {
        total += 1;
        if let Some(r) = region_of(v) {
            *per_region.entry(r).or_insert(0) += 1;
        }
    }
    per_region
        .into_iter()
        .find(|(_, n)| *n * 2 > total)
        .map(|(region, in_region)| RegionMajority {
            region,
            in_region,
            total,
        })
}

/// The admin-path form of [`region_majority_violation`] for a proposed control
/// voter change `before -> after` (ADR 0075 section 3.4).
///
/// A voter's region is read from `meta.members[voter].labels` (or, for a
/// candidate that is not a member yet, from `candidate`'s supplied labels).
/// `cluster_regions` is the number of distinct region values over **all**
/// members plus the candidate. Refuses (`Err`) iff the resulting set would put
/// a strict majority of voters in one region **and** the change does not
/// strictly reduce that region's share relative to `before`. The second clause
/// keeps a one-region bootstrap growable: while a cluster is spread out voter
/// by voter, each intermediate set may still be concentrated, and every step
/// that dilutes the concentration is allowed. An unlabelled or single-region
/// cluster is never refused.
///
/// Control-only voters have no `Member` row, so their labels are not in
/// `Metadata` and they count as unlabelled (they dilute but never create a
/// majority) until a config-borne label source exists.
///
/// # Errors
/// A human-readable refusal naming the region and the voter counts.
pub fn control_voter_change_check(
    meta: &crate::meta::Metadata,
    before: &std::collections::BTreeSet<NodeId>,
    after: &std::collections::BTreeSet<NodeId>,
    candidate: Option<(&NodeId, &BTreeMap<String, String>)>,
) -> Result<(), String> {
    let mut all: Vec<&str> = Vec::new();
    for m in meta.members.values() {
        if let Some(r) = m.labels.get(REGION_LABEL)
            && !all.contains(&r.as_str())
        {
            all.push(r.as_str());
        }
    }
    if let Some((_, labels)) = candidate
        && let Some(r) = labels.get(REGION_LABEL)
        && !all.contains(&r.as_str())
    {
        all.push(r.as_str());
    }
    let region_of = |id: &NodeId| -> Option<String> {
        if let Some((c, labels)) = candidate
            && c == id
        {
            return labels.get(REGION_LABEL).cloned();
        }
        meta.members
            .get(id)
            .and_then(|m| m.labels.get(REGION_LABEL).cloned())
    };
    let Some(v) = region_majority_violation(after.iter(), region_of, all.len()) else {
        return Ok(());
    };
    if let Some(b) = region_majority_violation(before.iter(), region_of, all.len())
        && b.region == v.region
        && v.in_region * b.total < b.in_region * v.total
    {
        return Ok(()); // strictly less concentrated than before: a step the right way
    }
    Err(v.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_env::nid;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn lan_is_the_historical_constants() {
        assert_eq!(TimingProfile::Lan.durations(), (ms(150), ms(50)));
    }

    #[test]
    fn wan_default_rtt_is_750_75() {
        let p = TimingProfile::Wan {
            max_region_rtt: DEFAULT_MAX_REGION_RTT,
        };
        assert_eq!(p.durations(), (ms(750), ms(75)));
    }

    #[test]
    fn wan_never_goes_below_lan_floors() {
        for rtt in [0, 1, 10, 30, 31] {
            let (e, h) = TimingProfile::Wan {
                max_region_rtt: ms(rtt),
            }
            .durations();
            assert!(e >= LAN_ELECTION_BASE, "rtt {rtt}ms: election {e:?}");
            assert!(h >= LAN_HEARTBEAT_INTERVAL, "rtt {rtt}ms: heartbeat {h:?}");
        }
        // 30 ms * 5 = 150 ms: exactly the floor.
        assert_eq!(
            TimingProfile::Wan {
                max_region_rtt: ms(30)
            }
            .durations(),
            (ms(150), ms(50))
        );
    }

    #[test]
    fn wan_scales_with_rtt_and_heartbeat_is_a_tenth_of_election() {
        let (e, h) = TimingProfile::Wan {
            max_region_rtt: ms(400),
        }
        .durations();
        assert_eq!(e, ms(2000));
        assert_eq!(h, ms(200));
        // Election is 10x the one-way RTT (rtt/2).
        assert_eq!(e, ms(400) / 2 * 10);
    }

    #[test]
    fn wan_saturates_instead_of_overflowing() {
        let (e, _) = TimingProfile::Wan {
            max_region_rtt: Duration::MAX,
        }
        .durations();
        assert_eq!(e, Duration::MAX);
    }

    fn regions(pairs: &[(u64, &str)]) -> BTreeMap<NodeId, String> {
        pairs
            .iter()
            .map(|(n, r)| (nid(*n), (*r).to_string()))
            .collect()
    }

    #[test]
    fn profile_is_wan_only_across_more_than_one_distinct_region() {
        let m = regions(&[(1, "a"), (2, "a"), (3, "b")]);
        let rtt = ms(150);
        let one = [nid(1), nid(2)];
        let two = [nid(1), nid(3)];
        assert_eq!(profile_for_replicas(&one, &m, rtt), TimingProfile::Lan);
        assert_eq!(
            profile_for_replicas(&two, &m, rtt),
            TimingProfile::Wan {
                max_region_rtt: rtt
            }
        );
    }

    #[test]
    fn unlabelled_members_never_make_a_group_wan() {
        let m = regions(&[(1, "a")]);
        let rs = [nid(1), nid(2), nid(3)];
        assert_eq!(profile_for_replicas(&rs, &m, ms(150)), TimingProfile::Lan);
        assert_eq!(
            profile_for_replicas(&rs, &BTreeMap::new(), ms(150)),
            TimingProfile::Lan
        );
    }

    #[test]
    fn region_map_reads_only_the_region_key() {
        let mut l1 = BTreeMap::new();
        l1.insert(REGION_LABEL.to_string(), "eu".to_string());
        l1.insert("zone".to_string(), "z1".to_string());
        let mut l2 = BTreeMap::new();
        l2.insert("zone".to_string(), "z1".to_string());
        let n1 = nid(1);
        let n2 = nid(2);
        let m = region_map([(&n1, &l1), (&n2, &l2)]);
        assert_eq!(m, regions(&[(1, "eu")]));
    }

    #[test]
    fn majority_check_rejects_a_region_majority_only_in_a_multi_region_cluster() {
        let m = regions(&[(1, "a"), (2, "a"), (3, "b"), (4, "c"), (5, "a")]);
        let of = |n: &NodeId| m.get(n).cloned();
        let ok = [nid(1), nid(3), nid(4)];
        assert_eq!(region_majority_violation(&ok, of, 3), None);
        let bad = [nid(1), nid(2), nid(3)];
        let v = region_majority_violation(&bad, of, 3).expect("a holds 2 of 3");
        assert_eq!((v.region.as_str(), v.in_region, v.total), ("a", 2, 3));
        // Half is not a majority: 2 of 4.
        let half = [nid(1), nid(2), nid(3), nid(4)];
        assert_eq!(region_majority_violation(&half, of, 3), None);
        // Single-region (or unlabelled) cluster: never refused.
        assert_eq!(region_majority_violation(&bad, of, 1), None);
        assert_eq!(region_majority_violation(&bad, of, 0), None);
    }

    #[test]
    fn unlabelled_voters_dilute_but_never_count_as_a_region() {
        let m = regions(&[(1, "a"), (3, "b")]);
        let of = |n: &NodeId| m.get(n).cloned();
        // a:1 of 3, two unlabelled: no majority.
        let vs = [nid(1), nid(8), nid(9)];
        assert_eq!(region_majority_violation(&vs, of, 2), None);
    }

    fn meta_with(regs: &[(u64, &str)]) -> crate::meta::Metadata {
        use crate::meta::{MetaCommand, NodeStatus};
        let mut m = crate::meta::Metadata::default();
        for (n, r) in regs {
            let mut labels = BTreeMap::new();
            labels.insert(REGION_LABEL.to_string(), (*r).to_string());
            m.apply(&MetaCommand::UpsertMember {
                node: nid(*n),
                labels,
                status: NodeStatus::Active,
            });
        }
        m
    }

    fn set(ids: &[u64]) -> std::collections::BTreeSet<NodeId> {
        ids.iter().map(|n| nid(*n)).collect()
    }

    #[test]
    fn voter_change_check_refuses_a_concentrating_step_in_a_multi_region_cluster() {
        let m = meta_with(&[(1, "a"), (2, "a"), (3, "b"), (4, "c")]);
        // {1,3,4} -> add 2: a holds 2 of 4 (not a strict majority): fine.
        assert!(
            control_voter_change_check(&m, &set(&[1, 3, 4]), &set(&[1, 2, 3, 4]), None).is_ok()
        );
        // {1,3} -> {1,2,3}: a holds 2 of 3, and concentration went up: refused.
        let e = control_voter_change_check(&m, &set(&[1, 3]), &set(&[1, 2, 3]), None)
            .expect_err("2 of 3 in region a");
        assert!(e.contains("region"), "{e}");
        // Removing a b voter from {1,2,3,4}->{1,2,4}: a holds 2 of 3: refused.
        assert!(
            control_voter_change_check(&m, &set(&[1, 2, 3, 4]), &set(&[1, 2, 4]), None).is_err()
        );
    }

    #[test]
    fn voter_change_check_allows_a_step_that_dilutes_an_existing_concentration() {
        // A one-region bootstrap growing out: {1,2,5} is 3 of 3 in a.
        let m = meta_with(&[(1, "a"), (2, "a"), (5, "a"), (3, "b"), (4, "c")]);
        // add b: 3 of 4 in a is still a majority but strictly less concentrated.
        assert!(
            control_voter_change_check(&m, &set(&[1, 2, 5]), &set(&[1, 2, 3, 5]), None).is_ok()
        );
        // add another a voter instead: more concentrated -> refused.
        assert!(
            control_voter_change_check(
                &m,
                &set(&[1, 2, 5]),
                &set(&[1, 2, 5, 6]),
                Some((&nid(6), &{
                    let mut l = BTreeMap::new();
                    l.insert(REGION_LABEL.to_string(), "a".to_string());
                    l
                }))
            )
            .is_err()
        );
    }

    #[test]
    fn voter_change_check_never_refuses_an_unlabelled_or_single_region_cluster() {
        let none = crate::meta::Metadata::default();
        assert!(control_voter_change_check(&none, &set(&[1]), &set(&[1, 2, 3]), None).is_ok());
        let one = meta_with(&[(1, "a"), (2, "a"), (3, "a")]);
        assert!(control_voter_change_check(&one, &set(&[1]), &set(&[1, 2, 3]), None).is_ok());
    }

    #[test]
    fn voter_change_check_counts_the_candidates_supplied_labels() {
        // Only one region is on file; the candidate brings the second.
        let m = meta_with(&[(1, "a"), (2, "a")]);
        let mut l = BTreeMap::new();
        l.insert(REGION_LABEL.to_string(), "b".to_string());
        // {1} -> {1,2,3}: a holds 2 of 3 (>half) and grew from 1 of 1... share fell
        // from 100% to 66%: allowed as a diluting step.
        assert!(
            control_voter_change_check(&m, &set(&[1]), &set(&[1, 2, 3]), Some((&nid(3), &l)))
                .is_ok()
        );
        // {1,3} -> {1,2,3} where 3 is b: a 2 of 3, before 1 of 2 (not a majority): refused.
        assert!(
            control_voter_change_check(&m, &set(&[1, 3]), &set(&[1, 2, 3]), Some((&nid(3), &l)))
                .is_err()
        );
    }
}
