//! Pure, side-effect-free decision logic for the CP data-plane's routing and
//! hosting/GC predicates (extracted from `lib.rs`).
//!
//! `animusd` is the one crate that runs real distributed-system decision logic
//! (routing, provisioning, join-hosting, GC) exclusively over `ProdEnv`, with no
//! sim/unit coverage — every `animusd` test is a real-socket integration test.
//! This module pulls the pure *decisions* (no network/lock/disk access) out of
//! that machinery so they can be unit-tested directly, leaving the surrounding
//! `lib.rs` functions as thin `ProdEnv` wiring that gathers inputs and executes
//! the decision. Since ADR 0026 Stage B (stream-per-tablet addressing) a
//! tablet's CP group member id **is** simply the base `raftkv` id — the tablet
//! axis lives in the network `stream` and the `StorageScope` prefix/range, not
//! in a derived `NodeId` — so there is no more base↔member translation to keep
//! flat across split depth; see the root `CLAUDE.md` engineering-practices
//! entries for why that used to matter. The CP-route resolution below still
//! must never forward to a non-leader while a local replica is still forming.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use animus_env::NodeId;
use animus_tablet::{Epoch, Tablet, TabletId};

/// The tablet whose range contains `key`, chosen from `tablets` (already
/// filtered to one table, ADR 0023 table-scoped routing). Iteration order
/// determines the winner on an overlap; callers pass a `BTreeMap`-derived
/// iterator so the choice is deterministic on every node. `None` if no tablet in
/// `tablets` covers `key` (the caller waits — there is no whole-keyspace
/// fallback for table data).
pub(crate) fn tablet_for_key<'a>(
    tablets: impl Iterator<Item = (&'a TabletId, &'a Tablet)>,
    key: &[u8],
) -> Option<TabletId> {
    tablets
        .filter(|(_, t)| t.range.contains(key))
        .map(|(id, _)| *id)
        .next()
}

/// The outcome of resolving a CP op's leader route (the pure decision behind
/// `ClientCtx::resolve_cp_route`): serve **locally**, **forward** to a known
/// address, or **wait** for the local group to settle. Never forwards a CP op to
/// a node that may not host the leader yet — see [`decide_cp_route`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteDecision {
    /// This node hosts the tablet's current leader — serve from it directly.
    Local,
    /// Forward to the leader's node at this client-API address.
    Forward(SocketAddr),
    /// No leader reachable yet (no local leader, no route, election did not
    /// settle) — wait and retry, never forward blindly.
    Wait,
}

/// Decide how to route a CP op for a tablet whose leader isn't resolved yet,
/// given already-gathered facts about this node's local state:
///
/// - `has_local_leader` — this node hosts the tablet's current leader;
/// - `forward_hint` — a **known** forwarding address, resolved from a local
///   replica's leader hint (translated back to a base id and looked up in the
///   cross-process routing table) — `Some` only when a concrete leader is
///   already known;
/// - `has_local_replica` — this node hosts *any* local handle for the tablet's
///   group (leader or follower), regardless of whether it knows the leader;
/// - `is_replica` — this node's base id is in the tablet's replica set per
///   replicated `Metadata` (used only when there is no local handle at all);
/// - `fallback_forward` — a forwarding address toward *some* replica of the
///   tablet, used only as a last resort when this node is not itself a replica.
///
/// No network/lock/disk access — the caller (`ClientCtx::resolve_cp_route`)
/// gathers these from `ClusterEdgeState`/`Metadata` and executes the decision.
///
/// The critical rule this codifies (a known past bug class, "forwarded CP op:
/// not the leader here"): a node that already hosts *a* local replica handle for
/// the tablet (`has_local_replica`) but has no leader/hint yet is **mid-election
/// or mid-formation** — it must **wait**, never forward, because the only
/// "route" might be this very node and forwarding elsewhere just errors. Only a
/// node with **no** local handle at all considers forwarding, and even then, if
/// *it* is a replica (`is_replica`, e.g. its own join-host loop just hasn't
/// stood the group up yet), it must also wait rather than guess at another
/// node's address.
pub(crate) fn decide_cp_route(
    has_local_leader: bool,
    forward_hint: Option<SocketAddr>,
    has_local_replica: bool,
    is_replica: bool,
    fallback_forward: Option<SocketAddr>,
) -> RouteDecision {
    if has_local_leader {
        return RouteDecision::Local;
    }
    if let Some(addr) = forward_hint {
        return RouteDecision::Forward(addr);
    }
    if !has_local_replica {
        if is_replica {
            return RouteDecision::Wait;
        }
        if let Some(addr) = fallback_forward {
            return RouteDecision::Forward(addr);
        }
    }
    RouteDecision::Wait
}

/// This node's plan for join-hosting `tablet` (the pure decision behind
/// `cp_join_host_loop`), given its base `raftkv` id and the tablet's current
/// replica set / epoch from replicated `Metadata`. `None` means "not this
/// node's concern right now" — its base id is not in `replicas` at all.
///
/// Since a single-command (control-plane-only) split moves no data — a split
/// child's range is confined by its own `StorageScope` against the *same*
/// already-populated shared engine, not seeded from a handoff — a fresh split
/// child is formed exactly like a fresh whole-keyspace tablet: both get
/// `initial_formation: true`. (Contrast the old two-phase split, where a fresh
/// split child had to be *skipped* here and formed only via the data-plane
/// split hook's handoff, or an empty join-host start would have lost its data.)
///
/// This does **not** perform the per-node `minted` dedup claim (stateful, kept
/// in the caller), nor the `StorageScope::has_data` check the caller layers on
/// top to upgrade a *reforming-after-restart* join (this node already held
/// this tablet's data before the process restarted, so it needs the full
/// voter config to be able to re-elect immediately, even though `epoch` alone
/// would suggest "joining fresh") to `initial_formation: true` — that check
/// needs an async engine read, impure by construction, so it stays in
/// `cp_join_host`, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JoinHostPlan {
    /// `true` — a fresh tablet forming for the first time (whole-keyspace or a
    /// split child — both start from data already present in the shared
    /// engine, if any): start with the **full** voting config so a replica can
    /// campaign with no live leader. `false` — this node is *joining* an
    /// existing, already-led group (the reconciler placed it as a spare):
    /// start as a quiet **non-voter** until the leader adds it.
    pub(crate) initial_formation: bool,
}

pub(crate) fn plan_join_host(
    base_id: NodeId,
    replicas: &[NodeId],
    epoch: Epoch,
) -> Option<JoinHostPlan> {
    if !replicas.contains(&base_id) {
        return None;
    }
    Some(JoinHostPlan {
        initial_formation: epoch <= Epoch::INITIAL,
    })
}

/// Which of this node's `minted` tablets have been dropped from the replicated
/// tablet map and should be reclaimed (the pure predicate behind `cp_gc_loop`,
/// ADR 0024). A tablet is reclaimed iff it is in `minted` but absent from
/// `tablets`. The caller is responsible for the `last_applied == 0` recovery
/// guard (skip entirely before replicated `Metadata` has recovered, when an
/// empty default `Metadata` would otherwise read as "everything dropped") —
/// that gate gets a live `RaftNode` read the pure function has no business
/// taking, so it stays in `cp_gc_loop`.
pub(crate) fn tablets_to_reclaim(
    minted: &[TabletId],
    tablets: &BTreeMap<TabletId, Tablet>,
) -> Vec<TabletId> {
    minted
        .iter()
        .copied()
        .filter(|t| !tablets.contains_key(t))
        .collect()
}

/// Which of this node's `minted` tablets have had **this node** dropped from
/// their replica set while the tablet itself **still exists** — and so should be
/// *released* (stopped + its scope erased) on this node (the pure predicate
/// behind `cp_gc_loop`'s release phase, ADR 0029). The dual of
/// [`tablets_to_reclaim`]: reclaim fires on the tablet being **absent** (the
/// whole table was dropped, ADR 0024); release fires on the tablet being
/// **present** but no longer placing a replica on `base_id` (a drain, a
/// failure-repair swap, or an automatic rebalance moved it elsewhere).
///
/// A tablet is released iff it is in `minted` (this node hosts/hosted it) AND
/// present in `tablets` (still exists) AND `base_id` is **not** in that tablet's
/// `replicas` (this node is no longer supposed to be a replica).
///
/// The two predicates are **mutually exclusive** on the same input: reclaim
/// requires absence, release requires presence, so no tablet is ever both. The
/// caller layers the same `last_applied == 0` recovery guard on top (skip while
/// replicated `Metadata` hasn't recovered) and — critically — an independent
/// per-tablet check that this node's *own durable Raft log* config already
/// excludes `base_id` before acting, so a replay transient in `tablets` can't
/// erase live data (that check reads a live handle the pure function has no
/// business taking, so it stays in `cp_gc_loop`).
pub(crate) fn tablets_to_release(
    minted: &[TabletId],
    tablets: &BTreeMap<TabletId, Tablet>,
    base_id: NodeId,
) -> Vec<TabletId> {
    minted
        .iter()
        .copied()
        .filter(|t| {
            tablets
                .get(t)
                .is_some_and(|tab| !tab.replicas.contains(&base_id))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_tablet::KeyRange;

    const BASE: NodeId = 300;

    // --- tablet_for_key ------------------------------------------------------

    fn tablet(id: u64, start: &[u8], end: Option<&[u8]>) -> Tablet {
        Tablet::new(
            TabletId(id),
            KeyRange::new(start.to_vec(), end.map(|e| e.to_vec())),
            vec![300],
        )
    }

    #[test]
    fn tablet_for_key_finds_the_owning_range() {
        let map: BTreeMap<TabletId, Tablet> = [
            (TabletId(1), tablet(1, b"", Some(b"m"))),
            (TabletId(2), tablet(2, b"m", None)),
        ]
        .into_iter()
        .collect();

        assert_eq!(tablet_for_key(map.iter(), b"a"), Some(TabletId(1)));
        assert_eq!(tablet_for_key(map.iter(), b"z"), Some(TabletId(2)));
    }

    #[test]
    fn tablet_for_key_respects_half_open_boundaries() {
        let map: BTreeMap<TabletId, Tablet> = [
            (TabletId(1), tablet(1, b"", Some(b"m"))),
            (TabletId(2), tablet(2, b"m", None)),
        ]
        .into_iter()
        .collect();

        // The boundary key belongs to the right-hand (inclusive-start) tablet.
        assert_eq!(tablet_for_key(map.iter(), b"m"), Some(TabletId(2)));
        // Just below the boundary still belongs to the left-hand tablet.
        assert_eq!(tablet_for_key(map.iter(), b"lzzz"), Some(TabletId(1)));
    }

    #[test]
    fn tablet_for_key_handles_token_prefixed_keys() {
        // ADR 0022: real keys are `token || escape(pk) || rk`, i.e. arbitrary
        // binary (often non-printable) bytes as the leading token — exercise a
        // range split on a binary boundary, not just ASCII.
        let boundary: Vec<u8> = vec![0x7f, 0x00, 0x10];
        let map: BTreeMap<TabletId, Tablet> = [
            (TabletId(1), tablet(1, b"", Some(&boundary))),
            (TabletId(2), tablet(2, &boundary, None)),
        ]
        .into_iter()
        .collect();

        let low = vec![0x10, 0xff];
        let high = vec![0x7f, 0x00, 0x10, 0x01];
        assert_eq!(tablet_for_key(map.iter(), &low), Some(TabletId(1)));
        assert_eq!(tablet_for_key(map.iter(), &boundary), Some(TabletId(2)));
        assert_eq!(tablet_for_key(map.iter(), &high), Some(TabletId(2)));
    }

    #[test]
    fn tablet_for_key_is_none_when_unprovisioned() {
        let map: BTreeMap<TabletId, Tablet> = BTreeMap::new();
        assert_eq!(tablet_for_key(map.iter(), b"anything"), None);
    }

    // --- decide_cp_route -------------------------------------------------------

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn route_serves_locally_when_leader_is_hosted() {
        // has_local_leader wins regardless of anything else.
        assert_eq!(
            decide_cp_route(true, Some(addr(1)), true, true, Some(addr(2))),
            RouteDecision::Local
        );
        assert_eq!(
            decide_cp_route(true, None, false, false, None),
            RouteDecision::Local
        );
    }

    #[test]
    fn route_forwards_to_a_known_leader_hint() {
        assert_eq!(
            decide_cp_route(false, Some(addr(7)), false, false, None),
            RouteDecision::Forward(addr(7))
        );
        // Even when this node has a local (non-leader) replica handle, a concrete
        // hint still wins over waiting.
        assert_eq!(
            decide_cp_route(false, Some(addr(7)), true, true, None),
            RouteDecision::Forward(addr(7))
        );
    }

    /// The documented bug class: a node hosting *a* local replica handle (leader
    /// unknown — mid-election/mid-formation) must **wait**, never forward,
    /// because the only real "route" might be this very node.
    #[test]
    fn route_waits_when_local_replica_is_still_forming() {
        assert_eq!(
            decide_cp_route(false, None, true, true, Some(addr(9))),
            RouteDecision::Wait
        );
        assert_eq!(
            decide_cp_route(false, None, true, false, None),
            RouteDecision::Wait
        );
    }

    /// A node that is itself a replica but hosts no local handle at all (its own
    /// join-host loop hasn't stood the group up yet) must also wait — it must not
    /// guess at another node's address just because one is available.
    #[test]
    fn route_waits_when_unhosted_but_a_replica_of_the_tablet() {
        assert_eq!(
            decide_cp_route(false, None, false, true, Some(addr(9))),
            RouteDecision::Wait
        );
    }

    #[test]
    fn route_forwards_anywhere_when_not_a_replica_at_all() {
        assert_eq!(
            decide_cp_route(false, None, false, false, Some(addr(9))),
            RouteDecision::Forward(addr(9))
        );
    }

    #[test]
    fn route_waits_when_nothing_is_known_at_all() {
        assert_eq!(
            decide_cp_route(false, None, false, false, None),
            RouteDecision::Wait
        );
    }

    // --- plan_join_host --------------------------------------------------------

    #[test]
    fn join_host_skips_a_non_replica() {
        assert_eq!(plan_join_host(BASE, &[301, 302], Epoch::INITIAL), None);
    }

    /// A tablet at `Epoch::INITIAL` always forms fresh with the full voter
    /// config — whether it's a genuinely new whole-keyspace tablet or a
    /// single-command split's child. Unlike the old two-phase split (where a
    /// fresh split child had to be *skipped* here and formed only via the
    /// data-plane split hook's handoff), a single-command split moves no
    /// data — a split child's `StorageScope` is simply confined to its own
    /// range against the same already-populated shared engine — so there is
    /// no `range` parameter left to distinguish the two cases by; both are
    /// the same decision.
    #[test]
    fn join_host_forms_a_fresh_tablet_whole_or_split_child_the_same_way() {
        assert_eq!(
            plan_join_host(BASE, &[BASE], Epoch::INITIAL),
            Some(JoinHostPlan {
                initial_formation: true
            })
        );
    }

    #[test]
    fn join_host_joins_an_existing_group_as_non_voter() {
        // A bumped epoch means the reconciler placed this node into an
        // existing, already-led group.
        assert_eq!(
            plan_join_host(BASE, &[BASE], Epoch::INITIAL.next()),
            Some(JoinHostPlan {
                initial_formation: false
            })
        );
    }

    // --- tablets_to_reclaim ------------------------------------------------------

    #[test]
    fn reclaims_a_minted_tablet_absent_from_the_map() {
        let tablets: BTreeMap<TabletId, Tablet> =
            [(TabletId(1), tablet(1, b"", None))].into_iter().collect();
        let minted = [TabletId(1), TabletId(2)];
        assert_eq!(tablets_to_reclaim(&minted, &tablets), vec![TabletId(2)]);
    }

    #[test]
    fn does_not_reclaim_a_still_present_tablet() {
        let tablets: BTreeMap<TabletId, Tablet> = [
            (TabletId(1), tablet(1, b"", None)),
            (TabletId(2), tablet(2, b"", None)),
        ]
        .into_iter()
        .collect();
        let minted = [TabletId(1), TabletId(2)];
        assert_eq!(
            tablets_to_reclaim(&minted, &tablets),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn reclaim_over_empty_minted_set_is_empty() {
        let tablets: BTreeMap<TabletId, Tablet> = BTreeMap::new();
        assert_eq!(tablets_to_reclaim(&[], &tablets), Vec::<TabletId>::new());
    }

    // --- tablets_to_release ------------------------------------------------------

    /// A tablet built with an explicit replica set (rather than the default
    /// `vec![300]` of the `tablet` helper above).
    fn tablet_with_replicas(id: u64, replicas: Vec<NodeId>) -> Tablet {
        Tablet::new(TabletId(id), KeyRange::new(b"".to_vec(), None), replicas)
    }

    #[test]
    fn release_over_empty_minted_set_is_empty() {
        let tablets: BTreeMap<TabletId, Tablet> =
            [(TabletId(1), tablet_with_replicas(1, vec![BASE]))]
                .into_iter()
                .collect();
        assert_eq!(
            tablets_to_release(&[], &tablets, BASE),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn does_not_release_a_tablet_this_node_is_still_a_replica_of() {
        let tablets: BTreeMap<TabletId, Tablet> =
            [(TabletId(1), tablet_with_replicas(1, vec![BASE, 301, 302]))]
                .into_iter()
                .collect();
        assert_eq!(
            tablets_to_release(&[TabletId(1)], &tablets, BASE),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn releases_a_minted_present_tablet_this_node_is_no_longer_a_replica_of() {
        // The tablet still exists, but its replica set has moved off this node.
        let tablets: BTreeMap<TabletId, Tablet> =
            [(TabletId(1), tablet_with_replicas(1, vec![301, 302, 303]))]
                .into_iter()
                .collect();
        assert_eq!(
            tablets_to_release(&[TabletId(1)], &tablets, BASE),
            vec![TabletId(1)]
        );
    }

    #[test]
    fn does_not_release_a_minted_tablet_that_is_absent() {
        // Absence is `tablets_to_reclaim`'s job, not release's — a tablet dropped
        // from the map entirely must NOT be released by this predicate.
        let tablets: BTreeMap<TabletId, Tablet> = BTreeMap::new();
        assert_eq!(
            tablets_to_release(&[TabletId(1)], &tablets, BASE),
            Vec::<TabletId>::new()
        );
    }

    /// The two GC predicates partition this node's `minted` set: on the same
    /// input, no tablet is ever both reclaimed and released (reclaim requires
    /// absence, release requires presence + not-a-replica).
    #[test]
    fn reclaim_and_release_are_mutually_exclusive() {
        // Tablet 1: present, still a replica  -> neither.
        // Tablet 2: present, no longer a replica -> release only.
        // Tablet 3: absent -> reclaim only.
        let tablets: BTreeMap<TabletId, Tablet> = [
            (TabletId(1), tablet_with_replicas(1, vec![BASE, 301])),
            (TabletId(2), tablet_with_replicas(2, vec![301, 302])),
        ]
        .into_iter()
        .collect();
        let minted = [TabletId(1), TabletId(2), TabletId(3)];

        let reclaim = tablets_to_reclaim(&minted, &tablets);
        let release = tablets_to_release(&minted, &tablets, BASE);

        assert_eq!(reclaim, vec![TabletId(3)]);
        assert_eq!(release, vec![TabletId(2)]);
        // Disjoint: nothing appears in both.
        assert!(
            reclaim.iter().all(|t| !release.contains(t)),
            "reclaim {reclaim:?} and release {release:?} must be disjoint"
        );
    }
}
