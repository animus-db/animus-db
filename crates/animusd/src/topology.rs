//! Pure, side-effect-free decision logic for the CP data-plane's routing, id
//! translation, and hosting/GC predicates (extracted from `lib.rs`).
//!
//! `animusd` is the one crate that runs real distributed-system decision logic
//! (routing, provisioning, join-hosting, GC, id translation, split
//! orchestration) exclusively over `ProdEnv`, with no sim/unit coverage — every
//! `animusd` test is a real-socket integration test. This module pulls the pure
//! *decisions* (no network/lock/disk access) out of that machinery so they can be
//! unit-tested directly, leaving the surrounding `lib.rs` functions as thin
//! `ProdEnv` wiring that gathers inputs and executes the decision. See the root
//! `CLAUDE.md` engineering-practices entries this module's tests specifically
//! guard against: the base↔member id derivation must be **flat** (stable at any
//! split depth, not compounding through a parent), and the CP-route resolution
//! must never forward to a non-leader while a local replica is still forming.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use animus_env::NodeId;
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};

use crate::{CP_SPLIT_ID_STRIDE, TABLET};

/// This node's CP group **member id** for `tablet`, derived **flatly** from its
/// base `raftkv` id: the bootstrap tablet uses the base id; any split-created
/// tablet uses `base + tablet * CP_SPLIT_ID_STRIDE`. Flat (always from the base
/// id, not the parent's member id) so the derivation is identical at any split
/// depth and matches [`cp_members_for`] — a grandchild's member is
/// `base + grandchild * STRIDE`, not a compounding
/// `base + parent*STRIDE + grandchild*STRIDE`, so the reconfigure loop's
/// translated `desired` set always matches the running group's `config()`.
pub(crate) fn cp_member_id(base: NodeId, tablet: TabletId) -> NodeId {
    if tablet == TABLET {
        base
    } else {
        base + tablet.0 * CP_SPLIT_ID_STRIDE
    }
}

/// The inverse of [`cp_member_id`]: recover the stable **base** `raftkv` id from a
/// tablet group **member id**. Needed wherever a group-internal id (e.g. the
/// leader hint a local replica reports) must be resolved against state keyed by
/// base ids (`client_route`, `Metadata.members`, `tablets[t].replicas`). For the
/// bootstrap tablet member == base, which is why a missing reverse translation
/// *works* there and only breaks for derived-id tablets (every provisioned table
/// tablet and split child) — the bug class behind "no CP group leader reachable"
/// on a healthy group.
pub(crate) fn cp_base_id(member: NodeId, tablet: TabletId) -> NodeId {
    if tablet == TABLET {
        member
    } else {
        member - tablet.0 * CP_SPLIT_ID_STRIDE
    }
}

/// Translate a tablet's replica set — recorded in `Metadata.tablets[t].replicas`
/// as stable **base** `raftkv` ids (the node identities placement + failure
/// detection speak) — into that tablet's CP **group member ids**. The bootstrap
/// tablet's group uses the base ids directly; a split-created tablet's group uses
/// the derived `base + tablet * CP_SPLIT_ID_STRIDE`. This is the single source of
/// the base↔member mapping, so the reconfigure loop's `desired` matches the
/// running group's `config()` exactly (no spurious churn) — which is why the
/// replicated map can stay in base ids rather than being reconciled to the
/// derived member ids.
pub(crate) fn cp_members_for(
    tablet: TabletId,
    replicas: &[NodeId],
) -> std::collections::BTreeSet<NodeId> {
    replicas
        .iter()
        .map(|&base| cp_member_id(base, tablet))
        .collect()
}

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
/// replica set / epoch / range from replicated `Metadata`. `None` means "not
/// this node's concern right now":
///
/// - this node's base id is not in `replicas` at all, or
/// - the tablet is a **fresh split child** (`epoch <= Epoch::INITIAL` and a
///   non-whole range) — its data arrives via the split hook's handoff, so
///   starting an empty group here would lose it (ADR 0017 D1's join-vs-seed
///   distinction).
///
/// This does **not** perform the per-node `minted` dedup claim — that is
/// stateful (a mutable claim set) and stays in the caller, applied only to
/// tablets this returns `Some` for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JoinHostPlan {
    /// `true` — a fresh tablet forming for the first time (a `CreateTable`
    /// tablet at `INITIAL` with the whole range, or a restart of a tablet this
    /// node already hosts): start with the **full** voting config so a replica
    /// can campaign with no live leader. `false` — this node is *joining* an
    /// existing, already-led group (the reconciler placed it as a spare): start
    /// as a quiet **non-voter** until the leader adds it.
    pub(crate) initial_formation: bool,
}

pub(crate) fn plan_join_host(
    base_id: NodeId,
    replicas: &[NodeId],
    epoch: Epoch,
    range: &KeyRange,
) -> Option<JoinHostPlan> {
    if !replicas.contains(&base_id) {
        return None;
    }
    if epoch <= Epoch::INITIAL && *range != KeyRange::whole() {
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

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: NodeId = 300;

    // --- cp_member_id / cp_base_id -----------------------------------------

    #[test]
    fn member_id_identity_for_bootstrap_tablet() {
        // The bootstrap tablet (id 1) hosts on the node's main env: member == base.
        assert_eq!(cp_member_id(BASE, TABLET), BASE);
        assert_eq!(cp_base_id(BASE, TABLET), BASE);
    }

    #[test]
    fn member_id_derives_for_a_first_level_split() {
        let child = TabletId(2);
        let member = cp_member_id(BASE, child);
        assert_eq!(member, BASE + 2 * CP_SPLIT_ID_STRIDE);
        // Round-trips back to the base id.
        assert_eq!(cp_base_id(member, child), BASE);
    }

    /// Depth >= 2 (root CLAUDE.md: prove a recursive derivation at depth >= 2, not
    /// just once). A grandchild's member id must be derived **flatly** from the
    /// base id — `base + grandchild*STRIDE` — never compounding through the
    /// parent's already-derived member id (`parent_member + grandchild*STRIDE`),
    /// which would diverge from `cp_members_for`'s flat translation and make the
    /// reconfigure loop churn forever on a mismatch (the exact bug class recorded
    /// in the root CLAUDE.md's "prove recursive invariants at depth >= 2" entry).
    #[test]
    fn member_id_is_flat_at_split_depth_two() {
        let parent = TabletId(2);
        let grandchild = TabletId(5);

        let parent_member = cp_member_id(BASE, parent);
        let grandchild_member = cp_member_id(BASE, grandchild);

        // Flat: derived straight from the base id.
        assert_eq!(grandchild_member, BASE + 5 * CP_SPLIT_ID_STRIDE);
        // NOT compounding through the parent's member id.
        assert_ne!(
            grandchild_member,
            parent_member + grandchild.0 * CP_SPLIT_ID_STRIDE
        );
        // Both still invert correctly back to the same base id.
        assert_eq!(cp_base_id(parent_member, parent), BASE);
        assert_eq!(cp_base_id(grandchild_member, grandchild), BASE);
    }

    #[test]
    fn members_for_translates_a_whole_replica_set() {
        let replicas = [300, 301, 302];
        let bootstrap = cp_members_for(TABLET, &replicas);
        assert_eq!(
            bootstrap,
            [300, 301, 302]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );

        let split_child = TabletId(2);
        let derived = cp_members_for(split_child, &replicas);
        let expected: std::collections::BTreeSet<NodeId> = replicas
            .iter()
            .map(|&b| b + 2 * CP_SPLIT_ID_STRIDE)
            .collect();
        assert_eq!(derived, expected);
    }

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
        assert_eq!(
            plan_join_host(BASE, &[301, 302], Epoch::INITIAL, &KeyRange::whole()),
            None
        );
    }

    #[test]
    fn join_host_skips_a_fresh_split_child() {
        // INITIAL epoch + a non-whole range: this node's data arrives via the
        // split hook's handoff, not an empty join-host start.
        let range = KeyRange::new(b"m".to_vec(), None);
        assert_eq!(plan_join_host(BASE, &[BASE], Epoch::INITIAL, &range), None);
    }

    #[test]
    fn join_host_forms_a_fresh_whole_keyspace_tablet() {
        assert_eq!(
            plan_join_host(BASE, &[BASE], Epoch::INITIAL, &KeyRange::whole()),
            Some(JoinHostPlan {
                initial_formation: true
            })
        );
    }

    #[test]
    fn join_host_joins_an_existing_group_as_non_voter() {
        // A bumped epoch means the reconciler placed this node into an existing,
        // already-led group — even a non-whole range is fine here (unlike the
        // INITIAL case, this is not a fresh split child).
        let range = KeyRange::new(b"m".to_vec(), None);
        assert_eq!(
            plan_join_host(BASE, &[BASE], Epoch::INITIAL.next(), &range),
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
}
