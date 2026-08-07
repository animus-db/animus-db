//! Pure, side-effect-free decision logic for the CP data-plane's client
//! request **routing** (extracted from `lib.rs`).
//!
//! `animusd` is the one crate that runs real distributed-system decision logic
//! (routing, provisioning, hosting, GC) exclusively over `ProdEnv`, with no
//! sim/unit coverage of its own — every `animusd` test is a real-socket
//! integration test. This module pulls the pure *routing* decision (no
//! network/lock/disk access) out of that machinery so it can be unit-tested
//! directly, leaving `ClientCtx::resolve_cp_route` as thin `ProdEnv` wiring
//! that gathers inputs and executes the decision.
//!
//! **The per-node tablet hosting/GC decisions this module used to hold**
//! (`plan_join_host`, `tablets_to_reclaim`, `tablets_to_release`) **moved to
//! `animus_cp_data::host`** (ADR 0031 PR3/PR4): that crate's `plan` is now
//! the single pure decision behind the per-node tablet-host reconciler, and
//! its `Reconciler` executes the result — see `animus-cp-data/CLAUDE.md`'s
//! `host` module doc and `animusd/CLAUDE.md`'s tablet-host-reconciler entry.
//! Since ADR 0026 Stage B (stream-per-tablet addressing) a tablet's CP group
//! member id **is** simply the base `raftkv` id — the tablet axis lives in
//! the network `stream` and the `StorageScope` prefix/range, not in a
//! derived `NodeId` — so there is no more base↔member translation to keep
//! flat across split depth; see the root `CLAUDE.md` engineering-practices
//! entries for why that used to matter. The CP-route resolution below still
//! must never forward to a non-leader while a local replica is still forming.

use std::net::SocketAddr;

use animus_tablet::{Tablet, TabletId};

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
/// *it* is a replica (`is_replica`, e.g. its own tablet-host reconciler hasn't
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use animus_tablet::KeyRange;

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
    /// tablet-host reconciler hasn't stood the group up yet) must also wait — it
    /// must not guess at another node's address just because one is available.
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
}
