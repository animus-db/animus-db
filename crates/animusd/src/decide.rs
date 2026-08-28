//! Pure, side-effect-free decision predicates extracted from `impl ClientCtx`
//! (ADR 0061 Phase A rung A6 — the keystone: the first cut into the
//! 5,569-line brain, and the module Phase C's structural carve moves first).
//!
//! Every function here takes plain values (no `&self`, no `&CpGroup`, no
//! `ProdEnv`, no `tokio`) and returns a plain value — the same shape
//! [`crate::topology`] already established for CP-route resolution. A caller
//! in `lib.rs` gathers the (cheap, already-computed) facts a decision needs —
//! `leader.engine_applied_index()`, `leader.is_leader()`, `leader.is_frozen()`
//! — and calls the matching function here to make the actual decision, which
//! is then directly unit-testable without standing up a `CpGroup` (a real
//! `RaftKvNode<ProdEnv, _>` handle) at all.
//!
//! **What did *not* move here.** [`crate::topology::decide_cp_route`] already
//! owns CP leader-route resolution (`resolve_cp_route` fully delegates to
//! it) — nothing to extend. `ClientCtx::not_leader_refusal` is already thin
//! wiring over [`crate::topology::format_not_leader_refusal`] — nothing pure
//! left to pull out. See each function's own doc below for the handful of
//! candidates surveyed and found genuinely entangled (real Raft/lock state,
//! not just data) rather than pure.

use std::collections::{BTreeMap, BTreeSet};

use animus_control::Metadata;
use animus_env::NodeId;
use animus_tablet::{TOKEN_BYTES, TabletId};

use crate::ClientResponse;

/// ADR 0050 rung 5: the retryable refusal every mutating propose helper
/// returns for a frozen split parent (post-`Freeze`, pre-cutover). Ends in
/// `"; retry"` (the house retryability convention) so every existing client
/// retry loop re-resolves routing; distinct wording so tests/admin can tell
/// frozen from a fence/stale-routing refusal.
pub(crate) const FROZEN_REFUSAL: &str =
    "tablet frozen for split cutover (ADR 0050); a child will serve this range shortly; retry";

/// ADR 0050 rung 5: the shared pre-propose freeze refusal. A frozen split
/// parent (post-`KvCommand::Freeze`, pre-cutover/retire) refuses every
/// mutating propose with [`FROZEN_REFUSAL`], so the caller's ordinary retry
/// loop re-resolves routing and lands on a child once `CutoverSplit`
/// activates them — the same client shape as an election wait. Reads are
/// deliberately NOT gated (a frozen parent's state IS current until
/// cutover). The apply-time whole-range seal remains the backstop for the
/// propose-vs-apply sliver.
///
/// `is_frozen` is `CpGroup::is_frozen()`'s own value — a pure flag read on
/// the real handle; every caller in `lib.rs` reads it fresh immediately
/// before calling this.
pub(crate) fn frozen_refusal(is_frozen: bool) -> Result<(), String> {
    if is_frozen {
        return Err(FROZEN_REFUSAL.into());
    }
    Ok(())
}

/// Whether waiting any longer for `accepted_index`'s effect to appear can
/// still succeed — the confirm-side dual of `RaftKvNode::
/// wait_stage_outcome`'s own `!is_leader()` bail (ADR 0018 §2). Two futility
/// signals, either of which ends the wait:
///
/// - **the group has applied past the accepted entry's own log index without
///   the probed effect appearing** (`engine_applied_index >= accepted_index`
///   — the caller re-probes once after this returns `true`, closing the
///   probe-vs-apply race): whatever occupied that log position either
///   no-opped at apply (a freeze/seal miss, a failed `KindBatch` condition)
///   or is a different entry entirely (the accepted one was truncated by a
///   leadership change, and the new leader's election no-op has already
///   applied past it). Either way the effect will never appear from *this*
///   propose — only a fresh retry can land it;
/// - **this node no longer leads the group** (`!is_leader`): the accepted
///   entry may yet commit under the new leader (a retry is then a harmless
///   idempotent duplicate — per-key LWW converges), or it may have been
///   truncated — this node cannot tell which within bounded time, and the
///   caller's retry re-resolves routing to wherever the leader now is.
///
/// These confirm loops used to poll out the full `CLIENT_TIMEOUT` in both
/// states — correct, but a client-visible stall *per attempt* under
/// leadership churn (issue #268). A futile wait now fails fast with the
/// house retryable-error shape so the caller's own retry loop makes progress
/// instead. **Success still requires exact effect equality** — this coarser
/// signal only ever ends a wait, never acks one.
pub(crate) fn confirm_wait_is_futile(
    engine_applied_index: u64,
    is_leader: bool,
    accepted_index: u64,
) -> bool {
    engine_applied_index >= accepted_index || !is_leader
}

/// Whether a CP read error is a **transient routing/leadership/scope race**
/// the reader should retry with re-resolved routing (the `"; retry"` shape
/// every such error in this file carries), as opposed to a genuine failure
/// to surface. Shared by every CP read/write retry loop in `lib.rs`.
pub(crate) fn read_should_retry(e: &str) -> bool {
    e.ends_with("; retry")
}

/// Map a forwarded-op reply that should be a bare ack into `Result<(), String>`.
pub(crate) fn ok_or_err(resp: ClientResponse, what: &str) -> Result<(), String> {
    match resp {
        ClientResponse::PutOk => Ok(()),
        ClientResponse::Error(e) => Err(e),
        other => Err(format!("unexpected reply to {what}: {other:?}")),
    }
}

/// F11 (ADR 0042 §14, growth PR2): align a candidate split key to the token
/// boundary (`TOKEN_BYTES`) if `tablet`'s table is streamed, so one shard's
/// worth of a stream (which is keyed at token granularity) never ends up
/// straddling two post-split tablets. Also reports whether the (possibly
/// rounded) key is still a legal **interior** split point for `tablet`'s
/// current range (`KeyRange::split_at`'s own "strictly inside" rule).
/// Rounding a hot single-token partition's own key can collapse it onto
/// `range.start` — Fork E's accepted single-token hot-partition limit: one
/// very hot partition key ends up owning the tablet's entire range, and it
/// can never legally split without separating that same token's records
/// across siblings — the exact affinity F11 exists to protect. `viable ==
/// false` for an unknown tablet too (the caller's own subsequent lookup
/// reports that more precisely; this just never claims a key is fine for a
/// tablet this function can't even see).
pub(crate) fn align_split_key(
    meta: &Metadata,
    tablet: TabletId,
    split_key: Vec<u8>,
) -> (Vec<u8>, bool) {
    let Some(t) = meta.tablets.get(&tablet) else {
        return (split_key, false);
    };
    let streamed = t
        .table
        .as_deref()
        .is_some_and(|table| meta.table_stream(table).is_some());
    let key = if streamed {
        split_key[..TOKEN_BYTES.min(split_key.len())].to_vec()
    } else {
        split_key
    };
    let viable = t.range.split_at(&key).is_some();
    (key, viable)
}

/// ADR 0034: the key that roughly bisects `pairs`' total **bytes** (key +
/// value length), not just its position — the split point `auto_split_loop`
/// uses whenever a byte threshold is configured. With skewed value sizes a
/// plain positional median can leave one huge half and one tiny half, which
/// immediately re-triggers a split on the huge side instead of settling
/// below threshold.
///
/// Always returns an **interior** key (`i >= 1`, so never `pairs[0].0`,
/// matching the positional median's own "index > 0" guarantee). Requires
/// `pairs.len() >= 2` (the same precondition callers already check before
/// calling this — there is no meaningful split point for 0 or 1 keys).
pub(crate) fn byte_weighted_median(pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    debug_assert!(
        pairs.len() >= 2,
        "need >= 2 keys for an interior split point"
    );
    let total: u64 = pairs.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
    let half = total / 2;
    let mut best_idx = 1;
    let mut best_diff = u64::MAX;
    let mut prefix: u64 = 0; // bytes of pairs[0..i], updated *before* considering split `i`.
    for (i, (key, value)) in pairs.iter().enumerate() {
        if i >= 1 {
            let diff = prefix.abs_diff(half);
            if diff < best_diff {
                best_diff = diff;
                best_idx = i;
            }
        }
        prefix += (key.len() + value.len()) as u64;
    }
    pairs[best_idx].0.clone()
}

/// Another known client-API address for a tablet, distinct from every
/// address already in `tried` — the fallback
/// `ClientCtx::forward_to_tablet_leader`'s hinted retry chases once the
/// refusal's own leader hint is exhausted (already tried, or absent because
/// the refusing node's own replica was mid-election). Walks `replicas` in
/// order (callers pass a `Metadata`-derived, and therefore deterministic,
/// order); `None` once every known replica address has been tried (or none
/// has a known route at all).
pub(crate) fn other_tablet_replica_addr(
    replicas: &[NodeId],
    route: &BTreeMap<NodeId, String>,
    tried: &BTreeSet<String>,
) -> Option<String> {
    replicas
        .iter()
        .find_map(|id| route.get(id).cloned().filter(|a| !tried.contains(a)))
}

/// One step of `ClientCtx::forward_to_tablet_leader`'s hinted-retry loop —
/// the pure decision behind it, given the already-resolved `candidate`
/// address (the refusal's own leader hint if untried, else
/// [`other_tablet_replica_addr`]'s fallback, computed by the caller).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ForwardRetryStep {
    /// Retry the forwarded op at this address.
    Retry(String),
    /// Every known candidate for a **known** tablet refused with no leader
    /// to point at (`leader_hint=none`): the group is mid-election (a
    /// split-child/first-provision formation window, or a crashed leader).
    /// Back off, clear the tried-set, and run another pass — it resolves
    /// itself within an election timeout or two.
    WaitElection,
    /// No candidate left, and either the tablet itself couldn't be resolved
    /// (nothing to wait out) or waiting isn't applicable — surface the
    /// refusal as-is.
    GiveUp,
}

/// See [`ForwardRetryStep`]. `tablet_known` is `tablet.is_some()` at the call
/// site — only a resolvable tablet's group can be "mid-election"; an
/// unresolvable one has nothing to wait out.
pub(crate) fn decide_forward_retry(
    candidate: Option<String>,
    tablet_known: bool,
) -> ForwardRetryStep {
    match candidate {
        Some(a) => ForwardRetryStep::Retry(a),
        None if tablet_known => ForwardRetryStep::WaitElection,
        None => ForwardRetryStep::GiveUp,
    }
}

#[cfg(test)]
mod tests {
    use animus_control::{MetaCommand, StreamSpec, StreamViewType};
    use animus_env::nid;
    use animus_tablet::KeyRange;

    use super::*;

    // --- frozen_refusal ------------------------------------------------------

    #[test]
    fn frozen_refusal_ok_when_not_frozen() {
        assert_eq!(frozen_refusal(false), Ok(()));
    }

    #[test]
    fn frozen_refusal_errs_with_the_house_shape_when_frozen() {
        let err = frozen_refusal(true).unwrap_err();
        assert_eq!(err, FROZEN_REFUSAL);
        assert!(
            err.ends_with("; retry"),
            "must carry the house retryable shape so caller loops re-route"
        );
    }

    // --- confirm_wait_is_futile -----------------------------------------------

    #[test]
    fn confirm_wait_is_not_futile_while_leading_and_not_yet_applied() {
        assert!(!confirm_wait_is_futile(9, true, 10));
    }

    #[test]
    fn confirm_wait_is_futile_once_applied_past_the_accepted_index() {
        assert!(confirm_wait_is_futile(10, true, 10));
        assert!(confirm_wait_is_futile(11, true, 10));
    }

    #[test]
    fn confirm_wait_is_futile_once_leadership_is_lost_regardless_of_apply_progress() {
        // Not yet applied at all, but no longer leader: still futile.
        assert!(confirm_wait_is_futile(0, false, 10));
    }

    #[test]
    fn confirm_wait_is_futile_combines_both_signals_with_or() {
        // Applied past AND not leading: still futile (not exclusive).
        assert!(confirm_wait_is_futile(10, false, 10));
    }

    // --- read_should_retry -----------------------------------------------------

    #[test]
    fn read_should_retry_matches_the_house_retry_suffix() {
        assert!(read_should_retry("CP group leader moved; retry"));
    }

    #[test]
    fn read_should_retry_rejects_a_terminal_error() {
        assert!(!read_should_retry(
            "a condition on table `t` key was not met"
        ));
    }

    #[test]
    fn read_should_retry_requires_the_suffix_not_just_the_substring() {
        // "retry" appearing mid-message (not as the trailing shape) must not
        // count — only the exact house convention does.
        assert!(!read_should_retry("please retry later"));
    }

    // --- ok_or_err ---------------------------------------------------------

    #[test]
    fn ok_or_err_accepts_put_ok() {
        assert_eq!(ok_or_err(ClientResponse::PutOk, "test op"), Ok(()));
    }

    #[test]
    fn ok_or_err_passes_through_an_error_reply_verbatim() {
        assert_eq!(
            ok_or_err(ClientResponse::Error("boom".into()), "test op"),
            Err("boom".to_string())
        );
    }

    #[test]
    fn ok_or_err_rejects_an_unexpected_reply_shape() {
        let err = ok_or_err(ClientResponse::Value(None), "test op").unwrap_err();
        assert!(
            err.contains("test op"),
            "must name the op in the error: {err}"
        );
        assert!(err.contains("unexpected reply"));
    }

    // --- align_split_key -----------------------------------------------------
    //
    // Fuller coverage (streamed rounding, Fork E collapse, unknown tablet)
    // moved here verbatim from the pre-extraction `align_split_key_tests`
    // in-crate module below the fold.

    fn streamed_metadata_with_tablet(tablet: TabletId, range: KeyRange) -> Metadata {
        let mut m = Metadata::default();
        assert!(matches!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: animus_control::TableSchema::simple(
                    "pk",
                    animus_control::ColumnType::String
                ),
            }),
            animus_control::ApplyOutcome::Applied
        ));
        assert!(matches!(
            m.apply(&MetaCommand::SetTableStream {
                table: "orders".to_owned(),
                spec: Some(StreamSpec {
                    view_type: StreamViewType::NewAndOldImages,
                    label: "L1".to_owned(),
                }),
            }),
            animus_control::ApplyOutcome::Applied
        ));
        assert!(matches!(
            m.apply(&MetaCommand::CreateTablet {
                tablet,
                table: Some("orders".to_owned()),
                range,
                replicas: Vec::new(),
            }),
            animus_control::ApplyOutcome::Applied
        ));
        m
    }

    #[test]
    fn rounds_a_streamed_tables_key_down_to_the_token_boundary() {
        let tablet = TabletId(1);
        let m = streamed_metadata_with_tablet(tablet, KeyRange::whole());
        let raw = b"orders-mXX".to_vec(); // 10 bytes.
        let (rounded, viable) = align_split_key(&m, tablet, raw);
        assert_eq!(rounded, b"orders-m".to_vec());
        assert!(
            viable,
            "the rounded key is still strictly inside the whole range"
        );
    }

    #[test]
    fn leaves_an_unstreamed_tables_key_untouched() {
        let mut m = Metadata::default();
        assert!(matches!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("plain".to_owned()),
                range: KeyRange::whole(),
                replicas: Vec::new(),
            }),
            animus_control::ApplyOutcome::Applied
        ));
        let raw = b"any-length-key-at-all".to_vec();
        let (key, viable) = align_split_key(&m, TabletId(1), raw.clone());
        assert_eq!(key, raw);
        assert!(viable);
    }

    #[test]
    fn a_key_already_token_aligned_is_unchanged() {
        let tablet = TabletId(1);
        let m = streamed_metadata_with_tablet(tablet, KeyRange::whole());
        let raw = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();
        let (rounded, viable) = align_split_key(&m, tablet, raw.clone());
        assert_eq!(rounded, raw);
        assert!(viable);
    }

    #[test]
    fn reports_not_viable_when_the_rounded_key_collapses_onto_range_start() {
        let tablet = TabletId(2);
        let range = KeyRange {
            start: b"orders-m".to_vec(),
            end: None,
        };
        let m = streamed_metadata_with_tablet(tablet, range);
        let (rounded, viable) = align_split_key(&m, tablet, b"orders-mZZ".to_vec());
        assert_eq!(rounded, b"orders-m".to_vec());
        assert!(
            !viable,
            "a token-rounded key equal to the tablet's own range.start is not a legal split point"
        );
    }

    #[test]
    fn reports_not_viable_for_an_unknown_tablet() {
        let m = Metadata::default();
        let (key, viable) = align_split_key(&m, TabletId(999), b"whatever".to_vec());
        assert_eq!(key, b"whatever".to_vec());
        assert!(!viable);
    }

    // --- byte_weighted_median --------------------------------------------------

    fn pair(key: &str, value_len: usize) -> (Vec<u8>, Vec<u8>) {
        (key.as_bytes().to_vec(), vec![b'x'; value_len])
    }

    #[test]
    fn skewed_value_sizes_bisect_by_bytes_not_position() {
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> =
            (0..20).map(|i| pair(&format!("k{i:03}"), 1)).collect();
        pairs.push(pair("y0", 10_000));
        pairs.push(pair("y1", 10_000));

        let total: u64 = pairs.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
        let positional_median = pairs[pairs.len() / 2].0.clone();
        assert!(
            positional_median.starts_with(b"k"),
            "sanity: the plain positional median is a tiny-row key, not a \
             huge-row one, proving the two metrics genuinely disagree here"
        );

        let split = byte_weighted_median(&pairs);
        let split_idx = pairs
            .iter()
            .position(|(k, _)| k == &split)
            .expect("split key is one of the pairs");

        let left_bytes: u64 = pairs[..split_idx]
            .iter()
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum();
        let right_bytes: u64 = total - left_bytes;
        let half = total / 2;
        assert!(
            split_idx >= 20,
            "byte-weighted median (index {split_idx}) must fall at/after the \
             first huge value, not inside the tiny-row run — total={total}"
        );
        assert!(
            left_bytes >= half / 3 && right_bytes >= half / 3,
            "split should roughly halve bytes: left={left_bytes} right={right_bytes} half={half}"
        );
    }

    #[test]
    fn uniform_value_sizes_agree_with_positional_median() {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
            (0..10).map(|i| pair(&format!("k{i:03}"), 8)).collect();
        let positional = pairs[pairs.len() / 2].0.clone();
        let weighted = byte_weighted_median(&pairs);
        assert_eq!(
            weighted, positional,
            "uniform row sizes: byte-weighted and positional medians should coincide"
        );
    }

    #[test]
    fn never_returns_the_first_key() {
        let pairs = vec![pair("a", 100_000), pair("b", 1), pair("c", 1)];
        let split = byte_weighted_median(&pairs);
        assert_ne!(split, b"a".to_vec(), "must not return the first key");
    }

    // --- other_tablet_replica_addr / decide_forward_retry -----------------

    fn addr(port: u16) -> String {
        format!("127.0.0.1:{port}")
    }

    #[test]
    fn other_replica_addr_skips_already_tried_and_unrouted_replicas() {
        let replicas = vec![nid(1), nid(2), nid(3)];
        let mut route = BTreeMap::new();
        route.insert(nid(1), addr(1));
        route.insert(nid(3), addr(3));
        // nid(2) has no known route at all.
        let mut tried = BTreeSet::new();
        tried.insert(addr(1));

        assert_eq!(
            other_tablet_replica_addr(&replicas, &route, &tried),
            Some(addr(3)),
            "must skip the tried replica and the unrouted one, landing on the third"
        );
    }

    #[test]
    fn other_replica_addr_is_none_once_every_known_replica_is_tried() {
        let replicas = vec![nid(1), nid(2)];
        let mut route = BTreeMap::new();
        route.insert(nid(1), addr(1));
        route.insert(nid(2), addr(2));
        let mut tried = BTreeSet::new();
        tried.insert(addr(1));
        tried.insert(addr(2));

        assert_eq!(other_tablet_replica_addr(&replicas, &route, &tried), None);
    }

    #[test]
    fn other_replica_addr_is_none_for_an_empty_replica_set() {
        assert_eq!(
            other_tablet_replica_addr(&[], &BTreeMap::new(), &BTreeSet::new()),
            None
        );
    }

    #[test]
    fn forward_retry_retries_a_present_candidate() {
        assert_eq!(
            decide_forward_retry(Some(addr(9)), true),
            ForwardRetryStep::Retry(addr(9))
        );
        // Even for an unresolvable tablet, a candidate (e.g. the refusal's
        // own hint) still wins over giving up.
        assert_eq!(
            decide_forward_retry(Some(addr(9)), false),
            ForwardRetryStep::Retry(addr(9))
        );
    }

    #[test]
    fn forward_retry_waits_out_an_election_for_a_known_tablet_with_no_candidate() {
        assert_eq!(
            decide_forward_retry(None, true),
            ForwardRetryStep::WaitElection
        );
    }

    #[test]
    fn forward_retry_gives_up_when_the_tablet_itself_is_unresolvable() {
        assert_eq!(decide_forward_retry(None, false), ForwardRetryStep::GiveUp);
    }
}
