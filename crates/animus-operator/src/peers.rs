//! The `PeerReachable` status condition (ADR 0075 section 5.4, G-01 stage
//! G-e): peer-cluster health, derived from the MREC shipper health every
//! animusd node reports on `GET /admin/global-tables`.
//!
//! The operator does not drive replica creation (the database does, through
//! `ReplicaUpdates`) and does not dial peers itself; it only reads what the
//! pods already measure. Shipper health is **node-local** (a node reports the
//! shippers of the tablets it leads), so [`evaluate`] aggregates the answers
//! of every pod. It is a pure function so the semantics are unit-tested
//! without a cluster.
//!
//! # Definition
//!
//! Per configured peer region, over every shipper entry that names it (any
//! pod, any MREC table, any tablet):
//!
//! - **no entry** (no MREC table replicates with the peer yet, or the pods
//!   that lead its tablets did not answer): `Unknown`.
//! - **healthy** if at least one entry has no `last_error` and is either
//!   `caught_up` or acknowledged within [`PEER_ACK_BOUND_MS`]: `True`.
//! - otherwise (every entry errors, or none acknowledged within the bound):
//!   `False`, naming the first `last_error` or the staleness.
//!
//! The condition is `False` if any peer is `False`, else `Unknown` if any is
//! `Unknown` (or no pod answered at all), else `True`. There is no reachability
//! signal without an MREC table: a peer with TLS or network trouble but no
//! table shows `Unknown`, not `False`, by design.

use serde_json::Value;

use crate::crd::ConditionStatus;

/// A shipper whose last acknowledgment is older than this (and that is not
/// `caught_up`) counts as unreachable. Five minutes: well above the shipper's
/// backoff ceiling, so a flapping link does not flip the condition.
pub const PEER_ACK_BOUND_MS: u64 = 300_000;

/// One peer's verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerVerdict {
    pub region: String,
    pub status: ConditionStatus,
    pub detail: String,
}

/// The aggregate condition: its status and message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerReachability {
    pub status: ConditionStatus,
    pub message: String,
    pub peers: Vec<PeerVerdict>,
}

/// Evaluate peer health. `views[i]` is pod `i`'s `/admin/global-tables` answer,
/// `None` when the pod did not answer.
#[must_use]
pub fn evaluate(peers: &[String], views: &[Option<Value>], ack_bound_ms: u64) -> PeerReachability {
    if views.iter().all(Option::is_none) {
        return PeerReachability {
            status: ConditionStatus::Unknown,
            message: "no pod's /admin/global-tables answered".to_string(),
            peers: Vec::new(),
        };
    }
    let verdicts: Vec<PeerVerdict> = peers
        .iter()
        .map(|region| verdict_for(region, views, ack_bound_ms))
        .collect();
    let status = if verdicts.iter().any(|v| v.status == ConditionStatus::False) {
        ConditionStatus::False
    } else if verdicts
        .iter()
        .any(|v| v.status == ConditionStatus::Unknown)
    {
        ConditionStatus::Unknown
    } else {
        ConditionStatus::True
    };
    let message = verdicts
        .iter()
        .map(|v| format!("{}: {}", v.region, v.detail))
        .collect::<Vec<_>>()
        .join("; ");
    PeerReachability {
        status,
        message,
        peers: verdicts,
    }
}

fn verdict_for(region: &str, views: &[Option<Value>], ack_bound_ms: u64) -> PeerVerdict {
    let mut seen = 0usize;
    let mut first_error: Option<String> = None;
    let mut stalest: Option<u64> = None;
    for view in views.iter().flatten() {
        let Some(tables) = view.get("tables").and_then(Value::as_array) else {
            continue;
        };
        for shipper in tables
            .iter()
            .filter_map(|t| t.get("shippers").and_then(Value::as_array))
            .flatten()
        {
            if shipper.get("peer").and_then(Value::as_str) != Some(region) {
                continue;
            }
            seen += 1;
            let error = shipper
                .get("last_error")
                .and_then(Value::as_str)
                .map(str::to_string);
            let caught_up = shipper
                .get("caught_up")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let age = shipper.get("last_ack_age_ms").and_then(Value::as_u64);
            if error.is_none() && (caught_up || age.is_some_and(|a| a <= ack_bound_ms)) {
                return PeerVerdict {
                    region: region.to_string(),
                    status: ConditionStatus::True,
                    detail: "replication to the peer is acknowledging".to_string(),
                };
            }
            if first_error.is_none() {
                first_error = error;
            }
            if let Some(a) = age {
                stalest = Some(stalest.map_or(a, |s| s.min(a)));
            }
        }
    }
    if seen == 0 {
        return PeerVerdict {
            region: region.to_string(),
            status: ConditionStatus::Unknown,
            detail: "no MREC table replicates with this peer yet".to_string(),
        };
    }
    let detail = match (first_error, stalest) {
        (Some(e), _) => format!("shipper error: {e}"),
        (None, Some(a)) => format!(
            "no acknowledgment for {}s (bound {}s)",
            a / 1000,
            ack_bound_ms / 1000
        ),
        (None, None) => "no acknowledgment yet".to_string(),
    };
    PeerVerdict {
        region: region.to_string(),
        status: ConditionStatus::False,
        detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn view(shippers: Value) -> Option<Value> {
        Some(json!({"enabled": true, "tables": [{"table": "t", "shippers": shippers}]}))
    }

    fn peers() -> Vec<String> {
        vec!["eu".to_string()]
    }

    #[test]
    fn no_pod_answering_is_unknown() {
        let r = evaluate(&peers(), &[None, None], PEER_ACK_BOUND_MS);
        assert_eq!(r.status, ConditionStatus::Unknown);
    }

    #[test]
    fn no_mrec_table_is_unknown_not_false() {
        let r = evaluate(
            &peers(),
            &[Some(json!({"enabled": true, "tables": []}))],
            PEER_ACK_BOUND_MS,
        );
        assert_eq!(r.status, ConditionStatus::Unknown);
        assert!(r.message.contains("no MREC table"), "{}", r.message);
    }

    #[test]
    fn a_caught_up_shipper_without_error_is_true() {
        let v = view(json!([{"peer": "eu", "caught_up": true, "last_error": null,
                             "last_ack_age_ms": 9_000_000}]));
        let r = evaluate(&peers(), &[v], PEER_ACK_BOUND_MS);
        assert_eq!(r.status, ConditionStatus::True);
    }

    #[test]
    fn a_recent_ack_without_error_is_true_and_a_stale_one_is_false() {
        let fresh = view(
            json!([{"peer": "eu", "caught_up": false, "last_error": null,
                                 "last_ack_age_ms": 1000}]),
        );
        assert_eq!(
            evaluate(&peers(), &[fresh], PEER_ACK_BOUND_MS).status,
            ConditionStatus::True
        );
        let stale = view(
            json!([{"peer": "eu", "caught_up": false, "last_error": null,
                                 "last_ack_age_ms": 600_000}]),
        );
        let r = evaluate(&peers(), &[stale], PEER_ACK_BOUND_MS);
        assert_eq!(r.status, ConditionStatus::False);
        assert!(
            r.message.contains("no acknowledgment for 600s"),
            "{}",
            r.message
        );
    }

    #[test]
    fn an_erroring_shipper_is_false_and_names_the_error() {
        let v = view(json!([{"peer": "eu", "caught_up": false,
                             "last_error": "tls handshake failed", "last_ack_age_ms": 10}]));
        let r = evaluate(&peers(), &[v], PEER_ACK_BOUND_MS);
        assert_eq!(r.status, ConditionStatus::False);
        assert!(r.message.contains("tls handshake failed"), "{}", r.message);
    }

    #[test]
    fn one_healthy_shipper_among_failing_ones_is_true() {
        let a = view(json!([{"peer": "eu", "last_error": "boom", "caught_up": false}]));
        let b = view(json!([{"peer": "eu", "last_error": null, "caught_up": true}]));
        assert_eq!(
            evaluate(&peers(), &[a, None, b], PEER_ACK_BOUND_MS).status,
            ConditionStatus::True
        );
    }

    #[test]
    fn any_false_peer_dominates_and_unknown_beats_true() {
        let eu_ok = json!({"peer": "eu", "last_error": null, "caught_up": true});
        let ap_bad = json!({"peer": "ap", "last_error": "refused", "caught_up": false});
        let two = vec!["eu".to_string(), "ap".to_string()];
        let both = view(json!([eu_ok.clone(), ap_bad]));
        assert_eq!(
            evaluate(&two, &[both], PEER_ACK_BOUND_MS).status,
            ConditionStatus::False
        );
        let only_eu = view(json!([eu_ok]));
        assert_eq!(
            evaluate(&two, &[only_eu], PEER_ACK_BOUND_MS).status,
            ConditionStatus::Unknown
        );
    }

    #[test]
    fn other_peers_shippers_are_ignored() {
        let v = view(json!([{"peer": "ap", "last_error": null, "caught_up": true}]));
        assert_eq!(
            evaluate(&peers(), &[v], PEER_ACK_BOUND_MS).status,
            ConditionStatus::Unknown
        );
    }
}
