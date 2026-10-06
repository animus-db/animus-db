//! Adapters from the admin endpoints' JSON bodies to an [`Observation`]:
//! `GET /admin/cluster-version` (the view, incl. its `roll` object) and `GET
//! /admin/roll-health`. Tolerant by design: a missing optional field is
//! "unknown", never a panic, because the observer may be talking to a
//! previous-release node that predates the field or the endpoint.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::Value;

use crate::{Blocker, Health, NodeObs, Observation, Platform, Reason, Role};

/// Parse one `roll-health` body (or the `roll.health` summary embedded in
/// `cluster-version`: same `ok` + `reasons` shape).
pub fn parse_health(v: &Value) -> Health {
    match v.get("ok").and_then(Value::as_bool) {
        Some(true) => Health::Ok,
        Some(false) => Health::NotOk(parse_reasons(v)),
        None => Health::Unavailable,
    }
}

fn parse_reasons(v: &Value) -> Vec<Reason> {
    v.get("reasons")
        .and_then(Value::as_array)
        .map(|rs| {
            rs.iter()
                .map(|r| Reason {
                    kind: r
                        .get("kind")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string(),
                    node: r.get("node").and_then(Value::as_str).map(str::to_string),
                    tablet: r.get("tablet").and_then(Value::as_u64),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_role(s: &str) -> Role {
    match s.to_ascii_lowercase().as_str() {
        "data" => Role::Data,
        "control" => Role::Control,
        _ => Role::Combined,
    }
}

/// What the caller knows beyond the cluster-version view.
#[derive(Clone, Debug, Default)]
pub struct Inputs {
    /// The cluster version this roll finalizes to (fixed for the whole roll).
    pub goal: u32,
    /// Platform fact per node id; a missing node is [`Platform::Old`].
    pub platform: BTreeMap<String, Platform>,
    /// `roll-health` per node id; a missing node is [`Health::Unavailable`]
    /// (callers pass [`Health::Unreachable`] for a node they failed to ask).
    pub health: BTreeMap<String, Health>,
    pub control_leader: Option<String>,
    pub in_flight_for: Option<Duration>,
    pub settled_for: Option<Duration>,
}

/// Build an [`Observation`] from a `cluster-version` body and the caller's own
/// facts. `Err` only when the body is not a cluster-version view at all.
pub fn observation(view: &Value, inputs: &Inputs) -> Result<Observation, String> {
    let nodes = view
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| "cluster-version body has no `nodes` array".to_string())?;
    let era_active = view
        .get("era_active")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let active = view.get("active").and_then(Value::as_u64).unwrap_or(0) as u32;
    let nodes = nodes
        .iter()
        .filter_map(|n| {
            let id = n.get("node")?.as_str()?.to_string();
            let reported_new = n
                .get("range")
                .and_then(|r| r.get("max"))
                .and_then(Value::as_u64)
                .is_some_and(|m| m >= u64::from(inputs.goal));
            Some(NodeObs {
                role: parse_role(n.get("role").and_then(Value::as_str).unwrap_or("data")),
                status: n.get("status").and_then(Value::as_str).map(str::to_string),
                platform: inputs.platform.get(&id).copied().unwrap_or_default(),
                reported_new,
                health: inputs.health.get(&id).cloned().unwrap_or_default(),
                id,
            })
        })
        .collect();
    let finalize_blockers = view
        .get("blockers")
        .and_then(Value::as_array)
        .map(|bs| {
            bs.iter()
                .map(|b| Blocker {
                    node: b
                        .get("node")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    reason: b
                        .get("reason")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Observation {
        era_active,
        active,
        goal: inputs.goal,
        can_finalize: view
            .get("can_finalize")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        finalize_blockers,
        nodes,
        control_leader: inputs.control_leader.clone(),
        in_flight_for: inputs.in_flight_for,
        settled_for: inputs.settled_for,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_a_cluster_version_view() {
        let view = json!({
            "era_active": true, "active": 1, "can_finalize": false,
            "blockers": [{"node": "c", "reason": "member is Down"}],
            "nodes": [
                {"node": "a", "role": "combined", "status": "Active", "range": {"min":1,"max":2}},
                {"node": "d", "role": "data", "status": "Down", "range": {"min":1,"max":1}},
                {"node": "x", "role": "control", "status": null, "range": null},
            ],
        });
        let mut inputs = Inputs {
            goal: 2,
            ..Default::default()
        };
        inputs.platform.insert("a".into(), Platform::New);
        inputs.health.insert("a".into(), Health::Ok);
        let o = observation(&view, &inputs).unwrap();
        assert!(o.era_active);
        assert_eq!(o.active, 1);
        assert_eq!(o.finalize_blockers.len(), 1);
        assert_eq!(o.nodes.len(), 3);
        assert!(o.nodes[0].reported_new);
        assert_eq!(o.nodes[0].platform, Platform::New);
        assert!(!o.nodes[1].reported_new);
        assert_eq!(o.nodes[1].role, Role::Data);
        assert_eq!(o.nodes[1].health, Health::Unavailable);
        assert_eq!(o.nodes[2].role, Role::Control);
        assert_eq!(o.nodes[2].status, None);
        assert!(observation(&json!({}), &inputs).is_err());
    }

    #[test]
    fn parses_roll_health_bodies() {
        assert_eq!(
            parse_health(&json!({"ok": true, "reasons": []})),
            Health::Ok
        );
        let h = parse_health(&json!({"ok": false, "reasons": [
            {"kind": "member_not_active", "node": "c"},
            {"kind": "tablet_under_replicated", "tablet": 7},
        ]}));
        let Health::NotOk(rs) = h else { panic!() };
        assert_eq!(rs[0].node.as_deref(), Some("c"));
        assert_eq!(rs[1].tablet, Some(7));
        // A body with no verdict (an old node's 404 page) is "unavailable".
        assert_eq!(
            parse_health(&json!({"error": "not found"})),
            Health::Unavailable
        );
    }
}
