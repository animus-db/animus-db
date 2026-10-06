//! The server-side "is it safe to touch the next node" verdict (ADR 0073
//! Phase 3, D2): one definition behind `GET /admin/roll-health`, the
//! `roll.health` field of `GET /admin/cluster-version`, and (via the shared
//! tablet ladder below) the dashboard's Overview Version card.
//!
//! Pure: no `Env`, no I/O, no clock. The caller (`admin.rs`) snapshots the
//! replicated `Metadata`, the control group's liveness and this node's own
//! hosted groups into the plain inputs below; everything here is a
//! deterministic function of them, so each clause is unit-testable on its
//! own. Read-only and never used by a probe (readiness must not couple to
//! cluster-wide state it cannot fix; issues #595/#710).
//!
//! `ok` is true iff **all** of:
//! 1. the control group has a recent leader and, where the answering node can
//!    see it (the control leader), a quorum of reachable voters;
//! 2. the answering node's `Metadata` view is synced (has members);
//! 3. every `Metadata` member is `Active` (no `Down`/`Leaving`/`Joining`);
//! 4. no tablet is `quorum-lost` or `under-replicated` ([`tablet_status`], the
//!    port of the dashboard's `tabletStatus` ladder in `dashboard_core.js`);
//! 5. none of this node's hosted groups has a learner mid catch-up;
//! 6. every group this node hosts is caught up: it knows a leader and its
//!    engine is within [`CATCH_UP_BOUND`] entries of its commit index.
//!
//! `forming` tablets are reported but do not fail `ok` (a transition), exactly
//! as the dashboard treats them.

use std::collections::BTreeMap;

use animus_control::meta::{Member, Metadata, NodeStatus};
use animus_env::NodeId;
use animus_tablet::Tablet;
use serde_json::{Value, json};

/// How far (log entries) a hosted group's engine may trail its commit index
/// and still count as caught up. Small: a quiet group has zero lag, and a busy
/// one briefly a few entries; a rebuilding replica is thousands behind.
pub(crate) const CATCH_UP_BOUND: u64 = 16;

/// The dashboard's tablet ladder (`tabletStatus`), first match wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TabletStatus {
    /// Fewer than a quorum of the assigned replicas are on a live node.
    QuorumLost,
    /// Some assigned replica's node is `Down`.
    UnderReplicated,
    /// A leader is elected and every configured replica is hosted.
    Healthy,
    /// Every assigned replica's node is alive but the group has not converged.
    Forming,
}

impl TabletStatus {
    /// The dashboard's own string for this status (the oracle's comparison key).
    #[cfg(test)]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::QuorumLost => "quorum-lost",
            Self::UnderReplicated => "under-replicated",
            Self::Healthy => "healthy",
            Self::Forming => "forming",
        }
    }
}

/// The ladder of `dashboard_core.js::tabletStatus`, line for line.
///
/// `has_leader`/`hosted` are the group-level facts (a leader is known; how
/// many replicas are hosted-and-reachable). An empty `members` map treats
/// every replica as live (very early startup), as the JS does.
pub(crate) fn tablet_status(
    replicas: &[NodeId],
    members: &BTreeMap<NodeId, Member>,
    has_leader: bool,
    hosted: usize,
) -> TabletStatus {
    let configured = replicas.len();
    let live = |id: &NodeId| {
        members.is_empty()
            || members
                .get(id)
                .is_some_and(|m| m.status != NodeStatus::Down)
    };
    let live_assigned = replicas.iter().filter(|r| live(r)).count();
    let quorum = configured / 2 + 1;
    if configured > 0 && live_assigned < quorum {
        TabletStatus::QuorumLost
    } else if live_assigned < configured {
        TabletStatus::UnderReplicated
    } else if has_leader && hosted >= configured {
        TabletStatus::Healthy
    } else {
        TabletStatus::Forming
    }
}

/// The control group as the answering node sees it.
#[derive(Clone, Debug, Default)]
pub(crate) struct ControlView {
    /// Voter count, if known (a data-only node knows it only after a sync).
    pub(crate) voters: Option<usize>,
    /// Voters this node can positively see alive. `Some` only on the control
    /// leader (a follower has no contact table); `None` means "not observable
    /// here", in which case a recent leader is the evidence of a quorum.
    pub(crate) reachable: Option<usize>,
    /// A control leader has been seen within the health grace window.
    pub(crate) leader_recent: bool,
}

/// One group this node hosts (the subset of `/admin/raftkv` that matters).
#[derive(Clone, Debug)]
pub(crate) struct LocalGroup {
    pub(crate) tablet: u64,
    pub(crate) leader_known: bool,
    pub(crate) learners: usize,
    pub(crate) commit_index: u64,
    pub(crate) engine_applied_index: u64,
}

impl LocalGroup {
    fn caught_up(&self) -> bool {
        self.leader_known
            && self.commit_index.saturating_sub(self.engine_applied_index) <= CATCH_UP_BOUND
    }
}

/// The answering node's own view.
#[derive(Clone, Debug)]
pub(crate) struct LocalView {
    pub(crate) node: NodeId,
    pub(crate) groups: Vec<LocalGroup>,
}

/// A named reason `ok` is false.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Reason {
    pub(crate) kind: &'static str,
    pub(crate) node: Option<String>,
    pub(crate) tablet: Option<u64>,
}

impl Reason {
    fn new(kind: &'static str) -> Self {
        Self {
            kind,
            node: None,
            tablet: None,
        }
    }
    fn node(kind: &'static str, node: &NodeId) -> Self {
        Self {
            node: Some(node.to_string()),
            ..Self::new(kind)
        }
    }
    fn tablet(kind: &'static str, tablet: u64) -> Self {
        Self {
            tablet: Some(tablet),
            ..Self::new(kind)
        }
    }
    fn to_json(&self) -> Value {
        let mut o = json!({ "kind": self.kind });
        if let Some(n) = &self.node {
            o["node"] = json!(n);
        }
        if let Some(t) = self.tablet {
            o["tablet"] = json!(t);
        }
        o
    }
}

/// The verdict: `ok` plus everything it was computed from.
#[derive(Clone, Debug)]
pub(crate) struct RollHealth {
    pub(crate) ok: bool,
    pub(crate) reasons: Vec<Reason>,
    body: Value,
}

impl RollHealth {
    /// The `GET /admin/roll-health` body.
    pub(crate) fn to_json(&self) -> Value {
        self.body.clone()
    }

    /// The compact form embedded in `GET /admin/cluster-version`'s `roll`.
    pub(crate) fn summary_json(&self) -> Value {
        json!({
            "ok": self.ok,
            "reasons": self.reasons.iter().map(Reason::to_json).collect::<Vec<_>>(),
        })
    }
}

/// Compute the verdict. See the module doc for the clauses.
pub(crate) fn roll_health(meta: &Metadata, control: &ControlView, local: &LocalView) -> RollHealth {
    let mut reasons: Vec<Reason> = Vec::new();

    // 1. Control quorum + recent leader.
    if !control.leader_recent {
        reasons.push(Reason::new("no_control_leader"));
    }
    if let (Some(voters), Some(reachable)) = (control.voters, control.reachable)
        && reachable < voters / 2 + 1
    {
        reasons.push(Reason::new("control_quorum_lost"));
    }

    // 2. Metadata synced.
    if meta.members.is_empty() {
        reasons.push(Reason::new("metadata_not_synced"));
    }

    // 3. Members.
    let mut not_active: Vec<Value> = Vec::new();
    let mut active = 0usize;
    for (id, m) in &meta.members {
        if m.status == NodeStatus::Active {
            active += 1;
        } else {
            not_active.push(json!({ "node": id.to_string(), "status": format!("{:?}", m.status) }));
            reasons.push(Reason::node("member_not_active", id));
        }
    }

    // 4. Tablets (the dashboard ladder). Locally hosted groups contribute
    // their own leader knowledge; elsewhere only the metadata is visible.
    let local_by_tablet: BTreeMap<u64, &LocalGroup> =
        local.groups.iter().map(|g| (g.tablet, g)).collect();
    let (mut quorum_lost, mut under, mut forming) = (0usize, 0usize, 0usize);
    for (id, t) in &meta.tablets {
        let t: &Tablet = t;
        let has_leader = local_by_tablet.get(&id.0).is_none_or(|g| g.leader_known);
        match tablet_status(&t.replicas, &meta.members, has_leader, t.replicas.len()) {
            TabletStatus::QuorumLost => {
                quorum_lost += 1;
                reasons.push(Reason::tablet("tablet_quorum_lost", id.0));
            }
            TabletStatus::UnderReplicated => {
                under += 1;
                reasons.push(Reason::tablet("tablet_under_replicated", id.0));
            }
            TabletStatus::Forming => forming += 1,
            TabletStatus::Healthy => {}
        }
    }

    // 5/6. This node's own groups.
    let mut learners_pending = 0usize;
    let mut caught_up = 0usize;
    for g in &local.groups {
        if g.learners > 0 {
            learners_pending += 1;
            reasons.push(Reason::tablet("learner_pending", g.tablet));
        }
        if g.caught_up() {
            caught_up += 1;
        } else {
            reasons.push(Reason::tablet("local_group_not_caught_up", g.tablet));
        }
    }

    let ok = reasons.is_empty();
    let local_status = meta
        .members
        .get(&local.node)
        .map(|m| format!("{:?}", m.status));
    let body = json!({
        "ok": ok,
        "reasons": reasons.iter().map(Reason::to_json).collect::<Vec<_>>(),
        "control_quorum": {
            "voters": control.voters,
            "reachable": control.reachable,
            "leader_recent": control.leader_recent,
        },
        "members": { "active": active, "not_active": not_active },
        "tablets": {
            "total": meta.tablets.len(),
            "quorum_lost": quorum_lost,
            "under_replicated": under,
            "forming": forming,
            "learners_pending": learners_pending,
        },
        "local": {
            "node": local.node.to_string(),
            "status": local_status,
            "hosted_groups": local.groups.len(),
            "caught_up_groups": caught_up,
        },
    });
    RollHealth { ok, reasons, body }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_tablet::{KeyRange, TabletId};

    fn nid(s: &str) -> NodeId {
        NodeId::propose(s).unwrap()
    }

    fn member(status: NodeStatus) -> Member {
        Member {
            labels: Default::default(),
            status,
            has_activated: true,
        }
    }

    /// Three Active members, two tablets with all three replicas each.
    fn healthy_meta() -> Metadata {
        let mut m = Metadata::default();
        for n in ["a", "b", "c"] {
            m.members.insert(nid(n), member(NodeStatus::Active));
        }
        for t in [1u64, 2] {
            m.tablets.insert(
                TabletId(t),
                Tablet::new(
                    TabletId(t),
                    KeyRange::whole(),
                    vec![nid("a"), nid("b"), nid("c")],
                ),
            );
        }
        m
    }

    fn ctl() -> ControlView {
        ControlView {
            voters: Some(3),
            reachable: Some(3),
            leader_recent: true,
        }
    }

    fn group(tablet: u64) -> LocalGroup {
        LocalGroup {
            tablet,
            leader_known: true,
            learners: 0,
            commit_index: 100,
            engine_applied_index: 100,
        }
    }

    fn local(groups: Vec<LocalGroup>) -> LocalView {
        LocalView {
            node: nid("a"),
            groups,
        }
    }

    fn kinds(h: &RollHealth) -> Vec<&'static str> {
        h.reasons.iter().map(|r| r.kind).collect()
    }

    #[test]
    fn a_healthy_cluster_is_ok() {
        let h = roll_health(&healthy_meta(), &ctl(), &local(vec![group(1), group(2)]));
        assert!(h.ok, "{:?}", h.reasons);
        let j = h.to_json();
        assert_eq!(j["members"]["active"], 3);
        assert_eq!(j["tablets"]["total"], 2);
        assert_eq!(j["local"]["hosted_groups"], 2);
        assert_eq!(j["local"]["caught_up_groups"], 2);
        assert_eq!(j["local"]["status"], "Active");
    }

    // One test per clause: dropping a clause from `roll_health` fails exactly
    // the matching test (the mutation check recorded in the ADR).

    #[test]
    fn clause_no_recent_control_leader() {
        let mut c = ctl();
        c.leader_recent = false;
        let h = roll_health(&healthy_meta(), &c, &local(vec![group(1)]));
        assert!(!h.ok);
        assert_eq!(kinds(&h), ["no_control_leader"]);
    }

    #[test]
    fn clause_control_quorum_lost_only_when_observable() {
        let mut c = ctl();
        c.reachable = Some(1);
        let h = roll_health(&healthy_meta(), &c, &local(vec![]));
        assert_eq!(kinds(&h), ["control_quorum_lost"]);
        // Not observable (follower): a recent leader is the evidence.
        c.reachable = None;
        assert!(roll_health(&healthy_meta(), &c, &local(vec![])).ok);
        // Exactly a quorum is fine.
        c.reachable = Some(2);
        assert!(roll_health(&healthy_meta(), &c, &local(vec![])).ok);
    }

    #[test]
    fn clause_metadata_not_synced_fails_closed() {
        let h = roll_health(&Metadata::default(), &ctl(), &local(vec![]));
        assert_eq!(kinds(&h), ["metadata_not_synced"]);
    }

    #[test]
    fn clause_every_non_active_member_blocks() {
        for st in [NodeStatus::Down, NodeStatus::Leaving, NodeStatus::Joining] {
            let mut m = healthy_meta();
            m.members.insert(nid("c"), member(st));
            m.tablets.clear();
            let h = roll_health(&m, &ctl(), &local(vec![]));
            assert!(!h.ok, "{st:?}");
            assert_eq!(kinds(&h), ["member_not_active"], "{st:?}");
            assert_eq!(h.reasons[0].node.as_deref(), Some("c"));
            assert_eq!(
                h.to_json()["members"]["not_active"][0]["status"],
                format!("{st:?}")
            );
        }
    }

    #[test]
    fn clause_under_replicated_and_quorum_lost_tablets() {
        let mut m = healthy_meta();
        m.members.insert(nid("c"), member(NodeStatus::Down));
        let h = roll_health(&m, &ctl(), &local(vec![]));
        assert!(kinds(&h).contains(&"tablet_under_replicated"));
        assert_eq!(h.to_json()["tablets"]["under_replicated"], 2);
        m.members.insert(nid("b"), member(NodeStatus::Down));
        let h = roll_health(&m, &ctl(), &local(vec![]));
        assert!(kinds(&h).contains(&"tablet_quorum_lost"));
        assert_eq!(h.to_json()["tablets"]["quorum_lost"], 2);
    }

    #[test]
    fn forming_is_reported_but_does_not_fail_ok() {
        let mut g = group(1);
        g.leader_known = false;
        // Tablet 1 has no leader on this node => `forming`; the group itself
        // is not caught up (no leader), which is the local clause below.
        let m = healthy_meta();
        let h = roll_health(&m, &ctl(), &local(vec![g]));
        assert_eq!(h.to_json()["tablets"]["forming"], 1);
        assert_eq!(kinds(&h), ["local_group_not_caught_up"]);
        // Forming alone (a tablet this node does not host is invisible): ok.
        assert!(roll_health(&m, &ctl(), &local(vec![])).ok);
    }

    #[test]
    fn clause_learner_pending() {
        let mut g = group(1);
        g.learners = 1;
        let h = roll_health(&healthy_meta(), &ctl(), &local(vec![g]));
        assert_eq!(kinds(&h), ["learner_pending"]);
        assert_eq!(h.to_json()["tablets"]["learners_pending"], 1);
    }

    #[test]
    fn clause_restarted_node_with_an_uncaught_up_group() {
        let mut g = group(2);
        g.commit_index = 1000;
        g.engine_applied_index = 1000 - CATCH_UP_BOUND - 1;
        let h = roll_health(&healthy_meta(), &ctl(), &local(vec![group(1), g]));
        assert_eq!(kinds(&h), ["local_group_not_caught_up"]);
        assert_eq!(h.reasons[0].tablet, Some(2));
        assert_eq!(h.to_json()["local"]["caught_up_groups"], 1);
        // Exactly at the bound is caught up.
        let mut g = group(2);
        g.commit_index = 1000;
        g.engine_applied_index = 1000 - CATCH_UP_BOUND;
        assert!(roll_health(&healthy_meta(), &ctl(), &local(vec![g])).ok);
    }

    // ---- the oracle: Rust ladder == dashboard_core.js ladder ----

    type LadderCase = (Vec<NodeId>, BTreeMap<NodeId, Member>, bool, usize);

    /// Every (configured, down-set, has_leader, hosted, members-empty) shape
    /// small enough to enumerate. Returns `(label, replicas, members, has_leader, hosted)`.
    fn ladder_cases() -> Vec<LadderCase> {
        let ids: Vec<NodeId> = ["a", "b", "c"].iter().map(|s| nid(s)).collect();
        let mut out = Vec::new();
        for configured in 0..=3usize {
            let replicas: Vec<NodeId> = ids[..configured].to_vec();
            // 0 = absent from members, 1 = Active, 2 = Down, 3 = Joining, 4 = Leaving
            for code in 0..5usize.pow(3) {
                let mut members = BTreeMap::new();
                let mut c = code;
                for id in &ids {
                    match c % 5 {
                        1 => members.insert(id.clone(), member(NodeStatus::Active)),
                        2 => members.insert(id.clone(), member(NodeStatus::Down)),
                        3 => members.insert(id.clone(), member(NodeStatus::Joining)),
                        4 => members.insert(id.clone(), member(NodeStatus::Leaving)),
                        _ => None,
                    };
                    c /= 5;
                }
                for has_leader in [false, true] {
                    for hosted in 0..=3usize {
                        // A leader is a hosted group: `leader` implies `hosted >= 1`.
                        if has_leader && hosted == 0 {
                            continue;
                        }
                        out.push((replicas.clone(), members.clone(), has_leader, hosted));
                    }
                }
            }
        }
        out
    }

    /// The table-driven oracle: the Rust ladder must equal the dashboard's
    /// `tabletStatus` on every enumerated state, evaluated by running the real
    /// `dashboard_core.js` under `node`. Skipped (loudly) only where no `node`
    /// binary exists; CI's runners have one.
    #[test]
    fn ladder_equals_the_dashboard_tablet_status() {
        let cases = ladder_cases();
        // Rust side.
        let rust: Vec<&'static str> = cases
            .iter()
            .map(|(r, m, l, h)| tablet_status(r, m, *l, *h).as_str())
            .collect();
        // Spot-check the semantics so the oracle cannot pass vacuously.
        for s in ["quorum-lost", "under-replicated", "healthy", "forming"] {
            assert!(rust.contains(&s), "the case table never produces {s}");
        }
        // JS side.
        let js_cases: Vec<Value> = cases
            .iter()
            .map(|(r, m, l, h)| {
                json!({
                    "replicas": r.iter().map(|n| n.to_string()).collect::<Vec<_>>(),
                    "members": m.iter()
                        .map(|(k, v)| (k.to_string(), json!({"status": format!("{:?}", v.status)})))
                        .collect::<serde_json::Map<_, _>>(),
                    "has_leader": l,
                    "hosted": h,
                })
            })
            .collect();
        let script = r#"
const fs = require("fs");
const src = fs.readFileSync(process.argv[2], "utf8");
const cases = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
// Extract the real `tabletStatus` text (the file touches `window`/`document`
// at load, so it is not loaded whole).
const start = src.indexOf("function tabletStatus(");
const end = src.indexOf("\n}\n", start) + 3;
if (start < 0 || end < start) throw new Error("tabletStatus not found in dashboard_core.js");
let STATE = { status: null };
const tabletStatus = eval("(" + src.slice(start, end) + ")");
const out = cases.map((c) => {
  STATE.status = { members: c.members };
  const gs = [];
  for (let i = 0; i < c.hosted; i++) gs.push({ g: { is_leader: i === 0 && c.has_leader } });
  return tabletStatus({ replicas: c.replicas }, gs);
});
process.stdout.write(JSON.stringify(out));
"#;
        let dir = std::env::temp_dir().join(format!("roll_health_oracle_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (script_p, cases_p) = (dir.join("o.js"), dir.join("cases.json"));
        std::fs::write(&script_p, script).unwrap();
        std::fs::write(&cases_p, serde_json::to_vec(&js_cases).unwrap()).unwrap();
        let core_p = dir.join("dashboard_core.js");
        std::fs::write(&core_p, crate::dashboard::CORE_JS).unwrap();
        let run = std::process::Command::new("node")
            .arg(&script_p)
            .arg(&core_p)
            .arg(&cases_p)
            .output();
        let _ = std::fs::remove_dir_all(&dir);
        let out = match run {
            Ok(o) => o,
            Err(e) => {
                eprintln!("SKIPPED ladder oracle: no `node` binary ({e})");
                return;
            }
        };
        assert!(
            out.status.success(),
            "node failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let js: Vec<String> = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(js.len(), rust.len());
        for (i, (j, r)) in js.iter().zip(&rust).enumerate() {
            assert_eq!(j, r, "case {i}: {:?}", cases[i]);
        }
    }

    #[test]
    fn ladder_rust_semantics() {
        let ids: Vec<NodeId> = ["a", "b", "c"].iter().map(|s| nid(s)).collect();
        let mut m: BTreeMap<NodeId, Member> = ids
            .iter()
            .map(|i| (i.clone(), member(NodeStatus::Active)))
            .collect();
        assert_eq!(tablet_status(&ids, &m, true, 3), TabletStatus::Healthy);
        assert_eq!(tablet_status(&ids, &m, false, 3), TabletStatus::Forming);
        assert_eq!(tablet_status(&ids, &m, true, 2), TabletStatus::Forming);
        m.insert(ids[2].clone(), member(NodeStatus::Down));
        assert_eq!(
            tablet_status(&ids, &m, true, 3),
            TabletStatus::UnderReplicated
        );
        m.insert(ids[1].clone(), member(NodeStatus::Down));
        assert_eq!(tablet_status(&ids, &m, true, 3), TabletStatus::QuorumLost);
        // Early startup: no members known => everything live.
        assert_eq!(
            tablet_status(&ids, &BTreeMap::new(), true, 3),
            TabletStatus::Healthy
        );
    }
}
