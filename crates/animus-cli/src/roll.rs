//! `animus cluster roll plan|wait|status` (ADR 0073 Phase 3, P3-B, D3/D5/D6).
//!
//! A **supervisor that restarts nothing itself**: the platform (systemd, a
//! pod revision, an operator's own tooling) replaces the process; this command
//! says which node is next (`plan`), blocks until the node that was just
//! replaced is healthy and has reported the new range (`wait`), and renders
//! the derived roll state (`status`). Every decision is `animus_roll::decide`
//! over an [`Observation`] built from the admin bodies, the same pure state
//! machine the operator's partition driver uses: this module re-implements no
//! rule. There is no roll state to persist: each call re-derives everything
//! from the cluster, so a restarted or second supervisor decides identically.
//!
//! Inputs, per call, all from the **one** admin address given: `GET
//! /admin/cluster-version` (the view incl. its `roll` object), `GET
//! /admin/roll-health` (D2), `GET /admin/raft` (the control leader's id). A
//! previous-release node (a first roll over Phase 1 binaries) has no
//! `cluster-version` endpoint: the view is then built from `/admin/status`
//! (members, roles) and every node counts as not yet on the new binary.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use animus_roll::json::{Inputs, observation, parse_health};
use animus_roll::{Action, Config, FinalizeMode, Health, NodeObs, Observation, Platform, Role};
use serde_json::{Value, json};

use crate::{http_call, run_finalize};

/// The point of no return, printed by `plan` before anything is touched.
pub(crate) const OPTION_B_WARNING: &str = "Point of no return: from its first start of the new \
binary a node writes the new on-disk formats. There is no rollback once a node has run the new \
binary; a bad roll is fixed forward (see docs/runbook/upgrade.md).";

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600);
const DEFAULT_INTERVAL: Duration = Duration::from_secs(2);
/// Bound on one per-node `cluster-version` probe (a restarting node must not stall `plan`).
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RollCmd {
    Plan {
        addr: String,
        json: bool,
    },
    Status {
        addr: String,
        json: bool,
    },
    Wait {
        addr: String,
        node: Option<String>,
        timeout: Duration,
        interval: Duration,
        finalize: bool,
        yes: bool,
    },
}

/// `5`, `5s`, `500ms`, `10m`, `2h`.
pub(crate) fn parse_duration(s: &str) -> Result<Duration, String> {
    let bad = || format!("{s:?} is not a duration (examples: 30s, 10m, 2h)");
    let (num, unit) = match s.find(|c: char| !c.is_ascii_digit()) {
        Some(i) => s.split_at(i),
        None => (s, "s"),
    };
    let n: u64 = num.parse().map_err(|_| bad())?;
    match unit {
        "ms" => Ok(Duration::from_millis(n)),
        "s" => Ok(Duration::from_secs(n)),
        "m" => Ok(Duration::from_secs(n.saturating_mul(60))),
        "h" => Ok(Duration::from_secs(n.saturating_mul(3600))),
        _ => Err(bad()),
    }
}

/// Pure argument parsing for `cluster roll ...` (`args` starts at `plan`).
pub(crate) fn parse_roll_args(args: &[String]) -> Result<RollCmd, String> {
    let sub = args
        .first()
        .map(String::as_str)
        .ok_or("cluster roll needs a subcommand: plan | wait | status")?;
    let name = format!("cluster roll {sub}");
    let addr = args
        .get(1)
        .filter(|a| !a.starts_with("--"))
        .ok_or_else(|| format!("{name} needs <admin-addr>"))?
        .clone();
    let mut it = args[2..].iter();
    match sub {
        "plan" | "status" => {
            let mut json = false;
            for a in it {
                match a.as_str() {
                    "--json" => json = true,
                    other => return Err(format!("{name}: unknown argument {other:?}")),
                }
            }
            Ok(if sub == "plan" {
                RollCmd::Plan { addr, json }
            } else {
                RollCmd::Status { addr, json }
            })
        }
        "wait" => {
            let (mut node, mut timeout, mut interval) = (None, DEFAULT_TIMEOUT, DEFAULT_INTERVAL);
            let (mut finalize, mut yes) = (false, false);
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--finalize" => finalize = true,
                    "--yes" => yes = true,
                    "--node" => node = Some(it.next().ok_or("--node needs a node id")?.clone()),
                    "--timeout" => {
                        timeout = parse_duration(it.next().ok_or("--timeout needs a duration")?)?;
                    }
                    "--interval" => {
                        interval = parse_duration(it.next().ok_or("--interval needs a duration")?)?;
                    }
                    other => return Err(format!("{name}: unknown argument {other:?}")),
                }
            }
            if finalize && !yes {
                return Err(format!(
                    "--finalize needs --yes: {}",
                    crate::FINALIZE_WARNING
                ));
            }
            if yes && !finalize {
                return Err(format!("{name}: --yes only goes with --finalize"));
            }
            Ok(RollCmd::Wait {
                addr,
                node,
                timeout,
                interval,
                finalize,
                yes,
            })
        }
        other => Err(format!(
            "cluster roll: unknown subcommand {other:?} (plan | wait | status)"
        )),
    }
}

// ---- building an observation (pure) ----------------------------------------

/// The roll's goal: the next cluster version. Fixed by the view's `active`.
fn goal_of(view: &Value) -> u32 {
    view["active"].as_u64().unwrap_or(0) as u32 + 1
}

fn node_id(n: &Value) -> Option<&str> {
    n["node"].as_str()
}

fn on_new(n: &Value, goal: u32) -> bool {
    n["range"]["max"]
        .as_u64()
        .is_some_and(|m| m >= u64::from(goal))
}

/// Build the observation for `view` (a `cluster-version` body or the legacy
/// stand-in). A node is on the new binary iff its recorded range max reaches
/// the goal (the platform's own fact is not visible from here); every node is
/// given `health`, the verdict of the node asked: the cluster-wide clauses are
/// the same wherever they are asked and `decide`'s gate needs one verdict per
/// node. `force_new` marks one node as running the new binary regardless (the
/// node `wait` is polling answers from it).
pub(crate) fn build_observation(
    view: &Value,
    health: &Health,
    leader: Option<&str>,
    force_new: Option<&str>,
) -> Result<Observation, String> {
    let goal = goal_of(view);
    let mut inputs = Inputs {
        goal,
        control_leader: leader.map(str::to_string),
        ..Inputs::default()
    };
    for n in view["nodes"].as_array().map_or(&[][..], Vec::as_slice) {
        let Some(id) = node_id(n) else { continue };
        let new = on_new(n, goal) || force_new == Some(id);
        inputs.platform.insert(
            id.to_string(),
            if new { Platform::New } else { Platform::Old },
        );
        inputs.health.insert(id.to_string(), health.clone());
    }
    observation(view, &inputs)
}

/// The legacy stand-in for a node with no `/admin/cluster-version` (a Phase 1
/// binary): members + roles from `/admin/status` (the serialized `Metadata`).
/// No range was ever reported, so every node is on the old binary.
pub(crate) fn legacy_view(status: &Value) -> Value {
    let members = status["members"].as_object();
    let nodes: Vec<Value> = members
        .map(|m| {
            m.iter()
                .map(|(id, row)| {
                    let role = status["node_addrs"][id]["role"]
                        .as_str()
                        .unwrap_or("combined")
                        .to_ascii_lowercase();
                    json!({
                        "node": id,
                        "role": role,
                        "status": row["status"],
                        "range": null,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    json!({
        "era_active": false,
        "active": 0,
        "nodes": nodes,
        "blockers": [],
        "can_finalize": false,
        "legacy": true,
    })
}

/// Fold per-node probes into `view`: `probes` maps a node id to the body that
/// node's own `GET /admin/cluster-version` returned. Before the version era
/// starts no node has a *recorded* range, so the replicated view calls every
/// node old; a node that answers its own endpoint with `own_range.max` at the
/// goal is running the new binary (a Phase 1 binary has no such endpoint), so
/// its range is filled in from its own answer. A node the view already places
/// on the new binary is left alone.
pub(crate) fn apply_probes(view: &mut Value, probes: &BTreeMap<String, Value>) {
    let goal = goal_of(view);
    let Some(nodes) = view["nodes"].as_array_mut() else {
        return;
    };
    for n in nodes {
        if on_new(n, goal) {
            continue;
        }
        let Some(own) = node_id(n)
            .and_then(|id| probes.get(id))
            .map(|b| &b["own_range"])
        else {
            continue;
        };
        if own["max"].as_u64().is_some_and(|m| m >= u64::from(goal)) {
            n["range"] = own.clone();
            n["probed"] = json!(true);
        }
    }
}

// ---- plan (pure) ------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PlanStep {
    Transfer {
        from: String,
        to: String,
    },
    Restart {
        node: String,
        role: Role,
        leader: bool,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PlanOutcome {
    /// The order the roll will take, from the next node on.
    Steps(Vec<PlanStep>),
    /// Nothing left to restart (`why` says what comes next).
    Nothing(String),
    /// A precondition fails; nothing may be touched.
    Refused(String),
}

fn role_str(r: Role) -> &'static str {
    match r {
        Role::Data => "data",
        Role::Control => "control",
        Role::Combined => "combined",
    }
}

/// The plan: the first action is `decide` over the real observation (so a
/// closed gate refuses); the order after it is `decide` iterated over the same
/// observation with each restarted node marked done and healthy, which is
/// exactly the sequence a roll that goes well takes.
pub(crate) fn plan(obs: &Observation) -> PlanOutcome {
    let cfg = Config::default();
    let mut sim = obs.clone();
    let mut steps = Vec::new();
    let mut first = true;
    for _ in 0..(4 * obs.nodes.len() + 4) {
        let action = decide_for(&cfg, &sim);
        match action {
            Action::Restart { node } => {
                let n = sim.nodes.iter_mut().find(|n| n.id == node).expect("node");
                steps.push(PlanStep::Restart {
                    leader: sim.control_leader.as_deref() == Some(node.as_str()),
                    role: n.role,
                    node: node.clone(),
                });
                mark_done(n);
            }
            Action::TransferControlLeadership { from, to } => {
                sim.control_leader = Some(to.clone());
                steps.push(PlanStep::Transfer { from, to });
            }
            Action::Blocked(b) => {
                return if first {
                    PlanOutcome::Refused(b.to_string())
                } else {
                    // Unreachable for a healthy simulation; fail closed anyway.
                    PlanOutcome::Refused(format!("{b} (after {} steps)", steps.len()))
                };
            }
            Action::Wait { node, why } => {
                return PlanOutcome::Refused(format!(
                    "node {node} is mid-roll and not yet done: {}",
                    why.join("; ")
                ));
            }
            Action::AwaitEra => {
                return end_of_plan(
                    steps,
                    "every node runs the new binary; the version era starts once every member has reported",
                );
            }
            Action::ReadyToFinalize { to } => {
                return end_of_plan(
                    steps,
                    &format!(
                        "every node reports the new range; then `animus cluster finalize` raises the cluster version to {to}"
                    ),
                );
            }
            Action::Complete => {
                return end_of_plan(
                    steps,
                    "the roll is complete: the cluster version is already at the goal",
                );
            }
            Action::Soak { .. } | Action::Finalize { .. } => unreachable!("Manual config"),
        }
        if first {
            first = false;
            // The rest of the order assumes each restart goes well.
            for n in &mut sim.nodes {
                n.health = Health::Ok;
            }
            // ...and that Finalize is allowed once every node has.
            sim.can_finalize = true;
            sim.finalize_blockers.clear();
        }
    }
    PlanOutcome::Refused("the plan did not converge (a bug)".into())
}

fn decide_for(cfg: &Config, o: &Observation) -> Action {
    animus_roll::decide(cfg, o)
}

fn mark_done(n: &mut NodeObs) {
    n.platform = Platform::New;
    n.reported_new = true;
    n.health = Health::Ok;
}

fn end_of_plan(steps: Vec<PlanStep>, why: &str) -> PlanOutcome {
    if steps.is_empty() {
        PlanOutcome::Nothing(why.to_string())
    } else {
        PlanOutcome::Steps(steps)
    }
}

fn plan_json(view: &Value, outcome: &PlanOutcome) -> Value {
    let mut o = json!({
        "era_active": view["era_active"],
        "active": view["active"],
        "goal": goal_of(view),
    });
    match outcome {
        PlanOutcome::Steps(steps) => {
            o["ok"] = json!(true);
            o["steps"] = Value::Array(steps.iter().map(step_json).collect());
        }
        PlanOutcome::Nothing(why) => {
            o["ok"] = json!(true);
            o["steps"] = json!([]);
            o["note"] = json!(why);
        }
        PlanOutcome::Refused(why) => {
            o["ok"] = json!(false);
            o["refused"] = json!(why);
        }
    }
    o
}

fn step_json(s: &PlanStep) -> Value {
    match s {
        PlanStep::Transfer { from, to } => {
            json!({"action": "transfer_control_leadership", "from": from, "to": to})
        }
        PlanStep::Restart { node, role, leader } => {
            json!({"action": "restart", "node": node, "role": role_str(*role), "control_leader": leader})
        }
    }
}

fn format_plan(view: &Value, obs: &Observation, outcome: &PlanOutcome) -> String {
    let mut out = String::new();
    let goal = goal_of(view);
    out.push_str(&format!(
        "cluster version: active {} -> {goal}{}\n",
        view["active"],
        if view["era_active"].as_bool().unwrap_or(false) {
            ""
        } else {
            " (version era not started)"
        }
    ));
    out.push_str("nodes:\n");
    for n in &obs.nodes {
        out.push_str(&format!(
            "  {:<12} {:<9} {:<8} {}\n",
            n.id,
            role_str(n.role),
            n.status.as_deref().unwrap_or("-"),
            if n.platform == Platform::New {
                "on the new version"
            } else {
                "old"
            }
        ));
    }
    if let Some(l) = &obs.control_leader {
        out.push_str(&format!("control leader: {l}\n"));
    }
    match outcome {
        PlanOutcome::Refused(why) => out.push_str(&format!("refused: {why}\n")),
        PlanOutcome::Nothing(why) => out.push_str(&format!("nothing to restart: {why}\n")),
        PlanOutcome::Steps(steps) => {
            out.push_str(&format!("{OPTION_B_WARNING}\n\nroll order:\n"));
            for (i, s) in steps.iter().enumerate() {
                match s {
                    PlanStep::Transfer { from, to } => out.push_str(&format!(
                        "  {}. transfer control leadership {from} -> {to}  (animus admin control-transfer <leader-admin-addr> {to})\n",
                        i + 1
                    )),
                    PlanStep::Restart { node, role, leader } => out.push_str(&format!(
                        "  {}. restart {node} ({}{}) on the new binary, then: animus cluster roll wait <{node}'s admin addr>\n",
                        i + 1,
                        role_str(*role),
                        if *leader { ", control leader, after the transfer" } else { "" }
                    )),
                }
            }
            out.push_str(
                "then, once every member reports the new range: animus cluster finalize <control-leader-admin-addr>\n",
            );
        }
    }
    out
}

// ---- status (pure) ----------------------------------------------------------

fn describe_action(a: &Action) -> String {
    match a {
        Action::Restart { node } => format!("restart {node} on the new binary"),
        Action::TransferControlLeadership { from, to } => {
            format!("transfer control leadership {from} -> {to}, then restart {from}")
        }
        Action::Wait { node, why } => format!("wait for {node}: {}", why.join("; ")),
        Action::Blocked(b) => format!("blocked: {b}"),
        Action::AwaitEra => {
            "every node runs the new binary; waiting for the version era to start".into()
        }
        Action::Soak { remaining } => format!("soak, {}s remaining", remaining.as_secs()),
        Action::ReadyToFinalize { to } => format!(
            "every node reports the new range: run `animus cluster finalize` (to {to}, irreversible)"
        ),
        Action::Finalize { to, .. } => format!("finalize to {to}"),
        Action::Complete => "complete".into(),
    }
}

fn health_line(h: &Health) -> String {
    match h {
        Health::Ok => "ok".into(),
        Health::NotOk(rs) => format!(
            "NOT ok: {}",
            rs.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        ),
        Health::Unavailable => "unavailable (this node predates roll-health)".into(),
        Health::Unreachable => "unreachable".into(),
    }
}

fn format_status(view: &Value, health: &Health, next: &Action) -> String {
    let roll = &view["roll"];
    let list = |v: &Value| -> String {
        let items: Vec<&str> = v
            .as_array()
            .map_or(&[][..], Vec::as_slice)
            .iter()
            .filter_map(Value::as_str)
            .collect();
        if items.is_empty() {
            "-".into()
        } else {
            items.join(", ")
        }
    };
    let mut out = String::new();
    out.push_str(&format!(
        "active cluster version: {}{}\n",
        view["active"],
        if view["era_active"].as_bool().unwrap_or(false) {
            ""
        } else {
            " (version era not started)"
        }
    ));
    if roll.is_null() {
        out.push_str("roll: not reported by this node\n");
    } else {
        out.push_str(&format!(
            "roll: {} ({} of {} nodes on the new version)\n",
            roll["phase"].as_str().unwrap_or("?"),
            roll["on_new"],
            roll["total"]
        ));
        out.push_str(&format!(
            "remaining (roll order): {}\n",
            list(&roll["remaining"])
        ));
        out.push_str(&format!("down: {}\n", list(&roll["down"])));
        for b in roll["blockers"].as_array().map_or(&[][..], Vec::as_slice) {
            out.push_str(&format!(
                "blocker  {}: {}\n",
                b["node"].as_str().unwrap_or("?"),
                b["reason"].as_str().unwrap_or("?")
            ));
        }
    }
    out.push_str(&format!("roll health: {}\n", health_line(health)));
    out.push_str(&format!("next: {}\n", describe_action(next)));
    out
}

fn status_json(view: &Value, health: &Health, next: &Action) -> Value {
    json!({
        "era_active": view["era_active"],
        "active": view["active"],
        "roll": view["roll"],
        "health": health_line(health),
        "next": describe_action(next),
    })
}

// ---- wait (pure) ------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WaitVerdict {
    /// The node is on the new binary, `Active`, healthy and (era) reported;
    /// `next` is what `decide` says follows.
    Done { node: String, next: Action },
    /// Not yet; the reasons, for the progress line.
    Pending(String),
    /// Never going to succeed (a wrong `--node`).
    Fatal(String),
}

/// One poll's verdict for the node answering at the polled address.
/// `health_body` is its own `GET /admin/roll-health` body (it names the node
/// in `local.node`); `cfg` carries `--finalize` as `Auto { soak: 0 }`.
pub(crate) fn wait_verdict(
    view: &Value,
    health_body: Option<&Value>,
    leader: Option<&str>,
    want: Option<&str>,
    cfg: &Config,
) -> WaitVerdict {
    if view["legacy"].as_bool().unwrap_or(false) {
        return WaitVerdict::Pending(
            "the node still serves the previous-release admin API (not yet on the new binary)"
                .into(),
        );
    }
    let Some(health_body) = health_body else {
        return WaitVerdict::Pending("roll-health is not available from this node yet".into());
    };
    let Some(own) = health_body["local"]["node"].as_str() else {
        return WaitVerdict::Pending("roll-health does not name this node yet".into());
    };
    if let Some(w) = want
        && w != own
    {
        return WaitVerdict::Fatal(format!(
            "--node {w}: the admin address answers as {own}; give the admin address of {w}"
        ));
    }
    let goal = goal_of(view);
    if let Some(max) = view["own_range"]["max"].as_u64()
        && max < u64::from(goal)
    {
        return WaitVerdict::Pending(format!(
            "this node's binary supports cluster versions up to {max}, the next is {goal}: it is not (yet) the new build"
        ));
    }
    let health = parse_health(health_body);
    // Is *this* node done? Judged with every other node held back as old, so
    // another node's trouble cannot mask it.
    let mut solo = match build_observation(view, &health, leader, Some(own)) {
        Ok(o) => o,
        Err(e) => return WaitVerdict::Pending(e),
    };
    for n in &mut solo.nodes {
        if n.id != own {
            n.platform = Platform::Old;
        }
    }
    if let Action::Wait { node, why } = animus_roll::decide(cfg, &solo)
        && node == own
    {
        return WaitVerdict::Pending(why.join("; "));
    }
    let full = match build_observation(view, &health, leader, Some(own)) {
        Ok(o) => o,
        Err(e) => return WaitVerdict::Pending(e),
    };
    WaitVerdict::Done {
        node: own.to_string(),
        next: animus_roll::decide(cfg, &full),
    }
}

// ---- I/O --------------------------------------------------------------------

struct Snapshot {
    view: Value,
    health: Health,
    health_body: Option<Value>,
    leader: Option<String>,
}

async fn get_json(
    addr: &str,
    path: &str,
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<(u16, Option<Value>), String> {
    let (status, body) = http_call(addr, "GET", path, None, tls).await?;
    Ok((status, serde_json::from_str(&body).ok()))
}

async fn fetch_snapshot(
    addr: &str,
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<Snapshot, String> {
    let (st, view) = get_json(addr, "/admin/cluster-version", tls).await?;
    let view = match (st, view) {
        (200..=299, Some(v)) => v,
        (404, _) => {
            // A previous-release node: no cluster-version endpoint at all.
            let (st, status) = get_json(addr, "/admin/status", tls).await?;
            match (st, status) {
                (200..=299, Some(s)) => legacy_view(&s),
                _ => return Err(format!("admin request failed (HTTP {st}) on /admin/status")),
            }
        }
        (st, _) => {
            return Err(format!(
                "admin request failed (HTTP {st}) on /admin/cluster-version"
            ));
        }
    };
    let (view, new_admins) = probe_nodes(addr, view, tls).await;
    // The verdict is cluster-wide, so any node on the new binary can give it:
    // the asked node first, then the nodes the probes found on the new binary
    // (a previous-release node has no `roll-health`).
    let (mut health, mut health_body) = (Health::Unavailable, None);
    for a in std::iter::once(addr.to_string()).chain(new_admins) {
        match get_json(&a, "/admin/roll-health", tls).await {
            Ok((200..=299, Some(b))) => {
                (health, health_body) = (parse_health(&b), Some(b));
                break;
            }
            Ok(_) => {}
            Err(_) if a == addr => health = Health::Unreachable,
            Err(_) => {}
        }
    }
    let leader = match get_json(addr, "/admin/raft", tls).await {
        Ok((200..=299, Some(r))) => r["leader"].as_str().map(str::to_string),
        _ => None,
    };
    Ok(Snapshot {
        view,
        health,
        health_body,
        leader,
    })
}

/// Ask every node the view still calls old for its own `cluster-version` (see
/// [`apply_probes`]); a node that is down, or a previous-release binary (404),
/// stays old. Best effort: an unreadable `/admin/status` leaves the view as is.
/// Also returns the admin addresses of the nodes found on the new binary.
async fn probe_nodes(
    addr: &str,
    mut view: Value,
    tls: Option<&tokio_rustls::TlsConnector>,
) -> (Value, Vec<String>) {
    let goal = goal_of(&view);
    let old: Vec<String> = view["nodes"]
        .as_array()
        .map_or(&[][..], Vec::as_slice)
        .iter()
        .filter(|n| !on_new(n, goal))
        .filter_map(|n| node_id(n).map(str::to_string))
        .collect();
    if old.is_empty() {
        return (view, Vec::new());
    }
    let Ok((200..=299, Some(status))) = get_json(addr, "/admin/status", tls).await else {
        return (view, Vec::new());
    };
    let mut probes = BTreeMap::new();
    let mut new_admins = Vec::new();
    for id in old {
        let Some(admin) = status["node_addrs"][&id]["admin"].as_str() else {
            continue;
        };
        let probe = tokio::time::timeout(
            PROBE_TIMEOUT,
            get_json(admin, "/admin/cluster-version", tls),
        )
        .await;
        if let Ok(Ok((200..=299, Some(body)))) = probe {
            new_admins.push(admin.to_string());
            probes.insert(id, body);
        }
    }
    apply_probes(&mut view, &probes);
    (view, new_admins)
}

/// The control leader's admin address: its id from this node's `/admin/raft`,
/// its address from the replicated `node_addrs` in `/admin/status`.
async fn leader_admin_addr(addr: &str, tls: Option<&tokio_rustls::TlsConnector>) -> Option<String> {
    let (_, raft) = get_json(addr, "/admin/raft", tls).await.ok()?;
    let leader = raft?["leader"].as_str()?.to_string();
    let (_, status) = get_json(addr, "/admin/status", tls).await.ok()?;
    status?["node_addrs"][&leader]["admin"]
        .as_str()
        .map(str::to_string)
}

pub(crate) async fn run_roll(
    args: &[String],
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<(), String> {
    match parse_roll_args(args)? {
        RollCmd::Plan { addr, json } => {
            let s = fetch_snapshot(&addr, tls).await?;
            let obs = build_observation(&s.view, &s.health, s.leader.as_deref(), None)?;
            let outcome = plan(&obs);
            if json {
                println!("{}", plan_json(&s.view, &outcome));
            } else {
                print!("{}", format_plan(&s.view, &obs, &outcome));
            }
            match outcome {
                PlanOutcome::Refused(why) => Err(format!("roll refused: {why}")),
                _ => Ok(()),
            }
        }
        RollCmd::Status { addr, json } => {
            let s = fetch_snapshot(&addr, tls).await?;
            let obs = build_observation(&s.view, &s.health, s.leader.as_deref(), None)?;
            let next = animus_roll::decide(&Config::default(), &obs);
            if json {
                println!("{}", status_json(&s.view, &s.health, &next));
            } else {
                print!("{}", format_status(&s.view, &s.health, &next));
            }
            Ok(())
        }
        RollCmd::Wait {
            addr,
            node,
            timeout,
            interval,
            finalize,
            yes: _,
        } => {
            let cfg = Config {
                finalize: if finalize {
                    FinalizeMode::Auto {
                        soak: Duration::ZERO,
                    }
                } else {
                    FinalizeMode::Manual
                },
                ..Config::default()
            };
            let start = Instant::now();
            let mut last = String::new();
            let (done_node, next) = loop {
                let pending = match fetch_snapshot(&addr, tls).await {
                    Ok(s) => match wait_verdict(
                        &s.view,
                        s.health_body.as_ref(),
                        s.leader.as_deref(),
                        node.as_deref(),
                        &cfg,
                    ) {
                        WaitVerdict::Done { node, next } => break (node, next),
                        WaitVerdict::Fatal(e) => return Err(e),
                        WaitVerdict::Pending(why) => why,
                    },
                    Err(e) => format!("admin endpoint not answering: {e}"),
                };
                if pending != last {
                    eprintln!("waiting: {pending}");
                    last = pending.clone();
                }
                if start.elapsed() >= timeout {
                    return Err(format!(
                        "timed out after {}s waiting for the node: {pending}",
                        timeout.as_secs()
                    ));
                }
                tokio::time::sleep(interval).await;
            };
            println!("{done_node}: healthy, on the new version, range reported");
            match next {
                Action::Finalize { to, .. } => {
                    println!("last node: finalizing cluster version -> {to}");
                    // Finalize is served by the control leader only, and the
                    // last node rolled is the *former* leader: find it.
                    let target = leader_admin_addr(&addr, tls).await.unwrap_or(addr.clone());
                    if target != addr {
                        println!("the control leader's admin address is {target}");
                    }
                    run_finalize(&target, Some(to), true, tls).await
                }
                Action::ReadyToFinalize { to } => {
                    println!(
                        "all members report the new range; run `animus cluster finalize <control-leader-admin-addr>` (to {to}, irreversible)"
                    );
                    Ok(())
                }
                Action::Blocked(b) if finalize => Err(format!("cannot finalize: {b}")),
                Action::AwaitEra if finalize => {
                    Err("cannot finalize yet: the version era has not started".into())
                }
                other => {
                    println!("next: {}", describe_action(&other));
                    Ok(())
                }
            }
        }
    }
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| (*s).to_string()).collect()
    }

    /// Mirrors `animusd`'s `cluster_version_view` shape. d (data), a/b/c
    /// (combined); `maxes` are the recorded range maxes (None = unreported).
    fn view(active: u32, maxes: [Option<u32>; 4], statuses: [&str; 4]) -> Value {
        let ids = ["d", "a", "b", "c"];
        let roles = ["data", "combined", "combined", "combined"];
        let nodes: Vec<Value> = (0..4)
            .map(|i| {
                json!({
                    "node": ids[i], "role": roles[i], "status": statuses[i],
                    "range": maxes[i].map(|m| json!({"min": 1, "max": m})),
                    "build": "x", "reported": maxes[i].is_some(),
                })
            })
            .collect();
        json!({"era_active": true, "active": active, "own_range": {"min":1,"max":2},
               "nodes": nodes, "blockers": [], "can_finalize": false, "target": active + 1})
    }

    fn all_active() -> [&'static str; 4] {
        ["Active"; 4]
    }

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("10m").unwrap(), Duration::from_secs(600));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert!(parse_duration("m").is_err());
        assert!(parse_duration("5x").is_err());
    }

    #[test]
    fn arguments_parse_and_validate() {
        assert_eq!(
            parse_roll_args(&args(&["plan", "h:1", "--json"])).unwrap(),
            RollCmd::Plan {
                addr: "h:1".into(),
                json: true
            }
        );
        assert!(parse_roll_args(&args(&["plan"])).is_err());
        assert!(parse_roll_args(&args(&["plan", "h:1", "--bogus"])).is_err());
        assert!(parse_roll_args(&args(&["nope", "h:1"])).is_err());
        let w =
            parse_roll_args(&args(&["wait", "h:1", "--timeout", "5s", "--node", "n1"])).unwrap();
        assert_eq!(
            w,
            RollCmd::Wait {
                addr: "h:1".into(),
                node: Some("n1".into()),
                timeout: Duration::from_secs(5),
                interval: DEFAULT_INTERVAL,
                finalize: false,
                yes: false,
            }
        );
        // --finalize is irreversible: it must be explicit with --yes.
        assert!(parse_roll_args(&args(&["wait", "h:1", "--finalize"])).is_err());
        assert!(parse_roll_args(&args(&["wait", "h:1", "--yes"])).is_err());
        assert!(parse_roll_args(&args(&["wait", "h:1", "--finalize", "--yes"])).is_ok());
    }

    #[test]
    fn probes_place_pre_era_new_nodes_on_the_new_range() {
        // Before the era no node has a recorded range: all four look old.
        let mut v = view(0, [None; 4], ["Active"; 4]);
        // b answered its own endpoint with a range reaching the goal (1);
        // c's answer is for an older range; a/d did not answer.
        let probes = BTreeMap::from([
            ("b".to_string(), json!({"own_range": {"min": 1, "max": 2}})),
            ("c".to_string(), json!({"own_range": {"min": 1, "max": 0}})),
        ]);
        apply_probes(&mut v, &probes);
        let obs = build_observation(&v, &Health::Ok, Some("a"), None).unwrap();
        let PlanOutcome::Steps(steps) = plan(&obs) else {
            panic!("expected steps")
        };
        let order: Vec<&str> = steps
            .iter()
            .filter_map(|s| match s {
                PlanStep::Restart { node, .. } => Some(node.as_str()),
                _ => None,
            })
            .collect();
        assert!(!order.contains(&"b"), "{order:?}");
        assert!(order.contains(&"c") && order.contains(&"d") && order.contains(&"a"));
    }

    #[test]
    fn plan_order_is_data_then_control_with_the_leader_last() {
        let v = view(1, [Some(1); 4], all_active());
        let obs = build_observation(&v, &Health::Ok, Some("a"), None).unwrap();
        let outcome = plan(&obs);
        let PlanOutcome::Steps(steps) = outcome else {
            panic!("{outcome:?}")
        };
        let order: Vec<String> = steps
            .iter()
            .map(|s| match s {
                PlanStep::Restart { node, .. } => node.clone(),
                PlanStep::Transfer { from, to } => format!("{from}->{to}"),
            })
            .collect();
        assert_eq!(order, ["d", "b", "c", "a->b", "a"]);
    }

    #[test]
    fn plan_resumes_mid_roll_from_the_cluster_alone() {
        // d and b already report the new range.
        let v = view(1, [Some(2), Some(1), Some(2), Some(1)], all_active());
        let obs = build_observation(&v, &Health::Ok, Some("a"), None).unwrap();
        let outcome = plan(&obs);
        let PlanOutcome::Steps(steps) = outcome else {
            panic!("{outcome:?}")
        };
        assert_eq!(steps.len(), 3, "{steps:?}"); // c, transfer, a
        assert!(matches!(&steps[0], PlanStep::Restart { node, .. } if node == "c"));
    }

    #[test]
    fn plan_refuses_when_the_gate_is_closed() {
        let v = view(1, [Some(1); 4], ["Active", "Active", "Down", "Active"]);
        let obs = build_observation(&v, &Health::Ok, Some("a"), None).unwrap();
        let PlanOutcome::Refused(why) = plan(&obs) else {
            panic!()
        };
        assert!(why.contains("b is Down"), "{why}");
        let v = view(1, [Some(1); 4], all_active());
        let h = parse_health(
            &json!({"ok": false, "reasons": [{"kind": "tablet_under_replicated", "tablet": 7}]}),
        );
        let obs = build_observation(&v, &h, Some("a"), None).unwrap();
        let PlanOutcome::Refused(why) = plan(&obs) else {
            panic!()
        };
        assert!(why.contains("tablet_under_replicated"), "{why}");
        // No leader known and a control node is next.
        let obs = build_observation(&v, &Health::Ok, None, None).unwrap();
        // data node d goes first and is fine; the refusal comes at the control step.
        assert!(matches!(plan(&obs), PlanOutcome::Refused(_)));
    }

    #[test]
    fn plan_when_everything_is_on_the_new_range_is_nothing_to_restart() {
        let mut v = view(1, [Some(2); 4], all_active());
        v["can_finalize"] = json!(true);
        let obs = build_observation(&v, &Health::Ok, Some("a"), None).unwrap();
        assert!(matches!(plan(&obs), PlanOutcome::Nothing(_)));
    }

    #[test]
    fn legacy_view_comes_from_status_and_counts_every_node_old() {
        let status = json!({
            "members": {"n0": {"status": "Active"}, "n1": {"status": "Active"}},
            "node_addrs": {"n0": {"role": "combined"}},
        });
        let v = legacy_view(&status);
        let obs = build_observation(&v, &Health::Unavailable, Some("n0"), None).unwrap();
        assert!(!obs.era_active);
        assert!(obs.nodes.iter().all(|n| n.platform == Platform::Old));
        // Unavailable health is tolerated on old nodes: the plan proceeds.
        assert!(matches!(plan(&obs), PlanOutcome::Steps(_)));
    }

    fn healthy(node: &str) -> Value {
        json!({"ok": true, "reasons": [], "local": {"node": node}})
    }

    #[test]
    fn wait_is_pending_until_the_node_reports_the_new_range_and_is_healthy() {
        let cfg = Config::default();
        // Era active, d has not reported yet.
        let v = view(1, [None, Some(1), Some(1), Some(1)], all_active());
        let h = healthy("d");
        let r = wait_verdict(&v, Some(&h), Some("a"), None, &cfg);
        assert!(
            matches!(&r, WaitVerdict::Pending(w) if w.contains("range")),
            "{r:?}"
        );
        // Reported but roll-health says its groups are not caught up.
        let v = view(1, [Some(2), Some(1), Some(1), Some(1)], all_active());
        let h = json!({"ok": false, "reasons": [{"kind": "local_groups_catching_up"}], "local": {"node": "d"}});
        let r = wait_verdict(&v, Some(&h), Some("a"), None, &cfg);
        assert!(
            matches!(&r, WaitVerdict::Pending(w) if w.contains("local_groups_catching_up")),
            "{r:?}"
        );
        // Healthy and reported: done, next is the following node.
        let r = wait_verdict(&v, Some(&healthy("d")), Some("a"), None, &cfg);
        let WaitVerdict::Done { node, next } = r else {
            panic!()
        };
        assert_eq!(node, "d");
        assert_eq!(next, Action::Restart { node: "b".into() });
    }

    #[test]
    fn wait_on_the_last_node_offers_or_performs_finalize() {
        let mut v = view(1, [Some(2); 4], all_active());
        v["can_finalize"] = json!(true);
        let r = wait_verdict(&v, Some(&healthy("a")), Some("a"), None, &Config::default());
        assert!(matches!(
            r,
            WaitVerdict::Done {
                next: Action::ReadyToFinalize { to: 2 },
                ..
            }
        ));
        let auto = Config {
            finalize: FinalizeMode::Auto {
                soak: Duration::ZERO,
            },
            ..Config::default()
        };
        let r = wait_verdict(&v, Some(&healthy("a")), Some("a"), None, &auto);
        assert!(matches!(
            r,
            WaitVerdict::Done {
                next: Action::Finalize { to: 2, expected: 1 },
                ..
            }
        ));
        // A blocker (a Down member) stops it, by name.
        let mut v = view(1, [Some(2); 4], ["Active", "Active", "Down", "Active"]);
        v["can_finalize"] = json!(false);
        v["blockers"] = json!([{"node": "c", "reason": "member is Down"}]);
        let r = wait_verdict(&v, Some(&healthy("a")), Some("a"), None, &auto);
        // Not done: c is a member that is not Active and reported new -> in flight.
        assert!(
            matches!(
                r,
                WaitVerdict::Done {
                    next: Action::Wait { .. } | Action::Blocked(_),
                    ..
                }
            ),
            "{r:?}"
        );
    }

    #[test]
    fn wait_rejects_a_mismatched_node_and_an_old_build() {
        let v = view(1, [Some(2); 4], all_active());
        let r = wait_verdict(
            &v,
            Some(&healthy("a")),
            Some("a"),
            Some("b"),
            &Config::default(),
        );
        assert!(matches!(r, WaitVerdict::Fatal(_)));
        let mut v = view(1, [Some(1); 4], all_active());
        v["own_range"] = json!({"min": 1, "max": 1});
        let r = wait_verdict(&v, Some(&healthy("a")), Some("a"), None, &Config::default());
        assert!(
            matches!(&r, WaitVerdict::Pending(w) if w.contains("not (yet) the new build")),
            "{r:?}"
        );
        let legacy = legacy_view(&json!({"members": {"n0": {"status": "Active"}}}));
        let r = wait_verdict(&legacy, None, None, None, &Config::default());
        assert!(matches!(r, WaitVerdict::Pending(_)));
    }

    #[test]
    fn status_renders_the_roll_object() {
        let mut v = view(1, [Some(2), Some(1), Some(1), Some(1)], all_active());
        v["roll"] = json!({"phase": "rolling", "total": 4, "on_new": 1,
            "remaining": ["b", "c", "a"], "down": [], "blockers": [{"node": "b", "reason": "range [1,1] excludes target 2"}]});
        let obs = build_observation(&v, &Health::Ok, Some("a"), None).unwrap();
        let next = animus_roll::decide(&Config::default(), &obs);
        let text = format_status(&v, &Health::Ok, &next);
        assert!(text.contains("roll: rolling (1 of 4 nodes"), "{text}");
        assert!(text.contains("remaining (roll order): b, c, a"), "{text}");
        assert!(text.contains("next: restart b"), "{text}");
        let j = status_json(&v, &Health::Ok, &next);
        assert_eq!(j["roll"]["phase"], "rolling");
    }

    /// Parity with the server: for a view shaped like `cluster_version_view`'s
    /// output, the CLI's restart order is exactly `roll.remaining` (the server
    /// orders data-only, then control voters, the leader last, ties by id).
    #[test]
    fn plan_restart_order_matches_the_servers_roll_remaining() {
        for (maxes, remaining) in [
            ([Some(1); 4], vec!["d", "b", "c", "a"]),
            ([Some(2), Some(1), Some(1), Some(1)], vec!["b", "c", "a"]),
            ([Some(2), Some(1), Some(2), Some(2)], vec!["a"]),
        ] {
            let mut v = view(1, maxes, all_active());
            v["roll"] = json!({"remaining": remaining});
            let obs = build_observation(&v, &Health::Ok, Some("a"), None).unwrap();
            let outcome = plan(&obs);
            let PlanOutcome::Steps(steps) = outcome else {
                panic!("{outcome:?}")
            };
            let got: Vec<&str> = steps
                .iter()
                .filter_map(|s| match s {
                    PlanStep::Restart { node, .. } => Some(node.as_str()),
                    PlanStep::Transfer { .. } => None,
                })
                .collect();
            assert_eq!(got, remaining);
        }
    }
}
