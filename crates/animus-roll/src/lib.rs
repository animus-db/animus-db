//! The rolling-upgrade state machine (ADR 0073 Phase 3, workstream P3-C).
//!
//! **Pure and I/O-free**: no `Env`, no async, no clock, no serde types in the
//! decision. [`decide`] takes the current [`Observation`] (what the cluster and
//! the platform say *right now*) and returns the one next [`Action`]. It keeps
//! **no state**: a driver that restarts, or a second driver that takes over,
//! sees the same observation and reaches the same decision. That is the D5/D7
//! rule ("derived, never stored") made executable, and it is what lets
//! `animus-cli`'s `cluster roll` (a supervisor that restarts nothing itself) and
//! `animus-operator`'s partition driver share one definition of "what is the
//! next safe step of this roll".
//!
//! # The roll (ADR 0073 Phase 3, D1/D2/D6/D9)
//!
//! One node at a time, in the D1 order: data-only nodes first, then control
//! voters, the **control leader last**; ties by node id (the same order
//! `animusd`'s `roll.remaining` derives). For each node:
//!
//! 1. the gate ([`Block`] otherwise): every member `Active`, and every node
//!    that can answer `GET /admin/roll-health` says `ok` (a node still on the
//!    previous binary has no such endpoint and is tolerated as `Unavailable`;
//!    an *unreachable* node is not);
//! 2. if the node is the control leader, transfer control leadership away
//!    first ([`Action::TransferControlLeadership`]); tablet leaders are never
//!    moved (they re-elect on their own, maintainer decision 3);
//! 3. restart it ([`Action::Restart`]): the platform's job. This is the
//!    node's point of no return (Option B: fix forward, no rollback);
//! 4. wait until the node is on the new binary, `Active`, reports the new
//!    range (when the era is on) and its own roll-health is `ok`
//!    ([`Action::Wait`]); only then does the next node become eligible.
//!
//! **At most one node is ever below the gate**: a node that is restarting, or
//! on the new binary but not yet healthy and reported, blocks every other
//! restart. After the last node: [`Action::AwaitEra`] (a Phase 1 -> B2 roll: the
//! era starts by itself once every member is B2), then finalize per
//! [`FinalizeMode`] ([`Action::ReadyToFinalize`] for a human, [`Action::Finalize`]
//! for opt-in auto-finalize after a soak), never while a blocker or an unhealthy
//! verdict stands, then [`Action::Complete`].
//!
//! Timeouts are the caller's: [`Observation::in_flight_for`] and
//! [`Observation::settled_for`] carry elapsed time in from the caller's own
//! clock, so this crate never reads one.
//!
//! The [`json`] module parses `GET /admin/cluster-version` and `GET
//! /admin/roll-health` bodies into an [`Observation`]; the machine itself is
//! independent of the wire.

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

pub mod json;

/// What a node runs (from `GET /admin/cluster-version` `nodes[].role`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Data-only: no control replica.
    Data,
    /// Control-only voter.
    Control,
    /// Control voter and data node.
    Combined,
}

impl Role {
    /// Whether the node carries a control replica (can lead / receive a
    /// leadership transfer).
    pub fn is_control(self) -> bool {
        !matches!(self, Role::Data)
    }
}

/// What the platform (systemd, Kubernetes, the test harness) says about a
/// node's process. Supplied by the caller: the machine cannot restart a process
/// and never guesses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Platform {
    /// Still running the previous binary: not yet touched by this roll.
    #[default]
    Old,
    /// Being replaced: stopped, starting, or the pod not yet `Ready`.
    Restarting,
    /// Running the new binary.
    New,
}

/// One node's own `GET /admin/roll-health` answer, as the caller could get it.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Health {
    /// `ok: true`.
    Ok,
    /// `ok: false`, with the named reasons.
    NotOk(Vec<Reason>),
    /// The node answered but has no such endpoint (a previous-release binary).
    #[default]
    Unavailable,
    /// The node could not be asked (down, unreachable, timed out).
    Unreachable,
}

/// A named reason a verdict is not `ok` (`roll-health`'s `reasons[]`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Reason {
    pub kind: String,
    pub node: Option<String>,
    pub tablet: Option<u64>,
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if let Some(n) = &self.node {
            write!(f, " node={n}")?;
        }
        if let Some(t) = self.tablet {
            write!(f, " tablet={t}")?;
        }
        Ok(())
    }
}

/// A Finalize blocker (`cluster-version` `blockers[]`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blocker {
    pub node: String,
    pub reason: String,
}

/// One member of the roll's required set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeObs {
    pub id: String,
    pub role: Role,
    /// Replicated member status (`Active`, `Down`, `Leaving`, `Joining`);
    /// `None` when the cluster has no member row for it.
    pub status: Option<String>,
    pub platform: Platform,
    /// Its recorded range's max is `>= goal` (the new binary has reported).
    /// Meaningful only once the era is active.
    pub reported_new: bool,
    pub health: Health,
}

impl NodeObs {
    fn active(&self) -> bool {
        self.status.as_deref() == Some("Active")
    }
}

/// Everything the machine looks at. Build it fresh each tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    /// The versioning era is active (`cluster-version` `era_active`). A Phase 1
    /// -> B2 roll runs with it `false`: no version observation exists, and the
    /// gate is the platform fact plus roll-health.
    pub era_active: bool,
    /// The cluster version in force.
    pub active: u32,
    /// The cluster version this roll finalizes to. Fixed by the caller for the
    /// whole roll (typically `active + 1` at its start): recomputing it from
    /// `active` would make every node look old again the moment Finalize
    /// lands.
    pub goal: u32,
    /// `cluster-version` `can_finalize`.
    pub can_finalize: bool,
    pub finalize_blockers: Vec<Blocker>,
    pub nodes: Vec<NodeObs>,
    /// The control leader, if one is known to the observer.
    pub control_leader: Option<String>,
    /// How long the in-flight node has been in flight, by the caller's clock.
    pub in_flight_for: Option<Duration>,
    /// How long every node has been done and healthy, by the caller's clock
    /// (the soak clock; `None` is zero).
    pub settled_for: Option<Duration>,
}

/// How the roll ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinalizeMode {
    /// A human runs `animus cluster finalize` (the default, D6).
    Manual,
    /// Opt-in: finalize once every node is done and healthy for `soak`.
    Auto { soak: Duration },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub finalize: FinalizeMode,
    /// A node in flight this long is [`Block::NodeStalled`] rather than a
    /// plain wait (D9: pause and surface). The machine never acts on it.
    pub stall_after: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            finalize: FinalizeMode::Manual,
            stall_after: Duration::from_secs(600),
        }
    }
}

/// Why the gate is closed. Nothing is touched; re-observe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Block {
    /// Nothing to decide on (an empty required set).
    NoNodes,
    /// A member is not `Active` (`Down`, `Leaving`, `Joining`, or unknown).
    MemberNotActive { node: String, status: String },
    /// A node's roll-health is not `ok`.
    Unhealthy { node: String, reasons: Vec<Reason> },
    /// A node could not be asked, so the gate cannot be evaluated: fail closed.
    Unobservable { node: String },
    /// No verdict could be obtained from any node other than the target on a
    /// cluster where some node must have the endpoint.
    NoVerdict,
    /// A control node is next but no control leader is known.
    NoControlLeader,
    /// The control leader is next and there is nobody to hand leadership to.
    NoTransferTarget { leader: String },
    /// The in-flight node exceeded [`Config::stall_after`].
    NodeStalled { node: String, why: Vec<String> },
    /// Every node is done but Finalize is refused.
    FinalizeBlocked { blockers: Vec<Blocker> },
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Block::NoNodes => write!(f, "no nodes to roll"),
            Block::MemberNotActive { node, status } => {
                write!(f, "member {node} is {status}, not Active")
            }
            Block::Unhealthy { node, reasons } => {
                write!(f, "roll-health on {node} is not ok:")?;
                for r in reasons {
                    write!(f, " [{r}]")?;
                }
                Ok(())
            }
            Block::Unobservable { node } => write!(f, "cannot read roll-health from {node}"),
            Block::NoVerdict => write!(f, "no node could provide a roll-health verdict"),
            Block::NoControlLeader => write!(f, "no control leader is known"),
            Block::NoTransferTarget { leader } => write!(
                f,
                "control leader {leader} is next and no Active control node can take leadership"
            ),
            Block::NodeStalled { node, why } => {
                write!(f, "node {node} has not become healthy: {}", why.join("; "))
            }
            Block::FinalizeBlocked { blockers } => {
                write!(f, "finalize is blocked:")?;
                for b in blockers {
                    write!(f, " [{}: {}]", b.node, b.reason)?;
                }
                Ok(())
            }
        }
    }
}

/// The next step. Exactly one per [`decide`] call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Move control leadership from `from` (about to be restarted) to `to`.
    TransferControlLeadership { from: String, to: String },
    /// Restart `node` on the new binary (the platform's job).
    Restart { node: String },
    /// A node is in flight; do nothing until it is on the new binary, healthy
    /// and (era) reported.
    Wait { node: String, why: Vec<String> },
    /// Every node is done; the era (Phase 1 -> B2) has not started yet.
    AwaitEra,
    /// Auto-finalize soak running.
    Soak { remaining: Duration },
    /// Every node is done and healthy and Finalize is allowed: a human decides.
    ReadyToFinalize { to: u32 },
    /// Auto-finalize: raise the cluster version `expected -> to`.
    Finalize { to: u32, expected: u32 },
    /// The gate is closed.
    Blocked(Block),
    /// The roll is finished.
    Complete,
}

fn class_of(n: &NodeObs, leader: Option<&str>) -> u8 {
    if leader == Some(n.id.as_str()) {
        2
    } else if n.role == Role::Data {
        0
    } else {
        1
    }
}

/// Whether the node has finished its part of the roll.
fn done(n: &NodeObs, obs: &Observation) -> bool {
    n.platform == Platform::New
        && n.active()
        && (!obs.era_active || n.reported_new)
        && n.health == Health::Ok
}

fn pending_reasons(n: &NodeObs, obs: &Observation) -> Vec<String> {
    let mut why = Vec::new();
    match n.platform {
        Platform::Restarting => why.push("restarting".to_string()),
        Platform::Old => why.push("still on the old binary".to_string()),
        Platform::New => {}
    }
    if !n.active() {
        why.push(format!(
            "member status {}",
            n.status.as_deref().unwrap_or("unknown")
        ));
    }
    if obs.era_active && !n.reported_new {
        why.push("new range not yet reported".to_string());
    }
    match &n.health {
        Health::Ok => {}
        Health::NotOk(rs) => why.extend(rs.iter().map(|r| format!("roll-health: {r}"))),
        Health::Unavailable => why.push("roll-health unavailable".to_string()),
        Health::Unreachable => why.push("roll-health unreachable".to_string()),
    }
    why
}

/// The next action for this observation. Pure: equal observations give equal
/// actions, whatever was decided before.
pub fn decide(cfg: &Config, obs: &Observation) -> Action {
    decide_with_target(cfg, obs, None)
}

/// [`decide`] for a driver that fixes the order itself: `next` names the node
/// the driver will restart next, and the machine evaluates **that** node (the
/// gate excludes its own, about-to-be-stale verdict; a control leader gets its
/// [`Action::TransferControlLeadership`]) instead of the one its own D1 order
/// would pick. The Kubernetes operator needs this: a `StatefulSet` replaces
/// pods highest ordinal first, whatever the machine would prefer, so the gate
/// has to be judged for the pod the partition is about to admit. `next` is
/// honoured only if it names a node still on the old binary
/// ([`Platform::Old`]); anything else falls back to the machine's own order,
/// so `decide_with_target(cfg, obs, None)` is exactly [`decide`].
pub fn decide_with_target(cfg: &Config, obs: &Observation, next: Option<&str>) -> Action {
    if obs.nodes.is_empty() {
        return Action::Blocked(Block::NoNodes);
    }
    let leader = obs.control_leader.as_deref();

    // One node below the gate at a time: anything restarting, or new but not
    // yet healthy and reported, holds every other restart.
    let mut in_flight: Vec<&NodeObs> = obs
        .nodes
        .iter()
        .filter(|n| {
            n.platform == Platform::Restarting || (n.platform == Platform::New && !done(n, obs))
        })
        .collect();
    in_flight.sort_by(|a, b| a.id.cmp(&b.id));
    if let Some(n) = in_flight.first() {
        let why = pending_reasons(n, obs);
        if obs.in_flight_for.is_some_and(|d| d >= cfg.stall_after) {
            return Action::Blocked(Block::NodeStalled {
                node: n.id.clone(),
                why,
            });
        }
        return Action::Wait {
            node: n.id.clone(),
            why,
        };
    }

    let mut pending: Vec<&NodeObs> = obs
        .nodes
        .iter()
        .filter(|n| n.platform == Platform::Old)
        .collect();
    pending.sort_by(|a, b| {
        class_of(a, leader)
            .cmp(&class_of(b, leader))
            .then_with(|| a.id.cmp(&b.id))
    });
    let forced = next.and_then(|id| pending.iter().copied().find(|n| n.id == id));
    let Some(target) = forced.or_else(|| pending.first().copied()) else {
        return finish(cfg, obs);
    };

    // The gate (D2).
    if let Some(b) = gate(obs, Some(&target.id)) {
        return Action::Blocked(b);
    }

    // The control leader goes last, after handing leadership off.
    if target.role.is_control() {
        let Some(l) = leader else {
            return Action::Blocked(Block::NoControlLeader);
        };
        if l == target.id {
            let mut cands: Vec<&NodeObs> = obs
                .nodes
                .iter()
                .filter(|n| n.id != target.id && n.role.is_control() && n.active())
                .collect();
            // Prefer a node already on the new binary, then any healthy one.
            cands.sort_by(|a, b| {
                (a.platform != Platform::New, &a.id).cmp(&(b.platform != Platform::New, &b.id))
            });
            return match cands.first() {
                Some(to) if to.platform != Platform::Restarting => {
                    Action::TransferControlLeadership {
                        from: target.id.clone(),
                        to: to.id.clone(),
                    }
                }
                _ => Action::Blocked(Block::NoTransferTarget {
                    leader: l.to_string(),
                }),
            };
        }
    }
    Action::Restart {
        node: target.id.clone(),
    }
}

/// The D2 gate for touching (or finalizing past) the cluster, with `target`
/// the node about to be restarted (excluded from the verdicts: its own answer
/// is about to be stale).
fn gate(obs: &Observation, target: Option<&String>) -> Option<Block> {
    for n in &obs.nodes {
        if !n.active() {
            return Some(Block::MemberNotActive {
                node: n.id.clone(),
                status: n.status.clone().unwrap_or_else(|| "unknown".to_string()),
            });
        }
    }
    let mut ok_verdicts = 0usize;
    // Reasons are reported once per node, in id order.
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut nodes: Vec<&NodeObs> = obs.nodes.iter().collect();
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    for n in nodes {
        if target == Some(&n.id) || !seen.insert(n.id.as_str()) {
            continue;
        }
        match &n.health {
            Health::Ok => ok_verdicts += 1,
            Health::NotOk(reasons) => {
                return Some(Block::Unhealthy {
                    node: n.id.clone(),
                    reasons: reasons.clone(),
                });
            }
            Health::Unreachable => {
                return Some(Block::Unobservable { node: n.id.clone() });
            }
            // A node on the previous binary has no endpoint: tolerated. A node
            // already on the new one must answer (its absence is a gap, not a
            // pass).
            Health::Unavailable if n.platform == Platform::Old => {}
            Health::Unavailable => {
                return Some(Block::Unobservable { node: n.id.clone() });
            }
        }
    }
    // With at least one node on the new binary a verdict must exist; at the very
    // start of a roll over previous-release binaries none can (D8 case 1), and the
    // membership gate above is all there is.
    let any_new = obs.nodes.iter().any(|n| n.platform == Platform::New);
    if ok_verdicts == 0 && any_new {
        return Some(Block::NoVerdict);
    }
    None
}

fn finish(cfg: &Config, obs: &Observation) -> Action {
    if !obs.era_active {
        return Action::AwaitEra;
    }
    if obs.active >= obs.goal {
        return Action::Complete;
    }
    if let Some(b) = gate(obs, None) {
        return Action::Blocked(b);
    }
    if !obs.finalize_blockers.is_empty() || !obs.can_finalize {
        return Action::Blocked(Block::FinalizeBlocked {
            blockers: obs.finalize_blockers.clone(),
        });
    }
    let to = obs.active + 1;
    match cfg.finalize {
        FinalizeMode::Manual => Action::ReadyToFinalize { to },
        FinalizeMode::Auto { soak } => {
            let settled = obs.settled_for.unwrap_or(Duration::ZERO);
            if settled >= soak {
                Action::Finalize {
                    to,
                    expected: obs.active,
                }
            } else {
                Action::Soak {
                    remaining: soak - settled,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(id: &str, role: Role, platform: Platform) -> NodeObs {
        NodeObs {
            id: id.to_string(),
            role,
            status: Some("Active".to_string()),
            platform,
            reported_new: true,
            health: Health::Ok,
        }
    }

    /// d (data), a b c (combined); a leads; era on, goal 2.
    fn obs() -> Observation {
        Observation {
            era_active: true,
            active: 1,
            goal: 2,
            can_finalize: false,
            finalize_blockers: vec![],
            nodes: vec![
                n("a", Role::Combined, Platform::Old),
                n("b", Role::Combined, Platform::Old),
                n("c", Role::Combined, Platform::Old),
                n("d", Role::Data, Platform::Old),
            ],
            control_leader: Some("a".to_string()),
            in_flight_for: None,
            settled_for: None,
        }
    }

    fn node<'a>(o: &'a mut Observation, id: &str) -> &'a mut NodeObs {
        o.nodes.iter_mut().find(|x| x.id == id).unwrap()
    }

    fn cfg() -> Config {
        Config::default()
    }

    fn not_ok(kind: &str) -> Health {
        Health::NotOk(vec![Reason {
            kind: kind.to_string(),
            node: None,
            tablet: None,
        }])
    }

    fn restart(id: &str) -> Action {
        Action::Restart {
            node: id.to_string(),
        }
    }

    #[test]
    fn rolls_data_first_then_control_voters_then_the_leader() {
        let mut o = obs();
        assert_eq!(decide(&cfg(), &o), restart("d"));
        node(&mut o, "d").platform = Platform::New;
        assert_eq!(decide(&cfg(), &o), restart("b"));
        node(&mut o, "b").platform = Platform::New;
        assert_eq!(decide(&cfg(), &o), restart("c"));
        node(&mut o, "c").platform = Platform::New;
        // The control leader is last, and hands leadership to a node already
        // on the new binary first.
        assert_eq!(
            decide(&cfg(), &o),
            Action::TransferControlLeadership {
                from: "a".into(),
                to: "b".into()
            }
        );
        o.control_leader = Some("b".to_string());
        assert_eq!(decide(&cfg(), &o), restart("a"));
    }

    #[test]
    fn order_follows_the_current_leader_not_the_id() {
        let mut o = obs();
        o.control_leader = Some("b".to_string());
        node(&mut o, "d").platform = Platform::New;
        // a is not the leader now: it rolls before b, which is last.
        assert_eq!(decide(&cfg(), &o), restart("a"));
    }

    #[test]
    fn one_node_below_the_gate_at_a_time() {
        let mut o = obs();
        // Restarting, then new-but-unreported, then new-but-unhealthy: each
        // holds everything else.
        node(&mut o, "d").platform = Platform::Restarting;
        assert!(matches!(decide(&cfg(), &o), Action::Wait { ref node, .. } if node == "d"));
        node(&mut o, "d").platform = Platform::New;
        node(&mut o, "d").reported_new = false;
        assert!(matches!(decide(&cfg(), &o), Action::Wait { .. }));
        node(&mut o, "d").reported_new = true;
        node(&mut o, "d").health = not_ok("local_group_not_caught_up");
        let a = decide(&cfg(), &o);
        assert!(
            matches!(a, Action::Wait { ref node, .. } if node == "d"),
            "{a:?}"
        );
        node(&mut o, "d").health = Health::Unreachable;
        assert!(matches!(decide(&cfg(), &o), Action::Wait { .. }));
        node(&mut o, "d").health = Health::Ok;
        assert_eq!(decide(&cfg(), &o), restart("b"));
    }

    #[test]
    fn a_new_node_not_active_is_in_flight() {
        let mut o = obs();
        node(&mut o, "d").platform = Platform::New;
        node(&mut o, "d").status = Some("Down".into());
        assert!(matches!(decide(&cfg(), &o), Action::Wait { .. }));
    }

    #[test]
    fn without_the_era_a_new_node_needs_no_reported_range() {
        let mut o = obs();
        o.era_active = false;
        node(&mut o, "d").platform = Platform::New;
        node(&mut o, "d").reported_new = false;
        assert_eq!(decide(&cfg(), &o), restart("b"));
        // ... and the era variant refuses the same state.
        o.era_active = true;
        assert!(matches!(decide(&cfg(), &o), Action::Wait { .. }));
    }

    #[test]
    fn a_stalled_node_is_surfaced_not_skipped() {
        let mut o = obs();
        node(&mut o, "d").platform = Platform::Restarting;
        o.in_flight_for = Some(Duration::from_secs(599));
        assert!(matches!(decide(&cfg(), &o), Action::Wait { .. }));
        o.in_flight_for = Some(Duration::from_secs(600));
        assert!(matches!(
            decide(&cfg(), &o),
            Action::Blocked(Block::NodeStalled { ref node, .. }) if node == "d"
        ));
    }

    #[test]
    fn a_driver_chosen_target_is_the_one_the_gate_excludes_and_transfers_for() {
        // Machine order would pick `b` (leader a is last, data d first, then b).
        let mut o = obs();
        node(&mut o, "d").platform = Platform::New;
        assert_eq!(decide(&cfg(), &o), restart("b"));
        // The driver restarts `c` next (highest ordinal first): judged for c.
        assert_eq!(decide_with_target(&cfg(), &o, Some("c")), restart("c"));
        // b's own verdict is NOT excluded any more: unhealthy b closes the gate
        // for restarting c (the machine's pick would have ignored it).
        node(&mut o, "b").health = not_ok("tablet_under_replicated");
        assert!(matches!(
            decide_with_target(&cfg(), &o, Some("c")),
            Action::Blocked(Block::Unhealthy { ref node, .. }) if node == "b"
        ));
        // ... while c's own verdict is excluded (about to be stale).
        node(&mut o, "b").health = Health::Ok;
        node(&mut o, "c").health = not_ok("whatever");
        assert_eq!(decide_with_target(&cfg(), &o, Some("c")), restart("c"));
        // The control leader as the driver's target gets its transfer first.
        node(&mut o, "c").health = Health::Ok;
        assert_eq!(
            decide_with_target(&cfg(), &o, Some("a")),
            Action::TransferControlLeadership {
                from: "a".into(),
                to: "b".into()
            }
        );
        // A target that is not on the old binary falls back to the machine's order.
        assert_eq!(decide_with_target(&cfg(), &o, Some("d")), restart("b"));
        assert_eq!(decide_with_target(&cfg(), &o, Some("nope")), restart("b"));
        assert_eq!(decide_with_target(&cfg(), &o, None), decide(&cfg(), &o));
    }

    #[test]
    fn gate_blocks_on_each_non_active_status() {
        for st in ["Down", "Leaving", "Joining"] {
            let mut o = obs();
            node(&mut o, "c").status = Some(st.into());
            assert_eq!(
                decide(&cfg(), &o),
                Action::Blocked(Block::MemberNotActive {
                    node: "c".into(),
                    status: st.into()
                }),
                "{st}"
            );
        }
        let mut o = obs();
        node(&mut o, "c").status = None;
        assert!(matches!(
            decide(&cfg(), &o),
            Action::Blocked(Block::MemberNotActive { .. })
        ));
    }

    #[test]
    fn gate_blocks_on_an_unhealthy_verdict_from_any_other_node() {
        let mut o = obs();
        node(&mut o, "b").health = not_ok("tablet_under_replicated");
        let a = decide(&cfg(), &o);
        assert!(
            matches!(a, Action::Blocked(Block::Unhealthy { ref node, .. }) if node == "b"),
            "{a:?}"
        );
        // The target's own verdict is not consulted: it is about to restart.
        let mut o = obs();
        node(&mut o, "d").health = not_ok("anything");
        assert_eq!(decide(&cfg(), &o), restart("d"));
    }

    #[test]
    fn gate_fails_closed_on_an_unreachable_node() {
        let mut o = obs();
        node(&mut o, "c").health = Health::Unreachable;
        assert_eq!(
            decide(&cfg(), &o),
            Action::Blocked(Block::Unobservable { node: "c".into() })
        );
    }

    #[test]
    fn previous_release_nodes_without_the_endpoint_are_tolerated_only_at_the_start() {
        let mut o = obs();
        for x in &mut o.nodes {
            x.health = Health::Unavailable;
        }
        // Nothing new yet: only the membership gate applies (D8 case 1).
        assert_eq!(decide(&cfg(), &o), restart("d"));
        // Once a node runs the new binary, verdicts must exist.
        node(&mut o, "d").platform = Platform::New;
        node(&mut o, "d").health = Health::Unavailable;
        assert!(matches!(decide(&cfg(), &o), Action::Wait { .. }));
        node(&mut o, "d").health = Health::Ok;
        assert_eq!(decide(&cfg(), &o), restart("b"));
        // A new node that stops answering is a gap, not a pass.
        node(&mut o, "d").health = Health::Unavailable;
        node(&mut o, "b").platform = Platform::New;
        node(&mut o, "b").health = Health::Ok;
        let a = decide(&cfg(), &o);
        assert!(
            matches!(a, Action::Wait { ref node, .. } if node == "d"),
            "{a:?}"
        );
    }

    #[test]
    fn leader_with_no_one_to_take_over_blocks() {
        // Only a single control node: nobody to hand over to.
        let o2 = Observation {
            nodes: vec![
                n("a", Role::Combined, Platform::Old),
                n("d", Role::Data, Platform::New),
            ],
            ..obs()
        };
        assert_eq!(
            decide(&cfg(), &o2),
            Action::Blocked(Block::NoTransferTarget { leader: "a".into() })
        );
    }

    #[test]
    fn unknown_leader_blocks_a_control_node() {
        let mut o = obs();
        o.control_leader = None;
        node(&mut o, "d").platform = Platform::New;
        assert_eq!(decide(&cfg(), &o), Action::Blocked(Block::NoControlLeader));
        // A data node needs no leader.
        let mut o = obs();
        o.control_leader = None;
        assert_eq!(decide(&cfg(), &o), restart("d"));
    }

    fn all_new() -> Observation {
        let mut o = obs();
        for x in &mut o.nodes {
            x.platform = Platform::New;
            x.reported_new = true;
        }
        o.can_finalize = true;
        o
    }

    #[test]
    fn manual_finalize_is_offered_never_taken() {
        let o = all_new();
        assert_eq!(decide(&cfg(), &o), Action::ReadyToFinalize { to: 2 });
    }

    #[test]
    fn auto_finalize_waits_out_the_soak() {
        let c = Config {
            finalize: FinalizeMode::Auto {
                soak: Duration::from_secs(30),
            },
            ..cfg()
        };
        let mut o = all_new();
        o.settled_for = Some(Duration::from_secs(10));
        assert_eq!(
            decide(&c, &o),
            Action::Soak {
                remaining: Duration::from_secs(20)
            }
        );
        o.settled_for = Some(Duration::from_secs(30));
        assert_eq!(decide(&c, &o), Action::Finalize { to: 2, expected: 1 });
        // Soak 0 (the default): immediate.
        let c0 = Config {
            finalize: FinalizeMode::Auto {
                soak: Duration::ZERO,
            },
            ..cfg()
        };
        o.settled_for = None;
        assert_eq!(decide(&c0, &o), Action::Finalize { to: 2, expected: 1 });
    }

    #[test]
    fn never_finalizes_with_a_blocker_or_without_can_finalize() {
        let c = Config {
            finalize: FinalizeMode::Auto {
                soak: Duration::ZERO,
            },
            ..cfg()
        };
        let mut o = all_new();
        o.finalize_blockers = vec![Blocker {
            node: "c".into(),
            reason: "range [1,1] excludes target".into(),
        }];
        assert!(matches!(
            decide(&c, &o),
            Action::Blocked(Block::FinalizeBlocked { .. })
        ));
        // Even a stale `can_finalize: true` with a blocker listed is refused.
        o.can_finalize = true;
        assert!(matches!(
            decide(&c, &o),
            Action::Blocked(Block::FinalizeBlocked { .. })
        ));
        let mut o = all_new();
        o.can_finalize = false;
        assert!(matches!(
            decide(&c, &o),
            Action::Blocked(Block::FinalizeBlocked { .. })
        ));
        // An unhealthy cluster also holds the finalize.
        let mut o = all_new();
        node(&mut o, "b").health = not_ok("tablet_under_replicated");
        // (b is new and not healthy: in flight, so a wait, not a finalize.)
        assert!(matches!(decide(&c, &o), Action::Wait { .. }));
    }

    #[test]
    fn finalized_means_complete_and_never_a_second_roll() {
        let mut o = all_new();
        o.active = 2;
        // After Finalize the target moves on (`active + 1` = 3) and every node
        // reports a range max of 2: the fixed `goal` keeps them done.
        o.can_finalize = false;
        for x in &mut o.nodes {
            x.reported_new = true;
        }
        assert_eq!(decide(&cfg(), &o), Action::Complete);
    }

    #[test]
    fn without_the_era_the_last_node_awaits_it() {
        let mut o = all_new();
        o.era_active = false;
        o.active = 0;
        o.can_finalize = false;
        assert_eq!(decide(&cfg(), &o), Action::AwaitEra);
    }

    #[test]
    fn an_empty_cluster_is_blocked() {
        let mut o = obs();
        o.nodes.clear();
        assert_eq!(decide(&cfg(), &o), Action::Blocked(Block::NoNodes));
    }

    #[test]
    fn decisions_are_a_pure_function_of_the_observation() {
        // A "restarted driver" is a fresh call: same observation, same action,
        // at every point of a whole roll.
        let mut o = obs();
        let mut seen = Vec::new();
        for _ in 0..40 {
            let a = decide(&cfg(), &o);
            assert_eq!(a, decide(&cfg(), &o.clone()));
            seen.push(a.clone());
            match a {
                Action::Restart { node } => {
                    let x = o.nodes.iter_mut().find(|x| x.id == node).unwrap();
                    x.platform = Platform::New;
                    x.reported_new = true;
                    o.can_finalize = o.nodes.iter().all(|x| x.platform == Platform::New);
                }
                Action::TransferControlLeadership { to, .. } => o.control_leader = Some(to),
                Action::ReadyToFinalize { .. } => {
                    o.active = 2;
                }
                Action::Complete => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(seen.last(), Some(&Action::Complete));
        let restarts: Vec<&str> = seen
            .iter()
            .filter_map(|a| match a {
                Action::Restart { node } => Some(node.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(restarts, ["d", "b", "c", "a"]);
    }
}
