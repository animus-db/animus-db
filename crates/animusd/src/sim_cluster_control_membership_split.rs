//! `SimCluster`-driven conversion of `tests/control_membership_split.rs` —
//! ADR 0061 rung M (C-13) PR 6.
//!
//! This file's own 2 real-socket tests are NOT structurally equivalent: one
//! converts cleanly with no new production code; the other's own real
//! subject needs a "combined" (control-plane-voter) growth primitive that
//! `SimCluster::grow`'s own doc, and `sim_cluster_control_membership_admin.
//! rs`'s own module doc (ADR 0061 rung L, C-12 PR 4e), BOTH already flag as
//! **deferred and separately budgeted** — not the kind of "small additive
//! `#[cfg(test)]` extension" a single PR should build under time pressure.
//! See each disposition below for the precise reasoning; this is the
//! assess-and-close half of this PR, alongside the one real conversion.
//!
//! ## (1) `admin_add_control_member_races_a_control_only_self_registration_
//! and_still_converges` — **converted**
//!
//! The real test's own subject is a pure control-plane admin-vs-apply-task
//! timing race: propose a fabricated, never-dialed control-only `NodeAddrs`
//! directly at the leader (`ClientRequest::ProposeSchema`, no real bound
//! listener behind it — the real test's own doc says this explicitly: "as
//! observable as a genuine one"), then fire `POST /admin/control/member/
//! add` for that same id at the exact instant its own `RegisterNode` has
//! committed on the leader's log but the leader's own ADR 0038 async apply
//! task (`meta_apply_loop`) has not yet caught up. Every piece of this is
//! ALREADY `<E, R>`-generic and already reachable under `SimCluster`
//! (`SimCluster::propose_meta` — the identical direct-leader-propose bypass
//! `SimCluster::grow`/`seed_members` already use — for the fabrication step;
//! `SimCluster::admin` for the mutating add call), with exactly ONE new,
//! small accessor needed: [`SimCluster::control_raft_indices`]
//! (`sim_cluster.rs`) reads `(commit_index, engine_applied_index)` directly
//! off a node's own local `RaftNode<SimEnv>`, bypassing `/admin/raft`'s HTTP
//! round trip — necessary because [`SimCluster::admin`] always burns a full
//! 12s `OP_BUDGET` virtual-time jump internally (its own doc), which would
//! blow straight past the millisecond-scale window this race needs to be
//! observed inside. No `admin.rs` change was needed at all: `/admin/raft`'s
//! `raft_view<E, R>` was already generic (confirmed by reading it directly
//! before writing this module, per this rung's own standing discipline);
//! this accessor is a pure convenience shim for a scenario that needs the
//! same two numbers without paying that fixed cost.
//!
//! **Under `SimEnv` this race is a *tighter*, more mechanical target than
//! under real sockets, not a fuzzier one.** The apply task's own idle
//! back-off (`animus_control::node::APPLY_IDLE_POLL`, 5ms) means the
//! earliest it can notice a freshly committed entry is up to 5ms of virtual
//! time after commit — the scenario steps [`SimCluster::run_for`] in
//! 1ms increments (finer than that window) after each proposal, watching
//! `control_raft_indices` for the instant `commit_index` has advanced past
//! this attempt's own baseline while `engine_applied_index` has not yet
//! followed, firing the admin add call at exactly that instant — a bounded
//! search over fresh candidate ids (mirroring the real test's own "a tiny
//! bounded search... absorbs the rare case where the apply task wins a
//! given attempt before this test's own poll can observe the gap") absorbs
//! whichever attempts don't land inside the window. Deterministic and
//! seed-reproducible, unlike the real test's own real-clock race.
//!
//! ## (2) `grow_then_replace_a_voter_over_a_split_deployment_with_live_data_
//! traffic` — **assessed, not converted (stays real-socket)**
//!
//! This test's own real subject is a genuine control-plane VOTER growth
//! node: a freshly bound `Node::bind_control` process whose own local
//! `RaftCore` starts life outside the live group's config entirely (not a
//! voter, not a learner — a lone standalone core that believes the group is
//! `config.control_ids()` minus itself), self-registers over the real wire,
//! and is then admitted as a genuine, functioning voter via `POST /admin/
//! control/member/add` — one that can go on to receive real replication,
//! serve reads, and even become leader (`grown.is_control_leader()`, used
//! directly by the real test's own second phase). Reproducing this under
//! `SimCluster` needs `self.controls` (the `Vec<RaftNode<SimEnv>>` backing
//! every control-bearing node) to grow with a genuinely new participant
//! AFTER construction — precisely the primitive [`SimCluster::grow`]'s own
//! doc names explicitly: *"a `\"combined\"` (new control-plane voter) growth
//! node was scoped for this rung and deferred: it needs a genuinely new
//! `RaftNode<SimEnv>` joining the **live** control quorum (`self.controls`
//! growing, not just `self.nodes`), which is a materially different — and
//! separately budgeted — piece of machinery than a data-only node's
//! `ControlHandle::Remote` mirror."* `sim_cluster_control_membership_admin.
//! rs`'s own module doc (C-12 PR 4e) independently reaches the identical
//! conclusion for the admin file's own analogous test
//! (`grow_control_group_converges_everywhere`), and had to substitute a
//! weaker "remove then re-add an already-running control-bearing node" — a
//! substitute that cannot serve THIS test at all, since this test's own
//! point is specifically that the freshly-added voter is a real, previously
//! non-existent, newly-live participant (its later becoming leader is part
//! of what's being proven), not an already-running one being cycled.
//!
//! Two independent production doc comments — one from this rung's own
//! groundwork PR's template method, one from a prior, separately-landed
//! rung converting the closest sibling file — both name this exact gap as
//! deliberately out of scope for ordinary test-authorship work. Building it
//! for real (a fresh `RaftNode<SimEnv>` constructed with the live group's
//! CURRENT control ids as its own believed peer set while genuinely
//! excluded from that group's own config/learners, sharing this fixture's
//! one `Simulator` so `env.send`/`env.recv` reach it once `change_
//! membership` admits it, wired through the SAME per-node reconciler/
//! heartbeat-loop/TTL-reaper assembly `grow`'s data-only arm already
//! builds) is a materially new mechanism, not a "small additive `#[cfg(
//! test)]` extension" of an existing one — exactly the class of work this
//! PR's own brief says to stop short of and hand back as an honest
//! assess-and-close verdict rather than rush under budget pressure (the
//! precise mistake ADR 0061's own C-08 PR 2 near-miss already warns
//! against, `docs/engineering-lessons.md`).
//!
//! This test's own OTHER two distinguishing ingredients — a genuine split
//! deployment (control-only + data-only processes) and continuous
//! data-plane write traffic spanning the whole membership-change flow — are
//! not, on their own, blockers: `SimCluster::new_with_roles` already builds
//! a real mixed control/data-only fixture (`sim_cluster_control_data_
//! split.rs`), and a background writer loop over a `SimClusterHandle` is the
//! same `env.spawn_task`ed shape `sim_cluster_corpus.rs`'s own client tasks
//! already use. Neither is the actual obstacle; the missing "combined
//! growth" primitive is. Its own real mechanism (ADR 0037 runtime control
//! membership change, `admin_add_control_member`/`admin_remove_control_
//! member`) already has substantial `SimCluster` coverage elsewhere — the
//! 11 conversions in `sim_cluster_control_membership_admin.rs` (C-12 PR 4e)
//! plus this file's own conversion (1) above — so this residual is narrow:
//! specifically the "a genuinely fresh process joins the live control
//! quorum as a real voter, over a real split deployment, under continuous
//! data traffic" composition, not the ADD/REMOVE mechanics themselves.
//!
//! A future rung building the "combined growth" primitive for its own sake
//! (needed independently by `SimCluster::grow`'s "data" role's own sibling,
//! and by `data_join.rs`'s already-converted dial for the "control" role
//! per C-13's own opener plan §3/§4a option (a)) would make this test's
//! conversion the natural next PR on top of it — this module's own doc is
//! the pointer for that future work, not a permanent "never convert"
//! verdict.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeAddrs, ProposeResult};
use animus_env::{NodeId, nid};

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{env_seed, json};
use crate::config::NodeRole;

/// Converged-or-timeout poll on `cond(cluster)` — the shared shape every
/// `sim_cluster_*` module's own scenario-local convergence check uses
/// (duplicated, not reached into, per this crate's own convention).
fn poll_until(
    cluster: &mut SimCluster,
    budget: Duration,
    seed: u64,
    what: &str,
    mut cond: impl FnMut(&mut SimCluster) -> bool,
) {
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        if cond(cluster) {
            return;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: {what} never converged within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

fn admin_get(cluster: &mut SimCluster, node: u64, path: &str) -> (u16, serde_json::Value) {
    let (status, body) = cluster.admin(node, "GET", path, "", &[]);
    (status, json(&body))
}

fn admin_post(
    cluster: &mut SimCluster,
    node: u64,
    path: &str,
    body: &str,
) -> (u16, serde_json::Value) {
    let (status, resp) = cluster.admin(node, "POST", path, "", body.as_bytes());
    (status, json(&resp))
}

fn control_members(cluster: &mut SimCluster, node: u64) -> (u16, serde_json::Value) {
    admin_get(cluster, node, "/admin/control/members")
}

fn add_control_member(
    cluster: &mut SimCluster,
    node: u64,
    target: u64,
    addr: &str,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"node": nid(target).to_string(), "addr": addr}).to_string();
    admin_post(cluster, node, "/admin/control/member/add", &body)
}

fn voters_of(body: &serde_json::Value) -> Option<Vec<NodeId>> {
    body["voters"].as_array().map(|a| {
        a.iter()
            .filter_map(|v| v.as_str()?.parse::<NodeId>().ok())
            .collect()
    })
}

// ---------------------------------------------------------------------------
// (1) admin_add_control_member_races_a_control_only_self_registration_and_
//     still_converges
// ---------------------------------------------------------------------------

/// Step size for the commit-vs-apply race search — finer than the ADR 0038
/// apply task's own `APPLY_IDLE_POLL` (5ms), so a call landing anywhere
/// inside that window is observable. See the module doc for the full
/// mechanism.
const RACE_STEP: Duration = Duration::from_millis(1);
/// How many `RACE_STEP`s to watch after one proposal for its own commit to
/// land — generous relative to a healthy 3-voter group's own heartbeat
/// interval (50ms).
const RACE_STEPS_PER_ATTEMPT: u32 = 80;
/// How many distinct candidate ids to try before giving up — mirrors the
/// real test's own bounded search (50), widened since a fresh `SimCluster`
/// attempt is far cheaper than a fresh real TCP round trip.
const RACE_MAX_ATTEMPTS: u64 = 200;

fn run_admin_add_control_member_races_a_control_only_self_registration_and_still_converges(
    seed: u64,
) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Control; 3], 1);
    let leader = cluster.control_leader_index() as u64;

    let mut caught: Option<(u64, NodeAddrs, u16, serde_json::Value)> = None;
    'search: for attempt in 0..RACE_MAX_ATTEMPTS {
        let this_id = 100 + attempt;
        let addrs = NodeAddrs {
            internal: format!("127.0.0.1:{}", 20000 + attempt),
            client: format!("127.0.0.1:{}", 21000 + attempt),
            admin: format!("127.0.0.1:{}", 22000 + attempt),
            intra: format!("127.0.0.1:{}", 23000 + attempt),
            role: "control".to_string(),
        };
        let (before_commit, _) = cluster.control_raft_indices(leader);
        assert!(
            matches!(
                cluster.propose_meta(MetaCommand::RegisterNode {
                    node: nid(this_id),
                    addrs: addrs.clone(),
                    labels: BTreeMap::new(),
                }),
                ProposeResult::Accepted { .. }
            ),
            "seed={seed}: RegisterNode must be accepted by the current control leader \
             (attempt={attempt})"
        );

        for _ in 0..RACE_STEPS_PER_ATTEMPT {
            cluster.run_for(RACE_STEP);
            let (commit, applied) = cluster.control_raft_indices(leader);
            if commit > before_commit {
                if applied < commit {
                    let (status, body) =
                        add_control_member(&mut cluster, leader, this_id, &addrs.internal);
                    caught = Some((this_id, addrs, status, body));
                    break 'search;
                }
                // The apply task already caught up before this step could
                // observe the gap — try a fresh id.
                break;
            }
        }
    }
    let (this_id, addrs, status, body) = caught.unwrap_or_else(|| {
        panic!(
            "seed={seed}: never observed the committed-but-not-yet-applied window across \
             {RACE_MAX_ATTEMPTS} attempts — the race this scenario targets did not manifest \
             on this run"
        )
    });

    assert_eq!(
        status, 200,
        "seed={seed}: control/member/add must succeed even when it races the target's own \
         not-yet-locally-applied self-registration, not fail with \"already claimed by a \
         different registration\": {body}"
    );

    let mut want: Vec<NodeId> = vec![nid(0), nid(1), nid(2), nid(this_id)];
    want.sort();
    for n in 0..3u64 {
        let want = want.clone();
        poll_until(
            &mut cluster,
            Duration::from_secs(30),
            seed,
            &format!("node {n} converging to the raced voter set"),
            move |c| {
                let (status, body) = control_members(c, n);
                if status != 200 {
                    return false;
                }
                match voters_of(&body) {
                    Some(mut v) => {
                        v.sort();
                        v == want
                    }
                    None => false,
                }
            },
        );
    }

    // The address book must reflect the real self-registration exactly —
    // never a synthesized/blank one (the "malformed entry wins the race"
    // corruption variant the real investigation also observed).
    let final_addrs = cluster
        .metadata(leader)
        .node_addrs
        .get(&nid(this_id))
        .cloned()
        .expect("seed={seed}: grown node's own address book entry must exist");
    assert_eq!(
        final_addrs, addrs,
        "seed={seed}: the address book must reflect the real self-registration, never a \
         synthesized/blank one"
    );
}

#[test]
fn admin_add_control_member_races_a_control_only_self_registration_and_still_converges() {
    run_admin_add_control_member_races_a_control_only_self_registration_and_still_converges(
        env_seed(0xC13F_0001),
    );
}

#[test]
fn admin_add_control_member_races_a_control_only_self_registration_and_still_converges_over_seeds()
{
    for i in 0..5 {
        run_admin_add_control_member_races_a_control_only_self_registration_and_still_converges(
            0xC13F_1000 + i,
        );
    }
}
