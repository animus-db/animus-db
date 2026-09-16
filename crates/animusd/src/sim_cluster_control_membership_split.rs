//! `SimCluster`-driven conversion of `tests/control_membership_split.rs` —
//! ADR 0061 rung M (C-13) PR 6, extended by ADR 0061 rung N (C-14) PR 3.
//!
//! **C-13 PR 6 converted this file's first test and assessed-and-closed the
//! second one** (see the original account below, kept verbatim for the
//! history — the primitive it names as missing, `SimCluster::grow_control`,
//! did not exist yet). **C-14 PR 3 converts that second test too**, now that
//! `SimCluster::grow_control` (C-14 PR 2) supplies the exact "combined"
//! (control-plane-voter) growth primitive both dispositions below named as
//! deferred — the real `tests/control_membership_split.rs` is now converted
//! whole and deleted (both of its tests have a deterministic sibling here),
//! per this crate's own "a file left with zero tests is deleted" discipline
//! (precedent: `data_join.rs`/`seed_join.rs`, ADR 0061 rung M).
//!
//! ## (3) `grow_then_replace_a_voter_over_a_split_deployment_with_live_data_
//! traffic` — **converted (C-14 PR 3)**
//!
//! The real test's own subject — a genuine split deployment (3 control-only
//! + 2 data-only nodes) growing its control quorum by one real, previously
//! non-existent voter at runtime, then replacing a founding voter (transfer
//! leadership away if it's currently leading, crash it for good, remove it
//! via the real admin route), all while continuous data-plane traffic keeps
//! flowing — is now fully reachable: [`SimCluster::new_with_roles`] builds
//! the mixed control/data-only fixture (unchanged since C-12), the wire
//! `CreateTable`/[`SimCluster::put`]/[`SimCluster::get`] triple already
//! proves data-plane traffic through a split deployment (`sim_cluster_
//! control_data_split.rs`'s own precedent), and [`SimCluster::grow_control`]
//! (C-14 PR 2) is exactly the primitive this file's own C-13 PR 6 doc and
//! `sim_cluster_control_membership_admin.rs`'s own module doc (C-12 PR 4e)
//! both independently flagged as the missing piece — a genuinely fresh
//! `RaftNode<SimEnv>` self-registered over the real relayed discovery path
//! and admitted as a live voter through the real `POST /admin/control/
//! member/add` route, never a bypass propose.
//!
//! **Traffic idiom, and why it differs from the real test's own background
//! `tokio::spawn`ed writer**: `SimCluster` is driven `&mut self`, so a
//! genuinely concurrent background task racing the foreground admin/put/get
//! calls this scenario also issues is not the natural shape here (unlike
//! `sim_cluster_corpus.rs`'s own `SimClusterHandle`-based client tasks,
//! built for a materially different purpose — a randomized fault corpus,
//! not a fixed membership-change script). Instead, [`write_and_verify_
//! traffic_key`] issues a checkpoint write-then-read-back at each phase
//! boundary (before the grow, immediately after it, immediately after the
//! replace), each hardened with the identical bounded-retry converged-poll
//! idiom `sim_cluster_seed_join.rs`'s own private `retry_forwarding_proof`
//! describes (never a fixed-deadline one-shot assert) — and every key
//! written across the whole scenario is read back once more at the very
//! end, mirroring the real test's own final per-acked-key `await_value`
//! loop. Where the real test tolerates `>= 35/40` acked writes (a genuine
//! real-clock allowance for its own background task's pacing), this
//! scenario's bounded-retry writes are each REQUIRED to land — a strictly
//! stronger bar, not a weaker one, since nothing here is racing a real
//! clock.
//!
//! **Voter selection, and the `POST /admin/control/transfer` branch**: the
//! victim is a FIXED original voter id (`0`) — distinct from `grown` on
//! every seed, since `grown` is always `>= 5` (the post-construction node
//! count) — rather than the real test's own dynamic "whichever original
//! isn't currently leading" pick. This is deliberate, not a shortcut: a
//! fixed victim means whether it happens to be the CURRENT control leader
//! at replace time is genuinely seed-dependent (leader election in `SimEnv`
//! is itself seeded), so across the pinned seed and the five `_over_seeds`
//! seeds this scenario exercises BOTH branches for real — the transfer-away
//! path (bounded retry on `409`, mirroring `sim_cluster_control_growth.rs`'s
//! own `assert_grown_voter_crash_transfer_serve_and_restart` idiom exactly)
//! on whichever seeds land the leader on node `0`, and the no-transfer-
//! needed path on every other seed — rather than a selection that
//! deterministically avoids the leader and leaves the transfer branch
//! permanently untested. `victim` is always crashed only AFTER it is
//! confirmed non-leading (either by construction, or by the transfer having
//! just moved leadership away from it), so the removal's own leader lookup
//! never needs `control_leader_index_excluding`'s own stale-frozen-belief
//! guard (`sim_cluster_control_growth.rs`'s own gotcha) — the crashed node
//! here was never leader at the instant it crashed, unlike that module's
//! own scenario, which deliberately crashes the CURRENT leader.
//! `member/remove` is called with `force: true` unconditionally (not
//! conditionally, unlike the real test, which never needs it): the removing
//! node can be `grown` itself, freshly promoted by the just-armed transfer,
//! which — mirroring `sim_cluster_control_growth.rs`'s own documented
//! reasoning for the identical call — may not yet have exchanged enough
//! heartbeats with every OTHER survivor to satisfy the liveness-aware
//! quorum-loss guard without it; using `force` unconditionally avoids a
//! seed-dependent flake in exactly the cases the transfer branch above is
//! designed to exercise.
//!
//! No new production code was needed — every primitive this scenario
//! reaches (`SimCluster::new_with_roles`, wire `CreateTable`,
//! [`SimCluster::put`]/[`SimCluster::get`], [`SimCluster::grow_control`],
//! [`SimCluster::admin`] for `/admin/control/transfer` and `/admin/control/
//! member/remove`, [`SimCluster::control_voters`]) already existed before
//! this PR. **No product bug found** — every scenario passed at its pinned
//! seed and every `_over_seeds` seed on the first clean run once the wire/
//! traffic fixture shapes matched.
//!
//! Replays (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <scenario name>`.
//!
//! ---
//!
//! ## C-13 PR 6's own original account (kept for the history)
//!
//! This file's own 2 real-socket tests were NOT structurally equivalent: one
//! converts cleanly with no new production code; the other's own real
//! subject needed a "combined" (control-plane-voter) growth primitive that
//! `SimCluster::grow`'s own doc, and `sim_cluster_control_membership_admin.
//! rs`'s own module doc (ADR 0061 rung L, C-12 PR 4e), BOTH already flagged
//! as **deferred and separately budgeted** — not the kind of "small
//! additive `#[cfg(test)]` extension" a single PR should build under time
//! pressure. See each disposition below for the precise reasoning; this was
//! the assess-and-close half of that PR, alongside the one real conversion
//! — since superseded by (3) above, now that C-14 PR 2 supplied the primitive.
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
//! traffic` — **assessed, not converted (stays real-socket), AT THE TIME**
//! **(superseded by (3) above — C-14 PR 3 converts it once `grow_control`
//! exists)**
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
//! conversion the natural next PR on top of it — this module's own doc was
//! the pointer for that future work at the time. **That rung landed as
//! ADR 0061 rung N (C-14): `SimCluster::grow_control` (PR 2) is the
//! "combined growth" primitive named above, and (3) at the top of this
//! doc is that natural next PR (C-14 PR 3).**

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeAddrs, ProposeResult};
use animus_env::{NodeId, nid};

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{create_table_via_wire, env_seed, json};
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

// ---------------------------------------------------------------------------
// (3) grow_then_replace_a_voter_over_a_split_deployment_with_live_data_
//     traffic — ADR 0061 rung N, C-14 PR 3
// ---------------------------------------------------------------------------

/// The table this scenario's own traffic writes/reads.
const REPLACE_TRAFFIC_TABLE: &str = "membership_t";
/// The two data-only nodes (ids 3, 4 — `NodeRole::Control` occupies 0, 1, 2)
/// this scenario's own traffic round-robins across, mirroring the real
/// test's own `data_clients` list.
const REPLACE_DATA_NODES: [u64; 2] = [3, 4];

/// One hash-key (`pk`, string) `CreateTable`, issued from `node` — mirrors
/// `sim_cluster_seed_join.rs`'s identically-shaped `create_table` helper
/// (this crate's own "small fixtures duplicated per test module"
/// convention).
fn create_membership_table(cluster: &mut SimCluster, node: u64) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{REPLACE_TRAFFIC_TABLE}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    create_table_via_wire(cluster, node, &body)
}

/// Retry a fallible `SimCluster` op (a `put`/`get`) while it transiently
/// fails, driving a small step of virtual time forward between attempts —
/// the module's own converged-poll idiom (never a fixed-deadline one-shot
/// assert), duplicated from `sim_cluster_seed_join.rs`'s own private
/// `retry_forwarding_proof` per this crate's "small fixtures duplicated per
/// test module" convention (that function is private to its own module).
/// Panics, naming the seed and the exhausted attempt count, if `op` never
/// succeeds.
fn retry_op<T>(
    cluster: &mut SimCluster,
    seed: u64,
    what: &str,
    mut op: impl FnMut(&mut SimCluster) -> Result<T, String>,
) -> T {
    const MAX_ATTEMPTS: u32 = 20;
    const STEP: Duration = Duration::from_millis(200);
    for attempt in 0..MAX_ATTEMPTS {
        let last_err = match op(cluster) {
            Ok(v) => return v,
            Err(e) => e,
        };
        assert!(
            attempt + 1 < MAX_ATTEMPTS,
            "seed={seed}: {what} kept hitting a transient failure after {MAX_ATTEMPTS} \
             attempts (each spaced {STEP:?} of virtual time apart, step={attempt}) — \
             last error: {last_err}"
        );
        cluster.run_for(STEP);
    }
    unreachable!("loop above always returns or panics before exhausting MAX_ATTEMPTS")
}

/// Write `pk = value` (retried, never a fixed-deadline one-shot attempt),
/// then read it back (also retried) and assert the value round-tripped —
/// this scenario's own "data traffic still flows" checkpoint, issued
/// against whichever of [`REPLACE_DATA_NODES`] accepts the write/answers
/// the read first.
fn write_and_verify_traffic_key(cluster: &mut SimCluster, seed: u64, pk: &str, value: &[u8]) {
    retry_op(cluster, seed, &format!("writing traffic key {pk}"), |c| {
        let mut last = Err("neither data node accepted the write".to_string());
        for &node in &REPLACE_DATA_NODES {
            match c.put(node, REPLACE_TRAFFIC_TABLE, pk, "sk", value) {
                Ok(()) => return Ok(()),
                Err(e) => last = Err(e),
            }
        }
        last
    });
    let got = retry_op(
        cluster,
        seed,
        &format!("reading back traffic key {pk}"),
        |c| {
            for &node in &REPLACE_DATA_NODES {
                if let Ok(Some(v)) = c.get(node, REPLACE_TRAFFIC_TABLE, pk, "sk", true) {
                    return Ok(v);
                }
            }
            Err("neither data node returned the value yet".to_string())
        },
    );
    assert_eq!(
        got, value,
        "seed={seed}: traffic key {pk} read back the wrong value"
    );
}

/// Arm a leadership transfer to `target` and retry on the retryable `409`
/// this route can answer — mirrors `sim_cluster_control_growth.rs`'s own
/// `assert_grown_voter_crash_transfer_serve_and_restart` transfer-retry
/// block exactly (duplicated, not reached into, per this crate's own
/// convention). Unlike that module's own call, `target` here is never the
/// node whose stale post-crash belief needs `control_leader_index_
/// excluding` — this scenario only ever transfers leadership away from a
/// node BEFORE crashing it (see the caller's own comment), so a plain
/// [`SimCluster::control_leader_index`] is sound throughout.
fn transfer_leadership_with_retry(cluster: &mut SimCluster, target: u64, seed: u64) {
    let body = format!(r#"{{"to":"{}"}}"#, nid(target));
    let mut accepted = false;
    for _ in 0..40 {
        let leader_idx = cluster.control_leader_index();
        let leader = cluster.control_node_id(leader_idx);
        if leader == target {
            accepted = true;
            break;
        }
        let (status, resp) = cluster.admin(
            leader,
            "POST",
            "/admin/control/transfer",
            "",
            body.as_bytes(),
        );
        match status {
            200 => {
                accepted = true;
                break;
            }
            409 => cluster.run_for(Duration::from_millis(100)),
            other => panic!(
                "seed={seed}: transfer to {target} should be accepted or retryable: {other} {resp}"
            ),
        }
    }
    assert!(
        accepted,
        "seed={seed}: transfer to {target} was never accepted within budget"
    );
    let leader_after_idx = cluster.control_leader_index();
    let leader_after = cluster.control_node_id(leader_after_idx);
    assert_eq!(
        leader_after, target,
        "seed={seed}: control leadership never moved to {target} (now {leader_after})"
    );
}

fn run_grow_then_replace_a_voter_over_a_split_deployment_with_live_data_traffic(seed: u64) {
    // 3 control-only (ids 0,1,2) + 2 data-only (ids 3,4) — mirrors the real
    // test's own `support::bring_up_split(3, 2, ..)`.
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);

    // Wire-created (never `SimCluster::create_table`'s own hand-hosted
    // shortcut, which would pick replicas `0..replication` — the
    // CONTROL-only nodes in this mixed cluster, `sim_cluster_seed_join.rs`'s
    // own documented reason).
    let create_leader = cluster.control_leader_index() as u64;
    let (status, body) = create_membership_table(&mut cluster, create_leader);
    assert_eq!(
        status, 200,
        "seed={seed}: CreateTable({REPLACE_TRAFFIC_TABLE}) failed: {body}"
    );

    // Continuous write traffic spanning the whole grow + replace flow —
    // every key attempted here is retried (never a fixed-deadline one-shot
    // assert) and accumulated so the final loop below can re-verify every
    // one, mirroring the real test's own "a few writes land" gate plus its
    // final per-acked-key readback loop.
    let mut written: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..3 {
        let pk = format!("membership-{i}");
        let value = format!("v{i}").into_bytes();
        write_and_verify_traffic_key(&mut cluster, seed, &pk, &value);
        written.push((pk, value));
    }

    // ---- Phase 1: grow the control quorum 3 -> 4 --------------------------
    let grown = cluster.grow_control();

    // Every control-bearing node's own live voter belief includes the new
    // id — 4 voters total (`grow_control`'s own step 5 already converged
    // this before returning; re-checking here is this scenario's own
    // explicit assertion).
    let control_ids: Vec<u64> = (0..cluster.control_count())
        .map(|i| cluster.control_node_id(i))
        .collect();
    for &n in &control_ids {
        let voters = cluster
            .control_voters(n)
            .unwrap_or_else(|| panic!("seed={seed}: node {n} has no live voter belief at all"));
        assert!(
            voters.contains(&nid(grown)),
            "seed={seed}: node {n}'s own live voter belief must include the grown voter \
             {grown}: {voters:?}"
        );
        assert_eq!(
            voters.len(),
            4,
            "seed={seed}: node {n}'s own live voter belief should have exactly 4 members, \
             got {voters:?}"
        );
    }

    // Data traffic still flows after the grow.
    write_and_verify_traffic_key(&mut cluster, seed, "post-grow", b"ok");
    written.push(("post-grow".to_string(), b"ok".to_vec()));

    // ---- Phase 2: replace an ORIGINAL voter --------------------------------
    // A FIXED original voter id, distinct from `grown` on every seed (see
    // this module's own doc, above, for why a fixed rather than
    // leader-avoiding pick is deliberate: it exercises the transfer-away
    // branch below on whichever seeds happen to land the leader on it,
    // rather than never at all).
    let victim: u64 = 0;
    let leader_before_replace_idx = cluster.control_leader_index();
    let leader_before_replace = cluster.control_node_id(leader_before_replace_idx);
    if leader_before_replace == victim {
        transfer_leadership_with_retry(&mut cluster, grown, seed);
    }

    // Crash it for good — mirrors the real test's own `shutdown_graceful`
    // of a voter it never intends to bring back. `victim` is confirmed
    // non-leading at this point either way (by construction, or by the
    // transfer just above), so the removal's own leader lookup below needs
    // no `control_leader_index_excluding` guard.
    cluster.crash(victim);

    let remover_idx = cluster.control_leader_index();
    let remover = cluster.control_node_id(remover_idx);
    // `force: true` unconditionally — see this module's own doc for why:
    // the remover can be the freshly-promoted `grown` node, which may not
    // yet have exchanged enough heartbeats with every other survivor to
    // satisfy the liveness-aware quorum-loss guard without it.
    let remove_body = format!(r#"{{"node":"{}","force":true}}"#, nid(victim));
    let (status, resp) = cluster.admin(
        remover,
        "POST",
        "/admin/control/member/remove",
        "",
        remove_body.as_bytes(),
    );
    assert_eq!(
        status, 200,
        "seed={seed}: control/member/remove for victim {victim} failed: {resp}"
    );

    // Every SURVIVOR (the two remaining originals + grown) converges on the
    // resulting 3-voter set — never a fixed-deadline one-shot assert.
    let expected_after_remove: std::collections::BTreeSet<NodeId> = [0u64, 1, 2, grown]
        .into_iter()
        .filter(|&n| n != victim)
        .map(nid)
        .collect();
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "every survivor converging on the post-remove 3-voter set",
        |c| {
            [0u64, 1, 2, grown]
                .into_iter()
                .filter(|&n| n != victim)
                .all(|n| {
                    c.control_voters(n)
                        .is_some_and(|v: std::collections::BTreeSet<NodeId>| {
                            v == expected_after_remove
                        })
                })
        },
    );

    // Data traffic still flows after the full replace cycle.
    write_and_verify_traffic_key(&mut cluster, seed, "post-replace", b"ok");
    written.push(("post-replace".to_string(), b"ok".to_vec()));

    // Every key written across the WHOLE scenario still reads back
    // correctly — mirrors the real test's own final per-acked-key
    // `await_value` loop. Where the real test tolerates `>= 35/40` acked
    // writes (a real-clock allowance for its own background task's
    // pacing), every attempted write here was already required to land
    // (each `write_and_verify_traffic_key` call above panics on its own
    // exhaustion), so this final pass is a re-verification, not a filter.
    for (pk, value) in &written {
        let got = retry_op(
            &mut cluster,
            seed,
            &format!("final readback of {pk}"),
            |c| {
                for &node in &REPLACE_DATA_NODES {
                    if let Ok(Some(v)) = c.get(node, REPLACE_TRAFFIC_TABLE, pk, "sk", true) {
                        return Ok(v);
                    }
                }
                Err("neither data node returned the value yet".to_string())
            },
        );
        assert_eq!(
            &got, value,
            "seed={seed}: final readback of {pk} mismatched"
        );
    }
}

#[test]
fn grow_then_replace_a_voter_over_a_split_deployment_with_live_data_traffic() {
    run_grow_then_replace_a_voter_over_a_split_deployment_with_live_data_traffic(env_seed(
        0xC14F_0001,
    ));
}

#[test]
fn grow_then_replace_a_voter_over_a_split_deployment_with_live_data_traffic_over_seeds() {
    for i in 0..5 {
        run_grow_then_replace_a_voter_over_a_split_deployment_with_live_data_traffic(
            0xC14F_1000 + i,
        );
    }
}
