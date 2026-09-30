//! Release-vs-promote race corpus (`ANIMUS_RELEASE_RACE_SEEDS`, default 1).
//!
//! A 4-hour soak logged `refusing to start as a voter` three times on tablet
//! replicas nobody had wiped. Mechanism, reproduced here: `host::plan`
//! released a hosted tablet whose own voter config excluded this node -- but a
//! **learner** is never in `config()`, so a mid-catch-up learner always
//! counted as "excluded" -- and `Reconciler::teardown` then erased the files
//! from facts gathered at tick start, never re-checking membership. The
//! tablet leader, whose own view still listed the node, promoted it to voter
//! meanwhile; the next tick re-hosted the (now empty) replica, whose
//! issue #900 boot-time cluster check found peers naming it an established
//! voter they had heard from and permanently refused it: a group silently
//! running a voter short.
//!
//! Each cell scripts the race on one node's `Reconciler` (`A`) against a
//! bare two-node remainder of the group (`B`, `C`), drawing every timing from
//! `env.gen_below` (so a seed sweeps the promote-vs-release interleaving)
//! under mild network duplication/loss:
//!
//! 1. `A` is an original voter (variant `led`: it also leads once, so its
//!    peers have `heard_from(A)` -- the precondition of the refusal).
//! 2. `B` removes `A`; `A` releases legitimately (files erased).
//! 3. Placement re-adds `A`: `A` re-hosts empty as a quiet non-voter and the
//!    leader adds it as a learner.
//! 4. `A`'s own Metadata view flips to *excluding* it for many ticks (a stale
//!    or replay-transient view, at an unchanged epoch) while the leader
//!    promotes the caught-up learner after a random delay.
//! 5. The view includes `A` again.
//!
//! Asserted, per seed: `A`'s replica is never refused as a voter, is never
//! torn down while the leader counts it as a member, and ends a caught-up
//! voter.
//!
//! The destructive-step recheck in `Reconciler::finish_teardown` (a release
//! whose replica became a member between plan and erase keeps its files) is
//! covered by `host.rs`'s own unit tests: `tick()` always gathers fresh
//! facts, so a stale plan cannot be expressed through the public API.
//!
//! Red before the fix (learner counted as excluded => released and erased
//! while a learner), green after.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::host::{MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{Clock, EnvExt, Rng, nid};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};
use animus_test::corpus;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;
const A: u64 = 400;
const B: u64 = 401;
const C: u64 = 402;
const T: TabletId = TabletId(1);

fn tab(reps: &[u64], epoch: u64) -> MetadataView {
    let mut t = Tablet::new_for_table(
        T,
        "t",
        KeyRange::whole(),
        reps.iter().copied().map(nid).collect(),
    );
    t.epoch = Epoch(epoch);
    MetadataView {
        tablets: [(t.id, t)].into_iter().collect(),
        ..Default::default()
    }
}

#[derive(Debug, Clone, Default)]
struct Outcome {
    /// Teardowns of A's replica that started while the leader counted A as a
    /// voter or learner.
    torn_while_member: u32,
    refused: bool,
    final_voter: bool,
    final_caught_up: bool,
    #[allow(dead_code, reason = "read through Debug in assertion messages")]
    info: String,
}

fn run(seed: u64, led: bool) -> Outcome {
    let mut sim = Simulator::new(seed);
    let mut net = NetConfig::default();
    net.set_duplicate_prob(0.2);
    net.set_drop_prob(0.01);
    for id in [A, B, C] {
        sim.set_net_config_for(nid(id), net.clone());
    }
    let ea = sim.env(nid(A));
    let eb = sim.env(nid(B));
    let ec = sim.env(nid(C));
    let out: Arc<Mutex<Option<Outcome>>> = Arc::new(Mutex::new(None));
    let out2 = out.clone();
    let tenv = ea.clone();
    ea.spawn_task(async move {
        let leader_cell: Arc<Mutex<Option<KvNode>>> = Arc::new(Mutex::new(None));
        let torn = Arc::new(Mutex::new(0u32));
        let (lc, tc) = (leader_cell.clone(), torn.clone());
        let mut rec: Reconciler<SimEnv, MemoryEngine> = Reconciler::new(
            tenv.clone(),
            MemoryTabletEngines::new(),
            nid(A),
            |_t, _n| {},
            move |_t| {
                // Runs at teardown start: is A a member in the leader's eyes?
                let member = lc.lock().unwrap().as_ref().is_some_and(|l| {
                    l.config().contains(&nid(A)) || l.learners().contains(&nid(A))
                });
                if member {
                    *tc.lock().unwrap() += 1;
                }
            },
        );
        let full = vec![nid(A), nid(B), nid(C)];
        let b = KvNode::start_hosted(
            eb,
            full.clone(),
            MemoryEngine::new(),
            StorageScope::new(KeyRange::whole()),
            1,
        );
        let c = KvNode::start_hosted(
            ec,
            full,
            MemoryEngine::new(),
            StorageScope::new(KeyRange::whole()),
            1,
        );
        rec.tick(&tab(&[A, B, C], 1)).await;
        tenv.sleep(Duration::from_secs(3)).await;
        let an = rec.hosted_node(T).unwrap().clone();

        if led {
            // A leads once so its peers record `heard_from(A)`.
            if !an.is_leader() {
                let l = if b.is_leader() { b.clone() } else { c.clone() };
                for _ in 0..80 {
                    if l.transfer_leadership(nid(A)) {
                        break;
                    }
                    tenv.sleep(Duration::from_millis(100)).await;
                }
                for _ in 0..80 {
                    tenv.sleep(Duration::from_millis(100)).await;
                    if an.is_leader() {
                        break;
                    }
                }
            }
            assert!(
                an.is_leader(),
                "seed={seed}: A must lead once (led variant)"
            );
            for i in 0..40u64 {
                let _ = an.put(format!("k{i}").into_bytes(), vec![1; 64]);
            }
            tenv.sleep(Duration::from_secs(1)).await;
        }
        // Hand leadership to B (if A leads), then B removes A.
        for _ in 0..80 {
            if !an.is_leader() {
                break;
            }
            let _ = an.transfer_leadership(nid(B));
            tenv.sleep(Duration::from_millis(100)).await;
        }
        for _ in 0..80 {
            if b.is_leader() {
                break;
            }
            if c.is_leader() {
                let _ = c.transfer_leadership(nid(B));
            }
            tenv.sleep(Duration::from_millis(100)).await;
        }
        assert!(b.is_leader(), "seed={seed}: B must lead");
        for _ in 0..80 {
            if !matches!(
                b.change_membership([nid(B), nid(C)].into_iter().collect()),
                ProposeResult::Accepted { .. }
            ) {
                tenv.sleep(Duration::from_millis(100)).await;
                continue;
            }
            break;
        }
        for _ in 0..80 {
            tenv.sleep(Duration::from_millis(100)).await;
            if !an.config().contains(&nid(A)) {
                break;
            }
        }
        // Legitimate release of the removed replica: must still erase.
        for _ in 0..10 {
            rec.tick(&tab(&[B, C], 3)).await;
            tenv.sleep(Duration::from_millis(50)).await;
        }
        assert!(
            rec.hosted_node(T).is_none(),
            "seed={seed}: a genuinely removed replica must still be released"
        );
        *torn.lock().unwrap() = 0;
        *leader_cell.lock().unwrap() = Some(b.clone());

        // Placement re-adds A: re-host empty, leader adds it as a learner.
        rec.tick(&tab(&[A, B, C], 4)).await;
        tenv.sleep(Duration::from_millis(50 + tenv.gen_below(100)))
            .await;
        let _ = b.add_learner(nid(A));
        let an = rec.hosted_node(T).expect("A re-hosted").clone();
        // Wait until A itself knows it is a learner (the leader's entry landed).
        for _ in 0..100 {
            if an.learners().contains(&nid(A)) {
                break;
            }
            tenv.sleep(Duration::from_millis(50)).await;
        }

        // The racing promoter: after the learner is caught up, promote it
        // after a seed-drawn delay that straddles the release confirmation.
        let delta = tenv.gen_below(160);
        let (lead2, denv) = (b.clone(), tenv.clone());
        tenv.spawn_task(async move {
            for _ in 0..4000 {
                if lead2.learners().contains(&nid(A)) && lead2.learner_caught_up(&nid(A), 4) {
                    break;
                }
                denv.sleep(Duration::from_millis(1)).await;
            }
            denv.sleep(Duration::from_millis(delta)).await;
            let _ = lead2.promote_learner(nid(A));
        });

        // A's own view flips to excluding it (stale/replay-transient) for
        // many ticks at an UNCHANGED epoch, cadence drawn per seed.
        let cadence = 10 + tenv.gen_below(30);
        let v2 = tab(&[B, C], 5);
        for _ in 0..14 {
            rec.tick(&v2).await;
            tenv.sleep(Duration::from_millis(cadence)).await;
        }
        tenv.sleep(Duration::from_secs(2)).await;

        // Truth again: A is a replica.
        let v3 = tab(&[A, B, C], 6);
        let mut refused = false;
        let mut final_voter = false;
        let mut final_caught_up = false;
        for _ in 0..120 {
            rec.tick(&v3).await;
            tenv.sleep(Duration::from_millis(250)).await;
            if let Some(n) = rec.hosted_node(T) {
                if n.refused_as_voter() {
                    refused = true;
                    break;
                }
                if b.config().contains(&nid(A))
                    && n.config().contains(&nid(A))
                    && n.commit_index() >= b.commit_index()
                {
                    final_voter = true;
                    final_caught_up = true;
                    break;
                }
            }
        }
        let info = format!(
            "leader cfg={:?} learners={:?} A cfg={:?} A learners={:?}",
            b.config(),
            b.learners(),
            rec.hosted_node(T).map(|n| n.config()),
            rec.hosted_node(T).map(|n| n.learners()),
        );
        *out2.lock().unwrap() = Some(Outcome {
            torn_while_member: *torn.lock().unwrap(),
            refused,
            final_voter,
            final_caught_up,
            info,
        });
    });
    for _ in 0..900 {
        sim.run_for(Duration::from_secs(1));
        if out.lock().unwrap().is_some() {
            break;
        }
    }
    let r = out.lock().unwrap().clone();
    r.unwrap_or_else(|| panic!("seed={seed}: scenario did not finish"))
}

fn check(name: &str, led: bool) {
    corpus::for_each_seed(
        name,
        corpus::seeds_from_env("ANIMUS_RELEASE_RACE_SEEDS"),
        |seed| {
            let o = run(seed, led);
            assert!(
                !o.refused,
                "{name} seed={seed}: the re-hosted replica was refused as a voter -- the \
             release-vs-promote race: {o:?}"
            );
            assert_eq!(
                o.torn_while_member, 0,
                "{name} seed={seed}: the replica was torn down while the leader counted it as a \
             member (a learner is not 'excluded'): {o:?}"
            );
            assert!(
                o.final_voter && o.final_caught_up,
                "{name} seed={seed}: the replica must end a caught-up voter: {o:?}"
            );
        },
    );
}

#[test]
fn a_promoted_learner_is_never_released_and_erased() {
    check("release_race_plain", false);
}

#[test]
fn a_promoted_learner_that_previously_led_is_never_released_and_erased() {
    check("release_race_previously_led", true);
}
