//! Stage D **in-band new-group creation** (ADR 0017 D): a tablet split where each
//! original replica spawns its *own* new-tablet replica when the committed `Split`
//! applies — no external handoff, no harness `start_seeded`. This is the piece the
//! Stage-D split deferred: it needs the `Coresident` `Env`-seam extension so a
//! replica can mint a **sibling inbox at runtime** for the co-resident new group
//! (the inbox is single-consumer, so the new group needs its own id per node).
//!
//! Flow: wire each original replica with an [`in_band_split_hook`] (its own
//! co-resident env + its id in the new group); on apply of `Split`, the hook mints
//! `sibling(my_new_id)` and starts the new replica there, seeded with the
//! handed-off `[at, ∞)` range. The new group forms entirely from the apply path.
//!
//! Deterministic + seed-reproducible (drive with `run_for`, never `run()`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Original tablet group ids and the new tablet's group ids — one new inbox per
/// physical node (`NEW[i]` is co-resident with `ORIG[i]`), distinct from the
/// originals per the single-consumer rule.
const ORIG: [u64; 3] = [0, 1, 2];
const NEW: [u64; 3] = [10, 11, 12];

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

fn put(node: &KvNode, key: &[u8], value: &[u8], seed: u64) {
    assert!(
        matches!(
            node.put(key.to_vec(), value.to_vec()),
            ProposeResult::Accepted { .. }
        ),
        "leader rejected a put (seed={seed})"
    );
}

/// Bring up the original group with each replica wired for in-band split: on apply
/// of `Split`, replica `i` mints its co-resident sibling `NEW[i]` and starts the
/// new-tablet replica there. Returns the original nodes + the shared sink the
/// created new-group replicas land in.
fn group_with_in_band_split(sim: &Simulator) -> (Vec<KvNode>, Arc<Mutex<Vec<KvNode>>>) {
    let created: Arc<Mutex<Vec<KvNode>>> = Arc::new(Mutex::new(Vec::new()));
    let orig: Vec<KvNode> = (0..ORIG.len())
        .map(|i| {
            let env_i = sim.env(ORIG[i]);
            let hook = RaftKvNode::in_band_split_hook(
                env_i.clone(),
                NEW[i],
                NEW.to_vec(),
                MemoryEngine::new,
                Arc::clone(&created),
            );
            RaftKvNode::start_with_split_hook(env_i, ORIG.to_vec(), MemoryEngine::new(), hook)
        })
        .collect();
    (orig, created)
}

#[test]
fn split_creates_the_new_group_in_band() {
    let seed = 0x5B;
    let mut sim = Simulator::new(seed);
    let (orig, created) = group_with_in_band_split(&sim);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&orig, seed);

    // Populate the whole key space: k00..k19.
    for i in 0..20u32 {
        put(
            &orig[l],
            format!("k{i:02}").as_bytes(),
            format!("v{i}").as_bytes(),
            seed,
        );
    }
    sim.run_for(Duration::from_secs(2));

    // Split at "k10": keys >= k10 go to the new tablet. NO handoff capture, NO
    // harness start_seeded — every replica creates its new-group member in band on
    // apply.
    assert!(matches!(
        orig[l].propose_split(b"k10".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    // Commit the split on the original group, then let each replica's apply spawn
    // its co-resident new replica and the new group elect a leader.
    sim.run_for(Duration::from_secs(5));

    // The new group was created entirely in band: one replica per original node.
    let new = created.lock().unwrap().clone();
    assert_eq!(
        new.len(),
        ORIG.len(),
        "each original replica should have spawned a co-resident new-group replica (seed={seed})"
    );

    // The original now serves only [k00, k10): kept the lower range, dropped the
    // upper one on every replica.
    for n in &orig {
        assert_eq!(
            block_on(n.local_get(b"k05")),
            Some(b"v5".to_vec()),
            "orig kept lower range (seed={seed})"
        );
        assert_eq!(
            block_on(n.local_get(b"k15")),
            None,
            "orig dropped the handed-off range (seed={seed})"
        );
    }

    // The in-band-created new group serves [k10, ∞): the seeded handoff is present
    // on every new replica, and it does not hold the lower range.
    for n in &new {
        assert_eq!(
            block_on(n.local_get(b"k15")),
            Some(b"v15".to_vec()),
            "new group missing the handed-off range (seed={seed})"
        );
        assert_eq!(
            block_on(n.local_get(b"k05")),
            None,
            "new group should not hold the lower range (seed={seed})"
        );
    }

    // Both groups operate independently afterward: a write into each range lands on
    // the owning group only. The new group elected its own leader.
    let nl = leader(&new, seed);
    put(&new[nl], b"k17", b"v17new", seed);
    put(&orig[l], b"k03", b"v3new", seed);
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        block_on(new[nl].local_get(b"k17")),
        Some(b"v17new".to_vec())
    );
    assert_eq!(block_on(orig[l].local_get(b"k03")), Some(b"v3new".to_vec()));
    // The new write did not leak into the original group's now-foreign range.
    assert_eq!(block_on(orig[l].local_get(b"k17")), None);
}

#[test]
fn in_band_split_is_deterministic_from_seed() {
    let observe = |seed: u64| {
        let mut sim = Simulator::new(seed);
        let (orig, _created) = group_with_in_band_split(&sim);
        sim.run_for(Duration::from_secs(2));
        let l = leader(&orig, seed);
        for i in 0..10u32 {
            put(&orig[l], format!("k{i:02}").as_bytes(), b"v", seed);
        }
        sim.run_for(Duration::from_secs(1));
        let _ = orig[l].propose_split(b"k05".to_vec());
        sim.run_for(Duration::from_secs(5));
        sim.trace_lines()
    };
    assert_eq!(
        observe(0x77),
        observe(0x77),
        "same seed reproduces the trace"
    );
}
