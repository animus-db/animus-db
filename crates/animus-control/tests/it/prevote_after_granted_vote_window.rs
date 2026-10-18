//! Issue #930 under real faults: a voter that has just granted a REAL vote must
//! reject a competing PRE-vote until the winner's first `AppendEntries` arrives
//! (or its own election deadline lapses). Before the fix, `leader_id` was only
//! ever set by `AppendEntries`, so in the window between "granted the vote" and
//! "heard the winner" a voter was unprotected: another voter's pre-vote won, its
//! real election deposed the just-elected leader, and the cluster could chain
//! return bouts for seconds with no partition or crash involved.
//!
//! `pre_vote.rs` pins the rule on a bare `RaftCore`. Here a five-voter
//! `Simulator` cluster builds the window with per-link delays around a directed
//! leadership transfer (so no node's initial election timeout muddies it):
//!
//! * `W` (transfer target) campaigns for real at once and wins with the votes of
//!   the old leader `L` and the fast voter `Y`;
//! * `W`'s links to the slow voters `V`, `X` carry the vote request after `d1`
//!   (a little over one election base). The instant `W` becomes a candidate its
//!   requests are already in flight, so the links to `V`, `X` **and `Y`** are
//!   then switched to `d2 = d1 + 0.85 * base`: the first heartbeat reaches them
//!   only at `d2`. `V`/`X` grant a real vote at `d1` and hear the winner at `d2`;
//! * `Y` granted `W` immediately but, never hearing `W` before `d2`, its own
//!   election timer (fresh from that grant, `[base, 2*base)`) fires while `V` and
//!   `X` are in the window and `Y` pre-votes them. `Y + V + X` is a pre-vote
//!   majority unless the vote lease protects `V`/`X`.
//!
//! With the fix `V`/`X` are protected from their grant for at least one election
//! base (`d1 + base > d2`, and `> Y`'s latest fire), so `Y` never wins; without
//! it `Y` wins, its real election at `term + 1` deposes `W`. The delay/target
//! choice is seeded, so the corpus covers many alignments.
//!
//! Oracle (bounded term churn): once `W` is observed leading at term `T`, no
//! node may ever lead at a higher term during the run, and at the end `W` is the
//! sole leader at term `T`. Every assertion prints the seed; replay with
//! `ANIMUS_SEED=<seed> cargo test -p animus-control --test it
//! prevote_after_granted_vote_window::`.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{RaftNode, Role};
use animus_env::nid;
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

const N: u64 = 5;

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn seeds() -> Vec<u64> {
    if let Ok(s) = std::env::var("ANIMUS_SEED") {
        return vec![s.parse().expect("ANIMUS_SEED")];
    }
    (0..24).map(|i| 0x930_0000 + i * 0x9E37).collect()
}

fn link(base: Duration) -> NetConfig {
    let mut c = NetConfig::default();
    c.base_delay = base;
    c.max_jitter = Duration::from_millis(3);
    c
}

fn run_one(seed: u64) {
    let all: Vec<_> = (0..N).map(nid).collect();
    let mut sim = Simulator::new(seed);
    let nodes: Vec<RaftNode<SimEnv>> = (0..N)
        .map(|i| RaftNode::start(sim.env(nid(i)), all.clone(), MemoryEngine::new()))
        .collect();
    sim.run_for(Duration::from_secs(2));
    let leaders: Vec<usize> = (0..N as usize).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(leaders.len(), 1, "expected one leader (seed={seed})");
    let old = leaders[0];
    let others: Vec<usize> = (0..N as usize).filter(|&i| i != old).collect();
    let r = splitmix64(seed);
    let w = others[(r % 4) as usize];
    let rest: Vec<usize> = others.iter().copied().filter(|&i| i != w).collect();
    let y = rest[((r >> 8) % 3) as usize];
    // V and X get the vote request late; V, X and Y all hear the winner late.
    let slow: Vec<usize> = rest.iter().copied().filter(|&i| i != y).collect();
    let mute: Vec<usize> = rest.clone();
    let t0 = nodes[old].term();

    let base = nodes[0].election_timeout();
    // d1 >= 1.1 * base: the slow voters' lease (from their grant, >= base) then
    // outlives Y's latest possible fire (2 * base after its early grant).
    // d2 - d1 < base: the winner is heard before any lease can lapse.
    let d1 = base + base * (10 + (r >> 16) as u32 % 10) / 100;
    let d2 = d1 + base * 85 / 100;
    for &v in &slow {
        sim.set_link_net_config(nid(w as u64), nid(v as u64), link(d1));
    }

    assert!(
        nodes[old].transfer_leadership(nid(w as u64)),
        "transfer refused (seed={seed})"
    );
    // The instant W becomes a candidate its vote requests are already in flight
    // (on the d1 links); only then slow the first heartbeat on those links.
    let mut switched = false;
    for _ in 0..200 {
        sim.run_for(Duration::from_millis(1));
        if nodes[w].role() == Role::Candidate || nodes[w].is_leader() {
            for &v in &mute {
                sim.set_link_net_config(nid(w as u64), nid(v as u64), link(d2));
            }
            switched = true;
            break;
        }
    }
    assert!(switched, "W never campaigned (seed={seed})");

    // Observe leadership every 2ms for 5 virtual seconds.
    let mut led: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    let mut first: Option<(usize, u64)> = None;
    for _ in 0..2500 {
        for (i, n) in nodes.iter().enumerate() {
            if i != old && n.is_leader() {
                let who = led.entry(n.term()).or_default();
                if !who.contains(&i) {
                    who.push(i);
                }
                first.get_or_insert((i, n.term()));
            }
        }
        sim.run_for(Duration::from_millis(2));
    }
    let (winner, t1) = first.unwrap_or_else(|| panic!("no leader was elected (seed={seed})"));
    assert!(t1 > t0, "stale leadership observed (seed={seed})");
    assert_eq!(
        winner, w,
        "the transfer target did not win first: leaders by term {led:?} (seed={seed})"
    );
    let higher: Vec<_> = led.iter().filter(|(t, _)| **t > t1).collect();
    assert!(
        higher.is_empty(),
        "the just-elected leader {winner} (term {t1}) was deposed by a return-bout election: \
         leaders by term {led:?} (seed={seed}; issue #930 pre-vote window)"
    );
    let end: Vec<usize> = (0..N as usize).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(
        end,
        vec![winner],
        "leadership not stable on the first winner (seed={seed})"
    );
    assert_eq!(nodes[winner].term(), t1, "term churned (seed={seed})");
}

#[test]
fn voter_that_just_granted_a_real_vote_rejects_a_competing_prevote() {
    for seed in seeds() {
        run_one(seed);
    }
}
