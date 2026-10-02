//! The replicated credential catalog, end to end through real Raft (ADR
//! 0066 §1/§2/§3). Mirrors `backup_catalog.rs`'s own conventions for this
//! crate's catalog-shaped `MetaCommand` families:
//!
//! 1. `PutCredential` on the leader of a 3-node control group and see it
//!    committed and replicated to every follower;
//! 2. `RotateCredential` and confirm both the new secret and the grace
//!    window's `PreviousSecret` replicate identically everywhere, with the
//!    grace-window arithmetic checked directly against
//!    `Metadata::verify_secret_candidates`;
//! 3. **kill the leader** and assert the catalog survives on the survivors;
//! 4. `RevokeCredential` on the new leader and see the removal replicate;
//! 5. a real node restart (WAL + system-keyspace engine recovery, ADR 0038)
//!    recovers the catalog exactly like every other `Metadata` collection;
//! 6. every run is a pure function of its seed.

use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{MetaCommand, Policy, RaftNode, SecretKey};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader among {live:?}, found {leaders:?} (seed={seed})"
    );
    leaders[0]
}

fn propose_accepted(node: &RaftNode<SimEnv>, command: MetaCommand, what: &str, seed: u64) {
    assert!(
        matches!(node.propose(command), ProposeResult::Accepted { .. }),
        "{what} rejected (seed={seed})"
    );
}

#[test]
fn credentials_catalog_replicates_survives_leader_kill_and_revoke() {
    run(0xC7ED_0001);
}

fn run(seed: u64) {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    // 1. `PutCredential` — creates a fresh row, replicated identically to
    //    every node.
    propose_accepted(
        &nodes[leader],
        MetaCommand::PutCredential {
            id: "AKIDEXAMPLE".into(),
            secret: SecretKey::new("wJalrXUtnFEMI"),
            policy: Policy::allow_all(),
            enabled: true,
            now: 1_000,
        },
        "PutCredential",
        seed,
    );
    sim.run_for(Duration::from_secs(1));
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        let row = m
            .credential("AKIDEXAMPLE")
            .unwrap_or_else(|| panic!("node {i}: credential row missing (seed={seed})"));
        assert_eq!(row.secret, SecretKey::new("wJalrXUtnFEMI"));
        assert_eq!(row.previous, None);
        assert!(row.enabled);
        assert_eq!(row.created_at, 1_000);
        assert_eq!(row.updated_at, 1_000);
    }

    // 2. `RotateCredential` — the outgoing secret moves to `previous` with
    //    a grace window; both secrets verify inside it, only the new one
    //    once it closes.
    propose_accepted(
        &nodes[leader],
        MetaCommand::RotateCredential {
            id: "AKIDEXAMPLE".into(),
            new_secret: SecretKey::new("newsecretvalue"),
            grace_secs: 60,
            now: 1_100,
        },
        "RotateCredential",
        seed,
    );
    sim.run_for(Duration::from_secs(1));
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        let row = m
            .credential("AKIDEXAMPLE")
            .unwrap_or_else(|| panic!("node {i}: credential row missing (seed={seed})"));
        assert_eq!(row.secret, SecretKey::new("newsecretvalue"));
        let previous = row
            .previous
            .as_ref()
            .unwrap_or_else(|| panic!("node {i}: no grace window recorded (seed={seed})"));
        assert_eq!(previous.secret, SecretKey::new("wJalrXUtnFEMI"));
        assert_eq!(previous.valid_until, 1_100 + 60);

        // Inside the window: both secrets verify, current first.
        let candidates: Vec<&str> = m
            .verify_secret_candidates("AKIDEXAMPLE", 1_130)
            .map(SecretKey::as_str)
            .collect();
        assert_eq!(
            candidates,
            vec!["newsecretvalue", "wJalrXUtnFEMI"],
            "node {i}: expected both secrets valid inside the grace window (seed={seed})"
        );
        // Past the window: only the new secret.
        let candidates: Vec<&str> = m
            .verify_secret_candidates("AKIDEXAMPLE", 1_200)
            .map(SecretKey::as_str)
            .collect();
        assert_eq!(
            candidates,
            vec!["newsecretvalue"],
            "node {i}: expected only the current secret past the grace window (seed={seed})"
        );
    }

    // 3. Kill the leader; survivors re-elect and must still hold the
    //    catalog, byte-for-byte identical across survivors.
    sim.crash(nid(leader as u64));
    sim.run_for(Duration::from_secs(3));
    let survivors: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    let new_leader = unique_leader(&nodes, &survivors, seed);
    assert!(survivors.contains(&new_leader));

    let a = nodes[survivors[0]].metadata();
    let b = nodes[survivors[1]].metadata();
    assert_eq!(
        a.credentials, b.credentials,
        "survivor credential catalogs diverged after leader kill (seed={seed})"
    );
    assert_eq!(
        a.credential("AKIDEXAMPLE").unwrap().secret,
        SecretKey::new("newsecretvalue")
    );

    // 4. `RevokeCredential` on the new leader — the removal replicates, and
    //    a repeat is idempotent.
    propose_accepted(
        &nodes[new_leader],
        MetaCommand::RevokeCredential {
            id: "AKIDEXAMPLE".into(),
        },
        "RevokeCredential",
        seed,
    );
    sim.run_for(Duration::from_secs(1));
    for &i in &survivors {
        let m = nodes[i].metadata();
        assert!(
            m.credential("AKIDEXAMPLE").is_none(),
            "node {i}: credential row survived RevokeCredential (seed={seed})"
        );
    }
    propose_accepted(
        &nodes[new_leader],
        MetaCommand::RevokeCredential {
            id: "AKIDEXAMPLE".into(),
        },
        "repeated RevokeCredential",
        seed,
    );
}

#[test]
fn credentials_catalog_is_reproducible_from_seed() {
    fn trace(seed: u64) -> Vec<String> {
        let (mut sim, nodes) = cluster(seed);
        sim.run_for(Duration::from_secs(2));
        let leader = unique_leader(&nodes, &[0, 1, 2], seed);
        nodes[leader].propose(MetaCommand::PutCredential {
            id: "AKIDEXAMPLE".into(),
            secret: SecretKey::new("s0"),
            policy: Policy::allow_all(),
            enabled: true,
            now: 1_000,
        });
        sim.run_for(Duration::from_secs(1));
        nodes[leader].propose(MetaCommand::RotateCredential {
            id: "AKIDEXAMPLE".into(),
            new_secret: SecretKey::new("s1"),
            grace_secs: 60,
            now: 1_100,
        });
        sim.run_for(Duration::from_secs(1));
        sim.crash(nid(leader as u64));
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    }
    assert_eq!(trace(0xC7ED_5EED), trace(0xC7ED_5EED));
}

/// WAL/snapshot recovery (ADR 0038): a node whose process stops (tasks and
/// volatile state gone, its durable system-keyspace engine kept) and
/// restarts on the same disk recovers the credential catalog exactly like
/// every other `Metadata` collection — mirroring `backup_catalog.rs`'s own
/// pattern.
#[test]
fn credentials_catalog_survives_node_restart() {
    let seed = 0xC7ED_5715;
    let mut sim = Simulator::new(seed);
    // `MemoryEngine` clones share state (a real node's on-disk engine
    // surviving a process restart) — re-cloning the *same* handle at
    // restart is what exercises genuine durable recovery, not a fresh
    // empty engine.
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let mut nodes: Vec<RaftNode<SimEnv>> = NODES
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                engines[id as usize].clone(),
            )
        })
        .collect();

    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    propose_accepted(
        &nodes[leader],
        MetaCommand::PutCredential {
            id: "AKIDEXAMPLE".into(),
            secret: SecretKey::new("s0"),
            policy: Policy::allow_all(),
            enabled: true,
            now: 1_000,
        },
        "PutCredential",
        seed,
    );
    sim.run_for(Duration::from_secs(2));
    let follower = (0..3).find(|&i| i != leader).unwrap();
    assert!(
        nodes[follower]
            .metadata()
            .credential("AKIDEXAMPLE")
            .is_some(),
        "follower has the pre-stop credential row"
    );

    // Stop the follower's process; the surviving majority keeps committing
    // while it is down.
    sim.stop(nid(follower as u64));
    propose_accepted(
        &nodes[leader],
        MetaCommand::RotateCredential {
            id: "AKIDEXAMPLE".into(),
            new_secret: SecretKey::new("s1"),
            grace_secs: 60,
            now: 1_100,
        },
        "RotateCredential",
        seed,
    );
    sim.run_for(Duration::from_secs(2));

    // Restart the stopped node on the same disk — it recovers from the WAL
    // and the durable system-keyspace engine, exactly like a real restart.
    nodes[follower] = RaftNode::start(
        sim.env(nid(follower as u64)),
        NODES.iter().copied().map(nid).collect(),
        engines[follower].clone(),
    );
    sim.run_for(Duration::from_secs(3));

    let reference = nodes[leader].metadata();
    assert_eq!(
        reference.credential("AKIDEXAMPLE").unwrap().secret,
        SecretKey::new("s1")
    );
    for (i, n) in nodes.iter().enumerate() {
        let m = n.metadata();
        assert_eq!(
            m.credentials, reference.credentials,
            "node {i} credential catalog diverged after restart (seed={seed})"
        );
    }
}
