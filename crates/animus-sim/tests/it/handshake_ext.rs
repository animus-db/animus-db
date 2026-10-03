//! The handshake `ext` model (ADR 0073 Phase 2, P2-A): `set_network_ext_for`,
//! `Envelope::peer_ext` stamped with the sender's ext at send time, disjoint
//! version ranges refused in both directions, and the per-node
//! `Network::set_require_peer_ext` era-on refusal of empty-ext (Phase 1)
//! peers — all under drops, duplication, a partition, and a node restart that
//! changes its ext. Everything is a pure function of the seed.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::handshake::{encode_ext, parse_ext_range};
use animus_env::{Clock, EnvExt, Network, NodeId, nid};
use animus_sim::{NetConfig, Simulator};

fn seed_from_env(default: u64) -> u64 {
    match std::env::var("ANIMUS_SEED") {
        Ok(s) => s.parse().unwrap_or(default),
        Err(_) => default,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Got {
    to: u64,
    from: NodeId,
    now: u64,
    payload: Vec<u8>,
    peer_ext: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    got: Vec<Got>,
    refusals: Vec<u64>,
    phase1_drops: usize,
    protocol_drops: usize,
    trace: Vec<String>,
    require_on_at: u64,
    restart_at: u64,
}

const N: u64 = 4;
const ROUND: Duration = Duration::from_millis(10);

fn range_of(ext: &[u8]) -> (u32, u32) {
    parse_ext_range(ext)
        .expect("test ext well-formed")
        .unwrap_or((1, 1))
}

fn run_scenario(seed: u64) -> Outcome {
    let mut sim = Simulator::new(seed);
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(0.15);
    cfg.set_duplicate_prob(0.2);
    sim.set_net_config(cfg);

    // node 0: [1,2]; node 1: [2,3] (restarts to [4,5]); node 2: no ext (a
    // Phase 1 binary, i.e. [1,1]); node 3: [1,3] and turns the require flag
    // on mid-run.
    let mut ext: Vec<Vec<u8>> = vec![
        encode_ext(Some((1, 2)), Some("b0")),
        encode_ext(Some((2, 3)), None),
        Vec::new(),
        encode_ext(Some((1, 3)), None),
    ];
    for (i, e) in ext.iter().enumerate() {
        sim.set_network_ext_for(nid(i as u64), e.clone());
    }

    let log = Arc::new(Mutex::new(Vec::<Got>::new()));
    for i in 0..N {
        let env = sim.env(nid(i));
        let log = Arc::clone(&log);
        env.clone().spawn_task(async move {
            loop {
                let e = env.recv().await;
                log.lock().unwrap().push(Got {
                    to: i,
                    from: e.from.clone(),
                    now: env.now().0,
                    payload: e.payload.clone(),
                    peer_ext: e.peer_ext.to_vec(),
                });
            }
        });
    }

    // Seed-derived round where node 3 flips the require flag.
    let require_round = 3 + (seed % 4);
    let mut require_on_at = 0;
    let mut restart_at = 0;
    let send_round = |sim: &Simulator, ext: &[Vec<u8>], round: u8| {
        for i in 0..N {
            let env = sim.env(nid(i));
            let mut payload = vec![round];
            payload.extend_from_slice(&ext[i as usize]);
            env.clone().spawn_task(async move {
                for j in 0..N {
                    if j != i {
                        env.send(nid(j), payload.clone()).await;
                    }
                }
            });
        }
    };
    for r in 0..24u8 {
        if u64::from(r) == require_round {
            sim.env(nid(3)).set_require_peer_ext(true);
            require_on_at = sim.now().0;
        }
        if r == 10 {
            // Partition 2 <-> 3 for a few rounds, then heal.
            sim.partition_pair(nid(2), nid(3));
        }
        if r == 14 {
            sim.heal(nid(2), nid(3));
            sim.heal(nid(3), nid(2));
        }
        if r == 12 {
            // Quiesce (let every in-flight message land under the old ext),
            // then "restart" node 1 with a new, disjoint-from-most range.
            sim.run_for(Duration::from_millis(500));
            ext[1] = encode_ext(Some((4, 5)), Some("b1-new"));
            sim.set_network_ext_for(nid(1), ext[1].clone());
            restart_at = sim.now().0;
        }
        send_round(&sim, &ext, r);
        sim.run_for(ROUND);
    }
    sim.run_for(Duration::from_millis(500));

    let trace = sim.trace_lines();
    let count = |reason: &str| {
        trace
            .iter()
            .filter(|l| l.contains(&format!("({reason})")))
            .count()
    };
    Outcome {
        got: log.lock().unwrap().clone(),
        refusals: (0..N).map(|i| sim.protocol_refusals(&nid(i))).collect(),
        phase1_drops: count("phase1-peer-refused"),
        protocol_drops: count("protocol-refused"),
        trace,
        require_on_at,
        restart_at,
    }
}

fn check(seed: u64, o: &Outcome) {
    assert!(!o.got.is_empty(), "seed={seed}: nothing delivered at all");
    for g in &o.got {
        // peer_ext is the sender's ext as of send time (payload[1..] carries
        // exactly that, stamped by the sender at send time).
        assert_eq!(
            g.peer_ext,
            g.payload[1..].to_vec(),
            "seed={seed}: peer_ext must equal the sender's ext at send time ({g:?})"
        );
        // Disjoint ranges are never delivered. Node 1's ext changed only
        // after a full quiesce at `restart_at`, so each side's ext at this
        // delivery is known exactly.
        let sender = g.from.as_str().parse::<u64>().ok();
        let sender_range = range_of(&g.peer_ext);
        let own_ext_now = |node: u64| -> (u32, u32) {
            match node {
                0 => (1, 2),
                1 if g.now > o.restart_at => (4, 5),
                1 => (2, 3),
                2 => (1, 1),
                _ => (1, 3),
            }
        };
        let own = own_ext_now(g.to);
        assert!(
            sender_range.0 <= own.1 && own.0 <= sender_range.1,
            "seed={seed}: disjoint ranges must never be delivered ({g:?}, sender={sender:?})"
        );
        // After node 3 requires an ext, no empty-ext sender reaches it.
        if g.to == 3 && g.now > o.require_on_at {
            assert!(
                !g.peer_ext.is_empty(),
                "seed={seed}: node 3 delivered an empty-ext envelope after require=on ({g:?})"
            );
        }
    }
    assert!(
        o.protocol_drops > 0,
        "seed={seed}: disjoint ranges were never refused (vacuous scenario)"
    );
    assert!(
        o.phase1_drops > 0,
        "seed={seed}: the require flag never refused anything (vacuous scenario)"
    );
    let total: u64 = o.refusals.iter().sum();
    assert_eq!(
        total as usize,
        o.protocol_drops + o.phase1_drops,
        "seed={seed}: protocol_refusals must count exactly the refusal drops"
    );
}

#[test]
fn ext_stamped_at_send_time_and_refusals_hold_under_faults() {
    let base = seed_from_env(0xE47_0001);
    let seeds: Vec<u64> = if std::env::var("ANIMUS_SEED").is_ok() {
        vec![base]
    } else {
        (0..6).map(|k| base + k).collect()
    };
    for seed in seeds {
        let o = run_scenario(seed);
        check(seed, &o);
        // Reproducible: the same seed yields the identical run, including
        // every refusal count.
        let again = run_scenario(seed);
        assert_eq!(
            o, again,
            "seed={seed}: run must be a pure function of the seed"
        );
    }
}

#[test]
fn default_nodes_see_empty_peer_ext_and_no_refusals() {
    let seed = seed_from_env(0xE47_0002);
    let mut sim = Simulator::new(seed);
    let a = sim.env(nid(0));
    let b = sim.env(nid(1));
    let got = Arc::new(Mutex::new(None));
    {
        let (b, got) = (b.clone(), Arc::clone(&got));
        b.clone().spawn_task(async move {
            *got.lock().unwrap() = Some(b.recv().await);
        });
    }
    a.clone()
        .spawn_task(async move { a.send(nid(1), vec![7]).await });
    sim.run_for(Duration::from_millis(50));
    let e = got.lock().unwrap().clone().expect("delivered");
    assert!(e.peer_ext.is_empty(), "seed={seed}");
    assert_eq!(sim.protocol_refusals(&nid(1)), 0, "seed={seed}");
}
