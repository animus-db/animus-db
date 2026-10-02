//! ADR 0026's 2026-09-28 amendment (`Network::close_stream`) — a
//! seed-reproducible, fault-injecting end-to-end regression against the real
//! `host::Reconciler` teardown path.
//!
//! Before that amendment, `Demux`/`SimEnv`'s per-stream inbox queued every
//! frame addressed to a stream forever — including a tablet's stream on a
//! node that has since been dropped from its replica set (a live, healthy
//! voter removed via reconfiguration, mirroring the existing
//! `ANIMUS_RECONFIGURE_DROP_SEEDS` corpus, issue #781) but that peers may
//! keep addressing for a time during the teardown grace window. This file
//! drives that exact shape — a live follower released from a 3-way tablet
//! group's replica set through the real `host::Reconciler`, under injected
//! message loss/delay — and asserts:
//!
//! 1. the released node's own env inbox for the tablet's stream ends
//!    genuinely **empty**, not merely reported empty by `Reconciler` state;
//! 2. the stream is **tombstoned** (`Simulator::stream_is_closed`);
//! 3. a frame that arrives afterward (a peer still addressing the released
//!    node — the literal leak scenario this fix closes) is discarded, not
//!    queued;
//! 4. linearizability/convergence hold throughout for the surviving
//!    replicas, both immediately after the drop and after a fresh write;
//! 5. re-adding the SAME node to the SAME tablet later converges — proving
//!    `recv_stream`'s reopen actually works, not just that closing does.
//!
//! Harness shape mirrors `tests/reconciler_corpus.rs`'s own
//! `Reconciler`-driven-by-scripted-`MetadataView`s pattern (no live
//! control-plane `RaftNode` needed — `MetadataView` is a plain caller-
//! supplied projection). Deterministic + seed-reproducible (ADR 0003):
//! replay a specific run with `ANIMUS_SEED=<seed> cargo test -p
//! animus-cp-data --test demux_stream_teardown`.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::host::{MemoryTabletEngines, MetadataView, Reconciler};
use animus_env::{Clock, EnvExt, Network, NodeId, nid};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, Tablet, TabletId};

type Recon = Reconciler<SimEnv, MemoryEngine>;

const TABLET: TabletId = TabletId(1);
const TABLE: &str = "t";

fn node_x() -> NodeId {
    nid(700)
}
fn node_y() -> NodeId {
    nid(701)
}
fn node_z() -> NodeId {
    nid(702)
}
/// A driver env id used only to host this scenario's own top-level script
/// task and to send a raw "late peer" frame — never a cluster node id.
fn driver_id() -> NodeId {
    nid(799)
}

fn tablet(replicas: Vec<NodeId>) -> Tablet {
    Tablet::new_for_table(TABLET, TABLE, KeyRange::whole(), replicas)
}

fn view(replicas: Vec<NodeId>) -> MetadataView {
    MetadataView {
        tablets: [(TABLET, tablet(replicas))].into_iter().collect(),
        ..Default::default()
    }
}

fn make_reconciler(sim: &Simulator, id: NodeId) -> Recon {
    Reconciler::new(
        sim.env(id.clone()),
        MemoryTabletEngines::new(),
        id,
        |_t, _n| {},
        |_t| {},
    )
}

/// The current leader among whichever of `nodes` currently host the tablet,
/// asserting at most one.
fn current_leader(nodes: &[(&Recon, NodeId)]) -> Option<NodeId> {
    let leaders: Vec<NodeId> = nodes
        .iter()
        .filter(|(r, _)| r.hosted_node(TABLET).is_some_and(|n| n.is_leader()))
        .map(|(_, id)| id.clone())
        .collect();
    assert!(leaders.len() <= 1, "more than one leader: {leaders:?}");
    leaders.into_iter().next()
}

/// Retries a linearizable read via whichever of `live` currently leads,
/// tolerating a transient barrier failure (a leader change mid-election, or
/// a lossy-network probe timeout under this file's own injected loss rate)
/// — bounded, never a single-shot assert against a fault-injected run.
async fn wait_lin_read(
    driver: &SimEnv,
    recons: &BTreeMap<NodeId, &Recon>,
    live: &[NodeId],
    key: &[u8],
    tries: usize,
) -> Option<Vec<u8>> {
    for _ in 0..tries {
        let pairs: Vec<(&Recon, NodeId)> = live.iter().map(|id| (recons[id], id.clone())).collect();
        if let Some(l) = current_leader(&pairs)
            && let Some(node) = recons[&l].hosted_node(TABLET)
            && let Some(v) = node.linearizable_get(key).await
        {
            return Some(v);
        }
        driver.sleep(Duration::from_millis(200)).await;
    }
    None
}

/// Retries a propose via whichever of `live` currently leads, tolerating a
/// transient `NotLeader` (an election racing the caller, the same fault-
/// tolerance `wait_lin_read` above needs).
async fn wait_put_accepted(
    driver: &SimEnv,
    recons: &BTreeMap<NodeId, &Recon>,
    live: &[NodeId],
    key: &[u8],
    value: &[u8],
    tries: usize,
) -> bool {
    for _ in 0..tries {
        let pairs: Vec<(&Recon, NodeId)> = live.iter().map(|id| (recons[id], id.clone())).collect();
        if let Some(l) = current_leader(&pairs)
            && let Some(node) = recons[&l].hosted_node(TABLET)
            && matches!(
                node.put(key.to_vec(), value.to_vec()),
                animus_control::ProposeResult::Accepted { .. }
            )
        {
            return true;
        }
        driver.sleep(Duration::from_millis(200)).await;
    }
    false
}

fn poll_until(
    sim: &mut Simulator,
    budget: Duration,
    step: Duration,
    msg: &str,
    mut check: impl FnMut() -> bool,
) {
    let mut waited = Duration::ZERO;
    while waited < budget {
        sim.run_for(step);
        waited += step;
        if check() {
            return;
        }
    }
    panic!("{msg} (seed={})", sim.seed());
}

/// Drives the async `body` to completion under the outer `Simulator`,
/// exactly like `reconciler_corpus.rs`'s own `run` — `Reconciler::teardown`
/// internally polls `env.sleep()`, so the body must run as a spawned task
/// advanced by `run_for`, never a bare `block_on` (see this crate's root
/// `CLAUDE.md` gotcha).
fn run<F, Fut>(seed: u64, body: F)
where
    F: FnOnce(Simulator) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut sim = Simulator::new(seed);
    let driver_env = sim.env(driver_id());
    let done = Arc::new(Mutex::new(false));
    let done2 = Arc::clone(&done);
    let sim_in_task = sim.clone();
    driver_env.spawn_task(async move {
        body(sim_in_task).await;
        *done2.lock().unwrap() = true;
    });
    poll_until(
        &mut sim,
        Duration::from_secs(120),
        Duration::from_secs(1),
        "scenario never completed",
        move || *done.lock().unwrap(),
    );
}

fn scenario(seed: u64) {
    run(seed, move |sim| async move {
        // A modest, independent-per-message loss rate plus delay/jitter on
        // EVERY link (issue #781's own corpus spirit, `heartbeat_batch_
        // corpus.rs`'s 5% figure) — low enough that Raft still elects and
        // replicates within this scenario's own generous budget, high
        // enough to genuinely exercise "a peer's message to the released
        // node doesn't always land instantly," the real-world shape of the
        // teardown grace window this fix closes.
        let mut net = NetConfig::default();
        net.base_delay = Duration::from_millis(5);
        net.max_jitter = Duration::from_millis(20);
        net.set_drop_prob(0.05);
        sim.set_net_config(net);

        let driver = sim.env(driver_id());
        let mut rx = make_reconciler(&sim, node_x());
        let mut ry = make_reconciler(&sim, node_y());
        let mut rz = make_reconciler(&sim, node_z());

        // --- Bring up a 3-voter group {X, Y, Z} and confirm convergence. --
        let v1 = view(vec![node_x(), node_y(), node_z()]);
        let mut converged = false;
        for _ in 0..40 {
            rx.tick(&v1).await;
            ry.tick(&v1).await;
            rz.tick(&v1).await;
            driver.sleep(Duration::from_millis(300)).await;
            if rx.local_state().hosted.contains(&TABLET)
                && ry.local_state().hosted.contains(&TABLET)
                && rz.local_state().hosted.contains(&TABLET)
                && current_leader(&[(&rx, node_x()), (&ry, node_y()), (&rz, node_z())]).is_some()
            {
                converged = true;
                break;
            }
        }
        assert!(
            converged,
            "the initial 3-voter group never converged under loss/delay (seed={seed})"
        );

        let recons: BTreeMap<NodeId, &Recon> = [(node_x(), &rx), (node_y(), &ry), (node_z(), &rz)]
            .into_iter()
            .collect();
        let all_three = [node_x(), node_y(), node_z()];
        assert!(
            wait_put_accepted(&driver, &recons, &all_three, b"k", b"v1", 20).await,
            "the initial write was never accepted under loss/delay (seed={seed})"
        );
        driver.sleep(Duration::from_secs(2)).await;

        // --- Drop X: a live, healthy voter released from the tablet's -----
        // --- replica set via the real Reconciler teardown path. -----------
        let v2 = view(vec![node_y(), node_z()]);
        let mut released = false;
        for _ in 0..80 {
            rx.tick(&v2).await;
            ry.tick(&v2).await;
            rz.tick(&v2).await;
            driver.sleep(Duration::from_millis(300)).await;
            if rx.local_state().hosted.is_empty() {
                released = true;
                break;
            }
        }
        assert!(
            released,
            "X never released the tablet under loss/delay (seed={seed})"
        );

        // (1)+(2): the released node's own env inbox must end genuinely
        // empty AND tombstoned — not merely absent from `Reconciler` state.
        assert_eq!(
            sim.inbox_len(node_x(), TABLET.0),
            0,
            "X's own inbox for the tablet's stream must be empty after release (seed={seed})"
        );
        assert!(
            sim.stream_is_closed(node_x(), TABLET.0),
            "X's tablet stream must be tombstoned (closed) after release (seed={seed})"
        );

        // Let the raft group's own reconfigure (removing X as a voter) and
        // any leader change settle before checking convergence.
        let mut converged2 = false;
        for _ in 0..40 {
            ry.tick(&v2).await;
            rz.tick(&v2).await;
            driver.sleep(Duration::from_millis(300)).await;
            if current_leader(&[(&ry, node_y()), (&rz, node_z())]).is_some() {
                converged2 = true;
                break;
            }
        }
        assert!(
            converged2,
            "{{Y,Z}} never converged on a leader after dropping X (seed={seed})"
        );

        // (4): linearizability/convergence still hold — the pre-drop write
        // reads back via whichever of Y/Z now leads, and a fresh write
        // replicates to both.
        let recons2: BTreeMap<NodeId, &Recon> =
            [(node_y(), &ry), (node_z(), &rz)].into_iter().collect();
        let live2 = [node_y(), node_z()];
        assert_eq!(
            wait_lin_read(&driver, &recons2, &live2, b"k", 20).await,
            Some(b"v1".to_vec()),
            "linearizable read of the pre-drop write failed after dropping X (seed={seed})"
        );
        assert!(
            wait_put_accepted(&driver, &recons2, &live2, b"k2", b"v2", 20).await,
            "the post-drop write was never accepted (seed={seed})"
        );
        driver.sleep(Duration::from_secs(2)).await;
        for id in [node_y(), node_z()] {
            let node = recons2[&id].hosted_node(TABLET).unwrap();
            assert_eq!(
                node.local_get(b"k2").await,
                Some(b"v2".to_vec()),
                "node {id} missing the post-drop write (seed={seed})"
            );
        }

        // (3): a peer still addressing the released, now-closed stream
        // (the literal leak scenario) must have its frame discarded, never
        // queued.
        //
        // Several copies, not one: this file's network drops 5% of sends at
        // send time ("lossy"), and a single probe whose one send happens to
        // be lossily dropped never reaches the delivery-time closed-stream
        // check at all — so the "stream-closed" trace assertion below would
        // be a function of where the seed's RNG stream happens to land (it
        // did flip once unrelated raft/handshake changes shifted the draw
        // sequence). Eight sends make missing every one of them a
        // ~4e-11 event under any seed.
        for _ in 0..8 {
            sim.env(node_y())
                .send_stream(node_x(), TABLET.0, b"late-frame".to_vec())
                .await;
        }
        driver.sleep(Duration::from_millis(500)).await;
        assert_eq!(
            sim.inbox_len(node_x(), TABLET.0),
            0,
            "a frame addressed to the released, closed stream must be discarded, not queued \
             (seed={seed})"
        );
        assert!(
            sim.trace_lines()
                .iter()
                .any(|l| l.contains("stream-closed")),
            "the discarded frame must be traced with reason \"stream-closed\" (seed={seed})"
        );

        // (5): re-add X to the SAME tablet — proves `recv_stream`'s reopen
        // actually works, not just that closing does.
        let v3 = view(vec![node_x(), node_y(), node_z()]);
        let mut rehosted = false;
        for _ in 0..80 {
            rx.tick(&v3).await;
            ry.tick(&v3).await;
            rz.tick(&v3).await;
            driver.sleep(Duration::from_millis(300)).await;
            if rx.local_state().hosted.contains(&TABLET) {
                rehosted = true;
                break;
            }
        }
        assert!(
            rehosted,
            "X never re-hosted the tablet after being re-added (seed={seed})"
        );
        assert!(
            !sim.stream_is_closed(node_x(), TABLET.0),
            "X's own re-hosted driver must have reopened its tablet stream via recv_stream \
             (seed={seed})"
        );

        // X must genuinely rejoin and catch up (converge), not just claim a
        // local hosted-set entry.
        let mut rejoined = false;
        for _ in 0..80 {
            rx.tick(&v3).await;
            ry.tick(&v3).await;
            rz.tick(&v3).await;
            driver.sleep(Duration::from_millis(300)).await;
            let x_has_data = rx
                .hosted_node(TABLET)
                .map(|n| n.config().contains(&node_x()));
            if x_has_data == Some(true) {
                rejoined = true;
                break;
            }
        }
        assert!(
            rejoined,
            "X's re-added replica never converged into the group's own Raft config (seed={seed})"
        );
    });
}

#[test]
fn released_node_stream_ends_empty_and_tombstoned_then_reopens_on_readd() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xDE0F_0001);
    scenario(seed);
}
