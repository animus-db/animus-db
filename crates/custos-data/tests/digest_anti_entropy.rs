//! Range/Merkle-digest anti-entropy (ADR 0010).
//!
//! The original anti-entropy *full-pushed* every entry each round — provably
//! convergent but `O(data)` even when replicas already agree. These tests pin
//! the refinement: replicas first exchange a compact per-segment digest and then
//! transfer **only the divergent segments' entries**. Two converged replicas move
//! no entry data at all; a pair differing in one key moves only that key's
//! segment, not the whole dataset.
//!
//! We assert this at the wire level using the simulator's `Send` trace, which
//! records the byte length of every message: the entries that actually cross the
//! network are a small fraction of a full digest.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_data::{DataClient, ReplicaHandle, TabletView, serve_anti_entropy, serve_replica};
use custos_env::EnvExt;
use custos_sim::{SimEnv, Simulator, TraceEvent};
use custos_storage::{MemoryEngine, StorageEngine};
use custos_tablet::{Epoch, TabletId};

const REPLICAS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 10;
const TABLET: TabletId = TabletId(1);
const TIMEOUT: Duration = Duration::from_secs(2);

fn view() -> TabletView {
    TabletView {
        tablet: TABLET,
        replicas: REPLICAS.to_vec(),
        epoch: Epoch::INITIAL,
        r: 2,
        w: 2,
    }
}

fn run_op<T: Clone + Send + 'static>(
    sim: &mut Simulator,
    op: impl FnOnce(DataClient<SimEnv>) -> futures::future::BoxFuture<'static, T> + Send + 'static,
) -> T {
    let result: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let client_env = sim.env(CLIENT);
    let out = Arc::clone(&result);
    client_env.clone().spawn_task(async move {
        let client = DataClient::new(client_env);
        *out.lock().unwrap() = Some(op(client).await);
    });
    sim.run();
    result.lock().unwrap().clone().expect("op did not complete")
}

fn value_at(handle: &ReplicaHandle<MemoryEngine>, key: &[u8]) -> Option<Vec<u8>> {
    futures::executor::block_on(handle.storage().get(key))
        .unwrap()
        .map(|vv| vv.value)
}

fn start_anti_entropy(sim: &Simulator, handles: &[ReplicaHandle<MemoryEngine>]) {
    for &id in &REPLICAS {
        serve_anti_entropy(
            sim.env(id),
            handles[id as usize].storage().clone(),
            TABLET,
            Epoch::INITIAL,
            REPLICAS.to_vec(),
            Duration::from_millis(50),
        );
    }
}

/// Total bytes of repair `Send`s between the replicas after `since` (a trace
/// length), used to compare digest exchange against a hypothetical full push.
fn repair_send_bytes(trace: &[TraceEvent], since: usize) -> usize {
    trace[since..]
        .iter()
        .filter_map(|e| match e {
            TraceEvent::Send { from, to, len, .. }
                if REPLICAS.contains(from) && REPLICAS.contains(to) =>
            {
                Some(*len)
            }
            _ => None,
        })
        .sum()
}

#[test]
fn digest_anti_entropy_converges_one_divergent_key_among_many() {
    let seed = 0x0D16_2026;
    let sim = Simulator::new(seed);
    let handles: Vec<ReplicaHandle<MemoryEngine>> = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL))
        .collect();
    let mut sim = sim;

    // Land a sizeable shared dataset on all three replicas (full W=3 quorum).
    const N: usize = 200;
    for i in 0..N {
        let key = format!("key-{i:04}");
        let val = format!("value-for-{i:04}");
        let v = view();
        let (k, val) = (key.clone(), val.clone());
        let ok = run_op(&mut sim, move |c| {
            Box::pin(async move {
                c.write(&v, k.as_bytes(), val.as_bytes(), (i as u64) + 1, TIMEOUT)
                    .await
            })
        });
        assert!(ok, "seed-write {i} reached quorum (seed={seed})");
    }
    sim.run_for(Duration::from_millis(200)); // let W=3 reach all replicas

    // Replica 2 is isolated while a single NEW write lands on {0,1}. Now the only
    // divergence between replica 2 and the others is exactly one key.
    sim.partition_pair(2, 0);
    sim.partition_pair(2, 1);
    sim.partition_pair(2, CLIENT);
    let v = view();
    let ok = run_op(&mut sim, move |c| {
        Box::pin(async move {
            c.write(&v, b"divergent", b"only-new-value", (N as u64) + 1, TIMEOUT)
                .await
        })
    });
    assert!(ok, "divergent write reached {{0,1}} (seed={seed})");
    assert_eq!(value_at(&handles[2], b"divergent"), None, "(seed={seed})");

    // Heal and run digest anti-entropy. Measure only the repair traffic from here.
    sim.heal(2, 0);
    sim.heal(2, 1);
    sim.heal(2, CLIENT);

    // What the *old* full-push scheme moves in one round: every replica pushes
    // its whole `entries_with_tombstones()` digest to each of its two peers.
    let full_push_one_round: usize = handles
        .iter()
        .map(|h| {
            let entries =
                futures::executor::block_on(h.storage().entries_with_tombstones()).unwrap();
            // payload size × (peers it pushes to)
            serde_json::to_vec(&custos_data::DataMsg::Sync {
                tablet: TABLET,
                epoch: Epoch::INITIAL,
                entries,
            })
            .unwrap()
            .len()
                * (REPLICAS.len() - 1)
        })
        .sum();

    let mark = sim.trace().len();
    start_anti_entropy(&sim, &handles);
    // Two rounds is enough for the one divergent key to flow; stop promptly so we
    // measure the convergence cost, not endless idle digest rounds.
    sim.run_for(Duration::from_millis(120));

    // Convergence: replica 2 now holds the one missing key.
    assert_eq!(
        value_at(&handles[2], b"divergent"),
        Some(b"only-new-value".to_vec()),
        "digest anti-entropy did not converge the one divergent key (seed={seed})"
    );

    // Frugality: converging a single divergent key out of 200 moves **less than
    // one full-push round would** — the divergent segment's handful of entries
    // plus the small per-segment digests, not the entire dataset (ADR 0010).
    let bytes = repair_send_bytes(&sim.trace(), mark);
    assert!(
        bytes < full_push_one_round,
        "digest exchange moved {bytes} B converging one key, not below a single \
         full-push round of {full_push_one_round} B — optimization ineffective (seed={seed})"
    );
}

#[test]
fn converged_replicas_transfer_no_entry_data() {
    // When every replica already agrees, a round of digest anti-entropy exchanges
    // only the tiny per-segment digests and provokes zero `Sync` (entry) traffic:
    // each peer compares digests, finds no divergence, and asks for nothing.
    let seed = 0x0D16_2027;
    let sim = Simulator::new(seed);
    let handles: Vec<ReplicaHandle<MemoryEngine>> = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL))
        .collect();
    let mut sim = sim;

    for i in 0..20u64 {
        let key = format!("k{i:02}");
        let v = view();
        let ok = run_op(&mut sim, move |c| {
            Box::pin(async move { c.write(&v, key.as_bytes(), b"v", i + 1, TIMEOUT).await })
        });
        assert!(ok, "(seed={seed})");
    }
    sim.run_for(Duration::from_millis(200)); // converge all three via W=3

    // One digest-exchange round between an ordered replica pair sends, at most,
    // each side's `SyncDigest` (no `SyncPull`, no `Sync`, since nothing differs).
    // Bound the whole converged run by a few such rounds: if entry-carrying
    // `Sync` traffic were generated it would blow past this bound.
    let one_digest = {
        let segs = custos_data::digest::digest(
            &futures::executor::block_on(handles[0].storage().entries_with_tombstones()).unwrap(),
        );
        serde_json::to_vec(&custos_data::DataMsg::SyncDigest {
            tablet: TABLET,
            epoch: Epoch::INITIAL,
            from: 0,
            segments: segs,
        })
        .unwrap()
        .len()
    };

    let mark = sim.trace().len();
    start_anti_entropy(&sim, &handles);
    sim.run_for(Duration::from_secs(1));

    // 1s / 50ms ≈ 20 rounds, each replica → 2 peers ⇒ ≤ ~120 digest sends. Allow
    // generous head-room; the point is the bound is in *digests*, not *entries*.
    let bytes = repair_send_bytes(&sim.trace(), mark);
    let digest_only_ceiling = one_digest * 200;
    assert!(
        bytes <= digest_only_ceiling,
        "converged replicas moved {bytes} B (> {digest_only_ceiling} B of digests) — \
         entry-carrying Sync traffic leaked into a converged round (seed={seed})"
    );

    // And a sanity floor: convergence with no divergence still left every replica
    // holding the same data it started with.
    for id in REPLICAS {
        assert_eq!(value_at(&handles[id as usize], b"k00"), Some(b"v".to_vec()));
    }
}
