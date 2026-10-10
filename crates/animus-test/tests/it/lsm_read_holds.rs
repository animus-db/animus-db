//! Reader-side MVCC retention over `LsmEngine<SimEnv>` under compaction
//! pressure (issue #1206; ADR 0008's 2026-10-10 amendment, ADR 0018/0059).
//!
//! `LsmEngine` compaction drops history below `max_version -
//! tombstone_grace_versions`. Two kinds of reader need history older than a
//! fraction of a millisecond: a snapshot read (`RaftKvNode::read_at`, what a
//! `TransactGetItems` runs at its transaction timestamp) and the backup capture
//! driver, which replays one pinned `cut_version` across many ticks. The fix has
//! two halves, and each gets a positive cell and a negative control here:
//!
//! 1. **Time grace** (HLC wall units, 5 s by default): a `read_at` at a
//!    timestamp a few hundred ms old still returns the value as of that
//!    timestamp after heavy churn and compaction. Control: the same run with
//!    the old ~1 ms grace loses it.
//! 2. **Explicit hold** (`RaftKvNode::hold_version`, the primitive the capture
//!    driver keeps for a whole capture): a `cut_version` scan taken long after
//!    the grace has passed is byte-identical to the one taken at the cut (the
//!    identical re-put invariant of ADR 0059). Control: without the hold the
//!    rows differ.
//!
//! Every run is a pure function of its seed (`ANIMUS_LSM_HOLD_SEEDS=K` for
//! depth; `ANIMUS_SEED` is not used, the seeds are printed).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, RaftKvNode, hlc};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use animus_test::corpus;
use futures::executor::block_on;

/// `(logical key, value, version)` rows of one kind-scope scan.
type Rows = Vec<(Vec<u8>, Vec<u8>, u64)>;
type Node = RaftKvNode<SimEnv, LsmEngine<SimEnv>>;

const IDS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 100;
/// `LsmOptions` grace (versions) equal to the pre-#1206 default (~1 ms of HLC).
const OLD_GRACE: u64 = 1 << 20;

fn opts(grace: u64) -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 512,
        compaction_trigger: 2,
        target_table_bytes: 2048,
        level_fanout: 2,
        wal_segment_bytes: 1024,
        tombstone_grace_versions: grace,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

struct Cluster {
    sim: Simulator,
    nodes: Vec<Arc<Node>>,
    engines: Vec<LsmEngine<SimEnv>>,
}

impl Cluster {
    fn start(seed: u64, grace: u64) -> Cluster {
        let mut sim = Simulator::new(seed);
        let mut nodes = Vec::new();
        let mut engines = Vec::new();
        for &id in &IDS {
            let engine = block_on(LsmEngine::open_with(
                sim.env(nid(id)),
                "holds/",
                opts(grace),
            ))
            .expect("open lsm");
            engines.push(engine.clone());
            nodes.push(Arc::new(RaftKvNode::start(
                sim.env(nid(id)),
                IDS.iter().copied().map(nid).collect(),
                engine,
            )));
        }
        sim.run_for(Duration::from_millis(1500));
        Cluster {
            sim,
            nodes,
            engines,
        }
    }

    fn leader_index(&self) -> usize {
        self.nodes
            .iter()
            .position(|n| n.is_leader())
            .expect("a leader")
    }

    fn put(&self, key: &str, value: &str) {
        let leader = &self.nodes[self.leader_index()];
        assert!(matches!(
            leader.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()),
            ProposeResult::Accepted { .. }
        ));
    }

    /// Run `f` as a task on a client env and return its result.
    fn run<T: Send + 'static, F, Fut>(&mut self, f: F) -> T
    where
        F: FnOnce(Arc<Node>) -> Fut,
        Fut: std::future::Future<Output = T> + Send + 'static,
    {
        let node = Arc::clone(&self.nodes[self.leader_index()]);
        let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let out = Arc::clone(&slot);
        let fut = f(node);
        self.sim.env(nid(CLIENT)).spawn_task(async move {
            let v = fut.await;
            *out.lock().unwrap() = Some(v);
        });
        self.sim.run_for(Duration::from_millis(1500));
        slot.lock().unwrap().take().expect("task did not finish")
    }

    /// `rounds` rounds of `per_round` fresh-key puts plus an overwrite of
    /// `hot`, 100 ms of virtual time apart, so HLC wall time advances by
    /// `rounds * 100` ms and flushes/compactions run throughout.
    fn churn(&mut self, hot: &str, rounds: u64, per_round: u64) {
        for r in 0..rounds {
            for i in 0..per_round {
                self.put(&format!("churn/{r:04}/{i:03}"), &format!("payload-{r}-{i}"));
            }
            self.put(hot, &format!("hot-{r}"));
            self.sim.run_for(Duration::from_millis(100));
        }
        // Let the apply tasks drain.
        self.sim.run_for(Duration::from_millis(500));
    }

    fn compactions(&self) -> u64 {
        self.engines.iter().map(LsmEngine::compaction_count).sum()
    }
}

fn seeds(tag: &str) -> Vec<u64> {
    (0..corpus::seeds_from_env("ANIMUS_LSM_HOLD_SEEDS").max(3))
        .map(|k| corpus::name_seed(&format!("lsm_read_holds_{tag}_{k}")))
        .collect()
}

/// Write `hot = "v1"`, take its timestamp, churn for `churn_ms` of HLC time,
/// then `read_at(hot, ts)`. Returns `(read result, compactions run)`.
fn read_at_after_churn(seed: u64, grace: u64) -> (Option<Option<Vec<u8>>>, u64) {
    let mut c = Cluster::start(seed, grace);
    c.put("hot", "v1");
    c.sim.run_for(Duration::from_millis(300));
    let leader = &c.nodes[c.leader_index()];
    let version = block_on(
        leader
            .storage()
            .get(&leader.physical_key(KIND_BASE, b"hot")),
    )
    .expect("engine read ok")
    .expect("hot written")
    .version;
    let ts = hlc::unpack(version);
    // 20 rounds x 100 ms = 2 s of HLC wall time: past the old ~1 ms grace,
    // inside the 5 s default.
    c.churn("hot", 20, 6);
    let got = c.run(move |n| async move {
        // Drive the committed read ceiling past `ts`, as a real caller does.
        let _ = n.linearizable_get(b"hot").await;
        n.read_at(b"hot", ts).await
    });
    (got, c.compactions())
}

#[test]
fn read_at_an_old_timestamp_survives_compaction_with_the_time_grace() {
    for seed in seeds("grace") {
        let (got, compactions) =
            read_at_after_churn(seed, LsmOptions::default().tombstone_grace_versions);
        assert!(compactions >= 1, "seed={seed}: no compaction ran: vacuous");
        assert_eq!(
            got,
            Some(Some(b"v1".to_vec())),
            "seed={seed}: read_at at the old ts must return the value as of ts"
        );
    }
}

/// Negative control: the pre-#1206 ~1 ms grace loses the version the read needs.
#[test]
fn read_at_an_old_timestamp_is_lost_under_the_old_millisecond_grace() {
    let mut lost = 0;
    for seed in seeds("grace_ctl") {
        let (got, compactions) = read_at_after_churn(seed, OLD_GRACE);
        assert!(compactions >= 1, "seed={seed}: no compaction ran: vacuous");
        assert!(
            got.is_some(),
            "seed={seed}: read_at refused (not a GC effect)"
        );
        if got != Some(Some(b"v1".to_vec())) {
            lost += 1;
        }
    }
    assert!(
        lost > 0,
        "control: the old grace never lost the version, the cell has no teeth"
    );
}

/// The capture shape: pin `cut_version`, scan it, churn far past even the 5 s
/// grace, scan again at the same cut. Returns `(before, after)`.
fn capture_rescan(seed: u64, hold: bool) -> (Rows, Rows, u64) {
    let mut c = Cluster::start(seed, LsmOptions::default().tombstone_grace_versions);
    for i in 0..30 {
        c.put(&format!("row/{i:03}"), &format!("orig-{i}"));
    }
    c.sim.run_for(Duration::from_millis(300));
    let li = c.leader_index();
    let cut = c.nodes[li].engine_latest_version();
    // The capture driver's hold: taken once at the cut, kept across ticks.
    let _hold = hold.then(|| c.nodes[li].hold_version(cut));
    let scan = move |n: Arc<Node>| async move {
        n.local_scan_kind_snapshot(KIND_BASE, b"", cut, 1000)
            .await
            .0
    };
    let before = c.run(scan);
    // 70 rounds x 100 ms = 7 s of HLC wall time: past the 5 s grace. Overwrite
    // every captured row so the cut's versions are exactly what GC would drop.
    for r in 0..70u64 {
        for i in 0..30 {
            c.put(&format!("row/{i:03}"), &format!("later-{r}-{i}"));
        }
        c.sim.run_for(Duration::from_millis(100));
    }
    c.sim.run_for(Duration::from_millis(500));
    let after = c.run(scan);
    (before, after, c.compactions())
}

#[test]
fn held_cut_version_rescan_is_identical_across_compaction() {
    for seed in seeds("capture") {
        let (before, after, compactions) = capture_rescan(seed, true);
        assert!(compactions >= 1, "seed={seed}: no compaction ran: vacuous");
        assert_eq!(before.len(), 30, "seed={seed}: the cut sees all 30 rows");
        assert_eq!(
            before, after,
            "seed={seed}: a held cut must re-derive byte-identical rows (ADR 0059)"
        );
    }
}

/// Negative control: the same capture without the hold diverges.
#[test]
fn unheld_cut_version_rescan_diverges_after_the_grace() {
    let mut diverged = 0;
    for seed in seeds("capture_ctl") {
        let (before, after, compactions) = capture_rescan(seed, false);
        assert!(compactions >= 1, "seed={seed}: no compaction ran: vacuous");
        if before != after {
            diverged += 1;
        }
    }
    assert!(
        diverged > 0,
        "control: the unheld cut never diverged, the cell has no teeth"
    );
}
