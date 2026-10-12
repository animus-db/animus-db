//! ADR 0078 Tier 3: `ProdEnv` liveness of election backpressure (the per-node
//! campaign governor, the lag-stretched election timeout and the staggered
//! hosting) on real threads.
//!
//! `SimEnv` proves the ordering and the bound (`tests/it/
//! election_backpressure_corpus.rs`); it cannot prove that a real runtime's
//! timers and the `LoadMonitor`'s probe task behave the way the mechanisms
//! assume. This test does, at the per-push size:
//!
//! - **Per-push** ([`election_backpressure_liveness_g300`]): three `ProdEnv`
//!   nodes in this process, a runtime with **two** worker threads (so
//!   scheduling lateness is real), RF 3, `G = 300` tablet groups hosted *all at
//!   once* through each node's real `host::Reconciler` in the production
//!   configuration (heartbeat batching, quiescence, the governor at `K = 32`,
//!   the load monitor at `S_max = 4x`, the hosting stagger at `H = 100`).
//!   Converged-or-timeout: every group led within [`ALL_LED_BOUND`], no term
//!   above [`MAX_TERM_CEILING`], the governor's in-flight high-water mark at
//!   most `K` (nothing here takes an exempt permit). Then, once every group
//!   has quiesced, one node is killed (its groups halted, its reconciler
//!   stopped) and marked `Down` the way the control plane's failure detector
//!   would; every group it led must be re-led within the ADR's lost-leader
//!   bound (section 1, asserted literally with the worst-case `K_eff` and
//!   `base_eff`).
//! - **Ignored, manual** ([`density_repro_g3000_holds_led_count`]): the
//!   issue #1199 repro shape (`group_density_cost`, `ANIMUS_DENSITY_RF=3
//!   ANIMUS_DENSITY_GROUPS=3000 ANIMUS_DENSITY_BATCH=0`): 3000 RF3 groups on
//!   three nodes, hosted at once with the mechanisms at their defaults, must
//!   reach "all led" and hold the led count for a 120 s window. Run:
//!
//! ```text
//! cargo test --release -p animus-cp-data --features prod-heavy \
//!   --test election_backpressure_liveness -- --ignored --nocapture
//! ```
//!
//! `ANIMUS_BP_LIVE_GROUPS=N` overrides the ignored test's group count;
//! `ANIMUS_BP_LIVE_QUIESCE=0` disables quiescence (all groups stay awake). Output
//! lines are `BP-LIVE ...` (machine-readable).

// ADR 0003 / ADR 0061 Decision 4: a real-thread ProdEnv liveness test; the
// whole point is real time and real threads, which SimEnv cannot observe.
#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv election-backpressure liveness test (wall-clock polling, real sockets); ADR 0061 Decision 4"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use animus_control::timing::LAN_ELECTION_BASE;
use animus_cp_data::RaftKvNode;
use animus_cp_data::campaign_governor::{
    CampaignGovernor, DEFAULT_CAMPAIGN_MAX_INFLIGHT, ElectionBackpressure,
};
use animus_cp_data::host::{
    DEFAULT_HOST_SETTLE_TIMEOUT, DEFAULT_HOST_UNSETTLED_MAX, MemoryTabletEngines, MetadataView,
    Reconciler,
};
use animus_cp_data::load_monitor::{DEFAULT_LAG_STRETCH_MAX_X, LoadMonitor};
use animus_env::{Env, NodeId, ProdEnv, nid};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, Tablet, TabletId};
use tokio::time::{sleep, timeout};

type KvNode = RaftKvNode<ProdEnv, MemoryEngine>;
type Recon = Reconciler<ProdEnv, MemoryEngine>;

const NODES: usize = 3;
/// First tablet id (any value clear of the reserved streams).
const STREAM0: u64 = 1_000;
/// `--quiesce-after`'s production default.
const QUIESCE_AFTER: Duration = Duration::from_secs(5);
/// The reconciler's fallback tick when its ramp asks for nothing sooner
/// (`animusd`'s `tablet_host_reconciler_loop` fallback).
const RECON_FALLBACK: Duration = Duration::from_millis(500);
/// The test's poll step.
const POLL: Duration = Duration::from_millis(100);
/// How long after the kill the test marks the node `Down` in the view (the
/// control plane's failure detector, `DETECT_TIMEOUT`).
const DETECT: Duration = Duration::from_millis(500);
/// Every group led within this long of hosting all of them at once (G = 300,
/// three waves of `H = 100`, debug build, two worker threads). Measured on the
/// authoring host (4 cores): 1.0 to 1.5 s alone, 2.6 to 3.4 s with three
/// copies of this test running at once, 6.5 to 7.9 s with five. The bound is
/// the liveness claim, not a performance one: a herd that never settles (the
/// #1199 shape) misses it by construction.
const ALL_LED_BOUND: Duration = Duration::from_secs(60);
/// No group's term above this after hosting and after the node loss. A cold
/// start elects at term 1 (2 after a split vote) and losing the leader adds
/// one or two: measured max 3 alone, 4 with three concurrent copies, 6 with
/// five (each a 2-thread runtime on 4 cores). The issue's runaway is tens of
/// terms per group (#1199: churn 7 to 48).
const MAX_TERM_CEILING: u64 = 10;

fn unique_tmp_dir(tag: &str) -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("animus-bp-live-{}-{tag}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// One `ProdEnv` node: its reconciler (None once killed), the groups it
/// hosts (filled by the reconciler's `on_host` hook) and its governor.
struct Node {
    recon: Option<Recon>,
    hosted: Arc<Mutex<BTreeMap<TabletId, KvNode>>>,
    governor: CampaignGovernor,
    next_tick: Instant,
}

struct Cluster {
    nodes: Vec<Node>,
    view: MetadataView,
    g: usize,
    dirs: Vec<std::path::PathBuf>,
}

impl Cluster {
    async fn start(g: usize) -> Self {
        let dirs: Vec<_> = (0..NODES)
            .map(|i| unique_tmp_dir(&format!("{g}-n{i}")))
            .collect();
        let mut envs = Vec::new();
        for (i, d) in dirs.iter().enumerate() {
            let (env, _) = ProdEnv::bind(nid(i as u64), "127.0.0.1:0".parse().unwrap(), d)
                .await
                .expect("bind");
            envs.push(env);
        }
        let book: BTreeMap<NodeId, String> = envs
            .iter()
            .map(|e| (e.node_id(), e.local_addr().to_string()))
            .collect();
        for e in &envs {
            e.set_peers(book.clone());
        }
        let replicas: Vec<NodeId> = (0..NODES as u64).map(nid).collect();
        let view = MetadataView {
            tablets: (0..g as u64)
                .map(|i| {
                    let id = TabletId(STREAM0 + i);
                    (
                        id,
                        Tablet::new_for_table(id, "t", KeyRange::whole(), replicas.clone()),
                    )
                })
                .collect(),
            ..Default::default()
        };
        let nodes = envs
            .into_iter()
            .map(|env| {
                let hosted: Arc<Mutex<BTreeMap<TabletId, KvNode>>> = Arc::default();
                let on_host = Arc::clone(&hosted);
                let on_down = Arc::clone(&hosted);
                // The production node assembly (`animusd`'s
                // `node_election_backpressure` + the reconciler setters).
                let governor = CampaignGovernor::new(DEFAULT_CAMPAIGN_MAX_INFLIGHT, env.metrics());
                let monitor = LoadMonitor::new(
                    &env,
                    DEFAULT_LAG_STRETCH_MAX_X,
                    LAN_ELECTION_BASE,
                    env.metrics(),
                    Some(governor.clone()),
                );
                let id = env.node_id();
                let mut recon: Recon = Reconciler::new(
                    env,
                    MemoryTabletEngines::new(),
                    id,
                    move |t, n: &KvNode| {
                        on_host.lock().expect("hosted").insert(t, n.clone());
                    },
                    move |t| {
                        on_down.lock().expect("hosted").remove(&t);
                    },
                );
                recon.enable_heartbeat_batching();
                // `ANIMUS_BP_LIVE_QUIESCE=0` keeps every group awake (density
                // measurement, #1295); the default is production's.
                if std::env::var("ANIMUS_BP_LIVE_QUIESCE").as_deref() != Ok("0") {
                    recon.enable_quiescence(QUIESCE_AFTER);
                }
                recon.set_election_backpressure(ElectionBackpressure {
                    governor: Some(governor.clone()),
                    monitor: Some(monitor),
                });
                recon.set_host_stagger(DEFAULT_HOST_UNSETTLED_MAX, DEFAULT_HOST_SETTLE_TIMEOUT);
                Node {
                    recon: Some(recon),
                    hosted,
                    governor,
                    next_tick: Instant::now(),
                }
            })
            .collect();
        Cluster {
            nodes,
            view,
            g,
            dirs,
        }
    }

    /// Tick every live reconciler that is due (its ramp's short wake, else
    /// the 500 ms fallback), as `animusd`'s reconciler loop would.
    async fn reconcile(&mut self) {
        let now = Instant::now();
        for n in &mut self.nodes {
            let Some(r) = n.recon.as_mut() else { continue };
            if now < n.next_tick {
                continue;
            }
            r.tick(&self.view).await;
            n.next_tick = Instant::now() + r.next_wake().unwrap_or(RECON_FALLBACK);
        }
    }

    /// Poll step: reconcile, then sleep.
    async fn step(&mut self) {
        self.reconcile().await;
        sleep(POLL).await;
    }

    fn live(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.recon.is_some())
    }

    /// `(groups some live replica believes it leads, highest term on a live
    /// replica, quiesced live replicas, hosted live replicas)`.
    fn census(&self) -> (usize, u64, usize, usize) {
        let mut led = BTreeSet::new();
        let (mut max_term, mut quiesced, mut hosted) = (0, 0, 0);
        for n in self.live() {
            for (t, g) in n.hosted.lock().expect("hosted").iter() {
                hosted += 1;
                if g.is_leader() {
                    led.insert(*t);
                }
                if g.is_quiesced() {
                    quiesced += 1;
                }
                max_term = max_term.max(g.term());
            }
        }
        (led.len(), max_term, quiesced, hosted)
    }

    /// The groups node `i` leads.
    fn led_by(&self, i: usize) -> BTreeSet<TabletId> {
        self.nodes[i]
            .hosted
            .lock()
            .expect("hosted")
            .iter()
            .filter(|(_, g)| g.is_leader())
            .map(|(t, _)| *t)
            .collect()
    }

    /// Whether a live replica leads `t`.
    fn led_live(&self, t: TabletId) -> bool {
        self.live().any(|n| {
            n.hosted
                .lock()
                .expect("hosted")
                .get(&t)
                .is_some_and(KvNode::is_leader)
        })
    }

    /// Kill node `i`: stop its reconciler and halt every group it hosts.
    async fn kill(&mut self, i: usize) {
        self.nodes[i].recon = None;
        let groups: Vec<KvNode> = self.nodes[i]
            .hosted
            .lock()
            .expect("hosted")
            .values()
            .cloned()
            .collect();
        for g in &groups {
            g.shutdown();
        }
        let dl = Instant::now() + Duration::from_secs(30);
        while !groups.iter().all(KvNode::is_stopped) {
            assert!(Instant::now() < dl, "killed node's groups never stopped");
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Converged-or-timeout: every group led on a live replica within
    /// `bound`, sampling the census every poll step. Returns the time taken.
    async fn await_all_led(&mut self, what: &str, bound: Duration) -> Duration {
        let t0 = Instant::now();
        let mut last_log = Instant::now();
        loop {
            let (led, max_term, quiesced, hosted) = self.census();
            if led == self.g {
                return t0.elapsed();
            }
            if last_log.elapsed() >= Duration::from_secs(5) {
                println!(
                    "BP-LIVE g={} phase={what} t_ms={} led={led} hosted={hosted} quiesced={quiesced} max_term={max_term}",
                    self.g,
                    t0.elapsed().as_millis()
                );
                last_log = Instant::now();
            }
            assert!(
                t0.elapsed() < bound,
                "{what}: only {led}/{} groups led after {bound:?} (max_term={max_term})",
                self.g
            );
            self.step().await;
        }
    }

    fn governor_stats(&self) -> (usize, usize, u64) {
        self.nodes.iter().fold((0, 0, 0), |(h, n, d), node| {
            let s = node.governor.stats();
            (
                h.max(s.inflight_hwm),
                n.max(s.nonexempt_hwm),
                d + s.deferred,
            )
        })
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &self.nodes {
            for g in n.hosted.lock().expect("hosted").values() {
                g.shutdown();
            }
        }
        for d in &self.dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}

/// ADR 0078 section 1's lost-leader bound for `q` groups queued on one node,
/// at the worst case the mechanisms allow: `K_eff = K/4` (the load monitor's
/// floor) and `base_eff = base + S_max * base` (the stretch's cap), so
/// `T_hold_max = 2 * base_eff`. Plus the harness's own detection path: the
/// failure detector (`DETECT`), one reconciler fallback tick to wake the
/// quiesced followers, and one poll step.
fn lost_leader_bound(q: usize) -> Duration {
    let k_eff = (DEFAULT_CAMPAIGN_MAX_INFLIGHT / 4).max(1);
    let base_eff = LAN_ELECTION_BASE * (1 + DEFAULT_LAG_STRETCH_MAX_X as u32);
    let t_hold_max = 2 * base_eff;
    let holds = q.div_ceil(k_eff) as u32;
    2 * base_eff + holds * t_hold_max + DETECT + RECON_FALLBACK + POLL
}

/// Per-push Tier 3: see the module doc.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn election_backpressure_liveness_g300() {
    timeout(Duration::from_secs(240), async {
        let g = 300;
        let mut c = Cluster::start(g).await;

        // Host all at once (every tablet is in the first view) and converge.
        let took = c.await_all_led("host", ALL_LED_BOUND).await;
        let (_, max_term, _, _) = c.census();
        let (hwm, nonexempt_hwm, deferred) = c.governor_stats();
        println!(
            "BP-LIVE g={g} phase=all_led all_led_ms={} max_term={max_term} inflight_hwm={hwm} \
             nonexempt_hwm={nonexempt_hwm} deferred={deferred}",
            took.as_millis()
        );
        assert!(
            max_term <= MAX_TERM_CEILING,
            "max_term {max_term} above {MAX_TERM_CEILING} after hosting"
        );
        assert!(
            hwm <= DEFAULT_CAMPAIGN_MAX_INFLIGHT,
            "campaign inflight hwm {hwm} above K={DEFAULT_CAMPAIGN_MAX_INFLIGHT} (no exempt \
             campaign exists here)"
        );

        // Let every replica quiesce (the issue's idle-cluster shape), then
        // kill the node leading the most groups.
        let t0 = Instant::now();
        loop {
            let (led, _, quiesced, hosted) = c.census();
            assert_eq!(led, g, "a group lost its leader while idle");
            if quiesced == hosted && hosted == g * NODES {
                break;
            }
            assert!(
                t0.elapsed() < QUIESCE_AFTER + Duration::from_secs(30),
                "groups never all quiesced ({quiesced}/{hosted})"
            );
            c.step().await;
        }
        let victim = (0..NODES)
            .max_by_key(|&i| c.led_by(i).len())
            .expect("a node");
        let lost = c.led_by(victim);
        let q = lost.len();
        let bound = lost_leader_bound(q);
        let killed_at = Instant::now();
        c.kill(victim).await;
        // The failure detector marks it Down; the next reconciler tick wakes
        // every quiesced group that has it as a replica (ADR 0048 fork H).
        while killed_at.elapsed() < DETECT {
            c.step().await;
        }
        c.view.down.insert(nid(victim as u64));
        for n in &mut c.nodes {
            n.next_tick = Instant::now();
        }
        let mut pending = lost.clone();
        let mut worst = Duration::ZERO;
        while !pending.is_empty() {
            let t = killed_at.elapsed();
            pending.retain(|&tab| !c.led_live(tab));
            worst = t;
            assert!(
                t < bound,
                "{} of {q} groups the killed node led not re-led within the lost-leader \
                 bound {bound:?}",
                pending.len()
            );
            if pending.is_empty() {
                break;
            }
            c.step().await;
        }
        let (led, max_term, _, _) = c.census();
        let (hwm, nonexempt_hwm, deferred) = c.governor_stats();
        println!(
            "BP-LIVE g={g} phase=node_loss victim=n{victim} lost={q} relead_ms={} bound_ms={} \
             led={led} max_term={max_term} inflight_hwm={hwm} nonexempt_hwm={nonexempt_hwm} \
             deferred={deferred}",
            worst.as_millis(),
            bound.as_millis()
        );
        assert_eq!(led, g, "every group led after the node loss");
        assert!(
            max_term <= MAX_TERM_CEILING,
            "max_term {max_term} above {MAX_TERM_CEILING} after the node loss"
        );
        assert!(
            hwm <= DEFAULT_CAMPAIGN_MAX_INFLIGHT,
            "campaign inflight hwm {hwm} above K={DEFAULT_CAMPAIGN_MAX_INFLIGHT}"
        );
    })
    .await
    .expect("election_backpressure_liveness_g300 timed out");
}

/// The issue #1199 acceptance (ADR 0078 Tier 3, ignored): 3000 RF3 groups on
/// three nodes hosted at once reach "all led" and hold the led count for
/// 120 s. Release build recommended (see the module doc).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual acceptance: 3000 groups, several minutes; run with --ignored --nocapture in release"]
async fn density_repro_g3000_holds_led_count() {
    let g: usize = std::env::var("ANIMUS_BP_LIVE_GROUPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);
    let hold = Duration::from_secs(120);
    timeout(Duration::from_secs(1500), async {
        let mut c = Cluster::start(g).await;
        let took = c.await_all_led("host", Duration::from_secs(600)).await;
        let (_, max_term, _, _) = c.census();
        let (hwm, _, deferred) = c.governor_stats();
        println!(
            "BP-LIVE g={g} phase=all_led all_led_ms={} max_term={max_term} inflight_hwm={hwm} deferred={deferred}",
            took.as_millis()
        );
        // Hold: the led count must not fall for the whole window (the shape
        // that decays without the mechanisms).
        let t0 = Instant::now();
        let mut last_log = Instant::now();
        let mut min_led = g;
        while t0.elapsed() < hold {
            let (led, max_term, quiesced, hosted) = c.census();
            min_led = min_led.min(led);
            if last_log.elapsed() >= Duration::from_secs(10) {
                println!(
                    "BP-LIVE g={g} phase=hold t_s={} led={led} quiesced={quiesced} hosted={hosted} max_term={max_term}",
                    t0.elapsed().as_secs()
                );
                last_log = Instant::now();
            }
            c.step().await;
        }
        let (led, max_term, _, _) = c.census();
        let (hwm, _, deferred) = c.governor_stats();
        println!(
            "BP-LIVE g={g} phase=held min_led={min_led} led={led} max_term={max_term} inflight_hwm={hwm} deferred={deferred}"
        );
        assert_eq!(min_led, g, "the led count fell during the {hold:?} window");
        assert!(hwm <= DEFAULT_CAMPAIGN_MAX_INFLIGHT, "inflight hwm {hwm} above K");
    })
    .await
    .expect("density_repro_g3000_holds_led_count timed out");
}
