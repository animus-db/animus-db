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
    DEFAULT_HOST_SETTLE_TIMEOUT, DEFAULT_HOST_UNSETTLED_MAX, EngineFactory, MemoryTabletEngines,
    MetadataView, Reconciler,
};
use animus_cp_data::load_monitor::{DEFAULT_LAG_STRETCH_MAX_X, LoadMonitor};
use animus_env::{Env, NodeId, ProdEnv, nid};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use animus_tablet::{KeyRange, Tablet, TabletId};
use tokio::time::{sleep, timeout};

type KvNode<S> = RaftKvNode<ProdEnv, S>;

/// Max tokio oversleep (microseconds) of a 10 ms sleeper since the last
/// `diag` (issue #1297: how late does a task wake on this runtime?).
static SCHED_LAG_MAX_US: AtomicU64 = AtomicU64::new(0);
static SCHED_LAG_SUM_US: AtomicU64 = AtomicU64::new(0);
static SCHED_LAG_N: AtomicU64 = AtomicU64::new(0);

fn spawn_sched_probe() {
    tokio::spawn(async {
        loop {
            let t = Instant::now();
            sleep(Duration::from_millis(10)).await;
            let over = t.elapsed().saturating_sub(Duration::from_millis(10));
            let us = over.as_micros() as u64;
            SCHED_LAG_MAX_US.fetch_max(us, Ordering::Relaxed);
            SCHED_LAG_SUM_US.fetch_add(us, Ordering::Relaxed);
            SCHED_LAG_N.fetch_add(1, Ordering::Relaxed);
        }
    });
}
type Recon<S> = Reconciler<ProdEnv, S>;

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
struct Node<S: StorageEngine + 'static> {
    recon: Option<Recon<S>>,
    hosted: Arc<Mutex<BTreeMap<TabletId, KvNode<S>>>>,
    governor: CampaignGovernor,
    next_tick: Instant,
}

struct Cluster<S: StorageEngine + 'static> {
    nodes: Vec<Node<S>>,
    view: MetadataView,
    g: usize,
    dirs: Vec<std::path::PathBuf>,
}

impl<S: StorageEngine + 'static> Cluster<S> {
    async fn start<F: EngineFactory<S> + 'static>(
        g: usize,
        make_factory: impl Fn(&ProdEnv) -> F,
    ) -> Self {
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
                let hosted: Arc<Mutex<BTreeMap<TabletId, KvNode<S>>>> = Arc::default();
                let on_host = Arc::clone(&hosted);
                let on_down = Arc::clone(&hosted);
                // The production node assembly (`animusd`'s
                // `node_election_backpressure` + the reconciler setters).
                let node_metrics = env.metrics();
                let governor = CampaignGovernor::new(DEFAULT_CAMPAIGN_MAX_INFLIGHT, env.metrics());
                let monitor = LoadMonitor::new(
                    &env,
                    DEFAULT_LAG_STRETCH_MAX_X,
                    LAN_ELECTION_BASE,
                    env.metrics(),
                    Some(governor.clone()),
                );
                let id = env.node_id();
                let factory = make_factory(&env);
                let mut recon: Recon<S> = Reconciler::new(
                    env,
                    factory,
                    id,
                    move |t, n: &KvNode<S>| {
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
                // `ANIMUS_BP_LIVE_AWAKE_MAX=A` turns ADR 0079's awake-group
                // admission on at cap `A` per node (default off, as a node
                // without `--cores`).
                let awake = std::env::var("ANIMUS_BP_LIVE_AWAKE_MAX")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .filter(|a| *a > 0)
                    .map(|a| {
                        let adm = animus_cp_data::awake_admission::AwakeAdmission::new(
                            a,
                            node_metrics.clone(),
                        );
                        monitor.attach_awake(adm.clone());
                        adm
                    });
                recon.set_election_backpressure(ElectionBackpressure {
                    governor: Some(governor.clone()),
                    monitor: Some(monitor),
                    awake,
                    budget: None,
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

    fn live(&self) -> impl Iterator<Item = &Node<S>> {
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

    /// Hosted tablets no live replica currently believes it leads. Read per
    /// node, not atomically: a leadership transfer racing the scan can read
    /// as a one-poll blip either way, which the callers' persistence bound
    /// absorbs (they only act on a tablet that stays leaderless).
    fn leaderless(&self) -> BTreeSet<TabletId> {
        let (mut all, mut led) = (BTreeSet::new(), BTreeSet::new());
        for n in self.live() {
            for (t, g) in n.hosted.lock().expect("hosted").iter() {
                all.insert(*t);
                if g.is_leader() {
                    led.insert(*t);
                }
            }
        }
        all.difference(&led).copied().collect()
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
                .is_some_and(KvNode::<S>::is_leader)
        })
    }

    /// Kill node `i`: stop its reconciler and halt every group it hosts.
    async fn kill(&mut self, i: usize) {
        self.nodes[i].recon = None;
        let groups: Vec<KvNode<S>> = self.nodes[i]
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
        while !groups.iter().all(KvNode::<S>::is_stopped) {
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
                self.diag(what);
            }
            assert!(
                t0.elapsed() < bound,
                "{what}: only {led}/{} groups led after {bound:?} (max_term={max_term})",
                self.g
            );
            self.step().await;
        }
    }

    /// [`await_all_led`](Self::await_all_led) without the assertion: `None`
    /// when `bound` elapses first (density sweeps record non-convergence).
    async fn await_all_led_soft(&mut self, what: &str, bound: Duration) -> Option<Duration> {
        let t0 = Instant::now();
        let mut last_log = Instant::now();
        loop {
            let (led, max_term, quiesced, hosted) = self.census();
            if led == self.g {
                return Some(t0.elapsed());
            }
            if last_log.elapsed() >= Duration::from_secs(10) {
                println!(
                    "BP-LIVE g={} phase={what} t_ms={} led={led} hosted={hosted} quiesced={quiesced} max_term={max_term}",
                    self.g,
                    t0.elapsed().as_millis()
                );
                last_log = Instant::now();
            }
            if t0.elapsed() >= bound {
                return None;
            }
            self.step().await;
        }
    }

    /// One BP-LIVE line explaining what the leaderless groups wait on (#1297):
    /// per node the governor level, and over the leaderless groups the roles,
    /// deferred holds, and how many replicas of each are hosted.
    fn diag(&self, tag: &str) {
        use animus_control::Role;
        let mut by_group: BTreeMap<TabletId, Vec<(usize, Role, bool, u64)>> = BTreeMap::new();
        let mut led = BTreeSet::new();
        for (i, n) in self.nodes.iter().enumerate() {
            if n.recon.is_none() {
                continue;
            }
            for (t, g) in n.hosted.lock().expect("hosted").iter() {
                if g.is_leader() {
                    led.insert(*t);
                }
                by_group.entry(*t).or_default().push((
                    i,
                    g.role(),
                    g.campaign_deferred(),
                    g.term(),
                ));
            }
        }
        let (mut fol, mut pre, mut cand, mut held) = (0, 0, 0, 0);
        let mut replicas_hist = [0usize; 4];
        let mut terms: BTreeMap<u64, usize> = BTreeMap::new();
        let mut leaderless = 0;
        for (t, reps) in &by_group {
            if led.contains(t) {
                continue;
            }
            leaderless += 1;
            replicas_hist[reps.len().min(3)] += 1;
            for (_, role, deferred, term) in reps {
                match role {
                    Role::Follower => fol += 1,
                    Role::PreCandidate => pre += 1,
                    Role::Candidate => cand += 1,
                    Role::Leader => {}
                }
                held += usize::from(*deferred);
                *terms.entry(*term).or_default() += 1;
            }
        }
        let mut st = animus_control::CampaignStats::default();
        let mut stl = animus_control::CampaignStats::default();
        for n in &self.nodes {
            for (t, g) in n.hosted.lock().expect("hosted").iter() {
                let x = g.campaign_stats();
                let tgt = if led.contains(t) { &mut stl } else { &mut st };
                tgt.rounds_started += x.rounds_started;
                tgt.rounds_restarted += x.rounds_restarted;
                tgt.grants_rx += x.grants_rx;
                tgt.grants_rx_dropped_hold += x.grants_rx_dropped_hold;
                tgt.grants_rx_dropped_round += x.grants_rx_dropped_round;
                tgt.refusals_rx += x.refusals_rx;
                tgt.pv_granted += x.pv_granted;
                tgt.pv_refused_leader += x.pv_refused_leader;
                tgt.pv_refused_lease += x.pv_refused_lease;
                tgt.pv_refused_term_or_log += x.pv_refused_term_or_log;
                tgt.elections_started += x.elections_started;
                tgt.dropped_age_ns_sum += x.dropped_age_ns_sum;
                tgt.dropped_age_ns_max = tgt.dropped_age_ns_max.max(x.dropped_age_ns_max);
                tgt.counted_age_ns_sum += x.counted_age_ns_sum;
                tgt.dropped_window_ns_sum += x.dropped_window_ns_sum;
            }
        }
        let mut stretch_max = Duration::ZERO;
        for n in &self.nodes {
            for (_, g) in n.hosted.lock().expect("hosted").iter() {
                stretch_max = stretch_max.max(g.lag_stretch());
            }
        }
        let (mx, sm, cnt) = (
            SCHED_LAG_MAX_US.swap(0, Ordering::Relaxed),
            SCHED_LAG_SUM_US.swap(0, Ordering::Relaxed),
            SCHED_LAG_N.swap(0, Ordering::Relaxed),
        );
        println!(
            "BP-LAG {tag} sched_oversleep_max_ms={} mean_ms={} samples={cnt} stretch_max_ms={}",
            mx / 1000,
            sm / 1000 / cnt.max(1),
            stretch_max.as_millis()
        );
        println!(
            "BP-AGE {tag} leaderless: mean_age_of_dropped_grant_ms={} max_ms={} mean_round_window_ms={} mean_age_of_counted_ms={}",
            st.dropped_age_ns_sum / st.grants_rx_dropped_hold.max(1) / 1_000_000,
            st.dropped_age_ns_max / 1_000_000,
            st.dropped_window_ns_sum / st.grants_rx_dropped_hold.max(1) / 1_000_000,
            st.counted_age_ns_sum / st.grants_rx.max(1) / 1_000_000
        );
        println!("BP-STATS {tag} leaderless={st:?}");
        println!("BP-STATS {tag} led={stl:?}");
        let gov: Vec<String> = self
            .nodes
            .iter()
            .map(|n| {
                let s = n.governor.stats();
                format!(
                    "q={} inflight={} adm={} def={} exp={} keff={}",
                    s.queue_depth,
                    s.inflight,
                    s.admitted,
                    s.deferred,
                    s.expired_permits,
                    n.governor.k_eff()
                )
            })
            .collect();
        println!(
            "BP-DIAG {tag} leaderless={leaderless} hosted_replicas_hist(0..3)={replicas_hist:?} \
             roles(fol={fol},pre={pre},cand={cand}) deferred_holds={held} terms={terms:?} gov=[{}]",
            gov.join(" | ")
        );
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

impl<S: StorageEngine + 'static> Drop for Cluster<S> {
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
        let mut c = Cluster::start(g, |_| MemoryTabletEngines::new()).await;

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
        //
        // Leadership is an eventual property: under CPU starvation a leader
        // may legitimately step down or be replaced while the cluster idles
        // (availability, not safety; ADR 0078's #1297 amendment documents
        // late pre-vote grants under overload). So a momentary `led < g` is
        // not a failure; a tablet that stays leaderless longer than the
        // ADR's own lost-leader bound is. The bound is taken at `q = g`, the
        // conservative queue length: every group could in principle be
        // queued behind the governor on one node, and the formula is the
        // ADR's, not a new constant. The loop still exits only when all g
        // groups are led AND all quiesced.
        let idle_bound = lost_leader_bound(g);
        let mut leaderless_since: BTreeMap<TabletId, Instant> = BTreeMap::new();
        let (mut idle_leader_losses, mut worst_leaderless) = (0usize, Duration::ZERO);
        let t0 = Instant::now();
        loop {
            let (led, _, quiesced, hosted) = c.census();
            let now = Instant::now();
            let unled = c.leaderless();
            for t in &unled {
                leaderless_since.entry(*t).or_insert_with(|| {
                    idle_leader_losses += 1;
                    now
                });
            }
            for since in leaderless_since.values() {
                worst_leaderless = worst_leaderless.max(now - *since);
            }
            leaderless_since.retain(|t, _| unled.contains(t));
            if let Some((t, since)) = leaderless_since.iter().max_by_key(|(_, s)| now - **s) {
                assert!(
                    now - *since < idle_bound,
                    "tablet {t:?} stayed leaderless {:?} while idle, above the lost-leader \
                     bound {idle_bound:?} ({} leaderless now)",
                    now - *since,
                    leaderless_since.len()
                );
            }
            if led == g && quiesced == hosted && hosted == g * NODES {
                break;
            }
            assert!(
                t0.elapsed() < QUIESCE_AFTER + Duration::from_secs(30),
                "groups never all quiesced and led (led={led}/{g}, quiesced={quiesced}/{hosted})"
            );
            c.step().await;
        }
        println!(
            "BP-LIVE g={g} phase=idle idle_leader_losses={idle_leader_losses} \
             worst_leaderless_ms={}",
            worst_leaderless.as_millis()
        );
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

/// Env knob parsed as `T`, else `default`.
fn knob<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `(utime + stime ticks of this process, RSS kB, open fds, threads)` from
/// `/proc/self` (density calibration, ADR 0079 step 1). Zeros off Linux.
fn proc_sample() -> (u64, u64, usize, u64) {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // Fields after the parenthesised comm: state is field 3; utime/stime are
    // fields 14/15; num_threads is field 20.
    let rest = stat.rsplit_once(')').map_or("", |(_, r)| r);
    let f: Vec<&str> = rest.split_whitespace().collect();
    let num = |i: usize| f.get(i).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    let ticks = num(11) + num(12);
    let threads = num(17);
    let rss_kb = std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap_or(0);
    let fds = std::fs::read_dir("/proc/self/fd").map_or(0, Iterator::count);
    (ticks, rss_kb, fds, threads)
}

/// `(files, bytes, sstables)` under the nodes' data directories.
fn dir_stats(dirs: &[std::path::PathBuf]) -> (usize, u64, usize) {
    let (mut files, mut bytes, mut ssts) = (0, 0, 0);
    for d in dirs {
        for e in std::fs::read_dir(d).into_iter().flatten().flatten() {
            files += 1;
            bytes += e.metadata().map_or(0, |m| m.len());
            if e.file_name().to_string_lossy().contains("-sst-") {
                ssts += 1;
            }
        }
    }
    (files, bytes, ssts)
}

/// `LsmEngine<ProdEnv>` per-tablet factory: the shape of `animusd`'s
/// `LsmTabletFactory` (prefix `db-t<id>-`, `LsmOptions::production()`), for the
/// `ANIMUS_BP_LIVE_ENGINE=lsm` density calibration. Test-local, so the
/// product code is untouched.
struct LsmFactory {
    env: ProdEnv,
    files: animus_cp_data::tablet_files::TabletFileIndex<ProdEnv>,
}

fn lsm_prefix(t: TabletId) -> String {
    format!("db-t{}-", t.0)
}

fn lsm_parse(f: &str) -> Option<TabletId> {
    let rest = f.strip_prefix("db-t")?;
    let end = rest.find('-')?;
    rest[..end].parse::<u64>().ok().map(TabletId)
}

impl LsmFactory {
    fn new(env: ProdEnv) -> Self {
        Self {
            files: animus_cp_data::tablet_files::TabletFileIndex::new(
                env.clone(),
                lsm_prefix,
                lsm_parse,
            ),
            env,
        }
    }
}

#[async_trait::async_trait]
impl EngineFactory<LsmEngine<ProdEnv>> for LsmFactory {
    async fn open(&self, t: TabletId) -> Result<LsmEngine<ProdEnv>, String> {
        self.files.note_open(t);
        LsmEngine::open_with(self.env.clone(), &lsm_prefix(t), LsmOptions::production())
            .await
            .map_err(|e| e.to_string())
    }
    async fn probe(&self, t: TabletId) -> bool {
        self.files.probe(t).await
    }
    async fn destroy(&self, t: TabletId) {
        let _ = self.files.destroy(t).await;
    }
    async fn flush_engine(&self, engine: &LsmEngine<ProdEnv>) -> Result<(), String> {
        engine.flush_now().await.map_err(|e| e.to_string())
    }
    async fn clone_engine(
        &self,
        source: &LsmEngine<ProdEnv>,
        target: TabletId,
        keep: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<LsmEngine<ProdEnv>, String> {
        self.files.note_open(target);
        source
            .clone_to_filtered(lsm_prefix(target), keep)
            .await
            .map_err(|e| e.to_string())
    }
}

impl<S: StorageEngine + 'static> Cluster<S> {
    /// Propose one put to `t`'s current leader; `true` iff a leader accepted
    /// it (appended locally, ProposeResult::Accepted).
    fn put_leader(&self, t: TabletId, key: Vec<u8>, value: Vec<u8>) -> bool {
        for n in self.live() {
            let hosted = n.hosted.lock().expect("hosted");
            if let Some(g) = hosted.get(&t)
                && g.is_leader()
            {
                return matches!(
                    g.put(key, value),
                    animus_control::raft::ProposeResult::Accepted { .. }
                );
            }
        }
        false
    }
}

/// The issue #1199 acceptance (ADR 0078 Tier 3, ignored): 3000 RF3 groups on
/// three nodes hosted at once reach "all led" and hold the led count for
/// 120 s. Release build recommended (see the module doc).
///
/// Density calibration knobs (ADR 0079 step 1; all default to the historic
/// behaviour): `ANIMUS_BP_LIVE_WORKERS` (runtime worker threads, 2),
/// `ANIMUS_BP_LIVE_ENGINE=lsm` (`LsmEngine<ProdEnv>` per tablet, real files),
/// `ANIMUS_BP_LIVE_WRITE_RPS` (client puts per second spread round-robin
/// over the groups during the hold; 0), `ANIMUS_BP_LIVE_VALUE_BYTES` (128), `ANIMUS_BP_LIVE_WRITE_GROUPS` (all; a hot subset),
/// `ANIMUS_BP_LIVE_HOLD_SECS` (120), `ANIMUS_BP_LIVE_CONVERGE_SECS` (600), `ANIMUS_BP_LIVE_ASSERT=0` (report the
/// outcome in a `BP-LIVE verdict` line instead of failing, for sweeps). The
/// hold lines carry `cpu_ticks`/`rss_kb`/`fds`/`threads`/`files`/`ssts` from
/// `/proc/self`, so the test binary samples itself.
#[test]
#[ignore = "manual acceptance: 3000 groups, several minutes; run with --ignored --nocapture in release"]
fn density_repro_g3000_holds_led_count() {
    let workers: usize = knob("ANIMUS_BP_LIVE_WORKERS", 2);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("runtime");
    if std::env::var("ANIMUS_BP_LIVE_ENGINE").as_deref() == Ok("lsm") {
        rt.block_on(density_run(true, workers));
    } else {
        rt.block_on(density_run(false, workers));
    }
}

async fn density_run(lsm: bool, workers: usize) {
    if lsm {
        let mut c = Cluster::start(knob("ANIMUS_BP_LIVE_GROUPS", 3000), |env: &ProdEnv| {
            LsmFactory::new(env.clone())
        })
        .await;
        density_body(&mut c, true, workers).await;
    } else {
        let mut c = Cluster::start(knob("ANIMUS_BP_LIVE_GROUPS", 3000), |_| {
            MemoryTabletEngines::new()
        })
        .await;
        density_body(&mut c, false, workers).await;
    }
}

async fn density_body<S: StorageEngine + 'static>(c: &mut Cluster<S>, lsm: bool, workers: usize) {
    let g = c.g;
    let hold = Duration::from_secs(knob("ANIMUS_BP_LIVE_HOLD_SECS", 120));
    let rps: u64 = knob("ANIMUS_BP_LIVE_WRITE_RPS", 0);
    let value_bytes: usize = knob("ANIMUS_BP_LIVE_VALUE_BYTES", 128);
    let converge = Duration::from_secs(knob("ANIMUS_BP_LIVE_CONVERGE_SECS", 600));
    let write_groups: usize = knob("ANIMUS_BP_LIVE_WRITE_GROUPS", g).clamp(1, g);
    let assert_on = std::env::var("ANIMUS_BP_LIVE_ASSERT").as_deref() != Ok("0");
    println!(
        "BP-LIVE g={g} phase=config workers={workers} engine={} write_rps={rps} write_groups={write_groups} value_bytes={value_bytes} hold_s={}",
        if lsm { "lsm" } else { "memory" },
        hold.as_secs()
    );
    let verdict = timeout(Duration::from_secs(1500), async {
        spawn_sched_probe();
        let base = proc_sample();
        let took = c.await_all_led_soft("host", converge).await;
        let (led0, max_term, _, _) = c.census();
        let (hwm, _, deferred) = c.governor_stats();
        let hosted_s = proc_sample();
        println!(
            "BP-LIVE g={g} phase=all_led all_led_ms={} led={led0} max_term={max_term} inflight_hwm={hwm} deferred={deferred} \
             host_cpu_ticks={} rss_kb={} fds={} threads={}",
            took.map_or(-1, |t| t.as_millis() as i64),
            hosted_s.0 - base.0,
            hosted_s.1,
            hosted_s.2,
            hosted_s.3
        );
        // Hold: the led count must not fall for the whole window (the shape
        // that decays without the mechanisms). Optional client writes ride
        // along, `rps` per second round-robin across the groups.
        let t0 = Instant::now();
        let hold_base = proc_sample();
        let mut last_log = Instant::now();
        let mut min_led = g;
        let (mut sent, mut accepted, mut next_group) = (0u64, 0u64, 0usize);
        let mut budget = 0f64;
        let mut last_tick = Instant::now();
        let payload = vec![0xA5u8; value_bytes];
        while t0.elapsed() < hold {
            let (led, max_term, quiesced, hosted) = c.census();
            min_led = min_led.min(led);
            if rps > 0 {
                budget += rps as f64 * last_tick.elapsed().as_secs_f64();
                last_tick = Instant::now();
                while budget >= 1.0 {
                    budget -= 1.0;
                    let t = TabletId(STREAM0 + (next_group % write_groups) as u64);
                    let key = format!("k{sent:012}").into_bytes();
                    sent += 1;
                    next_group += 1;
                    if c.put_leader(t, key, payload.clone()) {
                        accepted += 1;
                    }
                }
            }
            if last_log.elapsed() >= Duration::from_secs(10) {
                let s = proc_sample();
                let (files, bytes, ssts) = if lsm { dir_stats(&c.dirs) } else { (0, 0, 0) };
                println!(
                    "BP-LIVE g={g} phase=hold t_s={} led={led} quiesced={quiesced} hosted={hosted} max_term={max_term} \
                     cpu_ticks={} rss_kb={} fds={} threads={} writes_sent={sent} writes_accepted={accepted} \
                     files={files} dir_bytes={bytes} ssts={ssts}",
                    t0.elapsed().as_secs(),
                    s.0 - hold_base.0,
                    s.1,
                    s.2,
                    s.3
                );
                last_log = Instant::now();
            }
            c.step().await;
        }
        let (led, max_term, _, _) = c.census();
        let (hwm, _, deferred) = c.governor_stats();
        let end = proc_sample();
        let hold_s = hold.as_secs_f64();
        let (files, bytes, ssts) = if lsm { dir_stats(&c.dirs) } else { (0, 0, 0) };
        println!(
            "BP-LIVE g={g} phase=held min_led={min_led} led={led} max_term={max_term} inflight_hwm={hwm} deferred={deferred} \
             hold_cores={:.3} rss_kb={} fds={} writes_sent={sent} writes_accepted={accepted} files={files} dir_bytes={bytes} ssts={ssts}",
            (end.0 - hold_base.0) as f64 / 100.0 / hold_s,
            end.1,
            end.2
        );
        (took.is_some(), min_led, hwm)
    })
    .await
    .expect("density_repro_g3000_holds_led_count timed out");
    let (converged, min_led, hwm) = verdict;
    println!(
        "BP-LIVE g={g} verdict converged={converged} min_led={min_led} held={}",
        converged && min_led == g
    );
    if assert_on {
        assert!(converged, "not every group led within the bound");
        assert_eq!(min_led, g, "the led count fell during the {hold:?} window");
        assert!(
            hwm <= DEFAULT_CAMPAIGN_MAX_INFLIGHT,
            "inflight hwm {hwm} above K"
        );
    }
}
