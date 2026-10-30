//! `SimCluster`'s `LsmEngine`-backed engine option (ADR 0073 Phase 1, P1-D
//! tier 2): the factory side of [`SimEngineBackend::Lsm`].
//!
//! `SimCluster` was `MemoryEngine`-only, so a restarted node's engines were
//! either a retained in-memory registry (tablets) or a brand-new empty
//! engine (control system keyspace) — neither reads a single durable byte
//! back off the node's `SimEnv` disk, which is exactly what an upgrade
//! restart must prove. With [`SimEngineBackend::Lsm`] every engine is a real
//! `LsmEngine<SimEnv>` over the node's own retained disk:
//!
//! - the **control system-keyspace** engine (`SYSKV_LSM_PREFIX`, ADR 0038's
//!   driver-applied `Metadata` mirror) — opened by [`start_control`];
//! - one **per-tablet** engine (`db-t{tablet}-…`, ADR 0050), opened by
//!   [`SimLsmTabletFactory`] under the real [`Reconciler`] — the same file
//!   naming `LsmTabletFactory` uses in production.
//!
//! **Opens are strict.** Both `.expect`/panic on an open failure, and the
//! tablet factory's `open` panics instead of returning `Err`: the
//! reconciler's own response to an `Err` is "destroy the engine and reopen
//! it fresh" (issue #554), which would turn a bad transcode or a recovery
//! bug into a silent clean wipe that Raft then repopulates — the masking
//! `docs/lessons/testing/` warns about for strict-open harnesses. The panic
//! surfaces out of `Simulator::run_for` for the corpus's `catch_unwind`.
//!
//! The default backend is [`SimEngineBackend::Memory`]: every pre-existing
//! `SimCluster` constructor still builds exactly what it always did.
#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the embedded build exposes a subset of this fixture through `EmbeddedCluster`; the rest is the SimCluster test tier's surface"
    )
)]

use std::collections::BTreeSet;

use animus_cp_data::host::{EngineFactory, MetadataView, Reconciler};
use animus_sim::SimEnv;
use futures::executor::block_on;

use super::*;

/// ADR 0081 Tier 2: LSM options and a shared compaction "wedge" every tablet
/// engine of the cluster is built with, installed per thread like
/// `SimOverload`. With the wedge set the engines' compaction gate withholds every
/// permit (urgent included), the stand-in for compaction that cannot keep up.
#[derive(Clone)]
pub(crate) struct SimStall {
    pub(crate) opts: animus_storage::LsmOptions,
    pub(crate) wedge: Arc<std::sync::atomic::AtomicBool>,
}

thread_local! {
    static SIM_STALL: std::cell::RefCell<Option<SimStall>> =
        const { std::cell::RefCell::new(None) };
}

/// Removes the thread's [`SimStall`] when dropped.
pub(crate) struct SimStallGuard(());

impl Drop for SimStallGuard {
    fn drop(&mut self) {
        SIM_STALL.with(|d| *d.borrow_mut() = None);
    }
}

impl SimStall {
    pub(crate) fn install(self) -> SimStallGuard {
        SIM_STALL.with(|d| *d.borrow_mut() = Some(self));
        SimStallGuard(())
    }

    fn current() -> Option<SimStall> {
        SIM_STALL.with(|d| d.borrow().clone())
    }
}

/// Issue #1389: a per-node memtable budget (bytes, floor, cap) every tablet
/// engine of the cluster joins, installed per thread like [`SimStall`]. Each
/// node's factory builds its own [`animus_storage::FlushBudget`], as
/// production's `BackgroundBudget` does per node.
#[derive(Clone, Copy)]
pub(crate) struct SimFlushBudget {
    pub(crate) budget: usize,
    pub(crate) floor: usize,
    pub(crate) cap: usize,
}

thread_local! {
    static SIM_FLUSH_BUDGET: std::cell::RefCell<Option<SimFlushBudget>> =
        const { std::cell::RefCell::new(None) };
}

/// Removes the thread's [`SimFlushBudget`] when dropped.
pub(crate) struct SimFlushBudgetGuard(());

impl Drop for SimFlushBudgetGuard {
    fn drop(&mut self) {
        SIM_FLUSH_BUDGET.with(|d| *d.borrow_mut() = None);
    }
}

impl SimFlushBudget {
    pub(crate) fn install(self) -> SimFlushBudgetGuard {
        SIM_FLUSH_BUDGET.with(|d| *d.borrow_mut() = Some(self));
        SimFlushBudgetGuard(())
    }

    fn current() -> Option<SimFlushBudget> {
        SIM_FLUSH_BUDGET.with(|d| *d.borrow())
    }
}

struct SimWedge(Arc<std::sync::atomic::AtomicBool>);

impl animus_storage::CompactionGate for SimWedge {
    fn acquire(&self, _bypass: bool) -> Option<animus_storage::CompactionPermit> {
        if self.0.load(std::sync::atomic::Ordering::Acquire) {
            None
        } else {
            Some(Box::new(()))
        }
    }
}

/// Which storage engine a [`SimCluster`] builds under its control plane and
/// its tablet-host reconcilers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum SimEngineBackend {
    /// `MemoryEngine` everywhere — the historic behaviour, the default.
    #[default]
    Memory,
    /// `LsmEngine<SimEnv>` over each node's own retained disk, strict opens.
    Lsm,
}

/// The `LsmEngine<SimEnv>` per-tablet engine seam — [`LsmTabletFactory`]'s
/// shape (ADR 0050 rung 1) over a `SimEnv` disk, strict on `open`.
pub(crate) struct SimLsmTabletFactory {
    env: SimEnv,
    stall: Option<SimStall>,
    flush_budget: Option<animus_storage::FlushBudget>,
    files: animus_cp_data::tablet_files::TabletFileIndex<SimEnv>,
}

impl SimLsmTabletFactory {
    pub(crate) fn new(env: SimEnv) -> Self {
        Self {
            stall: SimStall::current(),
            flush_budget: SimFlushBudget::current()
                .map(|b| animus_storage::FlushBudget::new(b.budget, b.floor, b.cap)),
            files: animus_cp_data::tablet_files::TabletFileIndex::new(
                env.clone(),
                |t| tablet_lsm_prefix(t.0),
                parse_tablet_id_from_lsm_filename,
            ),
            env,
        }
    }
}

#[async_trait::async_trait]
impl EngineFactory<LsmEngine<SimEnv>> for SimLsmTabletFactory {
    async fn open(&self, tablet: TabletId) -> Result<LsmEngine<SimEnv>, String> {
        self.files.note_open(tablet);
        let opened = match &self.stall {
            Some(st) => {
                LsmEngine::open_with(self.env.clone(), tablet_lsm_prefix(tablet.0), st.opts).await
            }
            None => LsmEngine::open(self.env.clone(), tablet_lsm_prefix(tablet.0)).await,
        };
        match opened {
            Ok(engine) => {
                if let Some(st) = &self.stall {
                    engine.set_compaction_gate(Arc::new(SimWedge(st.wedge.clone())));
                }
                if let Some(fb) = &self.flush_budget {
                    engine.set_flush_budget(fb);
                }
                Ok(engine)
            }
            // Deliberately a panic, never `Err`: see the module doc.
            Err(e) => panic!(
                "strict open of the tablet {} LSM engine failed: {e}",
                tablet.0
            ),
        }
    }

    async fn probe(&self, tablet: TabletId) -> bool {
        self.files.probe(tablet).await
    }

    async fn destroy(&self, tablet: TabletId) {
        let _ = self.files.destroy(tablet).await;
    }

    async fn flush_engine(&self, engine: &LsmEngine<SimEnv>) -> Result<(), String> {
        engine.flush_now().await.map_err(|e| e.to_string())
    }

    async fn clone_engine(
        &self,
        source: &LsmEngine<SimEnv>,
        target: TabletId,
        keep: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<LsmEngine<SimEnv>, String> {
        self.files.note_open(target);
        let engine = source
            .clone_to_filtered(tablet_lsm_prefix(target.0), keep)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(st) = &self.stall {
            engine.set_compaction_gate(Arc::new(SimWedge(st.wedge.clone())));
        }
        if let Some(fb) = &self.flush_budget {
            engine.set_flush_budget(fb);
        }
        Ok(engine)
    }

    async fn local_tablets(&self) -> BTreeSet<TabletId> {
        self.files.local_tablets().await
    }
}

/// One node's tablet-host reconciler over whichever backend the cluster was
/// built with — the two `Reconciler` instantiations behind one small facade
/// so `spawn_reconciler_loop` stays a single function.
pub(crate) enum SimReconciler {
    Mem(Reconciler<SimEnv, MemoryEngine>),
    Lsm(Reconciler<SimEnv, LsmEngine<SimEnv>>),
}

impl SimReconciler {
    pub(crate) fn enable_quiescence(&mut self, after: Duration) {
        match self {
            SimReconciler::Mem(r) => r.enable_quiescence(after),
            SimReconciler::Lsm(r) => r.enable_quiescence(after),
        }
    }

    /// ADR 0073 Phase 2 (P2-C): inject the node's control-fed handle, exactly
    /// as production's node assembly does.
    pub(crate) fn set_cluster_features(
        &mut self,
        features: animus_control::version::ClusterFeatures,
    ) {
        match self {
            SimReconciler::Mem(r) => r.set_cluster_features(features),
            SimReconciler::Lsm(r) => r.set_cluster_features(features),
        }
    }

    /// ADR 0078 section 3: publish this node's staggered-hosting reading to the
    /// node's edge state, exactly as production's node assembly does.
    pub(crate) fn set_ramp_gauge(&mut self, gauge: animus_cp_data::host::HostingRampGauge) {
        match self {
            SimReconciler::Mem(r) => r.set_ramp_gauge(gauge),
            SimReconciler::Lsm(r) => r.set_ramp_gauge(gauge),
        }
    }

    /// ADR 0078 sections 1 and 2: the node's campaign governor and load
    /// monitor, shared by every data-plane group hosted here, exactly as
    /// production's node assembly installs them.
    pub(crate) fn set_election_backpressure(
        &mut self,
        bp: animus_cp_data::campaign_governor::ElectionBackpressure,
    ) {
        match self {
            SimReconciler::Mem(r) => r.set_election_backpressure(bp),
            SimReconciler::Lsm(r) => r.set_election_backpressure(bp),
        }
    }

    /// ADR 0078 section 3: the reconciler's own short re-tick request while a
    /// hosting ramp has deferred work.
    pub(crate) fn next_wake(&self) -> Option<Duration> {
        match self {
            SimReconciler::Mem(r) => r.next_wake(),
            SimReconciler::Lsm(r) => r.next_wake(),
        }
    }

    pub(crate) async fn tick(&mut self, view: &MetadataView) {
        match self {
            SimReconciler::Mem(r) => r.tick(view).await,
            SimReconciler::Lsm(r) => r.tick(view).await,
        }
    }
}

/// Start one node's control `RaftNode` over `backend`'s engine. `metrics:
/// None` mirrors the plain `RaftNode::start` the restart path uses;
/// `memory_syskv` is the node's persistent syskv engine for the `Memory`
/// backend (a clone shares state; pass the same one on restart).
/// `Some(m)` the `start_with_metrics` the construction/growth paths use. The
/// `Memory` arms are exactly the calls they replace.
///
/// `Lsm` opens the node's system-keyspace engine (`SYSKV_LSM_PREFIX`)
/// **strictly** off its retained disk: `block_on` is sound because a
/// `SimEnv` disk operation completes without the simulator being stepped.
pub(crate) fn start_control(
    backend: SimEngineBackend,
    env: SimEnv,
    ids: Vec<NodeId>,
    metrics: Option<MetricsHandle>,
    memory_syskv: MemoryEngine,
) -> RaftNode<SimEnv> {
    match backend {
        SimEngineBackend::Memory => match metrics {
            Some(m) => RaftNode::start_with_metrics(env, ids, m, memory_syskv),
            None => RaftNode::start(env, ids, memory_syskv),
        },
        SimEngineBackend::Lsm => {
            let engine = block_on(LsmEngine::open_with(
                env.clone(),
                SYSKV_LSM_PREFIX,
                LsmOptions::raw_versions(),
            ))
            .expect("strict open of the control system-keyspace LSM engine");
            match metrics {
                Some(m) => RaftNode::start_with_metrics(env, ids, m, engine),
                None => RaftNode::start(env, ids, engine),
            }
        }
    }
}
