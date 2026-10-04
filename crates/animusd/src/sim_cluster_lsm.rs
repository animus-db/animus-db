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

use std::collections::BTreeSet;

use animus_cp_data::host::{EngineFactory, MetadataView, Reconciler};
use animus_sim::SimEnv;
use futures::executor::block_on;

use super::*;

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
}

impl SimLsmTabletFactory {
    pub(crate) fn new(env: SimEnv) -> Self {
        Self { env }
    }
}

#[async_trait::async_trait]
impl EngineFactory<LsmEngine<SimEnv>> for SimLsmTabletFactory {
    async fn open(&self, tablet: TabletId) -> Result<LsmEngine<SimEnv>, String> {
        match LsmEngine::open(self.env.clone(), tablet_lsm_prefix(tablet.0)).await {
            Ok(engine) => Ok(engine),
            // Deliberately a panic, never `Err`: see the module doc.
            Err(e) => panic!(
                "strict open of the tablet {} LSM engine failed: {e}",
                tablet.0
            ),
        }
    }

    async fn probe(&self, tablet: TabletId) -> bool {
        let prefix = tablet_lsm_prefix(tablet.0);
        self.env
            .list()
            .await
            .unwrap_or_default()
            .iter()
            .any(|f| f.starts_with(&prefix))
    }

    async fn destroy(&self, tablet: TabletId) {
        let prefix = tablet_lsm_prefix(tablet.0);
        for f in self.env.list().await.unwrap_or_default() {
            if f.starts_with(&prefix) {
                let _ = self.env.remove(&f).await;
            }
        }
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
        source
            .clone_to_filtered(tablet_lsm_prefix(target.0), keep)
            .await
            .map_err(|e| e.to_string())
    }

    async fn local_tablets(&self) -> BTreeSet<TabletId> {
        self.env
            .list()
            .await
            .unwrap_or_default()
            .iter()
            .filter_map(|f| parse_tablet_id_from_lsm_filename(f))
            .collect()
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
            let engine = block_on(LsmEngine::open(env.clone(), SYSKV_LSM_PREFIX))
                .expect("strict open of the control system-keyspace LSM engine");
            match metrics {
                Some(m) => RaftNode::start_with_metrics(env, ids, m, engine),
                None => RaftNode::start(env, ids, engine),
            }
        }
    }
}
