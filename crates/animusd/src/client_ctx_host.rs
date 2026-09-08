//! `ClientCtx`'s implementations of `animus-node`'s host-capability traits
//! (ADR 0061 rung C2) — see `animus_node::host`'s own module doc for why
//! these traits exist and what each one is scoped to.
//!
//! **Every impl here is a thin, logic-free delegation to an already-existing
//! `ClientCtx`/`CpGroup`/`BackupStoreHandle` method.** Nothing in this file
//! makes a new decision; it only translates between the narrow trait shape
//! a moved loop wants and the concrete types this crate already has lying
//! around. If a future change needs new *logic* here, it almost certainly
//! belongs in the loop itself (`animus-node`) or in the method being
//! delegated to, not in this file.
//!
//! **Widened to `impl<E: Env, R: RelayClient> .. for ClientCtx<E, R>` (ADR
//! 0061 rung D4 PR 5)** — previously each impl here was pinned to the
//! concrete `ClientCtx` alias (`E = ProdEnv, R = AnimusdRelayClient`), the
//! one thing left stopping `animus_node::backup_janitor::
//! backup_janitor_loop` from being drivable under `SimEnv` at all (every
//! field each method reads — `self.edge`, `self.backup_store`,
//! `self.backup_janitor_progress`, `self.effective_metadata()`
//! — was already `E`/`R`-agnostic or already generic; only the four `impl`
//! headers themselves were concrete). No method body changed: `self.edge.
//! leader_handle()` already returns `Option<RaftNode<E>>` for the enclosing
//! `ClientCtx<E, R>`'s own `E` (`ClusterEdgeState<E>`, widened by rung D3 PR
//! 2a), and every other delegated-to method (`BackupStoreHandle::put`/
//! `list_local`/`delete_local`/`delete`, `Mutex::lock`, `edge.hosted_
//! groups()`, `dynamo::kind_write_item_at_leader::<E, R>`) was already
//! `E`/`R`-generic since rung C5. This is a pure signature widening, the
//! same "everything each method reads was already generic, only the impl
//! header wasn't" shape `sim_cluster_auto_split.rs`'s own `auto_split_loop`
//! widening found for its own two per-tablet bookkeeping maps.

use animus_control::{Metadata, RaftNode};
use animus_dynamo::AttributeValue;
use animus_env::{Env, NodeId};
use animus_node::backup_janitor::JanitorProgress;
use animus_node::host::{
    BackupJanitorProgressHost, BackupObjectStore, ControlLeaderHost, RelayClient,
    TtlReaperProgressHost, TtlScanHost,
};
use animus_node::ttl_reaper::TtlReaperProgress;
use animus_tablet::TabletId;
use async_trait::async_trait;

use crate::dynamo::{self, KindWriteOutcome};
use crate::{ClientCtx, KindWriteOp};

impl<E: Env, R: RelayClient> ControlLeaderHost<E> for ClientCtx<E, R> {
    fn control_leader(&self) -> Option<RaftNode<E>> {
        self.edge.leader_handle()
    }
}

/// **W-10 (ADR 0043 §A9's control-only-leader gap, closed)**: every method
/// here now always answers `Some(..)` — `ClientCtx::backup_store` is
/// provisioned on every node shape, including a control-only one, unlike
/// `DataRole`'s own fields (see that field's own doc). The trait itself
/// stays `Option`-returning (a genuinely store-less host is still a valid,
/// generically testable shape — see `animus_node::backup_janitor`'s own
/// `ControlOnlyStore` test double), this impl just never exercises the
/// `None` arm any more.
#[async_trait]
impl<E: Env, R: RelayClient> BackupObjectStore for ClientCtx<E, R> {
    async fn backup_put(&self, id: &str, bytes: &[u8]) -> Option<std::io::Result<Vec<NodeId>>> {
        Some(self.backup_store.put(id, bytes).await)
    }

    async fn backup_list_local(&self, prefix: &str) -> Option<std::io::Result<Vec<String>>> {
        Some(self.backup_store.list_local(prefix).await)
    }

    async fn backup_delete_local(&self, id: &str) -> Option<std::io::Result<()>> {
        Some(self.backup_store.delete_local(id).await)
    }

    async fn backup_delete_at(&self, replicas: &[NodeId], id: &str) -> Option<std::io::Result<()>> {
        Some(self.backup_store.delete(replicas, id).await)
    }
}

/// Roadmap U-07: `ClientCtx::backup_janitor_progress` is the shared
/// `Arc<Mutex<JanitorProgress>>` `animus_node::backup_janitor::
/// backup_janitor_loop` publishes into via this trait, and `GET
/// /admin/backup-store` reads back out — see that field's own doc.
impl<E: Env, R: RelayClient> BackupJanitorProgressHost for ClientCtx<E, R> {
    fn update_backup_janitor_progress(&self, update: &mut dyn FnMut(&mut JanitorProgress)) {
        let mut guard = self.backup_janitor_progress.lock().unwrap();
        update(&mut guard);
    }
}

/// Roadmap U-07: `ClientCtx::ttl_reaper_progress` is the shared
/// `Arc<Mutex<TtlReaperProgress>>` `animus_node::ttl_reaper::
/// ttl_reaper_loop` publishes into via this trait, and `GET /admin/ttl`
/// reads back out — see that field's own doc. Unlike
/// `BackupJanitorProgressHost` above, every node's own copy is a genuine
/// live answer (the reaper runs everywhere, self-gated per tablet), never
/// a stand-in for "not the leader."
///
/// **Widened to `impl<E: Env, R: RelayClient> .. for ClientCtx<E, R>` (ADR
/// 0061 rung I, C-09 PR 2)** — previously pinned to the concrete
/// `ClientCtx` alias, the one remaining concrete impl this module's own
/// doc comment (above) used to call out. `self.ttl_reaper_progress` is
/// already `E`/`R`-agnostic (a plain `Arc<Mutex<..>>`), so this is a pure
/// signature widening, the same shape `BackupJanitorProgressHost`
/// immediately above it was already widened to.
impl<E: Env, R: RelayClient> TtlReaperProgressHost for ClientCtx<E, R> {
    fn update_ttl_reaper_progress(&self, update: &mut dyn FnMut(&mut TtlReaperProgress)) {
        let mut guard = self.ttl_reaper_progress.lock().unwrap();
        update(&mut guard);
    }
}

#[async_trait]
impl<E: Env, R: RelayClient> TtlScanHost for ClientCtx<E, R> {
    fn ttl_metadata(&self) -> Metadata {
        self.effective_metadata()
    }

    fn led_tablets(&self) -> Vec<TabletId> {
        self.edge
            .hosted_groups()
            .into_iter()
            .filter(|(_, group)| group.is_leader())
            .map(|(tablet, _)| tablet)
            .collect()
    }

    async fn scan_base_capped(
        &self,
        tablet: TabletId,
        start: &[u8],
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Some((_, group)) = self
            .edge
            .hosted_groups()
            .into_iter()
            .find(|(t, _)| *t == tablet)
        else {
            return Vec::new();
        };
        group
            .local_scan_kind_capped(animus_cp_data::KIND_BASE, start, None, limit)
            .await
    }

    async fn ttl_delete_if_attribute_equals(
        &self,
        tablet: TabletId,
        table: &str,
        pk: &AttributeValue,
        sk: Option<&AttributeValue>,
        attribute: &str,
        expected: AttributeValue,
    ) -> Result<bool, String> {
        let Some((_, group)) = self
            .edge
            .hosted_groups()
            .into_iter()
            .find(|(t, _)| *t == tablet)
        else {
            return Err("tablet no longer hosted on this node".to_owned());
        };
        // ADR 0051 §6: wake — and only now — because there is genuinely a
        // delete to propose (mirrors the pre-move `ttl_reaper.rs`'s own
        // discipline; see `TtlScanHost::ttl_delete_if_attribute_equals`'s
        // doc).
        group.wake();
        let meta = self.effective_metadata();
        let condition = animus_dynamo::ConditionExpression::Compare(
            attribute.to_owned(),
            animus_dynamo::Comparator::Eq,
            expected,
        );
        match dynamo::kind_write_item_at_leader::<E, R>(
            self,
            &group,
            &meta,
            table,
            pk,
            sk,
            KindWriteOp::Delete,
            Some(&condition),
            // ADR 0051 §7: this delete is the TTL reaper's own, so its
            // change record carries the service `userIdentity`.
            true,
        )
        .await
        {
            Ok(KindWriteOutcome::Ok { .. }) => Ok(true),
            Ok(KindWriteOutcome::ConditionFailed) => Ok(false),
            Err(e) => Err(e.message),
        }
    }
}
