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

use animus_control::{Metadata, RaftNode};
use animus_dynamo::AttributeValue;
use animus_env::{NodeId, ProdEnv};
use animus_node::host::{BackupObjectStore, ControlLeaderHost, TtlScanHost};
use animus_tablet::TabletId;
use async_trait::async_trait;

use crate::dynamo::{self, KindWriteOutcome};
use crate::{ClientCtx, KindWriteOp};

impl ControlLeaderHost<ProdEnv> for ClientCtx {
    fn control_leader(&self) -> Option<RaftNode<ProdEnv>> {
        self.edge.leader_handle()
    }
}

#[async_trait]
impl BackupObjectStore for ClientCtx {
    async fn backup_put(&self, id: &str, bytes: &[u8]) -> Option<std::io::Result<Vec<NodeId>>> {
        let data = self.data_opt()?;
        Some(data.backup_store.put(id, bytes).await)
    }

    async fn backup_list_local(&self, prefix: &str) -> Option<std::io::Result<Vec<String>>> {
        let data = self.data_opt()?;
        Some(data.backup_store.list_local(prefix).await)
    }

    async fn backup_delete_local(&self, id: &str) -> Option<std::io::Result<()>> {
        let data = self.data_opt()?;
        Some(data.backup_store.delete_local(id).await)
    }

    async fn backup_delete_at(&self, replicas: &[NodeId], id: &str) -> Option<std::io::Result<()>> {
        let data = self.data_opt()?;
        Some(data.backup_store.delete(replicas, id).await)
    }
}

#[async_trait]
impl TtlScanHost for ClientCtx {
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
        match dynamo::kind_write_item_at_leader(
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
