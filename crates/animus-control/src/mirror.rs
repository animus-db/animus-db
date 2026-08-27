//! The control plane's **system-keyspace derivation + rebuild logic** (ADR
//! 0038). Introduced in PR2 as a shadow-mode dual-write mirror; **PR3 (the
//! cutover) promotes it to the real apply path's core**: given a just-applied
//! [`MetaCommand`] and the [`Metadata`] it was applied against, derive the
//! bounded set of [`syskv`] key/value writes that command implies, and
//! (separately) rebuild a [`Metadata`] back out of a `StorageEngine`'s system
//! keyspace. Both directions are exercised by the differential-oracle tests
//! (`tests/apply_engine.rs`, the PR3 successor to PR2's `mirror_engine.rs`):
//! drive a real `RaftNode` (its apply task is now the only writer) and assert
//! its published cache agrees with an independent engine rebuild.
//!
//! This module is **pure** (no `Env`, no I/O) except [`rebuild_metadata_from_engine`],
//! which only *reads* a [`StorageEngine`] — the actual write path (deriving
//! writes here, then `merge_batch`-ing them into an engine) is driven by
//! `node.rs`'s apply task (`meta_apply_loop`/`meta_apply_and_compact`), which
//! also owns the *only* mutable in-memory `Metadata` now that
//! `StateMachine::DRIVER_APPLIED = true` — there is no longer a separate
//! in-core copy this module's output merely shadows.
//!
//! ## Why derivation needs *pre*-apply state, not just post-apply `Metadata`
//!
//! The obvious signature would be `fn touched_keys(command: &MetaCommand,
//! after: &Metadata) -> Vec<KeyWrite>` — post-apply state only. That works for
//! most commands (an upsert's new value, a CAS's bumped tablet, a schema
//! change) but is **incomplete** for the two commands whose derived
//! *deletions* depend on identities that no longer exist once `apply` has run:
//!
//! - [`MetaCommand::DropTableTablets`] only carries a table *name* — which
//!   tablet ids it just removed is computed internally
//!   (`Metadata::tablets_for_table`) from state that is gone by the time
//!   `apply` returns.
//! - [`MetaCommand::DropTableTablets`] also prunes the
//!   legacy `Metadata::cp_member_addrs`/`cp_member_tablets` address book
//!   (`Metadata::prune_cp_member_addrs`) for any CP member registered against
//!   a tablet that just left the map — again, only knowable by comparing
//!   against what existed *before*.
//!
//! Rather than duplicate `Metadata::apply`'s internal pruning logic a second
//! time here (a correctness hazard if the two ever drift — the exact
//! "grep every gating match site" class of bug this crate's `CLAUDE.md`
//! flags), [`apply_and_derive_mirror`] takes `&mut Metadata` and captures the
//! small, targeted slice of pre-apply state each command's derivation
//! actually needs (the table's current tablet ids; a clone of
//! `cp_member_tablets`, bounded by "one entry per CP member ever registered")
//! **before** calling the real, unchanged `Metadata::apply`, then derives
//! writes by diffing against the post-apply result. This is a deliberate,
//! documented deviation from a post-state-only signature — see the PR2 report
//! for the trade-off.
//!
//! ## Full fidelity, not a partial mirror
//!
//! Every [`MetaCommand`] variant is mirrored, including the legacy
//! `RegisterCpAddr` and the monotonic tablet-id-allocator counter
//! (`next_tablet_id`) — none of these were in PR1's [`EntityKind`] set, so
//! this module's own PR2 changes to `syskv.rs` added
//! [`syskv::EntityKind::Counter`]/[`syskv::EntityKind::CpMemberAddr`] (a
//! third PR2 variant, `NodeIdAlloc`, mirrored the ADR 0036 allocator's
//! idempotency ledger — removed in ADR 0040 PR4 along with the allocator;
//! `MetaCommand::RegisterNode`'s claim lives entirely in the already-mirrored
//! `Member`/`NodeAddrs` kinds, no separate ledger needed). The payoff:
//! [`rebuild_metadata_from_engine`] produces a `Metadata` that is
//! `PartialEq`-identical to the real in-core one, not "identical modulo a
//! documented gap" — which is exactly what the differential-oracle test
//! asserts.

use std::collections::BTreeMap;

use animus_env::NodeId;
#[cfg(test)]
use animus_env::nid;
use animus_placement::PlacementPolicy;
use animus_storage::{StorageEngine, StorageError};
use animus_tablet::{Tablet, TabletId};
use serde::{Deserialize, Serialize};

use crate::meta::{ApplyOutcome, Member, MetaCommand, Metadata, NodeAddrs};
use crate::schema::TableSchema;
use crate::syskv::{self, DecodedKey, EntityKind};

/// The counter name for `Metadata::next_tablet_id` under
/// [`EntityKind::Counter`] (`syskv::counter_key`). The ADR 0036 allocator's
/// own sibling counter (`NEXT_ALLOC_ID_COUNTER`) was removed in ADR 0040 PR4
/// along with the allocator itself.
pub const NEXT_TABLET_ID_COUNTER: &str = "next_tablet_id";

/// One system-keyspace mutation an applied [`MetaCommand`] implies. The
/// mirror loop (`node.rs`) translates these into per-key-LWW
/// [`animus_storage::MergeOp`]s versioned at the command's own Raft log
/// index — never [`StorageEngine::write_batch`]/`put` (which enforce a
/// single engine-wide monotonic version), since a **combined** node's mirror
/// shares its engine with the CP data plane's own, independently-versioned
/// writes (see the PR2 report's inline-vs-offloaded write-path note).
///
/// **`Serialize`/`Deserialize` (ADR 0038 PR5)**: these ride the wire
/// verbatim as an incremental `WatchMetadata` reply's payload (`animusd`'s
/// `ClientResponse::MetadataDelta`, fed by [`crate::delta_ring::DeltaRing`]) —
/// see [`apply_key_write`] for the consumer side that installs a received
/// batch of these onto a plain `Metadata` with no engine of its own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyWrite {
    /// Upsert `key` to `value`.
    Put(Vec<u8>, Vec<u8>),
    /// Tombstone `key`.
    Delete(Vec<u8>),
}

/// The legacy CP-member address registration
/// (`Metadata::cp_member_addrs`/`cp_member_tablets`, `MetaCommand::RegisterCpAddr`)
/// mirrored as one value under [`EntityKind::CpMemberAddr`] — the two source
/// maps share a key (`NodeId`) but not necessarily a domain (`tablet` may be
/// absent from `cp_member_tablets` while present in `cp_member_addrs`), so
/// they're combined into one record rather than mirrored as two keys that
/// could disagree on presence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CpMemberAddrEntry {
    addr: String,
    tablet: Option<TabletId>,
}

/// Apply `command` to `meta` — delegating to the real, unchanged
/// [`Metadata::apply`] — and derive the system-keyspace [`KeyWrite`]s it
/// implies. Returns `(outcome, writes)`; `writes` is always empty when
/// `outcome != ApplyOutcome::Applied` (a rejected or no-op command changed
/// nothing, so there is nothing to mirror).
///
/// Every [`MetaCommand`] variant has an explicit arm — no wildcard — so a
/// future variant fails to compile here until its mirror behavior is a
/// deliberate decision, not an accidental fallthrough.
#[must_use]
pub fn apply_and_derive_mirror(
    meta: &mut Metadata,
    command: &MetaCommand,
) -> (ApplyOutcome, Vec<KeyWrite>) {
    // Capture whatever pre-apply state this command's derivation needs —
    // see the module doc for why post-apply state alone isn't enough for
    // `DropTableTablets`. Cheap/no-op for every other command.
    let dropped_tablets: Vec<TabletId> = match command {
        MetaCommand::DropTableTablets { table } => {
            meta.tablets_for_table(table).map(|(&id, _)| id).collect()
        }
        _ => Vec::new(),
    };
    let pre_cp_member_tablets: BTreeMap<NodeId, TabletId> = match command {
        MetaCommand::DropTableTablets { .. } => meta.cp_member_tablets.clone(),
        _ => BTreeMap::new(),
    };
    // ADR 0045: `DropTableTablets`/`DropTableIndex` both prune
    // `Metadata::index_backfill` rows as a side effect of their own apply —
    // exactly the same "identities gone by the time `apply` returns" hazard
    // the module doc above describes for `dropped_tablets`, so the rows each
    // command is *about* to prune are captured here, pre-apply, the same way.
    let pruned_index_backfill: Vec<(TabletId, String)> = match command {
        MetaCommand::DropTableTablets { table } => {
            let dropped: Vec<TabletId> = meta.tablets_for_table(table).map(|(&id, _)| id).collect();
            meta.index_backfill
                .keys()
                .filter(|(tablet, _)| dropped.contains(tablet))
                .cloned()
                .collect()
        }
        MetaCommand::DropTableIndex { table, index } => {
            let table_tablets: Vec<TabletId> =
                meta.tablets_for_table(table).map(|(&id, _)| id).collect();
            meta.index_backfill
                .keys()
                .filter(|(tablet, idx)| idx == index && table_tablets.contains(tablet))
                .cloned()
                .collect()
        }
        _ => Vec::new(),
    };
    // ADR 0059 §3: `DeleteBackup` only carries the backup id — which
    // `(backup_id, tablet)` progress rows it is about to prune is only
    // knowable by looking at pre-apply state, the identical
    // "identities gone by the time `apply` returns" hazard the module doc
    // above describes for `dropped_tablets`.
    let pruned_backup_progress: Vec<(crate::meta::BackupId, TabletId)> = match command {
        MetaCommand::DeleteBackup { backup_id } => meta
            .backup_tablet_progress
            .keys()
            .filter(|(id, _)| id == backup_id)
            .cloned()
            .collect(),
        _ => Vec::new(),
    };

    let outcome = meta.apply(command);
    if outcome != ApplyOutcome::Applied {
        return (outcome, Vec::new());
    }

    let mut writes = Vec::new();
    match command {
        MetaCommand::NoOp => {
            // `Metadata::apply` never returns `Applied` for `NoOp` — listed
            // explicitly so this match stays exhaustive without a wildcard.
        }
        MetaCommand::UpsertMember { node, .. } => {
            writes.push(put_json(syskv::member_key(node), &meta.members[node]));
        }
        MetaCommand::CreateTablet { tablet, .. } => {
            writes.push(put_json(syskv::tablet_key(*tablet), &meta.tablets[tablet]));
            writes.push(put_counter(NEXT_TABLET_ID_COUNTER, meta.next_tablet_id));
        }
        MetaCommand::CasTabletReplicas { tablet, .. } => {
            writes.push(put_json(syskv::tablet_key(*tablet), &meta.tablets[tablet]));
        }
        MetaCommand::BeginSplit {
            parent, children, ..
        } => {
            // ADR 0050 stage 1: the parent's row (state → Splitting, epoch
            // bumped) plus both freshly-minted `Building` children, their
            // inherited policies, and the advanced allocator counter.
            writes.push(put_json(syskv::tablet_key(*parent), &meta.tablets[parent]));
            writes.push(put_counter(NEXT_TABLET_ID_COUNTER, meta.next_tablet_id));
            for (child, _) in children {
                writes.push(put_json(syskv::tablet_key(*child), &meta.tablets[child]));
                if let Some(policy) = meta.policies.get(child) {
                    writes.push(put_json(syskv::policy_key(*child), policy));
                }
            }
        }
        MetaCommand::BeginSplitInPlace { parent, .. } => {
            // ADR 0058 Train 2 rung 3, Stage 1: only the parent's own row
            // changes (state → Splitting, `inplace_split` set, epoch
            // bumped) plus the advanced allocator counter — no child rows
            // exist yet (unlike `BeginSplit`, which mints `Building`
            // tablets here).
            writes.push(put_json(syskv::tablet_key(*parent), &meta.tablets[parent]));
            writes.push(put_counter(NEXT_TABLET_ID_COUNTER, meta.next_tablet_id));
        }
        MetaCommand::CutoverSplit { parent, .. } => {
            // ADR 0050 stage 4: the parent's row (and policy) are gone; both
            // children re-mirror (state → Active, epoch bumped) along with
            // their new lineage rows. The children are exactly the
            // `split_lineage` rows this apply just wrote for `parent` —
            // parents are removed at cutover and tablet ids never reused, so
            // the parent id uniquely identifies this cutover's children.
            //
            // ADR 0058 Train 2 rung 3's in-place branch also mirrors each
            // child's policy HERE — its own apply arm is the only place an
            // in-place child's policy is ever set at all (there is no
            // `Building` tablet-map row to attach it to earlier, unlike the
            // copy-based branch, whose own `BeginSplit` arm above already
            // mirrored it). Unconditional and idempotent for the copy-based
            // branch too: a child's policy there was already mirrored at
            // `BeginSplit` time, so this is a harmless duplicate write of
            // the identical value, not a second source of truth.
            writes.push(KeyWrite::Delete(syskv::tablet_key(*parent)));
            writes.push(KeyWrite::Delete(syskv::policy_key(*parent)));
            for (child, lineage) in meta
                .split_lineage
                .iter()
                .filter(|(_, l)| l.parent == *parent)
            {
                writes.push(put_json(syskv::tablet_key(*child), &meta.tablets[child]));
                writes.push(put_json(syskv::split_lineage_key(*child), lineage));
                if let Some(policy) = meta.policies.get(child) {
                    writes.push(put_json(syskv::policy_key(*child), policy));
                }
            }
        }
        MetaCommand::SetTabletPolicy { tablet, policy } => match policy {
            Some(p) => writes.push(put_json(syskv::policy_key(*tablet), p)),
            None => writes.push(KeyWrite::Delete(syskv::policy_key(*tablet))),
        },
        MetaCommand::CreateTableSchema { table, schema } => {
            writes.push(put_json(syskv::schema_key(table), schema));
        }
        MetaCommand::DropTableSchema { table } => {
            writes.push(KeyWrite::Delete(syskv::schema_key(table)));
        }
        MetaCommand::ReplaceTableSchema { table, schema } => {
            writes.push(put_json(syskv::schema_key(table), schema));
        }
        MetaCommand::DropTableTablets { table: _ } => {
            for id in &dropped_tablets {
                writes.push(KeyWrite::Delete(syskv::tablet_key(*id)));
                writes.push(KeyWrite::Delete(syskv::policy_key(*id)));
            }
            for (tablet, index) in &pruned_index_backfill {
                writes.push(KeyWrite::Delete(syskv::index_backfill_key(*tablet, index)));
            }
            for id in dead_cp_member_ids(&pre_cp_member_tablets, meta) {
                writes.push(KeyWrite::Delete(syskv::cp_member_addr_key(&id)));
            }
        }
        MetaCommand::DropTableIndex { table, .. } => {
            if let Some(schema) = meta.schemas.get(table) {
                writes.push(put_json(syskv::schema_key(table), schema));
            }
            // ADR 0045: this index's own backfill-completion rows are gone
            // the moment its definition is — see `pruned_index_backfill`'s
            // own pre-apply capture above.
            for (tablet, index) in &pruned_index_backfill {
                writes.push(KeyWrite::Delete(syskv::index_backfill_key(*tablet, index)));
            }
        }
        MetaCommand::CreateTableIndex { table, .. }
        | MetaCommand::SetIndexStatus { table, .. }
        // ADR 0042: a stream (de)configuration is part of the table's schema
        // entry, so it mirrors identically to an index change — the whole
        // (already-mutated) schema, re-serialized.
        | MetaCommand::SetTableStream { table, .. }
        // ADR 0051: TTL is likewise part of the table's schema entry, so it
        // mirrors exactly the same way.
        | MetaCommand::SetTableTtl { table, .. } => {
            if let Some(schema) = meta.schemas.get(table) {
                writes.push(put_json(syskv::schema_key(table), schema));
            }
        }
        MetaCommand::MarkIndexBackfilled { tablet, index, .. } => {
            // The value is always empty (presence alone is the fact).
            writes.push(KeyWrite::Put(
                syskv::index_backfill_key(*tablet, index),
                Vec::new(),
            ));
        }
        MetaCommand::RegisterCpAddr { id, addr, tablet } => {
            let entry = CpMemberAddrEntry {
                addr: addr.clone(),
                tablet: *tablet,
            };
            writes.push(put_json(syskv::cp_member_addr_key(id), &entry));
        }
        MetaCommand::RegisterNodeAddrs { node, addrs } => {
            writes.push(put_json(syskv::node_addrs_key(node), addrs));
        }
        MetaCommand::RemoveMember { node } => {
            writes.push(KeyWrite::Delete(syskv::member_key(node)));
            writes.push(KeyWrite::Delete(syskv::node_addrs_key(node)));
            writes.push(KeyWrite::Delete(syskv::cp_member_addr_key(node)));
        }
        MetaCommand::RegisterNode { node, .. } => {
            // `apply_and_derive_mirror` only reaches here when `outcome ==
            // Applied`, but that no longer guarantees `members[node]` is
            // present: `RegisterNode`'s apply never claims a `members` row
            // for a control-role registration (`addrs.role == "control"` —
            // see its own doc for why), and it can also apply as a
            // members-row *repair* against an id that already has an
            // identical `node_addrs` entry but somehow lost its members row.
            // `node_addrs[node]` alone is always guaranteed (this arm is the
            // only command that ever writes it out unconditionally on
            // `Applied`).
            if let Some(member) = meta.members.get(node) {
                writes.push(put_json(syskv::member_key(node), member));
            }
            writes.push(put_json(
                syskv::node_addrs_key(node),
                &meta.node_addrs[node],
            ));
        }
        MetaCommand::SealStreamShard { tablet, epoch, .. } => {
            writes.push(put_json(
                syskv::stream_shard_key(*tablet, *epoch),
                &meta.stream_shards[&(*tablet, *epoch)],
            ));
        }
        MetaCommand::ExpireStreamShards { rows, .. } => {
            // Both phases (mark and remove) only ever touch rows that are
            // still present after `remove: false`'s mark, or that this
            // apply's `remove: true` just deleted — `apply_and_derive_mirror`
            // always derives from `meta`'s own post-apply state, so a row
            // absent from `meta.stream_shards` (already removed, or the
            // command named a row that was never present at all — both
            // idempotent no-ops) mirrors as a tombstone rather than being
            // silently skipped, matching every other idempotent-delete arm
            // above (`DropTableSchema`, etc.) — a repeated delete of an
            // already-absent key is a harmless no-op write, not an error.
            for (tablet, epoch) in rows {
                match meta.stream_shards.get(&(*tablet, *epoch)) {
                    Some(row) => {
                        writes.push(put_json(syskv::stream_shard_key(*tablet, *epoch), row));
                    }
                    None => {
                        writes.push(KeyWrite::Delete(syskv::stream_shard_key(*tablet, *epoch)));
                    }
                }
            }
        }
        MetaCommand::BeginBackup { backup_id, .. } => {
            writes.push(put_json(
                syskv::backup_key(backup_id),
                &meta.backups[backup_id],
            ));
        }
        MetaCommand::RecordBackupTabletComplete {
            backup_id, tablet, ..
        } => {
            writes.push(put_json(
                syskv::backup_progress_key(backup_id, *tablet),
                &meta.backup_tablet_progress[&(backup_id.clone(), *tablet)],
            ));
        }
        MetaCommand::CompleteBackup { backup_id } | MetaCommand::FailBackup { backup_id, .. } => {
            writes.push(put_json(
                syskv::backup_key(backup_id),
                &meta.backups[backup_id],
            ));
        }
        MetaCommand::DeleteBackup { backup_id } => {
            writes.push(KeyWrite::Delete(syskv::backup_key(backup_id)));
            for (id, tablet) in &pruned_backup_progress {
                writes.push(KeyWrite::Delete(syskv::backup_progress_key(id, *tablet)));
            }
        }
        MetaCommand::MarkBackupDeleted { backup_id } => {
            writes.push(put_json(
                syskv::backup_key(backup_id),
                &meta.backups[backup_id],
            ));
        }
    }
    (outcome, writes)
}

/// Every id in `pre` (a `cp_member_tablets` snapshot taken *before* this
/// apply) whose associated tablet is no longer in `meta.tablets` (taken
/// *after*) — i.e. exactly the entries `Metadata::prune_cp_member_addrs`
/// just pruned as a side effect of this one apply. Diffing against `pre`
/// rather than re-deriving the prune predicate keeps this in lockstep with
/// `Metadata::apply`'s own logic without duplicating it.
fn dead_cp_member_ids(pre: &BTreeMap<NodeId, TabletId>, meta: &Metadata) -> Vec<NodeId> {
    pre.iter()
        .filter(|(_, tablet)| !meta.tablets.contains_key(tablet))
        .map(|(id, _)| id.clone())
        .collect()
}

/// Serialize `value` (the same `serde_json` encoding the WAL snapshot blob
/// uses) as a [`KeyWrite::Put`] at `key`.
fn put_json<T: Serialize>(key: Vec<u8>, value: &T) -> KeyWrite {
    KeyWrite::Put(
        key,
        serde_json::to_vec(value).expect("system-keyspace entity value serializes"),
    )
}

/// A [`KeyWrite::Put`] of a named counter's current value (big-endian, so a
/// raw engine scan sorts by numeric value like every other numeric id here).
fn put_counter(name: &str, value: u64) -> KeyWrite {
    KeyWrite::Put(syskv::counter_key(name), value.to_be_bytes().to_vec())
}

/// Decode an 8-byte big-endian `u64` written by [`put_counter`] or any
/// numeric-id `*_key` helper. Panics on a malformed value — this module never
/// writes anything else at these keys, so a mismatch is an internal bug, not
/// a data problem worth threading a `Result` through every call site for.
fn decode_u64(bytes: &[u8]) -> u64 {
    let array: [u8; 8] = bytes
        .try_into()
        .expect("system-keyspace u64 value is exactly 8 bytes");
    u64::from_be_bytes(array)
}

/// Decode a [`NodeId`]'s raw UTF-8 bytes (ADR 0040 PR3: a node id's key/value
/// bytes are its raw string bytes, not an 8-byte big-endian `u64` — see
/// `syskv::member_key`'s doc). Bypasses [`NodeId::propose`]'s charset
/// validation via [`NodeId::new_unchecked`] — this id was already validated
/// once at whatever intake boundary first proposed it; this is a trusted,
/// already-replicated round-trip, not fresh untrusted input. Panics on
/// non-UTF-8 bytes — this module never writes anything else at a node-id
/// key, so that would be an internal bug.
fn decode_node_id(bytes: Vec<u8>) -> NodeId {
    let s = String::from_utf8(bytes).expect("system-keyspace node id is UTF-8");
    NodeId::new_unchecked(s)
}

/// Rebuild a [`Metadata`] purely from a [`StorageEngine`]'s live system
/// keyspace — the mirror's read side, used by (a) the differential-oracle
/// test (assert this equals the real in-core `Metadata` at the same index),
/// (b) a restart's shadow-cache rebuild (`node.rs`'s `mirror_loop`), and (c)
/// the apply task's own cache-rebuild-on-restart step. Built from
/// [`apply_key_write`] (one `Put` per live entry — `entries()` never yields
/// tombstones, so this only ever exercises that half) so the bulk-rebuild
/// and incremental-delta decode paths can't drift apart.
///
/// Ignores any live key that doesn't decode as a system-keyspace key
/// ([`syskv::decode_key`] returning `None`) rather than failing — a shared
/// combined-node engine's `entries()` scan only ever includes reserved-
/// namespace keys here because [`syskv::is_reserved_name`] already rejects
/// any user table/keyspace name that could collide, but this stays
/// defensive rather than assuming it.
///
/// # Errors
/// Propagates a [`StorageEngine::entries`] backend failure.
pub async fn rebuild_metadata_from_engine<S: StorageEngine>(
    engine: &S,
) -> Result<Metadata, StorageError> {
    let mut meta = Metadata::default();
    for (key, versioned) in engine.entries().await? {
        apply_key_write(&mut meta, &KeyWrite::Put(key, versioned.value));
    }
    Ok(meta)
}

/// Install one already-derived system-keyspace [`KeyWrite`] directly onto a
/// `Metadata` — the incremental counterpart to [`rebuild_metadata_from_engine`]'s
/// bulk rebuild, and the read side of ADR 0038 PR5's `WatchMetadata` delta
/// reply: `animusd::control_handle::RemoteControlClient::observe_delta`
/// installs a leader-derived batch of these onto its own cached `Metadata`
/// with no engine of its own, exactly the "install pre-diffed key/value
/// pairs verbatim via the existing syskv decode/rebuild machinery" design.
///
/// Ignores a key that doesn't decode as a live entity ([`syskv::decode_key`]
/// returning `None`, or the `AppliedIndex` watermark key, which isn't part
/// of `Metadata`) — mirrors [`rebuild_metadata_from_engine`]'s defensive
/// treatment of an unrecognized key.
pub fn apply_key_write(meta: &mut Metadata, write: &KeyWrite) {
    match write {
        KeyWrite::Put(key, value) => apply_put(meta, key, value),
        KeyWrite::Delete(key) => apply_delete(meta, key),
    }
}

/// The `Put`/upsert half of [`apply_key_write`].
fn apply_put(meta: &mut Metadata, key: &[u8], value: &[u8]) {
    let Some(DecodedKey::Entity { kind, id }) = syskv::decode_key(key) else {
        return;
    };
    match kind {
        EntityKind::Tablet => {
            let tablet: Tablet =
                serde_json::from_slice(value).expect("mirrored tablet value decodes");
            meta.tablets.insert(TabletId(decode_u64(&id)), tablet);
        }
        EntityKind::Member => {
            let member: Member =
                serde_json::from_slice(value).expect("mirrored member value decodes");
            meta.members.insert(decode_node_id(id), member);
        }
        EntityKind::Schema => {
            let name = String::from_utf8(id).expect("schema id is UTF-8");
            let schema: TableSchema =
                serde_json::from_slice(value).expect("mirrored schema value decodes");
            meta.schemas.insert(name, schema);
        }
        EntityKind::Policy => {
            let policy: PlacementPolicy =
                serde_json::from_slice(value).expect("mirrored policy value decodes");
            meta.policies.insert(TabletId(decode_u64(&id)), policy);
        }
        EntityKind::NodeAddrs => {
            let addrs: NodeAddrs =
                serde_json::from_slice(value).expect("mirrored node-addrs value decodes");
            meta.node_addrs.insert(decode_node_id(id), addrs);
        }
        EntityKind::Counter => {
            let value = decode_u64(value);
            if id == NEXT_TABLET_ID_COUNTER.as_bytes() {
                meta.next_tablet_id = value;
            }
        }
        EntityKind::CpMemberAddr => {
            let node = decode_node_id(id);
            let entry: CpMemberAddrEntry =
                serde_json::from_slice(value).expect("mirrored cp-member-addr value decodes");
            meta.cp_member_addrs.insert(node.clone(), entry.addr);
            if let Some(tablet) = entry.tablet {
                meta.cp_member_tablets.insert(node, tablet);
            }
        }
        EntityKind::StreamShard => {
            let Some(key) = syskv::decode_stream_shard_id(&id) else {
                return;
            };
            let row: crate::meta::StreamShardRow =
                serde_json::from_slice(value).expect("mirrored stream-shard value decodes");
            meta.stream_shards.insert(key, row);
        }
        EntityKind::IndexBackfill => {
            if let Some(key) = syskv::decode_index_backfill_id(&id) {
                meta.index_backfill.insert(key, ());
            }
        }
        EntityKind::SplitLineage => {
            let lineage: crate::meta::SplitLineage =
                serde_json::from_slice(value).expect("mirrored split-lineage value decodes");
            meta.split_lineage
                .insert(TabletId(decode_u64(&id)), lineage);
        }
        EntityKind::Backup => {
            let backup_id = String::from_utf8(id).expect("backup id is UTF-8");
            let row: crate::meta::BackupRow =
                serde_json::from_slice(value).expect("mirrored backup value decodes");
            meta.backups.insert(backup_id, row);
        }
        EntityKind::BackupProgress => {
            // Physical encoding is `(tablet, backup_id)` (fixed-width
            // field first, see `EntityKind::BackupProgress`'s own doc);
            // `Metadata::backup_tablet_progress`'s own map key is
            // `(backup_id, tablet)` — swapped here.
            if let Some((tablet, backup_id)) = syskv::decode_backup_progress_id(&id) {
                let progress: crate::meta::BackupTabletProgress =
                    serde_json::from_slice(value).expect("mirrored backup-progress value decodes");
                meta.backup_tablet_progress
                    .insert((backup_id, tablet), progress);
            }
        }
    }
}

/// The `Delete`/tombstone half of [`apply_key_write`] — reachable only via a
/// real delta (never via [`rebuild_metadata_from_engine`], whose `entries()`
/// scan never yields a tombstone).
fn apply_delete(meta: &mut Metadata, key: &[u8]) {
    let Some(DecodedKey::Entity { kind, id }) = syskv::decode_key(key) else {
        return;
    };
    match kind {
        EntityKind::Tablet => {
            meta.tablets.remove(&TabletId(decode_u64(&id)));
        }
        EntityKind::Member => {
            meta.members.remove(&decode_node_id(id));
        }
        EntityKind::Schema => {
            let name = String::from_utf8(id).expect("schema id is UTF-8");
            meta.schemas.remove(&name);
        }
        EntityKind::Policy => {
            meta.policies.remove(&TabletId(decode_u64(&id)));
        }
        EntityKind::NodeAddrs => {
            meta.node_addrs.remove(&decode_node_id(id));
        }
        EntityKind::Counter => {
            // Never deleted in practice (a monotonic counter is only ever
            // `Put`) — listed for match exhaustiveness.
        }
        EntityKind::CpMemberAddr => {
            let node = decode_node_id(id);
            meta.cp_member_addrs.remove(&node);
            meta.cp_member_tablets.remove(&node);
        }
        EntityKind::StreamShard => {
            // Reachable in practice, unlike the never-pruned markers above
            // — `ExpireStreamShards { remove: true }` genuinely tombstones
            // a row (retention/drop-table cascade, ADR 0043 §A9).
            if let Some(key) = syskv::decode_stream_shard_id(&id) {
                meta.stream_shards.remove(&key);
            }
        }
        EntityKind::IndexBackfill => {
            // Reachable in practice — `DropTableTablets`/`DropTableIndex`
            // both prune rows as a side effect of their own apply (ADR 0045).
            if let Some(key) = syskv::decode_index_backfill_id(&id) {
                meta.index_backfill.remove(&key);
            }
        }
        EntityKind::SplitLineage => {
            // Never deleted in practice (`split_lineage` is never pruned)
            // — listed for match exhaustiveness.
            meta.split_lineage.remove(&TabletId(decode_u64(&id)));
        }
        EntityKind::Backup => {
            // Reachable in practice — `DeleteBackup` tombstones a row
            // outright (ADR 0059 §3).
            let backup_id = String::from_utf8(id).expect("backup id is UTF-8");
            meta.backups.remove(&backup_id);
        }
        EntityKind::BackupProgress => {
            // Reachable in practice — `DeleteBackup` prunes every one of
            // its own progress rows (ADR 0059 §3).
            if let Some((tablet, backup_id)) = syskv::decode_backup_progress_id(&id) {
                meta.backup_tablet_progress.remove(&(backup_id, tablet));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use animus_placement::PlacementPolicy;
    use animus_storage::MemoryEngine;
    use animus_tablet::{Epoch, KeyRange};

    use super::*;
    use crate::meta::NodeStatus;
    use crate::schema::ColumnType;

    fn schema(pk: &str) -> TableSchema {
        TableSchema::simple(pk, ColumnType::String)
    }

    /// Every [`MetaCommand`] variant that applies cleanly produces the
    /// expected mirror writes, and a rejected/no-op command produces none.
    /// One assertion block per variant, in the same order as the enum.
    #[test]
    fn no_op_produces_no_writes() {
        let mut meta = Metadata::default();
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &MetaCommand::NoOp);
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
    }

    #[test]
    fn upsert_member_writes_the_member() {
        let mut meta = Metadata::default();
        let command = MetaCommand::UpsertMember {
            node: nid(1),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(syskv::member_key(&nid(1)), &meta.members[&nid(1)])]
        );
    }

    #[test]
    fn create_tablet_writes_the_tablet_and_bumps_the_counter() {
        let mut meta = Metadata::default();
        let command = MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: None,
            range: KeyRange::whole(),
            replicas: vec![nid(1), nid(2)],
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![
                put_json(syskv::tablet_key(TabletId(1)), &meta.tablets[&TabletId(1)]),
                put_counter(NEXT_TABLET_ID_COUNTER, meta.next_tablet_id),
            ]
        );
    }

    #[test]
    fn create_tablet_rejected_on_duplicate_produces_no_writes() {
        let mut meta = Metadata::default();
        let create = MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: None,
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        };
        let _ = apply_and_derive_mirror(&mut meta, &create);
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &create);
        assert_eq!(outcome, ApplyOutcome::Rejected("tablet already exists"));
        assert!(writes.is_empty());
    }

    #[test]
    fn cas_tablet_replicas_writes_the_tablet() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: None,
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let command = MetaCommand::CasTabletReplicas {
            tablet: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            replicas: vec![nid(1), nid(2)],
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::tablet_key(TabletId(1)),
                &meta.tablets[&TabletId(1)]
            )]
        );
    }

    #[test]
    fn set_tablet_policy_some_writes_none_deletes() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: None,
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let set = MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("p", 2)),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &set);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::policy_key(TabletId(1)),
                &meta.policies[&TabletId(1)]
            )]
        );

        let clear = MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: None,
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &clear);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![KeyWrite::Delete(syskv::policy_key(TabletId(1)))]
        );
    }

    #[test]
    fn create_table_schema_writes_the_schema() {
        let mut meta = Metadata::default();
        let command = MetaCommand::CreateTableSchema {
            table: "orders".to_string(),
            schema: schema("id"),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(syskv::schema_key("orders"), &schema("id"))]
        );
    }

    #[test]
    fn drop_table_schema_deletes_it_and_is_idempotent() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let drop = MetaCommand::DropTableSchema {
            table: "orders".to_string(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &drop);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(writes, vec![KeyWrite::Delete(syskv::schema_key("orders"))]);

        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &drop);
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
    }

    #[test]
    fn replace_table_schema_writes_the_new_schema() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let mut replacement = schema("id");
        replacement.upsert_index(crate::schema::IndexDef {
            name: "by_x".to_string(),
            kind: crate::schema::IndexKind::Global,
            hash_attribute: "id".to_string(),
            sort_attribute: None,
            projection: crate::schema::IndexProjection::All,
            status: crate::schema::IndexStatus::Active,
        });
        let command = MetaCommand::ReplaceTableSchema {
            table: "orders".to_string(),
            schema: replacement.clone(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(syskv::schema_key("orders"), &replacement)]
        );
    }

    #[test]
    fn drop_table_tablets_deletes_every_tablet_policy_and_prunes_cp_member_addrs() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("t".to_string()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("p", 1)),
            },
        );
        meta.cp_member_addrs.insert(nid(7), "addr".to_string());
        meta.cp_member_tablets.insert(nid(7), TabletId(1));

        let command = MetaCommand::DropTableTablets {
            table: "t".to_string(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![
                KeyWrite::Delete(syskv::tablet_key(TabletId(1))),
                KeyWrite::Delete(syskv::policy_key(TabletId(1))),
                KeyWrite::Delete(syskv::cp_member_addr_key(&nid(7))),
            ]
        );

        // Idempotent: nothing left to drop.
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
    }

    #[test]
    fn create_and_drop_table_index_write_the_updated_schema() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let index = crate::schema::IndexDef {
            name: "by_x".to_string(),
            kind: crate::schema::IndexKind::Global,
            hash_attribute: "id".to_string(),
            sort_attribute: None,
            projection: crate::schema::IndexProjection::All,
            status: crate::schema::IndexStatus::Active,
        };
        let create = MetaCommand::CreateTableIndex {
            table: "orders".to_string(),
            index: index.clone(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &create);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::schema_key("orders"),
                meta.schemas.get("orders").unwrap()
            )]
        );

        let drop = MetaCommand::DropTableIndex {
            table: "orders".to_string(),
            index: "by_x".to_string(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &drop);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::schema_key("orders"),
                meta.schemas.get("orders").unwrap()
            )]
        );
    }

    /// A fixture for the `index_backfill` derivation tests below: a table
    /// with one `Creating` GSI and one tablet scoped to it, driven through
    /// `apply_and_derive_mirror` (not bare `Metadata::apply`) so the mirror
    /// writes each setup step derives are exercised too, even though this
    /// fixture itself doesn't assert on them.
    fn mirror_table_with_index_and_tablet(
        meta: &mut Metadata,
        table: &str,
        index: &str,
        tablet: TabletId,
    ) {
        let _ = apply_and_derive_mirror(
            meta,
            &MetaCommand::CreateTableSchema {
                table: table.to_string(),
                schema: schema("id"),
            },
        );
        let _ = apply_and_derive_mirror(
            meta,
            &MetaCommand::CreateTableIndex {
                table: table.to_string(),
                index: crate::schema::IndexDef {
                    name: index.to_string(),
                    kind: crate::schema::IndexKind::Global,
                    hash_attribute: "id".to_string(),
                    sort_attribute: None,
                    projection: crate::schema::IndexProjection::All,
                    status: crate::schema::IndexStatus::Creating,
                },
            },
        );
        let _ = apply_and_derive_mirror(
            meta,
            &MetaCommand::CreateTablet {
                tablet,
                table: Some(table.to_string()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
    }

    /// `MarkIndexBackfilled` (ADR 0045 §4) mirrors as a single empty `Put`
    /// at the row's `index_backfill_key`, and a repeat proposal is a `NoOp`
    /// that derives no further writes.
    #[test]
    fn mark_index_backfilled_writes_the_row_and_is_idempotent() {
        let mut meta = Metadata::default();
        mirror_table_with_index_and_tablet(&mut meta, "users", "by_email", TabletId(1));

        let command = MetaCommand::MarkIndexBackfilled {
            table: "users".to_string(),
            index: "by_email".to_string(),
            tablet: TabletId(1),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![KeyWrite::Put(
                syskv::index_backfill_key(TabletId(1), "by_email"),
                Vec::new()
            )]
        );

        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
    }

    /// `DropTableTablets` (ADR 0045) mirrors its `index_backfill` prune as a
    /// `Delete` per pruned row, alongside its existing tablet/policy
    /// deletes — a same-named index's row on a different, undropped table
    /// must not appear among the derived deletes.
    #[test]
    fn drop_table_tablets_also_deletes_index_backfill_rows() {
        let mut meta = Metadata::default();
        mirror_table_with_index_and_tablet(&mut meta, "users", "by_email", TabletId(1));
        mirror_table_with_index_and_tablet(&mut meta, "orders", "by_email", TabletId(2));
        for (table, tablet) in [("users", TabletId(1)), ("orders", TabletId(2))] {
            let (outcome, _) = apply_and_derive_mirror(
                &mut meta,
                &MetaCommand::MarkIndexBackfilled {
                    table: table.to_string(),
                    index: "by_email".to_string(),
                    tablet,
                },
            );
            assert_eq!(outcome, ApplyOutcome::Applied);
        }

        let command = MetaCommand::DropTableTablets {
            table: "users".to_string(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert!(
            writes.contains(&KeyWrite::Delete(syskv::index_backfill_key(
                TabletId(1),
                "by_email"
            ))),
            "the dropped table's row must be among the derived deletes: {writes:?}"
        );
        assert!(
            !writes.contains(&KeyWrite::Delete(syskv::index_backfill_key(
                TabletId(2),
                "by_email"
            ))),
            "the other table's same-named-index row must be untouched: {writes:?}"
        );
    }

    /// `DropTableIndex` (ADR 0045) mirrors its `index_backfill` prune as a
    /// `Delete` per pruned row, alongside the re-serialized schema — scoped
    /// to the owning table's own tablets, so a distinct table's row for a
    /// same-named index is untouched.
    #[test]
    fn drop_table_index_also_deletes_index_backfill_rows() {
        let mut meta = Metadata::default();
        mirror_table_with_index_and_tablet(&mut meta, "users", "by_email", TabletId(1));
        mirror_table_with_index_and_tablet(&mut meta, "orders", "by_email", TabletId(2));
        for (table, tablet) in [("users", TabletId(1)), ("orders", TabletId(2))] {
            let (outcome, _) = apply_and_derive_mirror(
                &mut meta,
                &MetaCommand::MarkIndexBackfilled {
                    table: table.to_string(),
                    index: "by_email".to_string(),
                    tablet,
                },
            );
            assert_eq!(outcome, ApplyOutcome::Applied);
        }

        let command = MetaCommand::DropTableIndex {
            table: "users".to_string(),
            index: "by_email".to_string(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert!(
            writes.contains(&put_json(
                syskv::schema_key("users"),
                meta.schemas.get("users").unwrap()
            )),
            "the updated schema must still be re-serialized: {writes:?}"
        );
        assert!(
            writes.contains(&KeyWrite::Delete(syskv::index_backfill_key(
                TabletId(1),
                "by_email"
            ))),
            "the dropped index's row must be among the derived deletes: {writes:?}"
        );
        assert!(
            !writes.contains(&KeyWrite::Delete(syskv::index_backfill_key(
                TabletId(2),
                "by_email"
            ))),
            "the other table's same-named index must be untouched: {writes:?}"
        );
    }

    /// ADR 0051: TTL is part of the table's schema entry, so it mirrors
    /// identically to an index change — the whole (already-mutated) schema,
    /// re-serialized under the same `schema_key`.
    #[test]
    fn set_table_ttl_writes_the_updated_schema() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let command = MetaCommand::SetTableTtl {
            table: "orders".to_string(),
            spec: Some(crate::schema::TtlSpec {
                attribute_name: "expiresAt".to_string(),
            }),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::schema_key("orders"),
                meta.schemas.get("orders").unwrap()
            )]
        );
    }

    #[test]
    fn register_cp_addr_writes_the_combined_entry() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: None,
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let command = MetaCommand::RegisterCpAddr {
            id: nid(5),
            addr: "127.0.0.1:9".to_string(),
            tablet: Some(TabletId(1)),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::cp_member_addr_key(&nid(5)),
                &CpMemberAddrEntry {
                    addr: "127.0.0.1:9".to_string(),
                    tablet: Some(TabletId(1)),
                }
            )]
        );
    }

    #[test]
    fn register_node_addrs_writes_the_address_book_entry() {
        let mut meta = Metadata::default();
        // ADR 0040 PR4: `RegisterNodeAddrs` is update-only — establish the
        // claim first (standing in for a config-bootstrapped member).
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::UpsertMember {
                node: nid(3),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            },
        );
        let addrs = NodeAddrs {
            internal: "a".to_string(),
            client: "b".to_string(),
            admin: "c".to_string(),
            intra: "d".to_string(),
            role: "combined".to_string(),
        };
        let command = MetaCommand::RegisterNodeAddrs {
            node: nid(3),
            addrs: addrs.clone(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(syskv::node_addrs_key(&nid(3)), &addrs)]
        );
    }

    #[test]
    fn remove_member_deletes_every_address_book_entry() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::UpsertMember {
                node: nid(4),
                labels: BTreeMap::new(),
                status: NodeStatus::Leaving,
            },
        );
        let command = MetaCommand::RemoveMember { node: nid(4) };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![
                KeyWrite::Delete(syskv::member_key(&nid(4))),
                KeyWrite::Delete(syskv::node_addrs_key(&nid(4))),
                KeyWrite::Delete(syskv::cp_member_addr_key(&nid(4))),
            ]
        );
    }

    #[test]
    fn register_node_writes_member_and_addrs_atomically() {
        let mut meta = Metadata::default();
        let addrs = NodeAddrs {
            internal: "127.0.0.1:9910".to_string(),
            client: "127.0.0.1:9010".to_string(),
            admin: "127.0.0.1:9510".to_string(),
            intra: "127.0.0.1:9610".to_string(),
            role: "combined".to_string(),
        };
        let command = MetaCommand::RegisterNode {
            node: nid(910),
            addrs: addrs.clone(),
            labels: BTreeMap::new(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![
                put_json(syskv::member_key(&nid(910)), &meta.members[&nid(910)]),
                put_json(syskv::node_addrs_key(&nid(910)), &addrs),
            ]
        );

        // Idempotent replay of an identical registration mints nothing new.
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
    }

    fn stream_spec(label: &str) -> crate::schema::StreamSpec {
        crate::schema::StreamSpec {
            view_type: crate::schema::StreamViewType::NewAndOldImages,
            label: label.to_string(),
        }
    }

    fn seal_cmd(tablet: TabletId, epoch: u64, end: u64) -> MetaCommand {
        MetaCommand::SealStreamShard {
            table: "orders".to_string(),
            label: "L1".to_string(),
            tablet,
            epoch,
            view_type: crate::schema::StreamViewType::NewAndOldImages,
            hlc_range: (end.saturating_sub(100), end),
            count: 1,
            seal_wall_ms: 1_700_000_000_000,
            replicas: vec![nid(1), nid(2)],
            object_id: format!("orders/L1/{}/{epoch}/test", tablet.0),
        }
    }

    /// `SealStreamShard` mirrors as one `Put` of the freshly-inserted row
    /// (ADR 0042 §3/ADR 0043 §A8) — and a first-committer-loses no-op
    /// derives no writes, mirroring every other rejected/no-op command.
    #[test]
    fn seal_stream_shard_writes_the_row() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::SetTableStream {
                table: "orders".to_string(),
                spec: Some(stream_spec("L1")),
            },
        );
        let command = seal_cmd(TabletId(1), 0, 100);
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::stream_shard_key(TabletId(1), 0),
                &meta.stream_shards[&(TabletId(1), 0)]
            )]
        );

        // A second, first-committer-loses proposal for the same identity is
        // a no-op — no writes derived, the row untouched.
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &seal_cmd(TabletId(1), 0, 999));
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
    }

    /// `ExpireStreamShards`'s two derived shapes: marking a still-present
    /// row mirrors as an updated `Put` (the row, now `expired: true`);
    /// removing it mirrors as a `Delete`.
    #[test]
    fn expire_stream_shards_marks_as_put_and_removes_as_delete() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::SetTableStream {
                table: "orders".to_string(),
                spec: Some(stream_spec("L1")),
            },
        );
        let _ = apply_and_derive_mirror(&mut meta, &seal_cmd(TabletId(1), 0, 100));

        let mark = MetaCommand::ExpireStreamShards {
            rows: vec![(TabletId(1), 0)],
            remove: false,
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &mark);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::stream_shard_key(TabletId(1), 0),
                &meta.stream_shards[&(TabletId(1), 0)]
            )]
        );
        assert!(meta.stream_shards[&(TabletId(1), 0)].expired);

        let remove = MetaCommand::ExpireStreamShards {
            rows: vec![(TabletId(1), 0)],
            remove: true,
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &remove);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![KeyWrite::Delete(syskv::stream_shard_key(TabletId(1), 0))]
        );
        assert!(!meta.stream_shards.contains_key(&(TabletId(1), 0)));
    }

    /// `BeginBackup` (ADR 0059 §3) mirrors as one `Put` of the freshly-minted
    /// row, and a duplicate-id rejection derives no writes.
    #[test]
    fn begin_backup_writes_the_row() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_string()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let command = MetaCommand::BeginBackup {
            backup_id: "backup-1".to_string(),
            table: "orders".to_string(),
            created_wall_ms: 1000,
            backup_name: "backup".to_string(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::backup_key("backup-1"),
                &meta.backups["backup-1"]
            )]
        );

        // A duplicate id is rejected outright — no writes derived.
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Rejected("backup id already exists"));
        assert!(writes.is_empty());
    }

    /// `RecordBackupTabletComplete` (ADR 0059 §3/§4) mirrors as one `Put` at
    /// the record's own `backup_progress_key`, and an identical repeat is a
    /// `NoOp` that derives no further writes.
    #[test]
    fn record_backup_tablet_complete_writes_the_row() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_string()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::BeginBackup {
                backup_id: "backup-1".to_string(),
                table: "orders".to_string(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
            },
        );

        let command = MetaCommand::RecordBackupTabletComplete {
            backup_id: "backup-1".to_string(),
            tablet: TabletId(1),
            cut_version: 10,
            bytes: 100,
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::backup_progress_key("backup-1", TabletId(1)),
                &meta.backup_tablet_progress[&("backup-1".to_string(), TabletId(1))]
            )]
        );

        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
    }

    /// `CompleteBackup`/`FailBackup` (ADR 0059 §3/§4) both mirror as one
    /// `Put` of the row's now-updated status.
    #[test]
    fn complete_and_fail_backup_write_the_updated_row() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_string()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::BeginBackup {
                backup_id: "backup-1".to_string(),
                table: "orders".to_string(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_string(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            },
        );

        let (outcome, writes) = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_string(),
            },
        );
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::backup_key("backup-1"),
                &meta.backups["backup-1"]
            )]
        );

        let mut failed_meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut failed_meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut failed_meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_string()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let _ = apply_and_derive_mirror(
            &mut failed_meta,
            &MetaCommand::BeginBackup {
                backup_id: "backup-1".to_string(),
                table: "orders".to_string(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
            },
        );
        let (outcome, writes) = apply_and_derive_mirror(
            &mut failed_meta,
            &MetaCommand::FailBackup {
                backup_id: "backup-1".to_string(),
                reason: "timeout".to_string(),
            },
        );
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::backup_key("backup-1"),
                &failed_meta.backups["backup-1"]
            )]
        );
    }

    /// `DeleteBackup` (ADR 0059 §3) mirrors as a `Delete` of the row itself
    /// plus a `Delete` per pruned progress record.
    #[test]
    fn delete_backup_deletes_the_row_and_its_progress_records() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_string()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::BeginBackup {
                backup_id: "backup-1".to_string(),
                table: "orders".to_string(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_string(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            },
        );

        let (outcome, writes) = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::DeleteBackup {
                backup_id: "backup-1".to_string(),
            },
        );
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![
                KeyWrite::Delete(syskv::backup_key("backup-1")),
                KeyWrite::Delete(syskv::backup_progress_key("backup-1", TabletId(1))),
            ]
        );

        // Idempotent: an already-deleted id derives no writes.
        let (outcome, writes) = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::DeleteBackup {
                backup_id: "backup-1".to_string(),
            },
        );
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
    }

    /// `MarkBackupDeleted` (ADR 0059 §3, Train 1 PR④) mirrors as one `Put`
    /// of the row's now-`Expired` status — the row itself is untouched
    /// (still present, `DeleteBackup` is the removal command).
    #[test]
    fn mark_backup_deleted_writes_the_updated_row() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_string()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::BeginBackup {
                backup_id: "backup-1".to_string(),
                table: "orders".to_string(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_string(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_string(),
            },
        );

        let (outcome, writes) = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::MarkBackupDeleted {
                backup_id: "backup-1".to_string(),
            },
        );
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::backup_key("backup-1"),
                &meta.backups["backup-1"]
            )]
        );
        assert!(meta.backup("backup-1").is_some(), "the row itself survives");

        // Idempotent once `Expired`.
        let (outcome, writes) = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::MarkBackupDeleted {
                backup_id: "backup-1".to_string(),
            },
        );
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
    }

    /// The incremental-delta consumer path (`apply_key_write`) for
    /// `DeleteBackup`'s derived deletes reaches the identical state a direct
    /// `Metadata::apply` does — exercises `apply_delete`'s `Backup`/
    /// `BackupProgress` arms, which a live engine scan (only ever `Put`s)
    /// never reaches.
    #[test]
    fn incremental_delta_apply_matches_direct_apply_for_deleted_backups() {
        let mut base = Metadata::default();
        base.apply(&MetaCommand::CreateTableSchema {
            table: "orders".to_string(),
            schema: schema("id"),
        });
        base.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_string()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        base.apply(&MetaCommand::BeginBackup {
            backup_id: "backup-1".to_string(),
            table: "orders".to_string(),
            created_wall_ms: 1000,
            backup_name: "backup".to_string(),
        });
        base.apply(&MetaCommand::RecordBackupTabletComplete {
            backup_id: "backup-1".to_string(),
            tablet: TabletId(1),
            cut_version: 10,
            bytes: 100,
        });

        let command = MetaCommand::DeleteBackup {
            backup_id: "backup-1".to_string(),
        };

        let mut shadow = base.clone();
        let (outcome, writes) = apply_and_derive_mirror(&mut shadow, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(writes.len(), 2, "the row and its one progress record");

        let mut direct = base.clone();
        direct.apply(&command);

        let mut mirror_side = base.clone();
        for w in &writes {
            apply_key_write(&mut mirror_side, w);
        }

        assert_eq!(shadow, direct);
        assert_eq!(mirror_side, direct);
        assert!(direct.backups.is_empty());
        assert!(direct.backup_tablet_progress.is_empty());
    }

    /// The read side: a `Metadata` rebuilt from a fresh [`MemoryEngine`] after
    /// mirroring a handful of commands equals a `Metadata` built by applying
    /// those same commands directly (no mirror involved) — the differential
    /// oracle at unit scale (`tests/mirror_engine.rs` sim-sweeps this).
    #[tokio::test]
    async fn rebuild_from_engine_matches_direct_apply() {
        use animus_storage::MergeOp;

        let engine = MemoryEngine::new();
        let mut shadow = Metadata::default();
        let mut direct = Metadata::default();
        let commands = [
            MetaCommand::UpsertMember {
                node: nid(1),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            },
            MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("t".to_string()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            },
            MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("p", 1)),
            },
            MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
            // ADR 0059 §3: exercise both new `EntityKind`s' read side too —
            // a `Backup` row (`table` here is data, so it can name any
            // table; "t" was already given a schema-less tablet above,
            // which `BeginBackup` doesn't require) and a `BackupProgress`
            // record.
            MetaCommand::CreateTableSchema {
                table: "t".to_string(),
                schema: schema("id"),
            },
            MetaCommand::BeginBackup {
                backup_id: "backup-1".to_string(),
                table: "t".to_string(),
                created_wall_ms: 1_000,
                backup_name: "backup".to_string(),
            },
            MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_string(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            },
            MetaCommand::RegisterNode {
                node: nid(2),
                addrs: NodeAddrs {
                    internal: "127.0.0.1:9902".to_string(),
                    client: "127.0.0.1:9002".to_string(),
                    admin: "127.0.0.1:9502".to_string(),
                    intra: "127.0.0.1:9602".to_string(),
                    role: "combined".to_string(),
                },
                labels: BTreeMap::new(),
            },
            MetaCommand::SetTableStream {
                table: "orders".to_string(),
                spec: Some(stream_spec("L1")),
            },
            seal_cmd(TabletId(1), 0, 100),
            MetaCommand::ExpireStreamShards {
                rows: vec![(TabletId(1), 0)],
                remove: false,
            },
            // ADR 0050: exercise the `split_lineage` mirror's read side too
            // — a live engine scan only ever yields `Put`s, so cutover's
            // lineage row is a value `rebuild_metadata_from_engine` has to
            // decode.
            MetaCommand::BeginSplit {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: vec![5],
                children: [(TabletId(3), vec![nid(1)]), (TabletId(4), vec![nid(1)])],
            },
            MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL.next(),
                cutover_wall_ms: 1_000,
            },
        ];
        for (index, command) in commands.iter().enumerate() {
            let version = index as u64 + 1;
            let (_, writes) = apply_and_derive_mirror(&mut shadow, command);
            let mut ops = Vec::new();
            for w in writes {
                ops.push(match w {
                    KeyWrite::Put(k, v) => MergeOp::put(k, v, version),
                    KeyWrite::Delete(k) => MergeOp::tombstone(k, version),
                });
            }
            engine.merge_batch(ops).await.expect("merge batch");
            direct.apply(command);
        }

        let rebuilt = rebuild_metadata_from_engine(&engine)
            .await
            .expect("rebuild");
        assert_eq!(rebuilt, direct);
        assert_eq!(rebuilt, shadow);
    }

    /// The incremental-delta consumer path (ADR 0038 PR5, `apply_key_write`):
    /// applying a command's derived `KeyWrite`s directly onto a `Metadata` —
    /// no engine, no [`rebuild_metadata_from_engine`] bulk scan, exactly what
    /// a `WatchMetadata` delta reply's consumer
    /// (`animusd::control_handle::RemoteControlClient::observe_delta`) does —
    /// reaches the identical state a direct `Metadata::apply` does. Uses
    /// `DropTableTablets` specifically because it derives `Delete`s, the half
    /// [`rebuild_from_engine_matches_direct_apply`] above never exercises
    /// (a live engine scan never yields a tombstone).
    #[test]
    fn incremental_delta_apply_matches_direct_apply_for_deletes() {
        let mut base = Metadata::default();
        base.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("t".to_string()),
            range: KeyRange::whole().split_at(&[5]).unwrap().0,
            replicas: vec![nid(1)],
        });
        let right_range = KeyRange::whole().split_at(&[5]).unwrap().1;
        base.tablets.insert(
            TabletId(2),
            Tablet::with_table(
                TabletId(2),
                Some("t".to_string()),
                right_range,
                vec![nid(1)],
            ),
        );
        base.cp_member_addrs.insert(nid(99), "addr:1".to_string());
        base.cp_member_tablets.insert(nid(99), TabletId(2));

        let command = MetaCommand::DropTableTablets {
            table: "t".to_string(),
        };

        let mut shadow = base.clone();
        let (outcome, writes) = apply_and_derive_mirror(&mut shadow, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert!(
            writes.iter().any(|w| matches!(w, KeyWrite::Delete(_))),
            "expected DropTableTablets to derive at least one delete"
        );

        let mut direct = base.clone();
        direct.apply(&command);

        let mut mirror_side = base.clone();
        for w in &writes {
            apply_key_write(&mut mirror_side, w);
        }

        assert_eq!(
            shadow, direct,
            "the apply task's own shadow matches direct apply"
        );
        assert_eq!(
            mirror_side, direct,
            "a delta-consumer's incremental apply matches direct apply"
        );
    }

    /// `apply_key_write` ignores a key it doesn't recognize (an undecodable
    /// key, or the `_applied_index` watermark — not part of `Metadata`)
    /// rather than panicking, mirroring `rebuild_metadata_from_engine`'s
    /// defensive treatment.
    #[test]
    fn apply_key_write_ignores_unrecognized_keys() {
        let mut meta = Metadata::default();
        let before = meta.clone();
        apply_key_write(
            &mut meta,
            &KeyWrite::Put(b"not a system key".to_vec(), b"value".to_vec()),
        );
        apply_key_write(
            &mut meta,
            &KeyWrite::Put(syskv::applied_index_key(), 5u64.to_be_bytes().to_vec()),
        );
        apply_key_write(
            &mut meta,
            &KeyWrite::Delete(b"not a system key either".to_vec()),
        );
        assert_eq!(meta, before);
    }
}
