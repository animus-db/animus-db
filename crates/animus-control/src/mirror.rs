//! The control plane's **shadow-mode system-keyspace mirror** (ADR 0038 PR2):
//! given a just-applied [`MetaCommand`] and the [`Metadata`] it was applied
//! against, derive the bounded set of [`syskv`] key/value writes that command
//! implies, and (separately) rebuild a [`Metadata`] back out of a
//! `StorageEngine`'s system keyspace. Both directions are exercised by the
//! differential-oracle tests (`tests/mirror_engine.rs`): drive a real
//! `Metadata`/`RaftCore` and a mirror side by side and assert they agree.
//!
//! This module is **pure** (no `Env`, no I/O) except [`rebuild_metadata_from_engine`],
//! which only *reads* a [`StorageEngine`] — the actual write path (deriving
//! writes here, then `merge_batch`-ing them into an engine) is driven by
//! `node.rs`'s `RaftNode::start_with_mirror`/`mirror_loop`.
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
//! - Both [`MetaCommand::MergeTablets`] and `DropTableTablets` also prune the
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
//! `RegisterCpAddr` and the two monotonic id-allocator counters
//! (`next_tablet_id`/`next_alloc_id`) and the `AllocateNodeId` idempotency
//! ledger (`node_id_allocations`) — none of these were in PR1's
//! [`EntityKind`] set, so this module's own PR2 changes to `syskv.rs` added
//! [`syskv::EntityKind::Counter`]/[`syskv::EntityKind::CpMemberAddr`]/
//! [`syskv::EntityKind::NodeIdAlloc`]. The payoff: [`rebuild_metadata_from_engine`]
//! produces a `Metadata` that is `PartialEq`-identical to the real in-core
//! one, not "identical modulo a documented gap" — which is exactly what the
//! differential-oracle test asserts.

use std::collections::BTreeMap;

use animus_env::NodeId;
use animus_placement::PlacementPolicy;
use animus_storage::{StorageEngine, StorageError};
use animus_tablet::{Tablet, TabletId};
use serde::{Deserialize, Serialize};

use crate::meta::{ApplyOutcome, Member, MetaCommand, Metadata, NodeAddrs};
use crate::schema::TableSchema;
use crate::syskv::{self, DecodedKey, EntityKind};

/// The counter name for `Metadata::next_tablet_id` under
/// [`EntityKind::Counter`] (`syskv::counter_key`).
pub const NEXT_TABLET_ID_COUNTER: &str = "next_tablet_id";
/// The counter name for `Metadata::next_alloc_id` under
/// [`EntityKind::Counter`] (`syskv::counter_key`).
pub const NEXT_ALLOC_ID_COUNTER: &str = "next_alloc_id";

/// One system-keyspace mutation an applied [`MetaCommand`] implies. The
/// mirror loop (`node.rs`) translates these into per-key-LWW
/// [`animus_storage::MergeOp`]s versioned at the command's own Raft log
/// index — never [`StorageEngine::write_batch`]/`put` (which enforce a
/// single engine-wide monotonic version), since a **combined** node's mirror
/// shares its engine with the CP data plane's own, independently-versioned
/// writes (see the PR2 report's inline-vs-offloaded write-path note).
#[derive(Clone, Debug, PartialEq, Eq)]
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
    // `DropTableTablets`/`MergeTablets`. Cheap/no-op for every other command.
    let dropped_tablets: Vec<TabletId> = match command {
        MetaCommand::DropTableTablets { table } => {
            meta.tablets_for_table(table).map(|(&id, _)| id).collect()
        }
        _ => Vec::new(),
    };
    let pre_cp_member_tablets: BTreeMap<NodeId, TabletId> = match command {
        MetaCommand::MergeTablets { .. } | MetaCommand::DropTableTablets { .. } => {
            meta.cp_member_tablets.clone()
        }
        _ => BTreeMap::new(),
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
            writes.push(put_json(syskv::member_key(*node), &meta.members[node]));
        }
        MetaCommand::CreateTablet { tablet, .. } => {
            writes.push(put_json(syskv::tablet_key(*tablet), &meta.tablets[tablet]));
            writes.push(put_counter(NEXT_TABLET_ID_COUNTER, meta.next_tablet_id));
        }
        MetaCommand::CasTabletReplicas { tablet, .. } => {
            writes.push(put_json(syskv::tablet_key(*tablet), &meta.tablets[tablet]));
        }
        MetaCommand::SplitTablet { tablet, new_id, .. } => {
            writes.push(put_json(syskv::tablet_key(*tablet), &meta.tablets[tablet]));
            writes.push(put_json(syskv::tablet_key(*new_id), &meta.tablets[new_id]));
            writes.push(put_counter(NEXT_TABLET_ID_COUNTER, meta.next_tablet_id));
            if let Some(policy) = meta.policies.get(new_id) {
                writes.push(put_json(syskv::policy_key(*new_id), policy));
            }
        }
        MetaCommand::MergeTablets { left, right, .. } => {
            writes.push(put_json(syskv::tablet_key(*left), &meta.tablets[left]));
            writes.push(KeyWrite::Delete(syskv::tablet_key(*right)));
            writes.push(KeyWrite::Delete(syskv::policy_key(*right)));
            writes.push(KeyWrite::Put(syskv::merged_key(*right), Vec::new()));
            for id in dead_cp_member_ids(&pre_cp_member_tablets, meta) {
                writes.push(KeyWrite::Delete(syskv::cp_member_addr_key(id)));
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
            for id in dead_cp_member_ids(&pre_cp_member_tablets, meta) {
                writes.push(KeyWrite::Delete(syskv::cp_member_addr_key(id)));
            }
        }
        MetaCommand::CreateTableIndex { table, .. }
        | MetaCommand::DropTableIndex { table, .. }
        | MetaCommand::SetTableMode { table, .. } => {
            if let Some(schema) = meta.schemas.get(table) {
                writes.push(put_json(syskv::schema_key(table), schema));
            }
        }
        MetaCommand::CreateKeyspace { keyspace } => {
            writes.push(KeyWrite::Put(syskv::keyspace_key(keyspace), Vec::new()));
        }
        MetaCommand::DropKeyspace { keyspace } => {
            writes.push(KeyWrite::Delete(syskv::keyspace_key(keyspace)));
        }
        MetaCommand::RegisterCpAddr { id, addr, tablet } => {
            let entry = CpMemberAddrEntry {
                addr: addr.clone(),
                tablet: *tablet,
            };
            writes.push(put_json(syskv::cp_member_addr_key(*id), &entry));
        }
        MetaCommand::RegisterNodeAddrs { node, addrs } => {
            writes.push(put_json(syskv::node_addrs_key(*node), addrs));
        }
        MetaCommand::RemoveMember { node } => {
            writes.push(KeyWrite::Delete(syskv::member_key(*node)));
            writes.push(KeyWrite::Delete(syskv::node_addrs_key(*node)));
            writes.push(KeyWrite::Delete(syskv::cp_member_addr_key(*node)));
        }
        MetaCommand::AllocateNodeId { nonce, .. } => {
            let node_id = meta.node_id_allocations[nonce];
            writes.push(put_json(
                syskv::member_key(node_id),
                &meta.members[&node_id],
            ));
            writes.push(KeyWrite::Put(
                syskv::node_id_alloc_key(nonce),
                node_id.to_be_bytes().to_vec(),
            ));
            writes.push(put_counter(NEXT_ALLOC_ID_COUNTER, meta.next_alloc_id));
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
        .map(|(&id, _)| id)
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

/// Rebuild a [`Metadata`] purely from a [`StorageEngine`]'s live system
/// keyspace — the mirror's read side, used by (a) the differential-oracle
/// test (assert this equals the real in-core `Metadata` at the same index),
/// (b) a restart's shadow-cache rebuild (`node.rs`'s `mirror_loop`), and (c)
/// a future PR3 apply task's own cache-rebuild-on-restart step.
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
        let Some(decoded) = syskv::decode_key(&key) else {
            continue;
        };
        let DecodedKey::Entity { kind, id } = decoded else {
            // `DecodedKey::AppliedIndex` — the watermark isn't part of
            // `Metadata`; the mirror loop reads it separately.
            continue;
        };
        match kind {
            EntityKind::Tablet => {
                let tablet: Tablet = serde_json::from_slice(&versioned.value)
                    .expect("mirrored tablet value decodes");
                meta.tablets.insert(TabletId(decode_u64(&id)), tablet);
            }
            EntityKind::Member => {
                let member: Member = serde_json::from_slice(&versioned.value)
                    .expect("mirrored member value decodes");
                meta.members.insert(decode_u64(&id), member);
            }
            EntityKind::Schema => {
                let name = String::from_utf8(id).expect("schema id is UTF-8");
                let schema: TableSchema = serde_json::from_slice(&versioned.value)
                    .expect("mirrored schema value decodes");
                meta.schemas.insert(name, schema);
            }
            EntityKind::Policy => {
                let policy: PlacementPolicy = serde_json::from_slice(&versioned.value)
                    .expect("mirrored policy value decodes");
                meta.policies.insert(TabletId(decode_u64(&id)), policy);
            }
            EntityKind::NodeAddrs => {
                let addrs: NodeAddrs = serde_json::from_slice(&versioned.value)
                    .expect("mirrored node-addrs value decodes");
                meta.node_addrs.insert(decode_u64(&id), addrs);
            }
            EntityKind::Keyspace => {
                let name = String::from_utf8(id).expect("keyspace id is UTF-8");
                meta.keyspaces.insert(name);
            }
            EntityKind::Merged => {
                meta.merged_tablets.insert(TabletId(decode_u64(&id)));
            }
            EntityKind::Counter => {
                let value = decode_u64(&versioned.value);
                if id == NEXT_TABLET_ID_COUNTER.as_bytes() {
                    meta.next_tablet_id = value;
                } else if id == NEXT_ALLOC_ID_COUNTER.as_bytes() {
                    meta.next_alloc_id = value;
                }
            }
            EntityKind::CpMemberAddr => {
                let node = decode_u64(&id);
                let entry: CpMemberAddrEntry = serde_json::from_slice(&versioned.value)
                    .expect("mirrored cp-member-addr value decodes");
                meta.cp_member_addrs.insert(node, entry.addr);
                if let Some(tablet) = entry.tablet {
                    meta.cp_member_tablets.insert(node, tablet);
                }
            }
            EntityKind::NodeIdAlloc => {
                let nonce = String::from_utf8(id).expect("node-id-alloc nonce is UTF-8");
                meta.node_id_allocations
                    .insert(nonce, decode_u64(&versioned.value));
            }
        }
    }
    Ok(meta)
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
            node: 1,
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(syskv::member_key(1), &meta.members[&1])]
        );
    }

    #[test]
    fn create_tablet_writes_the_tablet_and_bumps_the_counter() {
        let mut meta = Metadata::default();
        let command = MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: None,
            range: KeyRange::whole(),
            replicas: vec![1, 2],
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
            replicas: vec![1],
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
                replicas: vec![1],
            },
        );
        let command = MetaCommand::CasTabletReplicas {
            tablet: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            replicas: vec![1, 2],
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
    fn split_tablet_writes_both_tablets_the_counter_and_inherited_policy() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: None,
                range: KeyRange::whole(),
                replicas: vec![1],
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("p", 1)),
            },
        );
        let command = MetaCommand::SplitTablet {
            tablet: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: vec![5],
            new_id: TabletId(2),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![
                put_json(syskv::tablet_key(TabletId(1)), &meta.tablets[&TabletId(1)]),
                put_json(syskv::tablet_key(TabletId(2)), &meta.tablets[&TabletId(2)]),
                put_counter(NEXT_TABLET_ID_COUNTER, meta.next_tablet_id),
                put_json(syskv::policy_key(TabletId(2)), &meta.policies[&TabletId(2)]),
            ]
        );
    }

    /// `MergeTablets` also prunes any legacy `cp_member_addrs`/`cp_member_tablets`
    /// entry registered against the absorbed tablet — the pre/post-diff case
    /// this module's doc explains.
    #[test]
    fn merge_tablets_removes_the_right_tablet_and_prunes_its_cp_member_addr() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("t".to_string()),
                range: KeyRange::whole().split_at(&[5]).unwrap().0,
                replicas: vec![1],
            },
        );
        // Directly seed the second tablet + the legacy address-book entry
        // (there's no public command that creates an adjacent second tablet
        // without a split; construct the merge scenario by hand instead).
        let right_range = KeyRange::whole().split_at(&[5]).unwrap().1;
        meta.tablets.insert(
            TabletId(2),
            Tablet::with_table(TabletId(2), Some("t".to_string()), right_range, vec![1]),
        );
        meta.cp_member_addrs.insert(99, "addr:1".to_string());
        meta.cp_member_tablets.insert(99, TabletId(2));

        let command = MetaCommand::MergeTablets {
            left: TabletId(1),
            expected_left_epoch: Epoch::INITIAL,
            right: TabletId(2),
            expected_right_epoch: Epoch::INITIAL,
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![
                put_json(syskv::tablet_key(TabletId(1)), &meta.tablets[&TabletId(1)]),
                KeyWrite::Delete(syskv::tablet_key(TabletId(2))),
                KeyWrite::Delete(syskv::policy_key(TabletId(2))),
                KeyWrite::Put(syskv::merged_key(TabletId(2)), Vec::new()),
                KeyWrite::Delete(syskv::cp_member_addr_key(99)),
            ]
        );
        assert!(meta.cp_member_addrs.is_empty());
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
                replicas: vec![1],
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
                replicas: vec![1],
            },
        );
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("p", 1)),
            },
        );
        meta.cp_member_addrs.insert(7, "addr".to_string());
        meta.cp_member_tablets.insert(7, TabletId(1));

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
                KeyWrite::Delete(syskv::cp_member_addr_key(7)),
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

    #[test]
    fn set_table_mode_writes_the_updated_schema() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
        );
        let command = MetaCommand::SetTableMode {
            table: "orders".to_string(),
            mode: crate::ReplicationMode::Ap,
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
    fn create_and_drop_keyspace() {
        let mut meta = Metadata::default();
        let create = MetaCommand::CreateKeyspace {
            keyspace: "ks".to_string(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &create);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![KeyWrite::Put(syskv::keyspace_key("ks"), Vec::new())]
        );

        let drop = MetaCommand::DropKeyspace {
            keyspace: "ks".to_string(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &drop);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(writes, vec![KeyWrite::Delete(syskv::keyspace_key("ks"))]);
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
                replicas: vec![1],
            },
        );
        let command = MetaCommand::RegisterCpAddr {
            id: 5,
            addr: "127.0.0.1:9".to_string(),
            tablet: Some(TabletId(1)),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![put_json(
                syskv::cp_member_addr_key(5),
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
        let addrs = NodeAddrs {
            raftkv: "a".to_string(),
            client: "b".to_string(),
            admin: "c".to_string(),
            role: "combined".to_string(),
            control: None,
        };
        let command = MetaCommand::RegisterNodeAddrs {
            node: 3,
            addrs: addrs.clone(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(writes, vec![put_json(syskv::node_addrs_key(3), &addrs)]);
    }

    #[test]
    fn remove_member_deletes_every_address_book_entry() {
        let mut meta = Metadata::default();
        let _ = apply_and_derive_mirror(
            &mut meta,
            &MetaCommand::UpsertMember {
                node: 4,
                labels: BTreeMap::new(),
                status: NodeStatus::Leaving,
            },
        );
        let command = MetaCommand::RemoveMember { node: 4 };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(
            writes,
            vec![
                KeyWrite::Delete(syskv::member_key(4)),
                KeyWrite::Delete(syskv::node_addrs_key(4)),
                KeyWrite::Delete(syskv::cp_member_addr_key(4)),
            ]
        );
    }

    #[test]
    fn allocate_node_id_writes_member_ledger_entry_and_counter() {
        let mut meta = Metadata::default();
        let command = MetaCommand::AllocateNodeId {
            nonce: "join-1".to_string(),
            labels: BTreeMap::new(),
        };
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::Applied);
        let node_id = meta.node_id_allocations["join-1"];
        assert_eq!(
            writes,
            vec![
                put_json(syskv::member_key(node_id), &meta.members[&node_id]),
                KeyWrite::Put(
                    syskv::node_id_alloc_key("join-1"),
                    node_id.to_be_bytes().to_vec()
                ),
                put_counter(NEXT_ALLOC_ID_COUNTER, meta.next_alloc_id),
            ]
        );

        // Idempotent replay of the same nonce mints nothing new.
        let (outcome, writes) = apply_and_derive_mirror(&mut meta, &command);
        assert_eq!(outcome, ApplyOutcome::NoOp);
        assert!(writes.is_empty());
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
                node: 1,
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            },
            MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("t".to_string()),
                range: KeyRange::whole(),
                replicas: vec![1],
            },
            MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("p", 1)),
            },
            MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: schema("id"),
            },
            MetaCommand::CreateKeyspace {
                keyspace: "ks".to_string(),
            },
            MetaCommand::AllocateNodeId {
                nonce: "join-1".to_string(),
                labels: BTreeMap::new(),
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
}
