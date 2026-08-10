//! The control plane's **reserved system keyspace** (ADR 0038 PR1): pure key
//! encoding for mirroring `Metadata` into a per-node `StorageEngine`, plus the
//! reserved-namespace guard that keeps a user table/keyspace name from ever
//! colliding with it.
//!
//! **PR1** left this module inert — nothing called [`entity_key`] yet (no
//! engine wired to a `RaftNode`, no `StateMachine::DRIVER_APPLIED` change, no
//! `node.rs` change); only [`is_reserved_name`] was wired, at
//! `Metadata::apply`'s `CreateTableSchema`/`CreateKeyspace` arms and both wire
//! edges' `CreateTable`/`CREATE KEYSPACE` paths. **PR2** (`mirror.rs`) is the
//! first real writer: it derives per-command system-keyspace writes from
//! these keys and shadow-mirrors them into a `StorageEngine` **after** the
//! unchanged in-core `Metadata::apply` still runs (`DRIVER_APPLIED` stays
//! `false` — dual-write, zero behavior change). PR2 also added three
//! [`EntityKind`] variants (`Counter`/`CpMemberAddr`/`NodeIdAlloc`) this
//! module's doc covers inline where they're declared.
//!
//! ## Key layout
//!
//! Reuses [`animus_tablet::escape`] byte-for-byte (ADR 0022/0023) — the same
//! order-preserving, prefix-free primitive that already backs the data-plane
//! hash ring and `animus-dynamo`/`animus-cql`'s own key encodings — so this
//! crate doesn't invent a second escaping scheme:
//!
//! ```text
//! escape(RESERVED_NAMESPACE) || escape(entity_kind) || escape(entity_id)
//! ```
//!
//! e.g. `.../tablet/<tablet_id>`, `.../member/<node_id>`, `.../schema/<table>`,
//! `.../policy/<tablet_id>`, `.../node_addrs/<node_id>`, `.../keyspace/<name>`,
//! `.../merged/<tablet_id>`. A dedicated watermark key,
//! `escape(RESERVED_NAMESPACE) || escape("_applied_index")`
//! ([`applied_index_key`]), sits alongside the entity-kind segment (not under
//! one) — it records the async apply task's durable applied index (wired in a
//! later PR), mirroring `animus-cp-data`'s own `engine_applied_index`.
//!
//! Every command a later PR's apply task drains touches only the keys for the
//! entities it actually mutates (`SplitTablet` two-to-three tablet keys,
//! `CasTabletReplicas` one, `MergeTablets` two, …) — this is the actual
//! scalability fix over today's whole-`Metadata`-image snapshot/compaction
//! cost (see the design doc this PR implements the first slice of).
//!
//! ## Why `escape` is reused rather than re-derived here
//!
//! `animus-control` already depends on `animus-tablet` (for `Epoch`/`KeyRange`/
//! `Tablet`/`TabletId` in `meta.rs`), so importing `escape` from there adds no
//! new dependency edge — unlike the wire adapters (`animus-dynamo`/
//! `animus-cql`), which deliberately duplicate `escape` to stay
//! dependency-light of `animus-tablet`, this crate has no such constraint and
//! should reuse the primitive directly rather than triplicate it.

use animus_env::NodeId;
use animus_tablet::{TabletId, escape};

/// The top-level namespace no user table or keyspace name may claim. Reserved
/// for the control plane's own per-node system keyspace (ADR 0038).
pub const RESERVED_NAMESPACE: &str = "__animus_system";

/// The `_applied_index` watermark's key segment — a sibling of the
/// [`EntityKind`] segments, not one of them (nothing is mirrored *under* the
/// watermark; it is its own top-level entry).
const APPLIED_INDEX_SEGMENT: &[u8] = b"_applied_index";

/// Whether `name` is the reserved namespace itself, or merely **collides**
/// with it (shares its prefix) — e.g. a table literally named
/// `__animus_system`, or one that only starts with it
/// (`__animus_system_backup`). Both must be rejected: a combined node scopes
/// this keyspace into its shared engine via a reserved `StorageScope` keyed on
/// the exact namespace string (a later PR), and a prefix match is exactly the
/// collision that scoping scheme cannot tell apart from a real system key.
///
/// Case-sensitive, matching `TableName`'s documented case-sensitivity
/// (`schema.rs`) — a CQL identifier is already lowercased by that edge before
/// it reaches this check, and DynamoDB table names are case-sensitive
/// verbatim, so no case-folding belongs here.
#[must_use]
pub fn is_reserved_name(name: &str) -> bool {
    name.starts_with(RESERVED_NAMESPACE)
}

/// One system-keyspace entity kind (ADR 0038). Each gets its own segment so a
/// command touches only the keys of the entities it actually mutates.
///
/// The last three variants (`Counter`, `CpMemberAddr`, `NodeIdAlloc`) were
/// added in PR2 alongside the mirror itself (`mirror.rs`) — PR1 only encoded
/// the seven fields a shadow rebuild can reconstruct without them, but a
/// **byte-identical** `Metadata` round trip (the differential-oracle test)
/// also needs `Metadata`'s two monotonic id allocators
/// (`next_tablet_id`/`next_alloc_id`), the `AllocateNodeId` idempotency ledger
/// (`node_id_allocations`), and the legacy CP-member address book
/// (`cp_member_addrs`/`cp_member_tablets`) — so PR2 extends the enum rather
/// than leaving those fields unmirrored. Additive: every PR1 key a running
/// system produced still decodes identically (`from_segment` only gained
/// arms, `as_str` only gained cases).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EntityKind {
    /// A tablet (`Metadata::tablets`), keyed by [`TabletId`].
    Tablet,
    /// A cluster member (`Metadata::members`), keyed by [`NodeId`].
    Member,
    /// A table's replicated schema (`Metadata::schemas`), keyed by table name.
    Schema,
    /// A tablet's placement policy (`Metadata::policies`), keyed by
    /// [`TabletId`].
    Policy,
    /// A member's full address book (`Metadata::node_addrs`), keyed by
    /// [`NodeId`].
    NodeAddrs,
    /// A registered keyspace (`Metadata::keyspaces`), keyed by its name.
    Keyspace,
    /// A never-pruned merge marker (`Metadata::merged_tablets`), keyed by the
    /// merged-away [`TabletId`].
    Merged,
    /// A monotonic id-allocator counter (`Metadata::next_tablet_id` /
    /// `Metadata::next_alloc_id`), keyed by a fixed ASCII counter name (PR2:
    /// `mirror::NEXT_TABLET_ID_COUNTER` / `mirror::NEXT_ALLOC_ID_COUNTER`).
    /// Not a per-`MetaCommand` entity — a process-wide scalar — but shaped as
    /// an ordinary entity key (one segment, one id) rather than a bespoke
    /// watermark-style key (like [`applied_index_key`]) so a new counter can
    /// be added later without inventing a third key shape.
    Counter,
    /// A legacy CP-group member's address registration
    /// (`Metadata::cp_member_addrs`/`Metadata::cp_member_tablets`, mutated
    /// only by the back-compat-only `MetaCommand::RegisterCpAddr`), keyed by
    /// [`NodeId`] (PR2).
    CpMemberAddr,
    /// One `MetaCommand::AllocateNodeId` idempotency-ledger entry
    /// (`Metadata::node_id_allocations`), keyed by the join attempt's nonce
    /// string (PR2).
    NodeIdAlloc,
}

impl EntityKind {
    /// The ASCII segment identifying this kind in an encoded key.
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            EntityKind::Tablet => "tablet",
            EntityKind::Member => "member",
            EntityKind::Schema => "schema",
            EntityKind::Policy => "policy",
            EntityKind::NodeAddrs => "node_addrs",
            EntityKind::Keyspace => "keyspace",
            EntityKind::Merged => "merged",
            EntityKind::Counter => "counter",
            EntityKind::CpMemberAddr => "cp_member_addr",
            EntityKind::NodeIdAlloc => "node_id_alloc",
        }
    }

    /// Recover the kind from its encoded segment. `None` for an unrecognized
    /// segment (including [`APPLIED_INDEX_SEGMENT`] — that one decodes to
    /// [`DecodedKey::AppliedIndex`] instead, see [`decode_key`]).
    #[must_use]
    fn from_segment(segment: &[u8]) -> Option<Self> {
        Some(match segment {
            b"tablet" => EntityKind::Tablet,
            b"member" => EntityKind::Member,
            b"schema" => EntityKind::Schema,
            b"policy" => EntityKind::Policy,
            b"node_addrs" => EntityKind::NodeAddrs,
            b"keyspace" => EntityKind::Keyspace,
            b"merged" => EntityKind::Merged,
            b"counter" => EntityKind::Counter,
            b"cp_member_addr" => EntityKind::CpMemberAddr,
            b"node_id_alloc" => EntityKind::NodeIdAlloc,
            _ => return None,
        })
    }
}

/// Encode one entity's system-keyspace key: `escape(RESERVED_NAMESPACE) ||
/// escape(kind) || escape(id)`. The generic building block behind the typed
/// `*_key` helpers below; exposed directly for an `id` shape none of them
/// cover.
#[must_use]
pub fn entity_key(kind: EntityKind, id: &[u8]) -> Vec<u8> {
    let mut out = escape(RESERVED_NAMESPACE.as_bytes());
    out.extend(escape(kind.as_str().as_bytes()));
    out.extend(escape(id));
    out
}

/// The dedicated `_applied_index` watermark key (ADR 0038): `escape
/// (RESERVED_NAMESPACE) || escape("_applied_index")`. Recorded by a later PR's
/// apply task, mirroring `animus-cp-data`'s `engine_applied_index` so a
/// restart can rebuild the cache from the engine and replay only the log tail
/// beyond it.
#[must_use]
pub fn applied_index_key() -> Vec<u8> {
    let mut out = escape(RESERVED_NAMESPACE.as_bytes());
    out.extend(escape(APPLIED_INDEX_SEGMENT));
    out
}

/// A [`TabletId`]'s key under [`EntityKind::Tablet`].
#[must_use]
pub fn tablet_key(id: TabletId) -> Vec<u8> {
    entity_key(EntityKind::Tablet, &id.0.to_be_bytes())
}

/// A [`NodeId`]'s key under [`EntityKind::Member`].
#[must_use]
pub fn member_key(id: NodeId) -> Vec<u8> {
    entity_key(EntityKind::Member, &id.to_be_bytes())
}

/// A table name's key under [`EntityKind::Schema`].
#[must_use]
pub fn schema_key(table: &str) -> Vec<u8> {
    entity_key(EntityKind::Schema, table.as_bytes())
}

/// A [`TabletId`]'s key under [`EntityKind::Policy`].
#[must_use]
pub fn policy_key(id: TabletId) -> Vec<u8> {
    entity_key(EntityKind::Policy, &id.0.to_be_bytes())
}

/// A [`NodeId`]'s key under [`EntityKind::NodeAddrs`].
#[must_use]
pub fn node_addrs_key(id: NodeId) -> Vec<u8> {
    entity_key(EntityKind::NodeAddrs, &id.to_be_bytes())
}

/// A keyspace name's key under [`EntityKind::Keyspace`].
#[must_use]
pub fn keyspace_key(name: &str) -> Vec<u8> {
    entity_key(EntityKind::Keyspace, name.as_bytes())
}

/// A merged-away [`TabletId`]'s key under [`EntityKind::Merged`].
#[must_use]
pub fn merged_key(id: TabletId) -> Vec<u8> {
    entity_key(EntityKind::Merged, &id.0.to_be_bytes())
}

/// A named counter's key under [`EntityKind::Counter`] (PR2). `name` is a
/// fixed ASCII constant (`mirror::NEXT_TABLET_ID_COUNTER` /
/// `mirror::NEXT_ALLOC_ID_COUNTER`), not user input.
#[must_use]
pub fn counter_key(name: &str) -> Vec<u8> {
    entity_key(EntityKind::Counter, name.as_bytes())
}

/// A [`NodeId`]'s key under [`EntityKind::CpMemberAddr`] (PR2, the legacy
/// `Metadata::cp_member_addrs`/`cp_member_tablets` pair).
#[must_use]
pub fn cp_member_addr_key(id: NodeId) -> Vec<u8> {
    entity_key(EntityKind::CpMemberAddr, &id.to_be_bytes())
}

/// A join attempt's nonce string's key under [`EntityKind::NodeIdAlloc`]
/// (PR2, `Metadata::node_id_allocations`).
#[must_use]
pub fn node_id_alloc_key(nonce: &str) -> Vec<u8> {
    entity_key(EntityKind::NodeIdAlloc, nonce.as_bytes())
}

/// The decoded form of a system-keyspace key ([`decode_key`]'s result).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedKey {
    /// An entity key: its kind plus the raw (unescaped) id bytes.
    Entity {
        /// The entity kind.
        kind: EntityKind,
        /// The raw, unescaped entity id bytes (e.g. a `TabletId`/`NodeId`'s
        /// 8 big-endian bytes, or a table/keyspace name's UTF-8 bytes).
        id: Vec<u8>,
    },
    /// The `_applied_index` watermark key.
    AppliedIndex,
}

/// Decode one escaped segment off the front of `bytes`, per
/// [`animus_tablet::escape`]'s encoding (`0x00` doubled to `0x00 0x01`,
/// terminated by `0x00 0x00`). Returns the decoded segment and the remaining
/// bytes after its terminator, or `None` if `bytes` doesn't contain a
/// complete, well-formed escape (truncated, or a bare `0x00` not followed by
/// `0x00`/`0x01`).
fn unescape_one(bytes: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != 0x00 {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        match bytes.get(i + 1) {
            Some(0x00) => return Some((out, &bytes[i + 2..])),
            Some(0x01) => {
                out.push(0x00);
                i += 2;
            }
            _ => return None,
        }
    }
    None
}

/// Decode a key built by [`entity_key`]/[`applied_index_key`] (or one of the
/// typed `*_key` helpers) back into its [`DecodedKey`]. `None` if `key` isn't
/// a well-formed system-keyspace key: wrong/absent namespace, an unrecognized
/// entity-kind segment, a truncated escape, or trailing bytes after the id.
/// Used by this module's round-trip tests and by a later PR's engine-scan
/// decode path.
#[must_use]
pub fn decode_key(key: &[u8]) -> Option<DecodedKey> {
    let (namespace, rest) = unescape_one(key)?;
    if namespace != RESERVED_NAMESPACE.as_bytes() {
        return None;
    }
    let (segment, rest) = unescape_one(rest)?;
    if segment == APPLIED_INDEX_SEGMENT {
        return rest.is_empty().then_some(DecodedKey::AppliedIndex);
    }
    let kind = EntityKind::from_segment(&segment)?;
    let (id, rest) = unescape_one(rest)?;
    rest.is_empty().then_some(DecodedKey::Entity { kind, id })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_KINDS: [EntityKind; 10] = [
        EntityKind::Tablet,
        EntityKind::Member,
        EntityKind::Schema,
        EntityKind::Policy,
        EntityKind::NodeAddrs,
        EntityKind::Keyspace,
        EntityKind::Merged,
        EntityKind::Counter,
        EntityKind::CpMemberAddr,
        EntityKind::NodeIdAlloc,
    ];

    // --- reserved-name guard -------------------------------------------------

    #[test]
    fn reserved_namespace_itself_is_reserved() {
        assert!(is_reserved_name(RESERVED_NAMESPACE));
    }

    #[test]
    fn a_name_merely_prefixed_by_the_namespace_is_reserved() {
        assert!(is_reserved_name("__animus_system_backup"));
        assert!(is_reserved_name("__animus_systemx"));
    }

    #[test]
    fn an_ordinary_name_is_not_reserved() {
        assert!(!is_reserved_name("orders"));
        assert!(!is_reserved_name("ks.table"));
        // Shares a prefix with the reserved word but diverges before it ends.
        assert!(!is_reserved_name("__animus_syste"));
        assert!(!is_reserved_name("__animus"));
    }

    #[test]
    fn reserved_name_check_is_case_sensitive() {
        // Matches `TableName`'s documented case-sensitivity; CQL lowercases
        // before this check ever sees an identifier.
        assert!(!is_reserved_name("__ANIMUS_SYSTEM"));
    }

    // --- round trips ----------------------------------------------------------

    #[test]
    fn tablet_key_round_trips() {
        let id = TabletId(42);
        let key = tablet_key(id);
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::Tablet,
                id: 42u64.to_be_bytes().to_vec(),
            })
        );
    }

    #[test]
    fn member_key_round_trips() {
        let key = member_key(7);
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::Member,
                id: 7u64.to_be_bytes().to_vec(),
            })
        );
    }

    #[test]
    fn schema_key_round_trips() {
        let key = schema_key("orders");
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::Schema,
                id: b"orders".to_vec(),
            })
        );
    }

    #[test]
    fn policy_key_round_trips() {
        let key = policy_key(TabletId(9));
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::Policy,
                id: 9u64.to_be_bytes().to_vec(),
            })
        );
    }

    #[test]
    fn node_addrs_key_round_trips() {
        let key = node_addrs_key(300);
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::NodeAddrs,
                id: 300u64.to_be_bytes().to_vec(),
            })
        );
    }

    #[test]
    fn keyspace_key_round_trips() {
        let key = keyspace_key("my_ks");
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::Keyspace,
                id: b"my_ks".to_vec(),
            })
        );
    }

    #[test]
    fn merged_key_round_trips() {
        let key = merged_key(TabletId(5));
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::Merged,
                id: 5u64.to_be_bytes().to_vec(),
            })
        );
    }

    #[test]
    fn applied_index_key_round_trips() {
        assert_eq!(
            decode_key(&applied_index_key()),
            Some(DecodedKey::AppliedIndex)
        );
    }

    #[test]
    fn counter_key_round_trips() {
        let key = counter_key("next_tablet_id");
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::Counter,
                id: b"next_tablet_id".to_vec(),
            })
        );
    }

    #[test]
    fn cp_member_addr_key_round_trips() {
        let key = cp_member_addr_key(1301);
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::CpMemberAddr,
                id: 1301u64.to_be_bytes().to_vec(),
            })
        );
    }

    #[test]
    fn node_id_alloc_key_round_trips() {
        let key = node_id_alloc_key("join-1");
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::NodeIdAlloc,
                id: b"join-1".to_vec(),
            })
        );
    }

    #[test]
    fn ids_containing_zero_bytes_round_trip() {
        // A table/keyspace name is arbitrary UTF-8; exercise the escape's own
        // 0x00-doubling path through this module's composite key, not just
        // `animus_tablet::escape` in isolation.
        let name = "a\0b\0\0c";
        let key = schema_key(name);
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::Schema,
                id: name.as_bytes().to_vec(),
            })
        );
    }

    // --- ordering ---------------------------------------------------------

    #[test]
    fn tablet_keys_order_by_numeric_id() {
        // Big-endian id bytes ⇒ byte order == numeric order, matching the
        // convention `animus_tablet::partition_token` already uses.
        let ids = [1u64, 2, 15, 16, 255, 256, u64::MAX];
        let mut keys: Vec<Vec<u8>> = ids.iter().map(|&id| tablet_key(TabletId(id))).collect();
        let sorted = {
            let mut k = keys.clone();
            k.sort();
            k
        };
        assert_eq!(keys, sorted, "keys should already be in id order");
        // Also directly assert against the id order (redundant with the sort
        // check above, but pins the intent).
        keys.dedup();
        assert_eq!(keys.len(), ids.len());
    }

    #[test]
    fn schema_keys_order_lexicographically_by_name() {
        let names = ["a", "aa", "ab", "b"];
        let mut keys: Vec<Vec<u8>> = names.iter().map(|n| schema_key(n)).collect();
        let sorted = {
            let mut k = keys.clone();
            k.sort();
            k
        };
        assert_eq!(keys, sorted);
        keys.dedup();
        assert_eq!(keys.len(), names.len());
    }

    // --- prefix-freedom / cross-entity collision rejection -----------------

    #[test]
    fn no_two_distinct_entity_keys_prefix_one_another() {
        // A representative id set per kind, including empty/zero/high-bit ids
        // and a couple of string ids sharing prefixes with each other.
        let numeric_ids: [u64; 5] = [0, 1, 255, 256, u64::MAX];
        let string_ids = ["", "a", "ab", "abc", "__animus_system"];

        let mut keys: Vec<Vec<u8>> = Vec::new();
        for kind in ALL_KINDS {
            for id in numeric_ids {
                keys.push(entity_key(kind, &id.to_be_bytes()));
            }
            for id in string_ids {
                keys.push(entity_key(kind, id.as_bytes()));
            }
        }
        keys.push(applied_index_key());

        for (i, a) in keys.iter().enumerate() {
            for (j, b) in keys.iter().enumerate() {
                if i == j {
                    continue;
                }
                assert!(
                    !b.starts_with(a.as_slice()),
                    "key {a:?} is a prefix of distinct key {b:?}"
                );
            }
        }
    }

    #[test]
    fn unknown_namespace_does_not_decode() {
        assert_eq!(decode_key(&escape(b"not_the_system_namespace")), None);
    }

    #[test]
    fn unknown_entity_kind_does_not_decode() {
        let mut key = escape(RESERVED_NAMESPACE.as_bytes());
        key.extend(escape(b"not_a_real_kind"));
        key.extend(escape(b"id"));
        assert_eq!(decode_key(&key), None);
    }

    #[test]
    fn truncated_key_does_not_decode() {
        let mut key = tablet_key(TabletId(1));
        key.truncate(key.len() - 1);
        assert_eq!(decode_key(&key), None);
    }

    #[test]
    fn trailing_garbage_after_id_does_not_decode() {
        let mut key = tablet_key(TabletId(1));
        key.push(0xff);
        assert_eq!(decode_key(&key), None);
    }
}
