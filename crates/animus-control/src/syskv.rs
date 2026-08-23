//! The control plane's **reserved system keyspace** (ADR 0038 PR1): pure key
//! encoding for mirroring `Metadata` into a per-node `StorageEngine`, plus the
//! reserved-namespace guard that keeps a user table/keyspace name from ever
//! colliding with it.
//!
//! **PR1** left this module inert — nothing called [`entity_key`] yet (no
//! engine wired to a `RaftNode`, no `StateMachine::DRIVER_APPLIED` change, no
//! `node.rs` change); only [`is_reserved_name`] was wired, at
//! `Metadata::apply`'s `CreateTableSchema` arm and the DynamoDB edge's
//! `CreateTable` path. **PR2** (`mirror.rs`) is the
//! first real writer: it derives per-command system-keyspace writes from
//! these keys and shadow-mirrors them into a `StorageEngine` **after** the
//! unchanged in-core `Metadata::apply` still runs (`DRIVER_APPLIED` stays
//! `false` — dual-write, zero behavior change). PR2 also added three
//! [`EntityKind`] variants (`Counter`/`CpMemberAddr`, plus a third, `NodeIdAlloc`,
//! since removed in ADR 0040 PR4 with the allocator it mirrored) this
//! module's doc covers inline where they're declared.
//!
//! ## Key layout
//!
//! Reuses [`animus_tablet::escape`] byte-for-byte (ADR 0022/0023) — the same
//! order-preserving, prefix-free primitive that already backs the data-plane
//! hash ring and `animus-dynamo`'s own key encoding — so this
//! crate doesn't invent a second escaping scheme:
//!
//! ```text
//! escape(RESERVED_NAMESPACE) || escape(entity_kind) || escape(entity_id)
//! ```
//!
//! e.g. `.../tablet/<tablet_id>`, `.../member/<node_id>`, `.../schema/<table>`,
//! `.../policy/<tablet_id>`, `.../node_addrs/<node_id>`.
//! A dedicated watermark key,
//! `escape(RESERVED_NAMESPACE) || escape("_applied_index")`
//! ([`applied_index_key`]), sits alongside the entity-kind segment (not under
//! one) — it records the async apply task's durable applied index (wired in a
//! later PR), mirroring `animus-cp-data`'s own `engine_applied_index`.
//!
//! Every command a later PR's apply task drains touches only the keys for the
//! entities it actually mutates (`SplitTablet` two-to-three tablet keys,
//! `CasTabletReplicas` one, …) — this is the actual
//! scalability fix over today's whole-`Metadata`-image snapshot/compaction
//! cost (see the design doc this PR implements the first slice of).
//!
//! ## Why `escape` is reused rather than re-derived here
//!
//! `animus-control` already depends on `animus-tablet` (for `Epoch`/`KeyRange`/
//! `Tablet`/`TabletId` in `meta.rs`), so importing `escape` from there adds no
//! new dependency edge — unlike the DynamoDB wire adapter (`animus-dynamo`),
//! which deliberately duplicates `escape` to stay dependency-light of
//! `animus-tablet`, this crate has no such constraint and should reuse the
//! primitive directly rather than duplicate it.

use animus_env::NodeId;
#[cfg(test)]
use animus_env::nid;
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
/// (`schema.rs`) — DynamoDB table names are case-sensitive verbatim, so no
/// case-folding belongs here.
#[must_use]
pub fn is_reserved_name(name: &str) -> bool {
    name.starts_with(RESERVED_NAMESPACE)
}

/// One system-keyspace entity kind (ADR 0038). Each gets its own segment so a
/// command touches only the keys of the entities it actually mutates.
///
/// `Counter`/`CpMemberAddr` were added in PR2 alongside the mirror itself
/// (`mirror.rs`) — PR1 only encoded the seven fields a shadow rebuild can
/// reconstruct without them, but a **byte-identical** `Metadata` round trip
/// (the differential-oracle test) also needs `Metadata`'s monotonic tablet-id
/// allocator (`next_tablet_id`) and the legacy CP-member address book
/// (`cp_member_addrs`/`cp_member_tablets`) — so PR2 extends the enum rather
/// than leaving those fields unmirrored. A third PR2 variant, `NodeIdAlloc`
/// (the ADR 0036 `AllocateNodeId` idempotency ledger), was **removed in ADR
/// 0040 PR4** alongside the allocator it mirrored — `RegisterNode`'s claim
/// lives entirely in the already-mirrored `Member`/`NodeAddrs` kinds, no
/// separate ledger needed. Additive since: every PR1 key a running system
/// produced still decodes identically (`from_segment` only gained arms,
/// `as_str` only gained cases).
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
    /// A monotonic id-allocator counter (`Metadata::next_tablet_id`), keyed
    /// by a fixed ASCII counter name (PR2: `mirror::NEXT_TABLET_ID_COUNTER`;
    /// the ADR 0036 allocator's sibling counter, `next_alloc_id` /
    /// `NEXT_ALLOC_ID_COUNTER`, was removed in ADR 0040 PR4 along with the
    /// allocator itself). Not a per-`MetaCommand` entity — a process-wide
    /// scalar — but shaped as an ordinary entity key (one segment, one id)
    /// rather than a bespoke watermark-style key (like
    /// [`applied_index_key`]) so a new counter can be added later without
    /// inventing a third key shape.
    Counter,
    /// A legacy CP-group member's address registration
    /// (`Metadata::cp_member_addrs`/`Metadata::cp_member_tablets`, mutated
    /// only by the back-compat-only `MetaCommand::RegisterCpAddr`), keyed by
    /// [`NodeId`] (PR2).
    CpMemberAddr,
    /// A stream-shard segment catalog row (`Metadata::stream_shards`, ADR
    /// 0042 §3/ADR 0043 §A8), keyed by the composite `(TabletId, epoch)`
    /// pair — 16 raw bytes (`tablet.to_be_bytes() ++ epoch.to_be_bytes()`,
    /// [`stream_shard_key`]), unambiguous with no internal escaping needed
    /// since both fields are fixed-width. The value is the JSON-encoded
    /// `StreamShardRow`, same convention as `Tablet`/`Schema`/etc.
    StreamShard,
    /// A secondary-index backfill-completion row (`Metadata::index_backfill`,
    /// ADR 0045 §4), keyed by the composite `(TabletId, index name)` pair —
    /// the tablet's 8 big-endian bytes followed by the index name's raw UTF-8
    /// bytes ([`index_backfill_key`]). Unlike [`StreamShard`](Self::StreamShard)
    /// this id is **not** fixed-width (the index name is arbitrary length),
    /// but it needs no internal escaping either: [`decode_index_backfill_id`]
    /// always knows the tablet occupies exactly the first 8 bytes, and
    /// `entity_key` has already escaped the whole id blob as one opaque
    /// segment, so there is nothing for a variable-length suffix to
    /// ambiguously merge into. The value is always empty (presence alone is
    /// the fact).
    IndexBackfill,
    /// A copy-based split child's lineage row (`Metadata::split_lineage`,
    /// ADR 0050 fork F9), keyed by the child's [`TabletId`] — recorded by
    /// `MetaCommand::CutoverSplit`'s apply. The value is the JSON-encoded
    /// `SplitLineage`, same convention as `Tablet`/`Schema`/etc.
    SplitLineage,
}

impl EntityKind {
    /// The ASCII segment identifying this kind in an encoded key. `pub` since
    /// the admin browse surface (`GET /admin/system-table`, plan-syskv-ui)
    /// needs it to parse a `?kind=` query parameter and to render each row's
    /// kind back to the caller — previously private, only this module's own
    /// key-encoding helpers used it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            EntityKind::Tablet => "tablet",
            EntityKind::Member => "member",
            EntityKind::Schema => "schema",
            EntityKind::Policy => "policy",
            EntityKind::NodeAddrs => "node_addrs",
            EntityKind::Counter => "counter",
            EntityKind::CpMemberAddr => "cp_member_addr",
            EntityKind::StreamShard => "stream_shard",
            EntityKind::IndexBackfill => "index_backfill",
            EntityKind::SplitLineage => "split_lineage",
        }
    }

    /// Recover the kind from its encoded segment. `None` for an unrecognized
    /// segment (including [`APPLIED_INDEX_SEGMENT`] — that one decodes to
    /// [`DecodedKey::AppliedIndex`] instead, see [`decode_key`]). `pub` for
    /// the same reason as [`as_str`](Self::as_str) — the admin browse
    /// surface's `?kind=` filter parses the query string straight through
    /// this rather than re-deriving the segment table.
    #[must_use]
    pub fn from_segment(segment: &[u8]) -> Option<Self> {
        Some(match segment {
            b"tablet" => EntityKind::Tablet,
            b"member" => EntityKind::Member,
            b"schema" => EntityKind::Schema,
            b"policy" => EntityKind::Policy,
            b"node_addrs" => EntityKind::NodeAddrs,
            b"counter" => EntityKind::Counter,
            b"cp_member_addr" => EntityKind::CpMemberAddr,
            b"stream_shard" => EntityKind::StreamShard,
            b"index_backfill" => EntityKind::IndexBackfill,
            b"split_lineage" => EntityKind::SplitLineage,
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

/// The byte-lexicographic **successor** of `prefix`: the smallest byte string
/// that is strictly greater than every string having `prefix` as a prefix.
/// Standard prefix-range-scan technique — increment the last non-`0xFF` byte,
/// dropping any trailing `0xFF` bytes first (e.g. `[1, 2]` → `[1, 3]`; `[1,
/// 0xFF]` → `[2]`). Returns `None` if `prefix` is empty or consists entirely
/// of `0xFF` bytes — no finite byte string is a valid exclusive upper bound
/// for "every extension of this prefix" in that case (the range would have to
/// extend to the literal end of the keyspace). Exercised directly by this
/// module's tests, including the trailing-`0xFF` case; used by
/// [`reserved_scan_bounds`] to bound the reserved-namespace engine range scan
/// (never hits the `None` case there in practice, since
/// [`RESERVED_NAMESPACE`] doesn't end in `0xFF`, but the helper stays general
/// rather than assuming its one caller's prefix shape).
#[must_use]
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(&last) = out.last() {
        if last == 0xFF {
            out.pop();
        } else {
            *out.last_mut().expect("just confirmed non-empty via `last`") = last + 1;
            return Some(out);
        }
    }
    None
}

/// The `[start, end)` engine scan bounds covering the **entire** reserved
/// system keyspace — every [`EntityKind`] entry plus the `_applied_index`
/// watermark key, since both share the same [`RESERVED_NAMESPACE`] prefix.
///
/// **Load-bearing**: this is the bound the admin browse surface (`GET
/// /admin/system-table`, plan-syskv-ui) scans with via a single
/// [`animus_storage::StorageEngine::scan`] call, filtering by kind
/// **in memory** afterward — never [`animus_storage::StorageEngine::entries`],
/// which would scan the **whole engine**, i.e. every user table's data too on
/// a combined node sharing this engine with the CP data plane (ADR 0028). A
/// future "simplification" to `entries()` would silently turn an
/// O(system-keyspace) read into O(all-user-data-on-node) — see
/// `docs/engineering-lessons.md`.
#[must_use]
pub fn reserved_scan_bounds() -> (Vec<u8>, Vec<u8>) {
    let start = escape(RESERVED_NAMESPACE.as_bytes());
    let end =
        prefix_successor(&start).expect("RESERVED_NAMESPACE's escaped prefix has a successor");
    (start, end)
}

/// A [`TabletId`]'s key under [`EntityKind::Tablet`].
#[must_use]
pub fn tablet_key(id: TabletId) -> Vec<u8> {
    entity_key(EntityKind::Tablet, &id.0.to_be_bytes())
}

/// A [`NodeId`]'s key under [`EntityKind::Member`]. ADR 0040 PR3: a node id
/// is now a validated UTF-8 string, not a fixed-width `u64` — the key encodes
/// its raw bytes (still escaped + prefix-free via [`entity_key`]) instead of
/// 8 big-endian bytes.
#[must_use]
pub fn member_key(id: &NodeId) -> Vec<u8> {
    entity_key(EntityKind::Member, id.as_str().as_bytes())
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

/// A [`NodeId`]'s key under [`EntityKind::NodeAddrs`]. See [`member_key`]'s
/// doc for the ADR 0040 PR3 string-id encoding change.
#[must_use]
pub fn node_addrs_key(id: &NodeId) -> Vec<u8> {
    entity_key(EntityKind::NodeAddrs, id.as_str().as_bytes())
}

/// A named counter's key under [`EntityKind::Counter`] (PR2). `name` is a
/// fixed ASCII constant (`mirror::NEXT_TABLET_ID_COUNTER`), not user input.
#[must_use]
pub fn counter_key(name: &str) -> Vec<u8> {
    entity_key(EntityKind::Counter, name.as_bytes())
}

/// A [`NodeId`]'s key under [`EntityKind::CpMemberAddr`] (PR2, the legacy
/// `Metadata::cp_member_addrs`/`cp_member_tablets` pair). See [`member_key`]'s
/// doc for the ADR 0040 PR3 string-id encoding change.
#[must_use]
pub fn cp_member_addr_key(id: &NodeId) -> Vec<u8> {
    entity_key(EntityKind::CpMemberAddr, id.as_str().as_bytes())
}

/// A copy-based split child [`TabletId`]'s key under
/// [`EntityKind::SplitLineage`] (ADR 0050 fork F9), recorded by
/// `MetaCommand::CutoverSplit`'s apply.
#[must_use]
pub fn split_lineage_key(child: TabletId) -> Vec<u8> {
    entity_key(EntityKind::SplitLineage, &child.0.to_be_bytes())
}

/// A `(tablet, epoch)` pair's key under [`EntityKind::StreamShard`] (ADR
/// 0042 §3/ADR 0043 §A8): the raw 16-byte concatenation of both fixed-width
/// fields, big-endian — unambiguous with no internal escaping needed, since
/// [`decode_stream_shard_id`] always knows exactly where the boundary is.
#[must_use]
pub fn stream_shard_key(tablet: TabletId, epoch: u64) -> Vec<u8> {
    let mut id = Vec::with_capacity(16);
    id.extend_from_slice(&tablet.0.to_be_bytes());
    id.extend_from_slice(&epoch.to_be_bytes());
    entity_key(EntityKind::StreamShard, &id)
}

/// The inverse of [`stream_shard_key`]'s id half: split a decoded
/// [`EntityKind::StreamShard`] id back into `(tablet, epoch)`. `None` if
/// `id` isn't exactly 16 bytes — this module never writes anything else at
/// this kind's keys, so a mismatch is an internal bug, not a data problem.
#[must_use]
pub fn decode_stream_shard_id(id: &[u8]) -> Option<(TabletId, u64)> {
    if id.len() != 16 {
        return None;
    }
    let tablet = u64::from_be_bytes(id[..8].try_into().expect("checked length"));
    let epoch = u64::from_be_bytes(id[8..].try_into().expect("checked length"));
    Some((TabletId(tablet), epoch))
}

/// A `(tablet, index name)` pair's key under [`EntityKind::IndexBackfill`]
/// (ADR 0045 §4): the tablet's 8 big-endian bytes followed by the index
/// name's raw UTF-8 bytes. Unlike [`stream_shard_key`] this id is variable
/// length (the index name isn't fixed-width), but that's safe here: the
/// tablet always occupies exactly the first 8 bytes, and [`entity_key`] has
/// already escaped the whole blob as one opaque segment before this key
/// leaves this function, so a longer or shorter index name can never make one
/// entity's key collide with, or become a prefix of, another's.
#[must_use]
pub fn index_backfill_key(tablet: TabletId, index: &str) -> Vec<u8> {
    let mut id = Vec::with_capacity(8 + index.len());
    id.extend_from_slice(&tablet.0.to_be_bytes());
    id.extend_from_slice(index.as_bytes());
    entity_key(EntityKind::IndexBackfill, &id)
}

/// The inverse of [`index_backfill_key`]'s id half: split a decoded
/// [`EntityKind::IndexBackfill`] id back into `(tablet, index name)`. `None`
/// if `id` is shorter than the 8-byte tablet prefix, or if the remaining
/// bytes aren't valid UTF-8 — this module never writes anything else at this
/// kind's keys, so either is an internal bug, not a data problem.
#[must_use]
pub fn decode_index_backfill_id(id: &[u8]) -> Option<(TabletId, String)> {
    if id.len() < 8 {
        return None;
    }
    let tablet = u64::from_be_bytes(id[..8].try_into().expect("checked length"));
    let index = String::from_utf8(id[8..].to_vec()).ok()?;
    Some((TabletId(tablet), index))
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

    const ALL_KINDS: [EntityKind; 9] = [
        EntityKind::Tablet,
        EntityKind::Member,
        EntityKind::Schema,
        EntityKind::Policy,
        EntityKind::NodeAddrs,
        EntityKind::Counter,
        EntityKind::CpMemberAddr,
        EntityKind::StreamShard,
        EntityKind::IndexBackfill,
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
        // Matches `TableName`'s documented case-sensitivity.
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
        let key = member_key(&nid(7));
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::Member,
                id: b"n7".to_vec(),
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
        let key = node_addrs_key(&nid(300));
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::NodeAddrs,
                id: b"n300".to_vec(),
            })
        );
    }

    #[test]
    fn stream_shard_key_round_trips() {
        let key = stream_shard_key(TabletId(7), 3);
        let Some(DecodedKey::Entity { kind, id }) = decode_key(&key) else {
            panic!("expected a decodable entity key");
        };
        assert_eq!(kind, EntityKind::StreamShard);
        assert_eq!(decode_stream_shard_id(&id), Some((TabletId(7), 3)));
    }

    #[test]
    fn stream_shard_key_distinguishes_tablet_and_epoch() {
        // (7, 3) and (3, 7) must not collide despite sharing the same two
        // byte values, and neither must (7, 300) vs a hand-rolled variant
        // that would collide under a naive non-fixed-width concatenation.
        let a = stream_shard_key(TabletId(7), 3);
        let b = stream_shard_key(TabletId(3), 7);
        assert_ne!(a, b);
        let c = stream_shard_key(TabletId(7), 300);
        let d = stream_shard_key(TabletId(7), 3);
        assert_ne!(c, d);
    }

    #[test]
    fn decode_stream_shard_id_rejects_the_wrong_length() {
        assert_eq!(decode_stream_shard_id(&[0u8; 15]), None);
        assert_eq!(decode_stream_shard_id(&[0u8; 17]), None);
        assert_eq!(decode_stream_shard_id(&[]), None);
    }

    #[test]
    fn index_backfill_key_round_trips() {
        let key = index_backfill_key(TabletId(7), "by-email");
        let Some(DecodedKey::Entity { kind, id }) = decode_key(&key) else {
            panic!("expected a decodable entity key");
        };
        assert_eq!(kind, EntityKind::IndexBackfill);
        assert_eq!(
            decode_index_backfill_id(&id),
            Some((TabletId(7), "by-email".to_owned()))
        );
    }

    #[test]
    fn index_backfill_key_distinguishes_tablet_and_index_name() {
        // Two different tablets with the same index name, and the same
        // tablet with two different index names, must not collide.
        let a = index_backfill_key(TabletId(1), "by-email");
        let b = index_backfill_key(TabletId(2), "by-email");
        assert_ne!(a, b);
        let c = index_backfill_key(TabletId(1), "by-status");
        assert_ne!(a, c);
    }

    #[test]
    fn index_backfill_key_handles_varying_index_name_lengths_without_collision() {
        // A variable-length suffix (unlike `stream_shard_key`'s fixed-width
        // epoch) is the one new hazard this key shape introduces — check a
        // short name is never a prefix-confusable match for a longer one
        // sharing the same tablet.
        let short = index_backfill_key(TabletId(1), "a");
        let long = index_backfill_key(TabletId(1), "ab");
        assert_ne!(short, long);
        assert!(!long.starts_with(short.as_slice()) || short == long);
    }

    #[test]
    fn decode_index_backfill_id_rejects_a_too_short_id() {
        assert_eq!(decode_index_backfill_id(&[0u8; 7]), None);
        assert_eq!(decode_index_backfill_id(&[]), None);
    }

    #[test]
    fn decode_index_backfill_id_rejects_invalid_utf8() {
        let mut id = 1u64.to_be_bytes().to_vec();
        id.push(0xff); // not valid UTF-8 on its own
        assert_eq!(decode_index_backfill_id(&id), None);
    }

    #[test]
    fn stream_shard_keys_order_by_tablet_then_epoch() {
        let keys = [
            stream_shard_key(TabletId(1), 0),
            stream_shard_key(TabletId(1), 1),
            stream_shard_key(TabletId(1), 255),
            stream_shard_key(TabletId(1), 256),
            stream_shard_key(TabletId(2), 0),
        ];
        let mut sorted = keys.to_vec();
        sorted.sort();
        assert_eq!(keys.to_vec(), sorted, "keys should already be in order");
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
        let key = cp_member_addr_key(&nid(1301));
        assert_eq!(
            decode_key(&key),
            Some(DecodedKey::Entity {
                kind: EntityKind::CpMemberAddr,
                id: b"n1301".to_vec(),
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

    /// ADR 0040 PR3: node ids are now strings, and the string-id escaping
    /// machinery must reject exactly the collision class a naive
    /// concatenation would hit — one minted id's string being a literal
    /// prefix of another's (e.g. `"n1"` vs `"n10"`, or `nid`-style ids at
    /// different digit widths). Exercises the real `member_key`/
    /// `node_addrs_key`/`cp_member_addr_key` helpers directly (not just the
    /// generic `entity_key` the test above already covers) so a regression in
    /// any one of them is caught even if the others stay correct.
    #[test]
    fn string_ids_sharing_a_literal_prefix_do_not_collide() {
        let prefix_pairs = [(nid(1), nid(10)), (nid(1), nid(12)), (nid(9), nid(99))];
        for (short, long) in prefix_pairs {
            assert!(
                long.as_str().starts_with(short.as_str()),
                "test fixture: {long} should literally start with {short}"
            );
            let mut keys = vec![
                member_key(&short),
                member_key(&long),
                node_addrs_key(&short),
                node_addrs_key(&long),
                cp_member_addr_key(&short),
                cp_member_addr_key(&long),
            ];
            let sorted = {
                let mut k = keys.clone();
                k.sort();
                k
            };
            // No two keys collide or prefix one another.
            for (i, a) in keys.iter().enumerate() {
                for (j, b) in keys.iter().enumerate() {
                    if i == j {
                        continue;
                    }
                    assert!(
                        !b.starts_with(a.as_slice()),
                        "key for {short}/{long} pair: {a:?} prefixes {b:?}"
                    );
                }
            }
            keys.sort();
            assert_eq!(keys, sorted);
            keys.dedup();
            assert_eq!(keys.len(), 6, "every key must be distinct");
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

    // --- prefix_successor / reserved_scan_bounds ---------------------------

    #[test]
    fn prefix_successor_increments_the_last_byte() {
        assert_eq!(prefix_successor(&[1, 2]), Some(vec![1, 3]));
        assert_eq!(prefix_successor(&[0]), Some(vec![1]));
    }

    #[test]
    fn prefix_successor_drops_trailing_0xff_bytes() {
        assert_eq!(prefix_successor(&[1, 0xff]), Some(vec![2]));
        assert_eq!(prefix_successor(&[1, 0xff, 0xff]), Some(vec![2]));
        assert_eq!(prefix_successor(&[5, 0, 0xff]), Some(vec![5, 1]));
    }

    #[test]
    fn prefix_successor_is_none_for_empty_or_all_0xff() {
        assert_eq!(prefix_successor(&[]), None);
        assert_eq!(prefix_successor(&[0xff]), None);
        assert_eq!(prefix_successor(&[0xff, 0xff, 0xff]), None);
    }

    #[test]
    fn prefix_successor_is_a_strict_upper_bound_for_every_extension() {
        // Every string with `prefix` as a prefix must sort strictly below the
        // successor — spot-check a representative set of extensions.
        let prefix = vec![10, 20];
        let successor = prefix_successor(&prefix).unwrap();
        for ext in [vec![], vec![0], vec![0xff], vec![0, 1, 2], vec![0xff; 5]] {
            let mut extended = prefix.clone();
            extended.extend(ext);
            assert!(
                extended < successor,
                "{extended:?} should sort below successor {successor:?}"
            );
        }
        // And the successor is the *smallest* such bound: nothing strictly
        // between the longest all-0xFF extension of `prefix` and `successor`.
        let mut longest_extension = prefix.clone();
        longest_extension.extend([0xff; 8]);
        assert!(longest_extension < successor);
    }

    #[test]
    fn reserved_scan_bounds_covers_every_entity_key_and_the_watermark() {
        let (start, end) = reserved_scan_bounds();
        assert!(start < end);
        let mut keys: Vec<Vec<u8>> = ALL_KINDS
            .iter()
            .map(|&kind| entity_key(kind, b"some-id"))
            .collect();
        keys.push(applied_index_key());
        for key in keys {
            assert!(key >= start, "{key:?} should be >= scan start {start:?}");
            assert!(key < end, "{key:?} should be < scan end {end:?}");
        }
    }

    #[test]
    fn reserved_scan_bounds_excludes_an_unrelated_namespace() {
        let (start, end) = reserved_scan_bounds();
        let other = escape(b"not_the_system_namespace");
        assert!(
            other < start || other >= end,
            "an unrelated namespace's key must fall outside the reserved scan bounds"
        );
    }
}
