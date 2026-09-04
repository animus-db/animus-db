//! The DynamoDB JSON wire encoding (ADR 0006).
//!
//! DynamoDB clients speak JSON-over-HTTP: a request body like
//! `{"TableName":"t","Item":{"pk":{"S":"a"},"n":{"N":"1"}}}` and an
//! `X-Amz-Target: DynamoDB_20120810.PutItem` header naming the operation. This
//! module is the **pure, deterministic** translation between that JSON and the
//! crate's in-memory [`Item`] / [`AttributeValue`] model — no I/O, no
//! storage, no network. The transport edge (`animusd`) parses HTTP and routes
//! the decoded request through the distributed data plane; everything below the
//! HTTP edge stays on the `Env`-based paths.
//!
//! ## Supported subset
//!
//! Operations: `CreateTable`, `UpdateTable` (a `StreamSpecification` change,
//! or — ADR 0045 §6 — a single `GlobalSecondaryIndexUpdates` element),
//! `DescribeTable` (ADR 0042 §2), `DeleteTable` (drops the table and its
//! tablets through the same sink the dashboard uses),
//! `ListTables` (ascending name order, `Limit`/`ExclusiveStartTableName`
//! paginated, a materialized GSI's hidden table filtered out), `PutItem`,
//! `GetItem`, `DeleteItem`, `Query`, `Scan`, `UpdateItem`, `BatchWriteItem`,
//! `TransactWriteItems` (atomic, ADR 0018 §2/PR7), `TransactGetItems` (a
//! consistent multi-key read, ADR 0018 §2/PR7), `UpdateTimeToLive` /
//! `DescribeTimeToLive` (ADR 0051 — decode/encode only; the expiry predicate
//! itself is [`crate::ttl`], and the background reaper is `animusd`'s),
//! `CreateBackup`/`DescribeBackup`/`ListBackups`/`DeleteBackup` (ADR 0059,
//! Train 1 PR④ — on-demand backups; decode/encode + the ARN convention only,
//! this crate never touches the replicated backup catalog or the backup
//! store — `animusd::dynamo` and `animusd::backup_janitor` own those).
//!
//! AttributeValue types: the scalars `S` (string), `N` (number, carried
//! as text), `B` (binary, base64), `BOOL`, `NULL`; the document types `M` (map)
//! and `L` (list); and the set types `SS`/`NS`/`BS` — matching
//! [`AttributeValue`]. `PutItem` / `DeleteItem` accept a small
//! `ConditionExpression` subset (see [`crate::condition`]) and an optional
//! `ReturnValues` (`NONE`/`ALL_OLD`). `Query` accepts a partition-key equality
//! plus an optional sort-key condition (`=`, `BETWEEN`, `begins_with`), and an
//! optional `IndexName` to query a secondary index (a composite GSI / LSI may
//! carry the sort condition; a hash-only GSI may not). `CreateTable` accepts
//! `GlobalSecondaryIndexes` (hash-only or composite), `LocalSecondaryIndexes`
//! declarations, and an optional `StreamSpecification` (ADR 0042 §2 — the
//! label itself is minted by `animusd`, not decoded here). `UpdateTable`
//! accepts **either** a `StreamSpecification` change **or** exactly one
//! `GlobalSecondaryIndexUpdates` element (a `Create` or a `Delete` — no
//! `Update`/throughput shape), never both in the same call (ADR 0045 §6 Fork
//! C — a deliberate AWS deviation, documented in ADR 0045's deviations
//! table); `animusd` dispatches both halves — `Create` adds a live-backfilling
//! GSI to a populated table (ADR 0045 §2/§6) and `Delete` runs the four-step
//! convergent drop cascade (ADR 0045 §5). Any other index/key/throughput
//! change is rejected up front. `Scan` reads a whole table with `Limit` /
//! `ExclusiveStartKey` pagination and an optional `FilterExpression` (the
//! same predicate subset as `ConditionExpression`). GetItem/Query/Scan accept
//! a `ProjectionExpression` (dotted document paths `a.b.c`, list-index
//! segments `a[0]`/`a[0][1]`) or the legacy `AttributesToGet` (top-level
//! attribute names only). Deferred (rejected with a clear error): per-index
//! projection lists, `UpdateItem`-only `ReturnValues` modes, and adding an
//! LSI to an existing table (LSIs are create-time-only in real DynamoDB).

use std::borrow::Cow;
use std::collections::BTreeMap;

use animus_control::{IndexStatus, StreamViewType};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::capacity::{
    ConsumedCapacity, ItemCollectionMetrics, ReturnConsumedCapacity, ReturnItemCollectionMetrics,
    item_size,
};
use crate::condition::{Comparator, ConditionError, ConditionExpression, SortKeyCondition};
use crate::registry::{GlobalSecondaryIndex, IndexProjection, LocalSecondaryIndex, SecondaryIndex};
use crate::{AttributeValue, Item, TableSchema};

/// A stream (de)configuration decoded from an `UpdateTable`'s
/// `StreamSpecification` (ADR 0042 §2) — mutually exclusive with
/// [`IndexUpdate`] on the same call (ADR 0045 §6 Fork C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamUpdate {
    /// Enable the stream (or change its view type) — `animusd` mints a fresh
    /// `label` only when the table's stream was not already enabled.
    Enable(StreamViewType),
    /// Disable the stream.
    Disable,
}

/// A secondary-index change decoded from an `UpdateTable`'s
/// `GlobalSecondaryIndexUpdates` (ADR 0045 §6) — mutually exclusive with
/// [`StreamUpdate`] on the same call (Fork C). Exactly one element is
/// accepted per call, matching AWS's own "each `UpdateTable` may add or
/// remove at most one GSI" contract; a `Create`/`Delete` element that also
/// carries the other key, or an `Update` (throughput) element, is rejected at
/// decode time. Whether a *named* `Delete` targets an LSI (create-time-only
/// in real DynamoDB, so never deletable) can't be decided here — this layer
/// never sees the replicated catalog — so that check is `animusd`'s, same
/// division of labor as the `ConsistentRead`-against-a-GSI rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexUpdate {
    /// Add a new global secondary index to a (possibly populated) table —
    /// decoded like a `CreateTable` GSI declaration (this decoder only ever
    /// produces [`SecondaryIndex::Global`]; there is no
    /// `LocalSecondaryIndexUpdates` in the real API, and `animusd` rejects a
    /// directly-constructed `Local` variant defensively). `animusd` bridges
    /// it to a `Creating`-status `IndexDef` and proposes `CreateTableIndex`
    /// (ADR 0045 §2/§6) — the backfill seeder + completion aggregator (ADR
    /// 0045 §2/§4) do the rest, converging to `Active` with no further wire
    /// action.
    Create(SecondaryIndex),
    /// Remove the named secondary index (ADR 0045 §5's four-step convergent
    /// drop cascade).
    Delete(String),
}

/// A stream's description for a response (`CreateTable`'s echoed
/// `TableDescription`, or `DescribeTable`'s `Table`) — the pieces the wire
/// layer needs to render `StreamSpecification`/`LatestStreamArn`/
/// `LatestStreamLabel`, computed by the caller (`animusd`, which holds the
/// full replicated [`animus_control::StreamSpec`] and the table name the ARN
/// embeds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDescription {
    /// The declared view type.
    pub view_type: StreamViewType,
    /// This stream's current label (ADR 0042 §4).
    pub label: String,
}

/// A table's TTL configuration for a `DescribeTimeToLive` response (ADR
/// 0051) — the same small pure-bridge-type precedent as
/// [`StreamDescription`]: `animusd` holds the replicated catalog's real TTL
/// state and fills this in, so this crate never needs an `animus_control`
/// dependency for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlDescription {
    /// Whether TTL is currently enabled on the table.
    pub enabled: bool,
    /// The declared TTL attribute name. Present alongside `enabled: true`;
    /// `describe_time_to_live_response` omits `AttributeName` from the
    /// rendered JSON whenever `enabled` is `false`, matching AWS, regardless
    /// of what this field holds (a table may still remember its last
    /// attribute name after disabling).
    pub attribute_name: Option<String>,
}

/// The `X-Amz-Target` service+version prefix DynamoDB clients send.
pub const TARGET_PREFIX: &str = "DynamoDB_20120810.";

/// `Select` — **what** a `Query`/`Scan` returns, as distinct from
/// [`Projection`]'s *which attributes*.
///
/// Only [`Count`](Self::Count) changes the response shape: it suppresses
/// `Items` entirely, leaving `Count`/`ScannedCount` (and a
/// `LastEvaluatedKey` when the page was truncated). It does **not** change
/// what is read or how paging works — a filter still runs, `Limit` still
/// caps what is examined, and a `COUNT` page can still be truncated. That
/// matters: `Count` is the count of *matching* items on this page, not of
/// the whole query, so a client that wants a total must still page to
/// exhaustion.
///
/// The other three describe attribute selection that this adapter already
/// performs, and exist so the parameter validates the way DynamoDB does
/// rather than being silently accepted:
/// [`SpecificAttributes`](Self::SpecificAttributes) is the projection path;
/// [`AllProjectedAttributes`](Self::AllProjectedAttributes) is an index
/// read's declared projection (what an index query returns here anyway, per
/// ADR 0041); [`AllAttributes`](Self::AllAttributes) is the base-table
/// default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Select {
    /// Every attribute of the item. The default for a base-table read.
    #[default]
    AllAttributes,
    /// The queried index's declared projection. The default for an index
    /// read, and only valid when `IndexName` is present.
    AllProjectedAttributes,
    /// Exactly the paths named by `ProjectionExpression`/`AttributesToGet`,
    /// which must be present.
    SpecificAttributes,
    /// No `Items` — counts only.
    Count,
}

/// One segment of a projection document path: either a map key (a plain
/// attribute name, one `.`-separated component) or a list index (one `[n]`
/// suffix). A dotted path `a.b` is two `Field` segments; a list-index path
/// `a[0].b` is `Field("a")`, `Index(0)`, `Field("b")`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// A map (`M`) key.
    Field(String),
    /// A list (`L`) index — zero-based, matching DynamoDB.
    Index(usize),
}

/// A projection: the document paths a read should return (from a
/// `ProjectionExpression` or the legacy `AttributesToGet`). `None` on an
/// operation means "all attributes"; `Some(paths)` keeps only the requested
/// paths (a requested-but-absent path is simply omitted, as in DynamoDB).
///
/// Each element is a **document path**: a top-level attribute name optionally
/// followed by further `.`-separated map keys and/or `[n]` list-index
/// suffixes ([`PathSegment`]), so a projection can reach into nested `M`
/// (map) and `L` (list) attributes — `a.b`, `a[0]`, `a[0].b`, `a[0][1]` are
/// all valid. A path that traverses into the wrong container type, or names
/// an absent key / out-of-range index, yields nothing for that path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection(pub Vec<Vec<PathSegment>>);

impl Projection {
    /// Apply the projection to `item`, keeping only the requested document paths,
    /// reconstructing the nested structure each path reaches (so projecting
    /// `a.b` yields `{a:{b:..}}`, and projecting `a[1]`/`a[3]` out of a longer
    /// list yields `{a:[<elem 1>,<elem 3>]}` — a **compacted** list carrying just
    /// the selected elements, in ascending index order, exactly as DynamoDB
    /// documents it). Absent paths are skipped. Multiple paths sharing a prefix
    /// (including two different indices of the same list) are merged.
    #[must_use]
    pub fn apply(&self, item: &Item) -> Item {
        let mut root = Proj::Empty;
        for path in &self.0 {
            project_into_item(item, path, &mut root);
        }
        match finalize(root) {
            Some(AttributeValue::M(m)) => m,
            _ => Item::new(),
        }
    }
}

/// Intermediate accumulator for a merge of possibly many projected paths that
/// share structure. Mirrors `AttributeValue`'s two container shapes (`M`/`L`),
/// keeping a list's projected indices in a sorted map — so overlapping/nested
/// index projections merge correctly — until [`finalize`] compacts each one
/// into the DynamoDB-documented "just the selected elements, in order" list.
#[derive(Default)]
enum Proj {
    /// Nothing projected here yet.
    #[default]
    Empty,
    /// A path ended here: the whole subtree at this point is kept verbatim.
    Whole(AttributeValue),
    /// At least one path descended through a map key here.
    Map(BTreeMap<String, Proj>),
    /// At least one path descended through a list index here.
    List(BTreeMap<usize, Proj>),
}

/// Project one path's remaining `segments` out of `value`, merging into `dst`.
/// A path that does not resolve (wrong container type, absent key, or
/// out-of-range index) contributes nothing — `dst` is left as whatever earlier
/// paths already built.
fn project_into(value: &AttributeValue, segments: &[PathSegment], dst: &mut Proj) {
    // A path that already selected this whole subtree needs nothing more —
    // and must not be downgraded back into a partial Map/List by a
    // differently-shaped path processed afterward.
    if matches!(dst, Proj::Whole(_)) {
        return;
    }
    match segments.split_first() {
        None => *dst = Proj::Whole(value.clone()),
        Some((PathSegment::Field(name), rest)) => {
            let AttributeValue::M(map) = value else {
                return;
            };
            let Some(child) = map.get(name) else {
                return;
            };
            if !matches!(dst, Proj::Map(_)) {
                *dst = Proj::Map(BTreeMap::new());
            }
            let Proj::Map(m) = dst else {
                unreachable!("just set to Map above")
            };
            project_into(child, rest, m.entry(name.clone()).or_default());
        }
        Some((PathSegment::Index(i), rest)) => {
            let AttributeValue::L(list) = value else {
                return;
            };
            let Some(child) = list.get(*i) else {
                return;
            };
            if !matches!(dst, Proj::List(_)) {
                *dst = Proj::List(BTreeMap::new());
            }
            let Proj::List(m) = dst else {
                unreachable!("just set to List above")
            };
            project_into(child, rest, m.entry(*i).or_default());
        }
    }
}

/// [`project_into`]'s top-level entry point: the root of a document path is
/// always a top-level attribute name, sourced from `item` (an `Item` and an
/// `AttributeValue::M`'s inner map share the same shape, but the root has no
/// wrapping `AttributeValue` of its own).
fn project_into_item(item: &Item, segments: &[PathSegment], dst: &mut Proj) {
    let Some((PathSegment::Field(name), rest)) = segments.split_first() else {
        return; // A path always starts with a field segment (the parser guarantees this).
    };
    let Some(child) = item.get(name) else {
        return;
    };
    if !matches!(dst, Proj::Map(_)) {
        *dst = Proj::Map(BTreeMap::new());
    }
    let Proj::Map(m) = dst else {
        unreachable!("just set to Map above")
    };
    project_into(child, rest, m.entry(name.clone()).or_default());
}

/// Convert an accumulated [`Proj`] tree into the `AttributeValue` it
/// represents, compacting each `List` node's sparse index map into a plain,
/// ascending-order `Vec` — DynamoDB's documented list-projection contract
/// ("if you project `a[1]` and `a[3]`, the result list has two elements").
fn finalize(node: Proj) -> Option<AttributeValue> {
    match node {
        Proj::Empty => None,
        Proj::Whole(v) => Some(v),
        Proj::Map(m) => {
            let mut out = Item::new();
            for (k, v) in m {
                if let Some(val) = finalize(v) {
                    out.insert(k, val);
                }
            }
            Some(AttributeValue::M(out))
        }
        Proj::List(m) => {
            let out: Vec<AttributeValue> = m.into_values().filter_map(finalize).collect();
            Some(AttributeValue::L(out))
        }
    }
}

/// Apply an optional projection to an item: `Some(p)` projects, `None` returns
/// the item unchanged.
#[must_use]
pub fn project(projection: Option<&Projection>, item: &Item) -> Item {
    match projection {
        Some(p) => p.apply(item),
        None => item.clone(),
    }
}

/// The `ReturnValues` selector on a write. We support `NONE` (the default,
/// returns `{}`) and `ALL_OLD` (echo the item as it was before the write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnValues {
    /// `NONE` — return nothing (`{}`).
    None,
    /// `ALL_OLD` — return the item's previous state (the whole prior item).
    AllOld,
}

/// The `ReturnValues` selector on an `UpdateItem` — a superset of [`ReturnValues`]
/// that also reports the *new* state (`UpdateItem` is the only op that builds a
/// new item rather than replacing/removing it wholesale).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UpdateReturnValues {
    /// `NONE` (default) — return `{}`.
    #[default]
    None,
    /// `ALL_OLD` — the whole item before the update.
    AllOld,
    /// `ALL_NEW` — the whole item after the update.
    AllNew,
    /// `UPDATED_OLD` — the **previous** values of only the attributes this
    /// update changed. An attribute the update *created* has no previous
    /// value and is therefore absent.
    UpdatedOld,
    /// `UPDATED_NEW` — the **new** values of only the attributes this update
    /// changed. An attribute the update *removed* has no new value and is
    /// therefore absent.
    UpdatedNew,
}

/// One operand of a `SET` clause's right-hand side (an [`UpdateExpr`]): a
/// `:value` placeholder already resolved at decode time, a top-level
/// attribute name read from the item being updated, or a function call.
/// Nested document paths (`a.b`, `a[0]`) are a documented gap here — this
/// operand always names a **top-level** attribute (issue #375 PR1); see
/// [`UpdateAction::Set`]'s own doc.
///
/// A `Path` operand's read happens at **apply time** ([`eval_update_operand`]),
/// against whatever the fold has built so far — not necessarily the item's
/// original pre-update image — since [`apply_update`] evaluates each SET
/// action's expression against its own in-progress `item`. This mirrors
/// `ADD`'s existing `item.get(attr)` read exactly; it is a documented
/// simplification (DynamoDB's own within-expression ordering semantics are
/// stricter) rather than a modeled property.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateOperand {
    /// An already-resolved `:value`.
    Value(AttributeValue),
    /// A top-level attribute name, read from the item at apply time.
    Path(String),
    /// `if_not_exists(path, default)` — `path`'s current value if present,
    /// else `default` (itself evaluated, so it may be another function call
    /// or a `:value`). Evaluating to nothing (a `default` that is itself an
    /// absent path) is a `ValidationException` — `SET` can never assign
    /// "no value".
    IfNotExists(String, Box<UpdateOperand>),
    /// `list_append(a, b)` — the concatenation `a ++ b`; both operands must
    /// evaluate to a list (`L`). A missing operand, or a present one that
    /// isn't a list, is a `ValidationException`.
    ListAppend(Box<UpdateOperand>, Box<UpdateOperand>),
}

/// A `SET` clause's right-hand side: currently always a single
/// [`UpdateOperand`] (issue #375 PR1) — arithmetic (`operand +/- operand`)
/// lands in a follow-up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateExpr {
    /// A single operand, no arithmetic.
    Operand(UpdateOperand),
}

impl UpdateExpr {
    /// A plain `:value`/path/function-call `SET` expression — convenience
    /// wrapper around [`UpdateOperand::Value`]/`Self::Operand` used by every
    /// caller that just needs "SET this literal value" (the pre-arithmetic
    /// common case).
    #[must_use]
    pub fn value(v: AttributeValue) -> Self {
        UpdateExpr::Operand(UpdateOperand::Value(v))
    }
}

/// One action of an `UpdateItem` `UpdateExpression` (the supported subset): set a
/// top-level attribute to a value/path/function-call expression, or remove one.
///
/// **`Serialize`/`Deserialize` (ADR 0046 U3)**: rides the wire inside
/// `ClientRequest::KindWriteItem`'s `KindWriteOp::Update` — the leader-side
/// write evaluator applies `UpdateItem`'s own raw actions to the old image
/// it itself reads, rather than trusting a pre-computed new item from the
/// (possibly stale, possibly racing) edge that received the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateAction {
    /// `SET attr = expr` — set (or overwrite) a top-level attribute to the
    /// result of evaluating `expr` ([`UpdateExpr`]: a value, a path, or
    /// `if_not_exists(..)`/`list_append(..)`) against the item at apply time
    /// (issue #375 PR1). Nested document paths as the *target* (`SET a.b =
    /// :v`) are a documented gap — the target is always a top-level
    /// attribute name.
    Set(String, UpdateExpr),
    /// `REMOVE attr` — drop a top-level attribute if present.
    Remove(String),
    /// `ADD attr :v` — numeric addition when both sides are `N`, set union
    /// when both are the same set type. On an absent attribute it seeds the
    /// value, which is what makes `ADD` the idiomatic counter increment.
    Add(String, AttributeValue),
    /// `DELETE attr :v` — remove `:v`'s members from a set attribute. Only
    /// the set types; an empty result removes the attribute entirely, as
    /// DynamoDB does not store empty sets.
    Delete(String, AttributeValue),
}

/// One sub-request of a `BatchWriteItem` (within a single table's request list):
/// a put or a delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRequest {
    /// `PutRequest`: write `item`.
    Put(Item),
    /// `DeleteRequest`: delete the item identified by `key`.
    Delete(Item),
}

/// The `ReturnValuesOnConditionCheckFailure` selector on one
/// `TransactWriteItems` action (`Put`/`Delete`/`Update`/`ConditionCheck` —
/// every variant that carries a condition, ADR 0018's 2026-08-24
/// `CancellationReasons` amendment, issue #374 C2). Unlike the top-level
/// `ReturnValues`, this only ever matters when the *cancellation* is this
/// exact action's own condition failing: it selects whether that action's
/// `CancellationReasons` entry echoes the item's old image under `Item`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ReturnValuesOnConditionCheckFailure {
    /// `NONE` (default) — no `Item` on this action's cancellation reason.
    #[default]
    None,
    /// `ALL_OLD` — echo the item's old image under `Item` when this exact
    /// action is the one whose condition caused the cancellation.
    AllOld,
}

/// One action of a `TransactWriteItems` request. Each is condition-gated like a
/// conditional write; see [`Operation::TransactWriteItems`] for the (documented)
/// non-atomicity caveat.
///
/// **`Serialize`/`Deserialize`** (ADR 0018's 2026-08-24 `ClientRequestToken`
/// amendment): every nested type here already derives them (`Item` is a
/// `BTreeMap`, so encoding is deterministic, ADR 0003), which is what lets
/// `animusd::dynamo` compute a canonical `serde_json::to_vec` fingerprint of
/// the whole decoded `Vec<TransactAction>` to detect a `ClientRequestToken`
/// retry's actions matching (or not) the original request's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactAction {
    /// `Put`: write `item` in `table`, gated on `condition`.
    Put {
        /// Target table.
        table: String,
        /// The item to write.
        item: Item,
        /// Optional gate.
        condition: Option<ConditionExpression>,
        /// `ReturnValuesOnConditionCheckFailure` (ADR 0018's 2026-08-24
        /// `CancellationReasons` amendment).
        #[serde(default)]
        rvocf: ReturnValuesOnConditionCheckFailure,
    },
    /// `Delete`: delete `key` from `table`, gated on `condition`.
    Delete {
        /// Target table.
        table: String,
        /// The key to delete.
        key: Item,
        /// Optional gate.
        condition: Option<ConditionExpression>,
        /// `ReturnValuesOnConditionCheckFailure` (ADR 0018's 2026-08-24
        /// `CancellationReasons` amendment).
        #[serde(default)]
        rvocf: ReturnValuesOnConditionCheckFailure,
    },
    /// `Update`: apply `actions` to `key` in `table`, gated on `condition`.
    Update {
        /// Target table.
        table: String,
        /// The key to update.
        key: Item,
        /// The update actions (`SET`/`REMOVE`).
        actions: Vec<UpdateAction>,
        /// Optional gate.
        condition: Option<ConditionExpression>,
        /// `ReturnValuesOnConditionCheckFailure` (ADR 0018's 2026-08-24
        /// `CancellationReasons` amendment).
        #[serde(default)]
        rvocf: ReturnValuesOnConditionCheckFailure,
    },
    /// `ConditionCheck`: assert `condition` on `key` in `table` without writing.
    ConditionCheck {
        /// Target table.
        table: String,
        /// The key to check.
        key: Item,
        /// The asserted condition.
        condition: ConditionExpression,
        /// `ReturnValuesOnConditionCheckFailure` (ADR 0018's 2026-08-24
        /// `CancellationReasons` amendment).
        #[serde(default)]
        rvocf: ReturnValuesOnConditionCheckFailure,
    },
}

impl TransactAction {
    /// The table this action targets.
    #[must_use]
    pub fn table(&self) -> &str {
        match self {
            TransactAction::Put { table, .. }
            | TransactAction::Delete { table, .. }
            | TransactAction::Update { table, .. }
            | TransactAction::ConditionCheck { table, .. } => table,
        }
    }

    /// This action's own `ReturnValuesOnConditionCheckFailure` — every
    /// variant carries one (ADR 0018's 2026-08-24 `CancellationReasons`
    /// amendment).
    #[must_use]
    pub fn rvocf(&self) -> ReturnValuesOnConditionCheckFailure {
        match self {
            TransactAction::Put { rvocf, .. }
            | TransactAction::Delete { rvocf, .. }
            | TransactAction::Update { rvocf, .. }
            | TransactAction::ConditionCheck { rvocf, .. } => *rvocf,
        }
    }
}

/// A deterministic, lowercase-hex SHA-256 fingerprint of a decoded
/// `TransactWriteItems` request's actions (ADR 0018's 2026-08-24
/// `ClientRequestToken` amendment).
///
/// **Deterministic by construction**: every `TransactAction`/`Item`/
/// `ConditionExpression` in this crate is built on `BTreeMap` (ADR 0003), so
/// `serde_json::to_vec` of the *decoded* value — never the raw request
/// bytes, which could reorder JSON object keys or vary in whitespace for the
/// same logical request — renders the same bytes for the same actions
/// regardless of how the client formatted its JSON. `animusd::dynamo::
/// run_transact` hashes this to detect whether a retried `ClientRequestToken`
/// carries the *same* transaction (a legitimate retry) or a *different* one
/// reusing the token (an `IdempotentParameterMismatchException`).
#[must_use]
pub fn transact_write_fingerprint(actions: &[TransactAction]) -> String {
    let bytes = serde_json::to_vec(actions).expect("TransactAction serializes");
    hex_encode_lower(&Sha256::digest(bytes))
}

fn hex_encode_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from_digit(u32::from(b >> 4), 16).expect("nibble"));
        out.push(char::from_digit(u32::from(b & 0x0f), 16).expect("nibble"));
    }
    out
}

/// A parallel-`Scan` worker's slice: `Segment` of `TotalSegments`.
///
/// DynamoDB's contract is that the segments are disjoint and jointly cover the
/// table, so N workers each scanning their own segment see every item exactly
/// once between them. Here that falls out of the key layout: every data-plane
/// key leads with an 8-byte big-endian partition token (ADR 0022), so the
/// segments are equal slices of the 64-bit token ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanSegment {
    /// This worker's zero-based segment index; always `< total`.
    pub segment: u32,
    /// How many segments the scan is split into; always `>= 1`.
    pub total: u32,
}

/// One table's slice of a `BatchGetItem` request: the keys to read from
/// `table`, with the projection and consistency setting that apply to **all**
/// of them (DynamoDB scopes both per table, not per key — unlike
/// `TransactGetItems`, whose projection is per entry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchGet {
    /// Target table.
    pub table: String,
    /// The keys to read, in request order.
    pub keys: Vec<Item>,
    /// Optional projection applied to every item from this table.
    pub projection: Option<Projection>,
    /// `ConsistentRead` for this table's reads (per table request, not per
    /// batch — DynamoDB's own shape). Since ADR 0055 the `animusd` edge
    /// serves `false` from any replica's applied state rather than the
    /// linearizable path, so this genuinely selects a read.
    pub consistent_read: bool,
}

/// One item of a `TransactGetItems` request: a plain key read against `table`,
/// with an optional per-item projection (mirrors `GetItem`'s own `key`/
/// `projection` shape — `TransactGetItems`'s wire form is `{"Get": {TableName,
/// Key, ProjectionExpression}}` per entry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactGet {
    /// Target table.
    pub table: String,
    /// The key attributes.
    pub key: Item,
    /// Optional projection (the attributes to return; `None` = all).
    pub projection: Option<Projection>,
}

/// A decoded DynamoDB wire operation (the supported subset).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    /// `CreateTable`: register `schema` under `table`, with any secondary
    /// indexes (GSIs and LSIs) in `indexes`.
    CreateTable {
        /// New table name.
        table: String,
        /// The key schema (partition key + optional sort key).
        schema: TableSchema,
        /// The declared `AttributeType` (`S`/`N`/`B`) for each key attribute, from
        /// `AttributeDefinitions` (name → type). Used to record the key columns'
        /// types in the replicated catalog; a missing entry defaults to `S`.
        key_types: Vec<(String, String)>,
        /// Declared secondary indexes (global and local).
        indexes: Vec<SecondaryIndex>,
        /// `StreamSpecification` with `StreamEnabled: true` (ADR 0042 §2):
        /// the declared view type. `None` when the request declared no
        /// stream, or `StreamEnabled: false`. The `label` is minted by
        /// `animusd`, not decoded here — this is a pure wire layer.
        stream_view_type: Option<StreamViewType>,
    },
    /// `UpdateTable`: either a `StreamSpecification` change (ADR 0042 §2) or
    /// a single secondary-index change (ADR 0045 §6) — never both in one
    /// call (decode-time rejected, Fork C). Exactly one of `stream`/
    /// `index_update` is `Some`; any other index/key/throughput change is
    /// rejected up front at decode time.
    UpdateTable {
        /// Target table name.
        table: String,
        /// The requested stream (de)configuration, if this call changes the
        /// stream rather than an index.
        stream: Option<StreamUpdate>,
        /// The requested secondary-index change, if this call changes an
        /// index rather than the stream (ADR 0045 §6).
        index_update: Option<IndexUpdate>,
        /// The declared `AttributeType` for each attribute named in this
        /// call's own `AttributeDefinitions` (issue #319) — the identical
        /// `(AttributeName, AttributeType)` pairs [`CreateTable`](Self::
        /// CreateTable)'s own `key_types` field carries. Populated whenever
        /// `index_update` is `Some(IndexUpdate::Create(..))` (the one
        /// `UpdateTable` shape that can introduce a brand-new key
        /// attribute); empty for every other call (a `Delete`, or a stream
        /// change), since neither needs it. `animusd` threads this into the
        /// new index's own `IndexDef` so it records a real declared type
        /// instead of always defaulting to `S`.
        key_types: Vec<(String, String)>,
    },
    /// `DescribeTable` (ADR 0042 §2): a pure read of the replicated catalog
    /// (key schema, secondary-index definitions, stream configuration).
    DescribeTable {
        /// Target table name.
        table: String,
    },
    /// `DeleteTable`: drop `table` from the replicated catalog and reclaim
    /// its tablets (`animusd`'s `ClientCtx::drop_table`, the same sink
    /// the dashboard's delete button uses, ADR 0024 GC). A
    /// missing table is a `ResourceNotFoundException`, decided at the
    /// `animusd` edge (this crate never sees the replicated catalog).
    DeleteTable {
        /// Target table name.
        table: String,
    },
    /// `ListTables`: table names in ascending lexicographic order, paginated
    /// by `Limit` (default/cap 100) and `ExclusiveStartTableName` ("start
    /// strictly after this name"). A materialized GSI's hidden table
    /// (`<base>$<index>`, `animus_dynamo::index::index_table_name`) is
    /// internal and never listed — filtered at the `animusd` edge, which
    /// holds the replicated catalog this crate never sees.
    ListTables {
        /// Pagination cursor: list only names strictly greater than this one.
        exclusive_start_table_name: Option<String>,
        /// Max names to return this page (`None` = the default of 100; any
        /// value is capped at 100, matching real DynamoDB).
        limit: Option<usize>,
    },
    /// `PutItem`: insert or replace `item` in `table`.
    PutItem {
        /// Target table name.
        table: String,
        /// The item to write (must contain the table's key attributes).
        item: Item,
        /// Optional condition the write is gated on (e.g. `attribute_not_exists`).
        condition: Option<ConditionExpression>,
        /// What to echo back (`NONE` / `ALL_OLD`).
        return_values: ReturnValues,
        /// How much of the [`ConsumedCapacity`](crate::capacity::ConsumedCapacity)
        /// report the caller wants back (`NONE`/`TOTAL`/`INDEXES`).
        capacity: ReturnConsumedCapacity,
        /// Whether to report an
        /// [`ItemCollectionMetrics`](crate::capacity::ItemCollectionMetrics)
        /// (`NONE`/`SIZE`). Only ever answered for a table that has an LSI.
        metrics: ReturnItemCollectionMetrics,
    },
    /// `GetItem`: fetch the item identified by `key` from `table`.
    GetItem {
        /// Target table name.
        table: String,
        /// The key attributes (partition key, plus sort key for composite tables).
        key: Item,
        /// Optional projection (the attributes to return; `None` = all).
        projection: Option<Projection>,
        /// `ConsistentRead` (default `false`). **Since ADR 0055 this selects
        /// a real read path** at the `animusd` edge — `true` is the
        /// linearizable ReadIndex read, `false` is served from any replica's
        /// own applied state — where it used to be accept-and-ignore for
        /// correctness (ADR 0041 §5, when every read was linearizable
        /// regardless). This crate still only decodes it; the edge enforces
        /// it, and is also the one place that *rejects* it against a GSI.
        ///
        /// It has always decided capacity too: an eventually-consistent read
        /// is billed at half price.
        consistent_read: bool,
        /// How much of the [`ConsumedCapacity`](crate::capacity::ConsumedCapacity)
        /// report the caller wants back (`NONE`/`TOTAL`/`INDEXES`).
        capacity: ReturnConsumedCapacity,
    },
    /// `DeleteItem`: remove the item identified by `key` from `table`.
    DeleteItem {
        /// Target table name.
        table: String,
        /// The key attributes.
        key: Item,
        /// Optional condition the delete is gated on.
        condition: Option<ConditionExpression>,
        /// What to echo back (`NONE` / `ALL_OLD`).
        return_values: ReturnValues,
        /// How much of the [`ConsumedCapacity`](crate::capacity::ConsumedCapacity)
        /// report the caller wants back (`NONE`/`TOTAL`/`INDEXES`).
        capacity: ReturnConsumedCapacity,
        /// Whether to report an
        /// [`ItemCollectionMetrics`](crate::capacity::ItemCollectionMetrics)
        /// (`NONE`/`SIZE`). Only ever answered for a table that has an LSI.
        metrics: ReturnItemCollectionMetrics,
    },
    /// `Query`: items in a partition (`pk = ..`) matching an optional sort-key
    /// condition — against the base table, or a secondary index when `index` is
    /// set (a GSI query is a hash-key equality only; an LSI query may carry a
    /// sort condition on the index's alternate sort key). **Paginated**, the
    /// same `Limit`/`ExclusiveStartKey` contract as [`Scan`](Self::Scan):
    /// `limit` caps items examined (pushed down, not applied client-side) and
    /// `exclusive_start_key` resumes strictly after a previous page's
    /// `LastEvaluatedKey` — see `animusd::dynamo::run_base_query`'s doc for
    /// exactly how a `Query`'s pagination composes with the partition
    /// sub-range and the sort-key condition.
    Query {
        /// Target table name.
        table: String,
        /// The secondary index to query, if any (else the base table).
        index: Option<String>,
        /// The partition/index-key **attribute name** the request named, with
        /// any `#alias` resolved. Carried so the edge can reject a key
        /// condition naming something that is not the queried key; decode
        /// itself has no catalog to check against.
        partition_attr: String,
        /// The partition/index-key value (equality).
        partition_value: AttributeValue,
        /// The sort-key attribute name the request named, if it had a sort
        /// clause — carried for the same reason as `partition_attr`.
        sort_attr: Option<String>,
        /// Optional sort-key narrowing.
        sort_condition: Option<SortKeyCondition>,
        /// Max items to examine this page (`None` = all remaining).
        limit: Option<usize>,
        /// The exclusive start key (pagination cursor) from a previous
        /// page's `LastEvaluatedKey` — a base-table cursor is `{pk[, sk]}`;
        /// an index cursor also carries the index's own key attributes (see
        /// `animusd::dynamo`'s `gsi_key_item_of`/`lsi_key_item_of`).
        exclusive_start_key: Option<Item>,
        /// `ScanIndexForward` (default `true`). `false` walks the sort key
        /// **descending** — the highest sort key in the partition/index first,
        /// and `Limit` keeps the highest rather than the lowest. Pagination
        /// inverts with it: `LastEvaluatedKey` becomes the *lowest* key of the
        /// page and the next page resumes strictly below it.
        scan_index_forward: bool,
        /// Optional post-read `FilterExpression`, applied **after** the key
        /// condition has selected what to evaluate and after `limit` has
        /// capped it — exactly `Scan`'s contract. A filtered-out item still
        /// counts toward `ScannedCount` and still consumes a `Limit` slot, so
        /// a page can come back with fewer than `Limit` items and still carry
        /// a `LastEvaluatedKey`.
        filter: Option<ConditionExpression>,
        /// Optional projection (the attributes to return; `None` = all).
        projection: Option<Projection>,
        /// `Select` — what the response returns. Only `COUNT` changes the
        /// response shape (no `Items`); the rest validate the request.
        select: Select,
        /// `ConsistentRead` (default `false`). DynamoDB's own contract (ADR
        /// 0041 §5): legal against the base table or an LSI, an error against
        /// a **GSI** (eventually consistent by construction) — the `animusd`
        /// edge is the one place that rejects it, once `index` names a global
        /// index. **Since ADR 0055 it also selects the read path** on the
        /// non-GSI cases, rather than describing one that was linearizable
        /// either way.
        consistent_read: bool,
    },
    /// `Scan`: a full-table read with pagination and an optional filter — or,
    /// when `index` is set, the identical pagination/filter contract over a
    /// secondary index's own rows instead of the base table (ADR 0041 §5): a
    /// GSI scans its own hidden table's rows, an LSI scans the base table's
    /// `KIND_LSI` scope filtered to that one index. Unlike `Query`, an index
    /// `Scan` has no `KeyConditionExpression`/partition-equality narrowing —
    /// DynamoDB's own contract, since a `Scan` never takes a key condition on
    /// the base table either.
    Scan {
        /// Target table name.
        table: String,
        /// The secondary index to scan, if any (else the base table).
        index: Option<String>,
        /// Max items to return this page (`None` = all remaining).
        limit: Option<usize>,
        /// The exclusive start key (pagination cursor) from a previous page.
        exclusive_start_key: Option<Item>,
        /// Optional post-read filter (the `ConditionExpression` predicate set).
        filter: Option<ConditionExpression>,
        /// Optional projection (the attributes to return; `None` = all).
        projection: Option<Projection>,
        /// `Select` — what the response returns. Only `COUNT` changes the
        /// response shape (no `Items`); the rest validate the request.
        select: Select,
        /// The parallel-scan slice, when `Segment`/`TotalSegments` are given.
        segment: Option<ScanSegment>,
        /// `ConsistentRead` (default `false`), exactly as [`Query`](Self::Query)
        /// defines it: legal against the base table or an LSI and selecting
        /// the read path there since ADR 0055, an error against a **GSI** —
        /// the `animusd` edge is the one place that rejects it, mirroring
        /// `Query`'s identical enforcement point (ADR 0041 §5).
        consistent_read: bool,
    },
    /// `UpdateItem`: read-modify-write the item at `key`, applying `actions`
    /// (`SET`/`REMOVE`), gated on an optional `condition`.
    UpdateItem {
        /// Target table name.
        table: String,
        /// The key attributes.
        key: Item,
        /// The update actions to apply.
        actions: Vec<UpdateAction>,
        /// Optional condition the update is gated on.
        condition: Option<ConditionExpression>,
        /// What to echo back (`NONE`/`ALL_OLD`/`ALL_NEW`).
        return_values: UpdateReturnValues,
        /// How much of the [`ConsumedCapacity`](crate::capacity::ConsumedCapacity)
        /// report the caller wants back (`NONE`/`TOTAL`/`INDEXES`).
        capacity: ReturnConsumedCapacity,
        /// Whether to report an
        /// [`ItemCollectionMetrics`](crate::capacity::ItemCollectionMetrics)
        /// (`NONE`/`SIZE`). Only ever answered for a table that has an LSI.
        metrics: ReturnItemCollectionMetrics,
    },
    /// `BatchWriteItem`: a batch of put/delete requests grouped by table. Applied
    /// request-by-request (no cross-request atomicity, as in DynamoDB).
    BatchWriteItem {
        /// Per-table request lists, keyed by table name.
        requests: BTreeMap<String, Vec<WriteRequest>>,
    },
    /// `TransactWriteItems`: a list of condition-gated put/delete/update/check
    /// actions, applied **atomically** (ADR 0018 §2/PR7 — via `ClientCtx::cp_txn`,
    /// whole-or-nothing across however many tablets/tables the actions span).
    TransactWriteItems {
        /// The transaction's actions, in order.
        actions: Vec<TransactAction>,
        /// `ClientRequestToken` (ADR 0018's 2026-08-24 amendment): an
        /// optional idempotency token, 1..=36 characters. When present,
        /// `animusd::dynamo::run_transact` deduplicates a retried request
        /// carrying the same token against a durable record keyed by it —
        /// see that function's doc for the exact preflight/outcome
        /// protocol. `TransactGetItems` has no equivalent field: AWS gives
        /// reads nothing to deduplicate.
        token: Option<String>,
    },
    /// `TransactGetItems`: a consistent multi-key read (ADR 0018 §2/PR7 — a
    /// **serializable snapshot via quiescence-confirmation**, not a wait-free
    /// one; see `animusd::dynamo::run_transact_get`'s doc for the exact
    /// mechanism and its honest semantics).
    /// `BatchGetItem`: independent point reads across one or more tables.
    ///
    /// Deliberately **not** transactional — DynamoDB's `BatchGetItem` gives no
    /// cross-item atomicity, unlike `TransactGetItems`, so this reuses the
    /// ordinary `GetItem` read path per key rather than the quiescent
    /// multi-get. Responses are grouped by table, and DynamoDB does not
    /// promise any order within a table's list.
    BatchGetItem {
        /// One entry per requested table, in request order.
        requests: Vec<BatchGet>,
    },
    TransactGetItems {
        /// The keys to read, in request order (the response echoes this order).
        gets: Vec<TransactGet>,
    },
    /// `UpdateTimeToLive` (ADR 0051): declare, change, or disable a table's
    /// TTL attribute. AWS requires `AttributeName` even when `enabled` is
    /// `false` — a disable call must still name the attribute being
    /// disabled, which AWS validates matches the currently-enabled one. This
    /// layer decodes and passes both fields through unchanged; whether a
    /// disable's `attribute_name` must match the table's current one is
    /// `animusd`'s call (it holds the replicated catalog this crate never
    /// sees).
    UpdateTimeToLive {
        /// Target table name.
        table: String,
        /// The TTL attribute name.
        attribute_name: String,
        /// Whether TTL is being enabled (`true`) or disabled (`false`).
        enabled: bool,
    },
    /// `DescribeTimeToLive` (ADR 0051): a pure read of a table's TTL
    /// configuration (`animusd` supplies it from the replicated catalog).
    DescribeTimeToLive {
        /// Target table name.
        table: String,
    },
    /// `CreateBackup` (ADR 0059, Train 1 PR④): begin an on-demand backup of
    /// `table`, named `backup_name`. `animusd` validates the table exists
    /// (`TableNotFoundException`), mints a fresh opaque backup identity, and
    /// proposes the catalog row — capture then proceeds **asynchronously**
    /// (this call never waits for `AVAILABLE`).
    CreateBackup {
        /// The source table to back up.
        table: String,
        /// The client-supplied backup name (echoed back, never interpreted).
        backup_name: String,
    },
    /// `DescribeBackup` (ADR 0059, Train 1 PR④): a pure read of one backup's
    /// catalog row by its ARN — works even after the source table has been
    /// dropped (the manifest is a captured snapshot, ADR 0059 §2/§3).
    DescribeBackup {
        /// The backup's ARN (this adapter's own opaque catalog identity,
        /// ADR 0059 §3 — never parsed, only looked up).
        backup_arn: String,
    },
    /// `ListBackups` (ADR 0059, Train 1 PR④): paginated backup summaries in
    /// ascending-ARN order (the replicated catalog's own `BTreeMap` order),
    /// optionally filtered by source table name, creation-time range, and
    /// backup type.
    ListBackups {
        /// Filter to backups of this source table only, if given.
        table: Option<String>,
        /// Max summaries to return this page (`None` = the default of 100;
        /// any value is capped at 100, matching real DynamoDB).
        limit: Option<usize>,
        /// Pagination cursor: list only backups whose ARN sorts strictly
        /// after this one.
        exclusive_start_backup_arn: Option<String>,
        /// Only backups created at or after this epoch-millisecond instant,
        /// if given (`TimeRangeLowerBound`, an AWS `Timestamp` — epoch
        /// seconds on the wire, decoded to milliseconds here to match
        /// `BackupManifest::created_wall_ms`'s own unit).
        time_range_lower_bound_ms: Option<u64>,
        /// Only backups created at or before this epoch-millisecond instant,
        /// if given (`TimeRangeUpperBound`).
        time_range_upper_bound_ms: Option<u64>,
        /// `BackupType` filter (default `USER` — AWS's own default, and the
        /// only type this adapter ever produces in Train 1).
        backup_type: BackupTypeFilter,
    },
    /// `DeleteBackup` (ADR 0059, Train 1 PR④): mark a backup (by ARN) for
    /// reclaim. Rejected while still `CREATING` (`BackupInUseException`,
    /// AWS-faithful); actual object reclaim is the backup janitor's own
    /// async job (`animusd::backup_janitor`).
    DeleteBackup {
        /// The backup's ARN.
        backup_arn: String,
    },
    /// `RestoreTableFromBackup` (ADR 0059 §7, Train 2): restore `backup_arn`
    /// into a brand-new table named `target_table_name` — always a new
    /// table, never a merge/in-place restore (AWS's own contract; `animusd`
    /// rejects a pre-existing target with `TableAlreadyExistsException`).
    /// `global_secondary_index_override`, when `Some`, entirely **replaces**
    /// the backup's own captured GSI set (AWS's own
    /// `GlobalSecondaryIndexOverride` knob — `Some(vec![])` restores with no
    /// GSIs at all); `None` restores every GSI the backup's manifest
    /// recorded, unchanged. LSIs are never overridable (create-time-only,
    /// same as `CreateTable`) and always come from the manifest verbatim.
    RestoreTableFromBackup {
        /// The source backup's ARN.
        backup_arn: String,
        /// The new table's name.
        target_table_name: String,
        /// A full replacement GSI declaration, if given.
        global_secondary_index_override: Option<Vec<SecondaryIndex>>,
    },
    /// `RestoreTableToPointInTime` (ADR 0059 §10, Train 3 PR②): restore
    /// `source_table_name`'s point-in-time recovery (PITR) history into a
    /// brand-new table `target_table_name`, as of a target wall-clock
    /// second (`restore_date_time_secs`, AWS's own `Timestamp` shape — epoch
    /// seconds, possibly fractional, truncated to the second by `animusd`)
    /// or the table's own current `LatestRestorableDateTime`
    /// (`use_latest_restorable_time`). Exactly one of the two must be
    /// meaningful (`animusd` validates); `global_secondary_index_override`
    /// is the identical full-replacement GSI knob
    /// [`RestoreTableFromBackup`](Self::RestoreTableFromBackup) already has.
    RestoreTableToPointInTime {
        /// The source table's name (may already be dropped — ADR 0059
        /// §9/§10's own "PITR history survives the source table" carve-out).
        source_table_name: String,
        /// The new table's name.
        target_table_name: String,
        /// `RestoreDateTime`, decoded to epoch **milliseconds**
        /// ([`decode_backup_timestamp_ms`]'s own shape — full precision;
        /// `animusd` truncates to the whole second per AWS's own
        /// `RestoreDateTime` granularity) — `None` when
        /// `UseLatestRestorableTime` is set instead.
        restore_date_time_ms: Option<u64>,
        /// `UseLatestRestorableTime` — restore to the table's own current
        /// `LatestRestorableDateTime` rather than a caller-named second.
        use_latest_restorable_time: bool,
        /// A full replacement GSI declaration, if given.
        global_secondary_index_override: Option<Vec<SecondaryIndex>>,
    },
    /// `UpdateContinuousBackups` (ADR 0059 §9, Train 3): enable or disable
    /// `table`'s point-in-time recovery (PITR). `animusd` validates the
    /// table exists (`TableNotFoundException`) and proposes the catalog
    /// toggle; enabling starts the retention window at "now," disabling
    /// then re-enabling resets it (a fresh generation, never fake
    /// continuity — see `animus_control::PitrSpec`'s own doc).
    UpdateContinuousBackups {
        /// Target table name.
        table: String,
        /// Whether PITR is being enabled (`true`) or disabled (`false`).
        enabled: bool,
    },
    /// `DescribeContinuousBackups` (ADR 0059 §9, Train 3): a pure read of a
    /// table's PITR configuration and its currently-restorable window
    /// (`animusd` derives both from the replicated catalog).
    DescribeContinuousBackups {
        /// Target table name.
        table: String,
    },
    /// `TagResource` (roadmap W-06): add or overwrite tags on a table,
    /// addressed by its [`table_arn`] (`ResourceArn` on the wire, decoded
    /// and validated as a table ARN — malformed shape or a stream/backup
    /// ARN sharing the `table/<name>` prefix is a decode-time
    /// `ValidationException`; a well-formed ARN naming a table that does
    /// not exist is `animusd`'s call, ADR-faithful to `ResourceNotFoundException`).
    /// A tag key already present is overwritten (last writer wins, AWS's
    /// own `TagResource` semantics).
    TagResource {
        /// The target table, recovered from `ResourceArn` by [`parse_table_arn`].
        table: String,
        /// The `(key, value)` pairs to set.
        tags: BTreeMap<String, String>,
    },
    /// `UntagResource` (roadmap W-06): remove tags from a table by key,
    /// addressed the same way as [`TagResource`](Self::TagResource). A key
    /// not currently present is silently ignored, matching AWS.
    UntagResource {
        /// The target table, recovered from `ResourceArn`.
        table: String,
        /// The tag keys to remove.
        tag_keys: Vec<String>,
    },
    /// `ListTagsOfResource` (roadmap W-06): a pure read of a table's current
    /// tags, addressed the same way as [`TagResource`](Self::TagResource).
    /// No pagination — AWS's own `NextToken` is only ever needed past 50
    /// tags in a single response page, and this adapter always returns the
    /// whole set.
    ListTagsOfResource {
        /// The target table, recovered from `ResourceArn`.
        table: String,
    },
    /// `DescribeLimits` (roadmap W-06): a pure, static read of this
    /// adapter's account/table capacity ceilings. No fields to decode —
    /// AWS's own request body is `{}`.
    DescribeLimits,
    /// `DescribeEndpoints` (roadmap W-06): this node's own DynamoDB
    /// endpoint address, for SDK client discovery. No fields to decode —
    /// AWS's own request body is `{}`.
    DescribeEndpoints,
}

/// `ListBackups`'s `BackupType` filter — AWS's own `USER`/`SYSTEM`/
/// `AWS_BACKUP`/`ALL` vocabulary (ADR 0059, Train 1 PR④). This adapter only
/// ever produces `User` (on-demand) backups today: `System` (PITR base
/// snapshots) is Train 3's concern, and `AwsBackup` (AWS Backup service
/// integration) is never produced at all — so a `System`/`AwsBackup` filter
/// always yields an empty page, honestly, rather than being rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackupTypeFilter {
    /// On-demand backups (`CreateBackup`) — AWS's own default when
    /// `BackupType` is omitted.
    #[default]
    User,
    /// PITR base snapshots (Train 3) — never produced yet.
    System,
    /// AWS Backup service integration — never produced by this adapter.
    AwsBackup,
    /// Every type.
    All,
}

impl BackupTypeFilter {
    /// Whether an on-demand (`User`-type) backup passes this filter.
    #[must_use]
    pub fn matches_user_backup(self) -> bool {
        matches!(self, BackupTypeFilter::User | BackupTypeFilter::All)
    }
}

impl Operation {
    /// The single table this operation targets, if it has one.
    /// `BatchWriteItem`, `BatchGetItem`, `TransactWriteItems`,
    /// `TransactGetItems`, and `ListTables` span multiple (or zero) tables,
    /// so they return `None`.
    #[must_use]
    pub fn table(&self) -> Option<&str> {
        match self {
            Operation::CreateTable { table, .. }
            | Operation::UpdateTable { table, .. }
            | Operation::DescribeTable { table, .. }
            | Operation::DeleteTable { table, .. }
            | Operation::PutItem { table, .. }
            | Operation::GetItem { table, .. }
            | Operation::DeleteItem { table, .. }
            | Operation::Query { table, .. }
            | Operation::Scan { table, .. }
            | Operation::UpdateItem { table, .. }
            | Operation::UpdateTimeToLive { table, .. }
            | Operation::DescribeTimeToLive { table, .. }
            | Operation::UpdateContinuousBackups { table, .. }
            | Operation::DescribeContinuousBackups { table, .. }
            | Operation::CreateBackup { table, .. }
            | Operation::TagResource { table, .. }
            | Operation::UntagResource { table, .. }
            | Operation::ListTagsOfResource { table, .. } => Some(table),
            // `RestoreTableFromBackup` targets its brand-new *target* table
            // (mirroring `CreateTable`'s own "the table about to exist" —
            // the target doesn't exist yet either way).
            Operation::RestoreTableFromBackup {
                target_table_name, ..
            }
            | Operation::RestoreTableToPointInTime {
                target_table_name, ..
            } => Some(target_table_name),
            Operation::BatchWriteItem { .. }
            | Operation::BatchGetItem { .. }
            | Operation::TransactWriteItems { .. }
            | Operation::TransactGetItems { .. }
            | Operation::ListTables { .. }
            // `DescribeBackup`/`DeleteBackup` address a backup by ARN, not a
            // table name (ADR 0059 §3's own "keyed by backup identity, never
            // by table name" scar); `ListBackups`' `TableName` is an
            // optional filter, not a single target, mirroring `ListTables`'
            // own "no single table" shape.
            | Operation::DescribeBackup { .. }
            | Operation::ListBackups { .. }
            | Operation::DeleteBackup { .. }
            // `DescribeLimits`/`DescribeEndpoints` (roadmap W-06) address no
            // table at all — an account-wide static read and a pure
            // node-address read, respectively.
            | Operation::DescribeLimits
            | Operation::DescribeEndpoints => None,
        }
    }
}

/// One entry of AWS's `CancellationReasons` array on a
/// `TransactionCanceledException` (ADR 0018's 2026-08-24 `CancellationReasons`
/// amendment, issue #374 C2) — one per `TransactItems` action, in the
/// request's own order. `Code: "None"` (with a `null` `Message`, never
/// omitted — matching AWS's own wire shape) marks an action that was **not**
/// itself the cause of the cancellation. `Item` is present only on a
/// `ConditionalCheckFailed` entry, and only when that action requested
/// `ReturnValuesOnConditionCheckFailure: "ALL_OLD"` — every other entry omits
/// the field entirely (never `null`), matching AWS's own asymmetry between
/// `Message` (always rendered) and `Item` (rendered only when present).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CancellationReason {
    /// `"None"`, `"ConditionalCheckFailed"`, or `"TransactionConflict"` — the
    /// closed set this adapter emits.
    #[serde(rename = "Code")]
    pub code: &'static str,
    /// `None` renders as JSON `null` (not omitted) — AWS always includes this
    /// key, `null` for a `None`-coded entry.
    #[serde(rename = "Message")]
    pub message: Option<String>,
    /// The item's old image, DynamoDB-wire-encoded exactly like a `GetItem`
    /// result — present only for a `ConditionalCheckFailed` entry whose own
    /// action asked for `ReturnValuesOnConditionCheckFailure: "ALL_OLD"` and
    /// whose old image was in hand at the point of failure.
    #[serde(rename = "Item", skip_serializing_if = "Option::is_none")]
    pub item: Option<Value>,
}

impl CancellationReason {
    /// An action that was not the cause of the cancellation.
    #[must_use]
    pub fn none() -> Self {
        Self {
            code: "None",
            message: None,
            item: None,
        }
    }

    /// A `ConditionExpression`/`ConditionCheck` that evaluated to false.
    /// `item` is the old image to echo under `Item` when the action's own
    /// `ReturnValuesOnConditionCheckFailure` was `ALL_OLD`; pass `None` when
    /// it was `NONE` (the default) or the old image was not in hand.
    #[must_use]
    pub fn conditional_check_failed(item: Option<&Item>) -> Self {
        Self {
            code: "ConditionalCheckFailed",
            message: Some("The conditional request failed".into()),
            item: item.map(encode_item),
        }
    }

    /// A key already held a different, still-unresolved transaction's
    /// intent — a lost race, not a permanent condition failure (ADR 0018 §2
    /// `StageOutcome::IntentBlocked`). Never carries `Item`: AWS does not
    /// document one for this code, and this adapter has no old image in hand
    /// at the point this is minted.
    #[must_use]
    pub fn transaction_conflict() -> Self {
        Self {
            code: "TransactionConflict",
            message: Some("Transaction is ongoing for the item".into()),
            item: None,
        }
    }
}

/// The bracketed aggregate `message` AWS derives from a `CancellationReasons`
/// array, e.g. `"Transaction cancelled, please refer cancellation reasons for
/// specific reasons [None, ConditionalCheckFailed]"`.
#[must_use]
fn cancellation_aggregate_message(reasons: &[CancellationReason]) -> String {
    let codes = reasons
        .iter()
        .map(|r| r.code)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "Transaction cancelled, please refer cancellation reasons for specific reasons [{codes}]"
    )
}

/// A wire-level decode/encode failure, carrying the DynamoDB-style error code
/// (the `__type` field) and a human message.
#[derive(Debug, Clone, PartialEq)]
pub struct WireError {
    /// The DynamoDB error code, e.g. `ValidationException` or
    /// `UnknownOperationException`. Sent to clients as the `__type` field.
    pub code: &'static str,
    /// A human-readable message.
    pub message: String,
    /// The per-action `CancellationReasons` array (ADR 0018's 2026-08-24
    /// `CancellationReasons` amendment) — `Some` only on a
    /// `TransactionCanceledException` minted with per-action detail in hand
    /// ([`Self::transaction_canceled_with_reasons`]); `None` for every other
    /// error, and for the aggregate-only [`Self::transaction_canceled`] used
    /// where no per-action detail is available (e.g. a cached `CANCELLED`
    /// idempotency replay, which never persisted its original reasons).
    pub reasons: Option<Vec<CancellationReason>>,
}

impl WireError {
    /// A malformed or invalid request (e.g. too many `TransactWriteItems`
    /// actions, or two actions on the same item).
    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            code: "ValidationException",
            message: message.into(),
            reasons: None,
        }
    }

    /// An unrecognized `X-Amz-Target` operation.
    #[must_use]
    pub fn unknown_operation(target: &str) -> Self {
        Self {
            code: "UnknownOperationException",
            message: format!("unsupported operation `{target}`"),
            reasons: None,
        }
    }

    /// A malformed JSON body.
    #[must_use]
    pub fn serialization(message: impl Into<String>) -> Self {
        Self {
            code: "SerializationException",
            message: message.into(),
            reasons: None,
        }
    }

    /// A `ConditionExpression` evaluated to false — the write is rejected.
    #[must_use]
    pub fn conditional_check_failed(message: impl Into<String>) -> Self {
        Self {
            code: "ConditionalCheckFailedException",
            message: message.into(),
            reasons: None,
        }
    }

    /// A `TransactWriteItems`/`TransactGetItems` request was cancelled — a
    /// condition failure, a lost race against a concurrent write, or an
    /// internal 2PC abort (ADR 0018 §2/PR7). This is the DynamoDB exception
    /// type real `TransactWriteItems`/`TransactGetItems` failures use (as
    /// opposed to the bare `ConditionalCheckFailedException` a single-item
    /// conditional `PutItem`/`DeleteItem`/`UpdateItem` returns) — **aggregate
    /// form**: a single human message and no `CancellationReasons` array.
    /// Used where no per-action detail is available at the point of failure
    /// (a cached `CANCELLED` idempotency replay, which never persisted its
    /// original reasons; a structural/routing abort with no single
    /// responsible action). Prefer
    /// [`Self::transaction_canceled_with_reasons`] whenever the failing
    /// action is known (ADR 0018's 2026-08-24 `CancellationReasons`
    /// amendment, issue #374 C2).
    #[must_use]
    pub fn transaction_canceled(message: impl Into<String>) -> Self {
        Self {
            code: "TransactionCanceledException",
            message: message.into(),
            reasons: None,
        }
    }

    /// A `TransactWriteItems` request was cancelled, with AWS's full
    /// per-action `CancellationReasons` array in hand (ADR 0018's 2026-08-24
    /// `CancellationReasons` amendment, issue #374 C2) — `reasons` has one
    /// entry per `TransactItems` action, in the request's own order
    /// ([`CancellationReason::none`] for every action that was not the
    /// cause). The aggregate `message` is derived from `reasons` itself
    /// (AWS's own bracketed-code-list wording), never supplied separately —
    /// see [`cancellation_aggregate_message`].
    #[must_use]
    pub fn transaction_canceled_with_reasons(reasons: Vec<CancellationReason>) -> Self {
        Self {
            code: "TransactionCanceledException",
            message: cancellation_aggregate_message(&reasons),
            reasons: Some(reasons),
        }
    }

    /// A `TransactWriteItems` `ClientRequestToken` was reused with a
    /// **different** set of actions than the original request that minted
    /// it (ADR 0018's 2026-08-24 amendment) — the fingerprint of the
    /// decoded `Vec<TransactAction>` disagrees with the one stored under
    /// the token. The real AWS exception type for this condition.
    #[must_use]
    pub fn idempotent_parameter_mismatch(message: impl Into<String>) -> Self {
        Self {
            code: "IdempotentParameterMismatchException",
            message: message.into(),
            reasons: None,
        }
    }

    /// A `TransactWriteItems` `ClientRequestToken` names a record this node
    /// observed as still `PENDING` (ADR 0018's 2026-08-24 amendment) — the
    /// original request with this token may still be committing, or may
    /// have crashed before reaching an outcome; either way, retrying now is
    /// premature. **Deliberately conservative**: real DynamoDB tolerates a
    /// same-fingerprint retry racing a genuinely in-flight original request
    /// and serves the eventual outcome; this adapter narrows that to
    /// "retry later" rather than blocking or speculatively joining the
    /// in-flight attempt — see the ADR amendment for why. The real AWS
    /// exception type for this condition; a client's own SDK retry policy
    /// already expects and handles it.
    #[must_use]
    pub fn transaction_in_progress(message: impl Into<String>) -> Self {
        Self {
            code: "TransactionInProgressException",
            message: message.into(),
            reasons: None,
        }
    }

    /// Render as the DynamoDB error JSON body (`{"__type":..,"message":..}`,
    /// plus a `"CancellationReasons"` sibling array when [`Self::reasons`]
    /// is `Some`, ADR 0018's 2026-08-24 `CancellationReasons` amendment).
    #[must_use]
    pub fn to_json(&self) -> String {
        let body = ErrorBody {
            type_: format!("com.amazonaws.dynamodb.v20120810#{}", self.code),
            message: self.message.clone(),
            cancellation_reasons: self.reasons.clone(),
        };
        serde_json::to_string(&body).expect("error body serializes")
    }
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for WireError {}

impl From<ConditionError> for WireError {
    /// A `size()`/`begins_with()`/`contains()` applied to an existing
    /// attribute of a type outside that function's operand domain — the
    /// same `ValidationException` shape real DynamoDB returns, so a `?` on
    /// `ConditionExpression::evaluate` anywhere along a condition or filter
    /// path (conditional writes, `Query`/`Scan` filters, `TransactWriteItems`
    /// `ConditionCheck`) turns it into the right wire error automatically.
    fn from(err: ConditionError) -> Self {
        WireError::validation(err.message)
    }
}

#[derive(Serialize)]
struct ErrorBody {
    #[serde(rename = "__type")]
    type_: String,
    message: String,
    /// `Some` renders a `"CancellationReasons"` sibling array (ADR 0018's
    /// 2026-08-24 `CancellationReasons` amendment); `None` omits the key
    /// entirely, matching every non-`TransactWriteItems` error's wire shape
    /// exactly as before this amendment.
    #[serde(
        rename = "CancellationReasons",
        skip_serializing_if = "Option::is_none"
    )]
    cancellation_reasons: Option<Vec<CancellationReason>>,
}

/// Decode a request body for the operation named by `target` (the full
/// `X-Amz-Target` header value, e.g. `DynamoDB_20120810.PutItem`).
///
/// # Errors
/// Returns a [`WireError`] if the target is unsupported or the body is invalid.
pub fn decode_request(target: &str, body: &[u8]) -> Result<Operation, WireError> {
    let op = target.strip_prefix(TARGET_PREFIX).unwrap_or(target);
    let json: Value = serde_json::from_slice(body)
        .map_err(|e| WireError::serialization(format!("invalid JSON body: {e}")))?;
    let obj = json
        .as_object()
        .ok_or_else(|| WireError::validation("request body must be a JSON object"))?;

    match op {
        "CreateTable" => {
            let table = table_name(obj)?;
            let schema = decode_key_schema(obj)?;
            let key_types = decode_attribute_types(obj);
            let indexes = decode_indexes(obj)?;
            let stream_view_type = decode_create_table_stream_spec(obj)?;
            Ok(Operation::CreateTable {
                table,
                schema,
                key_types,
                indexes,
                stream_view_type,
            })
        }
        "UpdateTable" => decode_update_table(obj),
        "DescribeTable" => Ok(Operation::DescribeTable {
            table: table_name(obj)?,
        }),
        "DeleteTable" => Ok(Operation::DeleteTable {
            table: table_name(obj)?,
        }),
        "ListTables" => decode_list_tables(obj),
        "PutItem" => {
            let table = table_name(obj)?;
            let item = decode_item_field(obj, "Item")?;
            check_item_size(&item)?;
            let condition = decode_condition(obj)?;
            let return_values = decode_return_values(obj)?;
            let capacity = decode_return_consumed_capacity(obj)?;
            let metrics = decode_return_item_collection_metrics(obj)?;
            Ok(Operation::PutItem {
                table,
                item,
                condition,
                return_values,
                capacity,
                metrics,
            })
        }
        "GetItem" => {
            let table = table_name(obj)?;
            let key = decode_item_field(obj, "Key")?;
            let projection = decode_projection(obj)?;
            let consistent_read = decode_consistent_read(obj);
            let capacity = decode_return_consumed_capacity(obj)?;
            Ok(Operation::GetItem {
                table,
                key,
                projection,
                consistent_read,
                capacity,
            })
        }
        "DeleteItem" => {
            let table = table_name(obj)?;
            let key = decode_item_field(obj, "Key")?;
            let condition = decode_condition(obj)?;
            let return_values = decode_return_values(obj)?;
            let capacity = decode_return_consumed_capacity(obj)?;
            let metrics = decode_return_item_collection_metrics(obj)?;
            Ok(Operation::DeleteItem {
                table,
                key,
                condition,
                return_values,
                capacity,
                metrics,
            })
        }
        "Query" => decode_query(obj),
        "Scan" => decode_scan(obj),
        "UpdateItem" => decode_update_item(obj),
        "BatchWriteItem" => decode_batch_write(obj),
        "TransactWriteItems" => decode_transact_write(obj),
        "BatchGetItem" => decode_batch_get(obj),
        "TransactGetItems" => decode_transact_get(obj),
        "UpdateTimeToLive" => decode_update_time_to_live(obj),
        "DescribeTimeToLive" => Ok(Operation::DescribeTimeToLive {
            table: table_name(obj)?,
        }),
        "UpdateContinuousBackups" => decode_update_continuous_backups(obj),
        "DescribeContinuousBackups" => Ok(Operation::DescribeContinuousBackups {
            table: table_name(obj)?,
        }),
        "CreateBackup" => decode_create_backup(obj),
        "DescribeBackup" => Ok(Operation::DescribeBackup {
            backup_arn: backup_arn_field(obj)?,
        }),
        "ListBackups" => decode_list_backups(obj),
        "DeleteBackup" => Ok(Operation::DeleteBackup {
            backup_arn: backup_arn_field(obj)?,
        }),
        "RestoreTableFromBackup" => decode_restore_table_from_backup(obj),
        "RestoreTableToPointInTime" => decode_restore_table_to_point_in_time(obj),
        "TagResource" => decode_tag_resource(obj),
        "UntagResource" => decode_untag_resource(obj),
        "ListTagsOfResource" => Ok(Operation::ListTagsOfResource {
            table: resource_arn_table(obj)?,
        }),
        "DescribeLimits" => Ok(Operation::DescribeLimits),
        "DescribeEndpoints" => Ok(Operation::DescribeEndpoints),
        _ => Err(WireError::unknown_operation(target)),
    }
}

/// Decode the `ResourceArn` field shared by `TagResource`/`UntagResource`/
/// `ListTagsOfResource`, recovering the target table name via
/// [`parse_table_arn`]. Missing entirely, not a string, or not a
/// well-formed **table** ARN (including a well-formed stream/backup ARN,
/// which names a different resource) is `ValidationException` — the same
/// class of decode-time structural error every other malformed field in
/// this module gets; a well-formed ARN naming a table that genuinely
/// doesn't exist is `animusd`'s call (`ResourceNotFoundException`, since
/// only it holds the replicated catalog).
fn resource_arn_table(obj: &Map<String, Value>) -> Result<String, WireError> {
    let arn = obj
        .get("ResourceArn")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("missing string field `ResourceArn`"))?;
    parse_table_arn(arn)
        .map(str::to_owned)
        .ok_or_else(|| WireError::validation(format!("`ResourceArn` is not a table ARN: {arn}")))
}

/// Decode a `TagResource` body: `ResourceArn` plus `Tags`, an array of
/// `{"Key": .., "Value": ..}` objects. A later duplicate key in the array
/// overwrites an earlier one in the decoded map (the same "last one wins"
/// rule `MetaCommand::TagResource`'s own apply arm applies for a *repeated*
/// call) — DynamoDB imposes no ordering guarantee on this array either way.
fn decode_tag_resource(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = resource_arn_table(obj)?;
    let tags_arr = obj
        .get("Tags")
        .and_then(Value::as_array)
        .ok_or_else(|| WireError::validation("missing array field `Tags`"))?;
    let mut tags = BTreeMap::new();
    for entry in tags_arr {
        let entry = entry
            .as_object()
            .ok_or_else(|| WireError::validation("`Tags` entries must be objects"))?;
        let key = entry
            .get("Key")
            .and_then(Value::as_str)
            .ok_or_else(|| WireError::validation("`Tags` entry missing string field `Key`"))?;
        let value = entry
            .get("Value")
            .and_then(Value::as_str)
            .ok_or_else(|| WireError::validation("`Tags` entry missing string field `Value`"))?;
        tags.insert(key.to_owned(), value.to_owned());
    }
    Ok(Operation::TagResource { table, tags })
}

/// Decode an `UntagResource` body: `ResourceArn` plus `TagKeys`, an array of
/// strings.
fn decode_untag_resource(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = resource_arn_table(obj)?;
    let tag_keys = obj
        .get("TagKeys")
        .and_then(Value::as_array)
        .ok_or_else(|| WireError::validation("missing array field `TagKeys`"))?
        .iter()
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .ok_or_else(|| WireError::validation("`TagKeys` entries must be strings"))
        })
        .collect::<Result<Vec<String>, WireError>>()?;
    Ok(Operation::UntagResource { table, tag_keys })
}

/// Decode a `CreateBackup` body: `TableName` plus `BackupName` (validated
/// against AWS's own shape — 3..=255 characters of `[a-zA-Z0-9_.-]`).
fn decode_create_backup(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    let backup_name = obj
        .get("BackupName")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("missing string field `BackupName`"))?;
    validate_backup_name(backup_name)?;
    Ok(Operation::CreateBackup {
        table,
        backup_name: backup_name.to_owned(),
    })
}

/// AWS's own `BackupName` shape: 3..=255 characters, `[a-zA-Z0-9_.-]` only.
fn validate_backup_name(name: &str) -> Result<(), WireError> {
    let len_ok = (3..=255).contains(&name.len());
    let chars_ok = name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if len_ok && chars_ok {
        Ok(())
    } else {
        Err(WireError::validation(
            "`BackupName` must be 3-255 characters of [a-zA-Z0-9_.-]",
        ))
    }
}

fn backup_arn_field(obj: &Map<String, Value>) -> Result<String, WireError> {
    obj.get("BackupArn")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| WireError::validation("missing string field `BackupArn`"))
}

/// Decode a `ListBackups` body: an optional `TableName` filter, the shared
/// `Limit` contract ([`decode_limit`]), `ExclusiveStartBackupArn`, an
/// optional `TimeRangeLowerBound`/`TimeRangeUpperBound` pair (AWS
/// `Timestamp`s — epoch seconds on the wire, decoded to milliseconds to
/// match `BackupManifest::created_wall_ms`'s own unit), and `BackupType`.
fn decode_list_backups(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = obj
        .get("TableName")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let limit = decode_limit(obj)?;
    let exclusive_start_backup_arn = match obj.get("ExclusiveStartBackupArn") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_str()
                .ok_or_else(|| WireError::validation("`ExclusiveStartBackupArn` must be a string"))?
                .to_owned(),
        ),
    };
    let time_range_lower_bound_ms = decode_backup_timestamp_ms(obj, "TimeRangeLowerBound")?;
    let time_range_upper_bound_ms = decode_backup_timestamp_ms(obj, "TimeRangeUpperBound")?;
    let backup_type = match obj.get("BackupType") {
        None | Some(Value::Null) => BackupTypeFilter::default(),
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| WireError::validation("`BackupType` must be a string"))?;
            match s {
                "USER" => BackupTypeFilter::User,
                "SYSTEM" => BackupTypeFilter::System,
                "AWS_BACKUP" => BackupTypeFilter::AwsBackup,
                "ALL" => BackupTypeFilter::All,
                other => {
                    return Err(WireError::validation(format!(
                        "unknown `BackupType` `{other}`"
                    )));
                }
            }
        }
    };
    Ok(Operation::ListBackups {
        table,
        limit,
        exclusive_start_backup_arn,
        time_range_lower_bound_ms,
        time_range_upper_bound_ms,
        backup_type,
    })
}

/// Decode a `RestoreTableFromBackup` body (ADR 0059 §7, Train 2):
/// `BackupArn` + `TargetTableName`, plus an optional
/// `GlobalSecondaryIndexOverride` array — decoded via the identical
/// GSI-array logic [`decode_indexes`] uses for `CreateTable`'s own
/// `GlobalSecondaryIndexes` field (same shape, same [`MAX_GSI_PER_TABLE`]
/// cap, same duplicate-name rejection), just reading a differently-named
/// top-level field and never looking at `LocalSecondaryIndexes` at all (an
/// LSI is never overridable — real DynamoDB's own contract, and this
/// adapter's manifest is the only source for one either way).
fn decode_restore_table_from_backup(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let backup_arn = backup_arn_field(obj)?;
    let target_table_name = obj
        .get("TargetTableName")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("missing string field `TargetTableName`"))?
        .to_owned();
    let global_secondary_index_override = match obj.get("GlobalSecondaryIndexOverride") {
        None | Some(Value::Null) => None,
        Some(gsis) => {
            let gsis = gsis.as_array().ok_or_else(|| {
                WireError::validation("`GlobalSecondaryIndexOverride` must be an array")
            })?;
            if gsis.len() > MAX_GSI_PER_TABLE {
                return Err(WireError::validation(format!(
                    "too many GlobalSecondaryIndexOverride entries: {} declared, at most \
                     {MAX_GSI_PER_TABLE} allowed per table",
                    gsis.len()
                )));
            }
            let mut out = Vec::with_capacity(gsis.len());
            let mut seen = std::collections::BTreeSet::new();
            for gsi in gsis {
                let (name, schema, projection) = decode_index_entry(gsi, "GSI")?;
                if !seen.insert(name.clone()) {
                    return Err(WireError::validation(format!(
                        "duplicate index name `{name}` in GlobalSecondaryIndexOverride"
                    )));
                }
                out.push(SecondaryIndex::Global(GlobalSecondaryIndex {
                    name,
                    key_attribute: schema.partition_key,
                    sort_attribute: schema.sort_key,
                    projection,
                }));
            }
            Some(out)
        }
    };
    Ok(Operation::RestoreTableFromBackup {
        backup_arn,
        target_table_name,
        global_secondary_index_override,
    })
}

/// Decode a `RestoreTableToPointInTime` body (ADR 0059 §10, Train 3 PR②):
/// `SourceTableName` + `TargetTableName`, plus `RestoreDateTime` (an AWS
/// `Timestamp` — epoch seconds, possibly fractional) and/or
/// `UseLatestRestorableTime` (a bare boolean — only `true` is meaningful;
/// `false`/absent means "not requested," mirroring AWS's own optional-flag
/// shape), and the identical `GlobalSecondaryIndexOverride` array
/// [`decode_restore_table_from_backup`] already decodes. **Not** validating
/// here that exactly one of the two time selectors was given, or that
/// `RestoreDateTime` is present when required — `animusd` does, since it
/// alone knows the currently-restorable window to explain a rejection
/// against.
fn decode_restore_table_to_point_in_time(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let source_table_name = obj
        .get("SourceTableName")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("missing string field `SourceTableName`"))?
        .to_owned();
    let target_table_name = obj
        .get("TargetTableName")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("missing string field `TargetTableName`"))?
        .to_owned();
    let restore_date_time_ms = decode_backup_timestamp_ms(obj, "RestoreDateTime")?;
    let use_latest_restorable_time = match obj.get("UseLatestRestorableTime") {
        None | Some(Value::Null) => false,
        Some(v) => v
            .as_bool()
            .ok_or_else(|| WireError::validation("`UseLatestRestorableTime` must be a boolean"))?,
    };
    let global_secondary_index_override = match obj.get("GlobalSecondaryIndexOverride") {
        None | Some(Value::Null) => None,
        Some(gsis) => {
            let gsis = gsis.as_array().ok_or_else(|| {
                WireError::validation("`GlobalSecondaryIndexOverride` must be an array")
            })?;
            if gsis.len() > MAX_GSI_PER_TABLE {
                return Err(WireError::validation(format!(
                    "too many GlobalSecondaryIndexOverride entries: {} declared, at most \
                     {MAX_GSI_PER_TABLE} allowed per table",
                    gsis.len()
                )));
            }
            let mut out = Vec::with_capacity(gsis.len());
            let mut seen = std::collections::BTreeSet::new();
            for gsi in gsis {
                let (name, schema, projection) = decode_index_entry(gsi, "GSI")?;
                if !seen.insert(name.clone()) {
                    return Err(WireError::validation(format!(
                        "duplicate index name `{name}` in GlobalSecondaryIndexOverride"
                    )));
                }
                out.push(SecondaryIndex::Global(GlobalSecondaryIndex {
                    name,
                    key_attribute: schema.partition_key,
                    sort_attribute: schema.sort_key,
                    projection,
                }));
            }
            Some(out)
        }
    };
    Ok(Operation::RestoreTableToPointInTime {
        source_table_name,
        target_table_name,
        restore_date_time_ms,
        use_latest_restorable_time,
        global_secondary_index_override,
    })
}

/// Decode a `Timestamp` field (a JSON number — epoch seconds, possibly
/// fractional, AWS's own wire shape) into epoch **milliseconds**.
fn decode_backup_timestamp_ms(
    obj: &Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, WireError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let secs = v
                .as_f64()
                .ok_or_else(|| WireError::validation(format!("`{field}` must be a number")))?;
            Ok(Some((secs * 1000.0).max(0.0) as u64))
        }
    }
}

/// Decode an `UpdateItem` body: `Key`, an `UpdateExpression` (`SET`/`REMOVE`
/// clauses), an optional `ConditionExpression`, and an optional `ReturnValues`.
fn decode_update_item(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    let key = decode_item_field(obj, "Key")?;
    let expr = obj
        .get("UpdateExpression")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("missing string field `UpdateExpression`"))?;
    let actions = decode_update_expression(obj, expr)?;
    if actions.is_empty() {
        return Err(WireError::validation(
            "`UpdateExpression` has no SET/REMOVE actions",
        ));
    }
    let condition = decode_condition(obj)?;
    let return_values = decode_update_return_values(obj)?;
    let capacity = decode_return_consumed_capacity(obj)?;
    let metrics = decode_return_item_collection_metrics(obj)?;
    Ok(Operation::UpdateItem {
        table,
        key,
        actions,
        condition,
        return_values,
        capacity,
        metrics,
    })
}

/// A lexical token of an `UpdateExpression`, paired (by [`tokenize_update_expression`])
/// with the paren-nesting depth it sits at. `Word` keeps the exact source
/// slice — an attribute name/path (`a`, `a.b`), a `#alias`, a `:placeholder`,
/// or a clause-keyword spelling (`SET`/`set`/...); which of those it *means*
/// depends on where it sits in the token stream, never on its spelling alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateToken<'a> {
    Word(&'a str),
    Comma,
    Equals,
    LParen,
    RParen,
}

impl UpdateToken<'_> {
    /// Render a token back to text for an error message.
    fn text(&self) -> Cow<'_, str> {
        match self {
            UpdateToken::Word(w) => Cow::Borrowed(w),
            UpdateToken::Comma => Cow::Borrowed(","),
            UpdateToken::Equals => Cow::Borrowed("="),
            UpdateToken::LParen => Cow::Borrowed("("),
            UpdateToken::RParen => Cow::Borrowed(")"),
        }
    }
}

/// Tokenize an `UpdateExpression` into words and punctuation, each paired with
/// the paren-nesting depth (0 = top level) it appears at. A "word" is a
/// maximal run of characters that is none of whitespace, `,`, `=`, `(`, `)` —
/// so an attribute path (`a.b`), a `#alias`, and a `:placeholder` each come
/// out as one token, exactly like the substring scan this replaces treated
/// them, but a clause keyword is now just another word: whether it *starts a
/// clause* is a property of its position in the grammar, decided by the
/// caller, never of its spelling. Depth-tracking exists so a keyword spelled
/// inside function-call parens is never mistaken for a clause start — this
/// grammar has no function calls yet ([`resolve_placeholder`] only accepts a
/// bare `:placeholder`), so depth never actually exceeds 0 in an expression
/// that goes on to parse successfully, but the tracking is the foundation a
/// future document-path/function-call grammar needs.
fn tokenize_update_expression(expr: &str) -> Vec<(UpdateToken<'_>, u32)> {
    let mut tokens = Vec::new();
    let mut depth: u32 = 0;
    let mut chars = expr.char_indices().peekable();
    while let Some(&(start, c)) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        match c {
            ',' => {
                tokens.push((UpdateToken::Comma, depth));
                chars.next();
            }
            '=' => {
                tokens.push((UpdateToken::Equals, depth));
                chars.next();
            }
            '(' => {
                tokens.push((UpdateToken::LParen, depth));
                depth += 1;
                chars.next();
            }
            ')' => {
                depth = depth.saturating_sub(1);
                tokens.push((UpdateToken::RParen, depth));
                chars.next();
            }
            _ => {
                let mut end = start;
                while let Some(&(idx, ch)) = chars.peek() {
                    if ch.is_whitespace() || matches!(ch, ',' | '=' | '(' | ')') {
                        break;
                    }
                    end = idx + ch.len_utf8();
                    chars.next();
                }
                tokens.push((UpdateToken::Word(&expr[start..end]), depth));
            }
        }
    }
    tokens
}

/// The four `UpdateExpression` clause keywords, matched case-insensitively.
/// Returns the canonical lowercase spelling, used only to dispatch — never to
/// decide *whether* a word is a keyword-in-clause-start-position, which is
/// the caller's job.
fn update_clause_keyword(word: &str) -> Option<&'static str> {
    match word {
        w if w.eq_ignore_ascii_case("set") => Some("set"),
        w if w.eq_ignore_ascii_case("remove") => Some("remove"),
        w if w.eq_ignore_ascii_case("add") => Some("add"),
        w if w.eq_ignore_ascii_case("delete") => Some("delete"),
        _ => None,
    }
}

/// Consume one `Word` token, or fail with a message naming what was expected.
fn expect_update_word<'a>(
    tokens: &[(UpdateToken<'a>, u32)],
    i: &mut usize,
    expr: &str,
    what: &str,
) -> Result<&'a str, WireError> {
    match tokens.get(*i) {
        Some((UpdateToken::Word(w), _)) => {
            let w = *w;
            *i += 1;
            Ok(w)
        }
        Some((tok, _)) => Err(WireError::validation(format!(
            "`UpdateExpression` `{expr}` expected {what}, found `{}`",
            tok.text()
        ))),
        None => Err(WireError::validation(format!(
            "`UpdateExpression` `{expr}` expected {what}, found end of expression"
        ))),
    }
}

/// After one completed action, decide what comes next: `,` continues the
/// current clause's action list with another action (return `true`) — this
/// is unconditional, per the grammar, **even when the token right after the
/// comma is spelled like a clause keyword**: `SET add = :v, remove = :w` is
/// two `SET` actions on attributes literally named `add`/`remove`, not a
/// `SET` clause followed by a `REMOVE` clause, because a clause boundary is
/// never introduced by a comma. A clause keyword appearing directly (no
/// comma) or end-of-expression ends this clause (return `false`, leaving
/// `*i` positioned at the keyword or past the end); anything else is a
/// validation error — e.g. `SET a = :v b = :w` is missing its comma.
/// Tolerates one trailing `,` at the very end of the expression (matching
/// the substring scanner this replaces, which stripped a trailing comma off
/// each clause's raw text before splitting it into actions) — that specific
/// case is unambiguous since there is nothing after it to misparse.
fn continues_update_clause(
    tokens: &[(UpdateToken<'_>, u32)],
    i: &mut usize,
    expr: &str,
) -> Result<bool, WireError> {
    match tokens.get(*i) {
        Some((UpdateToken::Comma, _)) => {
            *i += 1;
            // A trailing comma with nothing after it: tolerate, don't try to
            // parse a nonexistent next action.
            Ok(tokens.get(*i).is_some())
        }
        Some((UpdateToken::Word(w), 0)) if update_clause_keyword(w).is_some() => Ok(false),
        None => Ok(false),
        Some((tok, _)) => Err(WireError::validation(format!(
            "`UpdateExpression` `{expr}` expected `,` or a clause keyword \
             (SET, REMOVE, ADD, DELETE), found `{}`",
            tok.text()
        ))),
    }
}

/// Decode a DynamoDB `UpdateExpression` (the supported subset). Recognized
/// clauses are `SET a = expr, b = expr`, `REMOVE c, d`, `ADD e :v`, and
/// `DELETE f :v`, in any order; the attribute names may use `#alias`
/// placeholders and the values `:placeholder`s. A `SET` right-hand side
/// (issue #375 PR1) is one [`UpdateExpr`]: a bare `:value`/path, or a
/// `if_not_exists(path, default)`/`list_append(a, b)` function call — see
/// [`parse_update_set_expr`]. Non-whitespace text before the first
/// recognized clause keyword is rejected too — e.g. `"foo SET x = :v"` —
/// rather than silently dropped, which would otherwise apply only the `SET`
/// and never surface the leading garbage.
///
/// This is a real clause tokenizer ([`tokenize_update_expression`]), not a
/// substring keyword scan: `SET`/`REMOVE`/`ADD`/`DELETE` are recognized as
/// clause-starting keywords **only** at a clause-start grammar position — the
/// very start of the expression, or immediately after a completed action (not
/// after a `,`, which continues that action's own clause, and not anywhere an
/// operand is expected) — so an *unaliased* top-level attribute literally
/// named `set`/`remove`/`add`/`delete` (e.g. `SET set = :v`, or the
/// multi-clause `SET set = :v REMOVE remove ADD add :n DELETE delete :ss`)
/// parses correctly instead of misparsing, closing what used to be a
/// documented gap here.
fn decode_update_expression(
    obj: &Map<String, Value>,
    expr: &str,
) -> Result<Vec<UpdateAction>, WireError> {
    let tokens = tokenize_update_expression(expr);
    // Find the first word, anywhere, that is a clause keyword at top level —
    // regardless of whether *this* parse would actually treat it as a clause
    // start (that grammar-position judgment happens below); this is purely to
    // classify "no clause keyword anywhere" (unsupported expression) from
    // "a clause keyword exists, but there's text before it" (leading garbage).
    let first_keyword_at = tokens.iter().position(|(tok, depth)| match tok {
        UpdateToken::Word(w) => *depth == 0 && update_clause_keyword(w).is_some(),
        _ => false,
    });
    match first_keyword_at {
        None => Err(WireError::validation(format!(
            "unsupported `UpdateExpression` `{expr}` \
             (supported clauses: SET, REMOVE, ADD, DELETE)"
        ))),
        Some(idx) if idx != 0 => Err(WireError::validation(format!(
            "`UpdateExpression` `{expr}` has unrecognized text before its first clause"
        ))),
        Some(_) => parse_update_clauses(obj, expr, &tokens),
    }
}

/// Parse the clause sequence of an already-tokenized `UpdateExpression`,
/// given `tokens[0]` is confirmed to be a clause keyword (checked by
/// [`decode_update_expression`] before calling this).
fn parse_update_clauses(
    obj: &Map<String, Value>,
    expr: &str,
    tokens: &[(UpdateToken<'_>, u32)],
) -> Result<Vec<UpdateAction>, WireError> {
    let mut actions = Vec::new();
    let mut i = 0usize;
    while i < tokens.len() {
        let kw = match tokens[i] {
            (UpdateToken::Word(w), 0) => update_clause_keyword(w),
            _ => None,
        };
        let kw = kw.ok_or_else(|| {
            WireError::validation(format!(
                "`UpdateExpression` `{expr}` expected `,` or a clause keyword \
                 (SET, REMOVE, ADD, DELETE), found `{}`",
                tokens[i].0.text()
            ))
        })?;
        i += 1;
        match kw {
            "set" => loop {
                let path = expect_update_word(tokens, &mut i, expr, "an attribute name")?;
                match tokens.get(i) {
                    Some((UpdateToken::Equals, _)) => i += 1,
                    _ => {
                        return Err(WireError::validation("SET clause must be `attr = :value`"));
                    }
                }
                let attr = resolve_attr_name(obj, path)?;
                let value_expr = parse_update_set_expr(obj, tokens, &mut i, expr)?;
                actions.push(UpdateAction::Set(attr, value_expr));
                if !continues_update_clause(tokens, &mut i, expr)? {
                    break;
                }
            },
            "remove" => loop {
                let path = expect_update_word(tokens, &mut i, expr, "an attribute name")?;
                let attr = resolve_attr_name(obj, path)?;
                actions.push(UpdateAction::Remove(attr));
                if !continues_update_clause(tokens, &mut i, expr)? {
                    break;
                }
            },
            // `ADD`/`DELETE` take `attr :value` pairs separated by spaces,
            // not `=` — a different shape from SET's.
            kw @ ("add" | "delete") => loop {
                let name = expect_update_word(tokens, &mut i, expr, "an attribute name")?;
                let value_tok = expect_update_word(tokens, &mut i, expr, "a `:value` placeholder")
                    .map_err(|_| {
                        WireError::validation(format!(
                            "{} clause must be `attr :value`, got `{name}`",
                            kw.to_uppercase()
                        ))
                    })?;
                let attr = resolve_attr_name(obj, name)?;
                let value = resolve_placeholder(obj, value_tok)?;
                if kw == "add" {
                    // A numeric ADD is the one non-idempotent write this
                    // adapter has. It is safe because
                    // `ClientCtx::cp_kind_write_item` does not re-apply a
                    // non-idempotent write on its own (see
                    // `kind_write_is_idempotent`): DynamoDB's guarantee is
                    // at-most-once per *request*, not exactly-once, so a
                    // client that retries an ADD which actually applied
                    // double-counts there too. What must never happen is
                    // the service counting twice for one request.
                    // Numeric ADD is the adapter's only non-idempotent
                    // write, and it is safe now for two reasons that had
                    // to land first.
                    //
                    // `cp_kind_write_item` no longer re-applies a
                    // non-idempotent write on its own, so at-most-once
                    // per request holds — DynamoDB's own guarantee, under
                    // which a *client* retry of an ADD that applied
                    // double-counts there too.
                    //
                    // And a `KindBatch` now records what it did at apply
                    // time, so a write that applied is acknowledged even
                    // when a concurrent update immediately overwrites it.
                    // Before that, confirmation compared the value back
                    // and reported "superseded ... retry" on 8 of 10
                    // concurrent increments that had in fact applied —
                    // and retrying is precisely what double-counts.
                    // Measured after both: ten concurrent increments are
                    // all accepted and leave the counter at exactly ten.
                    if !matches!(
                        value,
                        AttributeValue::N(_)
                            | AttributeValue::SS(_)
                            | AttributeValue::NS(_)
                            | AttributeValue::BS(_)
                    ) {
                        return Err(WireError::validation(
                            "ADD takes a number or a set operand (N, SS, NS or BS)",
                        ));
                    }
                    actions.push(UpdateAction::Add(attr, value));
                } else {
                    if !matches!(
                        value,
                        AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_)
                    ) {
                        return Err(WireError::validation(
                            "DELETE takes a set operand (SS, NS or BS)",
                        ));
                    }
                    actions.push(UpdateAction::Delete(attr, value));
                }
                if !continues_update_clause(tokens, &mut i, expr)? {
                    break;
                }
            },
            _ => unreachable!("update_clause_keyword only returns the four matched arms"),
        }
    }
    Ok(actions)
}

/// Consume one exact punctuation token (`Comma`/`RParen`/…), or fail with a
/// message naming what was expected. Compares by variant only — the two
/// payload-carrying variants (`Word`) are never passed as `want` here.
fn expect_update_punct(
    tokens: &[(UpdateToken<'_>, u32)],
    i: &mut usize,
    expr: &str,
    want: UpdateToken<'_>,
    what: &str,
) -> Result<(), WireError> {
    match tokens.get(*i) {
        Some((tok, _)) if *tok == want => {
            *i += 1;
            Ok(())
        }
        Some((tok, _)) => Err(WireError::validation(format!(
            "`UpdateExpression` `{expr}` expected {what}, found `{}`",
            tok.text()
        ))),
        None => Err(WireError::validation(format!(
            "`UpdateExpression` `{expr}` expected {what}, found end of expression"
        ))),
    }
}

/// Parse a `SET` clause's right-hand side: currently always a single
/// [`UpdateOperand`] (issue #375 PR1) — a follow-up teaches this arithmetic.
fn parse_update_set_expr(
    obj: &Map<String, Value>,
    tokens: &[(UpdateToken<'_>, u32)],
    i: &mut usize,
    expr: &str,
) -> Result<UpdateExpr, WireError> {
    let operand = parse_update_operand(obj, tokens, i, expr)?;
    Ok(UpdateExpr::Operand(operand))
}

/// Parse one [`UpdateOperand`]: a `:value` placeholder, a top-level
/// attribute name, or `name(...)` — a function call, recognized purely by a
/// `(` immediately following the name (never by the name's own spelling, the
/// same "grammar position decides, not spelling" discipline
/// [`decode_update_expression`]'s own doc establishes for clause keywords).
fn parse_update_operand(
    obj: &Map<String, Value>,
    tokens: &[(UpdateToken<'_>, u32)],
    i: &mut usize,
    expr: &str,
) -> Result<UpdateOperand, WireError> {
    let word = expect_update_word(tokens, i, expr, "a value, path, or function call")?;
    if matches!(tokens.get(*i), Some((UpdateToken::LParen, _))) {
        return parse_update_func_call(obj, tokens, i, expr, word);
    }
    if word.starts_with(':') {
        return Ok(UpdateOperand::Value(resolve_placeholder(obj, word)?));
    }
    Ok(UpdateOperand::Path(resolve_attr_name(obj, word)?))
}

/// Parse a function call's argument list — `tokens[*i]` is the `(` right
/// after `name`, not yet consumed. Only `if_not_exists`/`list_append` are
/// recognized (DynamoDB's names, case-sensitive — unlike the clause
/// keywords, these are never matched case-insensitively).
fn parse_update_func_call(
    obj: &Map<String, Value>,
    tokens: &[(UpdateToken<'_>, u32)],
    i: &mut usize,
    expr: &str,
    name: &str,
) -> Result<UpdateOperand, WireError> {
    *i += 1; // consume '('
    match name {
        "if_not_exists" => {
            let path_word = expect_update_word(tokens, i, expr, "a path")?;
            let path = resolve_attr_name(obj, path_word)?;
            expect_update_punct(tokens, i, expr, UpdateToken::Comma, "`,`")?;
            let default = parse_update_operand(obj, tokens, i, expr)?;
            expect_update_punct(tokens, i, expr, UpdateToken::RParen, "`)`")?;
            Ok(UpdateOperand::IfNotExists(path, Box::new(default)))
        }
        "list_append" => {
            let a = parse_update_operand(obj, tokens, i, expr)?;
            expect_update_punct(tokens, i, expr, UpdateToken::Comma, "`,`")?;
            let b = parse_update_operand(obj, tokens, i, expr)?;
            expect_update_punct(tokens, i, expr, UpdateToken::RParen, "`)`")?;
            Ok(UpdateOperand::ListAppend(Box::new(a), Box::new(b)))
        }
        other => Err(WireError::validation(format!(
            "`UpdateExpression` `{expr}` calls unsupported function `{other}` \
             (supported: if_not_exists, list_append)"
        ))),
    }
}

/// Evaluate a `SET` clause's right-hand side against `item` — the same
/// in-progress item [`apply_update`]'s own fold is building, so a `Path`
/// operand naming an attribute a prior action in the same expression
/// already set sees that action's result (see [`UpdateOperand`]'s own doc
/// on this simplification). `None` (an operand that evaluates to "no
/// value" — a bare absent path, never wrapped in `if_not_exists`) is a
/// `ValidationException`, since `SET` can never assign nothing.
fn eval_update_expr(item: &Item, expr: &UpdateExpr) -> Result<AttributeValue, WireError> {
    let missing = || {
        WireError::validation(
            "The provided expression refers to an attribute that does not exist in the item",
        )
    };
    match expr {
        UpdateExpr::Operand(op) => eval_update_operand(item, op)?.ok_or_else(missing),
    }
}

/// Evaluate one [`UpdateOperand`] against `item`. `Ok(None)` means the
/// operand names an attribute that does not currently exist — a legal
/// intermediate result (`if_not_exists`'s first argument), but never a
/// legal *final* `SET` value; the caller decides whether `None` is an
/// error.
fn eval_update_operand(
    item: &Item,
    operand: &UpdateOperand,
) -> Result<Option<AttributeValue>, WireError> {
    match operand {
        UpdateOperand::Value(v) => Ok(Some(v.clone())),
        UpdateOperand::Path(name) => Ok(item.get(name).cloned()),
        UpdateOperand::IfNotExists(name, default) => match item.get(name) {
            Some(v) => Ok(Some(v.clone())),
            None => eval_update_operand(item, default),
        },
        UpdateOperand::ListAppend(a, b) => {
            let missing =
                || WireError::validation("list_append operand does not exist in the item");
            let a = eval_update_operand(item, a)?.ok_or_else(missing)?;
            let b = eval_update_operand(item, b)?.ok_or_else(missing)?;
            let (AttributeValue::L(mut av), AttributeValue::L(bv)) = (a, b) else {
                return Err(WireError::validation(
                    "list_append operands must both be lists (L)",
                ));
            };
            av.extend(bv);
            Ok(Some(AttributeValue::L(av)))
        }
    }
}

/// Resolve a single top-level attribute name in an update clause, following a
/// `#alias` through `ExpressionAttributeNames`. Document paths are rejected here
/// (updates target a top-level attribute in this subset).
fn resolve_attr_name(obj: &Map<String, Value>, raw: &str) -> Result<String, WireError> {
    let name = if let Some(alias) = raw.strip_prefix('#') {
        obj.get("ExpressionAttributeNames")
            .and_then(Value::as_object)
            .and_then(|m| m.get(&format!("#{alias}")))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                WireError::validation(format!("name placeholder `#{alias}` is not defined"))
            })?
    } else {
        raw
    };
    Ok(reject_path(name)?.to_owned())
}

/// Decode one `TransactWriteItems` action's `ReturnValuesOnConditionCheckFailure`
/// (`NONE`/`ALL_OLD`, ADR 0018's 2026-08-24 `CancellationReasons` amendment).
/// Absent ⇒ `NONE`, matching AWS's own default.
fn decode_rvocf(
    obj: &Map<String, Value>,
) -> Result<ReturnValuesOnConditionCheckFailure, WireError> {
    match obj.get("ReturnValuesOnConditionCheckFailure") {
        None | Some(Value::Null) => Ok(ReturnValuesOnConditionCheckFailure::None),
        Some(v) => match v.as_str() {
            Some("NONE") => Ok(ReturnValuesOnConditionCheckFailure::None),
            Some("ALL_OLD") => Ok(ReturnValuesOnConditionCheckFailure::AllOld),
            Some(other) => Err(WireError::validation(format!(
                "unsupported `ReturnValuesOnConditionCheckFailure` `{other}` \
                 (supported: NONE, ALL_OLD)"
            ))),
            None => Err(WireError::validation(
                "`ReturnValuesOnConditionCheckFailure` must be a string",
            )),
        },
    }
}

/// Decode an `UpdateItem` `ReturnValues` (`NONE`/`ALL_OLD`/`ALL_NEW`). Absent ⇒
/// `NONE`. `UPDATED_OLD`/`UPDATED_NEW` are deferred (rejected).
fn decode_update_return_values(obj: &Map<String, Value>) -> Result<UpdateReturnValues, WireError> {
    match obj.get("ReturnValues") {
        None | Some(Value::Null) => Ok(UpdateReturnValues::None),
        Some(v) => match v.as_str() {
            Some("NONE") => Ok(UpdateReturnValues::None),
            Some("ALL_OLD") => Ok(UpdateReturnValues::AllOld),
            Some("ALL_NEW") => Ok(UpdateReturnValues::AllNew),
            Some("UPDATED_OLD") => Ok(UpdateReturnValues::UpdatedOld),
            Some("UPDATED_NEW") => Ok(UpdateReturnValues::UpdatedNew),
            Some(other) => Err(WireError::validation(format!(
                "unsupported `ReturnValues` `{other}` (supported: NONE, ALL_OLD, ALL_NEW)"
            ))),
            None => Err(WireError::validation("`ReturnValues` must be a string")),
        },
    }
}

/// AWS's per-item size cap: 400 KB (`409600` bytes), computed by
/// [`item_size`] — the sum, over every attribute, of the UTF-8 length of its
/// name plus the size of its value. Enforced on every decoded **write**
/// item: `PutItem`, `BatchWriteItem`'s `PutRequest`s, and
/// `TransactWriteItems`'s `Put` actions ([`check_item_size`]). Also enforced
/// on `UpdateItem`'s post-update result, inside [`apply_update`] itself:
/// that decode-time check alone can't cover a read-modify-write, since the
/// item as it exists before the update may already be under the cap but the
/// applied actions can push it over, so `apply_update` re-checks after
/// folding every action — the single choke point both `UpdateItem` and
/// `TransactWriteItems`'s `Update` action route through at the leader.
pub const MAX_ITEM_SIZE_BYTES: usize = 409_600;

/// Reject `item` if it exceeds [`MAX_ITEM_SIZE_BYTES`], matching real
/// DynamoDB's own `ValidationException` wording.
fn check_item_size(item: &Item) -> Result<(), WireError> {
    if item_size(item) > MAX_ITEM_SIZE_BYTES {
        return Err(WireError::validation(
            "Item size has exceeded the maximum allowed size",
        ));
    }
    Ok(())
}

/// AWS's `BatchWriteItem` cap: at most 25 request items, summed across every
/// table named in `RequestItems`, in one call.
pub const BATCH_WRITE_MAX_ITEMS: usize = 25;

/// AWS's `BatchGetItem` cap: at most 100 keys, summed across every table
/// named in `RequestItems`, in one call.
pub const BATCH_GET_MAX_KEYS: usize = 100;

/// AWS's `TransactWriteItems` cap: at most 100 actions in one call.
pub const TRANSACT_WRITE_MAX_ACTIONS: usize = 100;

/// AWS's `TransactGetItems` cap: at most 100 items in one call.
pub const TRANSACT_GET_MAX_ITEMS: usize = 100;

/// Decode a `BatchWriteItem` body: `{"RequestItems": {table: [{PutRequest|
/// DeleteRequest}, ..], ..}}`. Rejects more than [`BATCH_WRITE_MAX_ITEMS`]
/// request items total across every table, matching real DynamoDB.
fn decode_batch_write(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let items = obj
        .get("RequestItems")
        .and_then(Value::as_object)
        .ok_or_else(|| WireError::validation("missing object field `RequestItems`"))?;
    let mut requests = BTreeMap::new();
    for (table, list) in items {
        let arr = list.as_array().ok_or_else(|| {
            WireError::validation(format!("`RequestItems.{table}` must be an array"))
        })?;
        let mut reqs = Vec::with_capacity(arr.len());
        for entry in arr {
            let e = entry
                .as_object()
                .ok_or_else(|| WireError::validation("each batch request must be an object"))?;
            if let Some(put) = e.get("PutRequest").and_then(Value::as_object) {
                let item = decode_sub_item(put, "Item")?;
                check_item_size(&item)?;
                reqs.push(WriteRequest::Put(item));
            } else if let Some(del) = e.get("DeleteRequest").and_then(Value::as_object) {
                reqs.push(WriteRequest::Delete(decode_sub_item(del, "Key")?));
            } else {
                return Err(WireError::validation(
                    "batch request must have a `PutRequest` or `DeleteRequest`",
                ));
            }
        }
        requests.insert(table.clone(), reqs);
    }
    let total: usize = requests.values().map(Vec::len).sum();
    if total > BATCH_WRITE_MAX_ITEMS {
        return Err(WireError::validation(format!(
            "too many items in BatchWriteItem: {total} requested, at most \
             {BATCH_WRITE_MAX_ITEMS} allowed per call across all tables"
        )));
    }
    Ok(Operation::BatchWriteItem { requests })
}

/// Decode a `TransactWriteItems` body: `{"TransactItems": [{Put|Delete|Update|
/// ConditionCheck}, ..]}`. Rejects more than [`TRANSACT_WRITE_MAX_ACTIONS`]
/// actions, matching real DynamoDB.
fn decode_transact_write(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let items = obj
        .get("TransactItems")
        .and_then(Value::as_array)
        .ok_or_else(|| WireError::validation("missing array field `TransactItems`"))?;
    let mut actions = Vec::with_capacity(items.len());
    for entry in items {
        let e = entry
            .as_object()
            .ok_or_else(|| WireError::validation("each `TransactItems` entry must be an object"))?;
        let (kind, inner) = e.iter().next().filter(|_| e.len() == 1).ok_or_else(|| {
            WireError::validation("each transact item is one Put/Delete/Update/ConditionCheck")
        })?;
        let inner = inner
            .as_object()
            .ok_or_else(|| WireError::validation(format!("`{kind}` must be an object")))?;
        let table = table_name(inner)?;
        let rvocf = decode_rvocf(inner)?;
        let action = match kind.as_str() {
            "Put" => {
                let item = decode_sub_item(inner, "Item")?;
                check_item_size(&item)?;
                TransactAction::Put {
                    table,
                    item,
                    condition: decode_condition(inner)?,
                    rvocf,
                }
            }
            "Delete" => TransactAction::Delete {
                table,
                key: decode_sub_item(inner, "Key")?,
                condition: decode_condition(inner)?,
                rvocf,
            },
            "Update" => {
                let key = decode_sub_item(inner, "Key")?;
                let expr = inner
                    .get("UpdateExpression")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        WireError::validation("transact Update needs `UpdateExpression`")
                    })?;
                TransactAction::Update {
                    table,
                    key,
                    actions: decode_update_expression(inner, expr)?,
                    condition: decode_condition(inner)?,
                    rvocf,
                }
            }
            "ConditionCheck" => TransactAction::ConditionCheck {
                table,
                key: decode_sub_item(inner, "Key")?,
                condition: decode_condition(inner)?.ok_or_else(|| {
                    WireError::validation("ConditionCheck requires a `ConditionExpression`")
                })?,
                rvocf,
            },
            other => {
                return Err(WireError::validation(format!(
                    "unsupported transact action `{other}`"
                )));
            }
        };
        actions.push(action);
    }
    if actions.len() > TRANSACT_WRITE_MAX_ACTIONS {
        return Err(WireError::validation(format!(
            "too many actions in TransactWriteItems: {} requested, at most \
             {TRANSACT_WRITE_MAX_ACTIONS} allowed per call",
            actions.len()
        )));
    }
    let token = decode_client_request_token(obj)?;
    Ok(Operation::TransactWriteItems { actions, token })
}

/// Decode an optional `ClientRequestToken` field: AWS requires 1..=36
/// characters when present (ADR 0018's 2026-08-24 amendment). An absent
/// field is `Ok(None)` — the request opts out of idempotency, unchanged
/// prior behavior. A present-but-wrong-length value is a
/// `ValidationException`, matching AWS's own field-length validation.
fn decode_client_request_token(obj: &Map<String, Value>) -> Result<Option<String>, WireError> {
    match obj.get("ClientRequestToken") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => {
            if (1..=36).contains(&s.chars().count()) {
                Ok(Some(s.clone()))
            } else {
                Err(WireError::validation(format!(
                    "`ClientRequestToken` must be 1 to 36 characters, got {}",
                    s.chars().count()
                )))
            }
        }
        Some(_) => Err(WireError::validation(
            "`ClientRequestToken` must be a string",
        )),
    }
}

/// Decode a `BatchGetItem` body: `{"RequestItems": {"<table>": {"Keys": [..],
/// "ProjectionExpression": .., "ExpressionAttributeNames": .., "ConsistentRead":
/// ..}}}`.
///
/// Unlike `TransactGetItems`, the projection and consistency setting are scoped
/// to a **table**, not to an individual key, so they are decoded once per entry
/// and applied to every key under it.
///
/// `RequestItems` is a JSON object, so the tables arrive in whatever order the
/// map iterates; the response is keyed by table name, so that does not matter.
/// An empty `Keys` list for a table is rejected, as DynamoDB does — it is
/// almost always a client bug rather than an intentional no-op. Rejects more
/// than [`BATCH_GET_MAX_KEYS`] keys total across every table, matching real
/// DynamoDB.
fn decode_batch_get(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let tables = obj
        .get("RequestItems")
        .and_then(Value::as_object)
        .ok_or_else(|| WireError::validation("missing object field `RequestItems`"))?;
    if tables.is_empty() {
        return Err(WireError::validation(
            "`RequestItems` must name at least one table",
        ));
    }
    let mut requests = Vec::with_capacity(tables.len());
    for (table, spec) in tables {
        let spec = spec.as_object().ok_or_else(|| {
            WireError::validation(format!("`RequestItems.{table}` must be an object"))
        })?;
        let raw_keys = spec.get("Keys").and_then(Value::as_array).ok_or_else(|| {
            WireError::validation(format!("`RequestItems.{table}` needs an array `Keys`"))
        })?;
        if raw_keys.is_empty() {
            return Err(WireError::validation(format!(
                "`RequestItems.{table}.Keys` must not be empty"
            )));
        }
        let mut keys = Vec::with_capacity(raw_keys.len());
        for k in raw_keys {
            let map = k.as_object().ok_or_else(|| {
                WireError::validation(format!("each key in `{table}` must be an object"))
            })?;
            keys.push(decode_item(map)?);
        }
        requests.push(BatchGet {
            table: table.clone(),
            keys,
            projection: decode_projection(spec)?,
            consistent_read: decode_consistent_read(spec),
        });
    }
    let total: usize = requests.iter().map(|r| r.keys.len()).sum();
    if total > BATCH_GET_MAX_KEYS {
        return Err(WireError::validation(format!(
            "too many keys in BatchGetItem: {total} requested, at most \
             {BATCH_GET_MAX_KEYS} allowed per call across all tables"
        )));
    }
    Ok(Operation::BatchGetItem { requests })
}

/// Decode a `TransactGetItems` body: `{"TransactItems": [{"Get": {TableName,
/// Key, ProjectionExpression}}, ..]}`. Rejects more than
/// [`TRANSACT_GET_MAX_ITEMS`] items, matching real DynamoDB.
fn decode_transact_get(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let items = obj
        .get("TransactItems")
        .and_then(Value::as_array)
        .ok_or_else(|| WireError::validation("missing array field `TransactItems`"))?;
    let mut gets = Vec::with_capacity(items.len());
    for entry in items {
        let e = entry
            .as_object()
            .ok_or_else(|| WireError::validation("each `TransactItems` entry must be an object"))?;
        let inner = e
            .get("Get")
            .and_then(Value::as_object)
            .ok_or_else(|| WireError::validation("each transact-get item is `{\"Get\": {..}}`"))?;
        let table = table_name(inner)?;
        let key = decode_sub_item(inner, "Key")?;
        let projection = decode_projection(inner)?;
        gets.push(TransactGet {
            table,
            key,
            projection,
        });
    }
    if gets.len() > TRANSACT_GET_MAX_ITEMS {
        return Err(WireError::validation(format!(
            "too many items in TransactGetItems: {} requested, at most \
             {TRANSACT_GET_MAX_ITEMS} allowed per call",
            gets.len()
        )));
    }
    Ok(Operation::TransactGetItems { gets })
}

/// Decode the attribute-map at `field` of a nested request object into an [`Item`].
fn decode_sub_item(obj: &Map<String, Value>, field: &str) -> Result<Item, WireError> {
    decode_item_field(obj, field)
}

/// Decode a `CreateTable` body's `KeySchema` + `AttributeDefinitions` into a
/// [`TableSchema`]. We use the `KeySchema` (`HASH` / `RANGE`) for the key
/// attribute names; `AttributeDefinitions` types are accepted but, since our
/// model carries types per value, not enforced.
fn decode_key_schema(obj: &Map<String, Value>) -> Result<TableSchema, WireError> {
    let key_schema = obj
        .get("KeySchema")
        .and_then(Value::as_array)
        .ok_or_else(|| WireError::validation("missing array field `KeySchema`"))?;
    let mut partition_key = None;
    let mut sort_key = None;
    for entry in key_schema {
        let e = entry
            .as_object()
            .ok_or_else(|| WireError::validation("each `KeySchema` entry must be an object"))?;
        let name = e
            .get("AttributeName")
            .and_then(Value::as_str)
            .ok_or_else(|| WireError::validation("`KeySchema` entry missing `AttributeName`"))?;
        let role = e
            .get("KeyType")
            .and_then(Value::as_str)
            .ok_or_else(|| WireError::validation("`KeySchema` entry missing `KeyType`"))?;
        match role {
            "HASH" => partition_key = Some(name.to_owned()),
            "RANGE" => sort_key = Some(name.to_owned()),
            other => {
                return Err(WireError::validation(format!(
                    "unknown KeyType `{other}` (expected HASH or RANGE)"
                )));
            }
        }
    }
    let partition_key = partition_key
        .ok_or_else(|| WireError::validation("`KeySchema` has no HASH (partition) key"))?;
    Ok(TableSchema {
        partition_key,
        sort_key,
    })
}

/// Decode `AttributeDefinitions` into `(AttributeName, AttributeType)` pairs (the
/// declared `S`/`N`/`B` for each key attribute). Absent or malformed entries are
/// skipped — the schema bridge defaults a missing type to `String`.
fn decode_attribute_types(obj: &Map<String, Value>) -> Vec<(String, String)> {
    obj.get("AttributeDefinitions")
        .and_then(Value::as_array)
        .map(|defs| {
            defs.iter()
                .filter_map(|d| {
                    let d = d.as_object()?;
                    let name = d.get("AttributeName").and_then(Value::as_str)?;
                    let ty = d.get("AttributeType").and_then(Value::as_str)?;
                    Some((name.to_owned(), ty.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// AWS's cap on a table's `GlobalSecondaryIndexes`, declared at `CreateTable`
/// or accumulated one at a time via `UpdateTable` (`animusd::dynamo::
/// create_index`, which has the replicated catalog this pure decoder
/// doesn't, checks the running total against this same constant).
pub const MAX_GSI_PER_TABLE: usize = 20;

/// AWS's cap on a table's `LocalSecondaryIndexes` — create-time-only in real
/// DynamoDB, so this is the only place it is ever checked.
pub const MAX_LSI_PER_TABLE: usize = 5;

/// Decode the optional `GlobalSecondaryIndexes` + `LocalSecondaryIndexes` of a
/// `CreateTable` into a list of [`SecondaryIndex`]. A GSI's `KeySchema` is a
/// `HASH` attribute plus an optional `RANGE` (a composite GSI); an LSI's
/// `KeySchema` shares the base partition `HASH` and adds a `RANGE` (the index's
/// alternate sort key). Absent ⇒ an empty list. Rejects more than
/// [`MAX_GSI_PER_TABLE`] GSIs or [`MAX_LSI_PER_TABLE`] LSIs, matching real
/// DynamoDB.
fn decode_indexes(obj: &Map<String, Value>) -> Result<Vec<SecondaryIndex>, WireError> {
    let mut out = Vec::new();
    if let Some(gsis) = obj.get("GlobalSecondaryIndexes") {
        let gsis = gsis
            .as_array()
            .ok_or_else(|| WireError::validation("`GlobalSecondaryIndexes` must be an array"))?;
        if gsis.len() > MAX_GSI_PER_TABLE {
            return Err(WireError::validation(format!(
                "too many GlobalSecondaryIndexes: {} declared, at most {MAX_GSI_PER_TABLE} \
                 allowed per table",
                gsis.len()
            )));
        }
        for gsi in gsis {
            let (name, schema, projection) = decode_index_entry(gsi, "GSI")?;
            out.push(SecondaryIndex::Global(GlobalSecondaryIndex {
                name,
                key_attribute: schema.partition_key,
                sort_attribute: schema.sort_key,
                projection,
            }));
        }
    }
    if let Some(lsis) = obj.get("LocalSecondaryIndexes") {
        let lsis = lsis
            .as_array()
            .ok_or_else(|| WireError::validation("`LocalSecondaryIndexes` must be an array"))?;
        if lsis.len() > MAX_LSI_PER_TABLE {
            return Err(WireError::validation(format!(
                "too many LocalSecondaryIndexes: {} declared, at most {MAX_LSI_PER_TABLE} \
                 allowed per table",
                lsis.len()
            )));
        }
        let base = decode_key_schema(obj)?;
        for lsi in lsis {
            let (name, schema, projection) = decode_index_entry(lsi, "LSI")?;
            // An LSI's HASH must be the base table's partition key, and it must
            // declare a RANGE (its alternate sort key).
            if schema.partition_key != base.partition_key {
                return Err(WireError::validation(format!(
                    "LSI `{name}` HASH key must be the base partition key `{}`",
                    base.partition_key
                )));
            }
            let sort_attribute = schema.sort_key.ok_or_else(|| {
                WireError::validation(format!("LSI `{name}` must declare a RANGE (sort) key"))
            })?;
            out.push(SecondaryIndex::Local(LocalSecondaryIndex {
                name,
                sort_attribute,
                projection,
            }));
        }
    }
    // Index names must be unique across both kinds.
    let mut seen = std::collections::BTreeSet::new();
    for index in &out {
        if !seen.insert(index.name().to_owned()) {
            return Err(WireError::validation(format!(
                "duplicate index name `{}`",
                index.name()
            )));
        }
    }
    Ok(out)
}

/// Decode one index declaration object into its `(name, KeySchema, Projection)`.
fn decode_index_entry(
    value: &Value,
    kind: &str,
) -> Result<(String, TableSchema, IndexProjection), WireError> {
    let g = value
        .as_object()
        .ok_or_else(|| WireError::validation(format!("each {kind} must be an object")))?;
    let name = g
        .get("IndexName")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation(format!("{kind} missing `IndexName`")))?
        .to_owned();
    let schema = decode_key_schema(g)?;
    let projection = decode_index_projection(g)?;
    Ok((name, schema, projection))
}

/// Decode an index's optional `Projection` object
/// (`{"ProjectionType":"ALL"|"KEYS_ONLY"|"INCLUDE", "NonKeyAttributes":[..]}`).
/// Absent ⇒ `ALL`. `INCLUDE` requires a non-empty `NonKeyAttributes` list; the
/// other types must not carry one.
fn decode_index_projection(g: &Map<String, Value>) -> Result<IndexProjection, WireError> {
    let Some(proj) = g.get("Projection") else {
        return Ok(IndexProjection::All);
    };
    let proj = proj
        .as_object()
        .ok_or_else(|| WireError::validation("`Projection` must be an object"))?;
    let kind = proj
        .get("ProjectionType")
        .and_then(Value::as_str)
        .unwrap_or("ALL");
    let non_key = proj.get("NonKeyAttributes");
    match kind {
        "ALL" => Ok(IndexProjection::All),
        "KEYS_ONLY" => Ok(IndexProjection::KeysOnly),
        "INCLUDE" => {
            let arr = non_key.and_then(Value::as_array).ok_or_else(|| {
                WireError::validation("INCLUDE projection needs `NonKeyAttributes`")
            })?;
            let mut names = Vec::with_capacity(arr.len());
            for v in arr {
                let name = v.as_str().ok_or_else(|| {
                    WireError::validation("`NonKeyAttributes` elements must be strings")
                })?;
                names.push(reject_path(name)?.to_owned());
            }
            if names.is_empty() {
                return Err(WireError::validation("INCLUDE `NonKeyAttributes` is empty"));
            }
            Ok(IndexProjection::Include(names))
        }
        other => Err(WireError::validation(format!(
            "unsupported ProjectionType `{other}` (ALL, KEYS_ONLY, INCLUDE)"
        ))),
    }
}

/// Decode a `CreateTable`'s optional `StreamSpecification` (ADR 0042 §2) into
/// the declared view type, or `None` if the request declares no stream (no
/// `StreamSpecification` at all, or `StreamEnabled: false`).
fn decode_create_table_stream_spec(
    obj: &Map<String, Value>,
) -> Result<Option<StreamViewType>, WireError> {
    let Some(spec) = obj.get("StreamSpecification") else {
        return Ok(None);
    };
    let spec = spec
        .as_object()
        .ok_or_else(|| WireError::validation("`StreamSpecification` must be an object"))?;
    let enabled = spec
        .get("StreamEnabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !enabled {
        return Ok(None);
    }
    Ok(Some(decode_stream_view_type(spec)?))
}

/// Decode a `StreamSpecification` object's required `StreamViewType`.
fn decode_stream_view_type(spec: &Map<String, Value>) -> Result<StreamViewType, WireError> {
    let raw = spec
        .get("StreamViewType")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            WireError::validation(
                "`StreamSpecification` with `StreamEnabled: true` requires `StreamViewType`",
            )
        })?;
    match raw {
        "NEW_AND_OLD_IMAGES" => Ok(StreamViewType::NewAndOldImages),
        "NEW_IMAGE" => Ok(StreamViewType::NewImage),
        "OLD_IMAGE" => Ok(StreamViewType::OldImage),
        "KEYS_ONLY" => Ok(StreamViewType::KeysOnly),
        other => Err(WireError::validation(format!(
            "unsupported `StreamViewType` `{other}` (expected NEW_AND_OLD_IMAGES, \
             NEW_IMAGE, OLD_IMAGE, or KEYS_ONLY)"
        ))),
    }
}

/// The wire string for a [`StreamViewType`], for response encoding. `pub(crate)`
/// so `streams_wire` (the DynamoDB Streams service's own wire module) can
/// render the identical string for `DescribeStream`'s `StreamViewType`
/// field without a second source of truth for the mapping.
pub(crate) fn stream_view_type_str(vt: StreamViewType) -> &'static str {
    match vt {
        StreamViewType::NewAndOldImages => "NEW_AND_OLD_IMAGES",
        StreamViewType::NewImage => "NEW_IMAGE",
        StreamViewType::OldImage => "OLD_IMAGE",
        StreamViewType::KeysOnly => "KEYS_ONLY",
    }
}

/// `UpdateTable` top-level keys this adapter never implements a change for —
/// there is no provisioned-capacity model, no encryption-at-rest toggle, and
/// no global-tables replica set (see `website/compatibility.html`'s "no
/// billing meter" framing). Present unconditionally, so a body carrying only
/// one of these is a clear `ValidationException` naming the key rather than
/// the generic "requires either..." fallback or, worse, a silent no-op.
const UNSUPPORTED_UPDATE_TABLE_KEYS: &[&str] = &[
    "SSESpecification",
    "ReplicaUpdates",
    "ProvisionedThroughput",
];

/// Reject any `UpdateTable` top-level key this adapter doesn't model, each
/// with its own named `ValidationException` (mirroring
/// [`decode_index_updates`]'s per-shape rejections). `BillingMode` is a
/// deliberate special case, not a blanket rejection: `CreateTable` already
/// accepts (and never inspects) `BillingMode: "PAY_PER_REQUEST"` — the only
/// billing mode this adapter has, since there is no provisioned-capacity
/// model to switch to — so an `UpdateTable` re-asserting that same value is
/// tolerated the same way (a common SDK/CLI habit, e.g. `aws dynamodb
/// update-table --billing-mode PAY_PER_REQUEST`), while any other value
/// (`"PROVISIONED"`, or a non-string) is rejected by name. Tolerating the
/// key does not by itself satisfy the call — a `BillingMode`-only body still
/// falls through to the "requires either..." rejection below, since this
/// adapter models no billing-mode *change*, only the redundant restatement
/// of the mode it already has.
fn reject_unsupported_update_table_keys(obj: &Map<String, Value>) -> Result<(), WireError> {
    for key in UNSUPPORTED_UPDATE_TABLE_KEYS {
        if obj.contains_key(*key) {
            return Err(WireError::validation(format!(
                "UpdateTable: {key} is not supported"
            )));
        }
    }
    if let Some(mode) = obj.get("BillingMode")
        && mode.as_str() != Some("PAY_PER_REQUEST")
    {
        return Err(WireError::validation(
            "UpdateTable: BillingMode is not supported (only PAY_PER_REQUEST, \
             this adapter's only billing mode, may be restated)",
        ));
    }
    Ok(())
}

/// Decode an `UpdateTable` request: either a `StreamSpecification` change
/// (ADR 0042 §2) or a single `GlobalSecondaryIndexUpdates` element (ADR 0045
/// §6) — rejected up front if **both** are present in the same call (Fork
/// C, kept as "exactly one supported change per call"). `StreamEnabled: true`
/// decodes to [`StreamUpdate::Enable`] (requiring `StreamViewType`),
/// `StreamEnabled: false` to [`StreamUpdate::Disable`]; index-update decoding
/// is [`decode_index_updates`]. Any other shape (neither field present) is
/// rejected — this adapter models no throughput/key/billing-mode change. See
/// [`reject_unsupported_update_table_keys`] for the explicit per-key
/// rejections (`SSESpecification`/`ReplicaUpdates`/`ProvisionedThroughput`
/// always, `BillingMode` unless restating `PAY_PER_REQUEST`) this runs
/// before either shape is considered.
/// An index-update call's own `AttributeDefinitions` (issue #319) is decoded
/// via [`decode_attribute_types`] into [`Operation::UpdateTable`]'s
/// `key_types` field — empty for a stream change, which never introduces a
/// new key attribute.
fn decode_update_table(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    reject_unsupported_update_table_keys(obj)?;
    let has_index_updates = obj.contains_key("GlobalSecondaryIndexUpdates");
    let has_stream_spec = obj.contains_key("StreamSpecification");
    if has_index_updates && has_stream_spec {
        return Err(WireError::validation(
            "UpdateTable supports either a GlobalSecondaryIndexUpdates change or a \
             StreamSpecification change in one call, not both (ADR 0045 §6)",
        ));
    }
    if has_index_updates {
        let index_update = decode_index_updates(obj)?;
        let key_types = decode_attribute_types(obj);
        return Ok(Operation::UpdateTable {
            table,
            stream: None,
            index_update: Some(index_update),
            key_types,
        });
    }
    let Some(spec) = obj.get("StreamSpecification") else {
        return Err(WireError::validation(
            "UpdateTable requires either a StreamSpecification or a \
             GlobalSecondaryIndexUpdates change in this adapter",
        ));
    };
    let spec = spec
        .as_object()
        .ok_or_else(|| WireError::validation("`StreamSpecification` must be an object"))?;
    let enabled = spec
        .get("StreamEnabled")
        .and_then(Value::as_bool)
        .ok_or_else(|| WireError::validation("`StreamSpecification` missing `StreamEnabled`"))?;
    let stream = if enabled {
        StreamUpdate::Enable(decode_stream_view_type(spec)?)
    } else {
        StreamUpdate::Disable
    };
    Ok(Operation::UpdateTable {
        table,
        stream: Some(stream),
        index_update: None,
        key_types: Vec::new(),
    })
}

/// Decode `GlobalSecondaryIndexUpdates` (ADR 0045 §6) into a single
/// [`IndexUpdate`]. AWS accepts an array here (nominally one `Create`/
/// `Update`/`Delete` element per changed index, several per call); this
/// adapter accepts **exactly one** element, and that element must be exactly
/// one of `Create` or `Delete` (no `Update` — no throughput model at all,
/// and no combined-key object — each is its own decode error).
fn decode_index_updates(obj: &Map<String, Value>) -> Result<IndexUpdate, WireError> {
    let updates = obj
        .get("GlobalSecondaryIndexUpdates")
        .and_then(Value::as_array)
        .ok_or_else(|| WireError::validation("`GlobalSecondaryIndexUpdates` must be an array"))?;
    if updates.len() != 1 {
        return Err(WireError::validation(
            "UpdateTable supports exactly one GlobalSecondaryIndexUpdates element per call",
        ));
    }
    let entry = updates[0].as_object().ok_or_else(|| {
        WireError::validation("each GlobalSecondaryIndexUpdates element must be an object")
    })?;
    match (entry.get("Create"), entry.get("Delete"), entry.len()) {
        (Some(create), None, 1) => {
            let (name, schema, projection) = decode_index_entry(create, "GSI")?;
            Ok(IndexUpdate::Create(SecondaryIndex::Global(
                GlobalSecondaryIndex {
                    name,
                    key_attribute: schema.partition_key,
                    sort_attribute: schema.sort_key,
                    projection,
                },
            )))
        }
        (None, Some(delete), 1) => {
            let delete = delete
                .as_object()
                .ok_or_else(|| WireError::validation("`Delete` must be an object"))?;
            let name = delete
                .get("IndexName")
                .and_then(Value::as_str)
                .ok_or_else(|| WireError::validation("`Delete` missing `IndexName`"))?
                .to_owned();
            Ok(IndexUpdate::Delete(name))
        }
        _ => Err(WireError::validation(
            "each GlobalSecondaryIndexUpdates element must be exactly one of `Create` or \
             `Delete` (no `Update` — no throughput model)",
        )),
    }
}

/// Decode an `UpdateTimeToLive` request (ADR 0051): `TableName` plus a
/// `TimeToLiveSpecification` object carrying both `Enabled` and
/// `AttributeName` — both required by AWS, matched here exactly (AWS
/// requires `AttributeName` even on a disable call, since it must name the
/// attribute being disabled).
fn decode_update_time_to_live(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    let spec = obj
        .get("TimeToLiveSpecification")
        .and_then(Value::as_object)
        .ok_or_else(|| WireError::validation("missing object field `TimeToLiveSpecification`"))?;
    let enabled = spec
        .get("Enabled")
        .and_then(Value::as_bool)
        .ok_or_else(|| WireError::validation("`TimeToLiveSpecification` missing `Enabled`"))?;
    let attribute_name = spec
        .get("AttributeName")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("`TimeToLiveSpecification` missing `AttributeName`"))?
        .to_owned();
    Ok(Operation::UpdateTimeToLive {
        table,
        attribute_name,
        enabled,
    })
}

/// Decode an `UpdateContinuousBackups` request (ADR 0059 §9): `TableName`
/// plus a `PointInTimeRecoverySpecification` object carrying
/// `PointInTimeRecoveryEnabled`.
fn decode_update_continuous_backups(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    let spec = obj
        .get("PointInTimeRecoverySpecification")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            WireError::validation("missing object field `PointInTimeRecoverySpecification`")
        })?;
    let enabled = spec
        .get("PointInTimeRecoveryEnabled")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            WireError::validation(
                "`PointInTimeRecoverySpecification` missing `PointInTimeRecoveryEnabled`",
            )
        })?;
    Ok(Operation::UpdateContinuousBackups { table, enabled })
}

/// Decode the optional `ConditionExpression` + `ExpressionAttributeValues` of a
/// write. Supported forms: `attribute_not_exists(attr)`,
/// `attribute_exists(attr)`, and `attr = :placeholder`. Absent ⇒ `Ok(None)`.
fn decode_condition(obj: &Map<String, Value>) -> Result<Option<ConditionExpression>, WireError> {
    decode_predicate(obj, "ConditionExpression")
}

/// Decode the optional `ProjectionExpression` (a comma-separated list of
/// top-level attribute names, with `#name` placeholders resolved against
/// `ExpressionAttributeNames`) or the legacy `AttributesToGet` (a JSON array of
/// names). At most one may be present. Absent ⇒ `Ok(None)` (all attributes).
/// Decode `Select`, validating it against the rest of the request the way
/// DynamoDB does rather than accepting it and ignoring it.
///
/// Absent, it is inferred: a request carrying a projection means
/// `SPECIFIC_ATTRIBUTES`; otherwise an index read defaults to
/// `ALL_PROJECTED_ATTRIBUTES` and a base-table read to `ALL_ATTRIBUTES`.
///
/// Present, three rules are enforced, each of which AWS rejects and this
/// adapter previously ignored:
/// - `SPECIFIC_ATTRIBUTES` **requires** a projection (otherwise the request
///   names no attributes at all);
/// - every other value **forbids** one (the two would contradict);
/// - `ALL_PROJECTED_ATTRIBUTES` requires an `IndexName` (there is no
///   projection to speak of on a base table).
fn decode_select(
    obj: &Map<String, Value>,
    index: Option<&str>,
    projection: Option<&Projection>,
) -> Result<Select, WireError> {
    let Some(raw) = obj.get("Select") else {
        return Ok(if projection.is_some() {
            Select::SpecificAttributes
        } else if index.is_some() {
            Select::AllProjectedAttributes
        } else {
            Select::AllAttributes
        });
    };
    let name = raw
        .as_str()
        .ok_or_else(|| WireError::validation("Select must be a string"))?;
    let select = match name {
        "ALL_ATTRIBUTES" => Select::AllAttributes,
        "ALL_PROJECTED_ATTRIBUTES" => Select::AllProjectedAttributes,
        "SPECIFIC_ATTRIBUTES" => Select::SpecificAttributes,
        "COUNT" => Select::Count,
        other => {
            return Err(WireError::validation(format!(
                "unknown Select value {other:?}; expected one of ALL_ATTRIBUTES, \
                 ALL_PROJECTED_ATTRIBUTES, SPECIFIC_ATTRIBUTES, COUNT"
            )));
        }
    };
    match select {
        Select::SpecificAttributes if projection.is_none() => {
            return Err(WireError::validation(
                "Select=SPECIFIC_ATTRIBUTES requires a ProjectionExpression \
                 (or the legacy AttributesToGet)",
            ));
        }
        Select::SpecificAttributes => {}
        _ if projection.is_some() => {
            return Err(WireError::validation(
                "a ProjectionExpression (or the legacy AttributesToGet) may \
                 only be combined with Select=SPECIFIC_ATTRIBUTES",
            ));
        }
        Select::AllProjectedAttributes if index.is_none() => {
            return Err(WireError::validation(
                "Select=ALL_PROJECTED_ATTRIBUTES is only valid with an IndexName",
            ));
        }
        _ => {}
    }
    Ok(select)
}

fn decode_projection(obj: &Map<String, Value>) -> Result<Option<Projection>, WireError> {
    let has_expr = obj.contains_key("ProjectionExpression");
    let has_legacy = obj.contains_key("AttributesToGet");
    if has_expr && has_legacy {
        return Err(WireError::validation(
            "supply at most one of `ProjectionExpression` / `AttributesToGet`",
        ));
    }
    if let Some(expr) = obj.get("ProjectionExpression").and_then(Value::as_str) {
        let paths = expr
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|raw| parse_projection_path(obj, raw))
            .collect::<Result<Vec<_>, _>>()?;
        if paths.is_empty() {
            return Err(WireError::validation("`ProjectionExpression` is empty"));
        }
        return Ok(Some(Projection(paths)));
    }
    if let Some(arr) = obj.get("AttributesToGet") {
        let arr = arr
            .as_array()
            .ok_or_else(|| WireError::validation("`AttributesToGet` must be an array"))?;
        let mut paths = Vec::with_capacity(arr.len());
        for v in arr {
            let name = v.as_str().ok_or_else(|| {
                WireError::validation("`AttributesToGet` elements must be strings")
            })?;
            paths.push(vec![PathSegment::Field(reject_path(name)?.to_owned())]);
        }
        if paths.is_empty() {
            return Err(WireError::validation("`AttributesToGet` is empty"));
        }
        return Ok(Some(Projection(paths)));
    }
    Ok(None)
}

/// Parse one `ProjectionExpression` element into a document path: a
/// `.`-separated sequence of segments, each an attribute name (or `#alias`,
/// resolved through `ExpressionAttributeNames`) optionally followed by one or
/// more `[n]` list-index suffixes (e.g. `#p.list[0][1]` → `Field("profile")`,
/// `Field("list")`, `Index(0)`, `Index(1)`, once `#p` resolves to `profile`).
fn parse_projection_path(
    obj: &Map<String, Value>,
    raw: &str,
) -> Result<Vec<PathSegment>, WireError> {
    let mut segments = Vec::new();
    for seg in raw.split('.') {
        let seg = seg.trim();
        if seg.is_empty() {
            return Err(WireError::validation(format!(
                "projection path `{raw}` has an empty segment"
            )));
        }
        segments.extend(parse_projection_segment(obj, raw, seg)?);
    }
    Ok(segments)
}

/// Parse one `.`-separated piece of a projection path (`seg`, e.g. `list[0][1]`
/// or `#p`) into a field segment plus any trailing index segments. `raw` is
/// the whole path, kept only for error messages.
fn parse_projection_segment(
    obj: &Map<String, Value>,
    raw: &str,
    seg: &str,
) -> Result<Vec<PathSegment>, WireError> {
    let bracket = seg.find('[');
    let (name_part, rest) = match bracket {
        Some(i) => (&seg[..i], &seg[i..]),
        None => (seg, ""),
    };
    if name_part.is_empty() {
        return Err(WireError::validation(format!(
            "malformed list-index syntax in projection path `{raw}` (no attribute name before `[`)"
        )));
    }
    let name = if let Some(alias) = name_part.strip_prefix('#') {
        let names = obj
            .get("ExpressionAttributeNames")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                WireError::validation(format!(
                    "projection uses name placeholder `#{alias}` but `ExpressionAttributeNames` is absent"
                ))
            })?;
        names
            .get(&format!("#{alias}"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                WireError::validation(format!("name placeholder `#{alias}` is not defined"))
            })?
            .to_owned()
    } else {
        name_part.to_owned()
    };
    let mut out = vec![PathSegment::Field(name)];
    out.extend(parse_index_chain(rest, raw)?);
    Ok(out)
}

/// Parse a chain of `[n]` list-index suffixes (`rest`, e.g. `[0][1]`) into
/// `PathSegment::Index` entries. `raw` is the whole path, for error messages.
/// Each index must be a plain non-negative decimal integer — malformed syntax
/// (`[`, `[x]`, `[-1]`, `[01]`... any non-digit body) is a `ValidationException`,
/// matching how DynamoDB rejects it rather than DynamoDB's own index type
/// silently misinterpreting it.
fn parse_index_chain(mut rest: &str, raw: &str) -> Result<Vec<PathSegment>, WireError> {
    let malformed = || {
        WireError::validation(format!(
            "malformed list-index syntax in projection path `{raw}`"
        ))
    };
    let mut indices = Vec::new();
    while !rest.is_empty() {
        rest = rest.strip_prefix('[').ok_or_else(malformed)?;
        let close = rest.find(']').ok_or_else(malformed)?;
        let digits = &rest[..close];
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(malformed());
        }
        let index: usize = digits.parse().map_err(|_| malformed())?;
        indices.push(PathSegment::Index(index));
        rest = &rest[close + 1..];
    }
    Ok(indices)
}

/// Reject a non-top-level attribute name (containing `.` or `[`), used where only
/// a flat attribute name is meaningful (`AttributesToGet`, index
/// `NonKeyAttributes`).
fn reject_path(name: &str) -> Result<&str, WireError> {
    if name.contains('.') || name.contains('[') {
        return Err(WireError::validation(format!(
            "attribute path `{name}` is not supported here (top-level names only)"
        )));
    }
    Ok(name)
}

/// Decode the optional `ReturnValues` field of a write. We support `NONE`
/// (default) and `ALL_OLD`; `UPDATED_OLD`/`ALL_NEW`/`UPDATED_NEW` apply only to
/// `UpdateItem` (not implemented), so they are rejected for `Put`/`Delete`.
fn decode_return_values(obj: &Map<String, Value>) -> Result<ReturnValues, WireError> {
    match obj.get("ReturnValues") {
        None | Some(Value::Null) => Ok(ReturnValues::None),
        Some(v) => match v.as_str() {
            Some("NONE") => Ok(ReturnValues::None),
            Some("ALL_OLD") => Ok(ReturnValues::AllOld),
            Some(other) => Err(WireError::validation(format!(
                "unsupported `ReturnValues` `{other}` (supported: NONE, ALL_OLD)"
            ))),
            None => Err(WireError::validation("`ReturnValues` must be a string")),
        },
    }
}

/// Decode `ReturnConsumedCapacity` (ADR 0006): how much of the capacity report
/// the caller wants back. Absent ⇒ `NONE`, matching DynamoDB's default of
/// saying nothing.
fn decode_return_consumed_capacity(
    obj: &Map<String, Value>,
) -> Result<ReturnConsumedCapacity, WireError> {
    match obj.get("ReturnConsumedCapacity") {
        None | Some(Value::Null) => Ok(ReturnConsumedCapacity::None),
        Some(v) => match v.as_str() {
            Some("NONE") => Ok(ReturnConsumedCapacity::None),
            Some("TOTAL") => Ok(ReturnConsumedCapacity::Total),
            Some("INDEXES") => Ok(ReturnConsumedCapacity::Indexes),
            Some(other) => Err(WireError::validation(format!(
                "unsupported `ReturnConsumedCapacity` `{other}` \
                 (supported: NONE, TOTAL, INDEXES)"
            ))),
            None => Err(WireError::validation(
                "`ReturnConsumedCapacity` must be a string",
            )),
        },
    }
}

/// Decode `ReturnItemCollectionMetrics` (ADR 0006). Absent ⇒ `NONE`.
fn decode_return_item_collection_metrics(
    obj: &Map<String, Value>,
) -> Result<ReturnItemCollectionMetrics, WireError> {
    match obj.get("ReturnItemCollectionMetrics") {
        None | Some(Value::Null) => Ok(ReturnItemCollectionMetrics::None),
        Some(v) => match v.as_str() {
            Some("NONE") => Ok(ReturnItemCollectionMetrics::None),
            Some("SIZE") => Ok(ReturnItemCollectionMetrics::Size),
            Some(other) => Err(WireError::validation(format!(
                "unsupported `ReturnItemCollectionMetrics` `{other}` (supported: NONE, SIZE)"
            ))),
            None => Err(WireError::validation(
                "`ReturnItemCollectionMetrics` must be a string",
            )),
        },
    }
}

/// Decode a predicate from the string field named `field` (one of
/// `ConditionExpression` / `FilterExpression`) into a [`ConditionExpression`]:
/// `attribute_not_exists(attr)`, `attribute_exists(attr)`, or `attr = :v`
/// (resolved against `ExpressionAttributeValues`). Absent ⇒ `Ok(None)`.
fn decode_predicate(
    obj: &Map<String, Value>,
    field: &str,
) -> Result<Option<ConditionExpression>, WireError> {
    let Some(expr) = obj.get(field).and_then(Value::as_str) else {
        return Ok(None);
    };
    decode_predicate_or(obj, field, expr.trim()).map(Some)
}

/// Find a top-level (paren-depth-zero) occurrence of `needle`, case-insensitively,
/// scanning **right to left** so the split is left-associative.
///
/// Two things make this less trivial than a `find`. Parenthesised groups must be
/// skipped, or `(a = :x OR b = :y) AND c = :z` splits inside the group. And a
/// `BETWEEN`'s own ` AND ` — as in `a BETWEEN :lo AND :hi` — is *not* a
/// combinator: it belongs to the term. That one is handled by refusing to split
/// on an ` AND ` that a `BETWEEN` at the same depth is still waiting for.
fn find_top_level(haystack: &str, needle: &str) -> Option<usize> {
    let lower = haystack.to_ascii_lowercase();
    let needle = needle.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut depth = 0i32;
    // Positions of the token, recorded left to right, then chosen from the right.
    let mut hits: Vec<usize> = Vec::new();
    // How many ` AND `s the BETWEENs seen so far at depth 0 still owe.
    let mut pending_between = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            if lower[i..].starts_with(" between ") {
                pending_between += 1;
            }
            if lower[i..].starts_with(&needle) {
                if needle == " and " && pending_between > 0 {
                    // This AND closes a BETWEEN rather than joining two terms.
                    pending_between -= 1;
                } else {
                    hits.push(i);
                }
            }
        }
        i += 1;
    }
    hits.pop()
}

/// `OR` — the loosest binding, so it splits first.
fn decode_predicate_or(
    obj: &Map<String, Value>,
    field: &str,
    expr: &str,
) -> Result<ConditionExpression, WireError> {
    let expr = expr.trim();
    if let Some(at) = find_top_level(expr, " OR ") {
        let lhs = decode_predicate_or(obj, field, &expr[..at])?;
        let rhs = decode_predicate_and(obj, field, &expr[at + 4..])?;
        return Ok(ConditionExpression::Or(Box::new(lhs), Box::new(rhs)));
    }
    decode_predicate_and(obj, field, expr)
}

/// `AND` — binds tighter than `OR`, looser than `NOT`.
fn decode_predicate_and(
    obj: &Map<String, Value>,
    field: &str,
    expr: &str,
) -> Result<ConditionExpression, WireError> {
    let expr = expr.trim();
    if let Some(at) = find_top_level(expr, " AND ") {
        let lhs = decode_predicate_and(obj, field, &expr[..at])?;
        let rhs = decode_predicate_not(obj, field, &expr[at + 5..])?;
        return Ok(ConditionExpression::And(Box::new(lhs), Box::new(rhs)));
    }
    decode_predicate_not(obj, field, expr)
}

/// `NOT` — binds tightest, and a parenthesised group is a term.
fn decode_predicate_not(
    obj: &Map<String, Value>,
    field: &str,
    expr: &str,
) -> Result<ConditionExpression, WireError> {
    let expr = expr.trim();
    if let Some(rest) = strip_keyword_prefix(expr, "NOT ") {
        return Ok(ConditionExpression::Not(Box::new(decode_predicate_not(
            obj, field, rest,
        )?)));
    }
    // A fully-parenthesised expression is unwrapped and re-parsed from the top,
    // but only when the opening paren matches the closing one — `(a) OR (b)`
    // must not be mistaken for a single group.
    if expr.starts_with('(') && expr.ends_with(')') && matching_close(expr) == Some(expr.len() - 1)
    {
        return decode_predicate_or(obj, field, &expr[1..expr.len() - 1]);
    }
    decode_predicate_expr(obj, field, expr)
}

/// Strip a leading keyword (case-insensitively) when it stands as a word.
fn strip_keyword_prefix<'a>(expr: &'a str, keyword: &str) -> Option<&'a str> {
    let lower = expr.to_ascii_lowercase();
    lower
        .starts_with(&keyword.to_ascii_lowercase())
        .then(|| &expr[keyword.len()..])
}

/// Index of the `)` matching the `(` at position 0, if any.
fn matching_close(expr: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, c) in expr.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Parse one predicate term. Split out of [`decode_predicate`] so the boolean
/// combinators can recurse into it.
fn decode_predicate_expr(
    obj: &Map<String, Value>,
    field: &str,
    expr: &str,
) -> Result<ConditionExpression, WireError> {
    let expr = expr.trim();
    // Function forms first: their argument lists can contain commas and
    // operators that the comparator split would otherwise cut through.
    if let Some(inner) = func_arg(expr, "attribute_not_exists") {
        return Ok(ConditionExpression::AttributeNotExists(resolve_attr_name(
            obj,
            inner.trim(),
        )?));
    }
    if let Some(inner) = func_arg(expr, "attribute_exists") {
        return Ok(ConditionExpression::AttributeExists(resolve_attr_name(
            obj,
            inner.trim(),
        )?));
    }
    if let Some(inner) = func_arg(expr, "attribute_type") {
        let (attr, code) = inner.split_once(',').ok_or_else(|| {
            WireError::validation("attribute_type takes two arguments: attribute_type(a, :t)")
        })?;
        let attr = resolve_attr_name(obj, attr.trim())?;
        let code = match resolve_placeholder(obj, code.trim())? {
            AttributeValue::S(code) => code,
            other => {
                return Err(WireError::validation(format!(
                    "attribute_type's second argument must be a string type code, got {other:?}"
                )));
            }
        };
        const CODES: [&str; 10] = ["S", "N", "B", "BOOL", "NULL", "M", "L", "SS", "NS", "BS"];
        if !CODES.contains(&code.as_str()) {
            return Err(WireError::validation(format!(
                "unknown attribute_type code `{code}` (expected one of {})",
                CODES.join(", ")
            )));
        }
        return Ok(ConditionExpression::AttributeType(attr, code));
    }
    if let Some(inner) = func_arg(expr, "begins_with") {
        let (attr, ph) = inner.split_once(',').ok_or_else(|| {
            WireError::validation("begins_with takes two arguments: begins_with(a, :p)")
        })?;
        return Ok(ConditionExpression::BeginsWith(
            resolve_attr_name(obj, attr.trim())?,
            resolve_placeholder(obj, ph.trim())?,
        ));
    }
    if let Some(inner) = func_arg(expr, "contains") {
        let (attr, ph) = inner.split_once(',').ok_or_else(|| {
            WireError::validation("contains takes two arguments: contains(a, :v)")
        })?;
        return Ok(ConditionExpression::Contains(
            resolve_attr_name(obj, attr.trim())?,
            resolve_placeholder(obj, ph.trim())?,
        ));
    }
    // `size(a) <op> :v` — the only form where a function appears on the *left*
    // of a comparison, so it is recognised before the generic comparator split.
    if let Some((lhs, op, rhs)) = split_comparator(expr)
        && let Some(inner) = func_arg(lhs.trim(), "size")
    {
        let attr = resolve_attr_name(obj, inner.trim())?;
        let value = resolve_placeholder(obj, rhs.trim())?;
        return Ok(ConditionExpression::Size(
            attr,
            comparator_of(op, expr)?,
            value,
        ));
    }
    // `a BETWEEN :lo AND :hi`
    if let Some((attr, rest)) = split_once_ci(expr, " BETWEEN ") {
        let (lo, hi) = split_once_ci(rest, " AND ")
            .ok_or_else(|| WireError::validation("BETWEEN takes `:lo AND :hi`"))?;
        return Ok(ConditionExpression::Between(
            resolve_attr_name(obj, attr.trim())?,
            resolve_placeholder(obj, lo.trim())?,
            resolve_placeholder(obj, hi.trim())?,
        ));
    }
    // `a IN (:x, :y, ..)`
    if let Some((attr, rest)) = split_once_ci(expr, " IN ") {
        let list = rest.trim();
        let inner = list
            .strip_prefix('(')
            .and_then(|r| r.strip_suffix(')'))
            .ok_or_else(|| WireError::validation("IN takes a parenthesised list: a IN (:x, :y)"))?;
        let mut values = Vec::new();
        for ph in inner.split(',') {
            let ph = ph.trim();
            if ph.is_empty() {
                return Err(WireError::validation("IN list has an empty element"));
            }
            values.push(resolve_placeholder(obj, ph)?);
        }
        if values.is_empty() {
            return Err(WireError::validation("IN list must not be empty"));
        }
        return Ok(ConditionExpression::In(
            resolve_attr_name(obj, attr.trim())?,
            values,
        ));
    }
    if let Some((lhs, op, rhs)) = split_comparator(expr) {
        let attr = resolve_attr_name(obj, lhs.trim())?;
        let value = resolve_placeholder(obj, rhs.trim())?;
        return Ok(ConditionExpression::Compare(
            attr,
            comparator_of(op, expr)?,
            value,
        ));
    }
    Err(WireError::validation(format!(
        "unsupported {field} `{expr}` (supported: comparisons =, <>, <, <=, >, >=; \
         BETWEEN; IN; attribute_exists; attribute_not_exists; attribute_type; \
         begins_with; contains; size)"
    )))
}

/// Map a comparison operator's text to its [`Comparator`].
fn comparator_of(op: &str, expr: &str) -> Result<Comparator, WireError> {
    Ok(match op {
        "=" => Comparator::Eq,
        "<>" => Comparator::Ne,
        "<" => Comparator::Lt,
        "<=" => Comparator::Le,
        ">" => Comparator::Gt,
        ">=" => Comparator::Ge,
        other => {
            return Err(WireError::validation(format!(
                "unsupported operator `{other}` in `{expr}`"
            )));
        }
    })
}

/// If `expr` is exactly `name(arg)`, return the trimmed `arg`.
fn func_arg<'a>(expr: &'a str, name: &str) -> Option<&'a str> {
    let rest = expr.strip_prefix(name)?.trim_start();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    Some(inner.trim())
}

/// Resolve a `:placeholder` against `ExpressionAttributeValues`.
fn resolve_placeholder(
    obj: &Map<String, Value>,
    placeholder: &str,
) -> Result<AttributeValue, WireError> {
    if !placeholder.starts_with(':') {
        return Err(WireError::validation(format!(
            "condition right-hand side `{placeholder}` must be a `:` value placeholder"
        )));
    }
    let values = obj
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object)
        .ok_or_else(|| WireError::validation("missing `ExpressionAttributeValues`"))?;
    let raw = values.get(placeholder).ok_or_else(|| {
        WireError::validation(format!("placeholder `{placeholder}` is not defined"))
    })?;
    decode_attribute_value(placeholder, raw)
}

/// Decode a `Query` body: a `KeyConditionExpression` of the form
/// `<pk> = :pv [AND <sort-condition>]`, with values supplied via
/// `ExpressionAttributeValues`. The supported sort conditions are
/// `<sk> = :v`, `<sk> BETWEEN :lo AND :hi`, and `begins_with(<sk>, :p)`.
/// `Limit`/`ExclusiveStartKey` decode exactly like `Scan`'s
/// ([`decode_limit`]/[`decode_exclusive_start_key`], shared between the two).
fn decode_query(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    let expr = obj
        .get("KeyConditionExpression")
        .and_then(Value::as_str)
        .ok_or_else(|| WireError::validation("missing string field `KeyConditionExpression`"))?;
    // Split off an optional sort clause on the first ` AND ` (DynamoDB requires
    // the partition equality first).
    let (pk_clause, sort_clause) = match split_once_ci(expr, " AND ") {
        Some((a, b)) => (a.trim(), Some(b.trim())),
        None => (expr.trim(), None),
    };
    // Partition: `<attr> = :placeholder`. The attribute **name** is carried
    // (not discarded) so the `animusd` edge can check it really is the
    // queried table's or index's partition key — decode has no catalog. A
    // dropped name meant `KeyConditionExpression: "notthekey = :v"` was
    // silently served as a partition-key query against whatever value it
    // named, which DynamoDB rejects.
    let (pk_attr, pk_op, pk_placeholder) = split_comparator(pk_clause)
        .ok_or_else(|| WireError::validation("partition key condition must be `pk = :v`"))?;
    if pk_op != "=" {
        return Err(WireError::validation(format!(
            "partition key condition must be an equality, got `{pk_op}` in `{pk_clause}`"
        )));
    }
    let partition_attr = resolve_attr_name(obj, pk_attr.trim())?;
    let partition_value = resolve_placeholder(obj, pk_placeholder.trim())?;

    let index = obj
        .get("IndexName")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let (sort_attr, sort_condition) = match sort_clause {
        None => (None, None),
        Some(clause) => {
            let (attr, cond) = decode_sort_condition(obj, clause)?;
            (Some(attr), Some(cond))
        }
    };
    let limit = decode_limit(obj)?;
    let exclusive_start_key = decode_exclusive_start_key(obj)?;
    // Same predicate decoder `Scan` uses — a `Query`'s filter is the identical
    // post-read contract, just applied within one partition's key range.
    let filter = decode_predicate(obj, "FilterExpression")?;
    let projection = decode_projection(obj)?;
    let select = decode_select(obj, index.as_deref(), projection.as_ref())?;
    let consistent_read = decode_consistent_read(obj);
    // A sort condition on an index is meaningful only for a local secondary
    // index (which has an alternate sort key). The caller (registry) rejects a
    // sort condition against a hash-only GSI; here we accept the parse so the
    // index-kind decision can live in one place. Likewise `consistent_read`
    // is decoded unconditionally — whether it's legal depends on `index`'s
    // *kind* (GSI vs LSI vs base), which is only known once the replicated
    // catalog is consulted at the `animusd` edge, not here.
    // Absent means ascending, matching DynamoDB's default.
    let scan_index_forward = match obj.get("ScanIndexForward") {
        None | Some(Value::Null) => true,
        Some(v) => v
            .as_bool()
            .ok_or_else(|| WireError::validation("`ScanIndexForward` must be a boolean"))?,
    };
    Ok(Operation::Query {
        table,
        index,
        partition_attr,
        partition_value,
        sort_attr,
        sort_condition,
        limit,
        exclusive_start_key,
        scan_index_forward,
        filter,
        projection,
        select,
        consistent_read,
    })
}

/// Decode the optional `ConsistentRead` boolean (default `false`, matching
/// DynamoDB). Shared by `GetItem`/`Query`/`Scan`. This crate only decodes it:
/// the `animusd` edge both *rejects* it (a GSI `Query`/`Scan`, ADR 0041 §5 —
/// the only rejection) and, since ADR 0055, *acts* on it everywhere else,
/// choosing between the linearizable ReadIndex read and a replica-local one.
fn decode_consistent_read(obj: &Map<String, Value>) -> bool {
    obj.get("ConsistentRead")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// Decode the optional `Limit` (a non-negative integer, `None` when absent or
/// `null`). Shared by `Scan` and `Query` — both page the same way.
fn decode_limit(obj: &Map<String, Value>) -> Result<Option<usize>, WireError> {
    match obj.get("Limit") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(
            v.as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| WireError::validation("`Limit` must be a non-negative integer"))?,
        )),
    }
}

/// Decode the optional `ExclusiveStartKey` (an AttributeValue-map pagination
/// cursor, `None` when absent or `null`). Shared by `Scan` and `Query`.
fn decode_exclusive_start_key(obj: &Map<String, Value>) -> Result<Option<Item>, WireError> {
    match obj.get("ExclusiveStartKey") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(
            v.as_object()
                .ok_or_else(|| WireError::validation("`ExclusiveStartKey` must be an object"))
                .and_then(decode_item)?,
        )),
    }
}

/// Decode a `Scan` body: an optional `Limit`, an optional `ExclusiveStartKey`
/// (the AttributeValue-map cursor from a previous page's `LastEvaluatedKey`),
/// an optional `FilterExpression` (the `ConditionExpression` predicate set),
/// and an optional `IndexName` (ADR 0041 §5 — scans a secondary index's own
/// rows instead of the base table; no `KeyConditionExpression` here, unlike
/// `Query` — DynamoDB's `Scan` never takes one, index or not).
/// Decode `Segment`/`TotalSegments`, which DynamoDB requires together.
///
/// Giving one without the other is rejected rather than ignored: a client that
/// sends `Segment` alone almost certainly believes it is scanning a slice, and
/// silently handing it the whole table would have every worker return every
/// item — the parallel-scan equivalent of a filter that does not filter.
fn decode_scan_segment(obj: &Map<String, Value>) -> Result<Option<ScanSegment>, WireError> {
    let seg = obj.get("Segment");
    let total = obj.get("TotalSegments");
    let (seg, total) = match (seg, total) {
        (None, None) => return Ok(None),
        (Some(_), None) => {
            return Err(WireError::validation("`Segment` requires `TotalSegments`"));
        }
        (None, Some(_)) => {
            return Err(WireError::validation("`TotalSegments` requires `Segment`"));
        }
        (Some(s), Some(t)) => (s, t),
    };
    let as_u32 = |v: &Value, name: &str| -> Result<u32, WireError> {
        v.as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| {
                WireError::validation(format!("`{name}` must be a non-negative integer"))
            })
    };
    let segment = as_u32(seg, "Segment")?;
    let total = as_u32(total, "TotalSegments")?;
    if total == 0 {
        return Err(WireError::validation("`TotalSegments` must be at least 1"));
    }
    if segment >= total {
        return Err(WireError::validation(format!(
            "`Segment` {segment} is out of range for `TotalSegments` {total}"
        )));
    }
    Ok(Some(ScanSegment { segment, total }))
}

fn decode_scan(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
    let index = obj
        .get("IndexName")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let limit = decode_limit(obj)?;
    let exclusive_start_key = decode_exclusive_start_key(obj)?;
    let filter = decode_predicate(obj, "FilterExpression")?;
    let projection = decode_projection(obj)?;
    let select = decode_select(obj, index.as_deref(), projection.as_ref())?;
    let segment = decode_scan_segment(obj)?;
    let consistent_read = decode_consistent_read(obj);
    Ok(Operation::Scan {
        table,
        index,
        limit,
        exclusive_start_key,
        filter,
        projection,
        select,
        segment,
        consistent_read,
    })
}

/// Decode a `ListTables` body: an optional `ExclusiveStartTableName`
/// (pagination cursor) and an optional `Limit`, decoded exactly as `Scan`'s
/// (any non-negative integer) — the default-100/cap-100 clamp is
/// [`paginate_table_names`]'s job, not decode's.
fn decode_list_tables(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let exclusive_start_table_name = obj
        .get("ExclusiveStartTableName")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let limit = match obj.get("Limit") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| WireError::validation("`Limit` must be a non-negative integer"))?,
        ),
    };
    Ok(Operation::ListTables {
        exclusive_start_table_name,
        limit,
    })
}

fn decode_sort_condition(
    obj: &Map<String, Value>,
    clause: &str,
) -> Result<(String, SortKeyCondition), WireError> {
    let clause = clause.trim();
    if let Some(inner) = func_arg(clause, "begins_with") {
        // begins_with(<sk>, :p)
        let (attr, ph) = inner.split_once(',').ok_or_else(|| {
            WireError::validation("begins_with takes two arguments: begins_with(sk, :p)")
        })?;
        let attr = resolve_attr_name(obj, attr.trim())?;
        let value = resolve_placeholder(obj, ph.trim())?;
        return Ok((attr, SortKeyCondition::BeginsWith(value)));
    }
    if let Some((attr, rest)) = split_once_ci(clause, " BETWEEN ") {
        let (lo, hi) = split_once_ci(rest, " AND ")
            .ok_or_else(|| WireError::validation("BETWEEN takes `:lo AND :hi`"))?;
        let attr = resolve_attr_name(obj, attr.trim())?;
        let lo = resolve_placeholder(obj, lo.trim())?;
        let hi = resolve_placeholder(obj, hi.trim())?;
        return Ok((attr, SortKeyCondition::Between(lo, hi)));
    }
    if let Some((lhs, op, rhs)) = split_comparator(clause) {
        // `<>` is a legal `Comparator` (reused wholesale below) but is not in
        // AWS's own `KeyConditionExpression` grammar — a key condition can
        // only narrow a contiguous range, and "not equal to" isn't one, so
        // real DynamoDB rejects it here too. Every other comparator maps
        // straight onto a `Compare` condition, sharing `comparator_of` with
        // `decode_predicate` rather than re-deriving the same mapping.
        if op == "<>" {
            return Err(WireError::validation(format!(
                "`<>` is not a valid KeyConditionExpression operator in `{clause}` \
                 (AWS's KeyConditionExpression grammar has no not-equal comparator; \
                 supported: =, <, <=, >, >=, BETWEEN, begins_with)"
            )));
        }
        let attr = resolve_attr_name(obj, lhs.trim())?;
        let value = resolve_placeholder(obj, rhs.trim())?;
        return Ok((
            attr,
            SortKeyCondition::Compare(comparator_of(op, clause)?, value),
        ));
    }
    Err(WireError::validation(format!(
        "unsupported sort-key condition `{clause}` \
         (supported: =, <, <=, >, >=, BETWEEN, begins_with)"
    )))
}

/// Split `expr` on its **comparison operator**, longest match first, returning
/// `(lhs, op, rhs)`.
///
/// The longest-first order is the whole point. A naive `split_once('=')` — what
/// three parsers here used to do — cuts `price >= :p` into `("price >", " :p")`
/// and yields an equality against an attribute literally named `price >`, which
/// no item has. That is silent: the caller gets zero matches (or, on a
/// conditional write, a condition that can never hold) instead of an error. The
/// same cut turns a sort-key range `sk <= :v` into an equality, quietly
/// narrowing a range query to exact matches.
///
/// `<>` must precede `<`, and `<=`/`>=` must precede `=`, or the same
/// truncation reappears one operator along.
fn split_comparator(expr: &str) -> Option<(&str, &str, &str)> {
    // Longest first: a prefix of a longer operator must never win.
    const OPS: [&str; 6] = ["<>", "<=", ">=", "=", "<", ">"];
    let mut best: Option<(usize, &str)> = None;
    for op in OPS {
        if let Some(at) = expr.find(op) {
            // Earliest position wins; on a tie the longer operator wins, which
            // the OPS ordering already guarantees since it is scanned first.
            let better = match best {
                None => true,
                Some((at_best, op_best)) => {
                    at < at_best || (at == at_best && op.len() > op_best.len())
                }
            };
            if better {
                best = Some((at, op));
            }
        }
    }
    let (at, op) = best?;
    Some((&expr[..at], op, &expr[at + op.len()..]))
}

/// Case-insensitive `split_once` on a `needle` (used for ` AND `/` BETWEEN `,
/// which clients may send in any case). Returns byte-sliced halves of `s`.
fn split_once_ci<'a>(s: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    let lower = s.to_ascii_lowercase();
    let pos = lower.find(&needle.to_ascii_lowercase())?;
    Some((&s[..pos], &s[pos + needle.len()..]))
}

fn table_name(obj: &Map<String, Value>) -> Result<String, WireError> {
    obj.get("TableName")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| WireError::validation("missing string field `TableName`"))
}

fn decode_item_field(obj: &Map<String, Value>, field: &str) -> Result<Item, WireError> {
    let map = obj
        .get(field)
        .ok_or_else(|| WireError::validation(format!("missing field `{field}`")))?
        .as_object()
        .ok_or_else(|| WireError::validation(format!("`{field}` must be an object")))?;
    decode_item(map)
}

/// Decode a DynamoDB attribute-map JSON object into an [`Item`].
///
/// # Errors
/// Returns a [`WireError`] if any attribute value is malformed or uses an
/// unsupported type.
pub fn decode_item(map: &Map<String, Value>) -> Result<Item, WireError> {
    let mut item = Item::new();
    for (name, value) in map {
        item.insert(name.clone(), decode_attribute_value(name, value)?);
    }
    Ok(item)
}

fn decode_attribute_value(name: &str, value: &Value) -> Result<AttributeValue, WireError> {
    let obj = value.as_object().ok_or_else(|| {
        WireError::validation(format!("attribute `{name}` must be a typed object"))
    })?;
    let (ty, inner) = match obj.iter().next() {
        Some(entry) if obj.len() == 1 => entry,
        _ => {
            return Err(WireError::validation(format!(
                "attribute `{name}` must be a single-key typed object like {{\"S\":..}}"
            )));
        }
    };
    match ty.as_str() {
        "S" => inner
            .as_str()
            .map(|s| AttributeValue::S(s.to_owned()))
            .ok_or_else(|| WireError::validation(format!("`{name}`.S must be a string"))),
        "N" => inner
            .as_str()
            .map(|s| AttributeValue::N(s.to_owned()))
            .ok_or_else(|| WireError::validation(format!("`{name}`.N must be a string"))),
        "B" => inner
            .as_str()
            .ok_or_else(|| WireError::validation(format!("`{name}`.B must be a base64 string")))
            .and_then(|s| {
                base64_decode(s)
                    .map(AttributeValue::B)
                    .ok_or_else(|| WireError::validation(format!("`{name}`.B is not valid base64")))
            }),
        "BOOL" => inner
            .as_bool()
            .map(AttributeValue::Bool)
            .ok_or_else(|| WireError::validation(format!("`{name}`.BOOL must be a bool"))),
        "NULL" => Ok(AttributeValue::Null),
        "M" => {
            let map = inner.as_object().ok_or_else(|| {
                WireError::validation(format!("`{name}`.M must be an attribute-map object"))
            })?;
            let mut nested = BTreeMap::new();
            for (k, v) in map {
                nested.insert(
                    k.clone(),
                    decode_attribute_value(&format!("{name}.{k}"), v)?,
                );
            }
            Ok(AttributeValue::M(nested))
        }
        "L" => {
            let list = inner.as_array().ok_or_else(|| {
                WireError::validation(format!("`{name}`.L must be a list of typed values"))
            })?;
            let mut out = Vec::with_capacity(list.len());
            for (i, v) in list.iter().enumerate() {
                out.push(decode_attribute_value(&format!("{name}[{i}]"), v)?);
            }
            Ok(AttributeValue::L(out))
        }
        "SS" => decode_string_set(name, "SS", inner).map(AttributeValue::SS),
        "NS" => decode_string_set(name, "NS", inner).map(AttributeValue::NS),
        "BS" => {
            let arr = inner.as_array().ok_or_else(|| {
                WireError::validation(format!("`{name}`.BS must be an array of base64 strings"))
            })?;
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                let s = v.as_str().ok_or_else(|| {
                    WireError::validation(format!("`{name}`.BS elements must be base64 strings"))
                })?;
                let bytes = base64_decode(s).ok_or_else(|| {
                    WireError::validation(format!("`{name}`.BS element is not valid base64"))
                })?;
                out.push(bytes);
            }
            Ok(AttributeValue::BS(dedup_sorted(out)))
        }
        other => Err(WireError::validation(format!(
            "attribute `{name}` uses unsupported type `{other}` \
             (only S, N, B, BOOL, NULL, M, L, SS, NS, BS are supported)"
        ))),
    }
}

/// Decode an `SS`/`NS` array of strings into a sorted, deduplicated set.
fn decode_string_set(name: &str, ty: &str, inner: &Value) -> Result<Vec<String>, WireError> {
    let arr = inner.as_array().ok_or_else(|| {
        WireError::validation(format!("`{name}`.{ty} must be an array of strings"))
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        let s = v.as_str().ok_or_else(|| {
            WireError::validation(format!("`{name}`.{ty} elements must be strings"))
        })?;
        out.push(s.to_owned());
    }
    Ok(dedup_sorted(out))
}

/// Sort and deduplicate a set's elements, so the in-memory representation is
/// canonical (set membership is order-independent; a canonical form makes
/// equality and storage deterministic).
fn dedup_sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
    items.sort();
    items.dedup();
    items
}

/// Encode an [`Item`] as a DynamoDB attribute-map JSON object.
#[must_use]
pub fn encode_item(item: &Item) -> Value {
    let mut map = Map::new();
    for (name, value) in item {
        map.insert(name.clone(), encode_attribute_value(value));
    }
    Value::Object(map)
}

fn encode_attribute_value(value: &AttributeValue) -> Value {
    let mut obj = Map::new();
    match value {
        AttributeValue::S(s) => {
            obj.insert("S".into(), Value::String(s.clone()));
        }
        AttributeValue::N(n) => {
            obj.insert("N".into(), Value::String(n.clone()));
        }
        AttributeValue::B(b) => {
            obj.insert("B".into(), Value::String(base64_encode(b)));
        }
        AttributeValue::Bool(b) => {
            obj.insert("BOOL".into(), Value::Bool(*b));
        }
        AttributeValue::Null => {
            obj.insert("NULL".into(), Value::Bool(true));
        }
        AttributeValue::M(map) => {
            let mut nested = Map::new();
            for (k, v) in map {
                nested.insert(k.clone(), encode_attribute_value(v));
            }
            obj.insert("M".into(), Value::Object(nested));
        }
        AttributeValue::L(list) => {
            let encoded: Vec<Value> = list.iter().map(encode_attribute_value).collect();
            obj.insert("L".into(), Value::Array(encoded));
        }
        AttributeValue::SS(set) => {
            let arr = set.iter().map(|s| Value::String(s.clone())).collect();
            obj.insert("SS".into(), Value::Array(arr));
        }
        AttributeValue::NS(set) => {
            let arr = set.iter().map(|s| Value::String(s.clone())).collect();
            obj.insert("NS".into(), Value::Array(arr));
        }
        AttributeValue::BS(set) => {
            let arr = set
                .iter()
                .map(|b| Value::String(base64_encode(b)))
                .collect();
            obj.insert("BS".into(), Value::Array(arr));
        }
    }
    Value::Object(obj)
}

/// Serialize a response body, attaching `ConsumedCapacity` when the request
/// asked for one.
///
/// Every response that can carry capacity goes through here, including the ones
/// whose body is otherwise `{}` — a `PutItem` with `ReturnValues: NONE` still
/// owes the caller its capacity report, so "empty body" and "no capacity" are
/// deliberately not the same condition.
fn finish(mut obj: Map<String, Value>, capacity: Option<&ConsumedCapacity>) -> String {
    if let Some(capacity) = capacity {
        obj.insert("ConsumedCapacity".into(), capacity.encode());
    }
    serde_json::to_string(&Value::Object(obj)).expect("response serializes")
}

/// [`finish`], plus an `ItemCollectionMetrics` when the write had one to
/// report. Split from `finish` because only the three *write* operations can
/// carry metrics — a `GetItem` never does, and giving its builder the
/// parameter would invite exactly that mistake.
fn finish_write(
    mut obj: Map<String, Value>,
    capacity: Option<&ConsumedCapacity>,
    metrics: Option<&ItemCollectionMetrics>,
) -> String {
    if let Some(encoded) = metrics.and_then(|m| m.encode(encode_item)) {
        obj.insert("ItemCollectionMetrics".into(), encoded);
    }
    finish(obj, capacity)
}

/// The JSON body for a successful `GetItem`: `{"Item": {..}}`, or `{}` when the
/// item is absent (matching DynamoDB).
#[must_use]
pub fn get_item_response(item: Option<&Item>, capacity: Option<&ConsumedCapacity>) -> String {
    let mut obj = Map::new();
    if let Some(item) = item {
        obj.insert("Item".into(), encode_item(item));
    }
    finish(obj, capacity)
}

/// The JSON body for a successful `PutItem` / `DeleteItem` with
/// `ReturnValues: NONE`: `{}`.
#[must_use]
pub fn empty_response() -> String {
    "{}".to_string()
}

/// The JSON body for a successful `TransactGetItems`: `{"Responses": [{"Item":
/// {..}} | {}, ..]}`, one entry per requested key **in request order** — an
/// absent item is `{}` at that slot (matching `GetItem`'s own encoding), never
/// omitted (the response must stay index-aligned with the request).
#[must_use]
pub fn transact_get_response(items: &[Option<Item>]) -> String {
    let responses: Vec<Value> = items
        .iter()
        .map(|item| {
            let mut obj = Map::new();
            if let Some(item) = item {
                obj.insert("Item".into(), encode_item(item));
            }
            Value::Object(obj)
        })
        .collect();
    let mut obj = Map::new();
    obj.insert("Responses".into(), Value::Array(responses));
    serde_json::to_string(&Value::Object(obj)).expect("transact-get response serializes")
}

/// The JSON body for a successful `BatchGetItem`: `{"Responses": {"<table>":
/// [item, ..]}, "UnprocessedKeys": {}}`.
///
/// Keys that matched no item are simply absent from the table's list — a
/// `BatchGetItem` reports misses by omission, unlike `TransactGetItems`, whose
/// response is positional and carries an empty object per missing key.
///
/// `UnprocessedKeys` is always empty: this adapter reads every requested key
/// before responding rather than shedding load, so there is never a remainder
/// for the client to retry.
#[must_use]
pub fn batch_get_response(tables: &[(String, Vec<Item>)]) -> String {
    let mut responses = Map::new();
    for (table, items) in tables {
        let encoded: Vec<Value> = items.iter().map(encode_item).collect();
        responses.insert(table.clone(), Value::Array(encoded));
    }
    let mut obj = Map::new();
    obj.insert("Responses".into(), Value::Object(responses));
    obj.insert("UnprocessedKeys".into(), Value::Object(Map::new()));
    serde_json::to_string(&Value::Object(obj)).expect("batch get response serializes")
}

/// The JSON body for a successful write echoing `ReturnValues`. `old` is the
/// item as it was before the write (`None` when the key was absent); for
/// `ALL_OLD` a present prior item is returned under `Attributes`, an absent one
/// yields `{}` (matching DynamoDB). For `NONE` this is always `{}`.
#[must_use]
pub fn write_response(
    return_values: ReturnValues,
    old: Option<&Item>,
    capacity: Option<&ConsumedCapacity>,
    metrics: Option<&ItemCollectionMetrics>,
) -> String {
    let mut obj = Map::new();
    if let (ReturnValues::AllOld, Some(item)) = (return_values, old) {
        obj.insert("Attributes".into(), encode_item(item));
    }
    finish_write(obj, capacity, metrics)
}

/// The JSON body for a successful `UpdateItem` echoing `ReturnValues`: `ALL_OLD`
/// returns the item before the update under `Attributes`, `ALL_NEW` the item
/// after, `NONE` returns `{}`. An absent `old`/`new` (e.g. `ALL_OLD` on a key the
/// update created) yields `{}`.
#[must_use]
pub fn update_response(
    return_values: UpdateReturnValues,
    old: Option<&Item>,
    new: Option<&Item>,
    capacity: Option<&ConsumedCapacity>,
    metrics: Option<&ItemCollectionMetrics>,
) -> String {
    let attrs = match return_values {
        UpdateReturnValues::None => None,
        UpdateReturnValues::AllOld => old.cloned(),
        UpdateReturnValues::AllNew => new.cloned(),
        // The `UPDATED_*` pair reports only what actually changed, taken from
        // whichever side holds the value: the old image for `UPDATED_OLD`, the
        // new one for `UPDATED_NEW`. An attribute present on only one side is
        // therefore reported by exactly one of them — a created attribute has
        // no previous value, a removed one has no new value — which is why
        // this is a diff rather than a projection of one image.
        UpdateReturnValues::UpdatedOld => Some(changed_attributes(old, new, old)),
        UpdateReturnValues::UpdatedNew => Some(changed_attributes(old, new, new)),
    };
    let mut obj = Map::new();
    // DynamoDB omits `Attributes` entirely when nothing changed, rather than
    // returning an empty map.
    if let Some(item) = attrs.filter(|i| !i.is_empty()) {
        obj.insert("Attributes".into(), encode_item(&item));
    }
    finish_write(obj, capacity, metrics)
}

/// The attributes whose value differs between `old` and `new`, taken from
/// `from` (one of the two). An attribute missing from `from` is skipped, so
/// `UPDATED_OLD` omits what the update created and `UPDATED_NEW` omits what it
/// removed.
///
/// Key attributes fall out naturally: an update never changes them, so they
/// never differ and never appear.
fn changed_attributes(old: Option<&Item>, new: Option<&Item>, from: Option<&Item>) -> Item {
    let empty = Item::new();
    let old = old.unwrap_or(&empty);
    let new = new.unwrap_or(&empty);
    let Some(from) = from else {
        return Item::new();
    };
    from.iter()
        .filter(|(name, _)| old.get(*name) != new.get(*name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// The JSON body for a successful `BatchWriteItem`: `{"UnprocessedItems": {}}`
/// (we process every request, so nothing is ever left unprocessed).
#[must_use]
pub fn batch_write_response() -> String {
    let mut obj = Map::new();
    obj.insert("UnprocessedItems".into(), Value::Object(Map::new()));
    serde_json::to_string(&Value::Object(obj)).expect("batch response serializes")
}

/// Apply a sequence of [`UpdateAction`]s to a starting item (`None` ⇒ a fresh
/// item is built from the key by the caller before this), returning the new
/// item. Pure.
///
/// `REMOVE` drops a top-level attribute and is infallible. `SET` (issue #375
/// PR1) evaluates its right-hand-side [`UpdateExpr`] against the item — a
/// bare `:value` never fails, but `if_not_exists`/`list_append` can (a
/// missing operand, a non-list `list_append`). `ADD` and `DELETE` are typed
/// operations too, and DynamoDB rejects a mismatch (`ADD`ing a number to a
/// string set) rather than ignoring it. Hence the `Result` — silently
/// skipping a mismatched action would leave the caller believing an update
/// applied when it did not, which is the one outcome worse than an error.
///
/// This runs on the **leader** that owns the row (ADR 0046 U3), against the old
/// image the leader itself read, so `ADD`'s read-modify-write is evaluated
/// exactly once per applied write rather than against a possibly-stale image
/// from the edge.
///
/// After the full action list folds — not mid-fold — the result is checked
/// against [`MAX_ITEM_SIZE_BYTES`] ([`check_item_size`]): an item that is
/// temporarily over the cap partway through (e.g. a `SET` that pushes it over,
/// followed later in the same expression by a `REMOVE` that nets it back
/// under) still succeeds, matching AWS's own post-update-result contract.
/// This is the single choke point that covers both `UpdateItem` and
/// `TransactWriteItems`'s `Update` action, since both call this function.
pub fn apply_update(mut item: Item, actions: &[UpdateAction]) -> Result<Item, WireError> {
    for action in actions {
        match action {
            UpdateAction::Set(attr, value_expr) => {
                let value = eval_update_expr(&item, value_expr)?;
                item.insert(attr.clone(), value);
            }
            UpdateAction::Remove(attr) => {
                item.remove(attr);
            }
            UpdateAction::Add(attr, operand) => {
                let updated = match (item.get(attr), operand) {
                    // Absent: seed with the operand. This is what makes
                    // `ADD #c :one` the idiomatic counter increment on a row
                    // that does not exist yet.
                    (None, v) => v.clone(),
                    (Some(AttributeValue::N(cur)), AttributeValue::N(delta)) => AttributeValue::N(
                        crate::condition::add_numeric(cur, delta).ok_or_else(|| {
                            WireError::validation(format!(
                                "ADD on `{attr}`: `{cur}` and `{delta}` are not both numbers"
                            ))
                        })?,
                    ),
                    (Some(AttributeValue::SS(cur)), AttributeValue::SS(add)) => {
                        AttributeValue::SS(union_sorted(cur, add))
                    }
                    (Some(AttributeValue::NS(cur)), AttributeValue::NS(add)) => {
                        AttributeValue::NS(union_sorted(cur, add))
                    }
                    (Some(AttributeValue::BS(cur)), AttributeValue::BS(add)) => {
                        AttributeValue::BS(union_sorted(cur, add))
                    }
                    (Some(existing), operand) => {
                        return Err(WireError::validation(format!(
                            "ADD on `{attr}` needs a number or a matching set type, \
                             got {} += {}",
                            type_name(existing),
                            type_name(operand)
                        )));
                    }
                };
                item.insert(attr.clone(), updated);
            }
            UpdateAction::Delete(attr, operand) => {
                let Some(existing) = item.get(attr) else {
                    // Deleting from an absent attribute is a no-op, as in
                    // DynamoDB — not an error.
                    continue;
                };
                let remaining = match (existing, operand) {
                    (AttributeValue::SS(cur), AttributeValue::SS(rm)) => {
                        AttributeValue::SS(difference_sorted(cur, rm))
                    }
                    (AttributeValue::NS(cur), AttributeValue::NS(rm)) => {
                        AttributeValue::NS(difference_sorted(cur, rm))
                    }
                    (AttributeValue::BS(cur), AttributeValue::BS(rm)) => {
                        AttributeValue::BS(difference_sorted(cur, rm))
                    }
                    (existing, operand) => {
                        return Err(WireError::validation(format!(
                            "DELETE on `{attr}` needs matching set types, got {} -= {}",
                            type_name(existing),
                            type_name(operand)
                        )));
                    }
                };
                // DynamoDB does not store empty sets: emptying one removes the
                // attribute rather than leaving `SS: []` behind.
                if set_is_empty(&remaining) {
                    item.remove(attr);
                } else {
                    item.insert(attr.clone(), remaining);
                }
            }
        }
    }
    check_item_size(&item)?;
    Ok(item)
}

/// Sorted, de-duplicated union — the representation this crate keeps sets in.
fn union_sorted<T: Ord + Clone>(a: &[T], b: &[T]) -> Vec<T> {
    let mut out: Vec<T> = a.to_vec();
    out.extend(b.iter().cloned());
    out.sort();
    out.dedup();
    out
}

/// Sorted difference, `a` minus `b`.
fn difference_sorted<T: Ord + Clone>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().filter(|x| !b.contains(x)).cloned().collect()
}

/// Whether a set-typed value has no members.
fn set_is_empty(v: &AttributeValue) -> bool {
    match v {
        AttributeValue::SS(s) => s.is_empty(),
        AttributeValue::NS(s) => s.is_empty(),
        AttributeValue::BS(s) => s.is_empty(),
        _ => false,
    }
}

/// A human-readable type name for an error message.
fn type_name(v: &AttributeValue) -> &'static str {
    match v {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::M(_) => "M",
        AttributeValue::L(_) => "L",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
    }
}

/// The JSON body for a successful `Scan` **or `Query`** (both share this exact
/// shape now that `Query` paginates too — `animusd::dynamo`'s base/GSI/LSI
/// query paths build their response with this same encoder): `{"Items": [..],
/// "Count": n, "ScannedCount": s}`, plus a `LastEvaluatedKey` (the
/// AttributeValue-map pagination cursor) when the page was truncated by a
/// `Limit`. `scanned` counts the items read before filtering; `Count` the
/// items returned after.
#[must_use]
pub fn scan_response(items: &[Item], scanned: usize, last_evaluated_key: Option<&Item>) -> String {
    let encoded: Vec<Value> = items.iter().map(encode_item).collect();
    let mut obj = Map::new();
    obj.insert("Items".into(), Value::Array(encoded));
    obj.insert("Count".into(), Value::from(items.len()));
    obj.insert("ScannedCount".into(), Value::from(scanned));
    if let Some(key) = last_evaluated_key {
        obj.insert("LastEvaluatedKey".into(), encode_item(key));
    }
    serde_json::to_string(&Value::Object(obj)).expect("scan response serializes")
}

/// [`scan_response`], honouring [`Select`]: under [`Select::Count`] the
/// `Items` array is omitted entirely and only `Count`/`ScannedCount` (plus
/// any `LastEvaluatedKey`) are returned. Every other `Select` returns the
/// full page, since the attribute selection has already been applied to the
/// items by the time they get here.
///
/// The counts are identical either way — a `COUNT` request reads and filters
/// exactly what the same request without it would have.
#[must_use]
pub fn select_response(
    select: Select,
    items: &[Item],
    scanned: usize,
    last_evaluated_key: Option<&Item>,
) -> String {
    if select != Select::Count {
        return scan_response(items, scanned, last_evaluated_key);
    }
    let mut obj = Map::new();
    obj.insert("Count".into(), Value::from(items.len()));
    obj.insert("ScannedCount".into(), Value::from(scanned));
    if let Some(key) = last_evaluated_key {
        obj.insert("LastEvaluatedKey".into(), encode_item(key));
    }
    serde_json::to_string(&Value::Object(obj)).expect("count response serializes")
}

/// The synthetic ARN this adapter surfaces for a stream (ADR 0042 §4):
/// `arn:aws:dynamodb:animus:0:table/<table>/stream/<label>` — a
/// DynamoDB-shaped string with fixed placeholder region/account
/// (`animus`/`0`), matching this adapter's existing ARN conventions
/// elsewhere.
#[must_use]
pub fn stream_arn(table: &str, label: &str) -> String {
    format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}")
}

/// The synthetic ARN this adapter surfaces for a backup (ADR 0059, Train 1
/// PR④): `arn:aws:dynamodb:animus:0:table/<table>/backup/<backup_id>` —
/// [`stream_arn`]'s identical placeholder region/account convention, with
/// AWS's own `.../backup/<id>` suffix in place of `.../stream/<label>`.
/// `backup_id` is minted by the caller (`animusd::dynamo::create_backup`,
/// a fresh random suffix) — **this whole string, not just `backup_id`, is
/// the catalog's own opaque [`animus_control::BackupId`] key** (ADR 0059
/// §3: "an ARN-shaped string at the wire"), so a backup is looked up
/// directly by this value with no ARN parsing anywhere in this adapter.
#[must_use]
pub fn backup_arn(table: &str, backup_id: &str) -> String {
    format!("arn:aws:dynamodb:animus:0:table/{table}/backup/{backup_id}")
}

/// The synthetic ARN this adapter surfaces for a **table itself** (roadmap
/// W-06 — `TagResource`/`UntagResource`/`ListTagsOfResource`'s
/// `ResourceArn`, and this adapter's own `TableArn`): `arn:aws:dynamodb:
/// animus:0:table/<table>` — [`stream_arn`]/[`backup_arn`]'s identical
/// placeholder region/account convention, with no further suffix (unlike
/// those two, which append `/stream/<label>`/`/backup/<id>`).
#[must_use]
pub fn table_arn(table: &str) -> String {
    format!("arn:aws:dynamodb:animus:0:table/{table}")
}

/// The inverse of [`table_arn`]: recovers the bare table name from a
/// **table** ARN. Rejects (returns `None` for) anything that isn't exactly
/// `arn:aws:dynamodb:animus:0:table/<name>` with no further `/`-separated
/// segment — in particular a well-formed stream ARN
/// (`.../table/<name>/stream/<label>`) or backup ARN
/// (`.../table/<name>/backup/<id>`), which share this prefix but name a
/// different resource, not a table.
#[must_use]
pub fn parse_table_arn(arn: &str) -> Option<&str> {
    let rest = arn.strip_prefix("arn:aws:dynamodb:animus:0:table/")?;
    if rest.is_empty() || rest.contains('/') {
        return None;
    }
    Some(rest)
}

/// Build the shared `TableDescription`/`Table` object both
/// [`create_table_response`] and [`describe_table_response`] wrap: name, key
/// schema, `ACTIVE` status, any secondary indexes — each carrying its own
/// real `IndexStatus` (`CREATING`/`ACTIVE`/`DELETING`, ADR 0045 §6 Fork D)
/// plus, while `Creating`, `Backfilling: true` — and, when `stream` is
/// `Some`, `StreamSpecification`/`LatestStreamArn`/`LatestStreamLabel` (ADR
/// 0042 §2/§4).
///
/// **`Backfilling` is placed *per-index*, inside each `GlobalSecondaryIndexes[]`
/// entry** — matching real DynamoDB's `DescribeTable` shape exactly. (ADR
/// 0045 §6 originally sketched this as a table-level flag; that wording was
/// looser than AWS reality and is corrected here, not carried forward.)
///
/// `index_statuses` is the Fork-D side channel (a `(name, IndexStatus)`
/// list) rather than a field on [`SecondaryIndex`] itself, which stays a pure
/// `CreateTable`-*input* shape — mirroring [`StreamDescription`]'s own
/// separate-bridge precedent. An index absent from it (every index
/// [`create_table_response`] ever renders, and every LSI, which is `Active`
/// by construction — LSIs are create-time-only) renders as `Active`.
fn table_description_object(
    table: &str,
    schema: &TableSchema,
    indexes: &[SecondaryIndex],
    index_statuses: &[(String, IndexStatus)],
    stream: Option<&StreamDescription>,
    status: &str,
) -> Map<String, Value> {
    let mut key_schema = vec![key_schema_entry(&schema.partition_key, "HASH")];
    if let Some(sk) = &schema.sort_key {
        key_schema.push(key_schema_entry(sk, "RANGE"));
    }
    let mut desc = Map::new();
    desc.insert("TableName".into(), Value::String(table.to_owned()));
    desc.insert("TableArn".into(), Value::String(table_arn(table)));
    desc.insert("KeySchema".into(), Value::Array(key_schema));
    desc.insert("TableStatus".into(), Value::String(status.to_owned()));

    let mut gsis = Vec::new();
    let mut lsis = Vec::new();
    for index in indexes {
        match index {
            SecondaryIndex::Global(g) => {
                let mut ks = vec![key_schema_entry(&g.key_attribute, "HASH")];
                if let Some(sort) = &g.sort_attribute {
                    ks.push(key_schema_entry(sort, "RANGE"));
                }
                gsis.push(index_desc(
                    &g.name,
                    ks,
                    index_status_for(&g.name, index_statuses),
                ));
            }
            SecondaryIndex::Local(l) => {
                let ks = vec![
                    key_schema_entry(&schema.partition_key, "HASH"),
                    key_schema_entry(&l.sort_attribute, "RANGE"),
                ];
                // LSIs are always `Active` by construction (create-time-only,
                // never touched by `SetIndexStatus`) — never consulted from
                // `index_statuses`.
                lsis.push(index_desc(&l.name, ks, IndexStatus::Active));
            }
        }
    }
    if !gsis.is_empty() {
        desc.insert("GlobalSecondaryIndexes".into(), Value::Array(gsis));
    }
    if !lsis.is_empty() {
        desc.insert("LocalSecondaryIndexes".into(), Value::Array(lsis));
    }
    if let Some(s) = stream {
        let mut spec = Map::new();
        spec.insert("StreamEnabled".into(), Value::Bool(true));
        spec.insert(
            "StreamViewType".into(),
            Value::String(stream_view_type_str(s.view_type).into()),
        );
        desc.insert("StreamSpecification".into(), Value::Object(spec));
        desc.insert(
            "LatestStreamArn".into(),
            Value::String(stream_arn(table, &s.label)),
        );
        desc.insert("LatestStreamLabel".into(), Value::String(s.label.clone()));
    }
    desc
}

/// The JSON body for a successful `CreateTable`: a minimal `TableDescription`
/// echoing the name, key schema, any secondary indexes (under
/// `GlobalSecondaryIndexes` / `LocalSecondaryIndexes`), an `ACTIVE` status
/// (tables are immediately usable here — there is no provisioning phase),
/// and — when a stream was enabled (ADR 0042) — its `StreamSpecification` +
/// `LatestStreamArn`.
#[must_use]
pub fn create_table_response(
    table: &str,
    schema: &TableSchema,
    indexes: &[SecondaryIndex],
    stream: Option<&StreamDescription>,
) -> String {
    // Every index a `CreateTable` declares is `Active` by construction (ADR
    // 0041 §5: an empty, just-created table) — no status side channel needed.
    // `CreateTable` always blocks (`ClientCtx::await_table_serveable`) until
    // the table is genuinely `ACTIVE` before ever returning, so this is
    // never `CREATING` here the way `RestoreTableFromBackup`'s own initial
    // response can be (see `restore_table_response`).
    let desc = table_description_object(table, schema, indexes, &[], stream, "ACTIVE");
    let mut obj = Map::new();
    obj.insert("TableDescription".into(), Value::Object(desc));
    serde_json::to_string(&Value::Object(obj)).expect("create-table response serializes")
}

/// The JSON body for a successful `RestoreTableFromBackup` kickoff (ADR
/// 0059 §7, Train 2): the identical `TableDescription` shape
/// [`create_table_response`] wraps, but with an explicit `status` (always
/// `"CREATING"` in practice — the restore driver runs asynchronously, so
/// this response fires the instant the schema/tablet-mint steps commit, well
/// before the tablet is actually seeded/`Active`) and no `StreamSpecification`
/// (a restored table never starts with a stream, ADR 0059 §7 step 2). Every
/// GSI/LSI rendered here is `Active` by construction of the caller having
/// already declared it via `CreateTableIndex` before ever building this
/// response — `index_statuses` is deliberately not threaded through, since a
/// GSI added mid-restore genuinely starts `Creating`/backfilling and this
/// response's own caller (`animusd::dynamo::restore_table_from_backup`)
/// builds it strictly from the already-committed schema before that point.
#[must_use]
pub fn restore_table_response(
    table: &str,
    schema: &TableSchema,
    indexes: &[SecondaryIndex],
    status: &str,
) -> String {
    let desc = table_description_object(table, schema, indexes, &[], None, status);
    let mut obj = Map::new();
    obj.insert("TableDescription".into(), Value::Object(desc));
    serde_json::to_string(&Value::Object(obj)).expect("restore-table response serializes")
}

/// The JSON body for a successful `DescribeTable` (ADR 0042 §2): the same
/// shape as [`create_table_response`], plus `AttributeDefinitions` (derived
/// from `key_types` and `indexes`, covering every base **and** index key
/// attribute — see [`attribute_definitions`]'s own doc for the issue #319
/// coverage fix), wrapped under
/// `Table` (DynamoDB's own `DescribeTable` response shape, distinct from
/// `CreateTable`/`UpdateTable`'s `TableDescription`). `index_statuses` is the
/// caller's Fork-D side channel of each index's *real* replicated-catalog
/// status (`animusd::dynamo::describe_table` reads it off `Metadata`) — see
/// [`table_description_object`]'s doc. `status` is `"ACTIVE"` for every table
/// except one still mid-`RestoreTableFromBackup` (ADR 0059 §7, Train 2),
/// which reports `"CREATING"` — `animusd::dynamo::table_status` derives it
/// from the table's own tablet states (`Building` ⇒ `CREATING`), never
/// stored redundantly.
#[must_use]
pub fn describe_table_response(
    table: &str,
    schema: &TableSchema,
    key_types: &[(String, String)],
    indexes: &[SecondaryIndex],
    index_statuses: &[(String, IndexStatus)],
    stream: Option<&StreamDescription>,
    status: &str,
) -> String {
    let mut desc = table_description_object(table, schema, indexes, index_statuses, stream, status);
    desc.insert(
        "AttributeDefinitions".into(),
        Value::Array(attribute_definitions(schema, key_types, indexes)),
    );
    let mut obj = Map::new();
    obj.insert("Table".into(), Value::Object(desc));
    serde_json::to_string(&Value::Object(obj)).expect("describe-table response serializes")
}

/// The JSON body for a successful `DeleteTable`: the same
/// [`table_description_object`] every other table-description response
/// wraps (name, key schema, `AttributeDefinitions`, secondary indexes with
/// their real status, stream config — the exact fields
/// [`describe_table_response`] emits, populated from the same
/// `animusd`-supplied catalog snapshot) with `TableStatus` overridden to
/// `DELETING` — real DynamoDB's `DeleteTable` response, since the drop
/// itself is asynchronous there (unlike this adapter's own synchronous
/// cascade, ADR 0024). Wrapped under `TableDescription`, matching
/// `CreateTable`/`UpdateTable` rather than `DescribeTable`'s `Table` key.
#[must_use]
pub fn delete_table_response(
    table: &str,
    schema: &TableSchema,
    key_types: &[(String, String)],
    indexes: &[SecondaryIndex],
    index_statuses: &[(String, IndexStatus)],
    stream: Option<&StreamDescription>,
) -> String {
    // "ACTIVE" here is a placeholder immediately overridden below —
    // `table_description_object` always needs a status, but this response's
    // whole point is to report `DELETING` regardless of the table's actual
    // last-known status.
    let mut desc =
        table_description_object(table, schema, indexes, index_statuses, stream, "ACTIVE");
    desc.insert("TableStatus".into(), Value::String("DELETING".into()));
    desc.insert(
        "AttributeDefinitions".into(),
        Value::Array(attribute_definitions(schema, key_types, indexes)),
    );
    let mut obj = Map::new();
    obj.insert("TableDescription".into(), Value::Object(desc));
    serde_json::to_string(&Value::Object(obj)).expect("delete-table response serializes")
}

/// The `AttributeDefinitions` array (base partition key, base sort key when
/// composite, plus every secondary index's own key attribute not already
/// covered by the base) for `schema`/`indexes`, resolving each key
/// attribute's declared type from `key_types` (defaulting to `S` when
/// absent, mirroring `CreateTable`'s own decode). Shared by
/// [`describe_table_response`] and [`delete_table_response`] — the two
/// response shapes that echo it.
///
/// **Real DynamoDB's `AttributeDefinitions` covers every key attribute in
/// the table, base and index alike** — a GSI's hash/sort attribute or an
/// LSI's alternate sort attribute, not just the base table's own
/// partition/sort key (issue #319, closed). This function itself is
/// unchanged by that fix — it has always resolved every name in `names`
/// through a plain lookup in `key_types`, defaulting to `S` only when a
/// name genuinely isn't there. What changed is what its caller now passes:
/// `animusd::dynamo::describe_table`/`delete_table` extend the base table's
/// own typed key columns with `schema_bridge::index_attribute_types` (each
/// index's own `IndexDef.hash_attribute_type`/`sort_attribute_type`,
/// recorded from `CreateTable`/`UpdateTable`'s own `AttributeDefinitions`
/// when the caller supplied one — see `animus_control::IndexDef`'s own
/// doc), so an index-only key attribute now renders its **real** declared
/// type when one was ever recorded, and only falls back to the honest
/// "unknown, defaulted" `S` when it genuinely wasn't.
fn attribute_definitions(
    schema: &TableSchema,
    key_types: &[(String, String)],
    indexes: &[SecondaryIndex],
) -> Vec<Value> {
    let mut names = vec![schema.partition_key.clone()];
    if let Some(sk) = &schema.sort_key {
        names.push(sk.clone());
    }
    for index in indexes {
        match index {
            SecondaryIndex::Global(g) => {
                if !names.contains(&g.key_attribute) {
                    names.push(g.key_attribute.clone());
                }
                if let Some(sort) = &g.sort_attribute
                    && !names.contains(sort)
                {
                    names.push(sort.clone());
                }
            }
            // An LSI's hash is always the base partition key (already
            // pushed above) — only its own alternate sort attribute can be
            // new.
            SecondaryIndex::Local(l) => {
                if !names.contains(&l.sort_attribute) {
                    names.push(l.sort_attribute.clone());
                }
            }
        }
    }
    names
        .iter()
        .map(|n| attribute_definition(n, key_types))
        .collect()
}

fn attribute_definition(name: &str, key_types: &[(String, String)]) -> Value {
    let ty = key_types
        .iter()
        .find(|(n, _)| n == name)
        .map_or("S", |(_, t)| t.as_str());
    let mut e = Map::new();
    e.insert("AttributeName".into(), Value::String(name.to_owned()));
    e.insert("AttributeType".into(), Value::String(ty.to_owned()));
    Value::Object(e)
}

/// The default and cap for `ListTables`'s `Limit`, matching real DynamoDB.
pub const LIST_TABLES_MAX_LIMIT: usize = 100;

/// Paginate a full, already-lexicographically-sorted table-name list per
/// `ListTables`'s contract: skip past `exclusive_start_table_name` (start
/// strictly after it — a binary search, valid because `names` is sorted),
/// take at most `limit` (`None` defaults to [`LIST_TABLES_MAX_LIMIT`]; any
/// value is capped at it, matching real DynamoDB), and report the page's
/// last name as the pagination cursor **only when the listing was
/// truncated** (matching DynamoDB — an untruncated page carries no
/// `LastEvaluatedTableName`). `names` must already be the caller's *final*
/// candidate set — already filtered by whatever policy decides which names
/// to expose (`animusd::dynamo::list_tables` excludes a materialized GSI's
/// hidden table before calling this).
#[must_use]
pub fn paginate_table_names(
    names: &[String],
    exclusive_start_table_name: Option<&str>,
    limit: Option<usize>,
) -> (Vec<String>, Option<String>) {
    let limit = limit
        .unwrap_or(LIST_TABLES_MAX_LIMIT)
        .min(LIST_TABLES_MAX_LIMIT);
    let start = exclusive_start_table_name
        .map(|s| names.partition_point(|n| n.as_str() <= s))
        .unwrap_or(0);
    let remaining = &names[start..];
    let truncated = remaining.len() > limit;
    let page = remaining[..remaining.len().min(limit)].to_vec();
    let last_evaluated = truncated.then(|| page.last().cloned()).flatten();
    (page, last_evaluated)
}

// --- Backups (ADR 0059, Train 1 PR④) ---------------------------------------

/// A backup's `BackupDetails` (ADR 0059, Train 1 PR④) — DynamoDB's shared
/// shape for `CreateBackup`'s response and as one field of `DescribeBackup`'s
/// `BackupDescription` / an entry of `ListBackups`' `BackupSummaries`. Built
/// by `animusd::dynamo` from the replicated catalog (`animus_control::
/// BackupRow`), which this pure crate never reads directly.
///
/// `status` is always one of AWS's own three on-demand values (`"CREATING"`,
/// `"AVAILABLE"`, `"DELETED"`) — this adapter's internal `Failed`/`Expired`
/// catalog states are mapped to `"DELETED"` by the caller before this struct
/// is ever built (a backup a client can still see either exists or it
/// doesn't; AWS's own wire vocabulary has no third state to report either
/// failure as).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupDetails {
    /// This backup's ARN — also the catalog's own opaque identity
    /// ([`backup_arn`]'s doc).
    pub backup_arn: String,
    /// The client-supplied `BackupName`.
    pub backup_name: String,
    /// `"CREATING"` | `"AVAILABLE"` | `"DELETED"`.
    pub status: &'static str,
    /// Wall-clock creation time in epoch **milliseconds**
    /// (`BackupManifest::created_wall_ms`, ADR 0051's `env.wall_now()`
    /// discipline).
    pub creation_wall_ms: u64,
    /// Total captured bytes so far (`Metadata::backup_total_bytes`) — `0`
    /// while still `CREATING` and nothing has landed yet.
    pub size_bytes: u64,
}

fn backup_details_object(d: &BackupDetails) -> Map<String, Value> {
    let mut obj = Map::new();
    obj.insert("BackupArn".into(), Value::String(d.backup_arn.clone()));
    obj.insert("BackupName".into(), Value::String(d.backup_name.clone()));
    obj.insert("BackupStatus".into(), Value::String(d.status.into()));
    // Every backup this adapter produces today is on-demand (Train 1) —
    // PITR's `SYSTEM` base snapshots are Train 3, `AWS_BACKUP` is never
    // produced.
    obj.insert("BackupType".into(), Value::String("USER".into()));
    obj.insert(
        "BackupCreationDateTime".into(),
        wall_ms_timestamp(d.creation_wall_ms),
    );
    obj.insert(
        "BackupSizeBytes".into(),
        Value::Number(serde_json::Number::from(d.size_bytes)),
    );
    obj
}

/// Render a wall-clock millisecond instant as an AWS `Timestamp` (a JSON
/// number of epoch seconds, possibly fractional) — the identical shape
/// `streams_wire::stream_record_json`'s `ApproximateCreationDateTime` uses.
fn wall_ms_timestamp(wall_ms: u64) -> Value {
    Value::Number(
        serde_json::Number::from_f64((wall_ms as f64) / 1000.0)
            .unwrap_or_else(|| serde_json::Number::from(0)),
    )
}

/// The JSON body for a successful `CreateBackup`: `{"BackupDetails": {..}}`.
#[must_use]
pub fn create_backup_response(details: &BackupDetails) -> String {
    let mut obj = Map::new();
    obj.insert(
        "BackupDetails".into(),
        Value::Object(backup_details_object(details)),
    );
    serde_json::to_string(&Value::Object(obj)).expect("create-backup response serializes")
}

/// One index entry inside `SourceTableFeatureDetails` — name + key schema
/// only (unlike [`index_desc`], AWS's real `SourceTableFeatureDetails` index
/// entries carry no `IndexStatus`/`Backfilling` at all).
fn source_table_index_entry(name: &str, key_schema: Vec<Value>) -> Value {
    let mut e = Map::new();
    e.insert("IndexName".into(), Value::String(name.to_owned()));
    e.insert("KeySchema".into(), Value::Array(key_schema));
    Value::Object(e)
}

/// The JSON body for a successful `DescribeBackup` **or** `DeleteBackup`
/// (ADR 0059, Train 1 PR④) — both respond with the identical
/// `BackupDescription` shape (`DeleteBackup`'s own `details.status` is
/// `"DELETED"`, set by the caller before this is called):
/// `{"BackupDescription": {"BackupDetails": .., "SourceTableDetails": ..,
/// "SourceTableFeatureDetails": ..}}`. Built from the manifest's own
/// captured `TableSchema`/index/stream/TTL snapshot (a plain owned clone,
/// ADR 0059 §2 — never a live catalog lookup, so this works even after the
/// source table has been dropped), via the same `animus_dynamo::schema`
/// bridge `describe_table_response` uses for the *live* catalog.
#[must_use]
pub fn backup_description_response(
    details: &BackupDetails,
    table: &str,
    schema: &TableSchema,
    indexes: &[SecondaryIndex],
    stream: Option<&StreamDescription>,
    ttl: Option<&TtlDescription>,
) -> String {
    let mut key_schema = vec![key_schema_entry(&schema.partition_key, "HASH")];
    if let Some(sk) = &schema.sort_key {
        key_schema.push(key_schema_entry(sk, "RANGE"));
    }
    let mut source_table = Map::new();
    source_table.insert("TableName".into(), Value::String(table.to_owned()));
    source_table.insert("KeySchema".into(), Value::Array(key_schema));

    let mut features = Map::new();
    let mut gsis = Vec::new();
    let mut lsis = Vec::new();
    for index in indexes {
        match index {
            SecondaryIndex::Global(g) => {
                let mut ks = vec![key_schema_entry(&g.key_attribute, "HASH")];
                if let Some(sort) = &g.sort_attribute {
                    ks.push(key_schema_entry(sort, "RANGE"));
                }
                gsis.push(source_table_index_entry(&g.name, ks));
            }
            SecondaryIndex::Local(l) => {
                let ks = vec![
                    key_schema_entry(&schema.partition_key, "HASH"),
                    key_schema_entry(&l.sort_attribute, "RANGE"),
                ];
                lsis.push(source_table_index_entry(&l.name, ks));
            }
        }
    }
    if !gsis.is_empty() {
        features.insert("GlobalSecondaryIndexes".into(), Value::Array(gsis));
    }
    if !lsis.is_empty() {
        features.insert("LocalSecondaryIndexes".into(), Value::Array(lsis));
    }
    if let Some(s) = stream {
        let mut spec = Map::new();
        spec.insert(
            "StreamViewType".into(),
            Value::String(stream_view_type_str(s.view_type).into()),
        );
        spec.insert("StreamEnabled".into(), Value::Bool(true));
        features.insert("StreamDescription".into(), Value::Object(spec));
    }
    if let Some(t) = ttl
        && t.enabled
    {
        let mut tspec = Map::new();
        tspec.insert("TimeToLiveStatus".into(), Value::String("ENABLED".into()));
        if let Some(name) = &t.attribute_name {
            tspec.insert("AttributeName".into(), Value::String(name.clone()));
        }
        features.insert("TimeToLiveDescription".into(), Value::Object(tspec));
    }

    let mut desc = Map::new();
    desc.insert(
        "BackupDetails".into(),
        Value::Object(backup_details_object(details)),
    );
    desc.insert("SourceTableDetails".into(), Value::Object(source_table));
    desc.insert("SourceTableFeatureDetails".into(), Value::Object(features));
    let mut obj = Map::new();
    obj.insert("BackupDescription".into(), Value::Object(desc));
    serde_json::to_string(&Value::Object(obj)).expect("backup-description response serializes")
}

/// One `ListBackups` candidate: a backup's own [`BackupDetails`] plus its
/// source table name (AWS's real `BackupSummary` is a flat object carrying
/// both). `animusd::dynamo` builds this list from the replicated catalog
/// (`Metadata::backups`, whose `BTreeMap<BackupId, _>` iteration order is
/// already the ARN-lexicographic order [`paginate_backup_summaries`] relies
/// on) before calling it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupSummary {
    /// The source table this backup was taken from.
    pub table: String,
    /// This backup's own details.
    pub details: BackupDetails,
}

/// The default and cap for `ListBackups`'s `Limit`, matching real DynamoDB.
pub const LIST_BACKUPS_MAX_LIMIT: usize = 100;

/// Paginate an already-ARN-sorted candidate list per `ListBackups`'s
/// contract — [`paginate_table_names`]'s identical shape, generalized to a
/// keyed struct.
#[must_use]
pub fn paginate_backup_summaries(
    summaries: &[BackupSummary],
    exclusive_start_backup_arn: Option<&str>,
    limit: Option<usize>,
) -> (Vec<BackupSummary>, Option<String>) {
    let limit = limit
        .unwrap_or(LIST_BACKUPS_MAX_LIMIT)
        .clamp(1, LIST_BACKUPS_MAX_LIMIT);
    let start = exclusive_start_backup_arn
        .map(|arn| summaries.partition_point(|s| s.details.backup_arn.as_str() <= arn))
        .unwrap_or(0);
    let remaining = &summaries[start..];
    let truncated = remaining.len() > limit;
    let page = remaining[..remaining.len().min(limit)].to_vec();
    let last_evaluated = truncated
        .then(|| page.last().map(|s| s.details.backup_arn.clone()))
        .flatten();
    (page, last_evaluated)
}

/// The JSON body for a successful `ListBackups`: `{"BackupSummaries": [...],
/// "LastEvaluatedBackupArn": ".."}` (the latter present only when the
/// listing was truncated, matching [`list_tables_response`]'s own
/// convention).
#[must_use]
pub fn list_backups_response(
    page: &[BackupSummary],
    last_evaluated_backup_arn: Option<&str>,
) -> String {
    let summaries: Vec<Value> = page
        .iter()
        .map(|s| {
            let mut e = backup_details_object(&s.details);
            e.insert("TableName".into(), Value::String(s.table.clone()));
            Value::Object(e)
        })
        .collect();
    let mut obj = Map::new();
    obj.insert("BackupSummaries".into(), Value::Array(summaries));
    if let Some(arn) = last_evaluated_backup_arn {
        obj.insert(
            "LastEvaluatedBackupArn".into(),
            Value::String(arn.to_owned()),
        );
    }
    serde_json::to_string(&Value::Object(obj)).expect("list-backups response serializes")
}

/// The JSON body for a successful `ListTables`: `{"TableNames": [...]}`,
/// plus `"LastEvaluatedTableName"` only when [`paginate_table_names`]
/// reports the listing was truncated.
#[must_use]
pub fn list_tables_response(names: &[String], last_evaluated_table_name: Option<&str>) -> String {
    let mut obj = Map::new();
    obj.insert(
        "TableNames".into(),
        Value::Array(names.iter().cloned().map(Value::String).collect()),
    );
    if let Some(last) = last_evaluated_table_name {
        obj.insert(
            "LastEvaluatedTableName".into(),
            Value::String(last.to_owned()),
        );
    }
    serde_json::to_string(&Value::Object(obj)).expect("list-tables response serializes")
}

/// One index entry in a `TableDescription`/`Table`: name, key schema, and its
/// real `IndexStatus` (`CREATING`/`ACTIVE`/`DELETING`, DynamoDB's own
/// `SCREAMING_SNAKE_CASE`). `Backfilling: true` is added only while
/// `Creating` — matching AWS, which omits the attribute entirely once a GSI
/// has finished backfilling (never renders it as `false`).
fn index_desc(name: &str, key_schema: Vec<Value>, status: IndexStatus) -> Value {
    let mut g = Map::new();
    g.insert("IndexName".into(), Value::String(name.to_owned()));
    g.insert("KeySchema".into(), Value::Array(key_schema));
    g.insert(
        "IndexStatus".into(),
        Value::String(index_status_str(status).into()),
    );
    if status == IndexStatus::Creating {
        g.insert("Backfilling".into(), Value::Bool(true));
    }
    Value::Object(g)
}

/// The JSON body for a successful `UpdateTimeToLive` (ADR 0051):
/// `{"TimeToLiveSpecification": {"Enabled": bool, "AttributeName": ".."}}`
/// — AWS's own contract is to echo back exactly the spec that was applied,
/// not a separately-recomputed description, so this takes the same
/// `attribute_name`/`enabled` pair `decode_update_time_to_live` produced
/// rather than a [`TtlDescription`].
#[must_use]
pub fn update_time_to_live_response(attribute_name: &str, enabled: bool) -> String {
    let mut spec = Map::new();
    spec.insert("Enabled".into(), Value::Bool(enabled));
    spec.insert(
        "AttributeName".into(),
        Value::String(attribute_name.to_owned()),
    );
    let mut obj = Map::new();
    obj.insert("TimeToLiveSpecification".into(), Value::Object(spec));
    serde_json::to_string(&Value::Object(obj)).expect("update-ttl response serializes")
}

/// The JSON body for a successful `DescribeTimeToLive` (ADR 0051):
/// `{"TimeToLiveDescription": {"TimeToLiveStatus": "ENABLED"|"DISABLED",
/// "AttributeName": ".."}}`. `AttributeName` is **omitted entirely** when
/// the status is `DISABLED` — matching AWS, which never renders a null/empty
/// `AttributeName` for a disabled table.
///
/// **Deliberate simplification**: real DynamoDB's `TimeToLiveStatus`
/// vocabulary also includes the transient `ENABLING`/`DISABLING` values for
/// an asynchronous change still in flight. This adapter's `UpdateTimeToLive`
/// takes effect synchronously (there is no async TTL-enable pipeline here),
/// so only `ENABLED`/`DISABLED` are ever produced — see the crate guide's
/// "Still deferred" section.
#[must_use]
pub fn describe_time_to_live_response(desc: &TtlDescription) -> String {
    let mut inner = Map::new();
    inner.insert(
        "TimeToLiveStatus".into(),
        Value::String(if desc.enabled { "ENABLED" } else { "DISABLED" }.into()),
    );
    if desc.enabled
        && let Some(name) = &desc.attribute_name
    {
        inner.insert("AttributeName".into(), Value::String(name.clone()));
    }
    let mut obj = Map::new();
    obj.insert("TimeToLiveDescription".into(), Value::Object(inner));
    serde_json::to_string(&Value::Object(obj)).expect("describe-ttl response serializes")
}

/// The JSON body for a successful `TagResource`/`UntagResource` (roadmap
/// W-06): both are AWS-faithfully a bare `{}` on success — neither op
/// echoes anything back.
#[must_use]
pub fn tag_or_untag_resource_response() -> String {
    "{}".to_owned()
}

/// The JSON body for a successful `ListTagsOfResource` (roadmap W-06):
/// `{"Tags": [{"Key": .., "Value": ..}, ...]}`, sorted by key (`tags`'
/// `BTreeMap` order) — never a `NextToken`, since this adapter always
/// returns the whole set (see [`Operation::ListTagsOfResource`]'s own doc).
#[must_use]
pub fn list_tags_of_resource_response(tags: &BTreeMap<String, String>) -> String {
    let entries: Vec<Value> = tags
        .iter()
        .map(|(k, v)| {
            let mut e = Map::new();
            e.insert("Key".into(), Value::String(k.clone()));
            e.insert("Value".into(), Value::String(v.clone()));
            Value::Object(e)
        })
        .collect();
    let mut obj = Map::new();
    obj.insert("Tags".into(), Value::Array(entries));
    serde_json::to_string(&Value::Object(obj)).expect("list-tags response serializes")
}

/// `DescribeLimits`' static account-wide read capacity ceiling (roadmap
/// W-06) — real DynamoDB's own documented on-demand default.
pub const ACCOUNT_MAX_READ_CAPACITY_UNITS: u64 = 80_000;
/// `DescribeLimits`' static account-wide write capacity ceiling.
pub const ACCOUNT_MAX_WRITE_CAPACITY_UNITS: u64 = 80_000;
/// `DescribeLimits`' static per-table read capacity ceiling.
pub const TABLE_MAX_READ_CAPACITY_UNITS: u64 = 40_000;
/// `DescribeLimits`' static per-table write capacity ceiling.
pub const TABLE_MAX_WRITE_CAPACITY_UNITS: u64 = 40_000;

/// The JSON body for a successful `DescribeLimits` (roadmap W-06): AWS's
/// documented on-demand-default shape, `{AccountMaxReadCapacityUnits,
/// AccountMaxWriteCapacityUnits, TableMaxReadCapacityUnits,
/// TableMaxWriteCapacityUnits}`. This adapter has no provisioned-capacity
/// billing meter at all (root `CLAUDE.md`'s "no RCUs/WCUs to plan
/// around") — these four constants are reported honestly as a static
/// ceiling an SDK's own tooling can probe, never derived from anything
/// this adapter tracks.
#[must_use]
pub fn describe_limits_response() -> String {
    let mut obj = Map::new();
    obj.insert(
        "AccountMaxReadCapacityUnits".into(),
        Value::from(ACCOUNT_MAX_READ_CAPACITY_UNITS),
    );
    obj.insert(
        "AccountMaxWriteCapacityUnits".into(),
        Value::from(ACCOUNT_MAX_WRITE_CAPACITY_UNITS),
    );
    obj.insert(
        "TableMaxReadCapacityUnits".into(),
        Value::from(TABLE_MAX_READ_CAPACITY_UNITS),
    );
    obj.insert(
        "TableMaxWriteCapacityUnits".into(),
        Value::from(TABLE_MAX_WRITE_CAPACITY_UNITS),
    );
    serde_json::to_string(&Value::Object(obj)).expect("describe-limits response serializes")
}

/// How long an SDK is told it may cache a `DescribeEndpoints` result before
/// asking again — AWS's own documented default.
pub const DESCRIBE_ENDPOINTS_CACHE_PERIOD_MINUTES: u64 = 1440;

/// The JSON body for a successful `DescribeEndpoints` (roadmap W-06):
/// `{"Endpoints":[{"Address": .., "CachePeriodInMinutes": 1440}]}` — a
/// single entry naming the caller's own node (`animusd::dynamo::
/// describe_endpoints` supplies its own bound DynamoDB listen address; this
/// crate has no notion of "the cluster's other nodes").
#[must_use]
pub fn describe_endpoints_response(address: &str) -> String {
    let mut endpoint = Map::new();
    endpoint.insert("Address".into(), Value::String(address.to_owned()));
    endpoint.insert(
        "CachePeriodInMinutes".into(),
        Value::from(DESCRIBE_ENDPOINTS_CACHE_PERIOD_MINUTES),
    );
    let mut obj = Map::new();
    obj.insert(
        "Endpoints".into(),
        Value::Array(vec![Value::Object(endpoint)]),
    );
    serde_json::to_string(&Value::Object(obj)).expect("describe-endpoints response serializes")
}

/// A table's point-in-time recovery (PITR) configuration and restorable
/// window (ADR 0059 §9), as both `UpdateContinuousBackups` and
/// `DescribeContinuousBackups` render it. `animusd` derives every field
/// from the replicated catalog plus sealed-segment/base-snapshot coverage —
/// this crate never computes any of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PitrDescription {
    /// Whether PITR is currently enabled for this table.
    pub enabled: bool,
    /// The earliest wall-clock instant (epoch milliseconds) this table can
    /// currently be restored to — the retention floor or this generation's
    /// own enable time, whichever is later. `None` iff `enabled` is `false`
    /// (this adapter never reports a window for a disabled table, matching
    /// AWS's own contract).
    pub earliest_restorable_ms: Option<u64>,
    /// The latest wall-clock instant (epoch milliseconds) this table can
    /// currently be restored to — honestly trailing "now" by seal lag
    /// (ADR 0059 §9: "never claim 'now'"). `None` iff `enabled` is `false`.
    pub latest_restorable_ms: Option<u64>,
}

/// The shared `ContinuousBackupsDescription` object both
/// `UpdateContinuousBackups` and `DescribeContinuousBackups` respond with:
/// `{"ContinuousBackupsStatus": "ENABLED", "PointInTimeRecoveryDescription":
/// {"PointInTimeRecoveryStatus": "ENABLED"|"DISABLED",
/// "EarliestRestorableDateTime": .., "LatestRestorableDateTime": ..}}`.
/// `ContinuousBackupsStatus` (the outer field — whether the continuous-
/// backups *infrastructure itself* is available, as opposed to whether
/// *this table* has opted in) is always `"ENABLED"` in this adapter: PITR
/// infrastructure always exists here (there is no
/// `ContinuousBackupsUnavailableException` case to model). `Earliest`/
/// `LatestRestorableDateTime` are omitted entirely while disabled — the
/// identical "omit rather than render a meaningless value" convention
/// [`describe_time_to_live_response`]'s own `AttributeName` omission uses.
fn continuous_backups_description_object(desc: &PitrDescription) -> Value {
    let mut pitr = Map::new();
    pitr.insert(
        "PointInTimeRecoveryStatus".into(),
        Value::String(if desc.enabled { "ENABLED" } else { "DISABLED" }.into()),
    );
    if let Some(ms) = desc.earliest_restorable_ms {
        pitr.insert("EarliestRestorableDateTime".into(), wall_ms_timestamp(ms));
    }
    if let Some(ms) = desc.latest_restorable_ms {
        pitr.insert("LatestRestorableDateTime".into(), wall_ms_timestamp(ms));
    }
    let mut outer = Map::new();
    outer.insert(
        "ContinuousBackupsStatus".into(),
        Value::String("ENABLED".into()),
    );
    outer.insert("PointInTimeRecoveryDescription".into(), Value::Object(pitr));
    Value::Object(outer)
}

/// The JSON body for a successful `UpdateContinuousBackups` (ADR 0059 §9):
/// `{"ContinuousBackupsDescription": {..}}` — AWS's own contract renders
/// the resulting description, matching `describe_continuous_backups_response`'s
/// exact shape (a caller cannot tell the two calls' responses apart by body
/// shape alone, matching real DynamoDB).
#[must_use]
pub fn update_continuous_backups_response(desc: &PitrDescription) -> String {
    let mut obj = Map::new();
    obj.insert(
        "ContinuousBackupsDescription".into(),
        continuous_backups_description_object(desc),
    );
    serde_json::to_string(&Value::Object(obj))
        .expect("update-continuous-backups response serializes")
}

/// The JSON body for a successful `DescribeContinuousBackups` (ADR 0059
/// §9) — identical shape to [`update_continuous_backups_response`].
#[must_use]
pub fn describe_continuous_backups_response(desc: &PitrDescription) -> String {
    update_continuous_backups_response(desc)
}

/// DynamoDB's own `SCREAMING_SNAKE_CASE` rendering of an [`IndexStatus`].
fn index_status_str(status: IndexStatus) -> &'static str {
    match status {
        IndexStatus::Creating => "CREATING",
        IndexStatus::Active => "ACTIVE",
        IndexStatus::Deleting => "DELETING",
    }
}

/// Resolve `name`'s real status from the Fork-D side channel, defaulting to
/// `Active` for an index absent from it (see [`table_description_object`]'s
/// doc for why that default is correct, not merely convenient).
fn index_status_for(name: &str, statuses: &[(String, IndexStatus)]) -> IndexStatus {
    statuses
        .iter()
        .find(|(n, _)| n == name)
        .map_or(IndexStatus::Active, |(_, s)| *s)
}

fn key_schema_entry(name: &str, role: &str) -> Value {
    let mut e = Map::new();
    e.insert("AttributeName".into(), Value::String(name.to_owned()));
    e.insert("KeyType".into(), Value::String(role.to_owned()));
    Value::Object(e)
}

// --- base64 (standard alphabet, with padding) ------------------------------
//
// A tiny self-contained codec so the crate takes no new dependency for the `B`
// type. Standard alphabet (`A-Za-z0-9+/`) with `=` padding, matching the
// DynamoDB wire encoding for binary attributes. The `animusd` admin/dashboard
// display surfaces use the unpadded base64url variant below instead.

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode `bytes` as standard base64 (`A-Za-z0-9+/`, `=`-padded).
#[must_use]
pub fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(B64[b0 >> 2] as char);
        out.push(B64[((b0 & 0x03) << 4) | (b1 >> 4)] as char);
        out.push(if chunk.len() > 1 {
            B64[((b1 & 0x0f) << 2) | (b2 >> 6)] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[b2 & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

/// Decode standard `=`-padded base64: `None` on a length not a multiple of 4,
/// a character outside the alphabet, or over-padding.
#[must_use]
pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        if pad > 2 {
            return None;
        }
        let q: Vec<u8> = chunk
            .iter()
            .map(|&c| if c == b'=' { Some(0) } else { val(c) })
            .collect::<Option<_>>()?;
        let n = (usize::from(q[0]) << 18)
            | (usize::from(q[1]) << 12)
            | (usize::from(q[2]) << 6)
            | usize::from(q[3]);
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Some(out)
}

// --- base64url (URL-safe alphabet, no padding) ------------------------------
//
// RFC 4648 §5 with padding omitted — the display encoding for opaque bytes in
// `animusd`'s admin/dashboard surfaces, where a rendered value is pasted back
// into `?key=`/`?start=` query strings (the standard alphabet's `+` decodes as
// a space there, and `=` padding percent-encodes noisily). The decoder is
// strict (canonical): it rejects anything no byte string encodes to, which
// keeps "does it decode?" a meaningful discriminator for the key display.

const B64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Encode `bytes` as unpadded base64url (`A-Za-z0-9-_`, RFC 4648 §5).
#[must_use]
pub fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(B64URL[b0 >> 2] as char);
        out.push(B64URL[((b0 & 0x03) << 4) | (b1 >> 4)] as char);
        if chunk.len() > 1 {
            out.push(B64URL[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        }
        if chunk.len() > 2 {
            out.push(B64URL[b2 & 0x3f] as char);
        }
    }
    out
}

/// Decode unpadded base64url: `None` on a character outside the URL-safe
/// alphabet, an impossible length (`len % 4 == 1`), or non-canonical trailing
/// bits (a final quantum whose unused low bits are nonzero — no byte string
/// encodes to such a form).
#[must_use]
pub fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3 + 2);
    for chunk in bytes.chunks(4) {
        let mut acc: u32 = 0;
        for &c in chunk {
            acc = (acc << 6) | val(c)?;
        }
        match chunk.len() {
            4 => out.extend_from_slice(&[(acc >> 16) as u8, (acc >> 8) as u8, acc as u8]),
            3 => {
                if acc & 0x03 != 0 {
                    return None;
                }
                out.extend_from_slice(&[(acc >> 10) as u8, (acc >> 2) as u8]);
            }
            2 => {
                if acc & 0x0f != 0 {
                    return None;
                }
                out.push((acc >> 4) as u8);
            }
            _ => unreachable!("len % 4 == 1 was rejected above"),
        }
    }
    Some(out)
}

/// The serialized form of an item as stored in the data plane. A live item is
/// `{"item": {..}}`; a deleted item is recorded as a tombstone (`{"tombstone":
/// true}`) because the data plane has no native delete yet (ADR 0010). A
/// [`GetItem`] treats a tombstone as absent.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredItem {
    Item(BTreeMap<String, AttributeValue>),
    Tombstone,
}

/// Serialize a live item to the bytes the data plane stores at its key.
#[must_use]
pub fn encode_stored_item(item: &Item) -> Vec<u8> {
    serde_json::to_vec(&StoredItem::Item(item.clone())).expect("stored item serializes")
}

/// Serialize a delete tombstone (the data plane has no native delete).
#[must_use]
pub fn encode_tombstone() -> Vec<u8> {
    serde_json::to_vec(&StoredItem::Tombstone).expect("tombstone serializes")
}

/// Decode bytes read from the data plane back into an item, or `None` for an
/// absent key or a tombstone.
///
/// # Errors
/// Returns a [`WireError`] if the stored bytes are not a valid encoded item.
pub fn decode_stored_item(bytes: &[u8]) -> Result<Option<Item>, WireError> {
    let stored: StoredItem = serde_json::from_slice(bytes)
        .map_err(|e| WireError::serialization(format!("corrupt stored item: {e}")))?;
    Ok(match stored {
        StoredItem::Item(item) => Some(item),
        StoredItem::Tombstone => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.into())
    }

    /// A one-segment field path — the common case in these tests.
    fn field(name: &str) -> Vec<PathSegment> {
        vec![PathSegment::Field(name.into())]
    }

    fn n(v: &str) -> AttributeValue {
        AttributeValue::N(v.into())
    }

    #[test]
    fn decodes_a_put_item_request() {
        let body = br#"{"TableName":"users","Item":{"id":{"S":"u1"},"n":{"N":"42"},
                        "ok":{"BOOL":true},"void":{"NULL":true}}}"#;
        let op = decode_request("DynamoDB_20120810.PutItem", body).unwrap();
        let Operation::PutItem { table, item, .. } = op else {
            panic!("expected PutItem");
        };
        assert_eq!(table, "users");
        assert_eq!(item.get("id"), Some(&s("u1")));
        assert_eq!(item.get("n"), Some(&AttributeValue::N("42".into())));
        assert_eq!(item.get("ok"), Some(&AttributeValue::Bool(true)));
        assert_eq!(item.get("void"), Some(&AttributeValue::Null));
    }

    #[test]
    fn decodes_get_and_delete_keys() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}}}"#;
        match decode_request("DynamoDB_20120810.GetItem", body).unwrap() {
            Operation::GetItem { table, key, .. } => {
                assert_eq!(table, "t");
                assert_eq!(key.get("id"), Some(&s("k")));
            }
            other => panic!("expected GetItem, got {other:?}"),
        }
        match decode_request("DynamoDB_20120810.DeleteItem", body).unwrap() {
            Operation::DeleteItem { table, .. } => assert_eq!(table, "t"),
            other => panic!("expected DeleteItem, got {other:?}"),
        }
    }

    #[test]
    fn decodes_delete_table_request() {
        let body = br#"{"TableName":"t"}"#;
        match decode_request("DynamoDB_20120810.DeleteTable", body).unwrap() {
            Operation::DeleteTable { table } => assert_eq!(table, "t"),
            other => panic!("expected DeleteTable, got {other:?}"),
        }
    }

    #[test]
    fn decodes_list_tables_request_with_all_fields() {
        let body = br#"{"ExclusiveStartTableName":"foo","Limit":25}"#;
        match decode_request("DynamoDB_20120810.ListTables", body).unwrap() {
            Operation::ListTables {
                exclusive_start_table_name,
                limit,
            } => {
                assert_eq!(exclusive_start_table_name.as_deref(), Some("foo"));
                assert_eq!(limit, Some(25));
            }
            other => panic!("expected ListTables, got {other:?}"),
        }
    }

    #[test]
    fn decodes_list_tables_request_with_no_fields() {
        match decode_request("DynamoDB_20120810.ListTables", b"{}").unwrap() {
            Operation::ListTables {
                exclusive_start_table_name,
                limit,
            } => {
                assert_eq!(exclusive_start_table_name, None);
                assert_eq!(limit, None);
            }
            other => panic!("expected ListTables, got {other:?}"),
        }
    }

    #[test]
    fn decode_list_tables_rejects_a_negative_limit() {
        let body = br#"{"Limit":-1}"#;
        let err = decode_request("DynamoDB_20120810.ListTables", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn unknown_target_is_rejected() {
        // `BatchGetItem` is supported now; a malformed body is a validation
        // error rather than an unknown operation.
        let err = decode_request("DynamoDB_20120810.BatchGetItem", b"{}").unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn unsupported_type_is_rejected() {
        // `XX` is not a DynamoDB attribute type (M/L/SS/NS/BS are now supported).
        let body = br#"{"TableName":"t","Item":{"id":{"XX":""}}}"#;
        let err = decode_request("DynamoDB_20120810.PutItem", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn item_json_round_trips() {
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        item.insert("blob".into(), AttributeValue::B(vec![0, 1, 2, 255, 254]));
        let encoded = encode_item(&item);
        let decoded = decode_item(encoded.as_object().unwrap()).unwrap();
        assert_eq!(decoded, item);
    }

    #[test]
    fn document_and_set_types_round_trip() {
        let mut nested = BTreeMap::new();
        nested.insert("k".into(), AttributeValue::N("9".into()));
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        item.insert("map".into(), AttributeValue::M(nested));
        item.insert(
            "list".into(),
            AttributeValue::L(vec![
                s("x"),
                AttributeValue::Bool(true),
                AttributeValue::Null,
            ]),
        );
        // Sets are canonicalized (sorted + deduped) on decode.
        item.insert(
            "ss".into(),
            AttributeValue::SS(vec!["a".into(), "b".into()]),
        );
        item.insert(
            "ns".into(),
            AttributeValue::NS(vec!["1".into(), "2".into()]),
        );
        item.insert(
            "bs".into(),
            AttributeValue::BS(vec![vec![0, 1], vec![2, 3]]),
        );
        let encoded = encode_item(&item);
        let decoded = decode_item(encoded.as_object().unwrap()).unwrap();
        assert_eq!(decoded, item);
    }

    #[test]
    fn set_decode_sorts_and_dedups() {
        let body = br#"{"TableName":"t","Item":{"id":{"S":"k"},
            "tags":{"SS":["c","a","a","b"]}}}"#;
        let Operation::PutItem { item, .. } =
            decode_request("DynamoDB_20120810.PutItem", body).unwrap()
        else {
            panic!("expected PutItem");
        };
        assert_eq!(
            item.get("tags"),
            Some(&AttributeValue::SS(vec![
                "a".into(),
                "b".into(),
                "c".into()
            ]))
        );
    }

    #[test]
    fn projection_keeps_only_requested_attributes() {
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        item.insert("name".into(), s("Ada"));
        item.insert("secret".into(), s("hidden"));
        let p = Projection(vec![field("id"), field("name"), field("absent")]);
        let projected = p.apply(&item);
        assert_eq!(projected.len(), 2);
        assert_eq!(projected.get("id"), Some(&s("u1")));
        assert_eq!(projected.get("name"), Some(&s("Ada")));
        assert!(!projected.contains_key("secret"));
    }

    #[test]
    fn decodes_projection_expression_with_name_aliases() {
        let body = br##"{"TableName":"t","Key":{"id":{"S":"k"}},
            "ProjectionExpression":"id, #n",
            "ExpressionAttributeNames":{"#n":"name"}}"##;
        let Operation::GetItem { projection, .. } =
            decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert_eq!(
            projection,
            Some(Projection(vec![field("id"), field("name")]))
        );
    }

    #[test]
    fn decodes_attributes_to_get_legacy_projection() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "AttributesToGet":["id","name"]}"#;
        let Operation::GetItem { projection, .. } =
            decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert_eq!(
            projection,
            Some(Projection(vec![field("id"), field("name")]))
        );
    }

    #[test]
    fn decodes_document_path_projection() {
        // Document-path projections (`a.b`) are supported.
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "ProjectionExpression":"a.b, c"}"#;
        let Operation::GetItem { projection, .. } =
            decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert_eq!(
            projection,
            Some(Projection(vec![
                vec![
                    PathSegment::Field("a".into()),
                    PathSegment::Field("b".into())
                ],
                field("c"),
            ]))
        );
    }

    #[test]
    fn decodes_list_index_projection() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "ProjectionExpression":"a[0], a[0].b, matrix[1][2]"}"#;
        let Operation::GetItem { projection, .. } =
            decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert_eq!(
            projection,
            Some(Projection(vec![
                vec![PathSegment::Field("a".into()), PathSegment::Index(0)],
                vec![
                    PathSegment::Field("a".into()),
                    PathSegment::Index(0),
                    PathSegment::Field("b".into()),
                ],
                vec![
                    PathSegment::Field("matrix".into()),
                    PathSegment::Index(1),
                    PathSegment::Index(2),
                ],
            ]))
        );
    }

    #[test]
    fn decodes_list_index_projection_with_alias() {
        // `#p[0]` — the alias resolves the name part; the index suffix rides
        // straight through.
        let body = br##"{"TableName":"t","Key":{"id":{"S":"k"}},
            "ProjectionExpression":"#p[0]",
            "ExpressionAttributeNames":{"#p":"list"}}"##;
        let Operation::GetItem { projection, .. } =
            decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert_eq!(
            projection,
            Some(Projection(vec![vec![
                PathSegment::Field("list".into()),
                PathSegment::Index(0),
            ]]))
        );
    }

    #[test]
    fn rejects_malformed_list_index_syntax() {
        for expr in ["a[", "a[x]", "a[-1]", "a[0", "a]0[", "[0]"] {
            let body = format!(
                r#"{{"TableName":"t","Key":{{"id":{{"S":"k"}}}},
                    "ProjectionExpression":"{expr}"}}"#
            );
            let result = decode_request("DynamoDB_20120810.GetItem", body.as_bytes());
            let err = result.unwrap_err();
            assert_eq!(err.code, "ValidationException", "for `{expr}`");
        }
    }

    #[test]
    fn document_path_projection_reconstructs_nested() {
        let mut inner = BTreeMap::new();
        inner.insert("b".into(), s("keep"));
        inner.insert("z".into(), s("drop"));
        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::M(inner));
        item.insert("c".into(), s("top"));
        item.insert("d".into(), s("gone"));
        let projected = Projection(vec![
            vec![
                PathSegment::Field("a".into()),
                PathSegment::Field("b".into()),
            ],
            field("c"),
        ])
        .apply(&item);
        // `a` is reconstructed with only `b`; `c` kept; `d` dropped.
        let AttributeValue::M(a) = projected.get("a").expect("a present") else {
            panic!("a is a map");
        };
        assert_eq!(a.get("b"), Some(&s("keep")));
        assert!(!a.contains_key("z"));
        assert_eq!(projected.get("c"), Some(&s("top")));
        assert!(!projected.contains_key("d"));
    }

    #[test]
    fn list_index_projection_selects_and_compacts() {
        // Projecting a[1] and a[3] out of a longer list yields a *compacted*
        // two-element list, per DynamoDB's own documented contract.
        let mut item = Item::new();
        item.insert(
            "a".into(),
            AttributeValue::L(vec![s("z0"), s("z1"), s("z2"), s("z3"), s("z4")]),
        );
        let projected = Projection(vec![
            vec![PathSegment::Field("a".into()), PathSegment::Index(3)],
            vec![PathSegment::Field("a".into()), PathSegment::Index(1)],
        ])
        .apply(&item);
        assert_eq!(
            projected.get("a"),
            Some(&AttributeValue::L(vec![s("z1"), s("z3")]))
        );
    }

    #[test]
    fn list_index_projection_out_of_range_yields_nothing() {
        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::L(vec![s("only")]));
        let projected = Projection(vec![vec![
            PathSegment::Field("a".into()),
            PathSegment::Index(5),
        ]])
        .apply(&item);
        assert!(!projected.contains_key("a"));
    }

    #[test]
    fn nested_list_index_projection_descends_into_a_map_element() {
        // list[0].b — the first list element is a map; only `b` survives.
        let mut inner = BTreeMap::new();
        inner.insert("b".into(), s("keep"));
        inner.insert("z".into(), s("drop"));
        let mut item = Item::new();
        item.insert(
            "list".into(),
            AttributeValue::L(vec![AttributeValue::M(inner), s("second")]),
        );
        let projected = Projection(vec![vec![
            PathSegment::Field("list".into()),
            PathSegment::Index(0),
            PathSegment::Field("b".into()),
        ]])
        .apply(&item);
        let AttributeValue::L(list) = projected.get("list").expect("list present") else {
            panic!("list is a list");
        };
        assert_eq!(list.len(), 1, "only index 0 selected");
        let AttributeValue::M(m) = &list[0] else {
            panic!("element is a map");
        };
        assert_eq!(m.get("b"), Some(&s("keep")));
        assert!(!m.contains_key("z"));
    }

    #[test]
    fn nested_index_of_a_list_selects_and_compacts() {
        // matrix[0][2] and matrix[0][0] out of a nested list — a compacted
        // two-element inner list at index 0; index 1 of the outer list is
        // never touched.
        let mut item = Item::new();
        item.insert(
            "matrix".into(),
            AttributeValue::L(vec![
                AttributeValue::L(vec![s("m00"), s("m01"), s("m02")]),
                AttributeValue::L(vec![s("m10"), s("m11")]),
            ]),
        );
        let projected = Projection(vec![
            vec![
                PathSegment::Field("matrix".into()),
                PathSegment::Index(0),
                PathSegment::Index(2),
            ],
            vec![
                PathSegment::Field("matrix".into()),
                PathSegment::Index(0),
                PathSegment::Index(0),
            ],
        ])
        .apply(&item);
        let AttributeValue::L(outer) = projected.get("matrix").expect("matrix present") else {
            panic!("matrix is a list");
        };
        assert_eq!(outer.len(), 1, "only outer index 0 selected");
        assert_eq!(outer[0], AttributeValue::L(vec![s("m00"), s("m02")]));
    }

    #[test]
    fn decodes_return_values_all_old() {
        let body = br#"{"TableName":"t","Item":{"id":{"S":"k"}},
            "ReturnValues":"ALL_OLD"}"#;
        let Operation::PutItem { return_values, .. } =
            decode_request("DynamoDB_20120810.PutItem", body).unwrap()
        else {
            panic!("expected PutItem");
        };
        assert_eq!(return_values, ReturnValues::AllOld);
    }

    #[test]
    fn write_response_echoes_old_item_for_all_old() {
        let mut old = Item::new();
        old.insert("id".into(), s("k"));
        let body = write_response(ReturnValues::AllOld, Some(&old), None, None);
        assert!(body.contains("\"Attributes\""));
        assert!(body.contains("\"S\":\"k\""));
        // ALL_OLD on an absent key is `{}`; NONE is always `{}`.
        assert_eq!(write_response(ReturnValues::AllOld, None, None, None), "{}");
        assert_eq!(
            write_response(ReturnValues::None, Some(&old), None, None),
            "{}"
        );
    }

    #[test]
    fn base64_round_trips_all_lengths() {
        for len in 0..20usize {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 37 % 256) as u8).collect();
            let encoded = base64_encode(&bytes);
            assert_eq!(base64_decode(&encoded), Some(bytes), "len {len}");
        }
    }

    #[test]
    fn base64url_round_trips_all_lengths_unpadded() {
        for len in 0..20usize {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 37 % 256) as u8).collect();
            let encoded = base64url_encode(&bytes);
            assert!(
                !encoded.contains('=') && !encoded.contains('+') && !encoded.contains('/'),
                "unpadded URL-safe alphabet only: {encoded}"
            );
            assert_eq!(base64url_decode(&encoded), Some(bytes), "len {len}");
        }
        // The URL-safe substitutions: 0xfb 0xff 0xfe is "+//+" in the standard
        // alphabet.
        assert_eq!(base64url_encode(&[0xfb, 0xff, 0xfe]), "-__-");
    }

    #[test]
    fn base64url_decode_is_strict() {
        // Padding is not accepted (the encoding is unpadded).
        assert_eq!(base64url_decode("ij8cAHfStuE="), None);
        // Standard-alphabet characters are outside the URL-safe alphabet.
        assert_eq!(base64url_decode("+A"), None);
        assert_eq!(base64url_decode("/A"), None);
        // len % 4 == 1 is impossible for any byte string.
        assert_eq!(base64url_decode("AAAAA"), None);
        // Non-canonical trailing bits: "AB" has nonzero unused low bits ("AA"
        // is the canonical encoding of the single byte 0x00).
        assert_eq!(base64url_decode("AB"), None);
        assert_eq!(base64url_decode("AA"), Some(vec![0x00]));
        assert_eq!(base64url_decode("AAB"), None);
        assert_eq!(base64url_decode("AAA"), Some(vec![0x00, 0x00]));
    }

    #[test]
    fn get_item_response_omits_missing_item() {
        assert_eq!(get_item_response(None, None), "{}");
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        let body = get_item_response(Some(&item), None);
        assert!(body.contains("\"Item\""));
        assert!(body.contains("\"S\":\"u1\""));
    }

    #[test]
    fn stored_item_tombstone_reads_as_absent() {
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        let bytes = encode_stored_item(&item);
        assert_eq!(decode_stored_item(&bytes).unwrap(), Some(item));
        let tomb = encode_tombstone();
        assert_eq!(decode_stored_item(&tomb).unwrap(), None);
    }

    #[test]
    fn decodes_create_table_with_composite_key() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                    {"AttributeName":"sk","AttributeType":"S"}]}"#;
        match decode_request("DynamoDB_20120810.CreateTable", body).unwrap() {
            Operation::CreateTable {
                table,
                schema,
                key_types,
                indexes,
                ..
            } => {
                assert_eq!(table, "t");
                assert_eq!(schema, TableSchema::composite("pk", "sk"));
                assert_eq!(
                    key_types,
                    vec![
                        ("pk".to_owned(), "S".to_owned()),
                        ("sk".to_owned(), "S".to_owned())
                    ]
                );
                assert!(indexes.is_empty());
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn create_table_simple_key() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#;
        match decode_request("DynamoDB_20120810.CreateTable", body).unwrap() {
            Operation::CreateTable { schema, .. } => {
                assert_eq!(schema, TableSchema::simple("id"));
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn decodes_put_with_attribute_not_exists_condition() {
        let body = br#"{"TableName":"t","Item":{"pk":{"S":"a"}},
            "ConditionExpression":"attribute_not_exists(pk)"}"#;
        let Operation::PutItem { condition, .. } =
            decode_request("DynamoDB_20120810.PutItem", body).unwrap()
        else {
            panic!("expected PutItem");
        };
        assert_eq!(
            condition,
            Some(ConditionExpression::AttributeNotExists("pk".into()))
        );
    }

    #[test]
    fn decodes_put_with_equality_condition() {
        let body = br#"{"TableName":"t","Item":{"pk":{"S":"a"}},
            "ConditionExpression":"v = :want",
            "ExpressionAttributeValues":{":want":{"N":"7"}}}"#;
        let Operation::PutItem { condition, .. } =
            decode_request("DynamoDB_20120810.PutItem", body).unwrap()
        else {
            panic!("expected PutItem");
        };
        assert_eq!(
            condition,
            Some(ConditionExpression::Compare(
                "v".into(),
                Comparator::Eq,
                AttributeValue::N("7".into())
            ))
        );
    }

    #[test]
    fn decodes_query_partition_only() {
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"part"}}}"#;
        match decode_request("DynamoDB_20120810.Query", body).unwrap() {
            Operation::Query {
                table,
                index,
                partition_attr,
                partition_value,
                sort_attr,
                sort_condition,
                limit,
                exclusive_start_key,
                scan_index_forward,
                filter,
                projection,
                select,
                consistent_read,
            } => {
                assert!(filter.is_none(), "no FilterExpression in the body");
                assert!(scan_index_forward, "ScanIndexForward defaults to true");
                assert_eq!(
                    select,
                    Select::AllAttributes,
                    "a base-table read with no projection defaults to ALL_ATTRIBUTES"
                );
                assert_eq!(table, "t");
                assert_eq!(index, None);
                assert_eq!(partition_attr, "pk", "the key name is carried, not dropped");
                assert_eq!(partition_value, s("part"));
                assert_eq!(sort_attr, None);
                assert_eq!(sort_condition, None);
                assert_eq!(limit, None, "no Limit in the body");
                assert_eq!(
                    exclusive_start_key, None,
                    "no ExclusiveStartKey in the body"
                );
                assert_eq!(projection, None);
                assert!(!consistent_read, "default is false");
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    /// `Query` decodes `Limit`/`ExclusiveStartKey` exactly like `Scan`
    /// (`decodes_scan_with_limit_and_filter`'s pair) — the pagination gap
    /// this crate used to have (`decode_query` never parsed either field).
    #[test]
    fn decodes_query_with_limit_and_exclusive_start_key() {
        let body = br#"{"TableName":"t","Limit":3,
            "ExclusiveStartKey":{"pk":{"S":"part"},"sk":{"S":"k5"}},
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"part"}}}"#;
        match decode_request("DynamoDB_20120810.Query", body).unwrap() {
            Operation::Query {
                limit,
                exclusive_start_key,
                ..
            } => {
                assert_eq!(limit, Some(3));
                let esk = exclusive_start_key.expect("ExclusiveStartKey present");
                assert_eq!(esk.get("pk"), Some(&s("part")));
                assert_eq!(esk.get("sk"), Some(&s("k5")));
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    /// Omitting `Limit`/`ExclusiveStartKey` on a `Query` decodes both as
    /// `None` — the same default `decodes_query_partition_only` already
    /// covers via its explicit-destructure assertions; this test names the
    /// property directly for the pagination fields.
    #[test]
    fn decodes_query_without_limit_or_exclusive_start_key() {
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"part"}}}"#;
        match decode_request("DynamoDB_20120810.Query", body).unwrap() {
            Operation::Query {
                limit,
                exclusive_start_key,
                ..
            } => {
                assert_eq!(limit, None);
                assert_eq!(exclusive_start_key, None);
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    /// A non-integer `Limit` on `Query` is rejected exactly like `Scan`'s own
    /// `Limit` validation (`decode_limit` is now shared between the two).
    #[test]
    fn rejects_non_integer_query_limit() {
        let body = br#"{"TableName":"t","Limit":"two",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"part"}}}"#;
        let err = decode_request("DynamoDB_20120810.Query", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    /// `ConsistentRead` decodes on `GetItem`/`Query`/`Scan` alike (ADR 0041
    /// §5). What the flag then *means* is entirely the `animusd` edge's
    /// business — the GSI rejection, and since ADR 0055 the read-path choice
    /// — and is e2e-tested there, since this crate never sees the replicated
    /// catalog needed to know an index's kind.
    #[test]
    fn decodes_consistent_read_true_on_get_item_query_and_scan() {
        let get = br#"{"TableName":"t","Key":{"pk":{"S":"a"}},"ConsistentRead":true}"#;
        let Operation::GetItem {
            consistent_read, ..
        } = decode_request("DynamoDB_20120810.GetItem", get).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert!(consistent_read);

        let query = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"part"}},
            "ConsistentRead":true}"#;
        let Operation::Query {
            consistent_read, ..
        } = decode_request("DynamoDB_20120810.Query", query).unwrap()
        else {
            panic!("expected Query");
        };
        assert!(consistent_read);

        let scan = br#"{"TableName":"t","ConsistentRead":true}"#;
        let Operation::Scan {
            consistent_read, ..
        } = decode_request("DynamoDB_20120810.Scan", scan).unwrap()
        else {
            panic!("expected Scan");
        };
        assert!(consistent_read);
    }

    /// Omitting `ConsistentRead` entirely defaults to `false` — already
    /// covered for `Query`/`Scan` above (`decodes_query_partition_only`/
    /// `decodes_scan_with_limit_and_filter`); this covers `GetItem`, whose
    /// happy-path decode test elsewhere uses `..`.
    #[test]
    fn get_item_consistent_read_defaults_to_false() {
        let body = br#"{"TableName":"t","Key":{"pk":{"S":"a"}}}"#;
        let Operation::GetItem {
            consistent_read, ..
        } = decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert!(!consistent_read);
    }

    /// A `Query`'s `FilterExpression` decodes through the same predicate
    /// decoder `Scan` uses. Before this it was never read at all, so a filter
    /// rode through as `None` and the edge returned unfiltered results.
    #[test]
    fn decodes_query_filter_expression() {
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"kind = :k",
            "ExpressionAttributeValues":{":p":{"S":"x"},":k":{"S":"blue"}}}"#;
        let Operation::Query { filter, .. } =
            decode_request("DynamoDB_20120810.Query", body).unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(
            filter,
            Some(ConditionExpression::Compare(
                "kind".into(),
                Comparator::Eq,
                s("blue")
            ))
        );

        // The function forms decode too.
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"attribute_not_exists(gone)",
            "ExpressionAttributeValues":{":p":{"S":"x"}}}"#;
        let Operation::Query { filter, .. } =
            decode_request("DynamoDB_20120810.Query", body).unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(
            filter,
            Some(ConditionExpression::AttributeNotExists("gone".into()))
        );

        // Absent stays `None` rather than defaulting to something permissive.
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"x"}}}"#;
        let Operation::Query { filter, .. } =
            decode_request("DynamoDB_20120810.Query", body).unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(filter, None);
    }

    #[test]
    fn decodes_query_sort_conditions() {
        // equality
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p AND sk = :s",
            "ExpressionAttributeValues":{":p":{"S":"x"},":s":{"S":"y"}}}"#;
        let Operation::Query { sort_condition, .. } =
            decode_request("DynamoDB_20120810.Query", body).unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(
            sort_condition,
            Some(SortKeyCondition::Compare(Comparator::Eq, s("y")))
        );

        // between (mixed case AND/BETWEEN)
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p and sk between :lo and :hi",
            "ExpressionAttributeValues":{":p":{"S":"x"},":lo":{"S":"a"},":hi":{"S":"m"}}}"#;
        let Operation::Query { sort_condition, .. } =
            decode_request("DynamoDB_20120810.Query", body).unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(
            sort_condition,
            Some(SortKeyCondition::Between(s("a"), s("m")))
        );

        // begins_with
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p AND begins_with(sk, :pre)",
            "ExpressionAttributeValues":{":p":{"S":"x"},":pre":{"S":"ab"}}}"#;
        let Operation::Query { sort_condition, .. } =
            decode_request("DynamoDB_20120810.Query", body).unwrap()
        else {
            panic!("expected Query");
        };
        assert_eq!(sort_condition, Some(SortKeyCondition::BeginsWith(s("ab"))));
    }

    /// Issue #373: `KeyConditionExpression` used to reject `<`/`<=`/`>`/`>=`
    /// sort-key operators outright. Each now decodes onto the same
    /// [`SortKeyCondition::Compare`] shape `=` already used, sharing
    /// [`comparator_of`] with `FilterExpression`/`ConditionExpression`
    /// decoding rather than a parallel mapping.
    #[test]
    fn decodes_query_sort_range_operators() {
        let decode = |op: &str| {
            let body = format!(
                r#"{{"TableName":"t",
                    "KeyConditionExpression":"pk = :p AND sk {op} :s",
                    "ExpressionAttributeValues":{{":p":{{"S":"x"}},":s":{{"N":"5"}}}}}}"#
            );
            let Operation::Query { sort_condition, .. } =
                decode_request("DynamoDB_20120810.Query", body.as_bytes()).unwrap()
            else {
                panic!("expected Query");
            };
            sort_condition
        };
        assert_eq!(
            decode("<"),
            Some(SortKeyCondition::Compare(Comparator::Lt, n("5")))
        );
        assert_eq!(
            decode("<="),
            Some(SortKeyCondition::Compare(Comparator::Le, n("5")))
        );
        assert_eq!(
            decode(">"),
            Some(SortKeyCondition::Compare(Comparator::Gt, n("5")))
        );
        assert_eq!(
            decode(">="),
            Some(SortKeyCondition::Compare(Comparator::Ge, n("5")))
        );
    }

    /// `<>` is a real [`Comparator`] variant but not part of AWS's
    /// `KeyConditionExpression` grammar (there is no not-equal *range*), so it
    /// stays explicitly rejected rather than silently accepted once the other
    /// four comparators opened up.
    #[test]
    fn key_condition_rejects_not_equal() {
        let body = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p AND sk <> :s",
            "ExpressionAttributeValues":{":p":{"S":"x"},":s":{"N":"5"}}}"#;
        let err = decode_request("DynamoDB_20120810.Query", body).unwrap_err();
        assert!(
            err.message.contains("<>"),
            "error should name the rejected operator: {}",
            err.message
        );
    }

    #[test]
    fn create_table_response_shape() {
        let body = create_table_response("t", &TableSchema::composite("pk", "sk"), &[], None);
        assert!(body.contains("\"TableStatus\":\"ACTIVE\""));
        assert!(body.contains("\"HASH\""));
        assert!(body.contains("\"RANGE\""));
        assert!(!body.contains("GlobalSecondaryIndexes"));
        assert!(!body.contains("StreamSpecification"));
    }

    #[test]
    fn create_table_response_includes_stream_spec_when_enabled() {
        let stream = StreamDescription {
            view_type: StreamViewType::NewAndOldImages,
            label: "2026-08-14T00:00:00.000-n1".into(),
        };
        let body = create_table_response("t", &TableSchema::simple("id"), &[], Some(&stream));
        assert!(body.contains("\"StreamEnabled\":true"));
        assert!(body.contains("\"StreamViewType\":\"NEW_AND_OLD_IMAGES\""));
        assert!(body.contains(
            "\"LatestStreamArn\":\"arn:aws:dynamodb:animus:0:table/t/stream/2026-08-14T00:00:00.000-n1\""
        ));
        assert!(body.contains("\"LatestStreamLabel\""));
    }

    #[test]
    fn describe_table_response_shape() {
        let stream = StreamDescription {
            view_type: StreamViewType::KeysOnly,
            label: "lbl".into(),
        };
        let body = describe_table_response(
            "t",
            &TableSchema::composite("pk", "sk"),
            &[("pk".into(), "S".into()), ("sk".into(), "N".into())],
            &[],
            &[],
            Some(&stream),
            "ACTIVE",
        );
        assert!(body.contains("\"Table\""));
        assert!(body.contains("\"AttributeDefinitions\""));
        assert!(body.contains("\"AttributeType\":\"N\""));
        assert!(body.contains("\"StreamViewType\":\"KEYS_ONLY\""));
    }

    /// `DescribeTable`'s `AttributeDefinitions` must cover a GSI's own key
    /// attribute(s), an LSI's own alternate sort attribute, and the base
    /// table's own keys — the AWS-faithful shape (issue #319's DescribeTable
    /// fidelity gap): before this fix `attribute_definitions` only ever
    /// looked at `schema.partition_key`/`schema.sort_key`, so a composite
    /// GSI's `score` (hash) and `rank` (sort) — neither of them the base
    /// table's own `id` — never appeared in the response at all.
    ///
    /// **Flipped (issue #319, closed)**: `key_types` here is no longer just
    /// the base table's own typed columns — it's what `animusd::dynamo::
    /// describe_table` actually builds now, extending it with
    /// `schema_bridge::index_attribute_types` off each index's own
    /// (control-catalog-recorded) `hash_attribute_type`/
    /// `sort_attribute_type`. So `score`/`rank`/`alt_sort` each render their
    /// **real** declared type, not the old blanket `"S"` placeholder — this
    /// function's own logic (a plain name → type lookup) never changed; only
    /// what `animusd` now passes into it did. `alt_sort` deliberately has no
    /// entry in `key_types` at all, proving the untyped fallback still works
    /// correctly for an attribute nobody declared a type for.
    #[test]
    fn describe_table_response_attribute_definitions_cover_index_key_attributes() {
        let gsi = SecondaryIndex::Global(GlobalSecondaryIndex {
            name: "by-score".into(),
            key_attribute: "score".into(),
            sort_attribute: Some("rank".into()),
            projection: IndexProjection::All,
        });
        let lsi = SecondaryIndex::Local(LocalSecondaryIndex {
            name: "by-alt-sort".into(),
            // Shares the base partition key's own name coincidentally to
            // prove dedup: `id` must appear exactly once in the output.
            sort_attribute: "alt_sort".into(),
            projection: IndexProjection::All,
        });
        // The base table's own key type, plus `score`/`rank`'s own declared
        // types — the shape `animusd::dynamo::describe_table` merges via
        // `schema_bridge::index_attribute_types`. `alt_sort` is deliberately
        // absent (untyped).
        let key_types = [
            ("id".to_owned(), "S".to_owned()),
            ("score".to_owned(), "N".to_owned()),
            ("rank".to_owned(), "B".to_owned()),
        ];
        let body = describe_table_response(
            "t",
            &TableSchema::simple("id"),
            &key_types,
            &[gsi, lsi],
            &[],
            None,
            "ACTIVE",
        );
        // Scope the assertions to the `AttributeDefinitions` array itself —
        // `AttributeName` also appears inside `KeySchema`/
        // `GlobalSecondaryIndexes`/`LocalSecondaryIndexes`, which would
        // otherwise inflate the count below.
        let attr_defs_start = body.find("\"AttributeDefinitions\":[").expect("present");
        let attr_defs_end = attr_defs_start + body[attr_defs_start..].find(']').expect("closes");
        let attr_defs = &body[attr_defs_start..=attr_defs_end];
        for (name, ty) in [
            ("id", "S"),
            ("score", "N"),
            ("rank", "B"),
            // `alt_sort` has no declared type in `key_types` above, so it
            // must still fall back to the honest "unknown" placeholder.
            ("alt_sort", "S"),
        ] {
            assert!(
                attr_defs.contains(&format!(
                    "{{\"AttributeName\":\"{name}\",\"AttributeType\":\"{ty}\"}}"
                )),
                "missing/wrong AttributeDefinitions entry for `{name}` (want type `{ty}`): \
                 {attr_defs}"
            );
        }
        assert_eq!(
            attr_defs.matches("\"AttributeName\":\"id\"").count(),
            1,
            "base partition key must appear exactly once even though nothing \
             else names it: {attr_defs}"
        );
    }

    /// `DescribeTable`'s per-index `IndexStatus` (ADR 0045 §6 Fork D): each of
    /// the three real statuses renders in DynamoDB's own
    /// `SCREAMING_SNAKE_CASE`, and `Backfilling: true` appears **only**
    /// alongside `CREATING` — never `false`, and never for `ACTIVE`/
    /// `DELETING` (matching AWS, which omits the attribute once backfilling
    /// finishes).
    #[test]
    fn describe_table_response_reports_real_index_status_and_backfilling() {
        let gsi = SecondaryIndex::Global(GlobalSecondaryIndex {
            name: "by-email".into(),
            key_attribute: "email".into(),
            sort_attribute: None,
            projection: IndexProjection::All,
        });

        for (status, want_status_str, want_backfilling) in [
            (IndexStatus::Creating, "CREATING", true),
            (IndexStatus::Active, "ACTIVE", false),
            (IndexStatus::Deleting, "DELETING", false),
        ] {
            let body = describe_table_response(
                "t",
                &TableSchema::simple("id"),
                &[],
                std::slice::from_ref(&gsi),
                &[("by-email".into(), status)],
                None,
                "ACTIVE",
            );
            assert!(
                body.contains(&format!("\"IndexStatus\":\"{want_status_str}\"")),
                "status {status:?}: expected IndexStatus {want_status_str} in {body}"
            );
            assert_eq!(
                body.contains("\"Backfilling\":true"),
                want_backfilling,
                "status {status:?}: unexpected Backfilling presence in {body}"
            );
            assert!(
                !body.contains("\"Backfilling\":false"),
                "Backfilling must never render as false (AWS omits the attribute instead): {body}"
            );
        }
    }

    /// An index absent from the `index_statuses` side channel — the shape
    /// [`create_table_response`] always passes — defaults to `ACTIVE` with no
    /// `Backfilling`, matching a just-created, empty-by-construction table's
    /// indexes (ADR 0041 §5).
    #[test]
    fn create_table_response_reports_indexes_as_active() {
        let gsi = SecondaryIndex::Global(GlobalSecondaryIndex {
            name: "by-email".into(),
            key_attribute: "email".into(),
            sort_attribute: None,
            projection: IndexProjection::All,
        });
        let body = create_table_response("t", &TableSchema::simple("id"), &[gsi], None);
        assert!(body.contains("\"IndexStatus\":\"ACTIVE\""));
        assert!(!body.contains("Backfilling"));
    }

    #[test]
    fn delete_table_response_shape() {
        let stream = StreamDescription {
            view_type: StreamViewType::KeysOnly,
            label: "lbl".into(),
        };
        let body = delete_table_response(
            "t",
            &TableSchema::composite("pk", "sk"),
            &[("pk".into(), "S".into()), ("sk".into(), "N".into())],
            &[],
            &[],
            Some(&stream),
        );
        // Wrapped under `TableDescription` (matching `CreateTable`/
        // `UpdateTable`), not `DescribeTable`'s `Table`.
        assert!(body.contains("\"TableDescription\""));
        assert!(!body.starts_with("{\"Table\""));
        assert!(body.contains("\"TableStatus\":\"DELETING\""));
        assert!(!body.contains("\"TableStatus\":\"ACTIVE\""));
        // Same descriptive fields `DescribeTable` emits.
        assert!(body.contains("\"AttributeDefinitions\""));
        assert!(body.contains("\"AttributeType\":\"N\""));
        assert!(body.contains("\"StreamViewType\":\"KEYS_ONLY\""));
        assert!(body.contains("\"TableName\":\"t\""));
    }

    #[test]
    fn paginate_table_names_defaults_limit_to_100_and_caps_it() {
        let names: Vec<String> = (0..150).map(|i| format!("t{i:03}")).collect();

        // No `Limit` at all: default is 100, and since 150 > 100 the page is
        // truncated.
        let (page, last) = paginate_table_names(&names, None, None);
        assert_eq!(page.len(), 100);
        assert_eq!(page.first().unwrap(), "t000");
        assert_eq!(page.last().unwrap(), "t099");
        assert_eq!(last.as_deref(), Some("t099"));

        // A `Limit` above 100 is capped at 100, not honored as-is.
        let (page, last) = paginate_table_names(&names, None, Some(1000));
        assert_eq!(page.len(), 100);
        assert_eq!(last.as_deref(), Some("t099"));
    }

    #[test]
    fn paginate_table_names_exclusive_start_table_name_positions_strictly_after() {
        let names: Vec<String> = ["a", "b", "c", "d"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let (page, last) = paginate_table_names(&names, Some("b"), None);
        // Strictly after "b": "b" itself is excluded.
        assert_eq!(page, vec!["c".to_owned(), "d".to_owned()]);
        assert_eq!(last, None);

        // A start name equal to the last name yields an empty page.
        let (page, last) = paginate_table_names(&names, Some("d"), None);
        assert!(page.is_empty());
        assert_eq!(last, None);

        // A start name absent from the list still positions correctly
        // (between "b" and "c").
        let (page, _) = paginate_table_names(&names, Some("bb"), None);
        assert_eq!(page, vec!["c".to_owned(), "d".to_owned()]);
    }

    #[test]
    fn paginate_table_names_reports_last_evaluated_only_when_truncated() {
        let names: Vec<String> = ["a", "b", "c"].iter().map(|s| (*s).to_owned()).collect();

        // The whole (small) list fits within the limit: no truncation, no
        // `LastEvaluatedTableName`.
        let (page, last) = paginate_table_names(&names, None, Some(10));
        assert_eq!(page.len(), 3);
        assert_eq!(last, None);

        // A limit smaller than the candidate set truncates the page and
        // reports the page's own last name as the cursor.
        let (page, last) = paginate_table_names(&names, None, Some(2));
        assert_eq!(page, vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(last.as_deref(), Some("b"));

        // A limit exactly matching the remaining count is NOT truncated.
        let (page, last) = paginate_table_names(&names, None, Some(3));
        assert_eq!(page.len(), 3);
        assert_eq!(last, None);
    }

    #[test]
    fn list_tables_response_shape() {
        let names = vec!["a".to_owned(), "b".to_owned()];
        let body = list_tables_response(&names, None);
        assert!(body.contains("\"TableNames\":[\"a\",\"b\"]"));
        assert!(!body.contains("LastEvaluatedTableName"));

        let body = list_tables_response(&names, Some("b"));
        assert!(body.contains("\"LastEvaluatedTableName\":\"b\""));
    }

    #[test]
    fn decodes_update_table_stream_enable_and_disable() {
        let enable = br#"{"TableName":"t","StreamSpecification":
            {"StreamEnabled":true,"StreamViewType":"NEW_IMAGE"}}"#;
        match decode_request("DynamoDB_20120810.UpdateTable", enable).unwrap() {
            Operation::UpdateTable {
                table,
                stream,
                index_update,
                ..
            } => {
                assert_eq!(table, "t");
                assert_eq!(stream, Some(StreamUpdate::Enable(StreamViewType::NewImage)));
                assert_eq!(index_update, None);
            }
            other => panic!("expected UpdateTable, got {other:?}"),
        }

        let disable = br#"{"TableName":"t","StreamSpecification":{"StreamEnabled":false}}"#;
        match decode_request("DynamoDB_20120810.UpdateTable", disable).unwrap() {
            Operation::UpdateTable { stream, .. } => {
                assert_eq!(stream, Some(StreamUpdate::Disable));
            }
            other => panic!("expected UpdateTable, got {other:?}"),
        }
    }

    #[test]
    fn update_table_rejects_an_empty_index_updates_array() {
        let body = br#"{"TableName":"t","GlobalSecondaryIndexUpdates":[]}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_table_rejects_more_than_one_index_updates_element() {
        let body = br#"{"TableName":"t","GlobalSecondaryIndexUpdates":[
            {"Delete":{"IndexName":"a"}},{"Delete":{"IndexName":"b"}}]}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_table_decodes_a_delete_index_update() {
        let body = br#"{"TableName":"t","GlobalSecondaryIndexUpdates":[
            {"Delete":{"IndexName":"by-email"}}]}"#;
        match decode_request("DynamoDB_20120810.UpdateTable", body).unwrap() {
            Operation::UpdateTable {
                table,
                stream,
                index_update,
                key_types,
            } => {
                assert_eq!(table, "t");
                assert_eq!(stream, None);
                assert_eq!(
                    index_update,
                    Some(IndexUpdate::Delete("by-email".to_owned()))
                );
                assert!(key_types.is_empty(), "a Delete needs no declared type");
            }
            other => panic!("expected UpdateTable, got {other:?}"),
        }
    }

    #[test]
    fn update_table_decodes_a_create_index_update() {
        let body = br#"{"TableName":"t","GlobalSecondaryIndexUpdates":[
            {"Create":{"IndexName":"by-email",
                "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                "Projection":{"ProjectionType":"ALL"}}}]}"#;
        match decode_request("DynamoDB_20120810.UpdateTable", body).unwrap() {
            Operation::UpdateTable {
                table,
                index_update,
                key_types,
                ..
            } => {
                assert_eq!(table, "t");
                match index_update {
                    Some(IndexUpdate::Create(SecondaryIndex::Global(gsi))) => {
                        assert_eq!(gsi.name, "by-email");
                        assert_eq!(gsi.key_attribute, "email");
                    }
                    other => panic!("expected Create(Global(..)), got {other:?}"),
                }
                assert!(
                    key_types.is_empty(),
                    "no AttributeDefinitions in this request"
                );
            }
            other => panic!("expected UpdateTable, got {other:?}"),
        }
    }

    /// Issue #319: an `UpdateTable` GSI-`Create` call's own
    /// `AttributeDefinitions` is decoded into `key_types` (the same shape
    /// `CreateTable` already carries), so `animusd` can record the new
    /// index's own declared key attribute type instead of always defaulting
    /// to `S`.
    #[test]
    fn update_table_create_index_decodes_attribute_definitions() {
        let body = br#"{"TableName":"t",
            "AttributeDefinitions":[{"AttributeName":"score","AttributeType":"N"}],
            "GlobalSecondaryIndexUpdates":[
            {"Create":{"IndexName":"by-score",
                "KeySchema":[{"AttributeName":"score","KeyType":"HASH"}],
                "Projection":{"ProjectionType":"ALL"}}}]}"#;
        match decode_request("DynamoDB_20120810.UpdateTable", body).unwrap() {
            Operation::UpdateTable { key_types, .. } => {
                assert_eq!(key_types, vec![("score".to_owned(), "N".to_owned())]);
            }
            other => panic!("expected UpdateTable, got {other:?}"),
        }
    }

    #[test]
    fn update_table_rejects_an_update_shaped_index_element() {
        let body = br#"{"TableName":"t","GlobalSecondaryIndexUpdates":[
            {"Update":{"IndexName":"by-email"}}]}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
        // Pin the exact wording (issue W-04): "no `Update`" plus the reason
        // (no throughput model), not just the error code.
        assert_eq!(
            err.message,
            "each GlobalSecondaryIndexUpdates element must be exactly one of `Create` or \
             `Delete` (no `Update` — no throughput model)"
        );
    }

    #[test]
    fn update_table_rejects_index_and_stream_change_together() {
        let body = br#"{"TableName":"t",
            "GlobalSecondaryIndexUpdates":[{"Delete":{"IndexName":"by-email"}}],
            "StreamSpecification":{"StreamEnabled":false}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_table_requires_stream_specification_or_index_updates() {
        let body = br#"{"TableName":"t"}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_table_rejects_sse_specification() {
        let body = br#"{"TableName":"t","SSESpecification":{"Enabled":true}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
        assert_eq!(
            err.message,
            "UpdateTable: SSESpecification is not supported"
        );
    }

    #[test]
    fn update_table_rejects_replica_updates() {
        let body = br#"{"TableName":"t","ReplicaUpdates":[{"Create":{"RegionName":"us-west-2"}}]}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
        assert_eq!(err.message, "UpdateTable: ReplicaUpdates is not supported");
    }

    #[test]
    fn update_table_rejects_provisioned_throughput() {
        let body = br#"{"TableName":"t",
            "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
        assert_eq!(
            err.message,
            "UpdateTable: ProvisionedThroughput is not supported"
        );
    }

    #[test]
    fn update_table_rejects_an_unsupported_billing_mode() {
        let body = br#"{"TableName":"t","BillingMode":"PROVISIONED",
            "StreamSpecification":{"StreamEnabled":false}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
        assert!(err.message.contains("BillingMode"), "{}", err.message);
    }

    #[test]
    fn update_table_tolerates_pay_per_request_billing_mode_alongside_a_real_change() {
        // BillingMode: PAY_PER_REQUEST is this adapter's only billing mode
        // (see CreateTable, which accepts and never inspects it); restating
        // it on UpdateTable is a common SDK/CLI habit and must not block an
        // otherwise-valid stream/index change.
        let body = br#"{"TableName":"t","BillingMode":"PAY_PER_REQUEST",
            "StreamSpecification":{"StreamEnabled":false}}"#;
        match decode_request("DynamoDB_20120810.UpdateTable", body).unwrap() {
            Operation::UpdateTable { stream, .. } => {
                assert_eq!(stream, Some(StreamUpdate::Disable));
            }
            other => panic!("expected UpdateTable, got {other:?}"),
        }
    }

    #[test]
    fn update_table_rejects_a_billing_mode_only_body_with_no_modeled_change() {
        // Tolerating the key isn't the same as modeling a billing-mode
        // *change*: with no GSI/stream change alongside it, this still
        // falls through to the generic "requires either..." rejection.
        let body = br#"{"TableName":"t","BillingMode":"PAY_PER_REQUEST"}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_create_table_with_stream_enabled() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"OLD_IMAGE"}}"#;
        match decode_request("DynamoDB_20120810.CreateTable", body).unwrap() {
            Operation::CreateTable {
                stream_view_type, ..
            } => {
                assert_eq!(stream_view_type, Some(StreamViewType::OldImage));
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn decodes_create_table_with_stream_disabled_or_absent() {
        let disabled = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":false}}"#;
        match decode_request("DynamoDB_20120810.CreateTable", disabled).unwrap() {
            Operation::CreateTable {
                stream_view_type, ..
            } => assert_eq!(stream_view_type, None),
            other => panic!("expected CreateTable, got {other:?}"),
        }

        let absent = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#;
        match decode_request("DynamoDB_20120810.CreateTable", absent).unwrap() {
            Operation::CreateTable {
                stream_view_type, ..
            } => assert_eq!(stream_view_type, None),
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn decodes_create_table_with_gsi() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-email",
                 "KeySchema":[{"AttributeName":"email","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#;
        match decode_request("DynamoDB_20120810.CreateTable", body).unwrap() {
            Operation::CreateTable { indexes, .. } => {
                assert_eq!(
                    indexes,
                    vec![SecondaryIndex::Global(GlobalSecondaryIndex {
                        name: "by-email".into(),
                        key_attribute: "email".into(),
                        sort_attribute: None,
                        projection: IndexProjection::All,
                    })]
                );
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn decodes_composite_gsi_and_lsi() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"g",
                 "KeySchema":[{"AttributeName":"a","KeyType":"HASH"},
                              {"AttributeName":"b","KeyType":"RANGE"}]}],
            "LocalSecondaryIndexes":[
                {"IndexName":"l",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#;
        match decode_request("DynamoDB_20120810.CreateTable", body).unwrap() {
            Operation::CreateTable { indexes, .. } => {
                assert_eq!(
                    indexes,
                    vec![
                        SecondaryIndex::Global(GlobalSecondaryIndex {
                            name: "g".into(),
                            key_attribute: "a".into(),
                            sort_attribute: Some("b".into()),
                            projection: IndexProjection::All,
                        }),
                        SecondaryIndex::Local(LocalSecondaryIndex {
                            name: "l".into(),
                            sort_attribute: "alt".into(),
                            projection: IndexProjection::All,
                        }),
                    ]
                );
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn rejects_lsi_with_wrong_partition_key() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "LocalSecondaryIndexes":[
                {"IndexName":"l",
                 "KeySchema":[{"AttributeName":"other","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#;
        let err = decode_request("DynamoDB_20120810.CreateTable", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_query_against_an_index() {
        let body = br#"{"TableName":"t","IndexName":"by-email",
            "KeyConditionExpression":"email = :e",
            "ExpressionAttributeValues":{":e":{"S":"a@x"}}}"#;
        match decode_request("DynamoDB_20120810.Query", body).unwrap() {
            Operation::Query {
                index,
                partition_value,
                sort_condition,
                ..
            } => {
                assert_eq!(index.as_deref(), Some("by-email"));
                assert_eq!(partition_value, s("a@x"));
                assert_eq!(sort_condition, None);
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn decodes_scan_with_limit_and_filter() {
        let body = br#"{"TableName":"t","Limit":2,
            "ExclusiveStartKey":{"id":{"S":"k5"}},
            "FilterExpression":"attribute_exists(v)"}"#;
        match decode_request("DynamoDB_20120810.Scan", body).unwrap() {
            Operation::Scan {
                table,
                index,
                limit,
                exclusive_start_key,
                filter,
                projection,
                select,
                segment,
                consistent_read,
            } => {
                assert_eq!(table, "t");
                assert_eq!(index, None);
                assert_eq!(limit, Some(2));
                assert_eq!(select, Select::AllAttributes);
                assert_eq!(segment, None, "no Segment/TotalSegments in the body");
                assert_eq!(exclusive_start_key.unwrap().get("id"), Some(&s("k5")));
                assert_eq!(
                    filter,
                    Some(ConditionExpression::AttributeExists("v".into()))
                );
                assert_eq!(projection, None);
                assert!(!consistent_read, "default is false");
            }
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    /// `Scan` decodes an `IndexName` (ADR 0041 §5), same as `Query` — and
    /// omitting it leaves `index` `None` (already covered above by
    /// `decodes_scan_with_limit_and_filter`).
    #[test]
    fn decodes_scan_against_an_index() {
        let body = br#"{"TableName":"t","IndexName":"by-email","Limit":5}"#;
        match decode_request("DynamoDB_20120810.Scan", body).unwrap() {
            Operation::Scan { table, index, .. } => {
                assert_eq!(table, "t");
                assert_eq!(index.as_deref(), Some("by-email"));
            }
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    #[test]
    fn scan_response_includes_cursor_when_truncated() {
        let mut a = Item::new();
        a.insert("id".into(), s("k1"));
        let mut key = Item::new();
        key.insert("id".into(), s("k1"));
        let body = scan_response(&[a.clone()], 3, Some(&key));
        assert!(body.contains("\"Count\":1"));
        assert!(body.contains("\"ScannedCount\":3"));
        assert!(body.contains("\"LastEvaluatedKey\""));
        // No cursor when the page was not truncated.
        let body = scan_response(&[a], 1, None);
        assert!(!body.contains("LastEvaluatedKey"));
    }

    #[test]
    fn decodes_update_item_set_and_remove() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET a = :v, b = :w REMOVE c",
            "ExpressionAttributeValues":{":v":{"S":"x"},":w":{"N":"3"}},
            "ReturnValues":"ALL_NEW"}"#;
        let Operation::UpdateItem {
            actions,
            return_values,
            ..
        } = decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![
                UpdateAction::Set("a".into(), UpdateExpr::value(s("x"))),
                UpdateAction::Set("b".into(), UpdateExpr::value(AttributeValue::N("3".into()))),
                UpdateAction::Remove("c".into()),
            ]
        );
        assert_eq!(return_values, UpdateReturnValues::AllNew);
    }

    /// Leading whitespace before `SET` is fine — only non-whitespace leading
    /// text is rejected.
    #[test]
    fn decodes_update_item_with_leading_whitespace() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"   SET x = :v",
            "ExpressionAttributeValues":{":v":{"S":"y"}}}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![UpdateAction::Set("x".into(), UpdateExpr::value(s("y")))]
        );
    }

    /// Regression: unrecognized leading text before the first clause keyword
    /// used to be silently dropped (`"foo SET x = :v"` applied only `SET x =
    /// :v`) instead of being rejected.
    #[test]
    fn rejects_update_expression_with_leading_garbage() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"foo SET x = :v",
            "ExpressionAttributeValues":{":v":{"S":"y"}}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateItem", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    /// The tokenizer rewrite's whole point: an *unaliased* top-level
    /// attribute literally spelled like a clause keyword now parses as an
    /// attribute name wherever the grammar expects one, not as a clause
    /// keyword — issue #372.
    #[test]
    fn unaliased_reserved_word_as_top_level_attribute_parses() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET set = :v",
            "ExpressionAttributeValues":{":v":{"S":"x"}}}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![UpdateAction::Set("set".into(), UpdateExpr::value(s("x")))]
        );
    }

    /// A reserved word used as the attribute name on *both* sides of a SET
    /// clause's comma-separated action list.
    #[test]
    fn reserved_words_as_attribute_names_in_a_set_action_list() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET add = :v, remove = :w",
            "ExpressionAttributeValues":{":v":{"S":"x"},":w":{"S":"y"}}}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![
                UpdateAction::Set("add".into(), UpdateExpr::value(s("x"))),
                UpdateAction::Set("remove".into(), UpdateExpr::value(s("y"))),
            ]
        );
    }

    /// A reserved word as the sole `REMOVE` target.
    #[test]
    fn reserved_word_as_remove_target() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"REMOVE set"}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(actions, vec![UpdateAction::Remove("set".into())]);
    }

    /// Two reserved words in one `REMOVE` action list.
    #[test]
    fn reserved_words_in_a_remove_action_list() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"REMOVE remove, delete"}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![
                UpdateAction::Remove("remove".into()),
                UpdateAction::Remove("delete".into()),
            ]
        );
    }

    /// A reserved word as an `ADD` target.
    #[test]
    fn reserved_word_as_add_target() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"ADD add :n",
            "ExpressionAttributeValues":{":n":{"N":"1"}}}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![UpdateAction::Add(
                "add".into(),
                AttributeValue::N("1".into())
            )]
        );
    }

    /// A reserved word as a `DELETE` target.
    #[test]
    fn reserved_word_as_delete_target() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"DELETE delete :ss",
            "ExpressionAttributeValues":{":ss":{"SS":["a"]}}}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![UpdateAction::Delete(
                "delete".into(),
                AttributeValue::SS(vec!["a".into()])
            )]
        );
    }

    /// The interleaved case that most exercises the clause-boundary logic:
    /// every clause keyword also appears, immediately after, as the reserved
    /// word spelling of the *next* clause's target attribute — the tokenizer
    /// must recognize each leading keyword as a clause start while treating
    /// every other occurrence as a plain attribute name.
    #[test]
    fn mixed_multi_clause_reserved_word_attributes_parse() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET set = :v REMOVE remove ADD add :n DELETE delete :ss",
            "ExpressionAttributeValues":{":v":{"S":"x"},":n":{"N":"1"},":ss":{"SS":["a"]}}}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![
                UpdateAction::Set("set".into(), UpdateExpr::value(s("x"))),
                UpdateAction::Remove("remove".into()),
                UpdateAction::Add("add".into(), AttributeValue::N("1".into())),
                UpdateAction::Delete("delete".into(), AttributeValue::SS(vec!["a".into()])),
            ]
        );
    }

    /// `size` is a reserved word too (a `ConditionExpression` function name),
    /// but is not one of the four `UpdateExpression` clause keywords, so it
    /// was never actually at risk — kept as a belt-and-suspenders check.
    #[test]
    fn reserved_condition_function_name_as_top_level_attribute() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET size = :v",
            "ExpressionAttributeValues":{":v":{"S":"x"}}}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![UpdateAction::Set("size".into(), UpdateExpr::value(s("x")))]
        );
    }

    /// A reserved word aliased via `#alias` (the AWS-recommended way to name
    /// a reserved word) keeps working exactly as before.
    #[test]
    fn aliased_reserved_word_still_resolves_through_expression_attribute_names() {
        let body = br##"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET #s = :v",
            "ExpressionAttributeNames":{"#s":"set"},
            "ExpressionAttributeValues":{":v":{"S":"x"}}}"##;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![UpdateAction::Set("set".into(), UpdateExpr::value(s("x")))]
        );
    }

    /// Clause keywords stay case-insensitive at a genuine clause-start
    /// position — the substring scanner this replaces already lowercased
    /// before matching, so a lowercase `set` at the very start of the
    /// expression was, and remains, a real `SET` clause (unlike the
    /// reserved-word-as-attribute cases above, where the keyword spelling
    /// only appears in an *operand* position).
    #[test]
    fn clause_keywords_stay_case_insensitive_at_a_clause_start() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"set a = :v",
            "ExpressionAttributeValues":{":v":{"S":"x"}}}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![UpdateAction::Set("a".into(), UpdateExpr::value(s("x")))]
        );
    }

    /// A malformed sequence — a second `path = value` action missing its
    /// separating comma — is still rejected, not silently misparsed, even
    /// though the tokenizer no longer keys off keyword substrings.
    #[test]
    fn rejects_a_set_action_list_missing_its_comma() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET a = :v b = :w",
            "ExpressionAttributeValues":{":v":{"S":"x"},":w":{"S":"y"}}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateItem", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn apply_update_sets_and_removes() {
        let mut item = Item::new();
        item.insert("id".into(), s("k"));
        item.insert("c".into(), s("drop"));
        let new = apply_update(
            item,
            &[
                UpdateAction::Set("a".into(), UpdateExpr::value(s("x"))),
                UpdateAction::Remove("c".into()),
            ],
        )
        .expect("SET/REMOVE are infallible");
        assert_eq!(new.get("a"), Some(&s("x")));
        assert!(!new.contains_key("c"));
        assert_eq!(new.get("id"), Some(&s("k")));
    }

    // --- `if_not_exists`/`list_append` (issue #375 PR1) --------------------

    #[test]
    fn decodes_if_not_exists_in_set() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET a = if_not_exists(a, :v)",
            "ExpressionAttributeValues":{":v":{"N":"0"}}}"#;
        let Operation::UpdateItem { actions, .. } =
            decode_request("DynamoDB_20120810.UpdateItem", body).unwrap()
        else {
            panic!("expected UpdateItem");
        };
        assert_eq!(
            actions,
            vec![UpdateAction::Set(
                "a".into(),
                UpdateExpr::Operand(UpdateOperand::IfNotExists(
                    "a".into(),
                    Box::new(UpdateOperand::Value(AttributeValue::N("0".into())))
                ))
            )]
        );
    }

    #[test]
    fn if_not_exists_seeds_an_absent_attribute_and_leaves_a_present_one_alone() {
        let action = UpdateAction::Set(
            "a".into(),
            UpdateExpr::Operand(UpdateOperand::IfNotExists(
                "a".into(),
                Box::new(UpdateOperand::Value(AttributeValue::N("0".into()))),
            )),
        );
        let out =
            apply_update(Item::new(), std::slice::from_ref(&action)).expect("seeds the default");
        assert_eq!(out.get("a"), Some(&AttributeValue::N("0".into())));

        let mut present = Item::new();
        present.insert("a".into(), AttributeValue::N("7".into()));
        let out = apply_update(present, &[action]).expect("leaves the existing value alone");
        assert_eq!(out.get("a"), Some(&AttributeValue::N("7".into())));
    }

    /// `if_not_exists(a, :v)` where `a` is absent and `:v` is itself an
    /// absent-path default (never wrapped in another `if_not_exists`) has no
    /// value to assign — a `ValidationException`, not a silently-applied
    /// no-op.
    #[test]
    fn if_not_exists_with_no_default_value_is_a_validation_error() {
        let action = UpdateAction::Set(
            "a".into(),
            UpdateExpr::Operand(UpdateOperand::IfNotExists(
                "a".into(),
                Box::new(UpdateOperand::Path("also_absent".into())),
            )),
        );
        let err = apply_update(Item::new(), &[action]).expect_err("nothing to assign");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_list_append_both_operand_orders() {
        for expr in ["SET a = list_append(a, :l)", "SET a = list_append(:l, a)"] {
            let body = format!(
                r#"{{"TableName":"t","Key":{{"id":{{"S":"k"}}}},
                "UpdateExpression":"{expr}",
                "ExpressionAttributeValues":{{":l":{{"L":[{{"N":"3"}}]}}}}}}"#
            );
            decode_request("DynamoDB_20120810.UpdateItem", body.as_bytes())
                .unwrap_or_else(|e| panic!("`{expr}` should decode: {e:?}"));
        }
    }

    #[test]
    fn list_append_concatenates_in_order() {
        let mut item = Item::new();
        item.insert(
            "a".into(),
            AttributeValue::L(vec![AttributeValue::N("1".into())]),
        );
        let action = UpdateAction::Set(
            "a".into(),
            UpdateExpr::Operand(UpdateOperand::ListAppend(
                Box::new(UpdateOperand::Path("a".into())),
                Box::new(UpdateOperand::Value(AttributeValue::L(vec![
                    AttributeValue::N("2".into()),
                ]))),
            )),
        );
        let out = apply_update(item, &[action]).expect("both operands are lists");
        assert_eq!(
            out.get("a"),
            Some(&AttributeValue::L(vec![
                AttributeValue::N("1".into()),
                AttributeValue::N("2".into()),
            ]))
        );
    }

    #[test]
    fn list_append_on_a_non_list_operand_is_a_validation_error() {
        let mut item = Item::new();
        item.insert("a".into(), AttributeValue::N("1".into()));
        let action = UpdateAction::Set(
            "a".into(),
            UpdateExpr::Operand(UpdateOperand::ListAppend(
                Box::new(UpdateOperand::Path("a".into())),
                Box::new(UpdateOperand::Value(AttributeValue::L(vec![]))),
            )),
        );
        let err = apply_update(item, &[action]).expect_err("a is a number, not a list");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn list_append_on_a_missing_operand_is_a_validation_error() {
        let action = UpdateAction::Set(
            "a".into(),
            UpdateExpr::Operand(UpdateOperand::ListAppend(
                Box::new(UpdateOperand::Path("missing".into())),
                Box::new(UpdateOperand::Value(AttributeValue::L(vec![]))),
            )),
        );
        let err = apply_update(Item::new(), &[action]).expect_err("`missing` does not exist");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn unsupported_function_name_is_rejected() {
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "UpdateExpression":"SET a = nope(a, :v)",
            "ExpressionAttributeValues":{":v":{"N":"0"}}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateItem", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_batch_write() {
        let body = br#"{"RequestItems":{
            "t":[{"PutRequest":{"Item":{"id":{"S":"a"}}}},
                 {"DeleteRequest":{"Key":{"id":{"S":"b"}}}}]}}"#;
        let Operation::BatchWriteItem { requests } =
            decode_request("DynamoDB_20120810.BatchWriteItem", body).unwrap()
        else {
            panic!("expected BatchWriteItem");
        };
        let reqs = requests.get("t").expect("table t present");
        assert_eq!(reqs.len(), 2);
        assert!(matches!(reqs[0], WriteRequest::Put(_)));
        assert!(matches!(reqs[1], WriteRequest::Delete(_)));
    }

    #[test]
    fn decodes_transact_write() {
        let body = br#"{"TransactItems":[
            {"Put":{"TableName":"t","Item":{"id":{"S":"a"}},
                    "ConditionExpression":"attribute_not_exists(id)"}},
            {"Update":{"TableName":"t","Key":{"id":{"S":"b"}},
                       "UpdateExpression":"SET v = :v",
                       "ExpressionAttributeValues":{":v":{"N":"1"}}}},
            {"ConditionCheck":{"TableName":"t","Key":{"id":{"S":"c"}},
                               "ConditionExpression":"attribute_exists(id)"}}]}"#;
        let Operation::TransactWriteItems { actions, token } =
            decode_request("DynamoDB_20120810.TransactWriteItems", body).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_eq!(actions.len(), 3);
        assert_eq!(token, None, "no ClientRequestToken in this body");
        assert!(matches!(actions[0], TransactAction::Put { .. }));
        assert!(matches!(actions[1], TransactAction::Update { .. }));
        assert!(matches!(actions[2], TransactAction::ConditionCheck { .. }));
    }

    // --- `ClientRequestToken` (ADR 0018's 2026-08-24 amendment) -----------

    #[test]
    fn transact_write_decodes_a_present_client_request_token() {
        let body = br#"{"ClientRequestToken":"abc-123",
            "TransactItems":[{"Put":{"TableName":"t","Item":{"id":{"S":"a"}}}}]}"#;
        let Operation::TransactWriteItems { token, .. } =
            decode_request("DynamoDB_20120810.TransactWriteItems", body).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_eq!(token.as_deref(), Some("abc-123"));
    }

    #[test]
    fn transact_write_client_request_token_absent_decodes_to_none() {
        let body = transact_write_body(1);
        let Operation::TransactWriteItems { token, .. } =
            decode_request("DynamoDB_20120810.TransactWriteItems", body.as_bytes()).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_eq!(token, None);
    }

    #[test]
    fn transact_write_client_request_token_accepts_the_length_bounds() {
        let one_char = format!(
            r#"{{"ClientRequestToken":"x","TransactItems":[{}]}}"#,
            r#"{"Put":{"TableName":"t","Item":{"id":{"S":"a"}}}}"#
        );
        let Operation::TransactWriteItems { token, .. } =
            decode_request("DynamoDB_20120810.TransactWriteItems", one_char.as_bytes()).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_eq!(token.as_deref(), Some("x"));

        let thirty_six = "x".repeat(36);
        let at_cap = format!(
            r#"{{"ClientRequestToken":"{thirty_six}","TransactItems":[{}]}}"#,
            r#"{"Put":{"TableName":"t","Item":{"id":{"S":"a"}}}}"#
        );
        let Operation::TransactWriteItems { token, .. } =
            decode_request("DynamoDB_20120810.TransactWriteItems", at_cap.as_bytes()).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_eq!(token.as_deref(), Some(thirty_six.as_str()));
    }

    #[test]
    fn transact_write_rejects_a_too_long_client_request_token() {
        let too_long = "x".repeat(37);
        let body = format!(
            r#"{{"ClientRequestToken":"{too_long}","TransactItems":[{}]}}"#,
            r#"{"Put":{"TableName":"t","Item":{"id":{"S":"a"}}}}"#
        );
        let err = decode_request("DynamoDB_20120810.TransactWriteItems", body.as_bytes())
            .expect_err("37 characters exceeds the AWS cap");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn transact_write_fingerprint_ignores_json_key_order() {
        // Two bodies whose `TransactItems` array carries the same logical
        // actions but with JSON object keys in a different order must decode
        // to actions that fingerprint identically — the fingerprint is a
        // property of the decoded `Vec<TransactAction>` (all `BTreeMap`),
        // never a hash of the raw request bytes.
        let a = br#"{"TransactItems":[{"Put":{"TableName":"t","Item":{"id":{"S":"a"}},
            "ConditionExpression":"attribute_not_exists(id)"}}]}"#;
        let b = br#"{"TransactItems":[{"Put":{"ConditionExpression":"attribute_not_exists(id)",
            "Item":{"id":{"S":"a"}},"TableName":"t"}}]}"#;
        let Operation::TransactWriteItems {
            actions: actions_a, ..
        } = decode_request("DynamoDB_20120810.TransactWriteItems", a).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        let Operation::TransactWriteItems {
            actions: actions_b, ..
        } = decode_request("DynamoDB_20120810.TransactWriteItems", b).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_eq!(
            transact_write_fingerprint(&actions_a),
            transact_write_fingerprint(&actions_b)
        );
    }

    #[test]
    fn transact_write_fingerprint_differs_on_a_different_action() {
        let same_key_different_item = br#"{"TransactItems":[
            {"Put":{"TableName":"t","Item":{"id":{"S":"a"},"v":{"N":"1"}}}}]}"#;
        let original = br#"{"TransactItems":[
            {"Put":{"TableName":"t","Item":{"id":{"S":"a"},"v":{"N":"2"}}}}]}"#;
        let Operation::TransactWriteItems { actions: a, .. } = decode_request(
            "DynamoDB_20120810.TransactWriteItems",
            same_key_different_item,
        )
        .unwrap() else {
            panic!("expected TransactWriteItems");
        };
        let Operation::TransactWriteItems { actions: b, .. } =
            decode_request("DynamoDB_20120810.TransactWriteItems", original).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_ne!(
            transact_write_fingerprint(&a),
            transact_write_fingerprint(&b)
        );
    }

    #[test]
    fn transact_write_rejects_an_empty_client_request_token() {
        let body = r#"{"ClientRequestToken":"",
            "TransactItems":[{"Put":{"TableName":"t","Item":{"id":{"S":"a"}}}}]}"#;
        let err = decode_request("DynamoDB_20120810.TransactWriteItems", body.as_bytes())
            .expect_err("empty token is below the AWS minimum");
        assert_eq!(err.code, "ValidationException");
    }

    // --- Per-action `CancellationReasons` (ADR 0018's 2026-08-24
    // `CancellationReasons` amendment, issue #374 C2a) ----------------------

    #[test]
    fn cancellation_reason_json_shape_matches_aws_exactly() {
        // The exact shape ADR 0018's amendment specifies: `Message` always
        // present (`null` for `None`), `Item` present only on a
        // `ConditionalCheckFailed` entry that actually has one.
        let mut item = Item::new();
        item.insert("id".into(), s("k"));
        let err = WireError::transaction_canceled_with_reasons(vec![
            CancellationReason::none(),
            CancellationReason::conditional_check_failed(Some(&item)),
        ]);
        assert_eq!(err.code, "TransactionCanceledException");
        let json: Value = serde_json::from_str(&err.to_json()).unwrap();
        let reasons = json
            .get("CancellationReasons")
            .and_then(Value::as_array)
            .expect("CancellationReasons array present");
        assert_eq!(reasons.len(), 2);
        assert_eq!(reasons[0]["Code"], "None");
        assert_eq!(reasons[0]["Message"], Value::Null);
        assert!(
            reasons[0].get("Item").is_none(),
            "a `None` entry must omit `Item` entirely, not null it"
        );
        assert_eq!(reasons[1]["Code"], "ConditionalCheckFailed");
        assert_eq!(reasons[1]["Message"], "The conditional request failed");
        assert_eq!(reasons[1]["Item"]["id"]["S"], "k");
    }

    #[test]
    fn cancellation_reason_without_all_old_omits_item() {
        let err = WireError::transaction_canceled_with_reasons(vec![
            CancellationReason::conditional_check_failed(None),
        ]);
        let json: Value = serde_json::from_str(&err.to_json()).unwrap();
        let reason = &json["CancellationReasons"][0];
        assert_eq!(reason["Code"], "ConditionalCheckFailed");
        assert!(
            reason.get("Item").is_none(),
            "no old image in hand ⇒ no `Item`, even for a `ConditionalCheckFailed` entry"
        );
    }

    #[test]
    fn cancellation_reason_transaction_conflict_shape() {
        let err = WireError::transaction_canceled_with_reasons(vec![
            CancellationReason::none(),
            CancellationReason::none(),
            CancellationReason::transaction_conflict(),
        ]);
        let json: Value = serde_json::from_str(&err.to_json()).unwrap();
        let reasons = json["CancellationReasons"].as_array().unwrap();
        assert_eq!(reasons.len(), 3);
        assert_eq!(reasons[2]["Code"], "TransactionConflict");
        assert!(reasons[2]["Message"].is_string());
        assert!(reasons[2].get("Item").is_none());
    }

    #[test]
    fn cancellation_aggregate_message_brackets_the_codes_in_order() {
        let err = WireError::transaction_canceled_with_reasons(vec![
            CancellationReason::none(),
            CancellationReason::conditional_check_failed(None),
        ]);
        assert_eq!(
            err.message,
            "Transaction cancelled, please refer cancellation reasons for specific reasons \
             [None, ConditionalCheckFailed]"
        );
    }

    #[test]
    fn a_plain_transaction_canceled_carries_no_reasons() {
        // `transaction_canceled` (the aggregate-only constructor, still used
        // where no per-action detail is available) must not accidentally
        // gain a `CancellationReasons` key.
        let err = WireError::transaction_canceled("aggregate only");
        assert_eq!(err.reasons, None);
        let json: Value = serde_json::from_str(&err.to_json()).unwrap();
        assert!(json.get("CancellationReasons").is_none());
    }

    #[test]
    fn decodes_return_values_on_condition_check_failure_per_action() {
        let body = br#"{"TransactItems":[
            {"Put":{"TableName":"t","Item":{"id":{"S":"a"}},
                    "ConditionExpression":"attribute_not_exists(id)",
                    "ReturnValuesOnConditionCheckFailure":"ALL_OLD"}},
            {"ConditionCheck":{"TableName":"t","Key":{"id":{"S":"b"}},
                               "ConditionExpression":"attribute_exists(id)"}}]}"#;
        let Operation::TransactWriteItems { actions, .. } =
            decode_request("DynamoDB_20120810.TransactWriteItems", body).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_eq!(
            actions[0].rvocf(),
            ReturnValuesOnConditionCheckFailure::AllOld
        );
        // Absent ⇒ NONE, matching AWS's own default.
        assert_eq!(
            actions[1].rvocf(),
            ReturnValuesOnConditionCheckFailure::None
        );
    }

    #[test]
    fn rejects_an_invalid_return_values_on_condition_check_failure() {
        let body = br#"{"TransactItems":[
            {"Put":{"TableName":"t","Item":{"id":{"S":"a"}},
                    "ReturnValuesOnConditionCheckFailure":"ALL_NEW"}}]}"#;
        let err = decode_request("DynamoDB_20120810.TransactWriteItems", body)
            .expect_err("ALL_NEW is not a legal ReturnValuesOnConditionCheckFailure value");
        assert_eq!(err.code, "ValidationException");
    }

    // --- AWS batch/transaction count caps ----------------------------------

    /// A `BatchWriteItem` body with `n` `PutRequest`s against one table.
    fn batch_write_body(n: usize) -> String {
        let items: Vec<String> = (0..n)
            .map(|i| format!(r#"{{"PutRequest":{{"Item":{{"id":{{"S":"i{i}"}}}}}}}}"#))
            .collect();
        format!(r#"{{"RequestItems":{{"t":[{}]}}}}"#, items.join(","))
    }

    #[test]
    fn batch_write_accepts_the_cap_and_rejects_one_over_it() {
        let at_cap = batch_write_body(BATCH_WRITE_MAX_ITEMS);
        decode_request("DynamoDB_20120810.BatchWriteItem", at_cap.as_bytes())
            .expect("exactly the cap is accepted");

        let over_cap = batch_write_body(BATCH_WRITE_MAX_ITEMS + 1);
        let err = decode_request("DynamoDB_20120810.BatchWriteItem", over_cap.as_bytes())
            .expect_err("one over the cap is rejected");
        assert_eq!(err.code, "ValidationException");
    }

    /// A `BatchGetItem` body with `n` keys against one table.
    fn batch_get_body(n: usize) -> String {
        let keys: Vec<String> = (0..n)
            .map(|i| format!(r#"{{"id":{{"S":"i{i}"}}}}"#))
            .collect();
        format!(
            r#"{{"RequestItems":{{"t":{{"Keys":[{}]}}}}}}"#,
            keys.join(",")
        )
    }

    #[test]
    fn batch_get_accepts_the_cap_and_rejects_one_over_it() {
        let at_cap = batch_get_body(BATCH_GET_MAX_KEYS);
        decode_request("DynamoDB_20120810.BatchGetItem", at_cap.as_bytes())
            .expect("exactly the cap is accepted");

        let over_cap = batch_get_body(BATCH_GET_MAX_KEYS + 1);
        let err = decode_request("DynamoDB_20120810.BatchGetItem", over_cap.as_bytes())
            .expect_err("one over the cap is rejected");
        assert_eq!(err.code, "ValidationException");
    }

    /// A `TransactWriteItems` body with `n` `Put` actions, each its own key.
    fn transact_write_body(n: usize) -> String {
        let actions: Vec<String> = (0..n)
            .map(|i| format!(r#"{{"Put":{{"TableName":"t","Item":{{"id":{{"S":"i{i}"}}}}}}}}"#))
            .collect();
        format!(r#"{{"TransactItems":[{}]}}"#, actions.join(","))
    }

    #[test]
    fn transact_write_accepts_the_cap_and_rejects_one_over_it() {
        let at_cap = transact_write_body(TRANSACT_WRITE_MAX_ACTIONS);
        decode_request("DynamoDB_20120810.TransactWriteItems", at_cap.as_bytes())
            .expect("exactly the cap is accepted");

        let over_cap = transact_write_body(TRANSACT_WRITE_MAX_ACTIONS + 1);
        let err = decode_request("DynamoDB_20120810.TransactWriteItems", over_cap.as_bytes())
            .expect_err("one over the cap is rejected");
        assert_eq!(err.code, "ValidationException");
    }

    /// A `TransactGetItems` body with `n` `Get` items, each its own key.
    fn transact_get_body(n: usize) -> String {
        let gets: Vec<String> = (0..n)
            .map(|i| format!(r#"{{"Get":{{"TableName":"t","Key":{{"id":{{"S":"i{i}"}}}}}}}}"#))
            .collect();
        format!(r#"{{"TransactItems":[{}]}}"#, gets.join(","))
    }

    #[test]
    fn decodes_transact_get() {
        let body = transact_get_body(2);
        let Operation::TransactGetItems { gets } =
            decode_request("DynamoDB_20120810.TransactGetItems", body.as_bytes()).unwrap()
        else {
            panic!("expected TransactGetItems");
        };
        assert_eq!(gets.len(), 2);
        assert_eq!(gets[0].table, "t");
    }

    #[test]
    fn transact_get_accepts_the_cap_and_rejects_one_over_it() {
        let at_cap = transact_get_body(TRANSACT_GET_MAX_ITEMS);
        decode_request("DynamoDB_20120810.TransactGetItems", at_cap.as_bytes())
            .expect("exactly the cap is accepted");

        let over_cap = transact_get_body(TRANSACT_GET_MAX_ITEMS + 1);
        let err = decode_request("DynamoDB_20120810.TransactGetItems", over_cap.as_bytes())
            .expect_err("one over the cap is rejected");
        assert_eq!(err.code, "ValidationException");
    }

    // --- AWS 400 KB item-size cap -------------------------------------------

    /// A single-attribute item (`"a": {"S": ..}`) whose `item_size` is exactly
    /// `1 + value_len` (the attribute name `"a"` is one byte, an `S` value's
    /// size is its length).
    fn item_of_size(value_len: usize) -> String {
        format!(r#"{{"a":{{"S":"{}"}}}}"#, "x".repeat(value_len))
    }

    #[test]
    fn put_item_accepts_exactly_the_size_cap_and_rejects_one_byte_over() {
        let item = item_of_size(MAX_ITEM_SIZE_BYTES - 1);
        let at_cap = format!(r#"{{"TableName":"t","Item":{item}}}"#);
        decode_request("DynamoDB_20120810.PutItem", at_cap.as_bytes())
            .expect("exactly 409600 bytes is accepted");

        let item = item_of_size(MAX_ITEM_SIZE_BYTES);
        let over_cap = format!(r#"{{"TableName":"t","Item":{item}}}"#);
        let err = decode_request("DynamoDB_20120810.PutItem", over_cap.as_bytes())
            .expect_err("409601 bytes is rejected");
        assert_eq!(err.code, "ValidationException");
        assert!(
            err.message
                .contains("Item size has exceeded the maximum allowed size")
        );
    }

    #[test]
    fn batch_write_put_request_enforces_the_item_size_cap() {
        let item = item_of_size(MAX_ITEM_SIZE_BYTES - 1);
        let at_cap = format!(r#"{{"RequestItems":{{"t":[{{"PutRequest":{{"Item":{item}}}}}]}}}}"#);
        decode_request("DynamoDB_20120810.BatchWriteItem", at_cap.as_bytes())
            .expect("exactly 409600 bytes is accepted");

        let item = item_of_size(MAX_ITEM_SIZE_BYTES);
        let over_cap =
            format!(r#"{{"RequestItems":{{"t":[{{"PutRequest":{{"Item":{item}}}}}]}}}}"#);
        let err = decode_request("DynamoDB_20120810.BatchWriteItem", over_cap.as_bytes())
            .expect_err("409601 bytes is rejected");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn transact_write_put_action_enforces_the_item_size_cap() {
        let item = item_of_size(MAX_ITEM_SIZE_BYTES - 1);
        let at_cap =
            format!(r#"{{"TransactItems":[{{"Put":{{"TableName":"t","Item":{item}}}}}]}}"#);
        decode_request("DynamoDB_20120810.TransactWriteItems", at_cap.as_bytes())
            .expect("exactly 409600 bytes is accepted");

        let item = item_of_size(MAX_ITEM_SIZE_BYTES);
        let over_cap =
            format!(r#"{{"TransactItems":[{{"Put":{{"TableName":"t","Item":{item}}}}}]}}"#);
        let err = decode_request("DynamoDB_20120810.TransactWriteItems", over_cap.as_bytes())
            .expect_err("409601 bytes is rejected");
        assert_eq!(err.code, "ValidationException");
    }

    /// `apply_update`'s post-fold result is checked against the same cap:
    /// exactly `MAX_ITEM_SIZE_BYTES` is accepted, one byte over is rejected.
    /// This is the choke point both `UpdateItem` and `TransactWriteItems`'s
    /// `Update` action route through, so covering it here covers both.
    #[test]
    fn apply_update_result_accepts_exactly_the_size_cap_and_rejects_one_byte_over() {
        // "a" is a 1-byte attribute name, so a value of `MAX_ITEM_SIZE_BYTES -
        // 1` bytes makes the post-update item land exactly on the cap.
        let at_cap_value = AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES - 1));
        let out = apply_update(
            Item::new(),
            &[UpdateAction::Set(
                "a".into(),
                UpdateExpr::value(at_cap_value),
            )],
        )
        .expect("exactly the cap is accepted");
        assert_eq!(item_size(&out), MAX_ITEM_SIZE_BYTES);

        let over_cap_value = AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES));
        let err = apply_update(
            Item::new(),
            &[UpdateAction::Set(
                "a".into(),
                UpdateExpr::value(over_cap_value),
            )],
        )
        .expect_err("one byte over the cap is rejected");
        assert_eq!(err.code, "ValidationException");
        assert!(
            err.message
                .contains("Item size has exceeded the maximum allowed size")
        );
    }

    /// An item temporarily over the cap **mid-fold** must still succeed if
    /// the fold's own later action nets the *final* result back under it —
    /// the check runs once, after the whole action list folds, never
    /// mid-fold. Ordered `SET` (pushes it over) then `REMOVE` (nets it back
    /// under) so the over-size state genuinely occurs before the netting.
    #[test]
    fn apply_update_nets_under_the_cap_after_an_over_size_mid_fold_state() {
        let mut item = Item::new();
        item.insert("keep".into(), s("k"));

        let huge = AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES));
        let out = apply_update(
            item,
            &[
                UpdateAction::Set("temp".into(), UpdateExpr::value(huge)),
                UpdateAction::Remove("temp".into()),
            ],
        )
        .expect("nets back under the cap after the REMOVE, so it must succeed");
        assert!(!out.contains_key("temp"));
        assert_eq!(out.get("keep"), Some(&s("k")));
    }

    /// An update to an already-near-cap base item whose result tips over the
    /// cap is rejected — the pre-update image being under the cap does not
    /// exempt the post-update one.
    #[test]
    fn apply_update_rejects_when_it_tips_a_near_cap_base_item_over() {
        let mut item = Item::new();
        item.insert(
            "a".into(),
            AttributeValue::S("x".repeat(MAX_ITEM_SIZE_BYTES - 10)),
        );
        // base item size = 1 ("a") + (MAX_ITEM_SIZE_BYTES - 10) = MAX_ITEM_SIZE_BYTES - 9.

        let err = apply_update(
            item,
            &[UpdateAction::Set(
                "b".into(),
                UpdateExpr::value(AttributeValue::S("y".repeat(20))),
            )],
        )
        // adds 1 ("b") + 20 = 21 bytes -> MAX_ITEM_SIZE_BYTES + 12, over the cap.
        .expect_err("tips the near-cap base item over the cap");
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_response_echoes_new_for_all_new() {
        let mut new = Item::new();
        new.insert("a".into(), s("x"));
        let body = update_response(UpdateReturnValues::AllNew, None, Some(&new), None, None);
        assert!(body.contains("\"Attributes\""));
        assert!(body.contains("\"S\":\"x\""));
        assert_eq!(
            update_response(UpdateReturnValues::None, None, Some(&new), None, None),
            "{}"
        );
    }

    #[test]
    fn decodes_index_projection_types() {
        let body = br#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"k","KeySchema":[{"AttributeName":"e","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"KEYS_ONLY"}},
                {"IndexName":"i","KeySchema":[{"AttributeName":"o","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":["x","y"]}}]}"#;
        let Operation::CreateTable { indexes, .. } =
            decode_request("DynamoDB_20120810.CreateTable", body).unwrap()
        else {
            panic!("expected CreateTable");
        };
        let SecondaryIndex::Global(k) = &indexes[0] else {
            panic!("gsi 0");
        };
        assert_eq!(k.projection, IndexProjection::KeysOnly);
        let SecondaryIndex::Global(i) = &indexes[1] else {
            panic!("gsi 1");
        };
        assert_eq!(
            i.projection,
            IndexProjection::Include(vec!["x".into(), "y".into()])
        );
    }

    // --- AWS index-count caps (CreateTable) --------------------------------

    /// A `CreateTable` body declaring `n` GSIs, each with a unique name.
    fn create_table_with_gsis(n: usize) -> String {
        let gsis: Vec<String> = (0..n)
            .map(|i| {
                format!(r#"{{"IndexName":"gsi{i}","KeySchema":[{{"AttributeName":"e","KeyType":"HASH"}}]}}"#)
            })
            .collect();
        format!(
            r#"{{"TableName":"t","KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "GlobalSecondaryIndexes":[{}]}}"#,
            gsis.join(",")
        )
    }

    #[test]
    fn create_table_accepts_the_gsi_cap_and_rejects_one_over_it() {
        let at_cap = create_table_with_gsis(MAX_GSI_PER_TABLE);
        decode_request("DynamoDB_20120810.CreateTable", at_cap.as_bytes())
            .expect("exactly 20 GSIs is accepted");

        let over_cap = create_table_with_gsis(MAX_GSI_PER_TABLE + 1);
        let err = decode_request("DynamoDB_20120810.CreateTable", over_cap.as_bytes())
            .expect_err("21 GSIs is rejected");
        assert_eq!(err.code, "ValidationException");
    }

    /// A `CreateTable` body declaring `n` LSIs, each with its own sort
    /// attribute (LSIs must all share the base HASH key, so only the RANGE
    /// attribute varies).
    fn create_table_with_lsis(n: usize) -> String {
        let lsis: Vec<String> = (0..n)
            .map(|i| {
                format!(
                    r#"{{"IndexName":"lsi{i}","KeySchema":[
                        {{"AttributeName":"id","KeyType":"HASH"}},
                        {{"AttributeName":"r{i}","KeyType":"RANGE"}}]}}"#
                )
            })
            .collect();
        format!(
            r#"{{"TableName":"t","KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "LocalSecondaryIndexes":[{}]}}"#,
            lsis.join(",")
        )
    }

    #[test]
    fn create_table_accepts_the_lsi_cap_and_rejects_one_over_it() {
        let at_cap = create_table_with_lsis(MAX_LSI_PER_TABLE);
        decode_request("DynamoDB_20120810.CreateTable", at_cap.as_bytes())
            .expect("exactly 5 LSIs is accepted");

        let over_cap = create_table_with_lsis(MAX_LSI_PER_TABLE + 1);
        let err = decode_request("DynamoDB_20120810.CreateTable", over_cap.as_bytes())
            .expect_err("6 LSIs is rejected");
        assert_eq!(err.code, "ValidationException");
    }

    // --- UpdateTimeToLive / DescribeTimeToLive (ADR 0051) -----------------

    #[test]
    fn decodes_update_time_to_live_enable() {
        let body = br#"{"TableName":"t","TimeToLiveSpecification":
            {"Enabled":true,"AttributeName":"expiresAt"}}"#;
        match decode_request("DynamoDB_20120810.UpdateTimeToLive", body).unwrap() {
            Operation::UpdateTimeToLive {
                table,
                attribute_name,
                enabled,
            } => {
                assert_eq!(table, "t");
                assert_eq!(attribute_name, "expiresAt");
                assert!(enabled);
            }
            other => panic!("expected UpdateTimeToLive, got {other:?}"),
        }
    }

    #[test]
    fn decodes_update_time_to_live_disable() {
        // AWS requires `AttributeName` even to disable — it must name the
        // currently-enabled attribute.
        let body = br#"{"TableName":"t","TimeToLiveSpecification":
            {"Enabled":false,"AttributeName":"expiresAt"}}"#;
        match decode_request("DynamoDB_20120810.UpdateTimeToLive", body).unwrap() {
            Operation::UpdateTimeToLive {
                table,
                attribute_name,
                enabled,
            } => {
                assert_eq!(table, "t");
                assert_eq!(attribute_name, "expiresAt");
                assert!(!enabled);
            }
            other => panic!("expected UpdateTimeToLive, got {other:?}"),
        }
    }

    #[test]
    fn update_time_to_live_rejects_missing_table_name() {
        let body = br#"{"TimeToLiveSpecification":{"Enabled":true,"AttributeName":"ttl"}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTimeToLive", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_time_to_live_rejects_missing_specification() {
        let body = br#"{"TableName":"t"}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTimeToLive", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_time_to_live_rejects_a_mistyped_specification() {
        let body = br#"{"TableName":"t","TimeToLiveSpecification":"not-an-object"}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTimeToLive", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_time_to_live_rejects_missing_enabled() {
        let body = br#"{"TableName":"t","TimeToLiveSpecification":{"AttributeName":"ttl"}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTimeToLive", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_time_to_live_rejects_missing_attribute_name() {
        let body = br#"{"TableName":"t","TimeToLiveSpecification":{"Enabled":true}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateTimeToLive", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_describe_time_to_live() {
        let body = br#"{"TableName":"t"}"#;
        match decode_request("DynamoDB_20120810.DescribeTimeToLive", body).unwrap() {
            Operation::DescribeTimeToLive { table } => assert_eq!(table, "t"),
            other => panic!("expected DescribeTimeToLive, got {other:?}"),
        }
    }

    #[test]
    fn describe_time_to_live_rejects_missing_table_name() {
        let err = decode_request("DynamoDB_20120810.DescribeTimeToLive", b"{}").unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn update_time_to_live_response_shape() {
        let body = update_time_to_live_response("expiresAt", true);
        assert!(body.contains("\"TimeToLiveSpecification\""));
        assert!(body.contains("\"Enabled\":true"));
        assert!(body.contains("\"AttributeName\":\"expiresAt\""));

        let body = update_time_to_live_response("expiresAt", false);
        assert!(body.contains("\"Enabled\":false"));
        // The response still echoes the requested spec, including the
        // attribute name on a disable — AWS's own contract.
        assert!(body.contains("\"AttributeName\":\"expiresAt\""));
    }

    #[test]
    fn describe_time_to_live_response_enabled_includes_attribute_name() {
        let desc = TtlDescription {
            enabled: true,
            attribute_name: Some("expiresAt".into()),
        };
        let body = describe_time_to_live_response(&desc);
        assert!(body.contains("\"TimeToLiveDescription\""));
        assert!(body.contains("\"TimeToLiveStatus\":\"ENABLED\""));
        assert!(body.contains("\"AttributeName\":\"expiresAt\""));
    }

    /// The single most important shape assertion in this response: AWS omits
    /// `AttributeName` entirely (not `null`, not `""`) once TTL is disabled.
    #[test]
    fn describe_time_to_live_response_disabled_omits_attribute_name() {
        let desc = TtlDescription {
            enabled: false,
            attribute_name: Some("expiresAt".into()),
        };
        let body = describe_time_to_live_response(&desc);
        assert!(body.contains("\"TimeToLiveStatus\":\"DISABLED\""));
        assert!(
            !body.contains("AttributeName"),
            "AttributeName must be omitted when DISABLED: {body}"
        );
    }

    #[test]
    fn describe_time_to_live_response_disabled_with_no_remembered_name() {
        let desc = TtlDescription {
            enabled: false,
            attribute_name: None,
        };
        let body = describe_time_to_live_response(&desc);
        assert!(body.contains("\"TimeToLiveStatus\":\"DISABLED\""));
        assert!(!body.contains("AttributeName"));
    }

    // --- Resource tagging (roadmap W-06) ------------------------------------

    #[test]
    fn table_arn_round_trips_through_parse_table_arn() {
        let arn = table_arn("orders");
        assert_eq!(arn, "arn:aws:dynamodb:animus:0:table/orders");
        assert_eq!(parse_table_arn(&arn), Some("orders"));
    }

    #[test]
    fn parse_table_arn_rejects_a_stream_or_backup_arn() {
        assert_eq!(
            parse_table_arn("arn:aws:dynamodb:animus:0:table/orders/stream/L1"),
            None
        );
        assert_eq!(
            parse_table_arn("arn:aws:dynamodb:animus:0:table/orders/backup/abc"),
            None
        );
    }

    #[test]
    fn parse_table_arn_rejects_malformed_input() {
        assert_eq!(parse_table_arn("not-an-arn"), None);
        assert_eq!(parse_table_arn("arn:aws:dynamodb:animus:0:table/"), None);
    }

    #[test]
    fn decodes_tag_resource() {
        let body = br#"{"ResourceArn":"arn:aws:dynamodb:animus:0:table/orders",
            "Tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"payments"}]}"#;
        match decode_request("DynamoDB_20120810.TagResource", body).unwrap() {
            Operation::TagResource { table, tags } => {
                assert_eq!(table, "orders");
                assert_eq!(
                    tags,
                    BTreeMap::from([
                        ("env".to_owned(), "prod".to_owned()),
                        ("team".to_owned(), "payments".to_owned()),
                    ])
                );
            }
            other => panic!("expected TagResource, got {other:?}"),
        }
    }

    #[test]
    fn decode_tag_resource_rejects_a_malformed_resource_arn() {
        let body = br#"{"ResourceArn":"not-an-arn","Tags":[{"Key":"env","Value":"prod"}]}"#;
        let err = decode_request("DynamoDB_20120810.TagResource", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decode_tag_resource_rejects_missing_tags() {
        let body = br#"{"ResourceArn":"arn:aws:dynamodb:animus:0:table/orders"}"#;
        let err = decode_request("DynamoDB_20120810.TagResource", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_untag_resource() {
        let body = br#"{"ResourceArn":"arn:aws:dynamodb:animus:0:table/orders",
            "TagKeys":["env","team"]}"#;
        match decode_request("DynamoDB_20120810.UntagResource", body).unwrap() {
            Operation::UntagResource { table, tag_keys } => {
                assert_eq!(table, "orders");
                assert_eq!(tag_keys, vec!["env".to_owned(), "team".to_owned()]);
            }
            other => panic!("expected UntagResource, got {other:?}"),
        }
    }

    #[test]
    fn decodes_list_tags_of_resource() {
        let body = br#"{"ResourceArn":"arn:aws:dynamodb:animus:0:table/orders"}"#;
        match decode_request("DynamoDB_20120810.ListTagsOfResource", body).unwrap() {
            Operation::ListTagsOfResource { table } => assert_eq!(table, "orders"),
            other => panic!("expected ListTagsOfResource, got {other:?}"),
        }
    }

    #[test]
    fn list_tags_of_resource_response_shape() {
        let tags = BTreeMap::from([
            ("env".to_owned(), "prod".to_owned()),
            ("team".to_owned(), "payments".to_owned()),
        ]);
        let body = list_tags_of_resource_response(&tags);
        assert!(body.contains("\"Key\":\"env\""));
        assert!(body.contains("\"Value\":\"prod\""));
        assert!(body.contains("\"Key\":\"team\""));
    }

    #[test]
    fn list_tags_of_resource_response_empty() {
        let body = list_tags_of_resource_response(&BTreeMap::new());
        assert_eq!(body, "{\"Tags\":[]}");
    }

    #[test]
    fn tag_or_untag_resource_response_is_empty_object() {
        assert_eq!(tag_or_untag_resource_response(), "{}");
    }

    #[test]
    fn describe_table_response_includes_table_arn() {
        let body = describe_table_response(
            "orders",
            &TableSchema::simple("id"),
            &[],
            &[],
            &[],
            None,
            "ACTIVE",
        );
        assert!(body.contains("\"TableArn\":\"arn:aws:dynamodb:animus:0:table/orders\""));
    }

    #[test]
    fn decodes_describe_limits() {
        match decode_request("DynamoDB_20120810.DescribeLimits", b"{}").unwrap() {
            Operation::DescribeLimits => {}
            other => panic!("expected DescribeLimits, got {other:?}"),
        }
    }

    #[test]
    fn describe_limits_response_shape() {
        let body = describe_limits_response();
        assert!(body.contains("\"AccountMaxReadCapacityUnits\":80000"));
        assert!(body.contains("\"AccountMaxWriteCapacityUnits\":80000"));
        assert!(body.contains("\"TableMaxReadCapacityUnits\":40000"));
        assert!(body.contains("\"TableMaxWriteCapacityUnits\":40000"));
    }

    #[test]
    fn decodes_describe_endpoints() {
        match decode_request("DynamoDB_20120810.DescribeEndpoints", b"{}").unwrap() {
            Operation::DescribeEndpoints => {}
            other => panic!("expected DescribeEndpoints, got {other:?}"),
        }
    }

    #[test]
    fn describe_endpoints_response_shape() {
        let body = describe_endpoints_response("127.0.0.1:8000");
        assert!(body.contains("\"Address\":\"127.0.0.1:8000\""));
        assert!(body.contains("\"CachePeriodInMinutes\":1440"));
    }

    // --- Backups (ADR 0059, Train 1 PR④) -----------------------------------

    #[test]
    fn decodes_create_backup() {
        let body = br#"{"TableName":"orders","BackupName":"my-backup-1"}"#;
        match decode_request("DynamoDB_20120810.CreateBackup", body).unwrap() {
            Operation::CreateBackup { table, backup_name } => {
                assert_eq!(table, "orders");
                assert_eq!(backup_name, "my-backup-1");
            }
            other => panic!("expected CreateBackup, got {other:?}"),
        }
    }

    #[test]
    fn create_backup_rejects_missing_table_name() {
        let body = br#"{"BackupName":"my-backup-1"}"#;
        let err = decode_request("DynamoDB_20120810.CreateBackup", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn create_backup_rejects_missing_backup_name() {
        let body = br#"{"TableName":"orders"}"#;
        let err = decode_request("DynamoDB_20120810.CreateBackup", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn create_backup_rejects_a_too_short_backup_name() {
        let body = br#"{"TableName":"orders","BackupName":"ab"}"#;
        let err = decode_request("DynamoDB_20120810.CreateBackup", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn create_backup_rejects_an_illegal_character_in_backup_name() {
        let body = br#"{"TableName":"orders","BackupName":"bad name!"}"#;
        let err = decode_request("DynamoDB_20120810.CreateBackup", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn create_backup_accepts_the_boundary_backup_name_lengths() {
        for len in [3, 255] {
            let name = "a".repeat(len);
            let body = format!(r#"{{"TableName":"orders","BackupName":"{name}"}}"#);
            decode_request("DynamoDB_20120810.CreateBackup", body.as_bytes())
                .unwrap_or_else(|e| panic!("length {len} should be legal: {e:?}"));
        }
    }

    #[test]
    fn decodes_describe_backup() {
        let body = br#"{"BackupArn":"arn:aws:dynamodb:animus:0:table/orders/backup/abc"}"#;
        match decode_request("DynamoDB_20120810.DescribeBackup", body).unwrap() {
            Operation::DescribeBackup { backup_arn } => {
                assert_eq!(
                    backup_arn,
                    "arn:aws:dynamodb:animus:0:table/orders/backup/abc"
                );
            }
            other => panic!("expected DescribeBackup, got {other:?}"),
        }
    }

    #[test]
    fn describe_backup_rejects_missing_arn() {
        let err = decode_request("DynamoDB_20120810.DescribeBackup", b"{}").unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_delete_backup() {
        let body = br#"{"BackupArn":"arn:aws:dynamodb:animus:0:table/orders/backup/abc"}"#;
        match decode_request("DynamoDB_20120810.DeleteBackup", body).unwrap() {
            Operation::DeleteBackup { backup_arn } => {
                assert_eq!(
                    backup_arn,
                    "arn:aws:dynamodb:animus:0:table/orders/backup/abc"
                );
            }
            other => panic!("expected DeleteBackup, got {other:?}"),
        }
    }

    #[test]
    fn delete_backup_rejects_missing_arn() {
        let err = decode_request("DynamoDB_20120810.DeleteBackup", b"{}").unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn decodes_list_backups_with_every_field() {
        let body = br#"{
            "TableName":"orders",
            "Limit":5,
            "ExclusiveStartBackupArn":"arn:aws:dynamodb:animus:0:table/orders/backup/prev",
            "TimeRangeLowerBound":1000.5,
            "TimeRangeUpperBound":2000.0,
            "BackupType":"ALL"
        }"#;
        match decode_request("DynamoDB_20120810.ListBackups", body).unwrap() {
            Operation::ListBackups {
                table,
                limit,
                exclusive_start_backup_arn,
                time_range_lower_bound_ms,
                time_range_upper_bound_ms,
                backup_type,
            } => {
                assert_eq!(table.as_deref(), Some("orders"));
                assert_eq!(limit, Some(5));
                assert_eq!(
                    exclusive_start_backup_arn.as_deref(),
                    Some("arn:aws:dynamodb:animus:0:table/orders/backup/prev")
                );
                assert_eq!(time_range_lower_bound_ms, Some(1_000_500));
                assert_eq!(time_range_upper_bound_ms, Some(2_000_000));
                assert_eq!(backup_type, BackupTypeFilter::All);
            }
            other => panic!("expected ListBackups, got {other:?}"),
        }
    }

    #[test]
    fn decodes_list_backups_with_no_fields_defaults_to_user_type() {
        match decode_request("DynamoDB_20120810.ListBackups", b"{}").unwrap() {
            Operation::ListBackups {
                table,
                limit,
                exclusive_start_backup_arn,
                time_range_lower_bound_ms,
                time_range_upper_bound_ms,
                backup_type,
            } => {
                assert_eq!(table, None);
                assert_eq!(limit, None);
                assert_eq!(exclusive_start_backup_arn, None);
                assert_eq!(time_range_lower_bound_ms, None);
                assert_eq!(time_range_upper_bound_ms, None);
                assert_eq!(backup_type, BackupTypeFilter::User);
            }
            other => panic!("expected ListBackups, got {other:?}"),
        }
    }

    #[test]
    fn list_backups_rejects_an_unknown_backup_type() {
        let body = br#"{"BackupType":"NOT_A_TYPE"}"#;
        let err = decode_request("DynamoDB_20120810.ListBackups", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
    }

    #[test]
    fn backup_type_filter_matches_user_backup() {
        assert!(BackupTypeFilter::User.matches_user_backup());
        assert!(BackupTypeFilter::All.matches_user_backup());
        assert!(!BackupTypeFilter::System.matches_user_backup());
        assert!(!BackupTypeFilter::AwsBackup.matches_user_backup());
    }

    #[test]
    fn backup_arn_shape() {
        assert_eq!(
            backup_arn("orders", "abc123"),
            "arn:aws:dynamodb:animus:0:table/orders/backup/abc123"
        );
    }

    fn sample_backup_details() -> BackupDetails {
        BackupDetails {
            backup_arn: "arn:aws:dynamodb:animus:0:table/orders/backup/abc".into(),
            backup_name: "my-backup".into(),
            status: "AVAILABLE",
            creation_wall_ms: 1_723_000_000_500,
            size_bytes: 4096,
        }
    }

    #[test]
    fn create_backup_response_shape() {
        let body = create_backup_response(&sample_backup_details());
        assert!(body.contains("\"BackupDetails\""));
        assert!(
            body.contains("\"BackupArn\":\"arn:aws:dynamodb:animus:0:table/orders/backup/abc\"")
        );
        assert!(body.contains("\"BackupName\":\"my-backup\""));
        assert!(body.contains("\"BackupStatus\":\"AVAILABLE\""));
        assert!(body.contains("\"BackupType\":\"USER\""));
        assert!(body.contains("\"BackupSizeBytes\":4096"));
        assert!(body.contains("\"BackupCreationDateTime\":1723000000.5"));
    }

    #[test]
    fn backup_description_response_shape_with_indexes_stream_and_ttl() {
        let schema = TableSchema {
            partition_key: "pk".into(),
            sort_key: Some("sk".into()),
        };
        let indexes = vec![
            SecondaryIndex::Global(GlobalSecondaryIndex {
                name: "gsi1".into(),
                key_attribute: "gpk".into(),
                sort_attribute: None,
                projection: IndexProjection::All,
            }),
            SecondaryIndex::Local(LocalSecondaryIndex {
                name: "lsi1".into(),
                sort_attribute: "alt_sk".into(),
                projection: IndexProjection::All,
            }),
        ];
        let stream = StreamDescription {
            view_type: StreamViewType::NewAndOldImages,
            label: "L1".into(),
        };
        let ttl = TtlDescription {
            enabled: true,
            attribute_name: Some("expiresAt".into()),
        };
        let details = sample_backup_details();
        let body = backup_description_response(
            &details,
            "orders",
            &schema,
            &indexes,
            Some(&stream),
            Some(&ttl),
        );
        assert!(body.contains("\"BackupDescription\""));
        assert!(body.contains("\"SourceTableDetails\""));
        assert!(body.contains("\"TableName\":\"orders\""));
        assert!(body.contains("\"SourceTableFeatureDetails\""));
        assert!(body.contains("\"GlobalSecondaryIndexes\""));
        assert!(body.contains("\"IndexName\":\"gsi1\""));
        assert!(body.contains("\"LocalSecondaryIndexes\""));
        assert!(body.contains("\"IndexName\":\"lsi1\""));
        assert!(body.contains("\"StreamDescription\""));
        assert!(body.contains("\"StreamViewType\":\"NEW_AND_OLD_IMAGES\""));
        assert!(body.contains("\"TimeToLiveDescription\""));
        assert!(body.contains("\"TimeToLiveStatus\":\"ENABLED\""));
        assert!(body.contains("\"AttributeName\":\"expiresAt\""));
        // No IndexStatus/Backfilling — SourceTableFeatureDetails' own index
        // entries carry neither, unlike an ordinary TableDescription's.
        assert!(!body.contains("IndexStatus"));
        assert!(!body.contains("Backfilling"));
    }

    #[test]
    fn backup_description_response_with_no_stream_or_ttl_omits_both() {
        let schema = TableSchema {
            partition_key: "pk".into(),
            sort_key: None,
        };
        let details = sample_backup_details();
        let body = backup_description_response(&details, "orders", &schema, &[], None, None);
        assert!(!body.contains("StreamDescription"));
        assert!(!body.contains("TimeToLiveDescription"));
        assert!(!body.contains("GlobalSecondaryIndexes"));
        assert!(!body.contains("LocalSecondaryIndexes"));
    }

    #[test]
    fn backup_description_response_ttl_disabled_omits_ttl_description() {
        let schema = TableSchema {
            partition_key: "pk".into(),
            sort_key: None,
        };
        let ttl = TtlDescription {
            enabled: false,
            attribute_name: Some("expiresAt".into()),
        };
        let details = sample_backup_details();
        let body = backup_description_response(&details, "orders", &schema, &[], None, Some(&ttl));
        assert!(!body.contains("TimeToLiveDescription"));
    }

    fn summary(arn: &str) -> BackupSummary {
        BackupSummary {
            table: "orders".into(),
            details: BackupDetails {
                backup_arn: arn.into(),
                ..sample_backup_details()
            },
        }
    }

    #[test]
    fn paginate_backup_summaries_respects_limit_and_cursor() {
        let all: Vec<BackupSummary> = (0..5)
            .map(|i| {
                summary(&format!(
                    "arn:aws:dynamodb:animus:0:table/orders/backup/{i}"
                ))
            })
            .collect();
        let (page, last) = paginate_backup_summaries(&all, None, Some(2));
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].details.backup_arn, all[0].details.backup_arn);
        assert_eq!(page[1].details.backup_arn, all[1].details.backup_arn);
        assert_eq!(last.as_deref(), Some(all[1].details.backup_arn.as_str()));

        let (page2, last2) = paginate_backup_summaries(&all, last.as_deref(), Some(2));
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].details.backup_arn, all[2].details.backup_arn);
        assert_eq!(page2[1].details.backup_arn, all[3].details.backup_arn);
        assert!(last2.is_some());

        let (page3, last3) = paginate_backup_summaries(&all, last2.as_deref(), Some(2));
        assert_eq!(page3.len(), 1);
        assert_eq!(page3[0].details.backup_arn, all[4].details.backup_arn);
        assert_eq!(last3, None, "an untruncated final page carries no cursor");
    }

    #[test]
    fn paginate_backup_summaries_caps_limit_at_the_default() {
        let all: Vec<BackupSummary> = (0..3)
            .map(|i| {
                summary(&format!(
                    "arn:aws:dynamodb:animus:0:table/orders/backup/{i}"
                ))
            })
            .collect();
        let (page, last) = paginate_backup_summaries(&all, None, None);
        assert_eq!(page.len(), 3);
        assert_eq!(last, None);
    }

    #[test]
    fn list_backups_response_shape() {
        let all = vec![summary("arn:aws:dynamodb:animus:0:table/orders/backup/1")];
        let body = list_backups_response(
            &all,
            Some("arn:aws:dynamodb:animus:0:table/orders/backup/1"),
        );
        assert!(body.contains("\"BackupSummaries\""));
        assert!(body.contains("\"TableName\":\"orders\""));
        assert!(body.contains(
            "\"LastEvaluatedBackupArn\":\"arn:aws:dynamodb:animus:0:table/orders/backup/1\""
        ));
    }

    #[test]
    fn list_backups_response_omits_cursor_when_untruncated() {
        let body = list_backups_response(&[], None);
        assert!(!body.contains("LastEvaluatedBackupArn"));
    }

    /// `Select` is inferred when absent: a projection implies
    /// `SPECIFIC_ATTRIBUTES`, an index read defaults to the index's
    /// projection, and a plain table read to `ALL_ATTRIBUTES`.
    #[test]
    fn absent_select_is_inferred_from_the_rest_of_the_request() {
        let q = |body: &str| match decode_request("DynamoDB_20120810.Query", body.as_bytes())
            .expect("decodes")
        {
            Operation::Query { select, .. } => select,
            other => panic!("expected Query, got {other:?}"),
        };
        assert_eq!(
            q(r#"{"TableName":"t","KeyConditionExpression":"pk = :p",
                  "ExpressionAttributeValues":{":p":{"S":"a"}}}"#),
            Select::AllAttributes
        );
        assert_eq!(
            q(
                r#"{"TableName":"t","IndexName":"i","KeyConditionExpression":"pk = :p",
                  "ExpressionAttributeValues":{":p":{"S":"a"}}}"#
            ),
            Select::AllProjectedAttributes,
            "an index read defaults to its declared projection"
        );
        assert_eq!(
            q(r#"{"TableName":"t","KeyConditionExpression":"pk = :p",
                  "ExpressionAttributeValues":{":p":{"S":"a"}},
                  "ProjectionExpression":"a,b"}"#),
            Select::SpecificAttributes,
            "a projection implies SPECIFIC_ATTRIBUTES"
        );
    }

    /// Every explicit `Select` value round-trips, including `COUNT` — the one
    /// that changes the response shape.
    #[test]
    fn explicit_select_values_decode() {
        let q = |sel: &str| {
            let body = format!(
                r#"{{"TableName":"t","KeyConditionExpression":"pk = :p",
                     "ExpressionAttributeValues":{{":p":{{"S":"a"}}}},"Select":"{sel}"}}"#
            );
            match decode_request("DynamoDB_20120810.Query", body.as_bytes()).expect("decodes") {
                Operation::Query { select, .. } => select,
                other => panic!("expected Query, got {other:?}"),
            }
        };
        assert_eq!(q("ALL_ATTRIBUTES"), Select::AllAttributes);
        assert_eq!(q("COUNT"), Select::Count);
    }

    /// The four validations DynamoDB performs and this adapter previously
    /// ignored. Each was silently accepted before.
    #[test]
    fn select_is_validated_against_the_rest_of_the_request() {
        let err = |body: &str| {
            decode_request("DynamoDB_20120810.Query", body.as_bytes())
                .expect_err("must be rejected")
        };

        // SPECIFIC_ATTRIBUTES with nothing to select.
        err(r#"{"TableName":"t","KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{":p":{"S":"a"}},
                "Select":"SPECIFIC_ATTRIBUTES"}"#);

        // A projection contradicting a non-SPECIFIC Select.
        err(r#"{"TableName":"t","KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{":p":{"S":"a"}},
                "ProjectionExpression":"a","Select":"ALL_ATTRIBUTES"}"#);
        err(r#"{"TableName":"t","KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{":p":{"S":"a"}},
                "ProjectionExpression":"a","Select":"COUNT"}"#);

        // ALL_PROJECTED_ATTRIBUTES without an index to project.
        err(r#"{"TableName":"t","KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{":p":{"S":"a"}},
                "Select":"ALL_PROJECTED_ATTRIBUTES"}"#);

        // An unknown value is rejected rather than silently treated as default.
        err(r#"{"TableName":"t","KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{":p":{"S":"a"}},
                "Select":"EVERYTHING"}"#);
    }

    /// `Scan` decodes `Select` through the same path as `Query`.
    #[test]
    fn scan_decodes_select_too() {
        match decode_request(
            "DynamoDB_20120810.Scan",
            br#"{"TableName":"t","Select":"COUNT"}"#,
        )
        .expect("decodes")
        {
            Operation::Scan { select, .. } => assert_eq!(select, Select::Count),
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    /// Under `COUNT` the response carries no `Items` at all, while the counts
    /// and any cursor are exactly what the same page would otherwise report.
    #[test]
    fn count_select_omits_items_but_keeps_counts_and_cursor() {
        let item: Item = [("id".to_string(), s("k1"))].into_iter().collect();
        let items = vec![item.clone()];

        let full = select_response(Select::AllAttributes, &items, 4, Some(&item));
        assert!(full.contains("\"Items\""), "the normal shape keeps Items");

        let counted = select_response(Select::Count, &items, 4, Some(&item));
        assert!(
            !counted.contains("\"Items\""),
            "COUNT must not carry Items: {counted}"
        );
        assert!(counted.contains("\"Count\":1"), "{counted}");
        assert!(counted.contains("\"ScannedCount\":4"), "{counted}");
        assert!(
            counted.contains("\"LastEvaluatedKey\""),
            "a truncated COUNT page still paginates: {counted}"
        );
    }

    /// Regression (now with the operators supported): `>=` and `<=` were once
    /// cut by a naive `split_once('=')` into an equality against an attribute
    /// named `price >`. They must parse as the real operator against the whole
    /// attribute name — the property that bug violated.
    #[test]
    fn comparison_operators_parse_with_the_whole_attribute_name() {
        let cases = [
            (">=", Comparator::Ge),
            ("<=", Comparator::Le),
            (">", Comparator::Gt),
            ("<", Comparator::Lt),
            ("<>", Comparator::Ne),
            ("=", Comparator::Eq),
        ];
        for (op, expected) in cases {
            let body = format!(
                r#"{{"TableName":"t","FilterExpression":"price {op} :p",
                     "ExpressionAttributeValues":{{":p":{{"N":"5"}}}}}}"#
            );
            match decode_request("DynamoDB_20120810.Scan", body.as_bytes())
                .unwrap_or_else(|e| panic!("`{op}` must decode: {e:?}"))
            {
                Operation::Scan { filter, .. } => assert_eq!(
                    filter,
                    Some(ConditionExpression::Compare(
                        "price".into(),
                        expected,
                        AttributeValue::N("5".into())
                    )),
                    "`{op}` must keep the whole attribute name and its own operator"
                ),
                other => panic!("expected Scan, got {other:?}"),
            }
        }
    }

    /// Regression: `#alias` was resolved in `ProjectionExpression` but not in
    /// `FilterExpression`/`ConditionExpression`, so `#p = :v` became an
    /// equality against an attribute literally named `#p` — always false, and
    /// silently so. Aliases are mandatory for DynamoDB's reserved words, so
    /// this hit ordinary schemas.
    #[test]
    fn expression_attribute_names_resolve_in_predicates() {
        let body = r##"{"TableName":"t","FilterExpression":"#p = :v",
            "ExpressionAttributeNames":{"#p":"price"},
            "ExpressionAttributeValues":{":v":{"N":"5"}}}"##;
        match decode_request("DynamoDB_20120810.Scan", body.as_bytes()).expect("decodes") {
            Operation::Scan { filter, .. } => assert_eq!(
                filter,
                Some(ConditionExpression::Compare(
                    "price".into(),
                    Comparator::Eq,
                    AttributeValue::N("5".into())
                )),
                "the alias must resolve to the real attribute name"
            ),
            other => panic!("expected Scan, got {other:?}"),
        }

        let exists = r##"{"TableName":"t","FilterExpression":"attribute_exists(#p)",
            "ExpressionAttributeNames":{"#p":"price"}}"##;
        match decode_request("DynamoDB_20120810.Scan", exists.as_bytes()).expect("decodes") {
            Operation::Scan { filter, .. } => assert_eq!(
                filter,
                Some(ConditionExpression::AttributeExists("price".into())),
                "aliases resolve inside the function forms too"
            ),
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    /// Regression: the key condition's attribute name was discarded, so the
    /// edge could not tell `pk = :v` from `notthekey = :v`. It is now carried
    /// (alias-resolved) for the edge to check against the catalog.
    #[test]
    fn key_condition_carries_its_attribute_names() {
        let body = r##"{"TableName":"t",
            "KeyConditionExpression":"#k = :p AND #s = :s",
            "ExpressionAttributeNames":{"#k":"pk","#s":"sk"},
            "ExpressionAttributeValues":{":p":{"S":"a"},":s":{"S":"b"}}}"##;
        match decode_request("DynamoDB_20120810.Query", body.as_bytes()).expect("decodes") {
            Operation::Query {
                partition_attr,
                sort_attr,
                ..
            } => {
                assert_eq!(partition_attr, "pk", "alias-resolved partition key name");
                assert_eq!(sort_attr.as_deref(), Some("sk"), "and the sort key name");
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    /// Regression: a sort-key **range** — the main reason to have a sort key —
    /// used to be truncated into an equality, silently narrowing the result
    /// set (and, before issue #373 closed the gap, rejected outright rather
    /// than truncated). Each of the four range comparators now decodes to its
    /// own real [`SortKeyCondition::Compare`], never collapsing onto `Eq`.
    #[test]
    fn sort_key_ranges_decode_to_their_own_comparator_not_an_equality() {
        for (op, expected) in [
            (">=", Comparator::Ge),
            ("<=", Comparator::Le),
            (">", Comparator::Gt),
            ("<", Comparator::Lt),
        ] {
            let body = format!(
                r#"{{"TableName":"t","KeyConditionExpression":"pk = :p AND sk {op} :s",
                     "ExpressionAttributeValues":{{":p":{{"S":"a"}},":s":{{"S":"b"}}}}}}"#
            );
            match decode_request("DynamoDB_20120810.Query", body.as_bytes())
                .unwrap_or_else(|e| panic!("sort-key `{op}` must decode: {e:?}"))
            {
                Operation::Query {
                    sort_attr,
                    sort_condition,
                    ..
                } => {
                    assert_eq!(sort_attr.as_deref(), Some("sk"), "for `{op}`");
                    assert_eq!(
                        sort_condition,
                        Some(SortKeyCondition::Compare(expected, s("b"))),
                        "for `{op}` — must not silently collapse onto `=`"
                    );
                }
                other => panic!("expected Query, got {other:?}"),
            }
        }
        // BETWEEN and begins_with still work, and carry the sort attribute name.
        let between = br#"{"TableName":"t",
            "KeyConditionExpression":"pk = :p AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":p":{"S":"a"},":lo":{"S":"b"},":hi":{"S":"c"}}}"#;
        match decode_request("DynamoDB_20120810.Query", between).expect("decodes") {
            Operation::Query {
                sort_attr,
                sort_condition,
                ..
            } => {
                assert_eq!(sort_attr.as_deref(), Some("sk"));
                assert!(matches!(
                    sort_condition,
                    Some(SortKeyCondition::Between(..))
                ));
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    /// A partition key condition must be an equality; `pk >= :v` is rejected
    /// rather than silently accepted as one.
    #[test]
    fn partition_key_condition_must_be_an_equality() {
        let body = br#"{"TableName":"t","KeyConditionExpression":"pk >= :p",
             "ExpressionAttributeValues":{":p":{"S":"a"}}}"#;
        let err = decode_request("DynamoDB_20120810.Query", body).expect_err("must be rejected");
        assert_eq!(err.code, "ValidationException");
    }

    /// Every new predicate form decodes, including the ones whose argument
    /// lists contain commas the comparator split must not cut through.
    #[test]
    fn the_full_predicate_surface_decodes() {
        let decode_filter = |frag: &str| {
            let body = format!(
                r##"{{"TableName":"t","FilterExpression":"{frag}",
                     "ExpressionAttributeNames":{{"#a":"attr"}},
                     "ExpressionAttributeValues":{{
                        ":v":{{"N":"5"}},":lo":{{"N":"1"}},":hi":{{"N":"9"}},
                        ":s":{{"S":"pre"}},":t":{{"S":"S"}}}}}}"##
            );
            match decode_request("DynamoDB_20120810.Scan", body.as_bytes())
                .unwrap_or_else(|e| panic!("`{frag}` must decode: {e:?}"))
            {
                Operation::Scan { filter, .. } => filter.expect("a filter"),
                other => panic!("expected Scan, got {other:?}"),
            }
        };

        assert_eq!(
            decode_filter("a BETWEEN :lo AND :hi"),
            ConditionExpression::Between(
                "a".into(),
                AttributeValue::N("1".into()),
                AttributeValue::N("9".into())
            )
        );
        assert_eq!(
            decode_filter("a IN (:lo, :hi)"),
            ConditionExpression::In(
                "a".into(),
                vec![AttributeValue::N("1".into()), AttributeValue::N("9".into())]
            )
        );
        assert_eq!(
            decode_filter("begins_with(a, :s)"),
            ConditionExpression::BeginsWith("a".into(), AttributeValue::S("pre".into()))
        );
        assert_eq!(
            decode_filter("contains(a, :s)"),
            ConditionExpression::Contains("a".into(), AttributeValue::S("pre".into()))
        );
        assert_eq!(
            decode_filter("attribute_type(a, :t)"),
            ConditionExpression::AttributeType("a".into(), "S".into())
        );
        assert_eq!(
            decode_filter("size(a) > :v"),
            ConditionExpression::Size("a".into(), Comparator::Gt, AttributeValue::N("5".into())),
            "size() is the one form with a function on the left of a comparison"
        );
        // Aliases resolve in every form, not just the comparison one.
        assert_eq!(
            decode_filter("begins_with(#a, :s)"),
            ConditionExpression::BeginsWith("attr".into(), AttributeValue::S("pre".into()))
        );
        assert_eq!(
            decode_filter("#a BETWEEN :lo AND :hi"),
            ConditionExpression::Between(
                "attr".into(),
                AttributeValue::N("1".into()),
                AttributeValue::N("9".into())
            )
        );
    }

    /// Malformed forms are rejected rather than half-parsed.
    #[test]
    fn malformed_predicate_forms_are_rejected() {
        for frag in [
            "a IN :v",           // unparenthesised
            "a IN ()",           // empty list
            "a IN (:v,)",        // trailing empty element
            "attribute_type(a)", // missing the type code
            "begins_with(a)",    // missing the prefix
            "a BETWEEN :lo",     // missing AND :hi
        ] {
            let body = format!(
                r#"{{"TableName":"t","FilterExpression":"{frag}",
                     "ExpressionAttributeValues":{{":v":{{"N":"5"}},":lo":{{"N":"1"}}}}}}"#
            );
            assert!(
                decode_request("DynamoDB_20120810.Scan", body.as_bytes()).is_err(),
                "`{frag}` must be rejected, not half-parsed"
            );
        }
    }

    /// An unknown `attribute_type` code is rejected rather than silently
    /// matching nothing.
    #[test]
    fn unknown_attribute_type_code_is_rejected() {
        let body = br#"{"TableName":"t","FilterExpression":"a attribute_type(a, :t)",
             "ExpressionAttributeValues":{":t":{"S":"STRING"}}}"#;
        assert!(decode_request("DynamoDB_20120810.Scan", body).is_err());
    }

    /// Precedence: `NOT` binds tightest, then `AND`, then `OR` — so
    /// `a OR b AND c` is `a OR (b AND c)`, not `(a OR b) AND c`.
    #[test]
    fn boolean_precedence_is_not_then_and_then_or() {
        let f = |frag: &str| {
            let body = format!(
                r#"{{"TableName":"t","FilterExpression":"{frag}",
                     "ExpressionAttributeValues":{{":a":{{"N":"1"}},":b":{{"N":"2"}},":c":{{"N":"3"}}}}}}"#
            );
            match decode_request("DynamoDB_20120810.Scan", body.as_bytes())
                .unwrap_or_else(|e| panic!("`{frag}` must decode: {e:?}"))
            {
                Operation::Scan { filter, .. } => filter.expect("a filter"),
                other => panic!("expected Scan, got {other:?}"),
            }
        };
        let eq = |name: &str, ph: &str| {
            ConditionExpression::Compare(name.into(), Comparator::Eq, AttributeValue::N(ph.into()))
        };

        assert_eq!(
            f("a = :a OR b = :b AND c = :c"),
            ConditionExpression::Or(
                Box::new(eq("a", "1")),
                Box::new(ConditionExpression::And(
                    Box::new(eq("b", "2")),
                    Box::new(eq("c", "3"))
                ))
            ),
            "AND binds tighter than OR"
        );
        assert_eq!(
            f("NOT a = :a AND b = :b"),
            ConditionExpression::And(
                Box::new(ConditionExpression::Not(Box::new(eq("a", "1")))),
                Box::new(eq("b", "2"))
            ),
            "NOT binds tighter than AND"
        );
        assert_eq!(
            f("(a = :a OR b = :b) AND c = :c"),
            ConditionExpression::And(
                Box::new(ConditionExpression::Or(
                    Box::new(eq("a", "1")),
                    Box::new(eq("b", "2"))
                )),
                Box::new(eq("c", "3"))
            ),
            "parentheses override precedence"
        );
        // Left-associative chains.
        assert_eq!(
            f("a = :a AND b = :b AND c = :c"),
            ConditionExpression::And(
                Box::new(ConditionExpression::And(
                    Box::new(eq("a", "1")),
                    Box::new(eq("b", "2"))
                )),
                Box::new(eq("c", "3"))
            )
        );
    }

    /// The trap: `BETWEEN :lo AND :hi` contains an `AND` that belongs to the
    /// term, not to the combinator. Splitting on it would produce nonsense.
    #[test]
    fn between_s_own_and_is_not_a_combinator() {
        let body = br#"{"TableName":"t",
             "FilterExpression":"a BETWEEN :lo AND :hi AND b = :b",
             "ExpressionAttributeValues":{":lo":{"N":"1"},":hi":{"N":"9"},":b":{"N":"2"}}}"#;
        match decode_request("DynamoDB_20120810.Scan", body).expect("decodes") {
            Operation::Scan { filter, .. } => assert_eq!(
                filter,
                Some(ConditionExpression::And(
                    Box::new(ConditionExpression::Between(
                        "a".into(),
                        AttributeValue::N("1".into()),
                        AttributeValue::N("9".into())
                    )),
                    Box::new(ConditionExpression::Compare(
                        "b".into(),
                        Comparator::Eq,
                        AttributeValue::N("2".into())
                    ))
                )),
                "the first AND closes the BETWEEN; only the second joins terms"
            ),
            other => panic!("expected Scan, got {other:?}"),
        }

        // A bare BETWEEN must still parse as one term.
        let solo = br#"{"TableName":"t","FilterExpression":"a BETWEEN :lo AND :hi",
             "ExpressionAttributeValues":{":lo":{"N":"1"},":hi":{"N":"9"}}}"#;
        match decode_request("DynamoDB_20120810.Scan", solo).expect("decodes") {
            Operation::Scan { filter, .. } => {
                assert!(matches!(filter, Some(ConditionExpression::Between(..))));
            }
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    /// A combinator inside a parenthesised group must not be split at the top
    /// level, and `(a) OR (b)` is not one group.
    #[test]
    fn parenthesised_groups_are_respected() {
        let body = br#"{"TableName":"t",
             "FilterExpression":"(a = :a) OR (b = :b)",
             "ExpressionAttributeValues":{":a":{"N":"1"},":b":{"N":"2"}}}"#;
        match decode_request("DynamoDB_20120810.Scan", body).expect("decodes") {
            Operation::Scan { filter, .. } => assert!(
                matches!(filter, Some(ConditionExpression::Or(..))),
                "`(a) OR (b)` is a disjunction, not a single group: {filter:?}"
            ),
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    /// `ADD` seeds an absent attribute, increments a number, and unions a set.
    #[test]
    fn add_seeds_increments_and_unions() {
        let n = |v: &str| AttributeValue::N(v.into());
        let ss = |v: &[&str]| AttributeValue::SS(v.iter().map(|s| (*s).to_string()).collect());

        // Absent -> seeded. This is the counter-on-a-new-row case.
        let out =
            apply_update(Item::new(), &[UpdateAction::Add("c".into(), n("1"))]).expect("applies");
        assert_eq!(out.get("c"), Some(&n("1")));

        // Present -> incremented, exactly.
        let mut item = Item::new();
        item.insert("c".into(), n("41"));
        let out = apply_update(item, &[UpdateAction::Add("c".into(), n("1"))]).expect("applies");
        assert_eq!(out.get("c"), Some(&n("42")));

        // Sets union and stay sorted/deduplicated.
        let mut item = Item::new();
        item.insert("t".into(), ss(&["a", "b"]));
        let out =
            apply_update(item, &[UpdateAction::Add("t".into(), ss(&["b", "c"]))]).expect("applies");
        assert_eq!(
            out.get("t"),
            Some(&ss(&["a", "b", "c"])),
            "union, deduplicated"
        );
    }

    /// `DELETE` subtracts set members, and emptying a set removes the
    /// attribute — DynamoDB does not store empty sets.
    #[test]
    fn delete_subtracts_and_drops_an_emptied_set() {
        let ss = |v: &[&str]| AttributeValue::SS(v.iter().map(|s| (*s).to_string()).collect());

        let mut item = Item::new();
        item.insert("t".into(), ss(&["a", "b", "c"]));
        let out =
            apply_update(item, &[UpdateAction::Delete("t".into(), ss(&["b"]))]).expect("applies");
        assert_eq!(out.get("t"), Some(&ss(&["a", "c"])));

        let mut item = Item::new();
        item.insert("t".into(), ss(&["a"]));
        let out =
            apply_update(item, &[UpdateAction::Delete("t".into(), ss(&["a"]))]).expect("applies");
        assert!(
            !out.contains_key("t"),
            "an emptied set is removed, not stored as an empty set: {out:?}"
        );

        // Deleting from an absent attribute is a no-op, not an error.
        let out = apply_update(Item::new(), &[UpdateAction::Delete("t".into(), ss(&["a"]))])
            .expect("no-op");
        assert!(out.is_empty());
    }

    /// A typed mismatch is an error, never a silently skipped action — the
    /// caller must not believe an update applied when it did not.
    #[test]
    fn add_and_delete_reject_type_mismatches() {
        let mut item = Item::new();
        item.insert("s".into(), AttributeValue::S("text".into()));
        assert!(
            apply_update(
                item.clone(),
                &[UpdateAction::Add("s".into(), AttributeValue::N("1".into()))]
            )
            .is_err(),
            "ADD a number to a string must be rejected"
        );
        assert!(
            apply_update(
                item,
                &[UpdateAction::Delete(
                    "s".into(),
                    AttributeValue::SS(vec!["a".into()])
                )]
            )
            .is_err(),
            "DELETE a set from a string must be rejected"
        );
    }

    /// `ADD`/`DELETE` parse with their space-separated `attr :value` shape,
    /// alongside the `=`-shaped SET, and resolve `#alias`.
    #[test]
    fn add_and_delete_clauses_parse() {
        let body = r##"{"TableName":"t","Key":{"pk":{"S":"a"}},
            "UpdateExpression":"SET #s = :s ADD #c :new DELETE #t :rm",
            "ExpressionAttributeNames":{"#s":"name","#c":"tags2","#t":"tags"},
            "ExpressionAttributeValues":{":s":{"S":"x"},":new":{"SS":["a"]},
                ":rm":{"SS":["old"]}}}"##;
        match decode_request("DynamoDB_20120810.UpdateItem", body.as_bytes()).expect("decodes") {
            Operation::UpdateItem { actions, .. } => {
                assert_eq!(
                    actions,
                    vec![
                        UpdateAction::Set(
                            "name".into(),
                            UpdateExpr::value(AttributeValue::S("x".into()))
                        ),
                        UpdateAction::Add("tags2".into(), AttributeValue::SS(vec!["a".into()])),
                        UpdateAction::Delete("tags".into(), AttributeValue::SS(vec!["old".into()])),
                    ],
                    "all three clause shapes, with aliases resolved"
                );
            }
            other => panic!("expected UpdateItem, got {other:?}"),
        }
    }

    /// A malformed ADD/DELETE clause is rejected rather than half-parsed.
    #[test]
    fn malformed_add_clauses_are_rejected() {
        for expr in ["ADD c", "DELETE t"] {
            let body = format!(
                r#"{{"TableName":"t","Key":{{"pk":{{"S":"a"}}}},
                     "UpdateExpression":"{expr}",
                     "ExpressionAttributeValues":{{":one":{{"N":"1"}}}}}}"#
            );
            assert!(
                decode_request("DynamoDB_20120810.UpdateItem", body.as_bytes()).is_err(),
                "`{expr}` needs a value operand"
            );
        }
    }

    /// Numeric `ADD` decodes. Its safety comes from the write path — the
    /// service does not re-apply a non-idempotent write, and a `KindBatch`
    /// records what it did so an applied write is acknowledged even when a
    /// concurrent update overwrites it — not from refusing it here.
    #[test]
    fn numeric_add_decodes() {
        let body = br#"{"TableName":"t","Key":{"pk":{"S":"a"}},
             "UpdateExpression":"ADD c :one",
             "ExpressionAttributeValues":{":one":{"N":"1"}}}"#;
        match decode_request("DynamoDB_20120810.UpdateItem", body).expect("decodes") {
            Operation::UpdateItem { actions, .. } => assert_eq!(
                actions,
                vec![UpdateAction::Add("c".into(), AttributeValue::N("1".into()))]
            ),
            other => panic!("expected UpdateItem, got {other:?}"),
        }
    }

    /// `ADD` still rejects an operand that is neither a number nor a set.
    #[test]
    fn add_rejects_a_non_numeric_non_set_operand() {
        let body = br#"{"TableName":"t","Key":{"pk":{"S":"a"}},
             "UpdateExpression":"ADD c :s",
             "ExpressionAttributeValues":{":s":{"S":"x"}}}"#;
        assert!(decode_request("DynamoDB_20120810.UpdateItem", body).is_err());
    }

    /// An operand that is neither a number nor a set is refused for both
    /// clauses (`DELETE` additionally requires a set).
    #[test]
    fn add_and_delete_require_set_operands() {
        for expr in ["ADD c :s", "DELETE c :s"] {
            let body = format!(
                r#"{{"TableName":"t","Key":{{"pk":{{"S":"a"}}}},
                     "UpdateExpression":"{expr}",
                     "ExpressionAttributeValues":{{":s":{{"S":"x"}}}}}}"#
            );
            assert!(
                decode_request("DynamoDB_20120810.UpdateItem", body.as_bytes()).is_err(),
                "`{expr}` must require a set operand"
            );
        }
    }

    /// `BatchGetItem` decodes per-table keys, with the projection and
    /// consistency setting scoped to the table rather than to a key.
    #[test]
    fn batch_get_decodes_per_table_specs() {
        let body = br#"{"RequestItems":{
            "t1":{"Keys":[{"id":{"S":"a"}},{"id":{"S":"b"}}],
                  "ProjectionExpression":"id,v","ConsistentRead":true},
            "t2":{"Keys":[{"id":{"S":"c"}}]}}}"#;
        match decode_request("DynamoDB_20120810.BatchGetItem", body).expect("decodes") {
            Operation::BatchGetItem { mut requests } => {
                requests.sort_by(|a, b| a.table.cmp(&b.table));
                assert_eq!(requests.len(), 2);
                assert_eq!(requests[0].table, "t1");
                assert_eq!(requests[0].keys.len(), 2);
                assert_eq!(
                    requests[0].projection,
                    Some(Projection(vec![field("id"), field("v")]))
                );
                assert!(requests[0].consistent_read);
                assert_eq!(requests[1].table, "t2");
                assert_eq!(requests[1].keys.len(), 1);
                assert_eq!(requests[1].projection, None);
                assert!(
                    !requests[1].consistent_read,
                    "ConsistentRead defaults to false"
                );
            }
            other => panic!("expected BatchGetItem, got {other:?}"),
        }
    }

    /// Malformed request shapes are rejected rather than silently read as empty.
    #[test]
    fn batch_get_rejects_malformed_requests() {
        for body in [
            &br#"{}"#[..],
            &br#"{"RequestItems":{}}"#[..],
            &br#"{"RequestItems":{"t":{}}}"#[..],
            &br#"{"RequestItems":{"t":{"Keys":[]}}}"#[..],
            &br#"{"RequestItems":{"t":{"Keys":["nope"]}}}"#[..],
        ] {
            assert!(
                decode_request("DynamoDB_20120810.BatchGetItem", body).is_err(),
                "must reject {}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// The response groups items by table and always reports an empty
    /// `UnprocessedKeys`, since every requested key is read before responding.
    #[test]
    fn batch_get_response_groups_by_table() {
        let mut a = Item::new();
        a.insert("id".into(), s("a"));
        let body =
            batch_get_response(&[("t1".to_string(), vec![a]), ("t2".to_string(), Vec::new())]);
        assert!(body.contains(r#""t1":[{"id":{"S":"a"}}]"#), "{body}");
        assert!(
            body.contains(r#""t2":[]"#),
            "a table with no hits is an empty list: {body}"
        );
        assert!(body.contains(r#""UnprocessedKeys":{}"#), "{body}");
    }

    /// `Segment`/`TotalSegments` decode together and are validated.
    #[test]
    fn scan_segment_decodes_and_validates() {
        let ok = br#"{"TableName":"t","Segment":1,"TotalSegments":4}"#;
        match decode_request("DynamoDB_20120810.Scan", ok).expect("decodes") {
            Operation::Scan { segment, .. } => assert_eq!(
                segment,
                Some(ScanSegment {
                    segment: 1,
                    total: 4
                })
            ),
            other => panic!("expected Scan, got {other:?}"),
        }

        for body in [
            // One without the other is a client bug, not a whole-table scan.
            &br#"{"TableName":"t","Segment":0}"#[..],
            &br#"{"TableName":"t","TotalSegments":4}"#[..],
            // Out of range, and a zero split.
            &br#"{"TableName":"t","Segment":4,"TotalSegments":4}"#[..],
            &br#"{"TableName":"t","Segment":0,"TotalSegments":0}"#[..],
            &br#"{"TableName":"t","Segment":-1,"TotalSegments":4}"#[..],
        ] {
            assert!(
                decode_request("DynamoDB_20120810.Scan", body).is_err(),
                "must reject {}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// `UPDATED_OLD`/`UPDATED_NEW` report only what changed, from the side
    /// that holds the value.
    #[test]
    fn updated_return_values_report_only_the_diff() {
        let mut old = Item::new();
        old.insert("id".into(), s("k")); // key: never changes
        old.insert("keep".into(), s("same")); // untouched
        old.insert("edit".into(), s("before"));
        old.insert("gone".into(), s("dropped"));

        let mut new = Item::new();
        new.insert("id".into(), s("k"));
        new.insert("keep".into(), s("same"));
        new.insert("edit".into(), s("after"));
        new.insert("fresh".into(), s("created"));

        let body = update_response(
            UpdateReturnValues::UpdatedOld,
            Some(&old),
            Some(&new),
            None,
            None,
        );
        assert!(body.contains(r#""edit":{"S":"before"}"#), "changed: {body}");
        assert!(
            body.contains(r#""gone":{"S":"dropped"}"#),
            "removed has an old value: {body}"
        );
        assert!(
            !body.contains("fresh"),
            "created has no previous value: {body}"
        );
        assert!(!body.contains("keep"), "untouched is not reported: {body}");
        assert!(!body.contains(r#""id""#), "the key never changes: {body}");

        let body = update_response(
            UpdateReturnValues::UpdatedNew,
            Some(&old),
            Some(&new),
            None,
            None,
        );
        assert!(body.contains(r#""edit":{"S":"after"}"#), "changed: {body}");
        assert!(
            body.contains(r#""fresh":{"S":"created"}"#),
            "created has a new value: {body}"
        );
        assert!(!body.contains("gone"), "removed has no new value: {body}");
        assert!(!body.contains("keep"), "{body}");
    }

    /// When nothing changed, `Attributes` is omitted rather than sent empty.
    #[test]
    fn updated_return_values_omit_attributes_when_nothing_changed() {
        let mut item = Item::new();
        item.insert("id".into(), s("k"));
        for rv in [
            UpdateReturnValues::UpdatedOld,
            UpdateReturnValues::UpdatedNew,
        ] {
            assert_eq!(
                update_response(rv, Some(&item), Some(&item), None, None),
                empty_response(),
                "an unchanged item reports no Attributes"
            );
        }
    }

    /// Both new values decode.
    #[test]
    fn updated_return_values_decode() {
        for (raw, want) in [
            ("UPDATED_OLD", UpdateReturnValues::UpdatedOld),
            ("UPDATED_NEW", UpdateReturnValues::UpdatedNew),
        ] {
            let body = format!(
                r#"{{"TableName":"t","Key":{{"pk":{{"S":"a"}}}},
                     "UpdateExpression":"SET v = :v","ReturnValues":"{raw}",
                     "ExpressionAttributeValues":{{":v":{{"S":"x"}}}}}}"#
            );
            match decode_request("DynamoDB_20120810.UpdateItem", body.as_bytes()).expect("decodes")
            {
                Operation::UpdateItem { return_values, .. } => assert_eq!(return_values, want),
                other => panic!("expected UpdateItem, got {other:?}"),
            }
        }
    }

    /// The `capacity` field of whichever operation `body` decodes to under
    /// `target`, so each test reads as the one assertion it is making.
    fn decoded_capacity(target: &str, body: &[u8]) -> ReturnConsumedCapacity {
        match decode_request(target, body).expect("decodes") {
            Operation::PutItem { capacity, .. }
            | Operation::GetItem { capacity, .. }
            | Operation::DeleteItem { capacity, .. }
            | Operation::UpdateItem { capacity, .. } => capacity,
            other => panic!("no capacity on {other:?}"),
        }
    }

    #[test]
    fn return_consumed_capacity_defaults_to_none_on_every_item_operation() {
        // Absent is `NONE`: a request that never mentioned capacity must get a
        // response that never mentions it either.
        for (target, body) in [
            (
                "DynamoDB_20120810.PutItem",
                &br#"{"TableName":"t","Item":{"id":{"S":"k"}}}"#[..],
            ),
            (
                "DynamoDB_20120810.GetItem",
                &br#"{"TableName":"t","Key":{"id":{"S":"k"}}}"#[..],
            ),
            (
                "DynamoDB_20120810.DeleteItem",
                &br#"{"TableName":"t","Key":{"id":{"S":"k"}}}"#[..],
            ),
            (
                "DynamoDB_20120810.UpdateItem",
                &br#"{"TableName":"t","Key":{"id":{"S":"k"}},
                      "UpdateExpression":"SET a = :v",
                      "ExpressionAttributeValues":{":v":{"S":"x"}}}"#[..],
            ),
        ] {
            assert_eq!(
                decoded_capacity(target, body),
                ReturnConsumedCapacity::None,
                "{target} should default to NONE"
            );
            // An explicit `null` is the same as absent.
            let explicit_null = String::from_utf8(body.to_vec()).expect("utf8").replacen(
                '{',
                r#"{"ReturnConsumedCapacity":null,"#,
                1,
            );
            assert_eq!(
                decoded_capacity(target, explicit_null.as_bytes()),
                ReturnConsumedCapacity::None,
                "{target} should treat an explicit null as NONE"
            );
        }
    }

    #[test]
    fn return_consumed_capacity_decodes_each_level() {
        for (text, expected) in [
            ("NONE", ReturnConsumedCapacity::None),
            ("TOTAL", ReturnConsumedCapacity::Total),
            ("INDEXES", ReturnConsumedCapacity::Indexes),
        ] {
            let body = format!(
                r#"{{"TableName":"t","Key":{{"id":{{"S":"k"}}}},
                     "ReturnConsumedCapacity":"{text}"}}"#
            );
            assert_eq!(
                decoded_capacity("DynamoDB_20120810.GetItem", body.as_bytes()),
                expected
            );
        }
    }

    #[test]
    fn a_bad_return_consumed_capacity_is_rejected_rather_than_ignored() {
        // Silently downgrading an unrecognised level to `NONE` would drop the
        // report a client asked for without telling it — the failure mode this
        // series exists to remove.
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
                        "ReturnConsumedCapacity":"SOMETIMES"}"#;
        let err = decode_request("DynamoDB_20120810.GetItem", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
        assert!(err.message.contains("SOMETIMES"), "{}", err.message);
        assert!(err.message.contains("INDEXES"), "{}", err.message);

        let wrong_type = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
                              "ReturnConsumedCapacity":true}"#;
        let err = decode_request("DynamoDB_20120810.GetItem", wrong_type).unwrap_err();
        assert_eq!(err.code, "ValidationException");
        assert!(err.message.contains("must be a string"), "{}", err.message);
    }

    #[test]
    fn a_response_carries_consumed_capacity_only_when_one_was_built() {
        let cc = ConsumedCapacity::table_only("t", 1.0, ReturnConsumedCapacity::Total);
        // Present even when the body is otherwise empty: a `PutItem` with
        // `ReturnValues: NONE` still owes the caller its capacity report.
        let with = write_response(ReturnValues::None, None, Some(&cc), None);
        let parsed: Value = serde_json::from_str(&with).expect("json");
        assert_eq!(parsed["ConsumedCapacity"]["TableName"], "t");
        assert_eq!(parsed["ConsumedCapacity"]["CapacityUnits"], 1.0);
        assert!(parsed.get("Attributes").is_none());

        assert_eq!(write_response(ReturnValues::None, None, None, None), "{}");
        assert_eq!(get_item_response(None, None), "{}");
        assert_eq!(
            update_response(UpdateReturnValues::None, None, None, None, None),
            "{}"
        );
    }

    #[test]
    fn return_item_collection_metrics_decodes_and_defaults() {
        let put = |extra: &str| {
            let body = format!(r#"{{"TableName":"t","Item":{{"id":{{"S":"k"}}}}{extra}}}"#);
            match decode_request("DynamoDB_20120810.PutItem", body.as_bytes()) {
                Ok(Operation::PutItem { metrics, .. }) => Ok(metrics),
                Ok(other) => panic!("expected PutItem, got {other:?}"),
                Err(e) => Err(e),
            }
        };
        assert_eq!(put("").unwrap(), ReturnItemCollectionMetrics::None);
        assert_eq!(
            put(r#","ReturnItemCollectionMetrics":null"#).unwrap(),
            ReturnItemCollectionMetrics::None
        );
        assert_eq!(
            put(r#","ReturnItemCollectionMetrics":"NONE""#).unwrap(),
            ReturnItemCollectionMetrics::None
        );
        assert_eq!(
            put(r#","ReturnItemCollectionMetrics":"SIZE""#).unwrap(),
            ReturnItemCollectionMetrics::Size
        );

        // Rejected, not silently downgraded.
        let err = put(r#","ReturnItemCollectionMetrics":"SOMETIMES""#).unwrap_err();
        assert_eq!(err.code, "ValidationException");
        assert!(err.message.contains("SOMETIMES"), "{}", err.message);
        let err = put(r#","ReturnItemCollectionMetrics":7"#).unwrap_err();
        assert!(err.message.contains("must be a string"), "{}", err.message);
    }

    #[test]
    fn only_the_write_operations_carry_item_collection_metrics() {
        // `GetItem` has no such field and its builder takes no such argument —
        // a read never touches an item collection. This pins the decode side
        // of that: the field is simply not part of a `GetItem`.
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
                        "ReturnItemCollectionMetrics":"SIZE"}"#;
        // Accepted and ignored rather than rejected, matching how DynamoDB
        // treats a field that does not apply to the operation.
        assert!(matches!(
            decode_request("DynamoDB_20120810.GetItem", body),
            Ok(Operation::GetItem { .. })
        ));

        for target in [
            "DynamoDB_20120810.PutItem",
            "DynamoDB_20120810.DeleteItem",
            "DynamoDB_20120810.UpdateItem",
        ] {
            let body = match target {
                "DynamoDB_20120810.PutItem" => {
                    r#"{"TableName":"t","Item":{"id":{"S":"k"}},
                        "ReturnItemCollectionMetrics":"SIZE"}"#
                }
                "DynamoDB_20120810.DeleteItem" => {
                    r#"{"TableName":"t","Key":{"id":{"S":"k"}},
                        "ReturnItemCollectionMetrics":"SIZE"}"#
                }
                _ => {
                    r#"{"TableName":"t","Key":{"id":{"S":"k"}},
                        "UpdateExpression":"SET a = :v",
                        "ExpressionAttributeValues":{":v":{"S":"x"}},
                        "ReturnItemCollectionMetrics":"SIZE"}"#
                }
            };
            let decoded = decode_request(target, body.as_bytes()).expect("decodes");
            let metrics = match decoded {
                Operation::PutItem { metrics, .. }
                | Operation::DeleteItem { metrics, .. }
                | Operation::UpdateItem { metrics, .. } => metrics,
                other => panic!("unexpected {other:?}"),
            };
            assert_eq!(metrics, ReturnItemCollectionMetrics::Size, "{target}");
        }
    }

    #[test]
    fn a_write_response_carries_metrics_beside_everything_else() {
        let mut item = Item::new();
        item.insert("pk".to_string(), s("p1"));
        let cc = ConsumedCapacity::table_only("t", 1.0, ReturnConsumedCapacity::Total);
        let metrics = ItemCollectionMetrics {
            key: item.clone(),
            bytes: Some(1_073_741_824),
        };

        // All three coexist: the echoed attributes, the capacity report, and
        // the collection report.
        let body: Value = serde_json::from_str(&write_response(
            ReturnValues::AllOld,
            Some(&item),
            Some(&cc),
            Some(&metrics),
        ))
        .expect("json");
        assert_eq!(body["Attributes"]["pk"]["S"], "p1");
        assert_eq!(body["ConsumedCapacity"]["CapacityUnits"], 1.0);
        assert_eq!(
            body["ItemCollectionMetrics"]["ItemCollectionKey"]["pk"]["S"],
            "p1"
        );
        assert_eq!(body["ItemCollectionMetrics"]["SizeEstimateRangeGB"][1], 1.0);

        // And metrics alone, on an otherwise-empty body.
        let body: Value = serde_json::from_str(&write_response(
            ReturnValues::None,
            None,
            None,
            Some(&metrics),
        ))
        .expect("json");
        assert!(body.get("Attributes").is_none());
        assert!(body.get("ConsumedCapacity").is_none());
        assert!(body.get("ItemCollectionMetrics").is_some());

        // An update carries them the same way.
        let body: Value = serde_json::from_str(&update_response(
            UpdateReturnValues::None,
            None,
            None,
            None,
            Some(&metrics),
        ))
        .expect("json");
        assert!(body.get("ItemCollectionMetrics").is_some());
    }

    #[test]
    fn consumed_capacity_rides_alongside_the_bodys_own_fields() {
        // The capacity report is additive: it must not displace `Item` or
        // `Attributes`, which is the whole reason these share one serializer.
        let mut item = Item::new();
        item.insert("id".to_string(), s("k"));
        let cc = ConsumedCapacity::table_only("t", 0.5, ReturnConsumedCapacity::Total);

        let body: Value =
            serde_json::from_str(&get_item_response(Some(&item), Some(&cc))).expect("json");
        assert_eq!(body["Item"]["id"]["S"], "k");
        assert_eq!(body["ConsumedCapacity"]["CapacityUnits"], 0.5);

        let body: Value = serde_json::from_str(&write_response(
            ReturnValues::AllOld,
            Some(&item),
            Some(&cc),
            None,
        ))
        .expect("json");
        assert_eq!(body["Attributes"]["id"]["S"], "k");
        assert_eq!(body["ConsumedCapacity"]["CapacityUnits"], 0.5);

        let mut new = Item::new();
        new.insert("id".to_string(), s("k"));
        new.insert("a".to_string(), s("v"));
        let body: Value = serde_json::from_str(&update_response(
            UpdateReturnValues::AllNew,
            Some(&item),
            Some(&new),
            Some(&cc),
            None,
        ))
        .expect("json");
        assert_eq!(body["Attributes"]["a"]["S"], "v");
        assert_eq!(body["ConsumedCapacity"]["CapacityUnits"], 0.5);
    }
}
