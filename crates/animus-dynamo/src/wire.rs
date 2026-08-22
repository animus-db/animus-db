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
//! tablets through the same sink CQL `DROP TABLE`/the dashboard use),
//! `ListTables` (ascending name order, `Limit`/`ExclusiveStartTableName`
//! paginated, a materialized GSI's hidden table filtered out), `PutItem`,
//! `GetItem`, `DeleteItem`, `Query`, `Scan`, `UpdateItem`, `BatchWriteItem`,
//! `TransactWriteItems` (atomic, ADR 0018 §2/PR7), `TransactGetItems` (a
//! consistent multi-key read, ADR 0018 §2/PR7), `UpdateTimeToLive` /
//! `DescribeTimeToLive` (ADR 0051 — decode/encode only; the expiry predicate
//! itself is [`crate::ttl`], and the background reaper is `animusd`'s).
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
//! a `ProjectionExpression` / `AttributesToGet` (top-level attribute names
//! only). Deferred (rejected with a clear error): document-path projections
//! (`a.b`), per-index projection lists, `UpdateItem`-only `ReturnValues`
//! modes, and adding an LSI to an existing table (LSIs are create-time-only
//! in real DynamoDB).

use std::collections::BTreeMap;

use animus_control::{IndexStatus, StreamViewType};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::condition::{Comparator, ConditionExpression, SortKeyCondition};
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

/// A projection: the document paths a read should return (from a
/// `ProjectionExpression` or the legacy `AttributesToGet`). `None` on an
/// operation means "all attributes"; `Some(paths)` keeps only the requested
/// paths (a requested-but-absent path is simply omitted, as in DynamoDB).
///
/// Each element is a **dotted document path** `a.b.c`: a top-level attribute name
/// optionally followed by `.`-separated map keys, so a projection can reach into
/// nested `M` (map) attributes. A path that traverses into a non-map value (or an
/// absent key) yields nothing for that path. List-index paths (`a[0]`) remain
/// deferred — a `[` is rejected at decode time so the limitation is explicit.
///
/// The string form is kept (`Projection(pub Vec<String>)`), so a plain top-level
/// name is just a one-segment path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection(pub Vec<String>);

impl Projection {
    /// Apply the projection to `item`, keeping only the requested document paths,
    /// reconstructing the nested structure each path reaches (so projecting
    /// `a.b` yields `{a:{b:..}}`). Absent paths are skipped. Multiple paths
    /// sharing a prefix are merged.
    #[must_use]
    pub fn apply(&self, item: &Item) -> Item {
        let mut out = Item::new();
        for path in &self.0 {
            let segments: Vec<&str> = path.split('.').collect();
            project_path(item, &segments, &mut out);
        }
        out
    }
}

/// Project one dotted path's `segments` from `src` into `dst`, reconstructing the
/// nested map structure. The head names a top-level attribute; each further
/// segment descends into an `M` value. A path that does not resolve to a present
/// value (wrong type or absent key) contributes nothing.
fn project_path(src: &Item, segments: &[&str], dst: &mut Item) {
    let Some((head, rest)) = segments.split_first() else {
        return;
    };
    let Some(value) = src.get(*head) else {
        return;
    };
    if rest.is_empty() {
        // Whole sub-tree at this leaf. Merge with anything already projected
        // under `head` (e.g. a sibling deeper path) by preferring the broader
        // (whole-value) projection.
        dst.insert((*head).to_owned(), value.clone());
        return;
    }
    // Descend into a nested map only.
    let AttributeValue::M(inner) = value else {
        return;
    };
    // Recurse into the nested map, accumulating into the nested entry under
    // `head` (created/extended as a map).
    let entry = dst
        .entry((*head).to_owned())
        .or_insert_with(|| AttributeValue::M(BTreeMap::new()));
    if let AttributeValue::M(nested_dst) = entry {
        project_path(inner, rest, nested_dst);
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
}

/// One action of an `UpdateItem` `UpdateExpression` (the supported subset): set a
/// top-level attribute to a value, or remove one. `ADD`/`DELETE` (set/number
/// arithmetic) are deferred.
///
/// **`Serialize`/`Deserialize` (ADR 0046 U3)**: rides the wire inside
/// `ClientRequest::KindWriteItem`'s `KindWriteOp::Update` — the leader-side
/// write evaluator applies `UpdateItem`'s own raw actions to the old image
/// it itself reads, rather than trusting a pre-computed new item from the
/// (possibly stale, possibly racing) edge that received the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateAction {
    /// `SET attr = :v` — set (or overwrite) a top-level attribute.
    Set(String, AttributeValue),
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

/// One action of a `TransactWriteItems` request. Each is condition-gated like a
/// conditional write; see [`Operation::TransactWriteItems`] for the (documented)
/// non-atomicity caveat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactAction {
    /// `Put`: write `item` in `table`, gated on `condition`.
    Put {
        /// Target table.
        table: String,
        /// The item to write.
        item: Item,
        /// Optional gate.
        condition: Option<ConditionExpression>,
    },
    /// `Delete`: delete `key` from `table`, gated on `condition`.
    Delete {
        /// Target table.
        table: String,
        /// The key to delete.
        key: Item,
        /// Optional gate.
        condition: Option<ConditionExpression>,
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
    },
    /// `ConditionCheck`: assert `condition` on `key` in `table` without writing.
    ConditionCheck {
        /// Target table.
        table: String,
        /// The key to check.
        key: Item,
        /// The asserted condition.
        condition: ConditionExpression,
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
    /// `ConsistentRead` for this table's reads. Accept-and-ignore for the
    /// base table, which is linearizable here already (ADR 0041 §5).
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
    },
    /// `DescribeTable` (ADR 0042 §2): a pure read of the replicated catalog
    /// (key schema, secondary-index definitions, stream configuration).
    DescribeTable {
        /// Target table name.
        table: String,
    },
    /// `DeleteTable`: drop `table` from the replicated catalog and reclaim
    /// its tablets (`animusd`'s `ClientCtx::drop_table`, the same sink CQL
    /// `DROP TABLE` and the dashboard's delete button use, ADR 0024 GC). A
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
    },
    /// `GetItem`: fetch the item identified by `key` from `table`.
    GetItem {
        /// Target table name.
        table: String,
        /// The key attributes (partition key, plus sort key for composite tables).
        key: Item,
        /// Optional projection (the attributes to return; `None` = all).
        projection: Option<Projection>,
        /// Decoded but **accept-and-ignore** (ADR 0041 §5): a base-table read
        /// is always linearizable here, so `ConsistentRead: true` is already
        /// true and needs no enforcement. Only a GSI `Query` ever rejects it.
        consistent_read: bool,
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
        /// 0041 §5): legal and already-true against the base table or an LSI
        /// (both linearizable here); an error against a **GSI** (eventually
        /// consistent by construction) — the `animusd` edge is the one place
        /// that rejects it, once `index` names a global index.
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
        /// `ConsistentRead` (default `false`). Decoded but **accept-and-
        /// ignore** for the base table or an LSI (both linearizable here
        /// already); an error against a **GSI** — the `animusd` edge is the
        /// one place that rejects it, mirroring `Query`'s identical
        /// enforcement point (ADR 0041 §5).
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
            | Operation::DescribeTimeToLive { table, .. } => Some(table),
            Operation::BatchWriteItem { .. }
            | Operation::BatchGetItem { .. }
            | Operation::TransactWriteItems { .. }
            | Operation::TransactGetItems { .. }
            | Operation::ListTables { .. } => None,
        }
    }
}

/// A wire-level decode/encode failure, carrying the DynamoDB-style error code
/// (the `__type` field) and a human message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    /// The DynamoDB error code, e.g. `ValidationException` or
    /// `UnknownOperationException`. Sent to clients as the `__type` field.
    pub code: &'static str,
    /// A human-readable message.
    pub message: String,
}

impl WireError {
    /// A malformed or invalid request (e.g. too many `TransactWriteItems`
    /// actions, or two actions on the same item).
    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            code: "ValidationException",
            message: message.into(),
        }
    }

    /// An unrecognized `X-Amz-Target` operation.
    #[must_use]
    pub fn unknown_operation(target: &str) -> Self {
        Self {
            code: "UnknownOperationException",
            message: format!("unsupported operation `{target}`"),
        }
    }

    /// A malformed JSON body.
    #[must_use]
    pub fn serialization(message: impl Into<String>) -> Self {
        Self {
            code: "SerializationException",
            message: message.into(),
        }
    }

    /// A `ConditionExpression` evaluated to false — the write is rejected.
    #[must_use]
    pub fn conditional_check_failed(message: impl Into<String>) -> Self {
        Self {
            code: "ConditionalCheckFailedException",
            message: message.into(),
        }
    }

    /// A `TransactWriteItems`/`TransactGetItems` request was cancelled — a
    /// condition failure, a lost race against a concurrent write, or an
    /// internal 2PC abort (ADR 0018 §2/PR7). This is the DynamoDB exception
    /// type real `TransactWriteItems`/`TransactGetItems` failures use (as
    /// opposed to the bare `ConditionalCheckFailedException` a single-item
    /// conditional `PutItem`/`DeleteItem`/`UpdateItem` returns) — **simple
    /// form**: a single human message, not AWS's per-action
    /// `CancellationReasons` array (explicitly deferred, ADR 0018 PR1
    /// amendment decision 4 / the PR7 amendment).
    #[must_use]
    pub fn transaction_canceled(message: impl Into<String>) -> Self {
        Self {
            code: "TransactionCanceledException",
            message: message.into(),
        }
    }

    /// Render as the DynamoDB error JSON body (`{"__type":..,"message":..}`).
    #[must_use]
    pub fn to_json(&self) -> String {
        let body = ErrorBody {
            type_: format!("com.amazonaws.dynamodb.v20120810#{}", self.code),
            message: self.message.clone(),
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

#[derive(Serialize)]
struct ErrorBody {
    #[serde(rename = "__type")]
    type_: String,
    message: String,
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
            let condition = decode_condition(obj)?;
            let return_values = decode_return_values(obj)?;
            Ok(Operation::PutItem {
                table,
                item,
                condition,
                return_values,
            })
        }
        "GetItem" => {
            let table = table_name(obj)?;
            let key = decode_item_field(obj, "Key")?;
            let projection = decode_projection(obj)?;
            let consistent_read = decode_consistent_read(obj);
            Ok(Operation::GetItem {
                table,
                key,
                projection,
                consistent_read,
            })
        }
        "DeleteItem" => {
            let table = table_name(obj)?;
            let key = decode_item_field(obj, "Key")?;
            let condition = decode_condition(obj)?;
            let return_values = decode_return_values(obj)?;
            Ok(Operation::DeleteItem {
                table,
                key,
                condition,
                return_values,
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
        _ => Err(WireError::unknown_operation(target)),
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
    Ok(Operation::UpdateItem {
        table,
        key,
        actions,
        condition,
        return_values,
    })
}

/// Decode a DynamoDB `UpdateExpression` (the supported subset). Recognized
/// clauses are `SET a = :v, b = :w` and `REMOVE c, d`, in either order; the
/// attribute names may use `#alias` placeholders and the values `:placeholder`s.
/// `ADD`/`DELETE` clauses are rejected (deferred). Non-whitespace text before
/// the first recognized clause keyword is rejected too — e.g. `"foo SET x =
/// :v"` — rather than silently dropped, which would otherwise apply only the
/// `SET` and never surface the leading garbage. **Known remaining gap**: an
/// *unaliased* top-level attribute literally named `set`/`remove`/`add`/
/// `delete` (e.g. `SET set = :v`) still misparses, since this is a substring
/// keyword scan, not a real tokenizer — a compliant SDK always aliases a
/// reserved word via `#name`, so this is accepted as a documented, low-risk
/// limitation rather than reworked into a fuller parser here.
fn decode_update_expression(
    obj: &Map<String, Value>,
    expr: &str,
) -> Result<Vec<UpdateAction>, WireError> {
    // Split into clauses on the SET/REMOVE/ADD/DELETE keywords (case-insensitive).
    // We do a simple scan: find each keyword and the text up to the next keyword.
    let lower = expr.to_ascii_lowercase();
    let keywords = ["set ", "remove ", "add ", "delete "];
    // Collect (keyword, start-of-args) positions in order.
    let mut spans: Vec<(usize, usize, &str)> = Vec::new();
    for kw in keywords {
        let mut from = 0;
        while let Some(rel) = lower[from..].find(kw) {
            let at = from + rel;
            // Only at a clause boundary (start, or preceded by whitespace).
            if at == 0 || lower.as_bytes()[at - 1].is_ascii_whitespace() {
                spans.push((at, at + kw.len(), kw.trim()));
            }
            from = at + kw.len();
        }
    }
    spans.sort_by_key(|(at, _, _)| *at);
    if spans.is_empty() {
        return Err(WireError::validation(format!(
            "unsupported `UpdateExpression` `{expr}` \
             (supported clauses: SET, REMOVE, ADD, DELETE)"
        )));
    }
    // Reject any non-whitespace text before the first recognized clause
    // keyword — e.g. `UpdateExpression: "foo SET x = :v"` — instead of
    // silently dropping it and applying only the recognized part.
    let (first_at, _, _) = spans[0];
    if !expr[..first_at].trim().is_empty() {
        return Err(WireError::validation(format!(
            "`UpdateExpression` `{expr}` has unrecognized text before its first clause"
        )));
    }
    let mut actions = Vec::new();
    for (i, (_, args_start, kw)) in spans.iter().enumerate() {
        let args_end = spans.get(i + 1).map_or(expr.len(), |(at, _, _)| *at);
        let args = expr[*args_start..args_end].trim().trim_end_matches(',');
        match *kw {
            "set" => {
                for clause in args.split(',') {
                    let (lhs, rhs) = clause.split_once('=').ok_or_else(|| {
                        WireError::validation("SET clause must be `attr = :value`")
                    })?;
                    let attr = resolve_attr_name(obj, lhs.trim())?;
                    let value = resolve_placeholder(obj, rhs.trim())?;
                    actions.push(UpdateAction::Set(attr, value));
                }
            }
            "remove" => {
                for name in args.split(',') {
                    let attr = resolve_attr_name(obj, name.trim())?;
                    actions.push(UpdateAction::Remove(attr));
                }
            }
            // `ADD`/`DELETE` take `attr :value` pairs separated by spaces,
            // not `=` — a different shape from SET's.
            kw @ ("add" | "delete") => {
                for clause in args.split(',') {
                    let clause = clause.trim();
                    if clause.is_empty() {
                        continue;
                    }
                    let (name, ph) = clause.split_once(char::is_whitespace).ok_or_else(|| {
                        WireError::validation(format!(
                            "{} clause must be `attr :value`, got `{clause}`",
                            kw.to_uppercase()
                        ))
                    })?;
                    let attr = resolve_attr_name(obj, name.trim())?;
                    let value = resolve_placeholder(obj, ph.trim())?;
                    if kw == "add" {
                        // **Numeric ADD is deliberately refused.** It is the
                        // only non-idempotent update action, and this write
                        // path is at-least-once: `ClientCtx::cp_kind_write_item`
                        // retries `kind_write_item_at_leader`, which re-reads
                        // the old image and re-applies, and a write that
                        // landed can still report a retryable error (a failed
                        // OCC seatbelt is documented as indistinguishable from
                        // a fence miss). Re-applying SET/REMOVE, or a set
                        // union/difference, converges to the same state;
                        // re-applying `+1` does not. Measured: ten concurrent
                        // increments, two of them accepted, left the counter
                        // at 431.
                        //
                        // Refusing is the honest behaviour until the write
                        // path can carry a once-only guarantee — a silently
                        // over-counted counter is far worse than a rejected
                        // request.
                        if matches!(value, AttributeValue::N(_)) {
                            return Err(WireError::validation(
                                "numeric ADD is not supported: this write path may apply a \
                                 request more than once, which would over-count. Read the \
                                 value and SET it instead, or use a set-typed ADD (union \
                                 is idempotent).",
                            ));
                        }
                        if !matches!(
                            value,
                            AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_)
                        ) {
                            return Err(WireError::validation(
                                "ADD takes a set operand (SS, NS or BS)",
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
                }
            }
            other => {
                return Err(WireError::validation(format!(
                    "`UpdateExpression` clause `{other}` is not supported \
                     (SET, REMOVE, ADD, DELETE)"
                )));
            }
        }
    }
    Ok(actions)
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

/// Decode an `UpdateItem` `ReturnValues` (`NONE`/`ALL_OLD`/`ALL_NEW`). Absent ⇒
/// `NONE`. `UPDATED_OLD`/`UPDATED_NEW` are deferred (rejected).
fn decode_update_return_values(obj: &Map<String, Value>) -> Result<UpdateReturnValues, WireError> {
    match obj.get("ReturnValues") {
        None | Some(Value::Null) => Ok(UpdateReturnValues::None),
        Some(v) => match v.as_str() {
            Some("NONE") => Ok(UpdateReturnValues::None),
            Some("ALL_OLD") => Ok(UpdateReturnValues::AllOld),
            Some("ALL_NEW") => Ok(UpdateReturnValues::AllNew),
            Some(other) => Err(WireError::validation(format!(
                "unsupported `ReturnValues` `{other}` (supported: NONE, ALL_OLD, ALL_NEW)"
            ))),
            None => Err(WireError::validation("`ReturnValues` must be a string")),
        },
    }
}

/// Decode a `BatchWriteItem` body: `{"RequestItems": {table: [{PutRequest|
/// DeleteRequest}, ..], ..}}`.
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
                reqs.push(WriteRequest::Put(decode_sub_item(put, "Item")?));
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
    Ok(Operation::BatchWriteItem { requests })
}

/// Decode a `TransactWriteItems` body: `{"TransactItems": [{Put|Delete|Update|
/// ConditionCheck}, ..]}`.
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
        let action = match kind.as_str() {
            "Put" => TransactAction::Put {
                table,
                item: decode_sub_item(inner, "Item")?,
                condition: decode_condition(inner)?,
            },
            "Delete" => TransactAction::Delete {
                table,
                key: decode_sub_item(inner, "Key")?,
                condition: decode_condition(inner)?,
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
                }
            }
            "ConditionCheck" => TransactAction::ConditionCheck {
                table,
                key: decode_sub_item(inner, "Key")?,
                condition: decode_condition(inner)?.ok_or_else(|| {
                    WireError::validation("ConditionCheck requires a `ConditionExpression`")
                })?,
            },
            other => {
                return Err(WireError::validation(format!(
                    "unsupported transact action `{other}`"
                )));
            }
        };
        actions.push(action);
    }
    Ok(Operation::TransactWriteItems { actions })
}

/// Decode a `TransactGetItems` body: `{"TransactItems": [{"Get": {TableName,
/// Key, ProjectionExpression}}, ..]}`.
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
/// almost always a client bug rather than an intentional no-op.
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
    Ok(Operation::BatchGetItem { requests })
}

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

/// Decode the optional `GlobalSecondaryIndexes` + `LocalSecondaryIndexes` of a
/// `CreateTable` into a list of [`SecondaryIndex`]. A GSI's `KeySchema` is a
/// `HASH` attribute plus an optional `RANGE` (a composite GSI); an LSI's
/// `KeySchema` shares the base partition `HASH` and adds a `RANGE` (the index's
/// alternate sort key). Absent ⇒ an empty list.
fn decode_indexes(obj: &Map<String, Value>) -> Result<Vec<SecondaryIndex>, WireError> {
    let mut out = Vec::new();
    if let Some(gsis) = obj.get("GlobalSecondaryIndexes") {
        let gsis = gsis
            .as_array()
            .ok_or_else(|| WireError::validation("`GlobalSecondaryIndexes` must be an array"))?;
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

/// Decode an `UpdateTable` request: either a `StreamSpecification` change
/// (ADR 0042 §2) or a single `GlobalSecondaryIndexUpdates` element (ADR 0045
/// §6) — rejected up front if **both** are present in the same call (Fork
/// C, kept as "exactly one supported change per call"). `StreamEnabled: true`
/// decodes to [`StreamUpdate::Enable`] (requiring `StreamViewType`),
/// `StreamEnabled: false` to [`StreamUpdate::Disable`]; index-update decoding
/// is [`decode_index_updates`]. Any other shape (neither field present) is
/// rejected — this adapter models no throughput/key/billing-mode change.
fn decode_update_table(obj: &Map<String, Value>) -> Result<Operation, WireError> {
    let table = table_name(obj)?;
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
        return Ok(Operation::UpdateTable {
            table,
            stream: None,
            index_update: Some(index_update),
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
        let names = expr
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|raw| resolve_projection_name(obj, raw))
            .collect::<Result<Vec<_>, _>>()?;
        if names.is_empty() {
            return Err(WireError::validation("`ProjectionExpression` is empty"));
        }
        return Ok(Some(Projection(names)));
    }
    if let Some(arr) = obj.get("AttributesToGet") {
        let arr = arr
            .as_array()
            .ok_or_else(|| WireError::validation("`AttributesToGet` must be an array"))?;
        let mut names = Vec::with_capacity(arr.len());
        for v in arr {
            let name = v.as_str().ok_or_else(|| {
                WireError::validation("`AttributesToGet` elements must be strings")
            })?;
            names.push(reject_path(name)?.to_owned());
        }
        if names.is_empty() {
            return Err(WireError::validation("`AttributesToGet` is empty"));
        }
        return Ok(Some(Projection(names)));
    }
    Ok(None)
}

/// Resolve one `ProjectionExpression` element into a **dotted document path**,
/// resolving a `#alias` on each `.`-separated segment through
/// `ExpressionAttributeNames`. The result is the path with aliases substituted
/// (e.g. `#p.#c` → `profile.city`). List-index syntax (`[`) is still rejected.
fn resolve_projection_name(obj: &Map<String, Value>, raw: &str) -> Result<String, WireError> {
    let mut segments = Vec::new();
    for seg in raw.split('.') {
        let seg = seg.trim();
        if seg.is_empty() {
            return Err(WireError::validation(format!(
                "projection path `{raw}` has an empty segment"
            )));
        }
        let resolved = if let Some(alias) = seg.strip_prefix('#') {
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
            seg.to_owned()
        };
        reject_list_index(&resolved)?;
        segments.push(resolved);
    }
    Ok(segments.join("."))
}

/// Reject a list-index attribute path (containing `[`): nested-map document
/// paths (`a.b`) are supported, but list indexing (`a[0]`) is deferred.
fn reject_list_index(name: &str) -> Result<(), WireError> {
    if name.contains('[') {
        return Err(WireError::validation(format!(
            "list-index projection `{name}` is not supported (map paths `a.b` only)"
        )));
    }
    Ok(())
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
/// DynamoDB). Shared by `GetItem`/`Query`/`Scan` — see [`Operation::Query`]'s
/// doc for the one place this is ever enforced (a GSI `Query`, at the
/// `animusd` edge); everywhere else it is accept-and-ignore, since a base or
/// LSI read is already linearizable regardless of what the client asked for.
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
        // As in `decode_predicate`: reject a comparison we cannot represent
        // rather than truncating it into an equality. `sk >= :v` silently
        // becoming `sk = :v` narrows a range query to exact matches.
        if op != "=" {
            return Err(WireError::validation(format!(
                "unsupported sort-key operator `{op}` in `{clause}` \
                 (supported: =, BETWEEN, begins_with)"
            )));
        }
        let attr = resolve_attr_name(obj, lhs.trim())?;
        let value = resolve_placeholder(obj, rhs.trim())?;
        return Ok((attr, SortKeyCondition::Equals(value)));
    }
    Err(WireError::validation(format!(
        "unsupported sort-key condition `{clause}` (supported: =, BETWEEN, begins_with)"
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

/// The JSON body for a successful `GetItem`: `{"Item": {..}}`, or `{}` when the
/// item is absent (matching DynamoDB).
#[must_use]
pub fn get_item_response(item: Option<&Item>) -> String {
    let mut obj = Map::new();
    if let Some(item) = item {
        obj.insert("Item".into(), encode_item(item));
    }
    serde_json::to_string(&Value::Object(obj)).expect("response serializes")
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
pub fn write_response(return_values: ReturnValues, old: Option<&Item>) -> String {
    match (return_values, old) {
        (ReturnValues::AllOld, Some(item)) => {
            let mut obj = Map::new();
            obj.insert("Attributes".into(), encode_item(item));
            serde_json::to_string(&Value::Object(obj)).expect("write response serializes")
        }
        _ => empty_response(),
    }
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
) -> String {
    let attrs = match return_values {
        UpdateReturnValues::None => None,
        UpdateReturnValues::AllOld => old,
        UpdateReturnValues::AllNew => new,
    };
    match attrs {
        Some(item) => {
            let mut obj = Map::new();
            obj.insert("Attributes".into(), encode_item(item));
            serde_json::to_string(&Value::Object(obj)).expect("update response serializes")
        }
        None => empty_response(),
    }
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
/// `SET` sets/overwrites a top-level attribute and `REMOVE` drops one; both are
/// infallible. `ADD` and `DELETE` are **not**: they are typed operations, and
/// DynamoDB rejects a mismatch (`ADD`ing a number to a string set) rather than
/// ignoring it. Hence the `Result` — silently skipping a mismatched action
/// would leave the caller believing an update applied when it did not, which
/// is the one outcome worse than an error.
///
/// This runs on the **leader** that owns the row (ADR 0046 U3), against the old
/// image the leader itself read, so `ADD`'s read-modify-write is evaluated
/// exactly once per applied write rather than against a possibly-stale image
/// from the edge.
pub fn apply_update(mut item: Item, actions: &[UpdateAction]) -> Result<Item, WireError> {
    for action in actions {
        match action {
            UpdateAction::Set(attr, value) => {
                item.insert(attr.clone(), value.clone());
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
) -> Map<String, Value> {
    let mut key_schema = vec![key_schema_entry(&schema.partition_key, "HASH")];
    if let Some(sk) = &schema.sort_key {
        key_schema.push(key_schema_entry(sk, "RANGE"));
    }
    let mut desc = Map::new();
    desc.insert("TableName".into(), Value::String(table.to_owned()));
    desc.insert("KeySchema".into(), Value::Array(key_schema));
    desc.insert("TableStatus".into(), Value::String("ACTIVE".into()));

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
    let desc = table_description_object(table, schema, indexes, &[], stream);
    let mut obj = Map::new();
    obj.insert("TableDescription".into(), Value::Object(desc));
    serde_json::to_string(&Value::Object(obj)).expect("create-table response serializes")
}

/// The JSON body for a successful `DescribeTable` (ADR 0042 §2): the same
/// shape as [`create_table_response`], plus `AttributeDefinitions` (derived
/// from `key_types`, mirroring `CreateTable`'s own decode), wrapped under
/// `Table` (DynamoDB's own `DescribeTable` response shape, distinct from
/// `CreateTable`/`UpdateTable`'s `TableDescription`). `index_statuses` is the
/// caller's Fork-D side channel of each index's *real* replicated-catalog
/// status (`animusd::dynamo::describe_table` reads it off `Metadata`) — see
/// [`table_description_object`]'s doc.
#[must_use]
pub fn describe_table_response(
    table: &str,
    schema: &TableSchema,
    key_types: &[(String, String)],
    indexes: &[SecondaryIndex],
    index_statuses: &[(String, IndexStatus)],
    stream: Option<&StreamDescription>,
) -> String {
    let mut desc = table_description_object(table, schema, indexes, index_statuses, stream);
    desc.insert(
        "AttributeDefinitions".into(),
        Value::Array(attribute_definitions(schema, key_types)),
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
    let mut desc = table_description_object(table, schema, indexes, index_statuses, stream);
    desc.insert("TableStatus".into(), Value::String("DELETING".into()));
    desc.insert(
        "AttributeDefinitions".into(),
        Value::Array(attribute_definitions(schema, key_types)),
    );
    let mut obj = Map::new();
    obj.insert("TableDescription".into(), Value::Object(desc));
    serde_json::to_string(&Value::Object(obj)).expect("delete-table response serializes")
}

/// The `AttributeDefinitions` array (partition key, plus sort key when
/// composite) for `schema`, resolving each key attribute's declared type
/// from `key_types` (defaulting to `S` when absent, mirroring
/// `CreateTable`'s own decode). Shared by [`describe_table_response`] and
/// [`delete_table_response`] — the two response shapes that echo it.
fn attribute_definitions(schema: &TableSchema, key_types: &[(String, String)]) -> Vec<Value> {
    let mut attrs = vec![attribute_definition(&schema.partition_key, key_types)];
    if let Some(sk) = &schema.sort_key {
        attrs.push(attribute_definition(sk, key_types));
    }
    attrs
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
        let p = Projection(vec!["id".into(), "name".into(), "absent".into()]);
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
            Some(Projection(vec!["id".into(), "name".into()]))
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
            Some(Projection(vec!["id".into(), "name".into()]))
        );
    }

    #[test]
    fn decodes_document_path_projection() {
        // Document-path projections (`a.b`) are now supported.
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "ProjectionExpression":"a.b, c"}"#;
        let Operation::GetItem { projection, .. } =
            decode_request("DynamoDB_20120810.GetItem", body).unwrap()
        else {
            panic!("expected GetItem");
        };
        assert_eq!(projection, Some(Projection(vec!["a.b".into(), "c".into()])));
    }

    #[test]
    fn rejects_list_index_projection() {
        // List-index paths (`a[0]`) remain deferred.
        let body = br#"{"TableName":"t","Key":{"id":{"S":"k"}},
            "ProjectionExpression":"a[0]"}"#;
        let err = decode_request("DynamoDB_20120810.GetItem", body).unwrap_err();
        assert_eq!(err.code, "ValidationException");
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
        let projected = Projection(vec!["a.b".into(), "c".into()]).apply(&item);
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
        let body = write_response(ReturnValues::AllOld, Some(&old));
        assert!(body.contains("\"Attributes\""));
        assert!(body.contains("\"S\":\"k\""));
        // ALL_OLD on an absent key is `{}`; NONE is always `{}`.
        assert_eq!(write_response(ReturnValues::AllOld, None), "{}");
        assert_eq!(write_response(ReturnValues::None, Some(&old)), "{}");
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
        assert_eq!(get_item_response(None), "{}");
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        let body = get_item_response(Some(&item));
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
    /// §5) — accept-and-ignore everywhere except a GSI `Query`, whose
    /// rejection is enforced at the `animusd` edge (e2e-tested there, since
    /// this crate never sees the replicated catalog needed to know an
    /// index's kind).
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
        assert_eq!(sort_condition, Some(SortKeyCondition::Equals(s("y"))));

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
        );
        assert!(body.contains("\"Table\""));
        assert!(body.contains("\"AttributeDefinitions\""));
        assert!(body.contains("\"AttributeType\":\"N\""));
        assert!(body.contains("\"StreamViewType\":\"KEYS_ONLY\""));
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
            } => {
                assert_eq!(table, "t");
                assert_eq!(stream, None);
                assert_eq!(
                    index_update,
                    Some(IndexUpdate::Delete("by-email".to_owned()))
                );
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
                UpdateAction::Set("a".into(), s("x")),
                UpdateAction::Set("b".into(), AttributeValue::N("3".into())),
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
        assert_eq!(actions, vec![UpdateAction::Set("x".into(), s("y"))]);
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

    #[test]
    fn apply_update_sets_and_removes() {
        let mut item = Item::new();
        item.insert("id".into(), s("k"));
        item.insert("c".into(), s("drop"));
        let new = apply_update(
            item,
            &[
                UpdateAction::Set("a".into(), s("x")),
                UpdateAction::Remove("c".into()),
            ],
        )
        .expect("SET/REMOVE are infallible");
        assert_eq!(new.get("a"), Some(&s("x")));
        assert!(!new.contains_key("c"));
        assert_eq!(new.get("id"), Some(&s("k")));
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
        let Operation::TransactWriteItems { actions } =
            decode_request("DynamoDB_20120810.TransactWriteItems", body).unwrap()
        else {
            panic!("expected TransactWriteItems");
        };
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], TransactAction::Put { .. }));
        assert!(matches!(actions[1], TransactAction::Update { .. }));
        assert!(matches!(actions[2], TransactAction::ConditionCheck { .. }));
    }

    #[test]
    fn update_response_echoes_new_for_all_new() {
        let mut new = Item::new();
        new.insert("a".into(), s("x"));
        let body = update_response(UpdateReturnValues::AllNew, None, Some(&new));
        assert!(body.contains("\"Attributes\""));
        assert!(body.contains("\"S\":\"x\""));
        assert_eq!(
            update_response(UpdateReturnValues::None, None, Some(&new)),
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
    /// was truncated into an equality, silently narrowing the result set.
    #[test]
    fn sort_key_ranges_are_rejected_not_narrowed_to_equality() {
        for op in [">=", "<=", ">", "<"] {
            let body = format!(
                r#"{{"TableName":"t","KeyConditionExpression":"pk = :p AND sk {op} :s",
                     "ExpressionAttributeValues":{{":p":{{"S":"a"}},":s":{{"S":"b"}}}}}}"#
            );
            let err = decode_request("DynamoDB_20120810.Query", body.as_bytes())
                .expect_err(&format!("sort-key `{op}` must not become an equality"));
            assert_eq!(err.code, "ValidationException", "for `{op}`");
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
                        UpdateAction::Set("name".into(), AttributeValue::S("x".into())),
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

    /// Numeric `ADD` is refused at decode, with a message that says why.
    ///
    /// It is the only non-idempotent update action, and the kind-write path is
    /// at-least-once — it retries and re-applies, and a landed write can still
    /// report a retryable error. Ten concurrent increments, two accepted, were
    /// measured leaving the counter at 431. Refusing beats over-counting.
    #[test]
    fn numeric_add_is_refused_with_a_reason() {
        let body = br#"{"TableName":"t","Key":{"pk":{"S":"a"}},
             "UpdateExpression":"ADD c :one",
             "ExpressionAttributeValues":{":one":{"N":"1"}}}"#;
        let err = decode_request("DynamoDB_20120810.UpdateItem", body)
            .expect_err("numeric ADD must be refused");
        assert_eq!(err.code, "ValidationException");
        assert!(
            err.message.contains("more than once"),
            "the message must explain why, not just refuse: {}",
            err.message
        );
    }

    /// A non-set operand is refused for both clauses.
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
                    Some(Projection(vec!["id".into(), "v".into()]))
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
}
