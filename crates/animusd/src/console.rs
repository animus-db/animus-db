//! The AnimusDB **Data Console** (ADR 0052): a DynamoDB-shaped data app for
//! application developers — browsing/querying/editing their own tables and
//! items — deliberately separate from the operator dashboard the admin port
//! serves (`dashboard.rs`, ADR 0021). Its defining rule, enforced structurally
//! here rather than just documented: **this listener never serves
//! cluster-shaped state** — no nodes, replicas, tablets, Raft, quorum,
//! leaders, placement, or health.
//!
//! PR1 enforced that by giving this module no [`crate::ClientCtx`] at all.
//! PR2 (the tables-list screen) kept that guarantee while adding this
//! listener's first real JSON endpoint — `GET /console/api/tables` — via a
//! plain `TableSnapshotFn` closure `lib.rs` owns.
//!
//! **This PR (the table's own page, Config tab) needs more than schema
//! reads — it mutates a table's GSIs/stream/TTL and can delete the table
//! outright — so the injected seam widens from one closure to a small
//! [`ConsoleBackend`] trait.** The widening is in *shape* (async, several
//! methods) only, never in *kind*: every method still takes and returns
//! nothing but plain owned console types (`&str` table/index names,
//! [`TableDetail`], [`AddGsiRequest`], [`ConsoleError`], …) — never
//! `ClientCtx`, `Metadata`, `TableSchema`, `IndexDef`, or any other
//! cluster/schema-catalog type. `lib.rs` is still the **only** code that
//! implements this trait (on `ClientCtx`, so every method has the real
//! control-plane/CP-data primitives to call) and the only code that ever
//! imports a schema-catalog type on the console's behalf — this module
//! itself imports none. If a future screen here ever seems to need a
//! cluster type, that is the signal to add a narrower method/projection
//! type instead, the same way every method here was added — never to widen
//! this module's inputs back toward `ClientCtx`.
//!
//! **This PR (the Items tab) adds the table page's second tab** — browsing
//! (`Scan`/`Query`, paginated by DynamoDB's own `ExclusiveStartKey`/
//! `LastEvaluatedKey`), and creating/editing/deleting one item at a time.
//! The seam stays [`ConsoleBackend`] (five more methods:
//! [`ConsoleBackend::scan_items`]/[`ConsoleBackend::query_items`]/
//! [`ConsoleBackend::get_item`]/[`ConsoleBackend::put_item`]/
//! [`ConsoleBackend::delete_item`]) — no new kind of seam, same shape/kind
//! discipline as PR3's widening. The one new type worth calling out is
//! [`WireItem`]: unlike every other type in this module, an item is
//! deliberately **not** projected into a console-only shape — see its own
//! doc for why passing DynamoDB's wire shape straight through is the right
//! call here even though every other endpoint in this module projects.
//!
//! **This PR (the Stream data tab) adds the table page's third and final
//! tab** — a table's DynamoDB Streams shards and the records inside them
//! ([`ConsoleBackend::stream_shards`]/[`ConsoleBackend::get_shard_iterator`]/
//! [`ConsoleBackend::get_stream_records`], built on the real
//! `ListStreams`/`DescribeStream`/`GetShardIterator`/`GetRecords` wire
//! operations, same "reuse the real wire path" rule as every mutating
//! endpoint before it). **This is the PR where the "never show cluster
//! shape" rule gets genuinely sharp**, because a DynamoDB Streams shard is
//! *implemented* as a seal epoch of one tablet's own change log (ADR
//! 0042/0043) — so the question is no longer "does this field mention a
//! tablet" but "does this field, even though it never says the word
//! `tablet`, still let a viewer reconstruct tablet-level cluster shape."
//! [`ShardSummary::shard_id`] is deliberately surfaced anyway: it embeds a
//! tablet id and a seal epoch as digits (`shardId-<tablet>-<epoch>`,
//! `animus_cp_data::segment::shard_id`'s own format), but it is *also*
//! DynamoDB's own public wire identifier — a real client receives exactly
//! this string from `DescribeStream` and passes it straight back to
//! `GetShardIterator`, so an application developer debugging their own
//! stream needs to see and copy it regardless of what it happens to encode
//! underneath. What stays off this type, on purpose, is anything that
//! would tell a viewer *which node* serves a shard, *how many replicas*
//! back it, or *whether it's currently leaderless* — none of which is
//! DynamoDB wire vocabulary and none of which this module or its trait
//! signatures ever have in scope to leak in the first place (no
//! `TabletId`/`NodeId`/replica-set type crosses this trait). See
//! [`ConsoleBackend::stream_shards`]'s own doc for the "no stream enabled"
//! honest-empty-answer decision and ADR 0052's Stream-tab amendment for the
//! full reasoning, including why a shard's own `ParentShardId` lineage is
//! surfaced (the same public-contract argument as the id itself) while a
//! seal's `replicas`/`object_id` (ADR 0042 §10, genuinely storage-internal)
//! never reaches [`ShardSummary`] at all.
//!
//! **This PR (the create-table form) ships the console's last screen** —
//! `POST /console/api/tables` ([`ConsoleBackend::create_table`]), the one
//! endpoint that can declare an LSI at all (see [`CreateLsiRequest`]'s doc
//! for why: DynamoDB LSIs are create-time-only, so this is structurally the
//! only place in this whole console an LSI can ever be declared — no
//! `add_lsi`/`drop_lsi` exists or ever will). Same discipline as every PR
//! before it: [`CreateTableRequest`] and its nested types are plain owned
//! console types, and the endpoint reuses the real `CreateTable`/
//! `UpdateTimeToLive` wire operations via `crate::dynamo::execute_routed`
//! rather than a second write path. See [`CreateTableRequest`]'s own doc for
//! what tracing `CreateTable`'s decoder found about index key attribute
//! types (the same gap issue #319 already found on the `UpdateTable` path,
//! just previously untraced on this one).
//!
//! Embedded at compile time (`include_str!`), no bundler/build step/external
//! assets — the same constraints `dashboard.rs` documents for the operator
//! console.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};

use crate::http;

/// The console's page shell, embedded at compile time.
const HTML: &str = include_str!("console.html");
/// The console's stylesheet: the embedded webfaces, then the shared design
/// tokens (ADR 0055), then this surface's own skin — same `concat!` shape as
/// `dashboard::CSS`, so both consoles serve one stylesheet from one constant.
const CSS: &str = concat!(
    include_str!("fonts.css"),
    include_str!("tokens.css"),
    include_str!("console.css"),
);
/// The console's client-side app (routing + every screen), vanilla JS, no
/// bundler — mirrors `dashboard.rs`'s `include_str!`'d asset shape.
const JS: &str = include_str!("console.js");

/// The tables-list endpoint's path — the console's first JSON route.
const TABLES_API_PATH: &str = "/console/api/tables";
/// Prefix for every per-table endpoint (`{TABLES_API_PATH}/{name}[/...]`).
const TABLES_API_PREFIX: &str = "/console/api/tables/";

/// One table's key shape, name + declared DynamoDB `AttributeType`
/// (`S`/`N`/`B`) — e.g. `{"name": "order_id", "attribute_type": "S"}` renders
/// as `order_id (S)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct KeySummary {
    pub(crate) name: String,
    pub(crate) attribute_type: String,
}

/// One *index* key attribute's shape. Unlike [`KeySummary`] the type is
/// `Option`: an index's key attribute has no declared type of its own
/// anywhere in the catalog — `animus_control::IndexDef` has no type field
/// at all, only the attribute *name*. A type is therefore knowable only
/// when that same attribute also happens to be a declared column of the
/// base table, and **the base table's own two key columns are the only
/// columns there are**: `animus_dynamo::schema::to_control` builds a
/// `ColumnDef` for `partition_key`/`sort_key` and nothing else, while
/// `index_to_control` never receives `key_types` in the first place.
///
/// This holds on **both** declaration paths, which an earlier revision of
/// this doc got wrong: it claimed a `CreateTable`-declared index kept its
/// type because its attributes arrive in `AttributeDefinitions`. They do
/// arrive — and then only the base table's own keys are looked up in them.
/// So a `CreateTable` GSI's own hash key and an LSI's own sort attribute
/// are just as untyped as a GSI added later through `UpdateTable`, whose
/// `GlobalSecondaryIndexUpdates` decoder ignores `AttributeDefinitions`
/// outright. Issue #319 covers both paths.
///
/// `None` renders as a bare attribute name rather than a fabricated `(S)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct IndexKeySummary {
    pub(crate) name: String,
    pub(crate) attribute_type: Option<String>,
}

/// A table's DynamoDB Streams configuration, console-shaped: just whether one
/// is enabled and, if so, which view type — never a shard/segment/sealing
/// detail (all of that is cluster/consumer-internal, ADR 0042/0043).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StreamSummary {
    pub(crate) enabled: bool,
    /// `Some` exactly when `enabled` — the DynamoDB wire label
    /// (`NEW_AND_OLD_IMAGES`/`NEW_IMAGE`/`OLD_IMAGE`/`KEYS_ONLY`).
    pub(crate) view_type: Option<String>,
}

/// A table's DynamoDB-style TTL configuration (ADR 0051), console-shaped:
/// whether it's enabled and, if so, which attribute holds the expiry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct TtlSummary {
    pub(crate) enabled: bool,
    /// `Some` exactly when `enabled`.
    pub(crate) attribute_name: Option<String>,
}

/// One user-visible table, projected for the tables-list screen. Plain,
/// fully owned data — no borrow, no cluster type reachable from any field.
///
/// `lsi_count` is `None`, not `Some(0)`, for a table with no sort key: an LSI
/// shares the base partition key and adds an alternate sort key, so a
/// hash-only table structurally cannot have one — that is a different fact
/// from "has a sort key, zero LSIs declared," and the console renders the two
/// differently (a dash vs. `0`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct TableSummary {
    pub(crate) name: String,
    pub(crate) partition_key: KeySummary,
    pub(crate) sort_key: Option<KeySummary>,
    pub(crate) gsi_count: u32,
    pub(crate) lsi_count: Option<u32>,
    pub(crate) stream: StreamSummary,
    pub(crate) ttl: TtlSummary,
}

/// A snapshot of the current user-visible tables, called fresh on every
/// `GET /console/api/tables` request. See the module doc for why this stays
/// a plain closure (PR2's own seam) rather than folding into
/// [`ConsoleBackend`] (PR3's seam, added alongside it for the mutating/
/// per-table endpoints): the tables list needs no table-name parameter and
/// nothing here ever needs to fail, so a bare `Fn` stays the simplest shape
/// for it.
pub(crate) type TableSnapshotFn = Arc<dyn Fn() -> Vec<TableSummary> + Send + Sync>;

/// A secondary index's declared projection (DynamoDB's own closed set:
/// `ALL`/`KEYS_ONLY`/`INCLUDE`), console-shaped. `non_key_attributes` is
/// `Some` exactly when `projection_type` is `"INCLUDE"` — mirrors the
/// `enabled`-gates-a-companion-field shape [`StreamSummary`]/[`TtlSummary`]
/// already use. Unlike an index *key* attribute's type ([`IndexKeySummary`]),
/// a projection genuinely is recorded in full for every index regardless of
/// how it was declared (`CreateTable` or the Config tab's `add_gsi`) — see
/// `lib.rs::console_projection_summary`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ProjectionSummary {
    pub(crate) projection_type: String,
    pub(crate) non_key_attributes: Option<Vec<String>>,
}

/// One global secondary index, console-shaped for the table detail screen:
/// its keys, its projection, and its lifecycle status (ADR 0045) — never its
/// hidden materialization table's own tablet/replica placement, which is
/// exactly the cluster-shaped detail this console must never surface.
/// `status` is a plain wire-label string (`"CREATING"`/`"ACTIVE"`/
/// `"DELETING"`) rather than `animus_control::IndexStatus` itself — this
/// module never imports that type at all (see the module doc); `lib.rs`
/// renders the label.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct GsiDetail {
    pub(crate) name: String,
    pub(crate) hash_attribute: IndexKeySummary,
    pub(crate) sort_attribute: Option<IndexKeySummary>,
    pub(crate) status: String,
    pub(crate) projection: ProjectionSummary,
}

/// One local secondary index, console-shaped: just its own alternate sort
/// key. **Deliberately no `status`/no hash key** — an LSI shares the base
/// table's partition key and its own storage scope (never a separate
/// materialized table the way a GSI is), and it is create-time-only in
/// DynamoDB, so it has no lifecycle to report and nothing to drop. The UI
/// must not reuse [`GsiDetail`]'s row template for these — see the Config
/// tab's Indexes section.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct LsiDetail {
    pub(crate) name: String,
    pub(crate) sort_attribute: IndexKeySummary,
}

/// One table's full configuration, for the table page's Config tab
/// (`GET /console/api/tables/{name}`). Everything [`TableSummary`] carries as
/// a count instead carries its full declaration here (every GSI, every LSI).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct TableDetail {
    pub(crate) name: String,
    pub(crate) partition_key: KeySummary,
    pub(crate) sort_key: Option<KeySummary>,
    pub(crate) gsis: Vec<GsiDetail>,
    pub(crate) lsis: Vec<LsiDetail>,
    pub(crate) stream: StreamSummary,
    pub(crate) ttl: TtlSummary,
}

/// A request to add a global secondary index (`POST
/// .../gsi`) — decoded straight off the client's JSON body. `hash_attribute`/
/// `sort_attribute` are free text (an attribute name is per-item, never a
/// closed set — see the module doc on why the UI must not offer a picker).
///
/// **No attribute type.** DynamoDB's own `UpdateTable` carries one in
/// `AttributeDefinitions`, but this adapter's decoder for
/// `GlobalSecondaryIndexUpdates` never reads it (issue #319), so a type sent
/// here would be silently discarded and the index would still read back
/// untyped. Rather than offer a control whose value cannot survive the round
/// trip, the console asks for the name alone; restore the type here (and the
/// picker in `console.js`) once #319 makes it durable.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AddGsiRequest {
    pub(crate) index_name: String,
    pub(crate) hash_attribute: String,
    #[serde(default)]
    pub(crate) sort_attribute: Option<String>,
}

/// A request to enable/disable a table's stream (`POST .../stream`).
/// `view_type` is required exactly when `enabled` (checked by the backend,
/// same "required iff" shape DynamoDB's own `StreamSpecification` has).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SetStreamRequest {
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) view_type: Option<String>,
}

/// A request to enable/disable/reconfigure a table's TTL (`POST .../ttl`).
/// `attribute_name` is required on both enable and disable (AWS's own
/// `UpdateTimeToLive` contract — a disable call still names the attribute
/// being disabled).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SetTtlRequest {
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) attribute_name: Option<String>,
}

/// One key attribute declared on the create-table form: a name plus a
/// declared DynamoDB `AttributeType` (`S`/`N`/`B`). Unlike an index's own key
/// attribute ([`IndexKeySummary`]), a **base table's** partition/sort key
/// genuinely gets its declared type recorded — `CreateTable`'s
/// `AttributeDefinitions` — so this carries a real (non-`Option`) type, the
/// same shape [`KeySummary`] already uses for a committed table.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CreateKeyAttribute {
    pub(crate) name: String,
    pub(crate) attribute_type: String,
}

/// One local secondary index declared on the create-table form (`POST
/// /console/api/tables`) — **the only place in this whole console an LSI can
/// ever be declared**: DynamoDB LSIs are create-time-only, so there is no
/// `add_lsi`/`drop_lsi` endpoint anywhere else (see [`LsiDetail`]'s own doc).
/// No attribute type on `sort_attribute` — see [`CreateTableRequest`]'s own
/// doc for why an index's key attribute type is never recorded, not even at
/// `CreateTable` time.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CreateLsiRequest {
    pub(crate) index_name: String,
    pub(crate) sort_attribute: String,
}

/// One global secondary index declared on the create-table form.
/// `hash_attribute`/`sort_attribute` are free text (an attribute name is
/// per-item, never a closed set, same rule as [`AddGsiRequest`]).
/// `projection_type` is one of DynamoDB's own three values
/// (`ALL`/`KEYS_ONLY`/`INCLUDE`) — a genuinely closed set, so the form
/// offers a real control for it, unlike an attribute name/type;
/// `projection_non_key_attributes` is required (non-empty) exactly when
/// `projection_type` is `"INCLUDE"`. Unlike [`AddGsiRequest`], this **does**
/// reach a durable projection: `CreateTable`'s `Projection` decodes and
/// records for every declared index regardless of kind (see
/// [`CreateTableRequest`]'s own doc).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CreateGsiRequest {
    pub(crate) index_name: String,
    pub(crate) hash_attribute: String,
    #[serde(default)]
    pub(crate) sort_attribute: Option<String>,
    #[serde(default = "default_projection_type")]
    pub(crate) projection_type: String,
    #[serde(default)]
    pub(crate) projection_non_key_attributes: Option<Vec<String>>,
}

fn default_projection_type() -> String {
    "ALL".to_string()
}

/// A request to create a new table (`POST /console/api/tables`) — the
/// create-table form's request body, and the one endpoint on this listener
/// that can declare an LSI (see [`CreateLsiRequest`]'s own doc for why).
///
/// **What this form does *not* offer, and why**: an index (GSI or LSI) key
/// attribute's own type. It would be natural to assume `CreateTable`
/// behaves like the base table's own key (which genuinely does get a typed
/// `AttributeDefinitions` entry, [`CreateKeyAttribute`]) — but tracing
/// `animus_dynamo::wire::decode_key_schema`/`decode_attribute_types` and the
/// `animus_dynamo::schema` bridge (`to_control`/`index_to_control`) shows
/// otherwise: `to_control` builds a `ColumnDef` **only** for the base
/// table's own `partition_key`/`sort_key`, and `index_to_control` (the one
/// function that turns a decoded `SecondaryIndex` into the replicated
/// `IndexDef`, called identically for every index `CreateTable` declares)
/// never receives `key_types` at all — `IndexDef` itself has no type field
/// to put one in regardless. So an index's key attribute has no recorded
/// type **even when the index is declared at `CreateTable` time** — the
/// same gap issue #319 already documented for `UpdateTable`'s
/// `GlobalSecondaryIndexUpdates` path, just not previously traced for this
/// one. `console_index_key_summary`'s `Option` therefore resolves to `Some`
/// for an index key attribute only in the one structural coincidence where
/// that attribute name is *also* the base table's own declared partition or
/// sort key (true of every LSI's shared hash attribute, never true of its
/// own alternate sort attribute or of an ordinary GSI's own hash/sort). This
/// form asks for index key attribute *names* only, same as [`AddGsiRequest`]
/// — never a control whose value cannot survive its own round trip.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CreateTableRequest {
    pub(crate) table_name: String,
    pub(crate) partition_key: CreateKeyAttribute,
    #[serde(default)]
    pub(crate) sort_key: Option<CreateKeyAttribute>,
    #[serde(default)]
    pub(crate) lsis: Vec<CreateLsiRequest>,
    #[serde(default)]
    pub(crate) gsis: Vec<CreateGsiRequest>,
    #[serde(default)]
    pub(crate) stream_enabled: bool,
    #[serde(default)]
    pub(crate) stream_view_type: Option<String>,
    #[serde(default)]
    pub(crate) ttl_enabled: bool,
    #[serde(default)]
    pub(crate) ttl_attribute_name: Option<String>,
}

/// A console-shaped error: an HTTP status plus a human message — never a
/// `WireError`/cluster type. `lib.rs`'s [`ConsoleBackend`] impl translates
/// whatever underlying error it hit (a DynamoDB wire error, a control-plane
/// commit-wait timeout, a `drop_table` failure) into one of these.
#[derive(Clone, Debug)]
pub(crate) struct ConsoleError {
    pub(crate) status: u16,
    pub(crate) message: String,
}

impl ConsoleError {
    pub(crate) fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

/// One item (or key), in DynamoDB's own wire shape — `{"attr_name": {"S":
/// "value"}}`, one `AttributeValue` tag (`S`/`N`/`B`/`BOOL`/`NULL`/`L`/`M`/
/// `SS`/`NS`/`BS`) per attribute. **The Items tab (ADR 0052 PR4) passes this
/// shape straight through rather than projecting it into a friendlier
/// console-only type** — the same shape a real DynamoDB client already sees,
/// so the console never has to invent (and keep in sync) a lossy translation
/// of a data model this crate deliberately doesn't own. `console.rs` treats
/// it as opaque JSON — it interprets no attribute name or value, only moves
/// the map between the wire and the HTTP body; `console.js` is what renders
/// it readably and is where the "never lie about a type" rule actually gets
/// enforced (every attribute keeps its real tag; the UI never fabricates one
/// the way an earlier Add-GSI draft did for index key types, issue #319).
pub(crate) type WireItem = serde_json::Map<String, serde_json::Value>;

/// A page of items (`GET`-shaped, but see [`ScanItemsRequest`]'s doc for why
/// this and [`QueryItemsRequest`] are POST) from a `Scan` or `Query` —
/// [`ConsoleBackend::scan_items`]/[`ConsoleBackend::query_items`]'s shared
/// return shape. `scanned_count` and `last_evaluated_key` are DynamoDB's own
/// pagination vocabulary (`ScannedCount`/`LastEvaluatedKey`), console-cased.
/// `last_evaluated_key` is currently always `None` for a `Query` result:
/// the underlying wire operation now has a real `Limit`/`ExclusiveStartKey`
/// contract (`animus-dynamo`'s `wire::decode_query` parses both, matching
/// `Scan`), but [`QueryItemsRequest`] below doesn't yet expose either
/// field — a documented, deliberate scope cut for the console screen alone
/// (a `Query` is scoped to one partition, so an unpaginated single-shot read
/// stays a reasonable console-UI tradeoff even now that the wire edge itself
/// no longer has this gap); wiring the Items tab onto real `Query`
/// pagination is a natural console-side follow-up, not attempted here.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ItemsPage {
    pub(crate) items: Vec<WireItem>,
    pub(crate) scanned_count: u64,
    pub(crate) last_evaluated_key: Option<WireItem>,
}

/// A `Scan` request (`POST .../items/scan`) — mirrors DynamoDB's own `Scan`
/// parameters closely enough that [`ConsoleBackend::scan_items`] can forward
/// them almost verbatim. **POST, not `GET`**, even though a scan doesn't
/// mutate anything: `ExclusiveStartKey` is an arbitrary nested JSON object
/// (an `AttributeValue` map), which has no clean `GET`-query-string
/// encoding — the same reason DynamoDB's own `Scan`/`Query` are POST
/// operations rather than `GET`s. `index_name` is a real closed set (one of
/// the table's own declared GSI/LSI names, from this same table's
/// [`TableDetail`]) — never free text, so `console.js` renders it with a
/// `<select>`, not a text input.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ScanItemsRequest {
    #[serde(default)]
    pub(crate) index_name: Option<String>,
    #[serde(default)]
    pub(crate) limit: Option<u32>,
    #[serde(default)]
    pub(crate) exclusive_start_key: Option<WireItem>,
}

/// A `Query`'s sort-key condition (`POST .../items/query`) — the same three
/// shapes `animus_dynamo::wire::decode_sort_condition` accepts
/// (`=`/`BETWEEN`/`begins_with`), so [`ConsoleBackend::query_items`] can
/// build the identical `KeyConditionExpression` a real client would send.
/// Every value here is a raw `AttributeValue` JSON object (`{"S": "..."}` /
/// `{"N": "..."}` / `{"B": "..."}`) — a key attribute is always scalar in
/// DynamoDB, so `console.js` offers a real `S`/`N`/`B` control for it (a
/// closed set, unlike an attribute *name*), never a free-text guess at the
/// type.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum SortKeyQuery {
    Equals {
        value: serde_json::Value,
    },
    Between {
        lo: serde_json::Value,
        hi: serde_json::Value,
    },
    BeginsWith {
        value: serde_json::Value,
    },
}

/// A `Query` request (`POST .../items/query`) — `partition_value` is
/// required (a `Query` always narrows to one partition); `sort_condition` is
/// present only when the caller chose to narrow further and the target
/// (base table, or the named GSI/LSI) actually has a sort key. No
/// `limit`/`exclusive_start_key` yet — the wire operation itself now
/// supports both (see [`ItemsPage`]'s doc), but this console request shape
/// doesn't expose them; the Items tab still issues one unpaginated `Query`
/// per partition, a deliberate scope cut, not a wire-layer limitation.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct QueryItemsRequest {
    #[serde(default)]
    pub(crate) index_name: Option<String>,
    pub(crate) partition_value: serde_json::Value,
    #[serde(default)]
    pub(crate) sort_condition: Option<SortKeyQuery>,
}

/// `POST .../items/get` — a plain `GetItem` by key. A **found-or-not-found
/// 200**, not a 404: mirrors real DynamoDB's own `GetItem` contract (an
/// absent item is a normal, successful empty result, not an error) — see
/// [`ConsoleBackend::get_item`]'s `Option` return.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct GetItemRequest {
    pub(crate) key: WireItem,
}

/// `POST .../items/put` — create or wholesale-replace one item (DynamoDB's
/// own `PutItem` semantics: the entire item is `item`, never a partial
/// merge). Both the Items tab's "new item" and "edit" forms funnel through
/// this one request shape; see `console.js`'s own doc on why an edit never
/// lets the key attributes themselves change.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PutItemRequest {
    pub(crate) item: WireItem,
}

/// `POST .../items/delete` — a plain `DeleteItem` by key.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct DeleteItemRequest {
    pub(crate) key: WireItem,
}

/// One shard of a table's stream (ADR 0042 §2/ADR 0043 §A4), console-shaped
/// for the Stream data tab's shard list. See the module doc for the full
/// reasoning on why `shard_id`/`parent_shard_id` are safe to surface even
/// though they encode a tablet id and a seal epoch: both are DynamoDB's own
/// public wire vocabulary (a real client sees exactly these from
/// `DescribeStream`), not anything this type adds. What is deliberately
/// **absent**: which node/replica currently serves the shard, whether it's
/// currently leaderless, and the seal's own storage-internal `object_id`/
/// `replicas` (ADR 0042 §10) — none of that is DynamoDB wire vocabulary,
/// and none of it is ever in scope to leak here (this type has no
/// `TabletId`/`NodeId`/replica-set field to begin with).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ShardSummary {
    pub(crate) shard_id: String,
    pub(crate) parent_shard_id: Option<String>,
    /// The packed-HLC sequence number (ADR 0042 §5) this shard's own
    /// records start at, as the same decimal string DynamoDB's own
    /// `SequenceNumberRange.StartingSequenceNumber` already carries.
    pub(crate) starting_sequence_number: String,
    /// `None` for the one currently-open (still-growing) shard a live
    /// tablet has; `Some` once sealed.
    pub(crate) ending_sequence_number: Option<String>,
}

/// A request to page through a table's stream shard list (`GET
/// .../stream/shards[?exclusive_start_shard_id=...]`) — the shard-list
/// sibling of [`ScanItemsRequest`]'s own `ExclusiveStartKey` walk, over
/// `DescribeStream`'s real `ExclusiveStartShardId`/`LastEvaluatedShardId`
/// pagination (ADR 0042 §3: "a busy tablet churns roughly a shard a
/// seal-age interval," so a long-lived streamed table's shard count is a
/// real, unbounded-over-time list, not a small fixed one). Plain `GET` with
/// a query parameter, unlike the Items tab's scan/query (`POST`, ADR 0052
/// PR4's own doc): a shard id is always a flat string, never a nested
/// `AttributeValue` object, so it has a clean query-string encoding.
#[derive(Clone, Debug, Default)]
pub(crate) struct StreamShardsRequest {
    pub(crate) exclusive_start_shard_id: Option<String>,
}

/// One page of a table's stream shards (`GET .../stream/shards`) — the
/// **honest "no stream enabled" answer lives here**, as data, not as an
/// error: `enabled: false` with an empty `shards` list and a `200`, never a
/// `404`/`ConsoleError`. A table with no stream is the common case (ADR
/// 0052's own brief), and the Stream data tab must say so plainly rather
/// than rendering what would otherwise look like a broken, permanently-
/// empty grid — the same "found-or-not-found 200" discipline
/// [`ConsoleBackend::get_item`] already established for a missing item.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StreamShardsPage {
    pub(crate) enabled: bool,
    /// `Some` exactly when `enabled` — the DynamoDB wire label
    /// (`NEW_AND_OLD_IMAGES`/`NEW_IMAGE`/`OLD_IMAGE`/`KEYS_ONLY`), same
    /// vocabulary as [`StreamSummary::view_type`].
    pub(crate) view_type: Option<String>,
    /// `Some` exactly when `enabled` — DynamoDB's own synthetic stream ARN
    /// (`arn:aws:dynamodb:animus:0:table/<table>/stream/<label>`), the same
    /// public identifier `DescribeTable`'s `LatestStreamArn` already
    /// surfaces. Threaded back into [`GetShardIteratorRequest`] so the
    /// backend never has to re-derive "which stream" from a bare shard id.
    pub(crate) stream_arn: Option<String>,
    pub(crate) shards: Vec<ShardSummary>,
    pub(crate) last_evaluated_shard_id: Option<String>,
}

/// A request to mint a shard iterator (`POST .../stream/iterator`).
/// `iterator_type` is one of DynamoDB's own four values — a genuinely
/// closed set (`TRIM_HORIZON`/`LATEST`/`AT_SEQUENCE_NUMBER`/
/// `AFTER_SEQUENCE_NUMBER`), so `console.js` renders a real control for it,
/// never a free-text guess (the module doc's standing rule on closed sets
/// vs. free text). `sequence_number` is required exactly when
/// `iterator_type` needs one — checked by the same wire decoder
/// (`animus_dynamo::streams_wire::decode_request`) a real
/// `GetShardIterator` call already enforces, so the console adds no second
/// validation rule to keep in sync.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct GetShardIteratorRequest {
    pub(crate) shard_id: String,
    pub(crate) iterator_type: String,
    #[serde(default)]
    pub(crate) sequence_number: Option<String>,
}

/// A request to read one page of stream records (`POST
/// .../stream/records`) — `shard_iterator` is the opaque token a prior
/// [`ConsoleBackend::get_shard_iterator`] or
/// [`ConsoleBackend::get_stream_records`] call returned; walking a shard by
/// feeding each page's `next_shard_iterator` back in here is the Stream
/// tab's honest paging equivalent of the Items tab's `ExclusiveStartKey`
/// walk (ADR 0052 PR4) — never a fake offset, and never re-derived
/// client-side.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct GetStreamRecordsRequest {
    pub(crate) shard_iterator: String,
    #[serde(default)]
    pub(crate) limit: Option<u32>,
}

/// One page of stream records — DynamoDB's own `Record` wire shape
/// (`eventID`/`eventName`/`dynamodb: {Keys, OldImage, NewImage, ...}`, and
/// — when a record is a TTL-reaper delete (ADR 0051 §7) —
/// `userIdentity: {"PrincipalId": "dynamodb.amazonaws.com", "Type":
/// "Service"}`) passed straight through, exactly the same "no console-only
/// projection" call [`WireItem`] already made for an item and for the
/// identical reason: a stream record has no fixed console shape to project
/// onto without either inventing a lossy one or badly reinventing
/// DynamoDB's own record format. `console.rs` never interprets a record's
/// contents, only moves it between the wire and the HTTP body;
/// `console.js` is where `userIdentity`'s presence actually gets rendered
/// as "deleted by TTL expiry" — see that file's own doc.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct StreamRecordsPage {
    pub(crate) records: Vec<serde_json::Value>,
    pub(crate) next_shard_iterator: Option<String>,
}

/// The narrow, console-shaped seam for every endpoint beyond the tables
/// list — see the module doc for why this is a trait (not another bare
/// closure) and why widening it is still safe: every method's signature is
/// plain owned console types in, plain owned console types (or
/// [`ConsoleError`]) out. `lib.rs` is the one implementor (on `ClientCtx`)
/// and the one place a schema-catalog type is ever in scope while building
/// one of these methods' return values.
#[async_trait::async_trait]
pub(crate) trait ConsoleBackend: Send + Sync {
    /// Create a new table (`POST /console/api/tables`) — the create-table
    /// form's one endpoint, covering the base key schema, any LSIs (this
    /// request's own doc explains why they can *only* be declared here),
    /// any GSIs, a stream, and TTL, all in one call. Routes through the
    /// same real `CreateTable`/`UpdateTimeToLive` wire path every other
    /// mutating endpoint on this listener uses (see `lib.rs`'s impl).
    async fn create_table(&self, req: CreateTableRequest) -> Result<TableDetail, ConsoleError>;

    /// One table's full configuration, or `None` if no such table exists
    /// (rendered as a 404) — used by the table page's Config tab.
    async fn table_detail(&self, table: &str) -> Option<TableDetail>;

    /// Add a global secondary index to `table` (ADR 0045 §2/§6) — routes
    /// through the same `UpdateTable` path a real DynamoDB client would use
    /// (see the module doc). Returns the newly declared index, typically
    /// `status: "CREATING"` (it flips to `"ACTIVE"` once the backfill
    /// converges, observed on this same table's next `table_detail` poll).
    async fn add_gsi(&self, table: &str, req: AddGsiRequest) -> Result<GsiDetail, ConsoleError>;

    /// Drop `index` from `table` (ADR 0045 §5's convergent drop cascade).
    /// Refuses a local index the same way the real `UpdateTable` wire path
    /// does (LSIs are create-time-only in DynamoDB — never droppable).
    async fn drop_gsi(&self, table: &str, index: &str) -> Result<(), ConsoleError>;

    /// Enable, change the view type of, or disable `table`'s stream.
    async fn set_stream(
        &self,
        table: &str,
        req: SetStreamRequest,
    ) -> Result<StreamSummary, ConsoleError>;

    /// Enable, reconfigure, or disable `table`'s TTL (ADR 0051).
    async fn set_ttl(&self, table: &str, req: SetTtlRequest) -> Result<TtlSummary, ConsoleError>;

    /// Delete `table` outright (its schema and every tablet, incl. every
    /// GSI's hidden table — the same cascade `admin.rs::action_drop_table`
    /// and the DynamoDB wire's own `DeleteTable` (`dynamo.rs::delete_table`)
    /// drive). This method predates the wire operation and stays a thin
    /// direct `ClientCtx::drop_table` call (unlike `add_gsi`/`set_stream`/
    /// `set_ttl`, this console screen builds no DynamoDB-shaped JSON body to
    /// route through `execute_routed` — there is nothing here for one to
    /// carry beyond the table name).
    async fn delete_table(&self, table: &str) -> Result<(), ConsoleError>;

    /// Run a `Scan` over `table` (or one of its GSIs/LSIs, via
    /// [`ScanItemsRequest::index_name`]) — the Items tab's paginated browse.
    async fn scan_items(
        &self,
        table: &str,
        req: ScanItemsRequest,
    ) -> Result<ItemsPage, ConsoleError>;

    /// Run a `Query` against `table`'s partition key (or one of its
    /// GSIs'/LSIs') — the Items tab's by-key lookup.
    async fn query_items(
        &self,
        table: &str,
        req: QueryItemsRequest,
    ) -> Result<ItemsPage, ConsoleError>;

    /// Fetch one item by its full key. `None` (not a [`ConsoleError`]) when
    /// no such item exists — see [`GetItemRequest`]'s doc.
    async fn get_item(&self, table: &str, key: WireItem) -> Result<Option<WireItem>, ConsoleError>;

    /// Create or wholesale-replace one item.
    async fn put_item(&self, table: &str, item: WireItem) -> Result<(), ConsoleError>;

    /// Delete one item by its full key.
    async fn delete_item(&self, table: &str, key: WireItem) -> Result<(), ConsoleError>;

    /// One page of `table`'s stream shard list — the Stream data tab's
    /// landing read. `Ok` with `enabled: false` (never a [`ConsoleError`])
    /// when the table has no stream; see [`StreamShardsPage`]'s own doc.
    /// `Err` with a `404` only when `table` itself doesn't exist.
    async fn stream_shards(
        &self,
        table: &str,
        req: StreamShardsRequest,
    ) -> Result<StreamShardsPage, ConsoleError>;

    /// Mint a shard iterator for one of `table`'s stream shards — the
    /// Stream tab's "start reading here" action, given a shard id (from
    /// [`stream_shards`](Self::stream_shards)) and one of the four closed
    /// iterator types.
    async fn get_shard_iterator(
        &self,
        table: &str,
        req: GetShardIteratorRequest,
    ) -> Result<String, ConsoleError>;

    /// Read one page of records for a previously-minted shard iterator —
    /// the Stream tab's record viewer, walked forward via each page's own
    /// `next_shard_iterator` (see [`GetStreamRecordsRequest`]'s doc).
    async fn get_stream_records(
        &self,
        table: &str,
        req: GetStreamRecordsRequest,
    ) -> Result<StreamRecordsPage, ConsoleError>;
}

/// Accept loop for the console HTTP endpoint. One task per connection,
/// mirroring `admin::serve`/`dynamo::serve`'s own shape.
pub(crate) async fn serve(
    listener: TcpListener,
    tables: TableSnapshotFn,
    backend: Arc<dyn ConsoleBackend>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let tables = tables.clone();
                let backend = backend.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_conn(stream, tables, backend).await {
                        tracing::debug!(?err, "console connection closed");
                    }
                });
            }
            Err(err) => {
                tracing::warn!(?err, "console accept failed");
                return;
            }
        }
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    tables: TableSnapshotFn,
    backend: Arc<dyn ConsoleBackend>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    loop {
        let Some(request) = http::read_http_request(&mut stream, &mut buf).await? else {
            return Ok(()); // clean EOF
        };
        let keep_alive = request.keep_alive;
        let (status, content_type, body) = route(&request, &tables, backend.as_ref()).await;
        http::write_response(&mut stream, status, content_type, &body, keep_alive).await?;
        if !keep_alive {
            return Ok(());
        }
    }
}

/// Resolve one parsed request to a `(status, content-type, body)` triple —
/// every route this listener serves, static assets and JSON API alike, in
/// one place so `handle_conn` stays a plain read/dispatch/write loop.
async fn route(
    request: &http::HttpRequest,
    tables: &TableSnapshotFn,
    backend: &dyn ConsoleBackend,
) -> (u16, &'static str, String) {
    let method = request.method.as_str();
    let path = request.path.as_str();

    // Static assets, checked by exact path FIRST (mirrors
    // `admin.rs::static_asset`'s own ordering note) — `is_shell_path`'s
    // `/console/ui/` prefix match would otherwise swallow these.
    if method == "GET" && path == "/console/ui/console.css" {
        return (200, "text/css; charset=utf-8", CSS.to_string());
    }
    if method == "GET" && path == "/console/ui/console.js" {
        return (200, "text/javascript; charset=utf-8", JS.to_string());
    }
    if method == "GET" && path == TABLES_API_PATH {
        let summaries = tables();
        return (200, "application/json", tables_json(&summaries));
    }
    if method == "POST" && path == TABLES_API_PATH {
        return match parse_json_body::<CreateTableRequest>(&request.body) {
            Ok(req) => match backend.create_table(req).await {
                Ok(detail) => (
                    201,
                    "application/json",
                    wrap_json("table", serde_json::to_value(detail).unwrap_or_default()),
                ),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            },
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        };
    }

    if let Some(table_route) = parse_table_api_route(path) {
        return table_api_response(method, table_route, &request.query, &request.body, backend)
            .await;
    }

    if method != "GET" {
        return (405, "text/plain", "GET only".to_string());
    }
    if is_shell_path(path) {
        return (200, "text/html; charset=utf-8", HTML.to_string());
    }
    (404, "text/plain", "not found".to_string())
}

/// One `/console/api/tables/{name}[...]` request, already routed to its
/// [`TableApiRoute`] — dispatches to the matching [`ConsoleBackend`] method
/// and renders its result (or a decode/backend error) as JSON.
async fn table_api_response(
    method: &str,
    route: TableApiRoute,
    query: &str,
    body: &[u8],
    backend: &dyn ConsoleBackend,
) -> (u16, &'static str, String) {
    match (method, route) {
        ("GET", TableApiRoute::Table(table)) => match backend.table_detail(&table).await {
            Some(detail) => (200, "application/json", table_detail_json(&detail)),
            None => (404, "application/json", error_json("no such table")),
        },
        ("DELETE", TableApiRoute::Table(table)) => match backend.delete_table(&table).await {
            Ok(()) => (200, "application/json", ok_json()),
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        ("POST", TableApiRoute::Gsi(table)) => match parse_json_body::<AddGsiRequest>(body) {
            Ok(req) => match backend.add_gsi(&table, req).await {
                Ok(gsi) => (
                    200,
                    "application/json",
                    wrap_json("gsi", serde_json::to_value(gsi).unwrap_or_default()),
                ),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            },
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        ("DELETE", TableApiRoute::GsiNamed(table, index)) => {
            match backend.drop_gsi(&table, &index).await {
                Ok(()) => (200, "application/json", ok_json()),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            }
        }
        ("POST", TableApiRoute::Stream(table)) => match parse_json_body::<SetStreamRequest>(body) {
            Ok(req) => match backend.set_stream(&table, req).await {
                Ok(s) => (
                    200,
                    "application/json",
                    wrap_json("stream", serde_json::to_value(s).unwrap_or_default()),
                ),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            },
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        ("POST", TableApiRoute::Ttl(table)) => match parse_json_body::<SetTtlRequest>(body) {
            Ok(req) => match backend.set_ttl(&table, req).await {
                Ok(t) => (
                    200,
                    "application/json",
                    wrap_json("ttl", serde_json::to_value(t).unwrap_or_default()),
                ),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            },
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        ("POST", TableApiRoute::ItemsScan(table)) => {
            match parse_json_body::<ScanItemsRequest>(body) {
                Ok(req) => match backend.scan_items(&table, req).await {
                    Ok(page) => (200, "application/json", items_page_json(&page)),
                    Err(e) => (e.status, "application/json", error_json(&e.message)),
                },
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            }
        }
        ("POST", TableApiRoute::ItemsQuery(table)) => {
            match parse_json_body::<QueryItemsRequest>(body) {
                Ok(req) => match backend.query_items(&table, req).await {
                    Ok(page) => (200, "application/json", items_page_json(&page)),
                    Err(e) => (e.status, "application/json", error_json(&e.message)),
                },
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            }
        }
        ("POST", TableApiRoute::ItemsGet(table)) => match parse_json_body::<GetItemRequest>(body) {
            Ok(req) => match backend.get_item(&table, req.key).await {
                Ok(item) => (
                    200,
                    "application/json",
                    wrap_json(
                        "item",
                        item.map(serde_json::Value::Object)
                            .unwrap_or(serde_json::Value::Null),
                    ),
                ),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            },
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        ("POST", TableApiRoute::ItemsPut(table)) => match parse_json_body::<PutItemRequest>(body) {
            Ok(req) => match backend.put_item(&table, req.item).await {
                Ok(()) => (200, "application/json", ok_json()),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            },
            Err(e) => (e.status, "application/json", error_json(&e.message)),
        },
        ("POST", TableApiRoute::ItemsDelete(table)) => {
            match parse_json_body::<DeleteItemRequest>(body) {
                Ok(req) => match backend.delete_item(&table, req.key).await {
                    Ok(()) => (200, "application/json", ok_json()),
                    Err(e) => (e.status, "application/json", error_json(&e.message)),
                },
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            }
        }
        ("GET", TableApiRoute::StreamShards(table)) => {
            let req = StreamShardsRequest {
                exclusive_start_shard_id: http::query_param(query, "exclusive_start_shard_id"),
            };
            match backend.stream_shards(&table, req).await {
                Ok(page) => (200, "application/json", stream_shards_json(&page)),
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            }
        }
        ("POST", TableApiRoute::StreamIterator(table)) => {
            match parse_json_body::<GetShardIteratorRequest>(body) {
                Ok(req) => match backend.get_shard_iterator(&table, req).await {
                    Ok(it) => (
                        200,
                        "application/json",
                        wrap_json("shard_iterator", serde_json::Value::String(it)),
                    ),
                    Err(e) => (e.status, "application/json", error_json(&e.message)),
                },
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            }
        }
        ("POST", TableApiRoute::StreamRecords(table)) => {
            match parse_json_body::<GetStreamRecordsRequest>(body) {
                Ok(req) => match backend.get_stream_records(&table, req).await {
                    Ok(page) => (200, "application/json", stream_records_json(&page)),
                    Err(e) => (e.status, "application/json", error_json(&e.message)),
                },
                Err(e) => (e.status, "application/json", error_json(&e.message)),
            }
        }
        _ => (405, "application/json", error_json("method not allowed")),
    }
}

/// A decoded `/console/api/tables/{name}[...]` path, one variant per
/// resource this listener exposes beneath a table. Parsed once
/// ([`parse_table_api_route`]) and then matched against the request method
/// in [`table_api_response`] — an unrecognized method for a recognized
/// route falls to that match's own 405 arm, never here.
enum TableApiRoute {
    /// `/console/api/tables/{name}` — `GET` (detail) or `DELETE` (delete
    /// the table).
    Table(String),
    /// `/console/api/tables/{name}/gsi` — `POST` (add a GSI).
    Gsi(String),
    /// `/console/api/tables/{name}/gsi/{index}` — `DELETE` (drop a GSI).
    GsiNamed(String, String),
    /// `/console/api/tables/{name}/stream` — `POST` (set the stream).
    Stream(String),
    /// `/console/api/tables/{name}/ttl` — `POST` (set TTL).
    Ttl(String),
    /// `/console/api/tables/{name}/items/scan` — `POST` (paginated scan).
    ItemsScan(String),
    /// `/console/api/tables/{name}/items/query` — `POST` (query by key).
    ItemsQuery(String),
    /// `/console/api/tables/{name}/items/get` — `POST` (get one item by key).
    ItemsGet(String),
    /// `/console/api/tables/{name}/items/put` — `POST` (create/replace one item).
    ItemsPut(String),
    /// `/console/api/tables/{name}/items/delete` — `POST` (delete one item by key).
    ItemsDelete(String),
    /// `/console/api/tables/{name}/stream/shards` — `GET` (paginated shard list).
    StreamShards(String),
    /// `/console/api/tables/{name}/stream/iterator` — `POST` (mint a shard iterator).
    StreamIterator(String),
    /// `/console/api/tables/{name}/stream/records` — `POST` (read one page of records).
    StreamRecords(String),
}

/// Parse a path under [`TABLES_API_PREFIX`] into a [`TableApiRoute`], or
/// `None` if it names neither a table nor one of its known sub-resources
/// (falls through to the shell/404 handling in [`route`], same as any other
/// unrecognized path). Table and index names are percent-decoded (mirrors
/// `console.js::tableHref`'s `encodeURIComponent` on the way in).
fn parse_table_api_route(path: &str) -> Option<TableApiRoute> {
    let rest = path.strip_prefix(TABLES_API_PREFIX)?;
    let mut parts = rest.splitn(2, '/');
    let table = http::percent_decode(parts.next().unwrap_or(""));
    if table.is_empty() {
        return None;
    }
    match parts.next() {
        None => Some(TableApiRoute::Table(table)),
        Some("gsi") => Some(TableApiRoute::Gsi(table)),
        Some(tail) => match tail {
            "stream" => Some(TableApiRoute::Stream(table)),
            "ttl" => Some(TableApiRoute::Ttl(table)),
            "items/scan" => Some(TableApiRoute::ItemsScan(table)),
            "items/query" => Some(TableApiRoute::ItemsQuery(table)),
            "items/get" => Some(TableApiRoute::ItemsGet(table)),
            "items/put" => Some(TableApiRoute::ItemsPut(table)),
            "items/delete" => Some(TableApiRoute::ItemsDelete(table)),
            "stream/shards" => Some(TableApiRoute::StreamShards(table)),
            "stream/iterator" => Some(TableApiRoute::StreamIterator(table)),
            "stream/records" => Some(TableApiRoute::StreamRecords(table)),
            _ => tail.strip_prefix("gsi/").and_then(|index| {
                (!index.is_empty())
                    .then(|| TableApiRoute::GsiNamed(table, http::percent_decode(index)))
            }),
        },
    }
}

/// Decode a JSON request body into `T`, mapping a decode failure to a `400`
/// [`ConsoleError`] — every `POST` endpoint's first step.
fn parse_json_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, ConsoleError> {
    serde_json::from_slice(body)
        .map_err(|e| ConsoleError::new(400, format!("invalid JSON body: {e}")))
}

/// Encode the tables-list response body: `{"tables": [...]}`.
fn tables_json(tables: &[TableSummary]) -> String {
    serde_json::to_string(&serde_json::json!({ "tables": tables }))
        .unwrap_or_else(|_| "{\"tables\":[]}".to_string())
}

/// Encode a [`TableDetail`] as the top-level response body (a single
/// resource, unlike the tables list — no wrapping key).
fn table_detail_json(detail: &TableDetail) -> String {
    serde_json::to_string(detail).unwrap_or_else(|_| "{}".to_string())
}

/// Encode an [`ItemsPage`] as the top-level response body for `items/scan`/
/// `items/query` — a bare object (`items`/`scanned_count`/
/// `last_evaluated_key`), same "no extra wrapping key" convention
/// [`table_detail_json`] uses.
fn items_page_json(page: &ItemsPage) -> String {
    serde_json::to_string(page).unwrap_or_else(|_| "{\"items\":[],\"scanned_count\":0}".to_string())
}

/// Encode a [`StreamShardsPage`] as the top-level response body for
/// `stream/shards` — same bare-object convention as [`items_page_json`].
fn stream_shards_json(page: &StreamShardsPage) -> String {
    serde_json::to_string(page).unwrap_or_else(|_| "{\"enabled\":false,\"shards\":[]}".to_string())
}

/// Encode a [`StreamRecordsPage`] as the top-level response body for
/// `stream/records` — same bare-object convention as [`items_page_json`].
fn stream_records_json(page: &StreamRecordsPage) -> String {
    serde_json::to_string(page).unwrap_or_else(|_| "{\"records\":[]}".to_string())
}

/// Wrap one JSON value under `key` — the `{"gsi": ...}`/`{"stream":
/// ...}`/`{"ttl": ...}` response shape every mutating endpoint but
/// delete/drop uses (those two use [`ok_json`] instead, since there is no
/// resource left to describe).
fn wrap_json(key: &str, value: serde_json::Value) -> String {
    serde_json::to_string(&serde_json::json!({ key: value })).unwrap_or_else(|_| "{}".to_string())
}

/// The body of a bare-success response with nothing else to report (drop a
/// GSI, delete a table).
fn ok_json() -> String {
    "{\"ok\":true}".to_string()
}

/// Encode a `{"error": message}` body — the one error shape every endpoint
/// on this listener uses.
fn error_json(message: &str) -> String {
    serde_json::to_string(&serde_json::json!({ "error": message }))
        .unwrap_or_else(|_| "{\"error\":\"internal error\"}".to_string())
}

/// Whether `path` should serve the console shell — the root, a couple of
/// `/console` aliases, and any `/console/ui/<screen>` deep link (mirroring
/// `admin::is_ui_path`'s own shape): a bookmark/refresh of a screen's URL —
/// built or not — lands back on the shell instead of a 404, exactly like the
/// operator dashboard's own deep-link contract. The shell's own client-side
/// router (`console.js`) is what decides whether the path names a real
/// screen (the tables list, a table's own page) or an unbuilt one (the
/// create-table form).
fn is_shell_path(path: &str) -> bool {
    matches!(path, "/" | "/console" | "/console/" | "/console/ui")
        || path.starts_with("/console/ui/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_path_covers_root_aliases_and_deep_links() {
        assert!(is_shell_path("/"));
        assert!(is_shell_path("/console"));
        assert!(is_shell_path("/console/"));
        assert!(is_shell_path("/console/ui"));
        assert!(is_shell_path("/console/ui/tables"));
        assert!(is_shell_path("/console/ui/tables/orders"));
        assert!(!is_shell_path("/admin"));
        assert!(!is_shell_path("/consoleX"));
        // `is_shell_path` alone also matches the one static asset path
        // (`/console/ui/console.css`) — harmless, since `handle_conn`
        // checks the exact asset path FIRST and only falls through to this
        // predicate afterward, so the asset route always wins in practice.
        assert!(is_shell_path("/console/ui/console.css"));
        // Same for the JSON API paths — not under `/console/ui/` at all, so
        // they never collide with the shell predicate in the first place.
        assert!(!is_shell_path(TABLES_API_PATH));
        assert!(!is_shell_path("/console/api/tables/orders"));
    }

    #[test]
    fn table_api_route_parses_every_shape() {
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders"),
            Some(TableApiRoute::Table(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/gsi"),
            Some(TableApiRoute::Gsi(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/gsi/by-status"),
            Some(TableApiRoute::GsiNamed(t, i)) if t == "orders" && i == "by-status"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/stream"),
            Some(TableApiRoute::Stream(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/ttl"),
            Some(TableApiRoute::Ttl(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/items/scan"),
            Some(TableApiRoute::ItemsScan(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/items/query"),
            Some(TableApiRoute::ItemsQuery(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/items/get"),
            Some(TableApiRoute::ItemsGet(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/items/put"),
            Some(TableApiRoute::ItemsPut(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/items/delete"),
            Some(TableApiRoute::ItemsDelete(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/stream/shards"),
            Some(TableApiRoute::StreamShards(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/stream/iterator"),
            Some(TableApiRoute::StreamIterator(t)) if t == "orders"
        ));
        assert!(matches!(
            parse_table_api_route("/console/api/tables/orders/stream/records"),
            Some(TableApiRoute::StreamRecords(t)) if t == "orders"
        ));
        assert!(parse_table_api_route("/console/api/tables/orders/items").is_none());
        assert!(parse_table_api_route("/console/api/tables/orders/items/bogus").is_none());
        // A table name that needed percent-encoding round-trips.
        assert!(matches!(
            parse_table_api_route("/console/api/tables/a%20b"),
            Some(TableApiRoute::Table(t)) if t == "a b"
        ));
        // Not a per-table route at all (the list endpoint has no trailing
        // slash) and an unrecognized sub-resource both fall through.
        assert!(parse_table_api_route("/console/api/tables").is_none());
        assert!(parse_table_api_route("/console/api/tables/").is_none());
        assert!(parse_table_api_route("/console/api/tables/orders/bogus").is_none());
        assert!(parse_table_api_route("/console/api/tables/orders/gsi/").is_none());
    }

    fn sample_table() -> TableSummary {
        TableSummary {
            name: "orders".into(),
            partition_key: KeySummary {
                name: "order_id".into(),
                attribute_type: "S".into(),
            },
            sort_key: Some(KeySummary {
                name: "created_at".into(),
                attribute_type: "N".into(),
            }),
            gsi_count: 2,
            lsi_count: Some(1),
            stream: StreamSummary {
                enabled: true,
                view_type: Some("NEW_AND_OLD_IMAGES".into()),
            },
            ttl: TtlSummary {
                enabled: true,
                attribute_name: Some("expiresAt".into()),
            },
        }
    }

    /// The JSON shape a table with every feature turned on renders as, and —
    /// the property most worth pinning here — that no field name anywhere in
    /// it is cluster-shaped (no node/tablet/replica/raft/leader/quorum/
    /// placement/health vocabulary). The full server response is proven the
    /// same way against a live cluster in `tests/console_tables.rs`; this is
    /// the type-level half of that same regression.
    #[test]
    fn table_summary_serializes_console_shaped_fields_only() {
        let json = serde_json::to_value(sample_table()).unwrap();
        assert_eq!(json["name"], "orders");
        assert_eq!(json["partition_key"]["name"], "order_id");
        assert_eq!(json["partition_key"]["attribute_type"], "S");
        assert_eq!(json["sort_key"]["name"], "created_at");
        assert_eq!(json["sort_key"]["attribute_type"], "N");
        assert_eq!(json["gsi_count"], 2);
        assert_eq!(json["lsi_count"], 1);
        assert_eq!(json["stream"]["enabled"], true);
        assert_eq!(json["stream"]["view_type"], "NEW_AND_OLD_IMAGES");
        assert_eq!(json["ttl"]["enabled"], true);
        assert_eq!(json["ttl"]["attribute_name"], "expiresAt");

        let text = json.to_string().to_ascii_lowercase();
        for forbidden in [
            "node",
            "tablet",
            "replica",
            "raft",
            "leader",
            "quorum",
            "placement",
            "health",
        ] {
            assert!(
                !text.contains(forbidden),
                "found cluster-shaped substring `{forbidden}` in {text}"
            );
        }
    }

    /// A hash-only table (no sort key) renders `sort_key: null` and —
    /// distinctly — `lsi_count: null` (structurally absent), never `0` (which
    /// would mean "has a sort key, zero LSIs declared").
    #[test]
    fn table_with_no_sort_key_has_no_lsi_count() {
        let mut table = sample_table();
        table.sort_key = None;
        table.lsi_count = None;
        table.gsi_count = 0;
        table.stream = StreamSummary {
            enabled: false,
            view_type: None,
        };
        table.ttl = TtlSummary {
            enabled: false,
            attribute_name: None,
        };
        let json = serde_json::to_value(table).unwrap();
        assert!(json["sort_key"].is_null());
        assert!(json["lsi_count"].is_null());
        assert_eq!(json["gsi_count"], 0, "a zero GSI count still renders as 0");
        assert_eq!(json["stream"]["enabled"], false);
        assert!(json["stream"]["view_type"].is_null());
        assert_eq!(json["ttl"]["enabled"], false);
        assert!(json["ttl"]["attribute_name"].is_null());
    }

    #[test]
    fn tables_json_wraps_the_list_under_a_tables_key() {
        let body = tables_json(&[sample_table()]);
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(value["tables"].is_array());
        assert_eq!(value["tables"].as_array().unwrap().len(), 1);
        assert_eq!(value["tables"][0]["name"], "orders");
    }

    #[test]
    fn tables_json_of_an_empty_catalog_is_an_empty_array() {
        let body = tables_json(&[]);
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["tables"].as_array().unwrap().len(), 0);
    }

    fn sample_detail() -> TableDetail {
        TableDetail {
            name: "orders".into(),
            partition_key: KeySummary {
                name: "order_id".into(),
                attribute_type: "S".into(),
            },
            sort_key: Some(KeySummary {
                name: "created_at".into(),
                attribute_type: "N".into(),
            }),
            gsis: vec![GsiDetail {
                name: "by-status".into(),
                hash_attribute: IndexKeySummary {
                    name: "status".into(),
                    // `None` on purpose: this is the shape a GSI added
                    // through `UpdateTable` really has (issue #319).
                    attribute_type: None,
                },
                sort_attribute: None,
                status: "CREATING".into(),
                projection: ProjectionSummary {
                    projection_type: "KEYS_ONLY".into(),
                    non_key_attributes: None,
                },
            }],
            lsis: vec![LsiDetail {
                name: "by-score".into(),
                sort_attribute: IndexKeySummary {
                    name: "score".into(),
                    attribute_type: Some("N".into()),
                },
            }],
            stream: StreamSummary {
                enabled: true,
                view_type: Some("NEW_IMAGE".into()),
            },
            ttl: TtlSummary {
                enabled: false,
                attribute_name: None,
            },
        }
    }

    /// The table-detail JSON shape — again pinning the no-cluster-shape
    /// property, this time including the GSI/LSI arrays (an LSI carries no
    /// `status`/hash-attribute field at all, distinct from a GSI's row
    /// shape, per the module doc on why the two must not share a template).
    #[test]
    fn table_detail_serializes_console_shaped_fields_only() {
        let json = serde_json::to_value(sample_detail()).unwrap();
        assert_eq!(json["gsis"][0]["name"], "by-status");
        assert_eq!(json["gsis"][0]["status"], "CREATING");
        assert!(json["gsis"][0]["sort_attribute"].is_null());
        assert_eq!(
            json["gsis"][0]["projection"]["projection_type"],
            "KEYS_ONLY"
        );
        assert!(json["gsis"][0]["projection"]["non_key_attributes"].is_null());
        assert_eq!(json["lsis"][0]["name"], "by-score");
        assert_eq!(json["lsis"][0]["sort_attribute"]["name"], "score");
        assert!(
            json["lsis"][0].get("status").is_none(),
            "an LSI row carries no lifecycle status field at all"
        );

        let text = json.to_string().to_ascii_lowercase();
        for forbidden in [
            "node",
            "tablet",
            "replica",
            "raft",
            "leader",
            "quorum",
            "placement",
            "health",
        ] {
            assert!(
                !text.contains(forbidden),
                "found cluster-shaped substring `{forbidden}` in {text}"
            );
        }
    }

    #[test]
    fn table_detail_json_is_a_bare_object_not_wrapped() {
        let body = table_detail_json(&sample_detail());
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["name"], "orders");
        assert!(value.get("table").is_none(), "no extra wrapping key");
    }

    #[test]
    fn error_json_carries_the_message_under_error() {
        let body = error_json("no such table");
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["error"], "no such table");
    }

    #[test]
    fn wrap_json_nests_under_the_given_key() {
        let body = wrap_json("gsi", serde_json::json!({"name": "by-status"}));
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["gsi"]["name"], "by-status");
    }

    fn sample_wire_item() -> WireItem {
        serde_json::json!({"id": {"S": "o1"}, "n": {"N": "3"}})
            .as_object()
            .unwrap()
            .clone()
    }

    /// A page of items round-trips through JSON with the DynamoDB wire shape
    /// left untouched (no attribute name/value ever rewritten) and no
    /// cluster-shaped field anywhere — pinning [`WireItem`]'s "pass the wire
    /// shape straight through" decision at the type level, the same way
    /// [`table_summary_serializes_console_shaped_fields_only`] pins
    /// [`TableSummary`].
    #[test]
    fn items_page_serializes_the_wire_item_shape_untouched() {
        let page = ItemsPage {
            items: vec![sample_wire_item()],
            scanned_count: 1,
            last_evaluated_key: Some(sample_wire_item()),
        };
        let body = items_page_json(&page);
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["items"][0]["id"]["S"], "o1");
        assert_eq!(value["items"][0]["n"]["N"], "3");
        assert_eq!(value["scanned_count"], 1);
        assert_eq!(value["last_evaluated_key"]["id"]["S"], "o1");

        let text = body.to_ascii_lowercase();
        for forbidden in [
            "node",
            "tablet",
            "replica",
            "raft",
            "leader",
            "quorum",
            "placement",
            "health",
        ] {
            assert!(
                !text.contains(forbidden),
                "found cluster-shaped substring `{forbidden}` in {text}"
            );
        }
    }

    #[test]
    fn items_page_json_omits_last_evaluated_key_when_none() {
        let page = ItemsPage {
            items: vec![],
            scanned_count: 0,
            last_evaluated_key: None,
        };
        let value: serde_json::Value = serde_json::from_str(&items_page_json(&page)).unwrap();
        assert!(value["last_evaluated_key"].is_null());
        assert!(value["items"].as_array().unwrap().is_empty());
    }

    /// The three [`SortKeyQuery`] wire shapes a `Query` request body can
    /// carry, tagged by `kind`.
    #[test]
    fn sort_key_query_decodes_every_shape() {
        let eq: SortKeyQuery =
            serde_json::from_str(r#"{"kind":"equals","value":{"S":"a"}}"#).unwrap();
        assert!(
            matches!(eq, SortKeyQuery::Equals { value } if value == serde_json::json!({"S":"a"}))
        );

        let between: SortKeyQuery =
            serde_json::from_str(r#"{"kind":"between","lo":{"N":"1"},"hi":{"N":"9"}}"#).unwrap();
        assert!(matches!(between, SortKeyQuery::Between { .. }));

        let begins: SortKeyQuery =
            serde_json::from_str(r#"{"kind":"begins_with","value":{"S":"pre"}}"#).unwrap();
        assert!(matches!(begins, SortKeyQuery::BeginsWith { .. }));
    }

    #[test]
    fn get_item_request_decodes_a_bare_key_object() {
        let req: GetItemRequest = serde_json::from_str(r#"{"key":{"id":{"S":"o1"}}}"#).unwrap();
        assert_eq!(req.key["id"]["S"], "o1");
    }

    /// The no-stream-enabled answer is a plain `200` with `enabled: false`
    /// and an empty shard list, never a `404`/error shape — the property
    /// most worth pinning for the common case (ADR 0052's own brief).
    #[test]
    fn stream_shards_page_no_stream_is_a_plain_disabled_answer() {
        let page = StreamShardsPage {
            enabled: false,
            view_type: None,
            stream_arn: None,
            shards: Vec::new(),
            last_evaluated_shard_id: None,
        };
        let value: serde_json::Value = serde_json::from_str(&stream_shards_json(&page)).unwrap();
        assert_eq!(value["enabled"], false);
        assert!(value["shards"].as_array().unwrap().is_empty());
        assert!(value["stream_arn"].is_null());
    }

    /// A shard's own id/parent-lineage round-trip untouched — the module
    /// doc's "surfaced deliberately" property — and, the property that
    /// actually matters most here: nothing about which node/replica backs
    /// the shard leaks in alongside it.
    #[test]
    fn stream_shards_page_serializes_console_shaped_fields_only() {
        let page = StreamShardsPage {
            enabled: true,
            view_type: Some("NEW_AND_OLD_IMAGES".into()),
            stream_arn: Some("arn:aws:dynamodb:animus:0:table/orders/stream/L1".into()),
            shards: vec![
                ShardSummary {
                    shard_id: "shardId-7-0".into(),
                    parent_shard_id: None,
                    starting_sequence_number: "0".into(),
                    ending_sequence_number: Some("1000".into()),
                },
                ShardSummary {
                    shard_id: "shardId-7-1".into(),
                    parent_shard_id: Some("shardId-7-0".into()),
                    starting_sequence_number: "1000".into(),
                    ending_sequence_number: None,
                },
            ],
            last_evaluated_shard_id: None,
        };
        let body = stream_shards_json(&page);
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["shards"][0]["shard_id"], "shardId-7-0");
        assert_eq!(value["shards"][1]["parent_shard_id"], "shardId-7-0");
        assert!(value["shards"][0]["ending_sequence_number"].is_string());
        assert!(value["shards"][1]["ending_sequence_number"].is_null());

        let text = body.to_ascii_lowercase();
        for forbidden in [
            "\"node",
            "\"tablet",
            "\"replica",
            "\"raft",
            "\"leader",
            "\"quorum",
            "\"placement",
            "\"health",
            "\"epoch",
        ] {
            assert!(
                !text.contains(forbidden),
                "found cluster-shaped key `{forbidden}` in {text}"
            );
        }
    }

    /// A page of stream records round-trips DynamoDB's own `Record` shape
    /// untouched, `userIdentity` included when present — the record-viewer
    /// sibling of `items_page_serializes_the_wire_item_shape_untouched`.
    #[test]
    fn stream_records_page_serializes_the_wire_record_shape_untouched() {
        let record = serde_json::json!({
            "eventID": "shardId-7-0-42",
            "eventName": "REMOVE",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "animus",
            "dynamodb": {"Keys": {"id": {"S": "o1"}}, "SequenceNumber": "42"},
            "userIdentity": {"PrincipalId": "dynamodb.amazonaws.com", "Type": "Service"},
        });
        let page = StreamRecordsPage {
            records: vec![record],
            next_shard_iterator: Some("tok".into()),
        };
        let body = stream_records_json(&page);
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["records"][0]["eventName"], "REMOVE");
        assert_eq!(
            value["records"][0]["userIdentity"]["PrincipalId"],
            "dynamodb.amazonaws.com"
        );
        assert_eq!(value["next_shard_iterator"], "tok");
    }

    #[test]
    fn get_shard_iterator_request_decodes_with_and_without_sequence_number() {
        let req: GetShardIteratorRequest =
            serde_json::from_str(r#"{"shard_id":"shardId-1-0","iterator_type":"LATEST"}"#).unwrap();
        assert_eq!(req.shard_id, "shardId-1-0");
        assert_eq!(req.iterator_type, "LATEST");
        assert!(req.sequence_number.is_none());

        let req: GetShardIteratorRequest = serde_json::from_str(
            r#"{"shard_id":"shardId-1-0","iterator_type":"AT_SEQUENCE_NUMBER","sequence_number":"42"}"#,
        )
        .unwrap();
        assert_eq!(req.sequence_number.as_deref(), Some("42"));
    }

    /// A minimal create-table request (partition key only — the common
    /// case) decodes with every optional field defaulted away.
    #[test]
    fn create_table_request_decodes_the_minimal_shape() {
        let req: CreateTableRequest = serde_json::from_str(
            r#"{"table_name":"orders","partition_key":{"name":"order_id","attribute_type":"S"}}"#,
        )
        .unwrap();
        assert_eq!(req.table_name, "orders");
        assert_eq!(req.partition_key.name, "order_id");
        assert!(req.sort_key.is_none());
        assert!(req.lsis.is_empty());
        assert!(req.gsis.is_empty());
        assert!(!req.stream_enabled);
        assert!(!req.ttl_enabled);
    }

    /// The full shape — sort key, an LSI, a GSI (with an `INCLUDE`
    /// projection), a stream, and TTL — decodes every field, and a GSI's
    /// `projection_type` defaults to `ALL` when the client omits it (the
    /// same default DynamoDB's own `CreateTable` uses when `Projection` is
    /// absent, `animus_dynamo::wire::decode_index_projection`).
    #[test]
    fn create_table_request_decodes_the_full_shape() {
        let body = serde_json::json!({
            "table_name": "orders",
            "partition_key": {"name": "order_id", "attribute_type": "S"},
            "sort_key": {"name": "created_at", "attribute_type": "N"},
            "lsis": [{"index_name": "by-score", "sort_attribute": "score"}],
            "gsis": [
                {"index_name": "by-status", "hash_attribute": "status"},
                {
                    "index_name": "by-region",
                    "hash_attribute": "region",
                    "sort_attribute": "created_at",
                    "projection_type": "INCLUDE",
                    "projection_non_key_attributes": ["total"],
                },
            ],
            "stream_enabled": true,
            "stream_view_type": "NEW_AND_OLD_IMAGES",
            "ttl_enabled": true,
            "ttl_attribute_name": "expiresAt",
        });
        let req: CreateTableRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.sort_key.unwrap().attribute_type, "N");
        assert_eq!(req.lsis[0].sort_attribute, "score");
        assert_eq!(
            req.gsis[0].projection_type, "ALL",
            "omitted ⇒ ALL, DynamoDB's own default"
        );
        assert_eq!(req.gsis[1].projection_type, "INCLUDE");
        assert_eq!(
            req.gsis[1].projection_non_key_attributes,
            Some(vec!["total".to_string()])
        );
        assert!(req.stream_enabled);
        assert_eq!(req.stream_view_type.as_deref(), Some("NEW_AND_OLD_IMAGES"));
        assert!(req.ttl_enabled);
        assert_eq!(req.ttl_attribute_name.as_deref(), Some("expiresAt"));
    }
}
