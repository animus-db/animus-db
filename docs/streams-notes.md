# DynamoDB Streams — implementation notes (ADR 0042/0043)

Companion to ADR 0042 (write-side design: enable/disable, label lifecycle,
the sealer) and ADR 0043 (segment storage, replica repair, retention).
Linked from [`crates/animusd/CLAUDE.md`](../crates/animusd/CLAUDE.md)'s
module map and Wire-edges section. This file holds the `animusd`-side
wire-edge contracts, sealer/segment-store knob plumbing, and streams test
notes that don't live in a module's own `//!` doc and were moved here
verbatim during that guide's 2026-08-15 trim (see
`docs/engineering-lessons.md`) rather than being deleted.

## Console Streams tab (`dashboard_streams.js`)

`dashboard_streams.js` (ADR 0042/0043) is the Streams view: a list of
currently-`ENABLED` streams (`status.schemas.tables[t].stream`) plus any
`DISABLED`-but-in-grace-window one (a `status.stream_shards` row whose
`(table, label)` no longer matches the table's current schema stream,
F12-b), per-node stream metric tiles (the Console's first `/admin/metrics`
consumer — `dashboard_core.js`'s `loadAll()` fans it out alongside
`config`/`raft`/`raftkv`/`health`), and a detail panel merging the segment
catalog with a live `DescribeStream` call into a per-tablet shard chain,
plus a live-tail poller (`GetShardIterator`/`GetRecords`, following
`NextShardIterator`) — all through `POST /admin/data/dynamo`'s existing
proxy (bare `ListStreams`/`DescribeStream`/`GetShardIterator`/`GetRecords`
op names, `action_data_dynamo`'s `STREAMS_OPS` resolution). Enabling/
disabling a table's stream is a `dashboard_browser.js` Data Browser
action instead (a per-table `UpdateTable{StreamSpecification}` toggle,
next to that table's Indexes card), not a Streams-tab one — the same
reasoning that already puts create/drop table there.

**Shown on every role now, including control-only** (`ROLE_TABS`,
`dashboard_core.js`, ADR 0021 #10) — a control-only node holds the full
replicated `Metadata`, so the stream list and the shard-chain detail
(`ListStreams`/`DescribeStream`, both pure functions of `Metadata`) render
truthfully there; only the live-tail poller (`GetShardIterator`/
`GetRecords`) needs a genuine local CP data plane and degrades in-view
with a note + a `consoleLink` to a live data/combined node instead
(verified against a real split cluster, not assumed — see
`dashboard_streams.js`'s own doc). **A real backend gap this surfaced,
deliberately left unfixed** (a small, dashboard-scoped PR; see
`docs/engineering-lessons.md`): `dynamo_streams::get_records_sealed` calls
`ClientCtx::data()` unconditionally, which **panics** (an empty/dropped
HTTP reply, not a JSON error) on a control-only node reached via
`POST /admin/data/dynamo`'s existing proxy — `ClientCtx::data()`'s own doc
already says this must never be reachable from a client-dispatch path;
this call site is the one that violates it. The open-shard path
(`GetShardIterator{LATEST}`/`GetRecords`) doesn't panic but stalls the
full `SCHEMA_COMMIT_TIMEOUT` (~10s) before failing, since a control-only
node's `resolve_cp_route` blind-forward fallback has no local replica to
derive a real leader hint from and so never chases one.

## DynamoDB Streams wire edge (`dynamo.rs` / `dynamo_streams.rs`)

**Consumer-hidden records (ADR 0045 "E1" + ADR 0049 §1).** Two record
classes live in a change log but must never surface as stream events, and
both `GetRecords` serve paths (sealed segment decode and the open-tail
`hot_read`) filter them through the one shared predicate
`ChangeRecord::consumer_hidden` (`dynamo_streams::consumer_hidden`): the
backfill seeder's synthetic `seeded` dirty markers (real DynamoDB emits no
event for a GSI backfill's coverage sweep), and ADR 0049's image-less
`marker` records (written while the table had no stream — a stream begins
at enable, never retroactively). The **sealer deliberately does seal both
into segments** (watermark/trim mechanics stay uniform; the dead weight is
a few tens of bytes per record) — hiding is a serve-time decision, exactly
like view-type projection. Regressions cover each record class on each
serve path (a shared predicate only proves the paths *agree*, not that
each is reached with the shape in hand — issue #267):
`tests/dynamo_streams.rs::pre_enable_marker_records_never_surface_on_the_stream`
(`marker`, sealed + open) and `tests/stream_backfill_seed_filter.rs`
(`seeded` on a table streamed while a GSI backfill runs — one test per
serve path, seal knobs inverted between them); the `staged` flag's
defense-in-depth divergence case is unit-pinned in `animus-dynamo`
(`change_record_round_trips_and_names_its_event`).

**DynamoDB Streams (ADR 0042 §1/§2/§4/§9).** `TableSchema.stream:
Option<StreamSpec>` (replicated, ADR 0013) rides through the identical
`CreateTable`/`UpdateTable` surface as the key schema/indexes:
`CreateTable`'s `StreamSpecification` (`StreamEnabled`/`StreamViewType`)
is decoded into `Operation::CreateTable.stream_view_type`
(`animus-dynamo`'s `wire` module, pure — no label minting there); when
`Some`, `create_table` (this crate) mints a fresh label
(`mint_stream_label`, below) and proposes `MetaCommand::SetTableStream`
the same commit-wait shape the index-definition loop already uses
(`enable_stream`, shared with `UpdateTable`'s enable path). **`UpdateTable`
is new and stream-spec-only**: `wire::decode_update_table` rejects any
`GlobalSecondaryIndexUpdates` up front (ADR 0041 §5's own deferred item)
and requires a `StreamSpecification` — `StreamEnabled: true` decodes to
`StreamUpdate::Enable(view_type)` (rejected by `update_table` if a stream
is already enabled — the caller must disable first, matching ADR 0042
§9's "no same-command relabel" contract), `false` to `StreamUpdate::
Disable`. **`DescribeTable` is also new**: a pure read
(`describe_table`) of the replicated catalog — key schema (+
`AttributeDefinitions`, recovered from the catalog's typed `ColumnDef`s
via `animus_dynamo::schema::key_attribute_types`, the reverse of
`CreateTable`'s own `key_types` decode), index definitions, and
`StreamSpecification`/`LatestStreamArn`/`LatestStreamLabel` when a stream
is enabled (`wire::describe_table_response`, sharing `create_table_response`'s
`TableDescription`-object builder). The synthetic ARN
(`wire::stream_arn`) is `arn:aws:dynamodb:animus:0:table/<table>/
stream/<label>` — fixed placeholder region/account, matching this
adapter's existing ARN conventions. Round 3 needs no shard provisioning at
all: the hot shard is just the table's own existing `KIND_CHANGE` change
log (round-3 streams plan §A1), not a separate hidden per-stream table.
**The sealer landed in the round-3 sealer PR** (see `index_drain.rs`'s own
`//!` doc): `update_table`'s disable path now performs the F12-b
final seal (`dynamo.rs::disable_stream`, forcing every tablet's own hot
tail into a committed segment via `ClientCtx::force_seal_tablet` before
ever proposing `SetTableStream{None}`).

**The read path landed in PR6(`dynamo_streams.rs`, new module):** the
four `DynamoDBStreams_20120810.*` operations, dispatched on the **same**
listener as the item API (`dynamo.rs::dispatch` checks the target's
prefix and routes to `dynamo_streams::execute` — the decided
same-listener fork; every JSON shape and the iterator-token/shard-id
codecs are pure, in `animus_dynamo::streams_wire`, this module is the
read path's only impure layer).

- **`ListStreams`/`DescribeStream`** are pure functions of `Metadata`
  (F7 — the store is never load-bearing for a metadata read):
  `ListStreams` enumerates the current enabled label per table plus
  every `DISABLED`-but-unreaped label with a catalog row still present
  (F12-b); `DescribeStream` builds the shard chain from
  `stream_shard_rows_for_label` (closed, `EndingSequenceNumber` set)
  plus, only while `enabled`, one open shard per `tablets_for_table`
  entry at `current_open_epoch` (this tablet's own chain length —
  mirrors `index_drain::seal_now`'s identical computation). `resolve_label`
  is the one function every operation funnels through for F12-b's
  label validity: the table's *current* schema label, or any label
  with at least one still-present catalog row — neither ⇒
  `ResourceNotFoundException`. `StreamShardRow`/`SealStreamShard` grew a
  `view_type` field (a small `animus-control` catalog amendment,
  `#[serde(default)]`) — a `DISABLED` stream's grace-window
  `DescribeStream` has no live `StreamSpec` to read a view type from
  once `SetTableStream{None}` commits, so a shard's own row carries the
  view type declared *at seal time* instead (`Metadata::
  stream_view_type`, the read accessor); a view type never changes
  mid-stream, so every row of one label agrees.
- **`GetShardIterator`** mints a stateless `base64url({label, shard_id,
  position})` token (`animus_dynamo::streams_wire::encode_iterator`) —
  `position` is always the record HLC's own **exclusive** lower bound
  the next read filters on (`packed_hlc > position`), the same
  convention `segment::slice_to_hlc_range`'s `start_exclusive` and
  `index_drain::hot_read`'s `from_position` already use, so a token
  composes with either serve tier with no translation. `TRIM_HORIZON`/
  `AT`/`AFTER_SEQUENCE_NUMBER` read straight off the catalog row (sealed)
  or `effective_stream_shard_watermark` (open) with no round trip;
  `LATEST` on a sealed shard collapses to `hlc_range.1` (the
  immediate-null path); `LATEST` on a genuinely open shard needs one
  hot read (`ClientCtx::read_stream_hot_records(tablet, watermark,
  usize::MAX)`) to find the current max.
- **`GetRecords`** resolves the shard id against the catalog **fresh at
  every call** (never cached from mint time) — this is what makes an
  open-shard iterator survive a seal that happens between polls (ADR
  0042 §2): a catalog row present ⇒ the **sealed** path (any node —
  `SegmentStoreHandle::get_sealed(&row.replicas, &row.object_id)` — the
  ledger-named-object amendment, ADR 0043 §A3: `seg_id` is always resolved
  from the row's own `object_id`, never recomputed via `segment::
  segment_id` — then `segment::decode_and_slice(bytes, row.hlc_range)`, the
  superset-slice rule, ADR 0042 §10 — filtered/paginated, nulling
  `NextShardIterator`
  only once the sliced content is truly exhausted); absent ⇒ the
  **open** path (`ClientCtx::read_stream_hot_records`, forwarded to the
  tablet's own leader, no `ReadIndex` barrier, F8 — never nulls; an
  empty poll returns the *same* iterator, F4/§7), gated on the shard
  genuinely being the label's current live open epoch (else
  `TrimmedDataAccessException`). `ChangeRecord::event_name()` +
  `streams_wire::project_view`/`keys_from_images`/`stream_record_json`
  build each `Records[]` entry; `Keys` is recovered from whichever
  image is present (new preferred, old for a `REMOVE`) since both
  images always carry the full item.
- **`ClientRequest::StreamHotRead { tablet, from_position, limit }`**
  (new internal-only RPC, mirroring `ForceSeal`'s exact shape/doc
  pattern) is the open-shard forwarding payload — refused bare (gating
  sites: the `request_kind`/bare-refusal arms in `handle_request`, and
  the real handling arm in `cp_serve_forwarded`, which calls
  `index_drain::hot_read` — grepped per the house lesson on adding a
  forwarded-command variant), answered with the existing
  `ClientResponse::Pairs` shape (no new response variant — the packed
  HLC rides each key's own trailing 8 bytes, the same suffix
  `change_record_key` already appends, recovered by the caller).
  **ADR 0047**: `ForceSeal`/`StreamHotRead` now ride the intra port —
  a bare send on the client port is refused by `handle_request`'s port
  guard, not just the match arm's own bare-refusal check.
  `index_drain::hot_read` is `seal_now`'s read-only sibling: an
  identical `pending_changes()` scan/HLC-suffix-sort, filtered by
  `from_position` instead of the watermark, never sealing anything.
- **`SegmentStoreHandle::get_sealed`** (new, alongside the existing
  `put_sealed`) is the sealed-tier read: `ClusterSegmentStore::get_from`
  for the default `Cluster` variant (any recorded replica), or a plain
  local `get` for the single-directory `Fs` opt-in (replicas ignored —
  there is no per-node replica concept when every node already shares
  the identical directory).

`mint_stream_label` (ADR 0042 §4) is the proposer-side label mint: an
ISO8601-shaped string derived from **this node's own `env.now()`**
(`ClientCtx.env: ProdEnv`, a new field every `spawn_common_tail` caller
now threads in — the *only* `Env`-seam access point `ClientCtx` exposes
to the wire edges) suffixed with this node's own id (so two different
nodes minting at a coincidentally identical elapsed time can never
collide) — never the wall clock directly (ADR 0003's determinism-rule
convention, even though this crate is production-only `ProdEnv` wiring).
**Not a genuine calendar timestamp**: `ProdEnv::now()` is monotonic since
**process start**, not wall-clock epoch, so the rendered date drifts from
real time the longer a process has been up — an accepted cosmetic gap
(a stream's identity is `(table, label)`, validated byte-for-byte, never
parsed as a date), documented on the function itself. `iso8601_ish`/
`civil_from_days` (Howard Hinnant's public-domain algorithm) are a small,
dependency-free Gregorian calendar conversion — this crate takes no
date/time crate dependency for one cosmetic label format.

## Segment janitor + sealer knobs (Gotchas detail)

- **The DynamoDB Streams segment store + sealer knobs are wired via the
  `_with_orphan_sweep_after`-style layered-wrapper convention (ADR
  0042/0043, round-3 sealer PR)** — `BoundNode::start_with`/
  `BoundDataNode::start_data_with`/`run_node_with*`/`start_cluster_with*`
  all keep their exact pre-existing signatures, defaulting internally to
  `StreamSealKnobs::default()` (4 MiB / 4h, the ADR's own production
  defaults) and `SegmentStoreConfig::default()` (`Cluster`, the default
  K-replicated store); a `_streams`-suffixed sibling
  (`start_with_streams`/`start_data_with_streams`/`run_node_with_streams`/
  `start_cluster_with_streams`) takes the two explicit params. `main.rs`'s
  `--stream-seal-bytes B`/`--stream-seal-age SECS`/`--segment-store
  dir:PATH` flags (`--config/--node` and `--cluster N` only, so far — the
  split-deployment and data-only CLI paths are a named follow-up) call the
  `_streams` variants; a test that needs tiny seal thresholds (never the
  production defaults — see `index_drain.rs`'s `stream_sealer_tests`) does
  too. **`--stream-retention SECS` (round-3 PR7, the segment janitor's own
  knob) follows the identical convention** — `start_with_streams`/
  `start_cluster_with_streams`/`run_node_with_streams`/`start_cluster_inner`
  each gained one more trailing `Duration` parameter (defaulting to
  `DEFAULT_STREAM_RETENTION`, 24h, at every non-`_streams` call site,
  including every `start_cluster_with_auto_split*` wrapper), while
  `BoundControlNode::start_control_with` (control-only) hardcodes the
  default inline with no override yet — the same "split-deployment CLI
  path is a named follow-up" precedent this bullet's own opening sentence
  already established for the seal knobs/segment-store config. `main.rs`
  parses it identically to `--stream-seal-age`. `SegmentStoreHandle`
  (`Cluster(ClusterSegmentStore<ProdEnv,
  FsSegmentStore>)` or a bare opt-in `Fs(FsSegmentStore)`) and
  `StreamSealKnobs` live on `DataRole` (`ClientCtx.data()`), built by
  `build_segment_store` at node-assembly time — the **default** cluster
  variant roots its own per-node local `FsSegmentStore` at
  `<node dir>/segments` (a sibling of the `internal/` subdirectory
  `ProdEnv::bind` already owns; `BoundNode`/`BoundDataNode` gained a `dir`
  field to carry that path forward, since neither previously kept it past
  bind time) and is backed by a `ControlPlacementView` over this node's own
  control handle (live `Active` members; label-blind, matching
  `cluster_segment_store.rs`'s own current policy — a later PR that wants
  failure-domain-aware segment placement would extend this view).
- **`ClientRequest::ForceSeal { tablet }`** (round-3 sealer PR) is the
  internal-only RPC behind F12-b's disable-triggered final seal — addressed
  by tablet id directly (no client key to derive it from, unlike
  `KindWrite`/`KindScan`), refused bare, handled only inside
  `cp_serve_forwarded`. `ClientCtx::force_seal_tablet` is its caller-side
  wrapper (`dynamo.rs::disable_stream`, one call per tablet of the table
  being disabled) — a deliberately **simpler** retry shape than
  `cp_forward`'s hint-chasing loop (re-resolves routing from scratch every
  iteration rather than chasing a stale hint), acceptable for a rare,
  human-initiated admin-ish operation with no hot-path latency budget to
  protect. **Every send of an internal-only variant across the wire must
  wrap it in `ClientRequest::Forwarded`, even when the caller already knows
  it isn't the leader** — a first attempt called `ClientCtx::relay`
  directly with the bare `ForceSeal`, which compiled and passed every
  single-node test (the local branch never goes through `relay` at all)
  but failed loudly the moment a real multi-node test exercised the
  forwarding branch, exactly because the receiving side's bare-request
  refusal is designed to catch precisely that mistake. See
  `docs/engineering-lessons.md`'s Testing section for the general rule this
  is now an instance of (a forwarded-command test suite needs at least one
  non-leader-issued call).
- **`ClientRequest::StreamHotRead { tablet, from_position, limit }`** (PR6)
  is `ForceSeal`'s read-side sibling — the internal-only RPC behind
  `GetRecords`'/`GetShardIterator`'s open-shard path (ADR 0042 §7/§8):
  same addressing (by `tablet` directly), same bare refusal, same
  "handled only inside `cp_serve_forwarded`" contract, same reason
  (`is_relayable_command` doesn't apply — this is a data-plane RPC, not a
  `MetaCommand`). `ClientCtx::read_stream_hot_records` is its caller-side
  wrapper, copying `force_seal_tablet`'s exact retry shape (fresh
  `resolve_cp_route` every iteration, no hint-chasing) rather than
  `cp_forward`'s hot-path optimization — acceptable for a `GetRecords`
  poll, which already tolerates "not there yet" as part of the stream's
  own eventually consistent contract. Answered with the pre-existing
  `ClientResponse::Pairs` shape (no new response variant): the filtered/
  sorted/limited `(source_key, change_record bytes)` list, exactly what
  `index_drain::hot_read` (the leader-local, **no-`ReadIndex`** scan this
  RPC exists to reach — F8, never to be "upgraded" to a linearizable
  scan) returns. See `dynamo.rs`'s "DynamoDB Streams" entry above for the
  read path's own full design.

## Streams test notes

- `dynamo_streams.rs`'s coverage of PR6's `ListStreams`/`DescribeStream`/
  `GetShardIterator`/`GetRecords` read path: closed-shard chains, the
  iterator-survives-a-seal property, `Limit` pagination, cross-node reads
  of both sealed and open shards, and F12-b's disable grace window.
- The ADR 0043 §A9 segment janitor end to end in `stream_janitor.rs`:
  two-phase retention with on-disk object deletion, a control-leader kill
  mid-sweep, no empty-success gap across expiry, replica repair onto a
  fresh target, the full disable-grace lifecycle, and the drop-table
  cascade converging via the janitor alone — **every retention-focused
  test seals two epochs in sequence first**, since the epoch-derivation
  guard never physically removes a tablet's own current last epoch.
- The round-3 PR8 `streams_e2e.rs` suite: an auto-split mid-stream with a
  live consumer walking the lineage handover, a real `LsmEngine` restart
  surviving the catalog/segments/label, the `FsSegmentStore` opt-in, and a
  GSI+stream table proving ADR 0042 §8's trim min-rule coexistence — using
  a `drain_tablet_lineage` helper that walks a tablet's *whole* epoch
  chain, since a fixed shard's `NextShardIterator` null only ends one
  epoch, not the whole stream. (The suite's merge-stopgap-rejection case
  was removed along with `MergeTablets`, ADR 0044.) PR1 (ADR 0042 §8/ADR
  0043 §A4/§A6) added `manual_split_with_unsealed_backlog_under_
  production_seal_knobs`: a split landing on a genuinely unsealed backlog
  under non-degenerate (`seal_bytes: 1`-free) seal knobs, using the
  **age** trigger rather than the byte one — the byte trigger, under a
  real write burst crossing its threshold many times in quick succession,
  hit a separate, pre-existing timing sensitivity in `change_consumer_
  loop`'s seal arm (a handful of records occasionally missing from every
  segment *and* the open tail, reproducible with no split involved at
  all) that this PR did not chase down; see `docs/engineering-lessons.md`.
  Building this cell also found (and fixed, since it blocked writing the
  cell at all) a double-count bug in this file's own `drain_tablet_
  lineage`/`drain_all_tablets_lineage` test helpers: an epoch that closes
  while its open-tail iterator is still mid-walk must resume from that
  iterator, never re-mint `TRIM_HORIZON` — invisible under `tiny_seal_
  knobs`, whose open tail is always empty the instant it's polled.
  **`drain_all_tablets_lineage` also used to take its tablet set as a
  static snapshot for the whole drain** — a genuine, harness-only bug (no
  `src/` change): under D8's sustained write pressure a child tablet could
  itself split again *while the drain was already mid-walk*, minting a
  grandchild tablet id the walk never learns about, silently short-
  counting (~1/20 iterations). Fixed by re-resolving the live shard chain
  every pass via a fresh `DescribeStream` call (paginating
  `ExclusiveStartShardId`), folding any newly discovered tablet id in with
  a fresh cursor while never disturbing an already-tracked tablet's
  in-flight open-tail iterator; see `docs/engineering-lessons.md`.
- **ADR 0042 fork G (2026-08-16)**: `index_drain.rs`'s own `stream_sealer_
  tests` gained `sub_threshold_backlog_never_seals_while_below_both_triggers`
  (a real, nonzero `KIND_CHANGE` backlog under both huge knobs sits for many
  ticks with zero catalog rows — proving the unconditional every-tick scan
  is gone even with a real hot tail, `empty_hot_tail_never_seals`'s
  nonzero-bytes sibling) and `age_trigger_uses_catalog_seal_time_for_a_later_
  backlog` (a second seal, after a first one has already landed, is timed
  off `Metadata::last_seal_wall_ms` rather than the never-sealed fallback
  `age_trigger_seals_a_quiet_table` exercises). **The never-sealed
  fallback's own design went through two broken iterations before landing**
  — both caught by `streams_e2e.rs::manual_split_with_unsealed_backlog_
  under_production_seal_knobs` (a pre-existing regression this fork did not
  intend to touch) going deterministically red, then flaky: seeding the
  fallback at a bare driver-local "now" timestamp forgets how old a split
  child's *inherited* backlog actually is (silently delaying its first seal
  by however long the parent's own backlog had already been aging, and
  compounding across a cascade of auto-splits); a same-node "inherit the
  parent tablet's own memoized basis" patch is *also* wrong, since a split
  child is routinely led by a different node than its parent and the
  fallback map is per-node in-memory state — that node never even observed
  the parent tablet. The landed fix is a **one-time** `pending_changes()`
  scan of the true oldest pending record's own HLC, run only the first time
  a tablet is ever seen with a nonzero, never-sealed backlog and memoized
  from then on — correct (reads the real data, not a per-node guess) and
  still eliminates the overwhelming majority of the target cost (once per
  tablet's lifetime, not once per tick forever). See `seal_tick`'s own doc
  (`crates/animusd/src/index_drain.rs`) for the full account and
  `docs/engineering-lessons.md` for the generalized lesson.
  `ANIMUS_STREAM_SEEDS=20 cargo test -p animus-test --test
  stream_lineage_corpus` stayed green throughout — the corpus never
  exercises `seal_tick`'s internal trigger-derivation mechanism directly (it
  drives real seals via knobs the same way the pre-fork code did), so it is
  a regression check on the seal *sequence*/lineage, not on the trigger
  rewrite itself; the sealer-tests matrix above is what actually pins the
  new derivation down, and `manual_split_with_unsealed_backlog_under_
  production_seal_knobs` (a real 3-node `ProdEnv` cluster, in `streams_e2e.
  rs`) is what caught the cross-node inheritance gap the sealer-tests
  matrix's own single-node harness structurally cannot reach.
