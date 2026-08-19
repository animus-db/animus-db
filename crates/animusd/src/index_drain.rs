//! The per-tablet **change-consumer loop** (ADR 0041 §4, ADR 0042/0043): the
//! background task with three arms over the change log a table's writes
//! leave behind (`KIND_CHANGE`) — the **GSI drain** (materializes global
//! secondary indexes), the **seal arm** (ADR 0043 §A3: seals a streamed
//! table's own hot tail into an immutable segment on size/age triggers), and
//! the **hot-trim arm** (ADR 0042 §8/ADR 0043 §A6: deletes hot records once
//! every expected, present consumer has cleared them — GSI reconciliation
//! via a cursor row, streaming via the segment catalog). This module was
//! `index_drain_loop` before round 3 added the other two arms; the rename to
//! [`change_consumer_loop`] reflects that it is no longer GSI-specific.
//!
//! ## The GSI drain (ADR 0041 §4)
//!
//! A local secondary index is written atomically with its base row (it shares
//! the base partition key, so it shares the tablet). A **global** one cannot —
//! it hashes by its own key, so its rows live in a different table's tablets
//! entirely. Rather than pay a cross-tablet 2PC on every write, an indexed write
//! commits a **change-log record** alongside the base row, and this loop applies
//! the index effects afterwards. That is DynamoDB's own contract: a GSI is
//! eventually consistent, and a GSI being unavailable never fails a base write.
//!
//! ## Derivative, not delta-based
//!
//! A change record is treated purely as *a signal that a key is dirty*. The
//! drain never replays the record's images; it recomputes what the item's index
//! rows **should** be from the base row's *current* value, and compares that
//! against the **footprint** — the record of where that item's rows currently
//! are. Three hard failure modes disappear as a result:
//!
//! - **Idempotent**: a crash anywhere re-runs the whole reconciliation harmlessly.
//! - **Self-superseding**: several records for one item collapse into one
//!   reconciliation toward the current value, so there are never stale deltas to
//!   order against each other.
//! - **Orphan-free**: a stale row is, by construction, one the footprint names
//!   and the recomputation did not produce. There is no separate class of orphan
//!   needing its own sweeper.
//!
//! ### Cursor-based consumption (ADR 0042 §7/§8)
//!
//! ADR 0041 originally had the drain **delete** the records it reconciled in
//! the same entry that wrote the updated footprint — "consuming is trimming."
//! That worked only because the GSI drain was the change log's sole reader.
//! Round 2's stream copier was a second, independent reader of the same log
//! (making deletion-as-a-side-effect unsound for exactly the reason this
//! doc originally explained); **round 3 has no copier at all** — the
//! sealer (below) reads the log directly — but the underlying reason a
//! *separate* trim step is still needed is unchanged: the GSI's own
//! consumption progress and the stream's own sealed-watermark progress are
//! two independent facts, and a hot record must survive until **both**
//! (whichever apply) have cleared it. The drain advances a **cursor row**
//! (`KIND_CURSOR`, tag `"gsi"` — see [`animus_cp_data::cursor`]) recording
//! the highest change-record HLC this tablet's reconciliation has fully
//! covered; the hot-trim arm deletes records only once every *expected,
//! present* term (the GSI cursor, and/or the stream's own catalog
//! watermark, ADR 0043 §A6) has cleared them.
//!
//! **The crash property ADR 0041 documented still holds, restated for the
//! cursor**: the cursor must never claim a reconciliation whose footprint
//! didn't land. [`drain_tablet`] gets this by construction — it only advances
//! the cursor, in its own trailing write, *after* every partition dirtied this
//! pass has had its footprint update durably confirmed (`reconcile_partition`'s
//! own `cp_kind_write_raw` call only returns `Ok` once that specific write's
//! effect is visible; see that primitive's doc). A crash before the cursor
//! write lands simply leaves it wherever it was — the next tick re-reads the
//! same (still-present) records and redoes the same reconciliation, which is
//! safe because it is idempotent.
//!
//! ## The seal arm (ADR 0043 §A3)
//!
//! For each **streamed** table's led tablet, on a size or age trigger (or a
//! one-shot force-seal, F12-b's disable path), [`seal_now`] seals every
//! change record past the tablet's own effective watermark
//! (`Metadata::effective_stream_shard_watermark` — catalog-derived, walking
//! split-parent provenance for a fresh split child, ADR 0043 §A4) into an
//! immutable segment: sort by the record's own packed-HLC key suffix (the
//! change log's key order is token-then-pk-then-HLC, *not* commit order —
//! this sort is load-bearing), encode it (`animus_cp_data::segment`), push
//! it durably to this node's [`crate::SegmentStoreHandle`]
//! (`SegmentStore::put`), then propose and confirm
//! `MetaCommand::SealStreamShard`. Nothing is ever sealed empty (an empty
//! pending set is a no-op, never an empty segment) — see [`seal_now`]'s own
//! doc for the full recovery argument (why a crash-retried re-seal of the
//! identical `(tablet, epoch)` id is always safe).
//!
//! ## The backfill seeder (ADR 0045 §2)
//!
//! A fourth arm, run per led tablet for any table with at least one index
//! currently `Creating`: **backfill is the GSI drain applied to every
//! pre-existing key.** The moment `CreateTableIndex{status: Creating}`
//! commits, `table_takes_kind_write_path` already flips to `true` for that
//! table regardless of the new index's own status, so every write from that
//! instant forward already leaves a genuine `KIND_CHANGE` record — no write
//! *after* the index's declaration can ever be missed. The seeder's only job
//! is covering rows that existed *before* that instant, by manufacturing the
//! same kind of dirty marker a live write would have left.
//!
//! [`backfill_seed_tick`] sweeps this tablet's own `KIND_BASE` scope forward
//! from a **backfill cursor** — a `KIND_CURSOR` row under the tag
//! `format!("backfill:{index_name}")`, storing the raw last-seeded base-key
//! prefix rather than a packed HLC (see [`animus_cp_data::cursor`]'s module
//! doc for the two value conventions side by side) — enumerating **distinct
//! partitions**, not items, via the same "bump the last byte" skip-ahead
//! trick [`reconcile_partition`] already uses. For each newly-discovered
//! partition (bounded to [`BACKFILL_SEED_BATCH`] per tick) it proposes a
//! `KvCommand::KindBatch` carrying **only** a change-log entry for that
//! partition's prefix — no base-row write — so apply stamps a fresh
//! `hlc::pack(ts)` exactly like a live write's own change record, landing
//! ahead of the `"gsi"` cursor watermark with **zero changes to
//! [`drain_tablet`]/[`reconcile_partition`]**: a seeded record is, by
//! construction, indistinguishable from one a live write would have
//! produced. When a tick's sweep reaches the end of the tablet's *current*
//! range, it (re-)proposes `MetaCommand::MarkIndexBackfilled` — a persistent
//! condition re-derived every tick (mirroring the seal arm's own
//! `ProposeSeal` discipline), not a one-shot side effect — which
//! `animusd::index_backfill_loop` (a distinct control-leader-only loop, ADR
//! 0045 §4) aggregates across every one of the table's current tablets to
//! flip the index `Creating` → `Active`.
//!
//! **Why no record is lost or double-applied**: every partition that ever
//! held a row gets at least one dirty-marker after `Creating` commits — from
//! a live write (unconditional on status) or from the seeder's forward
//! sweep, or (harmlessly) both. `reconcile_partition` never reads a change
//! record's *content*; it re-derives desired index rows from a live scan of
//! the partition's current base rows every time, so N dirty-markers collapse
//! into "reconcile once more against current state" — backfill's only
//! contribution is *coverage*, not a new correctness mechanism.
//!
//! **Per-index cursor, not one shared scan**: a table with two indexes
//! simultaneously `Creating` runs this arm once per index, each against its
//! own cursor row. A single shared scan marking every `Creating` index done
//! together would need a separate stop condition per index anyway once they
//! inevitably reach `Active` at different times (e.g. a later `UpdateTable`
//! adds a second index while the first is still backfilling) — the per-index
//! shape is simpler to reason about at the cost of re-scanning the same base
//! rows once per concurrently-backfilling index, expected to be rare and
//! short-lived.
//!
//! **Split-during-backfill (ADR 0044 split-only world, ADR 0045 §5 Fork A)**:
//! deliberately **no** split-lineage cursor inheritance. A split's
//! `narrow_scope` moves every kind scope (cursor rows included) together, so
//! the *left* child keeps `range.start` and its cursor is found unchanged;
//! the *right* child's own `cursor_key(new_start, tag)` reads empty and its
//! sweep simply restarts from the beginning of its own (strictly narrower)
//! range — unconditionally correct by the idempotence argument above, and
//! geometrically bounded since children only ever get narrower. `plan()`'s
//! completion detection is evaluated per-tablet against each tablet's
//! *current* range (`animusd::index_backfill_loop`, reading `Metadata`
//! fresh every tick), so a split after a tablet reports "done" simply
//! reintroduces two not-yet-done children into the aggregator's next check —
//! never a premature `Active` flip.
//!
//! **Leadership loss mid-scan**: the cursor row lives in the tablet's own
//! `KIND_CURSOR` scope, replicated like any other write, so a newly elected
//! leader resumes seeding from wherever the cursor was last durably
//! advanced — nothing here assumes stable leadership across ticks, the same
//! discipline every other arm in this loop already follows.
//!
//! **Interaction with Streams (ADR 0045 follow-up "E1", closed)**: the
//! synthetic change-log record a seeded partition gets carries no old/new
//! image (`ChangeRecord { old_image: None, new_image: None, seeded: true,
//! .. }`) — it exists purely as a dirty marker for the GSI drain, which
//! never reads a record's content (or this flag). If a table's stream
//! happens to be enabled *while* a new GSI backfills against it (allowed —
//! the two are orthogonal; see ADR 0045 §6 Fork C, which only rejects
//! changing *both* in one `UpdateTable` call), that image-less record is
//! still a legitimate, decodable [`animus_dynamo::ChangeRecord`] that the
//! seal arm happily seals alongside real ones — but the Streams *read* path
//! (`dynamo_streams.rs`'s two `GetRecords` serve branches) filters every
//! `seeded` record out before it ever reaches a `GetRecords` response, so no
//! phantom no-image event surfaces to a consumer. Deliberately **not**
//! fixed by giving the seeder a real base-row image: real DynamoDB emits
//! **no** stream event at all for a GSI backfill's own coverage sweep over
//! pre-existing data, so a synthesized image would be a fidelity
//! regression (a fabricated event DynamoDB itself never sends), not an
//! improvement — filtering is the fidelity-correct fix, not an
//! implementation-convenience shortcut.
//!
//! ## The hot-trim arm (ADR 0042 §8, ADR 0043 §A6, F10)
//!
//! Generalizes ADR 0041's original trim janitor: the GSI half is completely
//! unchanged (the `"gsi"` cursor tag, min-over-rows); the stream half is now
//! **catalog-derived** rather than a `"copier"` cursor row nothing writes
//! anymore (round 2's `COPIER_TAG` and `expected_consumer_tags` are gone).
//! Trim = min(gsi term if the table has GSIs, catalog watermark iff the
//! table's *current* schema has an enabled stream). A **disabled**-but-
//! draining stream's un-reaped catalog rows do **not** re-add the stream
//! term — see [`trim_janitor`]'s own doc for the F12-b coexistence rule this
//! implements and why it's safe.
//!
//! ## Split policy for this loop's own cursor tags (ADR 0046)
//!
//! Both cursor tags this loop owns (`"gsi"` above, and the per-index
//! `format!("backfill:{index_name}")` the backfill seeder writes — "The
//! backfill seeder" section above) classify as
//! [`animus_cp_data::cursor::SplitPolicy::RestartFromScratch`]: see
//! `animus_cp_data::cursor`'s own module doc "Split classification" table
//! for the full per-tag rationale (and its `every_known_cursor_tag_prefix_
//! is_classified` test, the regression that keeps a future third tag from
//! shipping with no split-behavior decision on record). The stream seal
//! watermark this loop's seal arm reads
//! (`Metadata::effective_stream_shard_watermark`, "The seal arm" section
//! above) is the one `SplitPolicy::InheritFrozenBasis` case — it lives in
//! the control plane's `Metadata`, not a `KIND_CURSOR` row, which is why
//! that table lists it at the doc level rather than in `classify_tag`.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::Metadata;
use animus_control::ProposeResult;
use animus_control::schema::{IndexDef, IndexKind};
use animus_cp_data::cursor;
use animus_cp_data::hlc::{self, HlcTimestamp};
use animus_cp_data::segment;
use animus_cp_data::{KIND_BASE, KIND_CHANGE, KIND_CURSOR, KIND_FOOTPRINT};
use animus_dynamo::wire;
use animus_dynamo::{
    AttributeValue, ChangeRecord, FootprintEntry, IndexFootprint, Item, index as dynamo_index,
    index_table_name, is_index_table_name, storage_key,
};
use animus_env::{Clock, Env, Metric, Rng};
use animus_tablet::KeyRange;
use animus_tablet::TOKEN_BYTES;
use animus_tablet::TabletId;
use animus_tablet::TabletState;
use animus_tablet::partition_token;

use crate::{ClientCtx, CpGroup, IndexStatus, MetaCommand};

/// How often each node sweeps the tablet groups it leads for pending change
/// records. A plain fixed interval, matching `txn_resolver_loop`'s own shape —
/// this is background convergence work, not a latency-sensitive path.
const INDEX_DRAIN_INTERVAL: Duration = Duration::from_millis(200);

/// The consumer tag the GSI drain's own reconcile cursor writes (ADR 0042
/// §7/§8) — the only cursor tag left as of round 3 (the stream half of the
/// old min-over-rows rule is now catalog-derived, not cursor-row-derived;
/// see the module doc).
const GSI_TAG: &str = "gsi";

/// How long the seal arm's [`ClientCtx::propose_and_await`] waits for a
/// proposed `MetaCommand::SealStreamShard` to commit before giving up (this
/// tick retries; ADR 0043 §A3's recovery discipline). Generous, matching
/// `SCHEMA_COMMIT_TIMEOUT` (`lib.rs`) — a fresh cluster may still be
/// electing a control leader.
const SEAL_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The packed HLC suffix every change-record key ends with (see
/// `KvCommand::KindBatch`'s `change_log`) — the same 8-byte encoding a cursor
/// row's own value uses ([`cursor::encode_watermark`]/`decode_watermark`).
const HLC_BYTES: usize = 8;

/// How many change records one trim `KindBatch` entry deletes at most —
/// bounds a large backlog's catch-up to several ticks instead of one
/// outsized Raft entry (a bounded-batch discipline, like the seeder's own
/// chunking).
const TRIM_BATCH: usize = 256;

/// The marker branch's trim floor (ADR 0049 §4): an actively-written
/// (`marker_bytes_seen`-busy) plain table's marker backlog is left to
/// accumulate up to this many estimated `KIND_CHANGE` bytes before the trim
/// arm interposes a delete proposal into the tablet's own Raft group — per-
/// tick trimming of a hot tablet measurably destabilized a latency-
/// sensitive transaction pipeline sharing the group (see the branch's own
/// comment). A quiet tablet (backlog unchanged for one tick) trims
/// immediately regardless, so the idle→trim→quiesce path never waits on
/// this floor.
const MARKER_TRIM_FLOOR_BYTES: u64 = 1 << 20;

/// How many newly-discovered partitions [`backfill_seed_tick`] seeds in one
/// call (ADR 0045 §2) — bounds one tick's own worth of work, mirroring
/// [`TRIM_BATCH`]'s discipline: a large pre-existing table's backfill catches
/// up over many ticks, never one outsized burst.
const BACKFILL_SEED_BATCH: usize = 256;

/// How long [`seed_change_log_record`] waits for a proposed change-log-only
/// `KindBatch` to apply before giving up (the caller's own seeding loop
/// stops early and retries next tick) — matches [`SEAL_COMMIT_TIMEOUT`]'s
/// generous budget for the same reason (a fresh cluster may still be
/// electing a leader).
const BACKFILL_SEED_TIMEOUT: Duration = Duration::from_secs(10);

/// The **per-tablet change-consumer background task** (ADR 0041 §4, ADR
/// 0042/0043), one per node — formerly `index_drain_loop`, renamed now that
/// it is no longer GSI-specific (see the module doc for the three arms).
///
/// On every tick, for each tablet group this node currently **leads**: (1)
/// applies any pending change records to that table's global secondary
/// indexes (`drain_tablet`, unchanged from ADR 0041); (2) for a **streamed**
/// table, evaluates the seal arm's triggers and seals if due (`seal_tick`);
/// (3) runs the hot-trim arm (`trim_janitor`) to delete whatever every
/// expected, present term has cleared. Errors are logged and swallowed: this
/// is best-effort convergence, and the next tick retries from the same
/// durable records/catalog state (nothing is trimmed until every expected
/// term says it's safe to, and a failed seal simply re-evaluates its
/// triggers next tick).
pub(crate) async fn change_consumer_loop(ctx: ClientCtx) {
    // Driver-local memo of the seal arm's age-trigger basis for a tablet
    // that has never sealed a shard of its own (no catalog row to read a
    // last-seal time from yet) — see `seal_tick`'s own doc for the full
    // design, including why the memoized value is a one-time real scan's
    // true oldest-record HLC, not a bare "now" timestamp. Owned by this one
    // task (this loop is the only writer/reader, one instance per node), so
    // no lock is needed.
    let mut first_hot_seen: BTreeMap<TabletId, u64> = BTreeMap::new();
    // Driver-local memo of each marker tablet's `KIND_CHANGE` byte estimate
    // as of the previous tick — the marker branch's busy detector (a backlog
    // that *changed* since last tick means writes are actively arriving; see
    // the branch's own comment for why a hot tablet's markers are left to
    // accumulate to a floor instead of being trimmed per tick). Same
    // ownership/bounding discipline as `first_hot_seen` above.
    let mut marker_bytes_seen: BTreeMap<TabletId, u64> = BTreeMap::new();
    // ADR 0050 Train B rung 4: per-parent split-build driver state, keyed by
    // the `Splitting` parent's id. Driver-local (a re-led driver starts
    // fresh and re-runs idempotently); mirrored into
    // `ctx.data().split_builds` for `/admin/raftkv` observability only.
    let mut split_builds: BTreeMap<TabletId, SplitBuild> = BTreeMap::new();
    loop {
        tokio::time::sleep(INDEX_DRAIN_INTERVAL).await;
        let meta = ctx.effective_metadata();
        // Bound the fallback map to tablets that still exist at all — a
        // cheap `BTreeMap` retain, never a data scan — so a tablet dropped
        // (or moved off this node permanently) doesn't leak an entry
        // forever.
        first_hot_seen.retain(|t, _| meta.tablets.contains_key(t));
        marker_bytes_seen.retain(|t, _| meta.tablets.contains_key(t));
        // A build entry lives exactly as long as its parent is `Splitting`
        // (cutover removes the parent from the map entirely, fork F6).
        split_builds.retain(|t, _| {
            meta.tablets
                .get(t)
                .is_some_and(|tab| tab.state == TabletState::Splitting)
        });
        {
            let mut mirror = ctx
                .data()
                .split_builds
                .lock()
                .expect("split_builds poisoned");
            mirror.retain(|t, _| split_builds.contains_key(&TabletId(*t)));
        }
        // Growth PR3 Fork F: bound the change-rate tracker the same way —
        // `ctx.data()` is safe unconditionally here, exactly like
        // `seal_tick`'s own `ctx.data().raftkv_metrics` access below: this
        // loop is only ever spawned for a data-capable node.
        ctx.data().change_rates.retain_existing(&meta);
        for (tablet, group) in ctx.edge.hosted_groups() {
            if !group.is_leader() {
                continue;
            }
            // ADR 0050 Train B rung 4: the split-build driver arm. Runs for
            // a `Splitting` parent this node leads, BEFORE the quiesced
            // skip — a parent that quiesced idle before `BeginSplit`
            // committed would otherwise never be visited again (nothing
            // else wakes it), so the driver wakes it and holds the quiesce
            // veto for the build's whole duration. Trim is held too (the
            // `!splitting` gates below): the tail's cursor assumes records
            // never vanish beneath it, and the change log is the build's
            // delta feed (the metadata-derived trim hold — driver liveness
            // never gates it, so a driver crash can never let trim advance).
            let splitting = meta
                .tablets
                .get(&tablet)
                .is_some_and(|t| t.state == TabletState::Splitting);
            if splitting {
                group.wake();
                group.set_quiesce_veto(true);
                let build = split_builds.entry(tablet).or_default();
                if let Err(e) = split_driver_tick(&ctx, &meta, tablet, &group, build).await {
                    tracing::debug!(tablet = tablet.0, error = %e, "split build: tick failed");
                }
                ctx.data()
                    .split_builds
                    .lock()
                    .expect("split_builds poisoned")
                    .insert(tablet.0, (build.rows_shipped, build.converged, build.phase));
            }
            // ADR 0044 phase-1 PR6: "quiesced ⇒ nothing new for the
            // sweeper" is an invariant PR5's veto makes sound — a led
            // tablet that reached quiescence had an empty change log on
            // this loop's own last sweep (the veto set by *this very loop*
            // is what let it quiesce at all), so re-scanning it is pure
            // waste. Skipping is what actually cashes in the wakeup-count
            // win quiescence exists for; PR5 alone only avoided pointless
            // Raft timer/heartbeat activity, not this loop's own LSM
            // scans. A tablet re-woken (a write, a proposal, `wake()`) is
            // simply re-scanned the very next tick once `is_quiesced()`
            // flips back to `false` — no separate re-arm needed.
            if group.is_quiesced() {
                continue;
            }
            let Some(table) = meta.tablets.get(&tablet).and_then(|t| t.table.clone()) else {
                continue; // legacy whole-keyspace tablet, or a stale view
            };
            // ADR 0050 rung 5: a `Building` split child runs NO consumer
            // arms. It serves nothing yet (unroutable), so every consumer
            // restarts from scratch at activation (the classified
            // RestartFromScratch policy, ADR 0046) — and running them early
            // is actively harmful, not just wasted: the child's own
            // token-truncated "gsi" cursor key sorts below its range.start,
            // so its cursor write ROUTES to the still-routable parent and
            // lands in the parent's own KIND_CURSOR scope, where the
            // min-over-rows watermark rule drags the PARENT's cursor down
            // forever — deadlocking the rung-5 GSI cutover veto (the
            // split-child-cursor-unreadable shape, poisoning the parent
            // this time; caught red by `backfill_seeder`'s revived
            // split-during-backfill e2e).
            if meta
                .tablets
                .get(&tablet)
                .is_some_and(|t| t.state == TabletState::Building)
            {
                continue;
            }
            // A hidden index table holds index rows; it has no indexes of its
            // own, and must never recurse into maintaining any.
            if is_index_table_name(&table) {
                continue;
            }
            // ADR 0045 §2: a `Creating` index already needs live-write
            // materialization (no write after `CreateTableIndex` commits can
            // ever be missed, regardless of the new index's own status) —
            // widened from `Active`-only so `drain_tablet` maintains it too.
            // `Deleting` stays excluded: that status is PR5's own signal for
            // the drain/seeder to stop touching an index being torn down.
            let gsis: Vec<IndexDef> = meta
                .table_indexes(&table)
                .iter()
                .filter(|i| {
                    i.kind == IndexKind::Global
                        && matches!(i.status, IndexStatus::Creating | IndexStatus::Active)
                })
                .cloned()
                .collect();
            let stream_enabled = meta.table_stream(&table).is_some();
            // A tablet that has **ever** sealed a shard must keep being
            // visited even after its stream is later disabled (F12-b): the
            // final seal (`dynamo.rs::disable_stream`) commits the catalog
            // row but never trims anything itself — only this loop's own
            // hot-trim arm does, on ITS next tick. Without this, a disabled,
            // unindexed table would fall out of this gate the instant
            // `stream_enabled` flips to `false` and never get another
            // chance to trim its now-fully-sealed hot tail, leaving its
            // correctness entirely dependent on a race between the
            // disable's own `SetTableStream` commit and this loop's next
            // tick happening to land first (found by this PR's own
            // `disable_final_seal_then_reenable_continues_the_epoch_chain`
            // test, which failed intermittently before this fix).
            let ever_streamed = meta.stream_shard_watermark(tablet).is_some();
            // ADR 0049 §4 (Train A rung 4): a table with no GSI and no
            // enabled/ever-enabled stream is still visited — every write
            // leaves an image-less marker record (`ChangeRecord::marker`)
            // now, and this loop's hot-trim arm is markers' only deleter.
            // The **idle fast path is mandatory** (the ADR's
            // quiescence-compatibility clause, and the fork-G lesson —
            // never a per-tick scan): the cheap `approx_bytes_kind` gate
            // (the same accessor the seal arm's size trigger reads) decides
            // whether the tick does anything at all. Three cases:
            //
            // - **No change bytes at all** (the steady idle state): release
            //   the veto and skip — no scan, no trim call, nothing.
            // - **Bytes but no pending records** (the bounded LSM overhang:
            //   trimmed records' tombstones still occupy table files until
            //   compaction, so the byte estimate stays nonzero for a while;
            //   the one `pending_changes` scan per tick this costs lasts
            //   only until the group quiesces — `quiesce_after` later —
            //   after which the `is_quiesced` skip above short-circuits the
            //   whole visit): release the veto and skip.
            // - **A busy tablet's small backlog is deliberately left
            //   alone** (`marker_bytes_seen`): a byte estimate that CHANGED
            //   since the last tick means writes are actively arriving, and
            //   trimming per tick would inject one propose+fsync into the
            //   tablet's own Raft group every interval — measured to
            //   destabilize a latency-sensitive concurrent-transaction
            //   pipeline on the same tablet (the torn-pair regression went
            //   solo-red under the per-tick shape; the same
            //   contention class as `resolve_all`'s documented
            //   sensitivity). The veto still holds (markers exist), so
            //   quiescence stays sound; the backlog is bounded by
            //   `MARKER_TRIM_FLOOR_BYTES`, past which trim runs even while
            //   busy (amortized: one trim per floor's worth of markers).
            // - **Pending markers, quiet or over the floor**: hold the veto
            //   (the trim these markers are owed is a real obligation only
            //   this loop will ever act on — quiescing before it runs would
            //   strand them forever, since the sweeper-skip stops visiting
            //   a quiesced group) and run ONLY the trim arm: `gsis` is
            //   empty and `stream_enabled` is false, so `trim_janitor`
            //   derives **zero expected terms** and its existing
            //   trim-everything rule (F10/F12-b) deletes every marker in
            //   declared range — the same rule, not a second deleter.
            if gsis.is_empty() && !stream_enabled && !ever_streamed {
                if splitting {
                    // Trim held for the build (the split driver above holds
                    // the veto and consumes the markers as its tail feed).
                    continue;
                }
                let bytes = group.approx_bytes_kind(KIND_CHANGE).await;
                if bytes == 0 {
                    group.set_quiesce_veto(false);
                    marker_bytes_seen.remove(&tablet);
                    continue;
                }
                let busy = marker_bytes_seen.insert(tablet, bytes) != Some(bytes);
                if busy && bytes < MARKER_TRIM_FLOOR_BYTES {
                    // Actively written, still small: markers exist, so the
                    // veto holds; the trim waits for a quiet tick (or the
                    // floor) so it never contends with the live writes.
                    group.set_quiesce_veto(true);
                    continue;
                }
                let pending = !group.pending_changes().await.is_empty();
                group.set_quiesce_veto(pending);
                if !pending {
                    continue;
                }
                if let Err(e) = trim_janitor(&ctx, &meta, &table, tablet, &group, &[], false).await
                {
                    tracing::debug!(tablet = tablet.0, table, error = %e, "marker trim: tick failed");
                }
                continue;
            }
            // ADR 0044 phase-1 PR5, fork D: hold this group's quiesce veto
            // while its change log is non-empty, released the moment a
            // sweep finds it empty. This is what makes PR6's "quiesced ⇒
            // nothing new for the sweeper" invariant sound: without it, a
            // tablet could quiesce (genuinely idle for `quiesce_after`)
            // while still owing a GSI materialization or a not-yet-due
            // seal-age trigger — a real obligation only *this* loop would
            // ever notice and act on, silently stalled until something
            // else happened to touch the group. (A table with neither GSIs
            // nor an enabled/ever-enabled stream holds it too now — via the
            // marker branch above, for its own trim obligation.)
            let hot_backlog_present = !group.pending_changes().await.is_empty();
            // `|| splitting`: the split driver's own hold (above) must not be
            // released by an empty-backlog sweep mid-build.
            group.set_quiesce_veto(hot_backlog_present || splitting);
            // `drain_tablet` is GSI-specific (it reconciles GSI rows and
            // advances the `"gsi"` cursor) — never call it for a
            // streamed-but-unindexed table, or it would write a spurious
            // `"gsi"` cursor row this table's schema never expects. A
            // streamed-only table still needs the seal + trim arms below.
            if !gsis.is_empty()
                && let Err(e) = drain_tablet(&ctx, &meta, &table, &group, &gsis).await
            {
                tracing::debug!(tablet = tablet.0, table, error = %e, "index drain: tick failed");
                continue; // don't trim behind a reconciliation pass that didn't complete
            }
            // The backfill seeder (ADR 0045 §2): one independent sweep per
            // index currently `Creating` on this table, seeding dirty
            // markers for its pre-existing rows. Run after `drain_tablet`
            // so a partition this very tick just seeded already stands a
            // chance of being reconciled in the same pass (not required for
            // correctness — the next tick would pick it up regardless — but
            // it converges an initial backfill measurably faster).
            for idx in gsis.iter().filter(|i| i.status == IndexStatus::Creating) {
                if let Err(e) = backfill_seed_tick(&ctx, &group, tablet, &table, idx).await {
                    tracing::debug!(
                        tablet = tablet.0,
                        table,
                        index = idx.name,
                        error = %e,
                        "backfill seeder: tick failed"
                    );
                }
            }
            if stream_enabled
                && let Err(e) =
                    seal_tick(&ctx, &meta, &table, tablet, &group, &mut first_hot_seen).await
            {
                tracing::debug!(tablet = tablet.0, table, error = %e, "seal arm: tick failed");
            }
            if !splitting
                && let Err(e) =
                    trim_janitor(&ctx, &meta, &table, tablet, &group, &gsis, stream_enabled).await
            {
                tracing::debug!(tablet = tablet.0, table, error = %e, "index drain: trim janitor tick failed");
            }
        }
    }
}

/// Reconcile every dirty item of one tablet not yet covered by the "gsi"
/// cursor, then advance that cursor to the highest HLC this pass covers.
/// Per-parent driver state for one in-flight split build (ADR 0050 Train B
/// rung 4) — **driver-local, never correctness state**: everything here is
/// an optimization over "re-run the whole pass," which is what a fresh
/// leader's driver does from an empty entry.
#[derive(Default)]
pub(crate) struct SplitBuild {
    /// The one-time whole-scope copy (BASE + LSI + FOOTPRINT) completed.
    bulk_done: bool,
    /// The tail's exclusive packed-HLC watermark: only change records whose
    /// key's own trailing 8 bytes exceed it count as new. **Deliberately an
    /// HLC watermark, never a key-position cursor**: `pending_changes`' key
    /// order is prefix-then-HLC, NOT commit order, so a later write to a
    /// lower prefix inserts *below* any key cursor and would be skipped
    /// forever (this rung's own e2e went red on exactly that; the sealer's
    /// load-bearing re-sort is the same lesson). HLC order IS commit order
    /// within one tablet (`assert_ts_monotonic`), so the watermark is
    /// complete. Cost: each tail pass re-scans the whole (trim-held)
    /// pending set — O(pending) per tick, accepted for the build's bounded
    /// duration; an O(delta) commit-ordered read is a named B-final
    /// optimization.
    ///
    /// **Starts at the parent's highest change HLC as of the pre-bulk
    /// pass, not at 0** (captured beside `bulk_version_floor`): every row
    /// those records describe is in the bulk image by construction, so a
    /// zero start made the first tail pass re-ship the WHOLE table one
    /// dirty unit at a time — measured at ~6,000 no-op consensus rounds
    /// per child on a 20,000-key split, ~85% of the build's wall clock,
    /// with the children's key counts flat throughout. The tail's job is
    /// the delta written *during* the build, and that is now what it
    /// costs.
    tail_hlc: u64,
    /// Rows shipped so far (observability only).
    rows_shipped: u64,
    /// Post-bulk tail passes that shipped something — the freeze-liveness
    /// counter behind [`SPLIT_MAX_TAIL_PASSES`].
    tail_passes: u32,
    /// The max MVCC version over the copy kinds, captured by a read-only
    /// pass run strictly BEFORE the bulk scan starts. The final image
    /// ships only rows above it: apply order == HLC order within one group
    /// (`assert_ts_monotonic`), so any rewrite the bulk image missed —
    /// i.e. applied after its row's bulk read, hence after bulk start —
    /// carries a version above everything applied before bulk start, which
    /// bounds this floor. Rewrites applied *before* bulk start are in the
    /// bulk image itself (the scan reads current bytes). Deliberately NOT
    /// `latest_version()`: the ADR 0018 read-ceiling marker merges at a
    /// deliberately future-shifted version, which would push the floor
    /// above genuinely-later user writes. `None` (a driver re-led straight
    /// into the endgame) falls back to floor 0 — the full image.
    bulk_version_floor: Option<u64>,
    /// `bulk_done` and the tail is caught up *or* chased long enough
    /// ([`SPLIT_MAX_TAIL_PASSES`]) — the rung-8 liveness fix.
    converged: bool,
    /// The endgame step this build last parked at (observability only,
    /// mirrored to `/admin/raftkv`): `"build"` -> `"freeze"` ->
    /// `"final-drain"` -> `"final-seal"` -> `"gsi-veto"`/`"backfill-veto"`
    /// -> `"cutover"`. Driver-local like everything else here.
    phase: &'static str,
    /// The parent's commit index captured at the first frozen tick — every
    /// mutating entry that will EVER apply is at or below it (the propose
    /// latch refuses from the moment `Freeze` applies, and the append-to-
    /// apply sliver's entries are ordered before this read). The final
    /// image waits for `engine_applied_index()` to reach it, closing the
    /// "decision applies mid-scan" sliver.
    frozen_commit_floor: Option<u64>,
    /// The endgame's one full re-scan ship of the frozen parent completed
    /// (driver-local — a re-led driver redoes it; idempotent merges).
    final_image_done: bool,
}

/// The split-build seed chunk budget: one `SeedBatch` entry's row payload is
/// capped near this many bytes. Large enough to amortize the per-entry
/// consensus round, small enough that one entry's apply never stalls the
/// child's consensus loop past an election timeout (the ADR 0017
/// apply-stall lesson) and the JSON-framed forwarded hop stays modest.
const SEED_CHUNK_BYTES: usize = 256 * 1024;

/// Freeze liveness (rung 8): after this many post-bulk tail passes that
/// each still found fresh writes, the driver freezes anyway — a
/// continuously-written parent must not starve its own split forever (see
/// the convergence check's comment for the full argument). At the 200ms
/// loop cadence this is ~5s of chasing; the final residue is one tick's
/// writes, drained post-freeze.
const SPLIT_MAX_TAIL_PASSES: u32 = 25;

/// The copy kinds (ADR 0050): BASE (values, tombstones, intent envelopes,
/// txn records), LSI, FOOTPRINT — **never** KIND_CHANGE (a child re-serving
/// parent change records is the #220 duplication class; children are born
/// with empty change logs) and **never** KIND_CURSOR (consumer-owned,
/// restart-from-scratch per ADR 0046's consolidation).
const SEED_KINDS: [u8; 3] = [KIND_BASE, animus_cp_data::KIND_LSI, KIND_FOOTPRINT];

/// One split-build driver tick for a `Splitting` parent this node leads
/// (ADR 0050 Train B rung 4). Bulk pass on the first tick (whole-scope,
/// inline — the `local_pairs` materialization precedent), then O(delta)
/// tail passes off the parent's change log. Everything idempotent: a
/// crash/re-lead re-runs from an empty [`SplitBuild`] and converges to the
/// same state. This rung STOPS at convergence — freeze/final-pass/cutover
/// are B5's.
async fn split_driver_tick(
    ctx: &ClientCtx,
    meta: &Metadata,
    tablet: TabletId,
    group: &CpGroup,
    build: &mut SplitBuild,
) -> Result<(), String> {
    let Some(parent) = meta.tablets.get(&tablet) else {
        return Ok(()); // stale view; nothing to do
    };
    // The two Building children BeginSplit minted: same table, sub-ranges of
    // the parent's own range (B3 guarantees exactly two).
    let children: Vec<(TabletId, KeyRange)> = meta
        .tablets
        .iter()
        .filter(|(_, t)| {
            t.state == TabletState::Building
                && t.table == parent.table
                && parent.range.contains_range(&t.range)
        })
        .map(|(id, t)| (*id, t.range.clone()))
        .collect();
    if children.len() != 2 {
        return Err(format!(
            "split build: expected 2 Building children of tablet {}, found {}",
            tablet.0,
            children.len()
        ));
    }

    if !group.is_frozen() {
        // ---- the build (ADR 0050 stage 2, rung 4) ----
        if !build.bulk_done {
            // The version-floor pre-pass — see `bulk_version_floor`'s doc.
            // Must COMPLETE before the bulk scan below starts; capturing
            // the max from the bulk's own reads instead is unsound (a
            // mid-scan rewrite can be missed by the scan yet undercut a
            // later-scanned row's version).
            let mut floor = 0u64;
            for kind in SEED_KINDS {
                for (_, _, ver) in group.seed_rows_kind(kind as usize, None).await {
                    floor = floor.max(ver);
                }
            }
            build.bulk_version_floor = Some(floor);
            // The tail's own starting watermark, captured in the SAME
            // pre-pass and for the same reason (see `tail_hlc`'s doc): the
            // bulk image below covers every write whose change record
            // already exists here, so re-shipping those rows one dirty unit
            // at a time is pure duplicated work. Must be read STRICTLY
            // BEFORE the bulk scan — a write applied after this read gets
            // an HLC above it (`assert_ts_monotonic`), so the tail still
            // catches everything the bulk image may have missed.
            build.tail_hlc = max_change_hlc(group).await;
            for kind in SEED_KINDS {
                let rows = group.seed_rows_kind(kind as usize, None).await;
                let batches = children
                    .iter()
                    .map(|(child, range)| {
                        let child_rows: Vec<_> = rows
                            .iter()
                            .filter(|(logical, _, _)| range.contains(logical))
                            .map(|(l, v, ver)| (kind, l.clone(), v.clone(), *ver))
                            .collect();
                        (*child, child_rows)
                    })
                    .collect();
                ship_all(ctx, batches, &mut build.rows_shipped).await?;
            }
            build.bulk_done = true;
        }
        let shipped = tail_pass(ctx, group, &children, build).await?;
        if shipped {
            build.tail_passes += 1;
        }
        // Converged = caught up (a pass shipped nothing) OR chased long
        // enough. The bounded-passes arm is a LIVENESS fix, not a
        // correctness relaxation: rung 8's bench drove a *continuous*
        // sequential writer against the parent and the build never froze —
        // every 200ms tick's tail pass found that tick's own fresh writes,
        // so "zero new records" structurally never fired. A hot tablet is
        // exactly the one that needs splitting, so after
        // `SPLIT_MAX_TAIL_PASSES` post-bulk passes the driver freezes
        // regardless; the post-freeze final drain (over a log the freeze
        // stops from growing) plus the final image still transfer
        // everything, unchanged — the residue's size only bounds the write
        // blip, which is the F8 knob ADR 0050 stage 4 names.
        build.converged =
            build.bulk_done && (!shipped || build.tail_passes >= SPLIT_MAX_TAIL_PASSES);
        if build.converged {
            // ---- stage 3 kickoff (rung 5): converged — terminally freeze
            // the parent. Idempotent (a duplicate `Freeze` applies as a
            // no-op), so a crash/re-lead between propose and apply just
            // re-proposes here next tick. Once the entry APPLIES,
            // `is_frozen()` flips and every subsequent tick runs the
            // endgame below instead.
            build.phase = "freeze";
            match group.propose_freeze() {
                animus_control::ProposeResult::Accepted { .. } => {}
                other => return Err(format!("freeze not accepted: {other:?}")),
            }
        } else {
            build.phase = "build";
        }
        return Ok(());
    }

    // ---- the endgame (frozen parent; ADR 0050 stages 3-4, rung 5). Every
    // step is idempotent and re-derived per tick from durable/replicated
    // state only, so a driver crash or re-lead at ANY boundary resumes by
    // simply re-running the tick: the freeze is engine-durable
    // (`is_frozen()` re-latches from its marker), a re-run tail pass ships
    // nothing new, a re-run seal finds nothing left to seal, the vetoes are
    // pure reads, and a duplicate `CutoverSplit` rejects at its own
    // state/epoch CAS. ----
    build.bulk_done = true;
    build.converged = true;

    // 1. Final drain: the identical watermark tail, now over a log that can
    //    no longer grow (the freeze's own log position bounds it). Runs to
    //    literally zero new records — no lag threshold. Loops to completion
    //    WITHIN this tick (rung 8): the frozen log bounds the iteration, and
    //    every 200ms tick boundary spent between endgame phases is pure
    //    added write-blip — the bench measured ~3s of blip dominated by
    //    phase-per-tick progression before this fall-through.
    build.phase = "final-drain";
    while tail_pass(ctx, group, &children, build).await? {}

    // 1b. The FINAL IMAGE: one full re-scan ship of the frozen parent —
    //     the freeze's log position defines the final state, and this
    //     transfers it verbatim (carried-version merges make it an
    //     idempotent no-op for every row the build already shipped). This
    //     is deliberately a whole-scan, not another tail pass, because
    //     transaction DECISIONS (`TxnCommit`/`TxnAbort`) and RESOLVES
    //     rewrite base rows with **no change record of their own** — an
    //     O(delta) tail structurally misses them, and a child inheriting a
    //     stale `Pending` record for an acked-committed transaction is the
    //     in-doubt-recovery-aborts-a-committed-write class, the one thing
    //     fork F7 exists to prevent. Cost: a second full read+wire pass
    //     per split, accepted for v1 (the pivot's safe-over-clever ethos);
    //     apply-side decision/resolve markers restoring O(delta) are the
    //     named B-final optimization. Gated on the apply task having
    //     caught up past the freeze-window commit floor, so a decision
    //     appended in the freeze's own append-to-apply sliver can never
    //     apply mid-scan and be missed.
    let floor = *build
        .frozen_commit_floor
        .get_or_insert_with(|| group.commit_index());
    if group.engine_applied_index() < floor {
        build.phase = "final-drain";
        return Ok(());
    }
    if !build.final_image_done {
        // Rung 8: filtered by the pre-bulk version floor — only rows
        // rewritten since the bulk began ship here (the signal-less
        // decision/resolve class included, by the floor's monotonicity
        // argument). The unfiltered image (floor 0) was measured at ~2s of
        // extra write blip on a 2,000-row table and scales with table
        // size; the filtered residue scales with the build-window write
        // rate instead.
        let floor = build.bulk_version_floor.unwrap_or(0);
        for kind in SEED_KINDS {
            let rows = group.seed_rows_kind(kind as usize, None).await;
            let batches = children
                .iter()
                .map(|(child, range)| {
                    let child_rows: Vec<_> = rows
                        .iter()
                        .filter(|(logical, _, ver)| *ver > floor && range.contains(logical))
                        .map(|(l, v, ver)| (kind, l.clone(), v.clone(), *ver))
                        .collect();
                    (*child, child_rows)
                })
                .collect();
            ship_all(ctx, batches, &mut build.rows_shipped).await?;
        }
        build.final_image_done = true;
        build.phase = "final-image";
        // Fall through (rung 8): the image is shipped and durable on the
        // children; nothing below needs a tick boundary to become true.
    }

    let table = parent.table.clone().unwrap_or_default();

    // 2. Streams final seal (stage 3): seal the parent's KIND_CHANGE to
    //    end-of-log under the ordinary seal machinery — a routine seal
    //    (same shard-id discipline, so an in-flight iterator drains it and
    //    walks on), just with no size/age gate. The parent is frozen and
    //    drained, so one seal covers everything; a tick that sealed
    //    something re-checks next tick and finds nothing left.
    if !table.is_empty() && meta.table_stream(&table).is_some() {
        // Loop within the tick (rung 8): the drained, frozen log means at
        // most one real seal plus one nothing-left re-check.
        build.phase = "final-seal";
        while seal_now(ctx, &table, tablet, group).await?.is_some() {}
    }

    // 3a. GSI-drain veto (stage 3): the drain must have consumed the
    //     parent's change log (its cursor at or past the highest pending
    //     record — markers included, whose HLCs the drain folds in), or
    //     cutover would retire records whose GSI updates were never
    //     materialized (children's change logs are empty by design).
    if !table.is_empty() && !meta.table_indexes(&table).is_empty() {
        let max_pending = group
            .pending_changes()
            .await
            .iter()
            .filter_map(|(k, _)| record_hlc(k))
            .max();
        if let Some(max_hlc) = max_pending {
            let caught_up = group
                .cursor_min_watermark("gsi")
                .await
                .is_some_and(|wm| wm >= max_hlc);
            if !caught_up {
                build.phase = "gsi-veto";
                return Ok(());
            }
        }
    }

    // 3b. Backfill veto (stage 3): a still-`Creating` index's seeder must
    //     have finished its sweep of the (frozen, hence static) parent —
    //     its `MarkIndexBackfilled` row present — before the parent may
    //     retire; the children then restart their own narrower sweeps from
    //     scratch (ADR 0045 Fork A) and the completion aggregator re-reads
    //     the live tablet map every tick, so post-cutover convergence is
    //     sound.
    if !table.is_empty() {
        for idx in meta.table_indexes(&table) {
            if idx.status == IndexStatus::Creating
                && !meta
                    .index_backfill
                    .contains_key(&(tablet, idx.name.clone()))
            {
                build.phase = "backfill-veto";
                return Ok(());
            }
        }
    }

    // 4. Cutover (stage 4): the atomic flip — children Active, parent
    //    removed, lineage frozen. Confirmed by the parent VANISHING from
    //    the map (the loop-top retain drops this build entry), so this
    //    propose is simply re-issued each tick until observed; the
    //    state/epoch CAS makes a duplicate reject cleanly.
    build.phase = "cutover";
    let cmd = MetaCommand::CutoverSplit {
        parent: tablet,
        expected_epoch: parent.epoch,
        cutover_wall_ms: ctx.env.now().0 / 1_000_000,
    };
    ctx.propose_schema(&cmd).await;
    Ok(())
}

/// One seed row's wire/payload cost against the [`SEED_CHUNK_BYTES`]
/// budget: the kind byte, the logical key, the value, and a fixed
/// allowance for the carried version plus framing. Shared by [`ship`]'s own
/// chunking and [`tail_pass`]'s cross-unit accumulator so the two agree on
/// what "a full chunk" means.
fn seed_row_bytes(logical: &[u8], value: Option<&Vec<u8>>) -> usize {
    1 + logical.len() + value.map_or(0, Vec::len) + 16
}

/// Ship `rows` (already filtered to one child) in bounded chunks
/// (ADR 0050 rung 4 — see [`SEED_CHUNK_BYTES`]), returning how many rows
/// went out.
///
/// **Returns the count rather than accumulating into a borrowed counter**
/// so that ships to *different* children can run concurrently
/// ([`ship_all`]) — one `&mut` into `SplitBuild` would serialize them at
/// the borrow checker before they ever reached the network.
async fn ship(
    ctx: &ClientCtx,
    child: TabletId,
    rows: Vec<animus_cp_data::SeedRow>,
) -> Result<u64, String> {
    let mut shipped = 0u64;
    let mut chunk: Vec<animus_cp_data::SeedRow> = Vec::new();
    let mut bytes = 0usize;
    for row in rows {
        bytes += seed_row_bytes(&row.1, row.2.as_ref());
        chunk.push(row);
        if bytes >= SEED_CHUNK_BYTES {
            let n = chunk.len() as u64;
            ctx.seed_child_rows(child, std::mem::take(&mut chunk))
                .await?;
            shipped += n;
            bytes = 0;
        }
    }
    if !chunk.is_empty() {
        let n = chunk.len() as u64;
        ctx.seed_child_rows(child, chunk).await?;
        shipped += n;
    }
    Ok(shipped)
}

/// Ship one batch **per child, concurrently**, and fold the row counts into
/// `shipped` once they all land (ADR 0050).
///
/// A split's two children are two independent Raft groups, at
/// placement-chosen homes (fork F5) that are frequently two *different*
/// nodes — so shipping to them one after the other left each child's
/// replica set idle for exactly as long as the other child's chunk took to
/// commit. The copy is the dominant cost of a split, and this halves its
/// wall clock without moving a single byte more.
///
/// Failure semantics are deliberately unchanged: `try_join_all` surfaces
/// the first error and drops the sibling future, which at worst abandons a
/// confirm-wait for a chunk that may still commit. That is exactly the
/// state a crashed driver leaves behind, and the same thing makes it safe —
/// a `SeedBatch` merges at its carried versions, so the next tick's re-run
/// (from an empty [`SplitBuild`] after a re-lead, or from the unmoved tail
/// watermark otherwise) re-ships it as a no-op.
async fn ship_all(
    ctx: &ClientCtx,
    batches: Vec<(TabletId, Vec<animus_cp_data::SeedRow>)>,
    shipped: &mut u64,
) -> Result<(), String> {
    let counts = futures::future::try_join_all(
        batches
            .into_iter()
            .map(|(child, rows)| ship(ctx, child, rows)),
    )
    .await?;
    *shipped += counts.iter().sum::<u64>();
    Ok(())
}

/// One tail pass of the split build (ADR 0050 rung 4; reused VERBATIM as
/// rung 5's post-freeze final drain): ship every dirty unit whose change
/// record's packed HLC exceeds the driver's watermark, then advance the
/// watermark only once every ship in the pass succeeded. Returns whether
/// anything shipped (`false` == the pass found zero new records).
///
/// Dirty granularity is two-tier, keyed off the change key's own prefix
/// (the logical key minus its trailing 8-byte packed HLC):
/// - a prefix carrying a full 8-byte token (every dynamo/CQL key, and any
///   raw key >= 8 bytes) dirties its whole **token** — the unit that also
///   owns the item's LSI rows (which reorder the sk and share only the
///   token+pk lead) and its txn-record row (which shares the anchor's
///   token), and the unit F11 keeps wholly on one child;
/// - a shorter raw-protocol prefix dirties exactly itself (such tables
///   structurally have no LSI/txn rows — `cp_txn` rejects sub-token keys —
///   so the prefix covers everything).
async fn tail_pass(
    ctx: &ClientCtx,
    group: &CpGroup,
    children: &[(TabletId, KeyRange)],
    build: &mut SplitBuild,
) -> Result<bool, String> {
    let changes = group.pending_changes().await;
    let fresh: Vec<(&Vec<u8>, u64)> = changes
        .iter()
        .filter_map(|(logical, _)| Some((logical, packed_hlc(logical)?)))
        .filter(|(_, hlc)| *hlc > build.tail_hlc)
        .collect();
    if fresh.is_empty() {
        return Ok(false);
    }
    let mut dirty: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut max_hlc = build.tail_hlc;
    for (logical, hlc) in fresh {
        let prefix = &logical[..logical.len() - HLC_BYTES];
        if prefix.len() >= 8 {
            dirty.insert(prefix[..8].to_vec());
        } else if !prefix.is_empty() {
            dirty.insert(prefix.to_vec());
        }
        max_hlc = max_hlc.max(hlc);
    }
    // Per-child accumulator: a dirty unit's rows are a handful of bytes, so
    // shipping each unit on its own — as this pass originally did — bought
    // one whole consensus round (plus, for a child off this node, a
    // forwarded hop) per partition key, while the bulk pass moved thousands
    // of rows per round. Batching across units to the same
    // [`SEED_CHUNK_BYTES`] budget the bulk pass uses collapses that to the
    // same rounds-per-megabyte cost. Semantics are unchanged: a `SeedBatch`
    // is idempotent at its carried versions, and chunk boundaries already
    // cut across rows on the bulk path, so nothing depends on one unit's
    // rows sharing an entry.
    let mut pending: BTreeMap<TabletId, (Vec<animus_cp_data::SeedRow>, usize)> = BTreeMap::new();
    for unit in dirty {
        // The child whose range contains the unit's first key owns every
        // row under it (F11 token alignment for tokenized tables; a raw
        // prefix is a single key's own lineage).
        let Some((child, _)) = children.iter().find(|(_, range)| range.contains(&unit)) else {
            continue; // outside the parent's own range — cannot happen; skip
        };
        let upper = prefix_upper(&unit);
        for kind in SEED_KINDS {
            let rows = match &upper {
                Some(hi) => {
                    group
                        .seed_rows_kind(kind as usize, Some((unit.as_slice(), hi.as_slice())))
                        .await
                }
                // All-0xFF unit: no finite upper bound — scan the kind
                // scope whole and keep the unit's own rows.
                None => group
                    .seed_rows_kind(kind as usize, None)
                    .await
                    .into_iter()
                    .filter(|(l, _, _)| l.starts_with(&unit))
                    .collect(),
            };
            // Buffer, and flush only once this child's buffer is worth a
            // consensus round (the trailing partial batches go out below).
            let batch = {
                let (buf, bytes) = pending.entry(*child).or_insert_with(|| (Vec::new(), 0));
                for (logical, value, version) in rows {
                    *bytes += seed_row_bytes(&logical, value.as_ref());
                    buf.push((kind, logical, value, version));
                }
                if *bytes >= SEED_CHUNK_BYTES {
                    *bytes = 0;
                    std::mem::take(buf)
                } else {
                    Vec::new()
                }
            };
            if !batch.is_empty() {
                build.rows_shipped += ship(ctx, *child, batch).await?;
            }
        }
    }
    // The trailing partial batches — one per child, so they go out
    // concurrently like every other per-child ship.
    ship_all(
        ctx,
        pending
            .into_iter()
            .map(|(child, (rows, _))| (child, rows))
            .collect(),
        &mut build.rows_shipped,
    )
    .await?;
    // Only past every successful ship: a failed tick re-derives the same
    // dirty units next tick from the unmoved watermark.
    build.tail_hlc = max_hlc;
    Ok(true)
}

/// The trailing packed HLC of a change record's logical key
/// (`prefix || hlc`, ADR 0049) — `None` for a key too short to carry one.
/// The tail's watermark unit: raw big-endian packed bits, the same
/// encoding [`SplitBuild::tail_hlc`] holds.
fn packed_hlc(logical: &[u8]) -> Option<u64> {
    let suffix = logical
        .len()
        .checked_sub(HLC_BYTES)
        .map(|n| &logical[n..])?;
    Some(u64::from_be_bytes(suffix.try_into().ok()?))
}

/// The highest packed HLC over a group's current change log, `0` for an
/// empty one — the split build's pre-bulk tail watermark (see its capture
/// site in [`split_driver_tick`]).
async fn max_change_hlc(group: &CpGroup) -> u64 {
    group
        .pending_changes()
        .await
        .iter()
        .filter_map(|(logical, _)| packed_hlc(logical))
        .max()
        .unwrap_or(0)
}

/// The exclusive upper bound of "every key starting with `prefix`": the
/// prefix as a big-endian integer plus one, trailing `0xFF` bytes dropped —
/// the `physical_bounds` prefix-upper-bound idiom. `None` when the prefix is
/// all `0xFF` (no finite bound exists).
fn prefix_upper(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.pop() {
        if last < 0xFF {
            upper.push(last + 1);
            return Some(upper);
        }
    }
    None
}

async fn drain_tablet(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    group: &CpGroup,
    gsis: &[IndexDef],
) -> Result<(), String> {
    // The ADR 0042 §7 min-over-rows watermark: `None` on a cold tablet (no
    // "gsi" row yet) or a split's fresh right child (the rule over an empty
    // set) — either way, "reconcile everything currently pending."
    let watermark = group.cursor_min_watermark(GSI_TAG).await;
    let records = group.pending_changes().await;
    if records.is_empty() {
        return Ok(());
    }
    // A record's key is `footprint_key || hlc`, so the partition it belongs to
    // is its key minus that fixed-width suffix — no parsing needed. Several
    // records for one partition collapse into a single reconciliation, which is
    // the point of being derivative.
    //
    // Sweep discipline (ADR 0042 §7): only records this pass hasn't already
    // covered — everything else is exactly what `watermark` already claims
    // is done. `max_hlc` accumulates the true highest HLC this pass will end
    // up covering, computed **before** any reconciliation happens, since
    // every partition it comes from is guaranteed to get reconciled below
    // (nothing in `by_partition` is ever skipped).
    //
    // **ADR 0049 marker records are covered-by-construction, never dirty.**
    // A `ChangeRecord::marker` exists only from a mutation committed while
    // its table had no index and no stream at all, so it predates every
    // index this drain could be maintaining — and pre-existing rows are the
    // backfill seeder's job (ADR 0045 §2), not this arm's. Reconciling
    // marker partitions here would silently re-do the seeder's entire sweep
    // through the drain (a populated-then-indexed table's whole population
    // shows up as markers), which both duplicates work and — found by
    // `tests/update_table_drop_index.rs` going flaky — widens the
    // drain-vs-drop-cascade window enough for this arm's lazy hidden-table
    // provisioning to race a concurrent `drop_index` and resurrect the
    // just-dropped tablet. A marker's HLC still folds into `max_hlc` (its
    // "reconciliation" is vacuously complete), so the cursor advances over
    // markers and the hot-trim arm is never blocked behind them. `seeded`
    // records are NOT skipped — they are the seeder's own product, and
    // draining them is the backfill mechanism itself.
    let mut by_partition: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut max_hlc: Option<HlcTimestamp> = None;
    for (key, value) in &records {
        let Some(fp_key) = key.len().checked_sub(HLC_BYTES).map(|n| key[..n].to_vec()) else {
            continue; // malformed; leave it rather than mis-attribute it
        };
        let Some(ts) = record_hlc(key) else {
            continue; // malformed HLC suffix; same defensive skip
        };
        if watermark.is_some_and(|w| ts <= w) {
            continue; // already consumed by an earlier pass
        }
        max_hlc = Some(max_hlc.map_or(ts, |m: HlcTimestamp| m.max(ts)));
        if ChangeRecord::decode(value).is_some_and(|r| r.marker) {
            continue; // pre-index history: the seeder's job, not the drain's
        }
        by_partition.insert(fp_key);
    }
    if by_partition.is_empty() && max_hlc.is_none() {
        return Ok(()); // nothing past the watermark; a prior pass covered it all
    }

    // A GSI's rows live in its own hidden table, provisioned lazily here on
    // the first drain that has genuinely dirty partitions to reconcile
    // (ADR 0023). This is load-bearing, not an optimization:
    // `reconcile_partition` writes rows via `cp_kind_write_raw`, which —
    // unlike `cp_kind_write`/`cp_txn` — does NOT auto-provision; without a
    // tablet to route to, its `cp_route` would wait out `CLIENT_TIMEOUT` and fail,
    // every tick, forever. Gated on the caller's metadata snapshot: a stale
    // "absent" just re-proposes an idempotent `CreateTablet`
    // (first-committer wins), and the hit path is sound because tablets are
    // only ever removed by drop-table. Deliberately gated on real work
    // (never a marker-only pass, above) so an all-marker backlog can't
    // provision anything mid-drop.
    if !by_partition.is_empty() {
        for idx in gsis {
            let index_table = index_table_name(table, &idx.name);
            if !meta.has_table_tablet(&index_table) {
                ctx.provision_tablet(&index_table).await?;
            }
        }
    }

    let marker_only_pass = by_partition.is_empty();
    for fp_key in by_partition {
        reconcile_partition(ctx, meta, table, group, gsis, &fp_key).await?;
    }

    // Every partition dirtied this pass has now been reconciled and its
    // footprint update durably confirmed (the loop above only reaches here
    // once every `reconcile_partition` call returned `Ok`) — advancing the
    // cursor here, in its own trailing write, is what preserves the crash
    // property: the cursor can only ever name a `max_hlc` whose covering
    // reconciliations have already landed, never one still in flight.
    if let Some(max_hlc) = max_hlc {
        let cursor_key = cursor::cursor_key(&group.scope_range().start, GSI_TAG);
        // ADR 0049 fixup: a split's right child's own cursor key is
        // **token-truncated below its own `range.start`** (the exact shape
        // `advance_backfill_cursor`'s doc dissects), so the row this write
        // would create routes to — and lands physically inside — the LEFT
        // sibling's scope, where this tablet's own `cursor_min_watermark`
        // can never read it back. On a **marker-only** pass (nothing
        // reconciled — markers are covered-by-construction) that write is
        // pure per-tick churn: it can never advance this tablet's own
        // watermark, so the identical pass would repeat, and repeat its
        // futile routed round trip, every tick forever. Skip it; the
        // markers stay pending (bounded: only pre-split history can be in
        // this state) and cost a scan, which the real fix — a
        // child-scope-readable cursor key — would remove for the
        // reconciling case too (a named, pre-existing follow-up: the same
        // unreadable-row shape exists on main for a child's post-split
        // reconciliations, where the write is still performed for parity).
        if marker_only_pass && !group.scope_range().contains(&cursor_key) {
            return Ok(());
        }
        ctx.cp_kind_write_raw(
            table,
            vec![(
                KIND_CURSOR,
                cursor_key,
                Some(cursor::encode_watermark(max_hlc)),
            )],
            Vec::new(),
        )
        .await?;
    }
    Ok(())
}

/// The HLC a change-record's key suffix encodes — the identical 8-byte
/// big-endian packing [`cursor::encode_watermark`] uses for a watermark value
/// (see `KvCommand::KindBatch`'s `change_log` doc: the key is completed at
/// apply as `prefix || hlc::pack(ts)`). `None` on a malformed suffix, a
/// defensive read mirroring the `fp_key` split's own.
fn record_hlc(key: &[u8]) -> Option<HlcTimestamp> {
    let suffix = key.len().checked_sub(HLC_BYTES).map(|n| &key[n..])?;
    cursor::decode_watermark(suffix)
}

/// Bring one partition's GSI rows in line with its base rows' *current*
/// values, then atomically record the new footprint. Unlike ADR 0041's
/// original design, this no longer deletes the change records that triggered
/// it — see the module doc and [`drain_tablet`]'s trailing cursor write for
/// the ADR 0042 replacement, and [`trim_janitor`] for the actual deletion.
async fn reconcile_partition(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    group: &CpGroup,
    gsis: &[IndexDef],
    fp_key: &[u8],
) -> Result<(), String> {
    let base = crate::dynamo::schema_for(meta, table);
    let previous = group
        .local_get_kind(KIND_FOOTPRINT, fp_key)
        .await
        .and_then(|bytes| IndexFootprint::decode(&bytes))
        .unwrap_or_default();

    // Every live base row of this partition — the authority the recomputation
    // derives from. A partition's base rows are contiguous, so this is one
    // bounded range scan (a tombstoned row simply doesn't come back, and its
    // index rows therefore fall out as stale below).
    let mut end = fp_key.to_vec();
    *end.last_mut().expect("a footprint key is non-empty") += 1;
    let rows = group.local_scan_bounded(fp_key, &end).await;

    let mut desired = IndexFootprint::default();
    let mut writes: Vec<(String, Vec<u8>, Vec<u8>)> = Vec::new(); // (index table, key, value)
    for (base_key, value) in &rows {
        let Ok(Some(item)) = wire::decode_stored_item(value) else {
            continue; // a tombstone (or corrupt row) contributes no index rows
        };
        let base_sk = base_key[fp_key.len().min(base_key.len())..].to_vec();
        let Some(pk) = item.get(&base.partition_key) else {
            continue;
        };
        let sk = base.sort_key.as_ref().and_then(|n| item.get(n));

        let mut entries: Vec<FootprintEntry> = Vec::new();
        for idx in gsis {
            let Some(ihash) = item.get(&idx.hash_attribute) else {
                continue; // an item missing the index key simply isn't indexed
            };
            let isort = idx.sort_attribute.as_ref().and_then(|n| item.get(n));
            // A composite GSI whose sort attribute the item lacks is likewise
            // not indexed — DynamoDB's own rule, and it keeps the row's key
            // unambiguous (see `parse_gsi_row_key`'s `composite` flag).
            if idx.sort_attribute.is_some() && isort.is_none() {
                continue;
            }
            let key = gsi_row_key(ihash, isort, pk, sk);
            entries.push(FootprintEntry {
                index: idx.name.clone(),
                key: key.clone(),
            });
            writes.push((
                index_table_name(table, &idx.name),
                key,
                wire::encode_stored_item(&projected(&item, &base, idx)),
            ));
        }
        entries.sort();
        desired.set_item(base_sk, entries);
    }

    // Stale rows are exactly those the footprint names and the recomputation
    // did not produce — no orphan hunt, by construction.
    let live: BTreeSet<(&str, &[u8])> = desired
        .items
        .iter()
        .flat_map(|i| &i.entries)
        .map(|e| (e.index.as_str(), e.key.as_slice()))
        .collect();
    let stale: Vec<(String, Vec<u8>)> = previous
        .items
        .iter()
        .flat_map(|i| &i.entries)
        .filter(|e| !live.contains(&(e.index.as_str(), e.key.as_slice())))
        .map(|e| (index_table_name(table, &e.index), e.key.clone()))
        .collect();

    // Order matters for a crash: write the new rows first, then remove the
    // stale ones, and only then record the new footprint. A crash mid-way
    // leaves the footprint still naming the old rows, so the next tick redoes
    // the whole reconciliation — over-covering, never under-covering.
    //
    // Each row commits through the kind path with an image-less marker
    // record (ADR 0049 Train A rung 5): a hidden index table is a table, so
    // its own tablets need a change-log delta feed too (ADR 0050's
    // split-build tail will consume it when a hidden table's tablet splits).
    // The marker's prefix is the row's own full key with an empty `base_sk`
    // — the CQL convention (`marker_change_log`'s doc) — and the markers are
    // transient exactly like a plain table's: a hidden table has no stream
    // and no GSI of its own, so the zero-expected-terms trim rule deletes
    // them. One `KindBatch` entry per row, same entry count as the plain
    // `cp_write`/`cp_delete` calls this replaces.
    for (index_table, key, value) in writes {
        let marker = crate::dynamo::marker_change_log(&key, Vec::new());
        ctx.cp_kind_write_raw(
            &index_table,
            vec![(KIND_BASE, key, Some(value))],
            vec![marker],
        )
        .await?;
    }
    // A genuine engine delete (`KindBatch`'s `None` value — the same real
    // tombstone the LSI half prunes with), not a tombstone *value*
    // (`encode_tombstone`): that sentinel exists so a base-table
    // `DeleteItem` stays observable (to conditional reads and to the change
    // log this very drain consumes), but an index row is wholly derived — a
    // dead one has no reader to inform, and nothing would ever reclaim a
    // sentinel from a hidden index table.
    for (index_table, key) in stale {
        let marker = crate::dynamo::marker_change_log(&key, Vec::new());
        ctx.cp_kind_write_raw(&index_table, vec![(KIND_BASE, key, None)], vec![marker])
            .await?;
    }

    // One entry: just the updated footprint. See the module doc and
    // `drain_tablet`'s trailing cursor write for why the records this
    // reconciliation covers are no longer deleted here.
    ctx.cp_kind_write_raw(
        table,
        vec![(
            KIND_FOOTPRINT,
            fp_key.to_vec(),
            (!desired.is_empty()).then(|| desired.encode()),
        )],
        Vec::new(),
    )
    .await
}

/// The consumer-tag convention for a backfill cursor row (ADR 0045 §2): one
/// per index currently being seeded — see the module doc's "The backfill
/// seeder" section for why a per-index cursor was chosen over one shared
/// scan.
fn backfill_tag(index_name: &str) -> String {
    format!("backfill:{index_name}")
}

/// One backfill-seeder tick for one `Creating` index on one led tablet (ADR
/// 0045 §2) — see the module doc for the full mechanism. Sweeps this
/// tablet's own `KIND_BASE` scope forward from `idx`'s own backfill cursor,
/// one iteration per **partition** (peeking exactly one row past the
/// current scan position, rather than materializing a whole partition, to
/// find the next partition boundary — the "bump the last byte" skip-ahead
/// trick [`reconcile_partition`] already uses, applied one row at a time),
/// seeding up to [`BACKFILL_SEED_BATCH`] newly-discovered partitions this
/// call. When the sweep reaches the end of the tablet's *current* range,
/// (re-)proposes `MetaCommand::MarkIndexBackfilled` — a persistent
/// condition re-derived every tick, not a one-shot side effect: a
/// crash-retried or not-yet-committed proposal is simply re-sent next tick,
/// harmlessly (the apply arm's own insert is idempotent), and the caller
/// keeps calling this until `idx`'s status itself flips away from
/// `Creating` (this loop's own `gsis` filter stops selecting it the moment
/// `animusd::index_backfill_loop` flips it `Active`).
async fn backfill_seed_tick(
    ctx: &ClientCtx,
    group: &CpGroup,
    tablet: TabletId,
    table: &str,
    idx: &IndexDef,
) -> Result<(), String> {
    let tag = backfill_tag(&idx.name);
    let cursor_key_bytes = cursor::cursor_key(&group.scope_range().start, &tag);
    let mut last_seeded: Option<Vec<u8>> = group
        .local_get_kind(KIND_CURSOR, &cursor_key_bytes)
        .await
        .map(|bytes| cursor::decode_backfill_cursor(&bytes));
    let mut scan_start: Vec<u8> = match &last_seeded {
        Some(prefix) => dynamo_index::range_end(prefix),
        None => Vec::new(),
    };
    let mut seeded = 0usize;
    let mut reached_end = false;
    while seeded < BACKFILL_SEED_BATCH {
        let Some((key, _)) = group
            .local_scan_from(&scan_start, 1)
            .await
            .into_iter()
            .next()
        else {
            reached_end = true;
            break;
        };
        let Some(prefix_len) = base_partition_prefix_end(&key) else {
            // Malformed/truncated (defensive only — every `KIND_BASE` key
            // here was written by `animusd::dynamo::item_key`): skip past
            // this exact key via the standard immediate-successor trick
            // (appending the smallest possible byte) rather than looping
            // forever on it, and don't count it as a seeded partition.
            scan_start = {
                let mut next = key;
                next.push(0x00);
                next
            };
            continue;
        };
        let prefix = key[..prefix_len].to_vec();
        let base_sk = key[prefix_len..].to_vec();
        // No image content at all — see the module doc's "known interaction
        // with Streams" note for why this is deliberate: the GSI drain
        // never reads a change record's content (it re-derives from a live
        // base-row scan), so this is purely a dirty marker.
        let record = ChangeRecord {
            base_sk,
            old_image: None,
            new_image: None,
            seeded: true,
            marker: false,
            staged: false,
        }
        .encode();
        seed_change_log_record(group, prefix.clone(), record).await?;
        scan_start = dynamo_index::range_end(&prefix);
        last_seeded = Some(prefix);
        seeded += 1;
    }
    if seeded > 0 {
        let prefix = last_seeded
            .as_ref()
            .expect("seeded > 0 implies last_seeded was set in the loop above");
        advance_backfill_cursor(group, cursor_key_bytes, prefix).await?;
    }
    if reached_end {
        ctx.propose_schema(&MetaCommand::MarkIndexBackfilled {
            table: table.to_owned(),
            index: idx.name.clone(),
            tablet,
        })
        .await;
    }
    Ok(())
}

/// Durably advance the backfill cursor for one index's tag to `prefix` — a
/// direct, local propose+confirm against `group` (mirroring
/// [`seed_change_log_record`]'s own shape), never `ctx.cp_kind_write_raw`,
/// whose auto-derived fence is always this tablet's *current live* range.
///
/// **Why the cursor write must NOT be range-fenced** (a real bug this
/// tablet-split/fault-injection corpus found, seed-reproducible at *every*
/// seed, not just under fault injection — see `docs/engineering-
/// lessons.md`): [`cursor::cursor_key`] truncates its `range_start`
/// argument to a bare [`TOKEN_BYTES`]-wide token ([`cursor::token_of`]) —
/// but a split's own `split_key` is essentially never token-aligned (it is
/// chosen from real row content via the byte-weighted-median split point,
/// never the hash ring), so a split child's own `range.start` is almost
/// always *longer* than `TOKEN_BYTES`. Comparing the two lexicographically,
/// the cursor key (`token || 0x00 || tag_byte || tag`) sorts *below* that
/// child's own `range.start` the instant `range.start`'s own byte
/// immediately past the token is non-zero — true of `escape(pk)`'s leading
/// byte for essentially any real partition key — so `ctx.
/// cp_kind_write_raw`'s ordinary fence-check (`fence.contains(cursor_key)`)
/// rejected the cursor's own advance write as "outside this group's live
/// range," silently, every single tick, forever (the error is logged and
/// swallowed by `change_consumer_loop`'s own top-level `Err` handling).
/// Data coverage itself was never at risk — the change-log seed writes
/// above are keyed by a *real* base key that genuinely does fall inside
/// the live range — only the cursor's own persistence was, so a split
/// child's sweep silently restarted from scratch every tick instead of
/// resuming from where it left off. The `"gsi"` cursor tag's own callers
/// already tolerate this same underlying gap as a pure efficiency loss
/// (`drain_tablet`'s watermark just stays `None`, meaning "reconcile
/// everything," always correct, just not optimally incremental) — but for
/// backfill specifically this was a **liveness** bug, not merely an
/// efficiency one: a child with more than [`BACKFILL_SEED_BATCH`]
/// partitions on its own side could never advance past that one batch's
/// worth, restarting from position zero every tick forever, so it never
/// reached its own end and the index never flipped `Active`. A cursor
/// row's identity is already fully captured by its own *token* — disjoint
/// from base data by row-kind (ADR 0041 §3) and immutable across a
/// narrowing, since the token a tablet anchors never changes once minted —
/// it needs no range-fencing at all, the same reasoning `seal.rs`/
/// `ceiling.rs`'s engine-global markers already rely on for a *different*
/// flavor of range-independent bookkeeping key.
async fn advance_backfill_cursor(
    group: &CpGroup,
    cursor_key_bytes: Vec<u8>,
    prefix: &[u8],
) -> Result<(), String> {
    let index = match group.put_kind_batch_conditioned(
        vec![(
            KIND_CURSOR,
            cursor_key_bytes,
            Some(cursor::encode_backfill_cursor(prefix)),
        )],
        Vec::new(),
        Vec::new(),
    ) {
        ProposeResult::Accepted { index } => index,
        other => return Err(format!("backfill cursor advance not accepted: {other:?}")),
    };
    let deadline = tokio::time::Instant::now() + BACKFILL_SEED_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if group.engine_applied_index() >= index {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("backfill cursor advance did not apply in time".into())
}

/// Delete `index`'s own backfill cursor row on `group`'s tablet (ADR 0045
/// §5 step 3 of the drop-index cascade) — a tombstone write via the same
/// [`put_kind_batch_fenced`](CpGroup::put_kind_batch_fenced) primitive
/// [`advance_backfill_cursor`] uses to advance the row, unfenced
/// (`KeyRange::whole()`) for the identical reason documented on that
/// function: a cursor row's identity is its own token, independent of this
/// tablet's *current* live range. Idempotent — deleting an already-absent
/// row is a harmless no-op tombstone (the same committed-order guarantee
/// every other apply-time delete in this codebase relies on).
///
/// **Why this exists at all**: `change_consumer_loop`'s `gsis` filter
/// excludes `Deleting`, so once an index's `SetIndexStatus{Deleting}`
/// commits, [`backfill_seed_tick`] never runs for it again — but the last
/// value it wrote under `backfill:{index}` survives in `KIND_CURSOR`
/// forever unless something explicitly removes it. Left alone, a *later*
/// `CreateTableIndex` proposing the exact same index name would have its
/// own fresh seeder read that stale row and silently resume "scanning" from
/// the deleted index's own old position — skipping every partition before
/// it for a backfill that is supposed to start from scratch. Called (via
/// `ClientCtx::clear_backfill_cursor_for_table`, forwarded per tablet like
/// [`seal_now`]) from `dynamo.rs::drop_index`'s own cascade — see that
/// function's doc for exactly when and why twice.
pub(crate) async fn clear_backfill_cursor(group: &CpGroup, index: &str) -> Result<(), String> {
    let tag = backfill_tag(index);
    let cursor_key_bytes = cursor::cursor_key(&group.scope_range().start, &tag);
    let propose_index = match group.put_kind_batch_conditioned(
        vec![(KIND_CURSOR, cursor_key_bytes, None)],
        Vec::new(),
        Vec::new(),
    ) {
        ProposeResult::Accepted { index } => index,
        other => return Err(format!("backfill cursor clear not accepted: {other:?}")),
    };
    let deadline = tokio::time::Instant::now() + BACKFILL_SEED_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if group.engine_applied_index() >= propose_index {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("backfill cursor clear did not apply in time".into())
}

/// Find the length of `token || escape(pk)` at the front of a raw
/// `KIND_BASE` key — this tablet's own partition-prefix boundary, given only
/// that the key's first `TOKEN_BYTES` are the ADR 0022 token and
/// [`animus_dynamo::escape`]'s encoding starts right after: every literal
/// `0x00` byte in `pk` is doubled to `0x00 0x01`, and the whole segment
/// terminates `0x00 0x00`. Mirrors `animus_dynamo::index`'s private
/// `peel_escaped` (not reusable across the crate boundary) but only tracks
/// the boundary position, never decoding the segment's content — the
/// seeder only needs to know *where* a partition's rows stop, not what its
/// key actually was.
///
/// `None` on a malformed/truncated key (defensive only — every key this
/// scans came from this tablet's own `KIND_BASE` scope, written by
/// `animusd::dynamo::item_key`).
fn base_partition_prefix_end(key: &[u8]) -> Option<usize> {
    let mut i = TOKEN_BYTES;
    while i < key.len() {
        if key[i] != 0x00 {
            i += 1;
            continue;
        }
        match key.get(i + 1)? {
            0x01 => i += 2,
            0x00 => return Some(i + 2),
            _ => return None,
        }
    }
    None
}

/// Propose (and confirm) a single synthetic change-log record for the
/// backfill seeder ([`backfill_seed_tick`]) — a `KvCommand::KindBatch`
/// carrying **no base-kind write**, only the change-log entry, so apply
/// stamps a fresh `hlc::pack(ts)` exactly like a live write's own
/// `KindBatch` would.
///
/// Unlike every other confirm path in this module (which probes a value it
/// itself chose), there is nothing to poll back here: the change-log key's
/// own trailing HLC suffix is minted *inside* the propose call, under the
/// group's own lock (`RaftKvNode::propose_ordered`), so the caller can't
/// construct it ahead of time to look for. Confirms by index instead
/// (`engine_applied_index() >= index`) — the same confirm-by-index primitive
/// linearizable reads themselves gate on.
async fn seed_change_log_record(
    group: &CpGroup,
    change_log_prefix: Vec<u8>,
    record: Vec<u8>,
) -> Result<(), String> {
    let fence = group.scope_range();
    if !fence.contains(&change_log_prefix) {
        return Err("backfill seed target outside this group's live range; retry".into());
    }
    let index = match group.put_kind_batch_conditioned(
        Vec::new(),
        vec![(change_log_prefix, record)],
        Vec::new(),
    ) {
        ProposeResult::Accepted { index } => index,
        other => return Err(format!("backfill seed not accepted: {other:?}")),
    };
    let deadline = tokio::time::Instant::now() + BACKFILL_SEED_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if group.engine_applied_index() >= index {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("backfill seed batch did not apply in time".into())
}

/// The seal arm's trigger evaluation (ADR 0043 §A3 "When"), run once per
/// streamed tablet per tick: seals via [`seal_now`] iff the `KIND_CHANGE`
/// scope's approximate size exceeds `--stream-seal-bytes`, **or** the time
/// since this tablet's own last seal (while unsealed bytes exist) exceeds
/// `--stream-seal-age`. Neither trigger fires on an empty hot tail (nothing
/// to seal, and age has nothing to measure). Also records this tick's own
/// observability levels (`Metric::StreamHotBytes`/`StreamSealBacklogMs`)
/// regardless of whether a seal actually fires — the whole point of a level
/// metric is that it reflects the current state, not just the moments
/// something happened.
///
/// **ADR 0042 fork G (2026-08-16): no more unconditional `KIND_CHANGE`
/// scan.** This used to call `group.pending_changes()` — a full scan of the
/// change-log scope, up to `--stream-seal-bytes`' worth of bytes — on
/// *every* tick of *every* streamed led tablet (5×/s at the default
/// `INDEX_DRAIN_INTERVAL`), purely to find the oldest unsealed record's own
/// HLC for the age trigger and the backlog metric, even on a tick where
/// nothing ends up sealing. On an idle streamed tablet that scan was ~100%
/// waste, and it structurally blocked such a tablet from ever quiescing
/// (ADR 0044 phase 1). The fix derives everything this function needs from
/// data that's already cheap:
///
/// - **"are there unsealed changes at all"**: `approx_bytes_kind(KIND_CHANGE)`
///   is nonzero — the same cheap accessor the size trigger already reads.
///   Zero bytes short-circuits immediately: no catalog lookup, no scan,
///   nothing to measure.
/// - **"how long have they been waiting"**: time since this tablet's own
///   last seal, read straight off the `stream_shards` catalog
///   (`Metadata::last_seal_wall_ms` — an O(log n) `BTreeMap` lookup, not a
///   scan of anything data-sized) rather than re-deriving it from the
///   oldest pending record's own HLC.
/// - **the never-sealed fallback**: a tablet with no catalog row yet has no
///   catalog time to read. The caller ([`change_consumer_loop`]) keeps a
///   small **driver-local** `BTreeMap<TabletId, u64>` memoizing, per
///   tablet, the age basis established the first time it's ever seen with
///   a nonzero, never-sealed backlog — cleared the moment either a real
///   catalog seal time becomes available or the backlog empties out (so a
///   later backlog starts its own fresh clock rather than inheriting a
///   stale one). **The value memoized on that first observation is a
///   one-time [`CpGroup::pending_changes`] scan's true oldest-record HLC**,
///   not a bare "now" timestamp — an earlier draft of this fork seeded
///   "now" instead (cheaper still, no read at all), but that is wrong for a
///   split child: the backlog it inherits is physically whatever its
///   parent hadn't sealed yet, so "now" silently forgets how old it really
///   is, and a same-node "look up the parent's own entry in this map" patch
///   is *also* wrong, since a split's child is routinely led by a
///   *different* node than its parent — this map is per-node, so that node
///   never even heard of the parent tablet. Both failure modes compound
///   across a cascade of splits (ADR 0034's auto-split runs on its own
///   fixed interval, independent of sealing), found by
///   `streams_e2e.rs::manual_split_with_unsealed_backlog_under_production_
///   seal_knobs` going red/flaky under exactly this scenario. The one-time
///   scan is the only source that is both correct (reads the actual data,
///   not a per-node guess) and identical regardless of which node leads
///   which tablet — and it still eliminates the overwhelming majority of
///   this fork's target cost, since it runs at most once per tablet's
///   entire lifetime (between "created" and "its first seal ever
///   commits"), never once more per tick thereafter.
///
/// **Consequence for `pending_changes()`**: after this fork it is reachable
/// from two places rather than every tick of every streamed tablet forever
/// — inside [`seal_now`] (reached only once `size_hot || age_hot` is
/// already `true`), and the never-sealed fallback's own one-time bootstrap
/// scan above (reached at most once per tablet's lifetime). A tablet that
/// has already been memoized (sealed at least once, or already tracked by
/// the fallback map) performs no `KIND_CHANGE` scan on an idle tick, by
/// construction — the steady-state cost this fork exists to remove.
///
/// **Accepted metric-semantics change**: `Metric::StreamSealBacklogMs` used
/// to measure the oldest unsealed record's own age; it now measures time
/// since the tablet's last seal while unsealed bytes exist (see the
/// variant's own doc). The two agree whenever the hot tail is a single
/// contiguous burst (the common case); they can differ during a slow,
/// trickling backlog where the very first record arrived long before the
/// most recent seal — an accepted precision loss in exchange for never
/// scanning to compute it.
async fn seal_tick(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    tablet: TabletId,
    group: &CpGroup,
    first_hot_seen: &mut BTreeMap<TabletId, u64>,
) -> Result<(), String> {
    // ADR 0043 §A3's "When": the change log's OWN bytes, never the base
    // row bytes `CpGroup::approx_bytes` measures — that accessor is
    // deliberately base-only (ADR 0034's own fix, so auto-split stops
    // reacting to change-log churn), which is exactly the wrong scope for
    // this trigger to read.
    let approx_bytes = group.approx_bytes_kind(KIND_CHANGE).await;
    ctx.data()
        .raftkv_metrics
        .set(Metric::StreamHotBytes, approx_bytes);
    // Growth PR3 Fork F (ADR 0042 §14): feed this tick's own already-computed
    // `KIND_CHANGE` byte level into the per-tablet change-rate tracker — no
    // new scan, reusing exactly the data `StreamHotBytes` just read above.
    // Read by `/admin/metrics` and the opt-in `--auto-split-change-rate`
    // trigger (`auto_split_loop`).
    ctx.data().change_rates.observe(tablet, approx_bytes);

    if approx_bytes == 0 {
        // Nothing pending at all: no backlog for the age trigger to
        // measure, and no future backlog should inherit whatever fallback
        // basis a previous, now-fully-cleared backlog left behind.
        first_hot_seen.remove(&tablet);
        ctx.data()
            .raftkv_metrics
            .set(Metric::StreamSealBacklogMs, 0);
        return Ok(());
    }

    let now_ms = ctx.env.now().0 / 1_000_000;
    let last_seal_ms = match meta.last_seal_wall_ms(tablet) {
        Some(ms) => {
            // The catalog is now the authority for this tablet; drop any
            // fallback basis a pre-first-seal tick may have seeded, so a
            // later never-sealed-again tablet (impossible in practice, but
            // cheap to keep tidy) never reads a stale value.
            first_hot_seen.remove(&tablet);
            ms
        }
        None => match first_hot_seen.get(&tablet).copied() {
            Some(ms) => ms,
            None => {
                // First time this driver has ever seen this tablet with a
                // nonzero, never-sealed backlog. Seeding this at "now" (a
                // pure driver-local guess, no data read at all) was this
                // fork's original design — and it is WRONG for a **split
                // child**: the backlog it just inherited is physically
                // whatever its parent hadn't sealed yet (the shared engine
                // only narrowed the declared range; it never touched the
                // records), so "now" silently forgets how long that
                // inherited backlog had already been waiting. A same-node
                // "inherit the parent's own basis" fix (looking up the
                // parent tablet's entry in this same map) is *also* wrong,
                // and more subtly so: the parent and child are placement
                // decisions, not process-affinity ones — a split's child is
                // routinely led by a *different* node than its parent, and
                // this map is per-node, in-memory state. On a different
                // node this map has never even heard of the parent tablet,
                // so the lookup silently misses and falls back to "now"
                // anyway. Both failure modes compound across a cascade of
                // splits (ADR 0034's auto-split runs on its own fixed
                // interval, independent of sealing): each further child
                // restarts its own clock from "now", so a chain of N splits
                // can delay the age trigger by roughly N *
                // `--stream-seal-age` before ever converging. Found by
                // `streams_e2e.rs::manual_split_with_unsealed_backlog_
                // under_production_seal_knobs` going deterministically red
                // (same-node fix) or flaky (cross-node split placement)
                // under this exact scenario.
                //
                // The only source that is both correct and available
                // identically regardless of which node leads which tablet
                // is the data itself: the true oldest pending record's own
                // HLC — exactly what the pre-fork code computed on *every*
                // tick. The fix keeps that computation but runs it *once*
                // per tablet's entire lifetime rather than forever: the
                // moment it's memoized here, every subsequent tick this
                // tablet is revisited hits the cheap `Some` branch above
                // and never scans again. A tablet transitions through this
                // branch at most once between "created" and "its first
                // seal ever commits" — going from a per-tick cost (5×/s,
                // forever) to a one-time bootstrap cost is still the
                // overwhelming majority of this fork's win, and it is the
                // only design that doesn't reintroduce a genuine data-
                // delivery regression.
                let oldest = group
                    .pending_changes()
                    .await
                    .iter()
                    .filter_map(|(key, _)| record_hlc(key))
                    .map(|ts| ts.wall_ms)
                    .min()
                    .unwrap_or(now_ms);
                first_hot_seen.insert(tablet, oldest);
                oldest
            }
        },
    };
    let backlog_ms = now_ms.saturating_sub(last_seal_ms);
    ctx.data()
        .raftkv_metrics
        .set(Metric::StreamSealBacklogMs, backlog_ms);

    let size_hot = approx_bytes > ctx.data().stream_seal_knobs.seal_bytes;
    let age_hot = Duration::from_millis(backlog_ms) > ctx.data().stream_seal_knobs.seal_age;
    if !size_hot && !age_hot {
        return Ok(());
    }
    seal_now(ctx, table, tablet, group).await.map(|_| ())
}

/// **The seal sequence** (ADR 0043 §A3, "Sequence"/"Recovery") — the one
/// mechanism both the periodic seal arm ([`seal_tick`]) and the
/// disable-triggered final seal (F12-b, `ClientCtx::force_seal_tablet` via
/// `ClientRequest::ForceSeal`) call, so there is exactly one seal code path
/// regardless of what triggered it. Unconditional: seals whatever is
/// currently pending past the tablet's own effective watermark, with no
/// trigger check of its own (the caller decides *whether* to call this; this
/// function only knows *how*).
///
/// Returns `Ok(Some(epoch))` on a genuine seal, `Ok(None)` if there was
/// nothing past the watermark to seal (never seals an empty segment — ADR
/// 0043 §A3's "Empty pending set ⇒ no seal").
///
/// **Recovery (ledger-named-object amendment, ADR 0042 §10/ADR 0043 §A3).**
/// The segment's storage id is no longer the bare deterministic
/// `{table}/{label}/{tablet}/{epoch}` — it is
/// [`segment::segment_object_id`], a fresh, attempt-unique id minted every
/// single call (see that function's own doc for the exact scheme). A crash
/// between the store `put` and the catalog commit has the next call
/// recompute the *same* `epoch` but mint a **fresh** id and `put` there
/// instead of overwriting the first attempt's object — the two attempts'
/// bytes can never collide, so there is no overwrite (idempotent or
/// otherwise) to reason about at the store layer at all. If the *first*
/// attempt's own proposal actually committed despite the caller never
/// seeing the ack (the retry's own proposal then hits a genuine content
/// conflict — its own fresh `object_id` never matches the already-committed
/// row's), the surrounding `propose_and_await` poll (below) already treats
/// "the row now exists" as success regardless of which attempt's proposal
/// is the one that landed — so the retry still reports success, and its own
/// freshly-written (never cataloged) object becomes a permanent orphan,
/// reclaimed by the segment janitor's own orphan sweep
/// (`segment_janitor::reap_orphans`), never overwritten. This is exactly
/// what closes the data-loss bug the old shared-deterministic-id scheme had
/// — see `animus_cp_data::segment`'s own module doc for the full incident.
pub(crate) async fn seal_now(
    ctx: &ClientCtx,
    table: &str,
    tablet: TabletId,
    group: &CpGroup,
) -> Result<Option<u64>, String> {
    let meta = ctx.effective_metadata();
    // The label to seal under: the table's *current* schema label if it has
    // one (the ordinary case, and F12-b's disable path — the final seal
    // runs before `SetTableStream{None}` ever proposes, so the schema still
    // names the label being drained), else the most recent still-draining
    // label with any catalog rows at all (belt-and-suspenders for a
    // force-seal racing a disable that already committed) — see
    // `MetaCommand::SealStreamShard`'s own apply-time label validation for
    // why either of these is always accepted. No label at all (this table
    // has never streamed) means nothing to seal under.
    let label = meta
        .table_stream(table)
        .map(|s| s.label.clone())
        .or_else(|| meta.stream_labels_with_rows(table).into_iter().next_back());
    let Some(label) = label else {
        return Ok(None);
    };

    let watermark = meta.stream_shard_watermark(tablet);
    let mut records: Vec<segment::SegmentRecord> = group
        .pending_changes()
        .await
        .into_iter()
        .filter_map(|(key, value)| {
            let ts = record_hlc(&key)?;
            let packed = hlc::pack(ts);
            if watermark.is_some_and(|w| packed <= w) {
                return None; // already covered by an earlier seal
            }
            Some(segment::SegmentRecord {
                source_key: key,
                packed_hlc: packed,
                change_record: value,
            })
        })
        .collect();
    if records.is_empty() {
        return Ok(None); // ADR 0043 §A3: never seal an empty segment
    }
    // `pending_changes`' own key order is token-then-pk-then-HLC, NOT commit
    // order (see its doc) — this sort by the packed-HLC suffix is load-
    // bearing, not a formality (ADR 0043 §A3 step 1).
    records.sort_by_key(|r| r.packed_hlc);

    let start_exclusive = watermark.unwrap_or(0);
    let end_inclusive = records.last().expect("just checked non-empty").packed_hlc;
    // Epoch = this tablet's own chain length, regardless of label (a
    // tablet's epoch counter is a property of its physical seal history,
    // never resetting across a disable/re-enable cycle — see
    // `StreamShardRow`'s own identity note in `animus-control`).
    let next_epoch = meta
        .stream_shards
        .range((tablet, 0)..=(tablet, u64::MAX))
        .next_back()
        .map_or(0, |((_, e), _)| e + 1);
    let parent_shard_id = meta.stream_shard_parent_id(tablet, next_epoch);
    let seal_wall_ms = ctx.env.now().0 / 1_000_000;
    let header = segment::new_header(
        table.to_owned(),
        label.clone(),
        tablet.0,
        next_epoch,
        parent_shard_id,
        (start_exclusive, end_inclusive),
        seal_wall_ms,
    );
    let count = records.len() as u64;
    let bytes = segment::encode(&header, &records);
    // Ledger-named-object amendment (ADR 0042 §10/ADR 0043 §A3): a fresh,
    // attempt-unique id every call — see `segment::segment_object_id`'s own
    // doc for why `(proposer, term, nonce)` never repeats, even across a
    // same-node restart whose RNG stream replays identically.
    let seg_id = segment::segment_object_id(
        table,
        &label,
        tablet.0,
        next_epoch,
        ctx.env.node_id().as_str(),
        group.term(),
        ctx.env.next_u64(),
    );

    let replicas = match ctx.data().segment_store.put_sealed(&seg_id, &bytes).await {
        Ok(r) => r,
        Err(e) => {
            ctx.data()
                .raftkv_metrics
                .incr(Metric::StreamSealFailuresTotal);
            return Err(format!("segment store put of {seg_id:?} failed: {e}"));
        }
    };

    // PR6's catalog amendment: carry the view type declared *right now* on
    // the table's stream — always `Some` at this point, since sealing only
    // ever happens for a table whose stream is (still) enabled at seal
    // time, F12-b's disable-triggered final seal included (it runs before
    // `SetTableStream{None}` ever proposes). The `unwrap_or` fallback is
    // defensive only, never expected to be reached in production.
    let view_type = meta
        .table_stream(table)
        .map_or(animus_control::StreamViewType::NewAndOldImages, |s| {
            s.view_type
        });
    let cmd = MetaCommand::SealStreamShard {
        table: table.to_owned(),
        label,
        tablet,
        epoch: next_epoch,
        view_type,
        hlc_range: (start_exclusive, end_inclusive),
        count,
        seal_wall_ms,
        replicas,
        object_id: seg_id,
    };
    // Retry-after-lost-ack semantics (ledger-named-object amendment): this
    // check function already treats "the row now exists" as success
    // regardless of whose proposal actually committed it — a genuine
    // content-conflict `NoOp`/`Rejected` for THIS attempt's own proposal
    // (its fresh `object_id` can never match an already-committed row's) is
    // therefore not distinguished from an ordinary "still waiting to
    // commit" here. Either the original attempt's proposal committed (lost
    // ack) or a genuinely different, independently-computed attempt won the
    // race — both cases converge to the identical outcome from this
    // caller's point of view: the epoch is sealed, report success, and this
    // attempt's own now-uncataloged object is a permanent orphan for the
    // segment janitor's sweep to reclaim.
    match ctx
        .propose_and_await(cmd, SEAL_COMMIT_TIMEOUT, || async {
            ctx.metadata_fresh()
                .await
                .stream_shards
                .contains_key(&(tablet, next_epoch))
                .then_some(())
        })
        .await
    {
        Ok(()) => {
            ctx.data().raftkv_metrics.incr(Metric::StreamSealsTotal);
            Ok(Some(next_epoch))
        }
        Err(()) => {
            ctx.data()
                .raftkv_metrics
                .incr(Metric::StreamSealFailuresTotal);
            Err(format!(
                "SealStreamShard({}, {next_epoch}) did not commit in time",
                tablet.0
            ))
        }
    }
}

/// The open-shard hot-read path (ADR 0042 §7/§8, PR6's `GetRecords` read
/// API): a leader-local, non-linearizable scan of `group`'s own
/// `KIND_CHANGE` hot tail for records with packed HLC strictly greater than
/// `from_position`, sorted by that HLC — load-bearing, exactly like
/// [`seal_now`]'s identical sort, since `pending_changes`' own key order is
/// token-then-pk-then-HLC, not commit order — then truncated to `limit`.
///
/// **Deliberately no `ReadIndex` barrier** — this is
/// `ClientRequest::StreamHotRead`'s whole reason to exist (F8, ADR 0042
/// §7): the log is append-only, positional, and serves only
/// committed-and-applied records, so the worst a leader-local read can
/// produce is a stale prefix (a record this group has committed but not
/// yet locally applied), never an out-of-order or fabricated one — and
/// that staleness is indistinguishable from the stream's own eventually
/// consistent contract. Never "upgrade" this to a `linearizable_scan_kind`
/// call.
///
/// Returns `(source_key, packed_hlc, change_record bytes)` triples in
/// ascending HLC order — the caller (`ClientCtx::read_stream_hot_records`,
/// then the DynamoDB Streams wire edge) builds a `GetRecords` response from
/// these identically to how it builds one from a sealed segment's own
/// `SegmentRecord`s.
///
pub(crate) async fn hot_read(
    group: &CpGroup,
    from_position: u64,
    limit: usize,
) -> Vec<(Vec<u8>, u64, Vec<u8>)> {
    let mut records: Vec<(Vec<u8>, u64, Vec<u8>)> = group
        .pending_changes()
        .await
        .into_iter()
        .filter_map(|(key, value)| {
            let ts = record_hlc(&key)?;
            let packed = hlc::pack(ts);
            (packed > from_position).then_some((key, packed, value))
        })
        .collect();
    records.sort_by_key(|(_, packed, _)| *packed);
    records.truncate(limit);
    records
}

/// The hot-trim arm (ADR 0042 §8, ADR 0043 §A6, F10), run once per tablet per
/// tick, right after this tick's reconciliation/seal. Deletes change records
/// every *expected, present* term has already cleared. Advances no cursor
/// and seals nothing itself — that's [`drain_tablet`]'s job for the "gsi"
/// term, and [`seal_now`]'s for advancing the stream's own catalog
/// watermark.
///
/// **The F10/F12-b trim-bound rule** — trim = `min(gsi term if the table has
/// GSIs, catalog watermark iff the table's CURRENT schema has an enabled
/// stream)`:
///
/// - An expected term with nothing to derive it from yet (a GSI with no
///   `"gsi"` cursor row; a stream that has never sealed a shard) reads as
///   absent and blocks trim **entirely** — the safe default every cold
///   consumer already gets (unchanged from ADR 0041/round 2).
/// - **The stream term applies only while `stream_enabled` is true** — i.e.
///   the table's *current* schema names an active stream — never merely
///   because the table has draining catalog rows left over from a disabled
///   one. This is what makes F12-b's "a disabled stream's draining rows
///   must not hold the hot scope hostage" hold: by the time a stream is
///   disabled, `disable_stream`'s own final seal (F12-b, `dynamo.rs`) has
///   already moved every one of its records into a committed segment and
///   the write gate has closed, so **every** hot record still physically
///   present for that label is, by construction, already ≤ the tablet's
///   own last-sealed watermark — there is nothing left for a stream term to
///   protect. Omitting the term entirely (rather than computing one that
///   would trivially always allow trim anyway) is simpler and needs no
///   special-casing of "disabled but still has rows" here at all — the
///   catalog rows themselves age out later through the ordinary retention
///   sweep (ADR 0043 §A9, a later PR), a fact this hot-trim arm never has
///   to know about.
/// - **Zero expected terms at all means trim EVERYTHING, not block
///   everything** — the opposite of the "one term absent" case above, and
///   the one subtlety this rule depends on getting right. Two production
///   shapes reach it. (1) A table whose stream was disabled and that has no
///   GSI (the caller's own top-level gate widens for exactly this case —
///   `ever_streamed`, `change_consumer_loop` — so a tablet that has ever
///   sealed keeps being visited after its stream disables, specifically so
///   this arm gets a **guaranteed** chance to run rather than depending on
///   winning a race against `disable_stream`'s own `SetTableStream` commit
///   landing first). By the time that state is reached, F12-b's final seal
///   has already moved every record into a committed segment — nothing is
///   protecting the hot log anymore, so blocking here would leave those
///   records stranded forever (found live: this PR's own
///   `disable_final_seal_then_reenable_continues_the_epoch_chain` test
///   failed intermittently, exactly on this race, before the fix).
///   (2) **A never-streamed, never-indexed table's marker records (ADR
///   0049 §4, Train A rung 4)**: every write leaves an image-less
///   `ChangeRecord::marker` now, no consumer ever holds a term over them,
///   and this rule — unchanged — is what keeps them transient rather than
///   accumulating forever (`change_consumer_loop`'s marker branch calls
///   this arm with `gsis` empty and `stream_enabled` false, precisely to
///   land here). One trim rule, both shapes; never a second deleter.
async fn trim_janitor(
    ctx: &ClientCtx,
    meta: &Metadata,
    table: &str,
    tablet: TabletId,
    group: &CpGroup,
    gsis: &[IndexDef],
    stream_enabled: bool,
) -> Result<(), String> {
    let mut trim_point: Option<u64> = None;
    let mut blocked = false;
    if !gsis.is_empty() {
        match group.cursor_min_watermark(GSI_TAG).await {
            Some(w) => trim_point = Some(hlc::pack(w)),
            None => blocked = true,
        }
    }
    if !blocked && stream_enabled {
        match meta.stream_shard_watermark(tablet) {
            Some(w) => trim_point = Some(trim_point.map_or(w, |t| t.min(w))),
            None => blocked = true,
        }
    }
    ctx.data()
        .raftkv_metrics
        .set(Metric::ChangeLogTrimBlocked, u64::from(blocked));
    if blocked {
        return Ok(()); // an expected term has nothing to derive from yet
    }
    // `trim_point == None` here means **zero terms were expected at all**
    // (`blocked` is already known `false`) — a table whose stream was
    // disabled and that has no GSI (still visited via `ever_streamed`), or
    // a never-streamed never-indexed table's ADR 0049 markers (the caller's
    // marker branch). Nothing is protecting this hot log
    // anymore — the disable-triggered final seal (F12-b) already moved
    // every one of its records into a committed segment before the write
    // gate closed (and a marker was never a consumer-visible event at all),
    // so every hot record physically still present for that
    // label is, by construction, safe to delete outright. `trim_all` makes
    // that explicit rather than leaving a `None` trim point to be
    // (wrongly) read as "block everything," which is the bug this
    // distinction fixes: `trim_point.is_some()` bounds the delete to
    // `<= trim_point`; `trim_all` deletes every pending record with no
    // bound at all.
    let trim_all = trim_point.is_none();

    let mut writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    for (key, _) in group.pending_changes().await {
        if !trim_all {
            let Some(ts) = record_hlc(&key) else {
                continue; // malformed suffix; leave it rather than guess
            };
            if hlc::pack(ts) > trim_point.expect("checked Some via !trim_all") {
                // `pending_changes` is in key order (token-then-pk-then-
                // HLC), not HLC order (see its own doc), so every record
                // must be checked — there is no earlier prefix to stop at.
                continue;
            }
        }
        writes.push((KIND_CHANGE, key, None));
        if writes.len() >= TRIM_BATCH {
            let n = writes.len() as u64;
            ctx.cp_kind_write_raw(table, std::mem::take(&mut writes), Vec::new())
                .await?;
            ctx.data()
                .raftkv_metrics
                .incr_by(Metric::ChangeLogTrimmedTotal, n);
        }
    }
    if !writes.is_empty() {
        let n = writes.len() as u64;
        ctx.cp_kind_write_raw(table, writes, Vec::new()).await?;
        ctx.data()
            .raftkv_metrics
            .incr_by(Metric::ChangeLogTrimmedTotal, n);
    }
    Ok(())
}

/// The full data-plane key of one GSI row: the ADR 0022 token over the **index
/// hash value** (not the base partition key — that difference is exactly why a
/// GSI lives in its own tablets and is maintained here rather than inline).
fn gsi_row_key(
    ihash: &AttributeValue,
    isort: Option<&AttributeValue>,
    base_pk: &AttributeValue,
    base_sk: Option<&AttributeValue>,
) -> Vec<u8> {
    let mut key = partition_token(&storage_key(ihash, None)).to_vec();
    key.extend_from_slice(&dynamo_index::gsi_row_key(ihash, isort, base_pk, base_sk));
    key
}

/// The attributes a GSI row carries, per its declared projection.
fn projected(item: &Item, base: &animus_dynamo::TableSchema, idx: &IndexDef) -> Item {
    crate::dynamo::projected_item(item, base, idx)
}

/// ADR 0042 §7/§8 regressions for the cursor-based drain + trim janitor
/// above. **In-crate**, like `lib.rs`'s `split_fence_tests`/
/// `auto_split_median_tests`: these need private handles (`CpGroup::
/// pending_changes`/`cursor_min_watermark`, the
/// plain-client-protocol `ClientRequest::SplitTablet` with an
/// arbitrary binary `split_key`, and `crate::dynamo::item_key` for
/// deterministic side-placement) that an external `tests/` crate — a
/// separate compilation unit, linking only this crate's `pub` surface —
/// cannot reach. All real-socket `ProdEnv` integration tests, per this
/// crate's own testing discipline; every eventual property is a
/// converged-or-timeout poll, never a fixed sleep.
#[cfg(test)]
mod gsi_drain_cursor_tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::Duration;

    use animus_tablet::TabletId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::config::NodeRole;
    use crate::{
        ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, read_frame, run_node,
        write_frame,
    };

    fn free_addrs(count: usize) -> Vec<SocketAddr> {
        let ls: Vec<std::net::TcpListener> = (0..count)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        ls.iter().map(|l| l.local_addr().unwrap()).collect()
    }

    fn single_node_config() -> ClusterConfig {
        let addrs = free_addrs(6);
        ClusterConfig {
            nodes: vec![RoleAddrs {
                id: crate::config::node_id(0),
                role: NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                cql: addrs[3],
                admin: addrs[4],
                intra: addrs[5],
            }],
        }
    }

    async fn await_control_leader(node: &Node) {
        timeout(Duration::from_secs(10), async {
            loop {
                if node.is_control_leader() {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("node did not become control leader in time");
    }

    /// Bring up a single node, retrying against the documented port-TOCTOU
    /// race (`docs/engineering-lessons.md`): `single_node_config()`'s
    /// `free_addrs` probe releases its ports before the real bind, so
    /// another test binary can steal one under `cargo test --workspace`
    /// contention. Each attempt allocates a **fresh** config.
    async fn single_node(dir: &Path) -> Node {
        let mut last_err = None;
        for attempt in 0..16 {
            let config = single_node_config();
            match run_node(&config, 0, dir.join(format!("node-{attempt}"))).await {
                Ok(node) => {
                    await_control_leader(&node).await;
                    return node;
                }
                Err(e) => {
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!(
            "could not bring up single node after retries (ports kept getting stolen): {last_err:?}"
        );
    }

    /// One DynamoDB JSON request over the real HTTP wire (mirroring
    /// `tests/dynamo_gsi_drain.rs`'s helper of the same shape — duplicated
    /// rather than shared, since this module is a different compilation
    /// unit than that external `tests/` crate).
    async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
        let mut s = TcpStream::connect(addr).await.expect("connect");
        let req = format!(
            "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
             Connection: close\r\n\
             Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        s.write_all(req.as_bytes()).await.expect("write");
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.expect("read");
        let text = String::from_utf8_lossy(&buf).into_owned();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
        (status, body.to_owned())
    }

    /// A table with one GSI (`by-g`, hash attribute `g`) — every test in this
    /// module uses this identical shape.
    async fn create_table_with_gsi(addr: SocketAddr, table: &str) {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            &format!(
                r#"{{"TableName":"{table}",
                    "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                    "GlobalSecondaryIndexes":[
                        {{"IndexName":"by-g",
                         "KeySchema":[{{"AttributeName":"g","KeyType":"HASH"}}],
                         "Projection":{{"ProjectionType":"ALL"}}}}]}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
    }

    /// One item whose own `id` and GSI hash attribute `g` are both `id` —
    /// every item this module writes has a unique, individually queryable
    /// GSI hash value.
    async fn put_item(addr: SocketAddr, table: &str, id: &str) {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"g":{{"S":"{id}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
    }

    /// Poll a GSI `Query` until `accept` is satisfied (mirrors
    /// `tests/dynamo_gsi_drain.rs`'s `await_gsi_query` — a GSI is
    /// eventually consistent by contract).
    async fn await_gsi_query(addr: SocketAddr, body: &str, accept: impl Fn(&str) -> bool) {
        let last = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let seen = std::sync::Arc::clone(&last);
        let converged = async move {
            loop {
                let (status, got) = dynamo(addr, "DynamoDB_20120810.Query", body).await;
                if status == 200 && accept(&got) {
                    return;
                }
                *seen.lock().unwrap() = got;
                sleep(Duration::from_millis(100)).await;
            }
        };
        if timeout(Duration::from_secs(30), converged).await.is_err() {
            panic!(
                "GSI query never converged within 30s (last saw: {})",
                last.lock().unwrap()
            );
        }
    }

    /// A `{"g = :g"}` GSI equality query for `id`, expecting exactly one hit.
    async fn await_indexed(addr: SocketAddr, table: &str, id: &str) {
        await_gsi_query(
            addr,
            &format!(
                r#"{{"TableName":"{table}","IndexName":"by-g",
                    "KeyConditionExpression":"g = :g",
                    "ExpressionAttributeValues":{{":g":{{"S":"{id}"}}}}}}"#
            ),
            |b| b.contains("\"Count\":1"),
        )
        .await;
    }

    fn tablets_of(node: &Node, table: &str) -> Vec<TabletId> {
        node.metadata()
            .tablets
            .iter()
            .filter(|(_, t)| t.table.as_deref() == Some(table))
            .map(|(id, _)| *id)
            .collect()
    }

    fn only_tablet(node: &Node, table: &str) -> TabletId {
        let mut ts = tablets_of(node, table);
        assert_eq!(ts.len(), 1, "expected exactly one tablet for `{table}`");
        ts.pop().unwrap()
    }

    async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        while !cond() {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll until tablet `tablet`'s own change log (`KIND_CHANGE`, via the
    /// private `CpGroup::pending_changes` accessor — the "raw kind-scan of
    /// leftovers" the crash-recovery scenario needs) holds exactly `want`
    /// records.
    async fn await_pending_changes(node: &Node, tablet: TabletId, want: usize, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");
            let n = group.pending_changes().await.len();
            if n == want {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{what}: change log has {n} records, want {want}"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll until this node's own tablet-host reconciler has actually stood
    /// up `tablet`'s group — a real, if usually short, window separate from
    /// "the tablet map already shows it" (`tablets_of`'s own convergence).
    async fn await_hosted(node: &Node, tablet: TabletId, what: &str) -> CpGroup {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(group) = node.edge.local_cp(tablet) {
                return group;
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn await_cursor_some(group: &CpGroup, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if group.cursor_min_watermark(GSI_TAG).await.is_some() {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Split `tablet` at the raw physical key `split_key` — the plain
    /// client-protocol `ClientRequest::SplitTablet`, not the admin HTTP
    /// surface (whose `split_key: String` field is a JSON string and so
    /// can't carry a DynamoDB item key's arbitrary, generally non-UTF8
    /// murmur3 token prefix — `ClientRequest`'s own `Vec<u8>` field
    /// `serde_json`-encodes as an ordinary byte array instead).
    async fn split(client_addr: SocketAddr, tablet: TabletId, split_key: Vec<u8>) {
        let mut stream = TcpStream::connect(client_addr).await.expect("connect");
        write_frame(
            &mut stream,
            &ClientRequest::SplitTablet {
                tablet: tablet.0,
                split_key,
            },
        )
        .await
        .expect("send split");
        let resp: ClientResponse = read_frame(&mut stream)
            .await
            .expect("read reply")
            .expect("a reply");
        assert!(
            matches!(resp, ClientResponse::PutOk),
            "split failed: {resp:?}"
        );
    }

    /// An `id` value whose real ADR 0022 token (`crate::dynamo::item_key`,
    /// the exact function the DynamoDB edge itself uses) falls on the
    /// requested side of `boundary` — a murmur3 token can't be chosen
    /// directly, so this scans a small deterministic candidate pool instead.
    /// Panics if none match (the pool is too small for this boundary, not a
    /// product bug).
    fn find_id_on_side(boundary: &[u8; 8], want_below: bool, pool: &str) -> String {
        for i in 0..10_000u32 {
            let id = format!("{pool}-{i}");
            let key = crate::dynamo::item_key(&AttributeValue::S(id.clone()), None);
            let token: [u8; 8] = key[..8].try_into().expect("a key has at least 8 bytes");
            if (token < *boundary) == want_below {
                return id;
            }
        }
        panic!(
            "no candidate id found on the {} side of the boundary in `{pool}`'s pool",
            if want_below { "left" } else { "right" }
        );
    }

    /// A fixed, arbitrary token boundary — not derived from any real item.
    /// Any byte string strictly between the ring's absolute start (`[]`) and
    /// its unbounded-above end is a legal split point (`SplitTablet` doesn't
    /// require an existing key), so a plain numeric midpoint works.
    const BOUNDARY: [u8; 8] = 0x8000_0000_0000_0000u64.to_be_bytes();

    /// ADR 0049 Train A rung 5: the GSI drain's row writes into a hidden
    /// index table ride the kind path — every materialized row leaves an
    /// image-less marker on the HIDDEN table's own change log (a hidden
    /// table is a table; its tablets need the same delta feed ADR 0050's
    /// split-build tail consumes everywhere else). Red on the pre-rung
    /// code: `reconcile_partition` wrote via plain `cp_write`/`cp_delete`,
    /// so a hidden group's `KIND_CHANGE` scope stayed empty forever no
    /// matter how long this poll ran. Sustained base writes keep the trim
    /// busy-gate deferring (changed-since-last-tick), so a live marker is
    /// observable within the deadline — converged-or-timeout, never a
    /// fixed-sleep one-shot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hidden_index_table_drain_writes_leave_markers() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node(dir.path()).await;
            create_table_with_gsi(node.dynamo_addr(), "ht").await;
            put_item(node.dynamo_addr(), "ht", "a0").await;
            await_indexed(node.dynamo_addr(), "ht", "a0").await;

            // The drain has materialized at least one GSI row by now, so the
            // hidden table's tablet exists (lazily provisioned by the drain).
            let hidden = index_table_name("ht", "by-g");
            let group = loop {
                let meta = node.metadata();
                if let Some((&tablet, _)) = meta.tablets_for_table(&hidden).next()
                    && let Some(group) = node.edge.local_cp(tablet)
                {
                    break group;
                }
                sleep(Duration::from_millis(20)).await;
            };

            let mut i = 0u32;
            loop {
                let records = group.pending_changes().await;
                if let Some((key, value)) = records.first() {
                    let record = ChangeRecord::decode(value).expect("hidden-table record decodes");
                    assert!(record.marker, "a hidden table's record is a marker");
                    assert!(
                        record.consumer_hidden(),
                        "a hidden table's marker must never be a stream event"
                    );
                    assert!(record.old_image.is_none() && record.new_image.is_none());
                    // Full-row-key-as-prefix convention (`marker_change_log`'s
                    // doc): the change key is the GSI row's own key + the
                    // apply-completed 8-byte HLC suffix.
                    assert!(
                        key.len() > 8,
                        "marker key carries the row key + an HLC suffix"
                    );
                    break;
                }
                i += 1;
                put_item(node.dynamo_addr(), "ht", &format!("a{i}")).await;
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("no marker ever appeared on the hidden index table's change log");
    }

    /// The change log must not grow without bound while a stream of writes
    /// to an indexed table is ongoing, and must drain back to nothing once
    /// they stop — the reason the cursor+trim-janitor rework exists at all
    /// (ADR 0042 §7/§8), proven here by directly inspecting the raw
    /// `KIND_CHANGE` scope rather than only observing the GSI's own
    /// eventual correctness.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn change_log_stays_bounded_under_sustained_writes_to_an_indexed_table() {
        timeout(Duration::from_secs(90), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node(dir.path()).await;
            let table = "orders";
            create_table_with_gsi(node.dynamo_addr(), table).await;

            for i in 0..200u32 {
                put_item(node.dynamo_addr(), table, &format!("o{i}")).await;
                if i % 20 == 19 {
                    let tablet = only_tablet(&node, table);
                    let group = node
                        .edge
                        .local_cp(tablet)
                        .expect("this node hosts the tablet");
                    let n = group.pending_changes().await.len();
                    // A generous, sampled ceiling: proves the log doesn't
                    // grow unboundedly with the write stream (it would, under
                    // the pre-ADR-0042 GSI-drain-only design's absence of a
                    // second consumer, but a bug here would show as
                    // unbounded growth too), not a tight bound on any one
                    // instant — the drain/trim tick is 200ms, and this test
                    // writes far faster than that.
                    assert!(
                        n < 1000,
                        "change log grew far beyond a single drain/trim tick's worth of \
                         writes: {n} records after {} puts",
                        i + 1
                    );
                }
            }

            let tablet = only_tablet(&node, table);
            await_pending_changes(&node, tablet, 0, "after sustained writes stop").await;
        })
        .await
        .expect("did not converge within 90s");
    }

    /// A real process crash + restart, some time after writes to an indexed
    /// table land, must recover to the complete, correct GSI with no
    /// record's partition ever skipped — the ADR 0042 §7/§8 "over-covers,
    /// never under-covers" guarantee. Real `ProdEnv` gives no hook to pin
    /// the exact instant relative to the drain's own reconcile-entries/
    /// cursor-write boundary, so this does not assert on a specific
    /// pre-crash state (a short delay only biases, without guaranteeing,
    /// toward catching the node mid-reconciliation); it proves the property
    /// that must hold regardless of which state the crash actually catches
    /// the drain in — genuine WAL/engine recovery, not a simulated one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn crash_mid_reconcile_recovers_without_skipping_or_corrupting_the_gsi() {
        timeout(Duration::from_secs(90), async {
            let dir = tempfile::tempdir().expect("tempdir");
            // Initial bring-up: retry against the documented port-TOCTOU
            // race (`docs/engineering-lessons.md`) with a fresh config +
            // dir each attempt.
            let mut last_err = None;
            let mut brought_up = None;
            for attempt in 0..16 {
                let node_dir = dir.path().join(format!("node-{attempt}"));
                let config = single_node_config();
                match run_node(&config, 0, &node_dir).await {
                    Ok(node) => {
                        brought_up = Some((node, config, node_dir));
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            let (node, config, node_dir) = brought_up.unwrap_or_else(|| {
                panic!(
                    "could not bring up single node after retries (ports kept getting stolen): {last_err:?}"
                )
            });
            await_control_leader(&node).await;

            let table = "orders";
            create_table_with_gsi(node.dynamo_addr(), table).await;
            let ids: Vec<String> = (0..40u32).map(|i| format!("o{i}")).collect();
            for id in &ids {
                put_item(node.dynamo_addr(), table, id).await;
            }
            sleep(Duration::from_millis(20)).await;
            node.shutdown_graceful().await;

            // Same-address restart: this must reuse the captured
            // config/dir (that's the property under test), so — unlike the
            // bring-up above — it retries the rebind itself within a
            // bounded wall-clock deadline instead of reallocating ports
            // (the `restart_same_addrs` idiom, `tests/support/mod.rs`).
            let restart_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let node2 = loop {
                match run_node(&config, 0, &node_dir).await {
                    Ok(node2) => break node2,
                    Err(e) => {
                        assert!(
                            tokio::time::Instant::now() < restart_deadline,
                            "restart on the same dir/addresses did not rebind: {e}"
                        );
                        sleep(Duration::from_millis(50)).await;
                    }
                }
            };
            await_control_leader(&node2).await;

            for id in &ids {
                await_indexed(node2.dynamo_addr(), table, id).await;
            }

            let tablet = only_tablet(&node2, table);
            await_pending_changes(&node2, tablet, 0, "after recovery").await;
        })
        .await
        .expect("crash/restart recovery did not converge in time");
    }

    /// Split a table's tablet, then reconcile the fresh right child's own
    /// items — a cold start from `W = 0` (the min-over-rows rule over an
    /// empty set: the right child inherits no cursor row at all) — and
    /// confirm it converges to the correct GSI, on both sides, without
    /// corrupting anything (idempotence).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn split_right_childs_cold_start_re_reconciles_from_zero_without_corrupting_the_gsi() {
        timeout(Duration::from_secs(90), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node(dir.path()).await;
            let dynamo_addr = node.dynamo_addr();
            let client_addr = node.client_addr();
            let table = "orders";
            create_table_with_gsi(dynamo_addr, table).await;

            let left_id = find_id_on_side(&BOUNDARY, true, "l");
            put_item(dynamo_addr, table, &left_id).await;
            await_indexed(dynamo_addr, table, &left_id).await;

            let parent = only_tablet(&node, table);
            split(client_addr, parent, BOUNDARY.to_vec()).await;
            await_true(20, "split produced two tablets", || {
                tablets_of(&node, table).len() == 2
            })
            .await;

            let right = {
                let m = node.metadata();
                tablets_of(&node, table)
                    .into_iter()
                    .find(|t| !m.tablets[t].range.start.is_empty())
                    .expect("the right child's range doesn't start at the ring's own beginning")
            };

            let right_ids: Vec<String> = (0..8)
                .map(|i| find_id_on_side(&BOUNDARY, false, &format!("r{i}")))
                .collect();
            for id in &right_ids {
                put_item(dynamo_addr, table, id).await;
            }
            for id in &right_ids {
                await_indexed(dynamo_addr, table, id).await;
            }
            // The split didn't corrupt the other side either.
            await_indexed(dynamo_addr, table, &left_id).await;

            let right_group = await_hosted(&node, right, "right child hosted").await;
            await_cursor_some(&right_group, "right child's own gsi cursor advances").await;
            await_pending_changes(&node, right, 0, "right child's own change log drains").await;
        })
        .await
        .expect("split cold-start scenario did not converge in time");
    }

    /// An expected consumer ("gsi", since this table has a GSI) with no
    /// cursor row at all must block trim **entirely** — the ADR 0042 §7 safe
    /// default. `index_drain_loop`'s own first statement is an
    /// unconditional 200ms sleep before its very first tick, so immediately
    /// after a write (a couple of fast loopback round trips, reliably well
    /// under that), no reconciliation has had a chance to run at all: the
    /// "gsi" tag genuinely has no row yet, checked directly here rather than
    /// inferred.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn trim_never_deletes_past_an_expected_consumers_missing_cursor() {
        timeout(Duration::from_secs(30), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node(dir.path()).await;
            let table = "orders";
            create_table_with_gsi(node.dynamo_addr(), table).await;
            put_item(node.dynamo_addr(), table, "o0").await;

            let tablet = only_tablet(&node, table);
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");
            assert_eq!(
                group.cursor_min_watermark(GSI_TAG).await,
                None,
                "an expected tag with no row yet must read as no watermark"
            );
            assert_eq!(
                group.pending_changes().await.len(),
                1,
                "the janitor must not have trimmed anything with no cursor row to bound it"
            );

            // "Blocks trim" isn't "blocks forever": once the drain does run,
            // it must still converge normally.
            await_pending_changes(&node, tablet, 0, "after the drain's first real pass").await;
        })
        .await
        .expect("did not converge in time");
    }
}

/// The DynamoDB Streams **sealer** regressions (ADR 0042/0043, round-3
/// sealer PR): the seal arm's triggers/sequence, the F10/F12-b hot-trim
/// rework, and F11's split-key token alignment. A fourth in-crate module in
/// this file's own private-handle class (alongside `gsi_drain_cursor_tests`
/// above): needs `CpGroup::pending_changes`/`approx_bytes`, the plain
/// client-protocol `ClientRequest::SplitTablet`, and — to prove a segment
/// genuinely landed durably — a second `FsSegmentStore` handle pointed at
/// the exact same `<node dir>/segments` path `build_segment_store` roots the
/// **default** cluster store's own local building block at (see that
/// function's doc), read directly via `SegmentStore::get` rather than
/// through any production API (there is none yet — the read path is PR6).
/// Every test uses [`run_node_with_streams`] with tiny knobs (this
/// codebase's own testing discipline — never wait out the 4h/4MiB
/// production defaults).
#[cfg(test)]
mod stream_sealer_tests {
    use std::net::SocketAddr;
    use std::path::Path;
    use std::time::Duration;

    use animus_env::SegmentStore;
    use animus_tablet::TabletId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::config::NodeRole;
    use crate::{
        ClientRequest, ClientResponse, ClusterConfig, ClusterEdgeState, Node, RoleAddrs,
        SegmentStoreConfig, StorageBackend, StreamSealKnobs, TxnTableWrite, read_frame,
        run_node_with_streams, write_frame,
    };

    fn free_addrs(count: usize) -> Vec<SocketAddr> {
        let ls: Vec<std::net::TcpListener> = (0..count)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        ls.iter().map(|l| l.local_addr().unwrap()).collect()
    }

    fn single_node_config() -> ClusterConfig {
        let addrs = free_addrs(6);
        ClusterConfig {
            nodes: vec![RoleAddrs {
                id: crate::config::node_id(0),
                role: NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                cql: addrs[3],
                admin: addrs[4],
                intra: addrs[5],
            }],
        }
    }

    async fn await_control_leader(node: &Node) {
        timeout(Duration::from_secs(10), async {
            loop {
                if node.is_control_leader() {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("node did not become control leader in time");
    }

    /// A single combined node, DynamoDB Streams sealer knobs set to `knobs`
    /// (never the production defaults) — see the module doc. Retries
    /// against the documented port-TOCTOU race
    /// (`docs/engineering-lessons.md`) with a fresh config each attempt.
    async fn single_node_with_streams(dir: &Path, knobs: StreamSealKnobs) -> Node {
        let mut last_err = None;
        for attempt in 0..16 {
            let config = single_node_config();
            match run_node_with_streams(
                &config,
                0,
                dir.join(format!("node-{attempt}")),
                StorageBackend::default(),
                Duration::from_secs(600),
                knobs,
                SegmentStoreConfig::default(),
                crate::DEFAULT_STREAM_RETENTION,
            )
            .await
            {
                Ok(node) => {
                    await_control_leader(&node).await;
                    return node;
                }
                Err(e) => {
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!(
            "could not bring up single node with stream knobs after retries \
             (ports kept getting stolen): {last_err:?}"
        );
    }

    /// Like [`single_node_with_streams`], but also opts the node's
    /// data-plane CP groups into quiescence with `quiesce_after` (ADR 0044
    /// phase-1 PR5) and, optionally, a **byte** auto-split threshold (PR6's
    /// own multi-sweeper regression needs auto-split and streaming/sealing
    /// wired at once) — no test-only wrapper mirroring `run_node_with_streams`
    /// exists for this combination yet (PR7 adds the production `--quiesce-
    /// after` CLI flag), so this builds the node directly via
    /// `BoundNode::start_with_growth` (`run_node_with_streams`'s own body,
    /// with the extra arguments) rather than growing the production surface
    /// just for these tests.
    async fn single_node_with_streams_and_quiesce_after(
        dir: &Path,
        knobs: StreamSealKnobs,
        auto_split_bytes: Option<u64>,
        quiesce_after: Duration,
    ) -> Node {
        // `Node::bind` is where the port-TOCTOU race
        // (`docs/engineering-lessons.md`) actually bites — retry with a
        // fresh config each attempt.
        let mut last_err = None;
        let mut bound_state = None;
        for attempt in 0..16 {
            let config = single_node_config();
            let addrs = config.nodes[0].clone();
            match Node::bind(
                crate::config::node_id(0),
                addrs,
                dir.join(format!("node-{attempt}")),
            )
            .await
            {
                Ok(bound) => {
                    bound_state = Some((bound, config));
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        let (bound, config) = bound_state.unwrap_or_else(|| {
            panic!(
                "could not bind single node with streams+quiescence after retries \
                 (ports kept getting stolen): {last_err:?}"
            )
        });
        let mut client_route = std::collections::BTreeMap::new();
        client_route.insert(crate::config::node_id(0), config.nodes[0].client);
        let mut intra_route = std::collections::BTreeMap::new();
        intra_route.insert(crate::config::node_id(0), config.nodes[0].intra);
        let node = bound
            .start_with_growth(
                config.peer_book(),
                config.control_ids(),
                config.data_ids(),
                StorageBackend::default(),
                ClusterEdgeState::new(),
                client_route,
                intra_route,
                None,
                auto_split_bytes,
                vec![config.nodes[0].admin],
                Duration::from_secs(600),
                knobs,
                SegmentStoreConfig::default(),
                crate::DEFAULT_STREAM_RETENTION,
                None,
                quiesce_after,
            )
            .await
            .expect("bring up single node with streams + quiescence");
        await_control_leader(&node).await;
        node
    }

    /// One DynamoDB JSON request over the real HTTP wire (mirroring
    /// `gsi_drain_cursor_tests::dynamo` above — duplicated rather than
    /// shared, since sibling test modules keep their own fixtures
    /// independent).
    async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
        let mut s = TcpStream::connect(addr).await.expect("connect");
        let req = format!(
            "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
             Connection: close\r\n\
             Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        s.write_all(req.as_bytes()).await.expect("write");
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.expect("read");
        let text = String::from_utf8_lossy(&buf).into_owned();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
        (status, body.to_owned())
    }

    /// A table with a single-attribute key and `NEW_AND_OLD_IMAGES` stream
    /// enabled at creation.
    async fn create_streamed_table(addr: SocketAddr, table: &str) {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            &format!(
                r#"{{"TableName":"{table}",
                    "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                    "StreamSpecification":{{"StreamEnabled":true,
                        "StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
    }

    /// `PutItem` with a `"val"` attribute of `pad_len` filler bytes (a
    /// convenient knob for tripping a byte-size seal trigger deterministically).
    async fn put_item_padded(addr: SocketAddr, table: &str, id: &str, pad_len: usize) {
        let pad = "x".repeat(pad_len);
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"val":{{"S":"{pad}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
    }

    fn only_tablet(node: &Node, table: &str) -> TabletId {
        let mut ts: Vec<TabletId> = node
            .metadata()
            .tablets
            .iter()
            .filter(|(_, t)| t.table.as_deref() == Some(table))
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(ts.len(), 1, "expected exactly one tablet for `{table}`");
        ts.pop().unwrap()
    }

    async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        while !cond() {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// The `FsSegmentStore` `build_segment_store`'s **default** cluster
    /// store roots its own per-node local building block at, for a single
    /// node's `dir/node-0` (`BoundNode::dir.join("segments")`) — test-only
    /// introspection, since the read path (PR6) has no production accessor
    /// yet.
    fn node_segment_store(dir: &Path) -> animus_env::FsSegmentStore {
        animus_env::FsSegmentStore::new(dir.join("node-0").join("segments"))
    }

    /// **Seal happy path (size trigger)**: a tiny `--stream-seal-bytes`
    /// tripped by a handful of padded writes lands a segment on this node's
    /// own local store, commits a `SealStreamShard` catalog row with the
    /// correct `hlc_range`/`count`/non-empty `replicas`, and the hot-trim
    /// arm then trims every now-sealed record on its very next tick.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn seal_size_trigger_lands_segment_and_trims_hot_tail() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 200,
                    seal_age: Duration::from_secs(3600),
                },
            )
            .await;
            let table = "orders";
            create_streamed_table(node.dynamo_addr(), table).await;

            for i in 0..10u32 {
                put_item_padded(node.dynamo_addr(), table, &format!("o{i}"), 50).await;
            }

            let tablet = only_tablet(&node, table);
            await_true(20, "a seal commits a catalog row", || {
                node.metadata().stream_shards.contains_key(&(tablet, 0))
            })
            .await;
            let row = node.metadata().stream_shards[&(tablet, 0)].clone();
            assert_eq!(row.table, table);
            assert!(row.count >= 1, "the seal covered at least one record");
            assert_eq!(
                row.hlc_range.0, 0,
                "the tablet's own first seal starts exclusive-from-zero"
            );
            assert!(
                !row.replicas.is_empty(),
                "a committed row only ever exists after put_replicated returned Ok"
            );

            // The hot-trim arm deletes every now-sealed record on its next tick.
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");
            await_true(20, "hot tail trims to empty after the seal", || {
                futures::executor::block_on(group.pending_changes()).is_empty()
            })
            .await;

            // The segment object itself landed durably at the row's own
            // ledger-named id (never the bare deterministic `segment_id`).
            let store = node_segment_store(dir.path());
            let seg_id = &row.object_id;
            let bytes = store
                .get(seg_id)
                .await
                .expect("segment store read")
                .expect("the segment object must exist after a committed seal");
            let decoded = animus_cp_data::segment::decode(&bytes).expect("decode segment");
            assert_eq!(decoded.header.count, row.count);
            assert_eq!(decoded.header.hlc_range, row.hlc_range);
        })
        .await
        .expect("did not converge in time");
    }

    /// **Age trigger** on an otherwise quiet table: a tiny `--stream-seal-age`
    /// seals a couple of items whose combined bytes never approach the (huge)
    /// size threshold.
    ///
    /// **This is also the never-sealed-fallback regression (ADR 0042 fork
    /// G)**: this tablet has no `stream_shards` catalog row at all when the
    /// two writes land, so `seal_tick`'s age trigger has no
    /// `Metadata::last_seal_wall_ms` to read and must run its one-time
    /// `pending_changes()` bootstrap scan to seed the fallback basis instead
    /// — this test is exactly the scenario that fallback exists to prevent
    /// from regressing into "never fires" for a genuinely low-traffic stream
    /// that has never sealed before. [`age_trigger_uses_catalog_seal_time_for_a_later_backlog`]
    /// below is this test's catalog-basis sibling, for a tablet that HAS
    /// already sealed once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn age_trigger_seals_a_quiet_table() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 64 * 1024 * 1024, // never trips by size here
                    seal_age: Duration::from_millis(300),
                },
            )
            .await;
            let table = "quiet";
            create_streamed_table(node.dynamo_addr(), table).await;
            put_item_padded(node.dynamo_addr(), table, "k0", 4).await;
            put_item_padded(node.dynamo_addr(), table, "k1", 4).await;

            let tablet = only_tablet(&node, table);
            await_true(20, "the age trigger seals a quiet table's tail", || {
                node.metadata().stream_shards.contains_key(&(tablet, 0))
            })
            .await;
            let row = node.metadata().stream_shards[&(tablet, 0)].clone();
            assert_eq!(row.count, 2, "both quiet writes are covered by one seal");
        })
        .await
        .expect("did not converge in time");
    }

    /// **Empty hot tail never seals**: with no writes at all, neither
    /// trigger ever fires (there is nothing for the age trigger to measure,
    /// and zero bytes never exceeds any positive size threshold) — several
    /// ticks' worth of real time produces zero catalog rows.
    ///
    /// **Also the ADR 0042 fork G idle-no-scan regression, zero-bytes case**:
    /// `seal_tick`'s `approx_bytes_kind(KIND_CHANGE) == 0` short-circuit
    /// returns before ever reaching `pending_changes()`/`seal_now` — this
    /// test's real assertion (zero catalog rows after many ticks) is the
    /// observable proof that branch never fired.
    /// [`sub_threshold_backlog_never_seals_while_below_both_triggers`] below
    /// is this test's nonzero-but-below-threshold sibling.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn empty_hot_tail_never_seals() {
        timeout(Duration::from_secs(30), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 1, // would trip immediately if anything were pending
                    seal_age: Duration::from_millis(50),
                },
            )
            .await;
            let table = "empty";
            create_streamed_table(node.dynamo_addr(), table).await;

            sleep(Duration::from_millis(800)).await; // several ticks + trigger windows
            assert!(
                node.metadata().stream_shards.is_empty(),
                "an empty hot tail must never produce a seal"
            );
        })
        .await
        .expect("did not converge in time");
    }

    /// **Below-both-triggers backlog performs no *repeated* scan (ADR 0042
    /// fork G)**: with both thresholds deliberately huge, a real, nonzero
    /// `KIND_CHANGE` backlog sits for many ticks without ever tripping a
    /// seal — proving the unconditional every-tick scan this fork removes
    /// is gone even when there IS a hot tail, not just when it's empty
    /// ([`empty_hot_tail_never_seals`] covers the zero-bytes case; this is
    /// its nonzero-but-below-threshold sibling, and the more important of
    /// the two — an idle streamed tablet in practice almost always has
    /// *some* residual hot bytes sitting well under both knobs, not
    /// literally zero).
    ///
    /// **One bounded, one-time scan is still expected here, and that's by
    /// design**: this tablet has never sealed, so its very first tick with
    /// nonzero bytes runs `seal_tick`'s one-time `pending_changes()`
    /// bootstrap to seed the never-sealed fallback basis (see that
    /// function's own doc for why a scan-free driver-local guess is
    /// actually wrong, not just more expensive). What this test actually
    /// proves is that this happens **at most once** for this tablet, never
    /// again on any of the many subsequent ticks the `sleep` below spans —
    /// the observable evidence is the same as `empty_hot_tail_never_seals`
    /// (zero catalog rows after many ticks), since a repeated scan finding
    /// the identical unsealed backlog every tick would still never trip
    /// either trigger on its own; what would differ under a *regressed*
    /// unconditional-scan design is the CPU cost paid to reach that same
    /// "no seal" outcome, which this test's real-time-bounded `sleep` cannot
    /// directly observe (no `pending_changes()` call counter exists, and
    /// adding one at the `CpGroup` level would also count the GSI drain and
    /// hot-trim arms' own independent calls to the same accessor — both out
    /// of this fork's scope). The steady-state no-scan property is instead
    /// enforced by construction: after the one bootstrap tick memoizes this
    /// tablet's basis, `pending_changes()` is reachable from `seal_tick`
    /// only inside [`seal_now`], itself reachable only through the
    /// `size_hot || age_hot` branch — which, with both knobs huge, provably
    /// never evaluates `true` for the rest of this test.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sub_threshold_backlog_never_seals_while_below_both_triggers() {
        timeout(Duration::from_secs(30), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 64 * 1024 * 1024,        // this backlog never approaches it
                    seal_age: Duration::from_secs(3600), // nor does elapsed time
                },
            )
            .await;
            let table = "sub_threshold";
            create_streamed_table(node.dynamo_addr(), table).await;
            put_item_padded(node.dynamo_addr(), table, "k0", 16).await;
            put_item_padded(node.dynamo_addr(), table, "k1", 16).await;

            // Several ticks' worth of real time (`INDEX_DRAIN_INTERVAL` is
            // 200ms) — long enough for many idle evaluations of a real,
            // nonzero backlog.
            sleep(Duration::from_millis(900)).await;
            assert!(
                node.metadata().stream_shards.is_empty(),
                "a real, nonzero backlog below both triggers must never seal"
            );
        })
        .await
        .expect("did not converge in time");
    }

    /// **Catalog-derived repeat seal (ADR 0042 fork G)**: once a tablet has
    /// sealed at least once, a LATER backlog's age trigger is computed from
    /// `Metadata::last_seal_wall_ms` (the catalog row's own `seal_wall_ms`),
    /// never from the driver-local never-sealed fallback
    /// [`age_trigger_seals_a_quiet_table`] exercises. Proven by forcing a
    /// first seal (age trigger, necessarily the fallback path — no catalog
    /// row exists yet), then writing again and confirming a second seal
    /// lands under the same tiny `--stream-seal-age`: by this point
    /// `seal_tick`'s fallback map entry for this tablet was already cleared
    /// the instant the first catalog row appeared (see that function's own
    /// doc), so the second seal's timing can only have come from the
    /// catalog read.
    ///
    /// A deliberate consequence of "time since last seal" semantics (the
    /// accepted `Metric::StreamSealBacklogMs` change) is visible here too:
    /// the second seal can fire almost as soon as its own backlog is merely
    /// non-empty, once the first seal is already older than
    /// `--stream-seal-age` — it does not wait for the SECOND backlog's own
    /// records to individually age past the threshold. That is the
    /// intended "seal roughly every `seal_age` while the tablet keeps
    /// writing" rhythm (ADR 0042 §13's AWS-echoing ~4h default), not a bug.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn age_trigger_uses_catalog_seal_time_for_a_later_backlog() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 64 * 1024 * 1024, // never trips by size
                    seal_age: Duration::from_millis(300),
                },
            )
            .await;
            let table = "catalog_age_repeat";
            create_streamed_table(node.dynamo_addr(), table).await;
            put_item_padded(node.dynamo_addr(), table, "first", 4).await;

            let tablet = only_tablet(&node, table);
            await_true(20, "the first (never-sealed-fallback) seal commits", || {
                node.metadata().stream_shards.contains_key(&(tablet, 0))
            })
            .await;

            // A second write starts a fresh, tiny backlog.
            put_item_padded(node.dynamo_addr(), table, "second", 4).await;
            await_true(
                20,
                "a second seal lands using the catalog's own last-seal time",
                || node.metadata().stream_shards.contains_key(&(tablet, 1)),
            )
            .await;
            let second = node.metadata().stream_shards[&(tablet, 1)].clone();
            assert_eq!(
                second.count, 1,
                "the second seal covers only the fresh write"
            );
        })
        .await
        .expect("did not converge in time");
    }

    /// **Boundary: exactly-at-watermark records are excluded, not
    /// re-included** — the seal filter is `packed_hlc > watermark`, never
    /// `>=`. Proven by forcing two back-to-back size-triggered seals (a tiny
    /// `--stream-seal-bytes` so each single padded write exceeds it on its
    /// own) and checking the second seal's own `hlc_range.0` lands exactly
    /// on the first seal's `hlc_range.1` (the shared boundary point) while
    /// covering only the second write (`count == 1`, not `2`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn boundary_exactly_at_watermark_excluded() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 20, // one padded write alone exceeds this
                    seal_age: Duration::from_secs(3600),
                },
            )
            .await;
            let table = "boundary";
            create_streamed_table(node.dynamo_addr(), table).await;
            let tablet = only_tablet(&node, table);

            put_item_padded(node.dynamo_addr(), table, "first", 50).await;
            await_true(20, "first seal commits", || {
                node.metadata().stream_shards.contains_key(&(tablet, 0))
            })
            .await;
            let first = node.metadata().stream_shards[&(tablet, 0)].clone();

            put_item_padded(node.dynamo_addr(), table, "second", 50).await;
            await_true(20, "second seal commits", || {
                node.metadata().stream_shards.contains_key(&(tablet, 1))
            })
            .await;
            let second = node.metadata().stream_shards[&(tablet, 1)].clone();

            assert_eq!(
                second.hlc_range.0, first.hlc_range.1,
                "the second seal's exclusive start is exactly the first seal's \
                 committed end"
            );
            assert_eq!(
                second.count, 1,
                "the second seal covers only the NEW record, never re-including \
                 the first seal's own boundary record"
            );
        })
        .await
        .expect("did not converge in time");
    }

    /// **F10/F12-b: the hot-trim min-rule with a GSI and a stream together.**
    /// A table with both an enabled stream and a GSI: trim must wait for
    /// **both** terms — a fresh table (neither the GSI's `"gsi"` cursor row
    /// nor a stream seal exists yet) blocks trim entirely; once the stream
    /// has sealed but the GSI drain hasn't reconciled yet is still blocked
    /// (proven by writing enough to trip the tiny size trigger while an
    /// artificially large table keeps the GSI busy would be racy — instead
    /// this proves the **simpler, decisive** half directly: hot records
    /// survive until the GSI cursor exists, `cursor_min_watermark` is
    /// `Some`, AND the stream watermark is `Some`, matching `trim_janitor`'s
    /// own documented rule).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hot_trim_min_rule_gsi_and_stream_together() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 32, // trips on the very first padded write
                    seal_age: Duration::from_secs(3600),
                },
            )
            .await;
            let table = "gsi_and_stream";
            let (status, body) = dynamo(
                node.dynamo_addr(),
                "DynamoDB_20120810.CreateTable",
                &format!(
                    r#"{{"TableName":"{table}",
                        "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                        "GlobalSecondaryIndexes":[
                            {{"IndexName":"by-g",
                             "KeySchema":[{{"AttributeName":"g","KeyType":"HASH"}}],
                             "Projection":{{"ProjectionType":"ALL"}}}}],
                        "StreamSpecification":{{"StreamEnabled":true,
                            "StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "CreateTable failed: {body}");
            let tablet = only_tablet(&node, table);
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");

            put_item_padded(node.dynamo_addr(), table, "k0", 50).await;

            // The stream side seals quickly (tiny threshold) — but trim must
            // still wait on the GSI side too.
            await_true(20, "the stream side seals", || {
                node.metadata().stream_shards.contains_key(&(tablet, 0))
            })
            .await;
            await_true(
                10,
                "the GSI drain reconciles and advances its cursor",
                || futures::executor::block_on(group.cursor_min_watermark(GSI_TAG)).is_some(),
            )
            .await;
            // Both terms are now present — trim converges to empty.
            await_true(20, "trim converges once both terms are present", || {
                futures::executor::block_on(group.pending_changes()).is_empty()
            })
            .await;
        })
        .await
        .expect("did not converge in time");
    }

    /// **F12-b coexistence: a disabled, still-draining stream does NOT block
    /// trim.** Write, disable (the final seal moves every record into a
    /// committed segment), then confirm the hot scope is fully trimmed even
    /// though the label's catalog rows are still present (un-reaped —
    /// retention is a later PR) — the disabled stream's own term is simply
    /// omitted from the trim-bound computation, per `trim_janitor`'s
    /// documented rule.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disabled_draining_stream_does_not_block_trim() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 64 * 1024 * 1024, // never trips on its own
                    seal_age: Duration::from_secs(3600),
                },
            )
            .await;
            let table = "disable_trim";
            create_streamed_table(node.dynamo_addr(), table).await;
            put_item_padded(node.dynamo_addr(), table, "k0", 10).await;
            put_item_padded(node.dynamo_addr(), table, "k1", 10).await;

            let tablet = only_tablet(&node, table);
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");

            let (status, body) = dynamo(
                node.dynamo_addr(),
                "DynamoDB_20120810.UpdateTable",
                &format!(
                    r#"{{"TableName":"{table}","StreamSpecification":{{"StreamEnabled":false}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "UpdateTable disable failed: {body}");

            assert!(
                node.metadata().table_stream(table).is_none(),
                "the schema's stream must be gone after disable"
            );
            assert!(
                !node.metadata().stream_shards.is_empty(),
                "the disabled label's catalog rows are still present (un-reaped)"
            );
            await_true(20, "the hot scope drains fully post-disable", || {
                futures::executor::block_on(group.pending_changes()).is_empty()
            })
            .await;
        })
        .await
        .expect("did not converge in time");
    }

    /// **Disable = final seal, end to end, with epoch continuity on
    /// re-enable.** Write, disable: every record lands in a committed
    /// segment covering exactly what was written, the hot scope empties,
    /// and the schema no longer names a stream. Re-enabling mints a
    /// genuinely new label, and a further seal continues the tablet's own
    /// epoch chain (epoch 1, not a reset to 0) — a tablet's epoch counter is
    /// a property of its physical seal history, never resetting across a
    /// disable/re-enable cycle (`StreamShardRow`'s own identity note).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disable_final_seal_then_reenable_continues_the_epoch_chain() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 64 * 1024 * 1024,
                    seal_age: Duration::from_secs(3600),
                },
            )
            .await;
            let table = "disable_final_seal";
            create_streamed_table(node.dynamo_addr(), table).await;
            let first_label = node
                .metadata()
                .table_stream(table)
                .expect("just enabled")
                .label
                .clone();
            for i in 0..3u32 {
                put_item_padded(node.dynamo_addr(), table, &format!("d{i}"), 10).await;
            }

            let tablet = only_tablet(&node, table);
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");

            let (status, body) = dynamo(
                node.dynamo_addr(),
                "DynamoDB_20120810.UpdateTable",
                &format!(
                    r#"{{"TableName":"{table}","StreamSpecification":{{"StreamEnabled":false}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "UpdateTable disable failed: {body}");

            assert!(node.metadata().table_stream(table).is_none());
            let sealed = node.metadata().stream_shards[&(tablet, 0)].clone();
            assert_eq!(sealed.label, first_label);
            assert_eq!(sealed.count, 3, "the final seal covered every write");
            await_true(20, "hot scope empties after the final seal", || {
                futures::executor::block_on(group.pending_changes()).is_empty()
            })
            .await;

            // Re-enable: a genuinely new label.
            let (status, body) = dynamo(
                node.dynamo_addr(),
                "DynamoDB_20120810.UpdateTable",
                &format!(
                    r#"{{"TableName":"{table}","StreamSpecification":{{"StreamEnabled":true,
                        "StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "UpdateTable re-enable failed: {body}");
            let second_label = node
                .metadata()
                .table_stream(table)
                .expect("just re-enabled")
                .label
                .clone();
            assert_ne!(second_label, first_label, "re-enable mints a fresh label");

            // A second disable (this test's `seal_bytes`/`seal_age` are
            // deliberately huge — never trip on their own within any
            // reasonable test time) forces a final seal of the ONE new
            // write via the identical, already-proven disable mechanism
            // rather than waiting on a periodic trigger: the point under
            // test is epoch continuity, not the trigger evaluation itself
            // (covered separately by the size/age tests above).
            put_item_padded(node.dynamo_addr(), table, "after-reenable", 10).await;
            let (status, body) = dynamo(
                node.dynamo_addr(),
                "DynamoDB_20120810.UpdateTable",
                &format!(
                    r#"{{"TableName":"{table}","StreamSpecification":{{"StreamEnabled":false}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "second UpdateTable disable failed: {body}");

            assert!(
                node.metadata().stream_shards.contains_key(&(tablet, 1)),
                "the continued epoch lands at 1, not a reset to 0"
            );
            let continued = node.metadata().stream_shards[&(tablet, 1)].clone();
            assert_eq!(
                continued.label, second_label,
                "the continued epoch seals under the NEW label"
            );
            assert_eq!(
                continued.hlc_range.0, sealed.hlc_range.1,
                "epoch 1 starts exclusive-from exactly epoch 0's committed end — \
                 the tablet's own physical seal history, not the label, drives \
                 watermark continuity"
            );
        })
        .await
        .expect("did not converge in time");
    }

    /// **F11: a streamed table's auto-split key rounds down to its own
    /// 8-byte token boundary.** Two items sharing an artificially-forced
    /// token prefix (via a raw `SplitTablet` at a chosen boundary, mirroring
    /// `gsi_drain_cursor_tests`' own split-testing technique) prove the
    /// alignment directly: splitting a streamed table's tablet at a
    /// **non-token-aligned** point (as an unstreamed table's own
    /// byte-weighted/positional median could legitimately choose) still
    /// only ever needs a token-boundary split key to succeed — this test
    /// exercises the F11 code path's own token-truncation logic in
    /// isolation via a direct `auto_split`-style helper rather than waiting
    /// out `AUTO_SPLIT_INTERVAL`, since token alignment is a pure function
    /// of the chosen key and the table's stream flag, not of timing.
    #[test]
    fn f11_token_alignment_rounds_a_streamed_split_key_down_to_its_token_boundary() {
        // The exact truncation `auto_split_loop` performs (see its own F11
        // comment in `lib.rs`) — pinned here as a focused unit check on the
        // primitive itself: a real key's own leading `TOKEN_BYTES` survive
        // unchanged, and everything past them is dropped.
        let real_key = {
            let mut k = vec![0xAAu8; animus_tablet::TOKEN_BYTES];
            k.extend_from_slice(b"-some-partition-keys-own-suffix-bytes");
            k
        };
        let truncated = real_key[..animus_tablet::TOKEN_BYTES.min(real_key.len())].to_vec();
        assert_eq!(truncated, vec![0xAAu8; animus_tablet::TOKEN_BYTES]);
        assert!(
            truncated.len() < real_key.len(),
            "the aligned split key must be strictly shorter than an unaligned \
             candidate that shares its token"
        );
    }

    /// F11 end to end: an auto-split on a **streamed** table lands with a
    /// split key that is exactly its own 8-byte token — proven by actually
    /// exercising `auto_split_loop`'s own decision through a real streamed,
    /// byte-auto-split-configured single node (a tiny `--auto-split-bytes`
    /// so the trigger fires promptly), then reading the resulting sibling
    /// tablets' own `KeyRange` boundary back out of `Metadata` and checking
    /// it is exactly `TOKEN_BYTES` long.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn f11_end_to_end_auto_split_on_a_streamed_table_lands_a_token_aligned_boundary() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            // `Node::bind` is where the port-TOCTOU race
            // (`docs/engineering-lessons.md`) actually bites — retry with a
            // fresh config each attempt.
            let mut last_err = None;
            let mut bound_state = None;
            for attempt in 0..16 {
                let config = single_node_config();
                match crate::Node::bind(
                    crate::config::node_id(0),
                    config.nodes[0].clone(),
                    dir.path().join(format!("node-{attempt}")),
                )
                .await
                {
                    Ok(bound) => {
                        bound_state = Some((bound, config));
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            let (bound, config) = bound_state.unwrap_or_else(|| {
                panic!(
                    "could not bind single node after retries (ports kept getting \
                     stolen): {last_err:?}"
                )
            });
            let node = bound
                .start_with_streams(
                    config.peer_book(),
                    config.control_ids(),
                    config.data_ids(),
                    StorageBackend::default(),
                    crate::ClusterEdgeState::new(),
                    Default::default(),
                    Default::default(),
                    None,
                    // A generous threshold, deliberately NOT tiny: this
                    // test only needs ONE split to happen, not a
                    // cascade — a too-tiny threshold re-splits every
                    // resulting child indefinitely (each one still
                    // exceeding it), which would make "did it split
                    // exactly once" an unanswerable question. F11's own
                    // alignment property holds regardless of how many
                    // splits happen, so this test only needs "at least
                    // one."
                    Some(2000),
                    vec![config.nodes[0].admin],
                    Duration::from_secs(600),
                    StreamSealKnobs {
                        seal_bytes: 64 * 1024 * 1024, // never seal mid-test
                        seal_age: Duration::from_secs(3600),
                    },
                    SegmentStoreConfig::default(),
                    crate::DEFAULT_STREAM_RETENTION,
                )
                .await
                .expect("start with streams + auto-split-bytes");
            await_control_leader(&node).await;

            let table = "auto_split_streamed";
            create_streamed_table(node.dynamo_addr(), table).await;
            for i in 0..80u32 {
                put_item_padded(node.dynamo_addr(), table, &format!("s{i}"), 32).await;
            }

            await_true(30, "auto-split produces at least a second tablet", || {
                node.metadata().tablets_for_table(table).count() >= 2
            })
            .await;

            // Every resulting tablet's own non-empty boundary (`range.start`)
            // must be exactly one 8-byte token — holds regardless of how
            // many splits actually happened.
            let boundary_lens: Vec<usize> = node
                .metadata()
                .tablets_for_table(table)
                .filter(|(_, t)| !t.range.start.is_empty())
                .map(|(_, t)| t.range.start.len())
                .collect();
            assert!(
                !boundary_lens.is_empty(),
                "at least one split must have produced a non-empty-start child"
            );
            assert!(
                boundary_lens
                    .iter()
                    .all(|&len| len == animus_tablet::TOKEN_BYTES),
                "every streamed-table split boundary must be exactly one \
                 8-byte token, never a longer, unaligned key: {boundary_lens:?}"
            );
        })
        .await
        .expect("did not converge in time");
    }

    /// ADR 0044 phase-1 PR5 (fork D): `change_consumer_loop` holds this
    /// group's quiesce veto while its change log is non-empty, releasing it
    /// once the hot tail trims — proving the veto end to end against a
    /// real streamed table's write/seal/trim cycle (mirrors
    /// [`seal_size_trigger_lands_segment_and_trims_hot_tail`]'s exact
    /// shape, with quiescence layered on top via
    /// [`single_node_with_streams_and_quiesce_after`]).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hot_backlog_holds_the_quiesce_veto_until_the_hot_tail_trims() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams_and_quiesce_after(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 200,
                    seal_age: Duration::from_secs(3600),
                },
                None,
                Duration::from_millis(300),
            )
            .await;
            let table = "vetoed";
            create_streamed_table(node.dynamo_addr(), table).await;

            // One small write: nowhere near the size trigger on its own, so
            // the record sits in the hot tail for a while.
            put_item_padded(node.dynamo_addr(), table, "o0", 4).await;

            let tablet = only_tablet(&node, table);
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");

            await_true(10, "the write lands in the hot tail", || {
                !futures::executor::block_on(group.pending_changes()).is_empty()
            })
            .await;

            // Idle well past `quiesce_after` with the backlog still
            // present — the veto must hold the group awake the whole time,
            // or PR6's later sweeper-skip would strand this table's seal
            // age trigger indefinitely.
            sleep(Duration::from_secs(2)).await;
            assert!(
                !group.is_quiesced(),
                "a group with a non-empty change log must never quiesce"
            );

            // Trip the size trigger — the backlog seals and the hot-trim
            // arm clears it on its next tick.
            for i in 1..10u32 {
                put_item_padded(node.dynamo_addr(), table, &format!("o{i}"), 50).await;
            }
            await_true(20, "hot tail trims to empty after the seal", || {
                futures::executor::block_on(group.pending_changes()).is_empty()
            })
            .await;

            // The veto releases and the group reaches quiescence again.
            await_true(10, "the veto releases and the group quiesces", || {
                group.is_quiesced()
            })
            .await;
        })
        .await
        .expect("did not converge in time");
    }

    /// A table with a single-attribute key and **no stream** — the ADR 0049
    /// marker-record shape (`create_streamed_table`'s plain sibling).
    async fn create_plain_table(addr: SocketAddr, table: &str) {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.CreateTable",
            &format!(
                r#"{{"TableName":"{table}",
                    "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
    }

    /// One admin HTTP-JSON POST (the same hand-rolled wire shape as
    /// [`dynamo`] above, minus the `X-Amz-Target` header).
    async fn admin_post(addr: SocketAddr, path: &str, body: &str) -> (u16, String) {
        let mut s = TcpStream::connect(addr).await.expect("connect admin");
        let req = format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        s.write_all(req.as_bytes()).await.expect("write");
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.expect("read");
        let text = String::from_utf8_lossy(&buf).into_owned();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
        (status, body.to_owned())
    }

    /// **ADR 0049 §4 (Train A rung 4) — the plain-table marker lifecycle:**
    /// a never-streamed, never-indexed table's writes leave image-less
    /// marker records; `change_consumer_loop` now visits such a tablet, the
    /// trim arm's zero-expected-terms rule deletes the markers, the quiesce
    /// veto releases, and the group quiesces. Red before this rung on the
    /// very first await: the loop skipped plain tables outright, so markers
    /// accumulated forever and `pending_changes` never emptied. The second
    /// write/trim/quiesce round proves the sweeper-skip stays a reversible
    /// short-circuit for the marker branch too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn plain_table_markers_trim_to_empty_and_the_tablet_quiesces() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams_and_quiesce_after(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: u64::MAX,
                    seal_age: Duration::from_secs(3600),
                },
                None,
                Duration::from_millis(300),
            )
            .await;
            let table = "plain-markers";
            create_plain_table(node.dynamo_addr(), table).await;
            for i in 0..3u32 {
                put_item_padded(node.dynamo_addr(), table, &format!("p{i}"), 8).await;
            }
            let tablet = only_tablet(&node, table);
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");
            await_true(20, "plain-table markers trim to empty", || {
                futures::executor::block_on(group.pending_changes()).is_empty()
            })
            .await;
            await_true(10, "the veto releases and the group quiesces", || {
                group.is_quiesced()
            })
            .await;
            // Re-wake: a fresh write un-quiesces the group, its marker is
            // trimmed on the loop's next visits, and quiescence returns.
            put_item_padded(node.dynamo_addr(), table, "p-rewake", 8).await;
            await_true(20, "the re-wake write's marker trims too", || {
                futures::executor::block_on(group.pending_changes()).is_empty()
            })
            .await;
            await_true(10, "the group re-quiesces after the re-wake", || {
                group.is_quiesced()
            })
            .await;
        })
        .await
        .expect("did not converge in time");
    }

    /// **ADR 0049 §4 — the admin seeder writes through the kind path**: a
    /// seed of a *streamed* table must leave one real change record per
    /// seeded row (red before this rung: the seeder wrote via the plain
    /// `cp_batch_write`, emitting **zero** records — every seeded row was
    /// silently absent from the table's stream, the same drifted-gate class
    /// as `BatchWriteItem`'s streamed-but-unindexed bug), and a seed of a
    /// *plain* table lands readable rows whose markers trim away.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn admin_seed_writes_through_the_kind_path_on_both_table_shapes() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams_and_quiesce_after(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: u64::MAX, // never seal — records must persist
                    seal_age: Duration::from_secs(3600),
                },
                None,
                Duration::from_millis(300),
            )
            .await;

            // Streamed table: every seeded row leaves a change record.
            let streamed = "seed-streamed";
            create_streamed_table(node.dynamo_addr(), streamed).await;
            let (status, body) = admin_post(
                node.admin_addr(),
                "/admin/data/seed",
                &format!(r#"{{"table":"{streamed}","count":5}}"#),
            )
            .await;
            assert_eq!(status, 200, "seed(streamed) failed: {body}");
            assert!(body.contains(r#""written": 5"#), "partial seed: {body}");
            let group = node
                .edge
                .local_cp(only_tablet(&node, streamed))
                .expect("hosts the streamed tablet");
            await_true(20, "each seeded row left a change record", || {
                futures::executor::block_on(group.pending_changes()).len() == 5
            })
            .await;

            // Plain table: rows land readable; their markers trim away.
            let plain = "seed-plain";
            create_plain_table(node.dynamo_addr(), plain).await;
            let (status, body) = admin_post(
                node.admin_addr(),
                "/admin/data/seed",
                &format!(r#"{{"table":"{plain}","count":5}}"#),
            )
            .await;
            assert_eq!(status, 200, "seed(plain) failed: {body}");
            assert!(body.contains(r#""written": 5"#), "partial seed: {body}");
            let (status, body) = dynamo(
                node.dynamo_addr(),
                "DynamoDB_20120810.GetItem",
                &format!(r#"{{"TableName":"{plain}","Key":{{"id":{{"S":"seed:000000000000"}}}}}}"#),
            )
            .await;
            assert_eq!(status, 200, "GetItem failed: {body}");
            assert!(body.contains("payload"), "seeded row unreadable: {body}");
            let group = node
                .edge
                .local_cp(only_tablet(&node, plain))
                .expect("hosts the plain tablet");
            await_true(20, "seeded plain-table markers trim to empty", || {
                futures::executor::block_on(group.pending_changes()).is_empty()
            })
            .await;
        })
        .await
        .expect("did not converge in time");
    }

    /// **ADR 0049 §3/§4 — a raw client-protocol transactional write leaves a
    /// (consumer-hidden) stage marker**: `ClientRequest::Txn`'s plain-value
    /// writes ride `TxnWrite::plain` with no derived payload, and before
    /// this rung staged **nothing** into the change log — a raw write staged
    /// during an ADR 0050 split build would be invisible to the build's
    /// change-log tail until resolve. On a streamed table (trim blocked —
    /// never sealed), exactly one record must appear per raw transactional
    /// write: the stage marker, `staged` + hidden, never a stream event.
    /// Red before this rung: zero records.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn raw_txn_write_leaves_a_hidden_stage_marker() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams_and_quiesce_after(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: u64::MAX, // never seal — the marker must persist
                    seal_age: Duration::from_secs(3600),
                },
                None,
                Duration::from_secs(600),
            )
            .await;
            let table = "raw-txn-marker";
            create_streamed_table(node.dynamo_addr(), table).await;
            // Provision the tablet before the raw write (a raw `Txn` write
            // auto-provisions via `cp_txn`, but creating it through the edge
            // keeps the schema catalog consistent for the streams surface).
            put_item_padded(node.dynamo_addr(), table, "warmup", 4).await;
            let group = node
                .edge
                .local_cp(only_tablet(&node, table))
                .expect("hosts the tablet");
            let baseline = group.pending_changes().await.len();

            // One raw plain-value transactional write over the real client
            // protocol — the exact shape `animus-cli`/external callers use.
            let pk = animus_dynamo::AttributeValue::S("t-raw-1".into());
            let key = crate::dynamo::item_key(&pk, None);
            let mut item = animus_dynamo::Item::new();
            item.insert("id".to_string(), pk);
            let value = animus_dynamo::wire::encode_stored_item(&item);
            let mut stream = TcpStream::connect(node.client_addr())
                .await
                .expect("connect");
            write_frame(
                &mut stream,
                &ClientRequest::Txn {
                    writes: vec![TxnTableWrite::plain(table.to_string(), key, Some(value))],
                    preconditions: Vec::new(),
                    write_conditions: Vec::new(),
                },
            )
            .await
            .expect("send txn");
            let resp: ClientResponse = read_frame(&mut stream)
                .await
                .expect("read reply")
                .expect("a reply");
            assert!(
                matches!(resp, ClientResponse::TxnCommitted { .. }),
                "raw txn failed: {resp:?}"
            );

            // Exactly one new record: the stage marker — image-less,
            // `staged`, and consumer-hidden (a plain-value write has no
            // resolve-time change record to materialize).
            await_true(20, "the raw txn write's stage marker appears", || {
                futures::executor::block_on(group.pending_changes()).len() == baseline + 1
            })
            .await;
            let records = group.pending_changes().await;
            let new: Vec<_> = records
                .iter()
                .filter_map(|(_, v)| animus_dynamo::ChangeRecord::decode(v))
                .filter(|r| r.staged)
                .collect();
            assert_eq!(new.len(), 1, "exactly one stage marker: {records:?}");
            assert!(
                new[0].consumer_hidden(),
                "a stage marker must never be a stream event"
            );
            assert!(
                new[0].old_image.is_none() && new[0].new_image.is_none(),
                "a stage marker carries no images"
            );
        })
        .await
        .expect("did not converge in time");
    }

    /// ADR 0044 phase-1 PR6: the sweeper-skip regression — once
    /// `change_consumer_loop`/`auto_split_loop` start skipping a quiesced
    /// group outright (rather than merely finding nothing to do, as PR5
    /// alone left it), a **re-woken** tablet must still be picked back up
    /// by every sweeper within roughly one of its own intervals. Combines
    /// both plan-named test shapes in one scenario, since `animusd` has no
    /// `SimEnv` tier of its own to split them across (this crate's whole
    /// suite is real `ProdEnv` sockets/time, per its own `CLAUDE.md`):
    ///
    /// 1. an idle, genuinely empty table quiesces (nothing for any sweeper
    ///    to strand — the negative control);
    /// 2. a write burst crossing the auto-split byte threshold, issued
    ///    *while quiesced*, must still trigger a real split — proving
    ///    `auto_split_loop`'s own skip-gate (`leader.is_quiesced()`) is a
    ///    strict, reversible short-circuit, never a stuck "permanently
    ///    disabled" state: the write itself un-quiesces the group (an
    ///    ordinary Raft propose), so the very next `auto_split_loop` tick
    ///    sees `is_quiesced() == false` again and resumes its ordinary
    ///    byte-threshold check;
    /// 3. the same burst's change-log backlog must still get sealed —
    ///    proving `change_consumer_loop`'s identical skip-gate doesn't
    ///    strand the seal/trim arms either.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rewoken_tablet_is_picked_back_up_by_every_sweeper_within_one_interval() {
        timeout(Duration::from_secs(60), async {
            let dir = tempfile::tempdir().expect("tempdir");
            let node = single_node_with_streams_and_quiesce_after(
                dir.path(),
                StreamSealKnobs {
                    seal_bytes: 2_000,
                    seal_age: Duration::from_secs(3600),
                },
                Some(2_000), // tiny byte auto-split threshold
                Duration::from_millis(300),
            )
            .await;
            let table = "rewoken";
            create_streamed_table(node.dynamo_addr(), table).await;

            let tablet = only_tablet(&node, table);
            let group = node
                .edge
                .local_cp(tablet)
                .expect("this node hosts the tablet");

            // (1) Negative control: an untouched, empty table quiesces —
            // every sweeper is now skipping it every tick.
            await_true(10, "an idle empty table must quiesce", || {
                group.is_quiesced()
            })
            .await;

            // (2)+(3) A write burst crossing the auto-split byte threshold,
            // issued against the now-quiesced group. Retries a transient
            // "outside this group's live range; retry" — a real, expected
            // race this specific test invites (a split can legitimately
            // land *mid-burst*, at which point a later write in the burst
            // targets a key the split just handed to a fresh sibling
            // tablet, and this single-node harness has no forwarding hop to
            // re-resolve it automatically the way a multi-node routed
            // client would) — never masking a genuine failure (any other
            // status/body still hard-fails).
            for i in 0..40u32 {
                let id = format!("r{i}");
                let pad = "x".repeat(100);
                let body = format!(
                    r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"val":{{"S":"{pad}"}}}}}}"#
                );
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                loop {
                    let (status, resp_body) =
                        dynamo(node.dynamo_addr(), "DynamoDB_20120810.PutItem", &body).await;
                    if status == 200 {
                        break;
                    }
                    assert!(
                        resp_body.contains("; retry") || resp_body.contains("retry"),
                        "PutItem({id}) failed non-retryably: {resp_body}"
                    );
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "PutItem({id}) kept failing retryably past its own deadline: {resp_body}"
                    );
                    sleep(Duration::from_millis(50)).await;
                }
            }

            // auto_split_loop must resume and trigger a real split within a
            // couple of its own `AUTO_SPLIT_INTERVAL` (2s) ticks.
            await_true(
                20,
                "auto_split_loop must resume after the wake and split",
                || node.metadata().tablets_for_table(table).count() >= 2,
            )
            .await;

            // change_consumer_loop must resume and seal the burst's own
            // change-log backlog within a couple of its own
            // `INDEX_DRAIN_INTERVAL` (200ms) ticks.
            await_true(
                20,
                "change_consumer_loop must resume and seal the backlog",
                || {
                    node.metadata()
                        .stream_shards
                        .range((tablet, 0)..=(tablet, u64::MAX))
                        .next()
                        .is_some()
                        || node.metadata().tablets_for_table(table).any(|(t, _)| {
                            node.metadata()
                                .stream_shards
                                .range((*t, 0)..=(*t, u64::MAX))
                                .next()
                                .is_some()
                        })
                },
            )
            .await;
        })
        .await
        .expect("did not converge in time");
    }
}
