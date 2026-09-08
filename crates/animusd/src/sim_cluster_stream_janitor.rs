//! `SimCluster`-driven deterministic coverage for the DynamoDB Streams
//! **segment janitor**'s own async loop (`segment_janitor::
//! segment_janitor_loop`, ADR 0043 §A9) — ADR 0061 rung G, C-07 PR 5.
//! Converts the reachable scenarios of `tests/stream_janitor.rs` (real
//! `ProdEnv`, real sockets/disk) into deterministic, seed-replayable
//! `SimCluster` siblings; see that file's own trimmed doc comment for what
//! stays `ProdEnv` and why, and `crates/animusd/CLAUDE.md`'s matching
//! appendix for the full before/after gate accounting.
//!
//! # The widening (`segment_janitor.rs`)
//!
//! `segment_janitor_loop`/`segment_janitor_tick` (and the private helper
//! they share, `update_segment_janitor_progress`) widened from `&ClientCtx`
//! to `<E: Env, R: RelayClient>` — a pure signature change, mirroring
//! `backup_janitor_loop`'s own D4 PR 5 widening exactly: `reap_orphans`
//! needed **no** change at all (it already took only a `&SegmentStoreHandle`
//! and a `&Metadata`, no `ClientCtx`/`RaftNode` in sight). The loop's one
//! real-clock site (`tokio::time::sleep(SEGMENT_JANITOR_INTERVAL)`) became
//! `ctx.env.sleep(..)`; every other site already read `ctx.env.now()`/
//! `ctx.env.metrics()`, which resolve through the `E: Env` bound with no
//! import needed (the `Clock` import this file used to carry became
//! unused and was dropped — a generic body reaches a supertrait's methods
//! through its own bound, no separate `use` required, unlike a concrete
//! type). Both production spawn sites (`spawn_common_tail`) still hand this
//! a concrete `ClientCtx` (`E = ProdEnv, R = AnimusdRelayClient`, the
//! type's own definition-site default) with zero call-site changes —
//! `cargo test -p animusd --test stream_janitor` on the untrimmed file
//! (11/11 passed) is this rung's own proof that production behavior is
//! byte-identical.
//!
//! # The spawn (`sim_cluster.rs`)
//!
//! `SimCluster::new`/`restart` spawn `segment_janitor_loop`
//! **unconditionally** on every node — mirroring `backup_janitor_loop`'s
//! own D4 PR 5 always-on shape, not `auto_split_loop`'s opt-in one: the
//! loop's own leader gate (`ctx.edge.leader_handle()` answering `None`)
//! already makes a non-leader's own tick a cheap idle sleep. The retention
//! window is a **constructor knob**
//! ([`SimCluster::new_with_segment_janitor_retention`]), not a live
//! setter — see that function's own doc, and
//! [`DEFAULT_SIM_SEGMENT_JANITOR_RETENTION`]'s (`sim_cluster.rs`), for why:
//! the production loop's own `retention: Duration` parameter is captured
//! by value at spawn time, so changing it after the fact would mean
//! spawning a second, concurrent loop rather than reconfiguring the first
//! one. `SimCluster::new`'s own default (3600s) is generous enough that no
//! scenario below crosses it by accident unless it deliberately asks for a
//! shorter one. `SimCluster::segment_janitor_progress(node)` mirrors
//! `backup_janitor_progress` exactly (`GET /admin/gc`'s own read, a plain
//! lock/clone/drop).
//!
//! **Nothing new needed covering by `Drop for SimCluster`/`SimCluster::
//! restart`'s existing teardown (issue #753's own discipline)**: the
//! spawned loop captures a plain `ClientCtx` clone, the identical shape
//! `backup_janitor_loop` already captures — no new `Weak`-tracked handle
//! (no new relay/control/raftkv registration) was added, so
//! `dropping_the_cluster_frees_every_nodes_relay_and_edge_state` needed no
//! extension; `restart` respawns the loop with the cluster's own stored
//! `segment_janitor_retention`, the identical `heartbeat_loop`/`backup_
//! janitor_loop`/`auto_split_loop` precedent.
//!
//! # A timing gotcha every scenario below has to design around
//!
//! **Every `SimCluster` op call ([`SimCluster::dynamo`]/`put`/`drive_
//! stream_seal`/`drive_inplace_split_cutover`/…) unconditionally burns a
//! full [`OP_BUDGET`] (12s) of virtual time, regardless of how quickly the
//! underlying work actually finishes** (`SimCluster::spawn_and_capture`'s
//! own `self.sim.run_for(OP_BUDGET)`, unconditional). This makes a genuine
//! "tiny" retention (the real-socket file's own 2s `TINY_RETENTION`)
//! useless for **catching a row mid-sweep**: by the time any op call that
//! could observe the row's state *returns*, retention has almost always
//! already elapsed silently inside that same opaque 12s window, and the
//! row may already be fully reclaimed before this module's own code ever
//! gets a chance to look. Two different strategies below, chosen per
//! scenario:
//!
//! - **Scenarios needing only eventual, whole-scenario convergence** (no
//!   scenario cares *which* tick reclaims a row, only that it eventually
//!   does) use a short retention (a handful of seconds) and poll to
//!   convergence afterward with a generous budget — the timing imprecision
//!   above is simply irrelevant to them.
//! - **The one scenario needing to catch the row genuinely mid-sweep**
//!   ([`expiry_survives_a_control_leader_kill_mid_sweep`]) instead uses a
//!   retention deliberately **larger** than the cluster's own deterministic
//!   setup-phase virtual-time cost (every op call burns exactly
//!   `OP_BUDGET`, so that cost is computable up front, not guessed), then
//!   switches to a **manual**, small-step [`SimCluster::run_for`] poll
//!   loop — never another op call — to advance virtual time in fine
//!   enough increments to observe the row marked-but-not-yet-removed and
//!   crash right there. See that scenario's own doc for the exact
//!   arithmetic.
//!
//! # Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! - [`two_phase_expiry_removes_the_row_and_every_replicas_object`] — the
//!   real-socket file's own happy path, adapted to this fixture's own
//!   **shared** `SimSegmentStore` (`SegmentStoreHandle::S3`, PR 2): unlike
//!   the real cluster/K-replica store, this store has **no per-node
//!   replica concept at all** (`row.replicas` is always empty for `S3`,
//!   the identical fact `Fs`/`EncryptedFs` already document) — see
//!   [`SegmentStoreHandle::S3`]'s own doc in `lib.rs`. This scenario
//!   therefore proves phase 1b's **empty-replicas** branch (`ctx.
//!   segment_store.delete_sealed(&[], seg_id)`), not the cataloged-
//!   replicas branch — a real, if narrower, proof: the row and its
//!   segment object both converge to gone.
//! - [`expiry_survives_a_control_leader_kill_mid_sweep`] — crash the
//!   control-plane leader genuinely mid-sweep (marked, not yet removed)
//!   and confirm the survivors finish the reclaim on their own.
//! - [`reader_never_sees_an_empty_success_gap_across_expiry`] — a
//!   `GetShardIterator` minted before a row's removal, drained while it's
//!   still live, then re-minted after removal: always a real record or a
//!   clean `TrimmedDataAccessException`, never an ambiguous empty `200`.
//! - [`disable_grace_lifecycle_end_to_end_with_reenable_coexistence`] —
//!   F12-b's disable-triggered final seal (`ctx.force_seal_tablet`, called
//!   automatically by `disable_stream`, needing no manual `drive_stream_
//!   seal` at all), the grace window, and re-enable's two coexisting
//!   labels, one draining while the other accumulates independently.
//! - [`drop_table_cascade_converges_via_the_janitor`] — the convergent
//!   design: dropping a table with live catalog rows converges them to
//!   zero via the janitor's ordinary retention-zero rule, with **no**
//!   dedicated cascade code path — proven at this fixture's own default
//!   (generous, 3600s) retention specifically so a pass can't be mistaken
//!   for "retention happened to elapse."
//! - [`mid_grace_drop_removes_both_coexisting_labels`] — the identical
//!   drop-table convergence, but with an old (disabled, draining) label
//!   and a new (currently enabled) label both carrying live rows at drop
//!   time — both converge to zero via the same sweep. Needs one explicit
//!   [`SimCluster::drive_stream_seal`] this fixture's own lack of a
//!   periodic seal loop makes necessary (see that scenario's own doc).
//! - [`metrics_reflect_a_completed_retention_cycle`] — roadmap U-07's own
//!   "observability lands with the mechanism" house rule, asserted via
//!   [`SimCluster::segment_janitor_progress`] rather than the real-socket
//!   file's `/metrics` `stream_segments_expired_total` counter: `Env::
//!   metrics()`'s own default is a **no-op** sink
//!   (`MetricsHandle::noop()`), and `SimCluster` never threads a recording
//!   handle into any node's `env` the way a component wanting sim-visible
//!   metrics is supposed to (`animus-env/CLAUDE.md`'s own "a sim test that
//!   wants to *read* counters threads a recording handle" rule) — so
//!   `Metric::StreamSegmentsExpiredTotal` is genuinely unreachable from
//!   this fixture, not merely untested. `SegmentJanitorProgress` is not a
//!   `Metric` at all (a plain `Arc<Mutex<..>>` struct this loop mutates
//!   directly, `segment_janitor.rs`'s own doc), so it carries the
//!   identical "observability lands with the mechanism" proof with no
//!   metrics-sink dependency.
//! - [`retired_parents_shards_are_not_reaped_early`] /
//!   [`retired_parents_final_shard_expires_by_retention`] — the ADR 0050
//!   Train B rung 6 retired-tablet rule, both halves, **reachable under
//!   this fixture**: a streamed table's in-place split IS drivable here
//!   (`ClientCtx::grow_stream`, this rung's own new [`SimCluster::
//!   grow_stream`] wrapper, plus the pre-existing [`SimCluster::
//!   drive_inplace_split_cutover`] from D4 PR 2) — the cutover driver's
//!   own streams-final-seal loop (`index_drain::inplace_split_driver_
//!   tick`) runs automatically as part of every `drive_inplace_split_
//!   cutover` call, so neither scenario needs a manual pre-split seal the
//!   way the real-socket file's own `await_chain_len` setup implies (that
//!   setup relies on a periodic seal loop this fixture doesn't run at
//!   all — see [`grow_and_await_cutover`]'s own doc).
//!
//! **Stays `ProdEnv`, per this rung's own scope** (see `tests/stream_
//! janitor.rs`'s own trimmed doc for the authoritative list/reasons):
//! [`repair_re_replicates_to_a_fresh_target_after_a_replica_node_dies`]
//! (the shared `SimSegmentStore` this fixture wraps in `SegmentStoreHandle::
//! S3` has no per-node replica concept at all — `row.replicas` is always
//! empty, so phase 2's replica-repair loop, which explicitly `continue`s
//! on an empty `replicas` list, never even runs; there is no honest way to
//! model "one specific replica lost its copy" over a store every node
//! already reads the identical bucket through) and `segment_janitor_
//! reclaims_objects_from_a_genuinely_control_only_leader` (needs a genuine
//! control-only/data-only role split — `BoundControlNode`/`BoundDataNode`
//! — which `SimCluster` has no analogue of at all).

use std::time::Duration;

use animus_control::Metadata;
use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

/// A single-hash-key (`pk`, string), streamed `CreateTable` — mirrors every
/// other `sim_cluster_*` module's identically-named helper, plus a
/// `StreamSpecification`, in the same single wire call the real-socket
/// file's own fixtures always use (never a separate enable step).
fn create_streamed_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],
            "StreamSpecification":{{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

fn put_item(cluster: &mut SimCluster, node: u64, table: &str, pk: &str) -> (u16, String) {
    let body = format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}}}}}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.PutItem", body.as_bytes())
}

fn delete_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(r#"{{"TableName":"{table}"}}"#);
    cluster.dynamo(node, "DynamoDB_20120810.DeleteTable", body.as_bytes())
}

fn update_table_stream(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    enable: bool,
) -> (u16, String) {
    let body = if enable {
        format!(
            r#"{{"TableName":"{table}","StreamSpecification":
                {{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
        )
    } else {
        format!(r#"{{"TableName":"{table}","StreamSpecification":{{"StreamEnabled":false}}}}"#)
    };
    cluster.dynamo(node, "DynamoDB_20120810.UpdateTable", body.as_bytes())
}

/// `table`'s own tablet id, resolved from the replicated catalog — every
/// table in this module is created over the real wire, never via
/// `SimCluster::create_table` itself, mirroring `sim_cluster_dynamo_
/// streams.rs`'s own identically-named helper.
fn tablet_of_table(cluster: &SimCluster, table: &str) -> TabletId {
    *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table `{table}` has no tablet"))
        .0
}

fn leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let tablet = tablet_of_table(cluster, table);
    cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("tablet {} has no leader", tablet.0))
}

/// The first sealed `(tablet, epoch)` row's own key, for `table`'s tablet —
/// mirrors `tests/stream_janitor.rs`'s own identically-named helper.
fn first_sealed(meta: &Metadata, table: &str) -> (TabletId, u64) {
    let tablet = tablet_of_table_meta(meta, table);
    meta.stream_shards
        .range((tablet, 0)..=(tablet, u64::MAX))
        .next()
        .map(|(&k, _)| k)
        .unwrap_or_else(|| panic!("no sealed shard yet for `{table}`"))
}

fn tablet_of_table_meta(meta: &Metadata, table: &str) -> TabletId {
    *meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table `{table}` has no tablet"))
        .0
}

/// Poll `run_for(STEP)` — **never another op call**, so the per-call
/// [`OP_BUDGET`] burn documented in this module's own doc never distorts
/// the wait — until `done` holds or `budget` is exceeded.
fn poll_run_for(
    cluster: &mut SimCluster,
    budget: Duration,
    msg: &str,
    done: impl FnMut(&mut SimCluster) -> bool,
) {
    poll_run_for_with_step(cluster, Duration::from_millis(100), budget, msg, done);
}

/// [`poll_run_for`] with an explicit step size — used by a scenario whose
/// own wait spans many multiples of the default 100ms step (a generous,
/// never-organically-elapsing retention, deliberately chosen so an earlier
/// phase of the same scenario can't race it — see [`run_retired_parents_
/// final_shard_expires_by_retention`]'s own doc), where a coarser step
/// keeps the iteration count reasonable with no precision loss this
/// particular wait needs.
fn poll_run_for_with_step(
    cluster: &mut SimCluster,
    step: Duration,
    budget: Duration,
    msg: &str,
    mut done: impl FnMut(&mut SimCluster) -> bool,
) {
    let seed = cluster.seed();
    let mut elapsed = Duration::ZERO;
    loop {
        if done(cluster) {
            return;
        }
        assert!(
            elapsed < budget,
            "{msg} did not converge within {budget:?} (seed={seed})"
        );
        cluster.run_for(step);
        elapsed += step;
    }
}

// ---------------------------------------------------------------------------
// Two-phase retention expiry
// ---------------------------------------------------------------------------

/// The real-socket file's own happy path — see this module's own doc for
/// why this proves the empty-replicas branch (this fixture's shared `S3`
/// store), not the cataloged-replicas one.
///
/// `retention` must exceed `OP_BUDGET` (12s): this scenario asserts the
/// segment OBJECT still exists **immediately** after `drive_stream_seal`
/// returns, but that call's own `spawn_and_capture` always burns the
/// *full* `OP_BUDGET` of virtual time regardless of how quickly the seal
/// itself lands (this module's own "timing gotcha", see the module doc) —
/// with a retention shorter than `OP_BUDGET`, the janitor's 200ms tick has
/// ample opportunity to mark-and-delete the just-sealed object during that
/// same call's own leftover budget, before this function ever reads
/// `stored` back out. 20s clears that margin comfortably while still
/// converging well inside the 60s final poll below.
fn run_two_phase_expiry_removes_the_row_and_every_replicas_object(seed: u64) {
    let retention = Duration::from_secs(20);
    let mut cluster = SimCluster::new_with_segment_janitor_retention(seed, 3, 3, retention);
    let table = "t";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let leader = leader_of_table(&cluster, table);

    // Two writes, EACH sealed before the next — the janitor's own "never
    // remove a tablet's own current max epoch" guard means epoch 0 only
    // ever becomes fully reclaimable once a LATER epoch exists. Capture
    // epoch 0's own row **immediately** after its own seal, before doing
    // anything else: with `retention` this small, a further op call (each
    // burning a full `OP_BUDGET`, this module's own "timing gotcha") can
    // easily let the janitor mark AND physically delete epoch 0's own
    // object (which happens as soon as it's marked, independent of the
    // max-epoch pin — only the CATALOG ROW's removal waits on that pin;
    // `segment_janitor.rs`'s own epoch-derivation-guard doc) before this
    // scenario ever gets to read it back.
    let (status, body) = put_item(&mut cluster, 0, table, "p1");
    assert_eq!(status, 200, "seed={seed}: PutItem(p1) failed: {body}");
    cluster.drive_stream_seal(leader);

    let meta_after_seal1 = cluster.metadata(0);
    let (tablet, epoch) = first_sealed(&meta_after_seal1, table);
    assert_eq!(
        epoch, 0,
        "seed={seed}: the first-sealed shard must be epoch 0"
    );
    let row = meta_after_seal1.stream_shards[&(tablet, epoch)].clone();
    assert!(
        row.replicas.is_empty(),
        "seed={seed}: this fixture's shared S3 store has no per-node replica \
         concept — a non-empty list here would mean the fixture's own store \
         choice changed underneath this scenario: {row:?}"
    );
    let stored = cluster.segment_store().stored_ids();
    assert!(
        stored.contains(&row.object_id),
        "seed={seed}: the segment object must exist right after its own seal: {stored:?}"
    );

    let (status, body) = put_item(&mut cluster, 0, table, "p2");
    assert_eq!(status, 200, "seed={seed}: PutItem(p2) failed: {body}");
    cluster.drive_stream_seal(leader);

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    poll_run_for(
        &mut cluster,
        Duration::from_secs(60),
        "row removed from every node's catalog",
        |c| {
            nodes
                .iter()
                .all(|&n| !c.metadata(n).stream_shards.contains_key(&(tablet, epoch)))
        },
    );

    let stored = cluster.segment_store().stored_ids();
    assert!(
        !stored.contains(&row.object_id),
        "seed={seed}: the segment object must be reclaimed: {stored:?}"
    );
}

#[test]
fn two_phase_expiry_removes_the_row_and_every_replicas_object() {
    run_two_phase_expiry_removes_the_row_and_every_replicas_object(env_seed(0x5E64_0001));
}

#[test]
fn two_phase_expiry_removes_the_row_and_every_replicas_object_over_seeds() {
    for i in 0..5 {
        run_two_phase_expiry_removes_the_row_and_every_replicas_object(0x5E64_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// Leader-kill mid-sweep
// ---------------------------------------------------------------------------

/// A crash of the control-plane leader **genuinely mid-sweep** — marked
/// expired, not yet removed — converges once a new leader takes over: the
/// mark/delete/remove sequence is idempotent and re-derived fresh every
/// tick, so a new leader simply resumes it.
///
/// **Timing, worked out explicitly (see the module doc's own "timing
/// gotcha" section)**: setup here is exactly 5 op calls — `CreateTable`,
/// `PutItem`(p1), [`SimCluster::drive_stream_seal`] (epoch 0),
/// `PutItem`(p2), `drive_stream_seal` (epoch 1) — each burning exactly
/// [`OP_BUDGET`] (12s), for a deterministic 60s of cumulative virtual time
/// by the time setup finishes. Epoch 0's own `seal_wall_ms` is captured
/// somewhere inside call 3's own `[24s, 36s)` window. `retention` (70s) is
/// chosen comfortably larger than the worst-case `60 - 24 = 36s` gap this
/// arithmetic bounds, so the row is **provably not yet eligible** for
/// marking when setup finishes — only from that point does this scenario
/// switch to a manual, small-step [`SimCluster::run_for`] poll (never
/// another op call, so no more opaque 12s bursts) to advance virtual time
/// finely enough to observe the row marked, and crash the control leader
/// **immediately**, before the segment janitor's own next 200ms tick could
/// physically remove it.
fn run_expiry_survives_a_control_leader_kill_mid_sweep(seed: u64) {
    let retention = Duration::from_secs(70);
    let mut cluster = SimCluster::new_with_segment_janitor_retention(seed, 3, 3, retention);
    let table = "t";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let leader = leader_of_table(&cluster, table);

    let (status, body) = put_item(&mut cluster, 0, table, "p1");
    assert_eq!(status, 200, "seed={seed}: PutItem(p1) failed: {body}");
    cluster.drive_stream_seal(leader);
    let (status, body) = put_item(&mut cluster, 0, table, "p2");
    assert_eq!(status, 200, "seed={seed}: PutItem(p2) failed: {body}");
    cluster.drive_stream_seal(leader);

    let meta = cluster.metadata(0);
    let (tablet, epoch) = first_sealed(&meta, table);
    assert_eq!(epoch, 0, "seed={seed}: {meta:?}");
    assert!(
        !meta.stream_shards[&(tablet, epoch)].expired,
        "seed={seed}: test premise: epoch 0 must not be marked yet when setup finishes \
         (retention/setup-time arithmetic is off — see this function's own doc)"
    );

    // Manual, small-step poll from here on — no more op calls, per the
    // module doc's own "timing gotcha" discipline.
    poll_run_for(
        &mut cluster,
        Duration::from_secs(90),
        "row was never marked expired anywhere",
        |c| {
            (0..c.node_count() as u64).any(|n| {
                c.metadata(n)
                    .stream_shards
                    .get(&(tablet, epoch))
                    .is_some_and(|r| r.expired)
            })
        },
    );

    // A short extra buffer before crashing (the propose-then-crash
    // discipline — see `docs/engineering-lessons.md`) — belt-and-
    // suspenders here, since observing `expired: true` through polled
    // `Metadata` already means the mark committed to a majority (`Metadata`
    // only reflects applied, hence committed, state), unlike a bare
    // `propose_meta` call whose `Accepted` return means only "appended
    // locally."
    cluster.run_for(Duration::from_millis(200));

    let victim = cluster.control_leader_index() as u64;
    cluster.crash(victim);

    let survivors: Vec<u64> = (0..cluster.node_count() as u64)
        .filter(|&n| n != victim)
        .collect();

    // No separate "wait for a new leader" poll — the reclaim-convergence
    // poll below can only succeed once a new leader has both emerged AND
    // finished the sweep, so it's proof enough on its own.
    poll_run_for(
        &mut cluster,
        Duration::from_secs(60),
        "row was never fully removed after the leader kill",
        |c| {
            survivors
                .iter()
                .all(|&n| !c.metadata(n).stream_shards.contains_key(&(tablet, epoch)))
        },
    );
}

#[test]
fn expiry_survives_a_control_leader_kill_mid_sweep() {
    run_expiry_survives_a_control_leader_kill_mid_sweep(env_seed(0x5E64_2001));
}

#[test]
fn expiry_survives_a_control_leader_kill_mid_sweep_over_seeds() {
    for i in 0..5 {
        run_expiry_survives_a_control_leader_kill_mid_sweep(0x5E64_2100 + i);
    }
}

// ---------------------------------------------------------------------------
// Reader racing expiry
// ---------------------------------------------------------------------------

/// A `GetRecords`/`GetShardIterator` call against a shard whose retention
/// window is elapsing either serves the records or reports
/// `TrimmedDataAccessException` — never an ambiguous empty-but-`200`
/// "success" once the object is genuinely gone.
fn run_reader_never_sees_an_empty_success_gap_across_expiry(seed: u64) {
    // Large enough that retention has NOT yet elapsed by the time this
    // scenario mints + drains its "straddling" iterator (4 op calls after
    // the seal, each burning a full `OP_BUDGET` — this module's own
    // "timing gotcha") — the segment janitor deletes a marked row's
    // physical OBJECT as soon as it's marked, independent of the
    // max-epoch pin (only the catalog row's removal waits on that), so a
    // retention too small here would make the drain itself race the
    // delete and turn this scenario's own premise (a live read before
    // expiry) into a false failure.
    let retention = Duration::from_secs(45);
    let mut cluster = SimCluster::new_with_segment_janitor_retention(seed, 3, 3, retention);
    let table = "t";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap_or_else(|| panic!("seed={seed}: no stream label after CreateTable"));
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");
    let leader = leader_of_table(&cluster, table);

    // Epoch 0: sealed BEFORE epoch 1 exists — mint + drain the iterator
    // against it now, before anything can remove it (removal is blocked
    // regardless of retention until a LATER epoch exists, this module's
    // own "never remove the tablet's own current max epoch" guard).
    let (status, body) = put_item(&mut cluster, 0, table, "p1");
    assert_eq!(status, 200, "seed={seed}: PutItem(p1) failed: {body}");
    cluster.drive_stream_seal(leader);

    let tablet = tablet_of_table(&cluster, table);
    let (tablet_check, epoch) = first_sealed(&cluster.metadata(0), table);
    assert_eq!(tablet_check, tablet, "seed={seed}");
    assert_eq!(epoch, 0, "seed={seed}");
    let shard_id = animus_cp_data::segment::shard_id(tablet.0, epoch);

    let (status, resp) = cluster.dynamo_streams(
        0,
        "DynamoDBStreams_20120810.GetShardIterator",
        format!(
            r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON"}}"#
        )
        .as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: GetShardIterator failed: {resp}");
    let straddling_iter = json(&resp)["ShardIterator"]
        .as_str()
        .unwrap_or_else(|| panic!("seed={seed}: no ShardIterator: {resp}"))
        .to_owned();

    let (status, resp) = cluster.dynamo_streams(
        0,
        "DynamoDBStreams_20120810.GetRecords",
        format!(r#"{{"ShardIterator":"{straddling_iter}"}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: GetRecords failed: {resp}");
    let v = json(&resp);
    let records = v["Records"].as_array().cloned().unwrap_or_default();
    assert_eq!(records.len(), 1, "seed={seed}: {resp}");
    let drained_iter = v["NextShardIterator"].as_str().map(str::to_owned);

    // Now seal epoch 1 — unblocking epoch 0's removal.
    let (status, body) = put_item(&mut cluster, 0, table, "p2");
    assert_eq!(status, 200, "seed={seed}: PutItem(p2) failed: {body}");
    cluster.drive_stream_seal(leader);

    poll_run_for(
        &mut cluster,
        Duration::from_secs(60),
        "epoch 0's row was never fully removed",
        |c| !c.metadata(0).stream_shards.contains_key(&(tablet, epoch)),
    );

    // A fresh mint against the now-gone shard: TrimmedDataAccessException.
    let (status, body) = cluster.dynamo_streams(
        0,
        "DynamoDBStreams_20120810.GetShardIterator",
        format!(
            r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON"}}"#
        )
        .as_bytes(),
    );
    assert_eq!(status, 400, "seed={seed}: {body}");
    assert!(
        body.contains("TrimmedDataAccessException"),
        "seed={seed}: {body}"
    );

    // The pre-minted, already-drained iterator: whatever it resolves to now
    // must be the identical exception, never a bare empty `200` success —
    // in practice `drained_iter` is `None` here (a fully-drained sealed
    // shard nulls its own iterator, per every PR 3 sibling scenario's own
    // observation), so this branch mirrors the real-socket file's own
    // defensive shape without ever actually firing under this fixture.
    if let Some(iter) = drained_iter {
        let (status, resp) = cluster.dynamo_streams(
            0,
            "DynamoDBStreams_20120810.GetRecords",
            format!(r#"{{"ShardIterator":"{iter}"}}"#).as_bytes(),
        );
        assert_eq!(
            status, 400,
            "seed={seed}: a post-removal poll must never be an empty 200: {resp}"
        );
        assert!(
            resp.contains("TrimmedDataAccessException"),
            "seed={seed}: {resp}"
        );
    }
}

#[test]
fn reader_never_sees_an_empty_success_gap_across_expiry() {
    run_reader_never_sees_an_empty_success_gap_across_expiry(env_seed(0x5E64_3001));
}

#[test]
fn reader_never_sees_an_empty_success_gap_across_expiry_over_seeds() {
    for i in 0..5 {
        run_reader_never_sees_an_empty_success_gap_across_expiry(0x5E64_3100 + i);
    }
}

// ---------------------------------------------------------------------------
// Disable-grace lifecycle end to end (F12-b)
// ---------------------------------------------------------------------------

/// Write → disable (F12-b's own final seal, `force_seal_tablet`, called
/// automatically by `disable_stream` — no manual [`SimCluster::drive_
/// stream_seal`] needed) → readable through grace (`ListStreams` still
/// names it) → retention passes → the label vanishes from `ListStreams` →
/// `ResourceNotFoundException`; re-enable during grace → two labels
/// coexist → the old one drains out while the new one accumulates
/// independently.
fn run_disable_grace_lifecycle_end_to_end_with_reenable_coexistence(seed: u64) {
    let retention = Duration::from_secs(5);
    let mut cluster = SimCluster::new_with_segment_janitor_retention(seed, 1, 1, retention);
    let table = "t";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let old_label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap_or_else(|| panic!("seed={seed}: no stream label after CreateTable"));

    let (status, body) = put_item(&mut cluster, 0, table, "p1");
    assert_eq!(status, 200, "seed={seed}: PutItem(p1) failed: {body}");

    let (status, body) = update_table_stream(&mut cluster, 0, table, false);
    assert_eq!(status, 200, "seed={seed}: disable failed: {body}");
    assert!(
        cluster.metadata(0).table_stream(table).is_none(),
        "seed={seed}: disable must commit synchronously (this call's own commit-wait)"
    );

    let (status, resp) = cluster.dynamo_streams(0, "DynamoDBStreams_20120810.ListStreams", b"{}");
    assert_eq!(status, 200, "seed={seed}: {resp}");
    assert!(resp.contains(&old_label), "seed={seed}: {resp}");

    let (status, body) = update_table_stream(&mut cluster, 0, table, true);
    assert_eq!(status, 200, "seed={seed}: re-enable failed: {body}");
    let new_label = cluster
        .metadata(0)
        .table_stream(table)
        .map(|s| s.label.clone())
        .unwrap_or_else(|| panic!("seed={seed}: no stream label after re-enable"));
    assert_ne!(old_label, new_label, "seed={seed}");

    let (status, body) = put_item(&mut cluster, 0, table, "p2");
    assert_eq!(status, 200, "seed={seed}: PutItem(p2) failed: {body}");

    let (status, resp) = cluster.dynamo_streams(0, "DynamoDBStreams_20120810.ListStreams", b"{}");
    assert_eq!(status, 200, "seed={seed}: {resp}");
    assert!(
        resp.contains(&old_label) && resp.contains(&new_label),
        "seed={seed}: both labels must coexist during the grace window: {resp}"
    );

    // The OLD label's own sealed epoch is still the tablet's own current
    // MAX epoch (nothing has sealed since) — its ROW removal (unlike its
    // OBJECT deletion, which happens as soon as it's marked regardless)
    // stays blocked by the max-epoch pin until something seals PAST it.
    // The real-socket file's own equivalent gets this from its periodic
    // seal loop (`tiny_seal_knobs`) sealing the new label's own p2 within
    // a fraction of a second; this fixture runs no such loop at all, so a
    // manual seal here is load-bearing, not optional.
    cluster.drive_stream_seal(0);

    // The OLD label's row is now fully reclaimable (marked, object already
    // gone, and now unblocked from removal by the seal above) — the very
    // next janitor tick removes it. The NEW label's own just-sealed row is
    // still comfortably younger than `retention`, so it survives this
    // short poll untouched.
    poll_run_for(
        &mut cluster,
        Duration::from_secs(60),
        "the old disabled label's rows never fully drained",
        |c| {
            !c.metadata(0)
                .stream_labels_with_rows(table)
                .contains(&old_label)
        },
    );

    let (status, resp) = cluster.dynamo_streams(0, "DynamoDBStreams_20120810.ListStreams", b"{}");
    assert_eq!(status, 200, "seed={seed}: {resp}");
    assert!(
        !resp.contains(&old_label),
        "seed={seed}: the old label must vanish from ListStreams once fully drained: {resp}"
    );
    assert!(
        resp.contains(&new_label),
        "seed={seed}: the new (current, enabled) label must still be listed: {resp}"
    );

    let old_arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{old_label}");
    let (status, body) = cluster.dynamo_streams(
        0,
        "DynamoDBStreams_20120810.DescribeStream",
        format!(r#"{{"StreamArn":"{old_arn}"}}"#).as_bytes(),
    );
    assert_eq!(status, 400, "seed={seed}: {body}");
    assert!(
        body.contains("ResourceNotFoundException"),
        "seed={seed}: {body}"
    );
}

#[test]
fn disable_grace_lifecycle_end_to_end_with_reenable_coexistence() {
    run_disable_grace_lifecycle_end_to_end_with_reenable_coexistence(env_seed(0x5E64_4001));
}

#[test]
fn disable_grace_lifecycle_end_to_end_with_reenable_coexistence_over_seeds() {
    for i in 0..5 {
        run_disable_grace_lifecycle_end_to_end_with_reenable_coexistence(0x5E64_4100 + i);
    }
}

// ---------------------------------------------------------------------------
// Drop-table cascade (the convergent design)
// ---------------------------------------------------------------------------

/// Dropping a table with live catalog rows converges its rows/objects to
/// zero — with no dedicated cascade code path in `drop_table` itself: this
/// is purely the janitor's ordinary retention-zero rule reacting to the
/// schema's own disappearance. Uses this fixture's own **default**
/// (generous, 3600s) retention — see [`SimCluster::new`]/[`DEFAULT_SIM_
/// SEGMENT_JANITOR_RETENTION`] — specifically so a pass can't be mistaken
/// for "retention happened to elapse" rather than the drop-table rule
/// actually firing.
fn run_drop_table_cascade_converges_via_the_janitor(seed: u64) {
    let mut cluster = SimCluster::new(seed, 1, 1);
    let table = "t";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = put_item(&mut cluster, 0, table, "p1");
    assert_eq!(status, 200, "seed={seed}: PutItem(p1) failed: {body}");
    cluster.drive_stream_seal(0);
    assert!(
        !cluster.metadata(0).stream_shards.is_empty(),
        "seed={seed}: test premise: at least one live catalog row exists"
    );

    let (status, body) = delete_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: DeleteTable failed: {body}");

    poll_run_for(
        &mut cluster,
        Duration::from_secs(60),
        "stream-shard catalog rows were never cleared after a table drop",
        |c| c.metadata(0).stream_shards.is_empty(),
    );
}

#[test]
fn drop_table_cascade_converges_via_the_janitor() {
    run_drop_table_cascade_converges_via_the_janitor(env_seed(0x5E64_5001));
}

#[test]
fn drop_table_cascade_converges_via_the_janitor_over_seeds() {
    for i in 0..5 {
        run_drop_table_cascade_converges_via_the_janitor(0x5E64_5100 + i);
    }
}

/// The mid-grace variant: drop a table while an old, disabled-but-draining
/// label and a new, currently-enabled label both have live catalog rows —
/// both converge to zero via the same janitor sweep. Needs one explicit
/// [`SimCluster::drive_stream_seal`] after re-enabling and writing under
/// the new label — the real-socket file's own equivalent relies on a
/// periodic seal loop (`tiny_seal_knobs`) this fixture never runs, to seal
/// the new label's own first row and genuinely produce two coexisting,
/// row-bearing labels rather than just one.
fn run_mid_grace_drop_removes_both_coexisting_labels(seed: u64) {
    let mut cluster = SimCluster::new(seed, 1, 1);
    let table = "t";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = put_item(&mut cluster, 0, table, "p1");
    assert_eq!(status, 200, "seed={seed}: PutItem(p1) failed: {body}");

    let (status, body) = update_table_stream(&mut cluster, 0, table, false);
    assert_eq!(status, 200, "seed={seed}: disable failed: {body}");
    assert!(
        cluster.metadata(0).table_stream(table).is_none(),
        "seed={seed}: disable must commit synchronously"
    );

    let (status, body) = update_table_stream(&mut cluster, 0, table, true);
    assert_eq!(status, 200, "seed={seed}: re-enable failed: {body}");
    let (status, body) = put_item(&mut cluster, 0, table, "p2");
    assert_eq!(status, 200, "seed={seed}: PutItem(p2) failed: {body}");
    cluster.drive_stream_seal(0);

    assert!(
        cluster.metadata(0).stream_labels_with_rows(table).len() >= 2,
        "seed={seed}: test premise: two coexisting labels must both carry live rows: {:?}",
        cluster.metadata(0).stream_labels_with_rows(table)
    );

    let (status, body) = delete_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: DeleteTable failed: {body}");

    poll_run_for(
        &mut cluster,
        Duration::from_secs(60),
        "both coexisting labels' rows were never cleared after the drop",
        |c| c.metadata(0).stream_shards.is_empty(),
    );
}

#[test]
fn mid_grace_drop_removes_both_coexisting_labels() {
    run_mid_grace_drop_removes_both_coexisting_labels(env_seed(0x5E64_6001));
}

#[test]
fn mid_grace_drop_removes_both_coexisting_labels_over_seeds() {
    for i in 0..5 {
        run_mid_grace_drop_removes_both_coexisting_labels(0x5E64_6100 + i);
    }
}

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------

/// A completed retention cycle leaves [`SimCluster::segment_janitor_
/// progress`] reflecting it — the "observability lands with the mechanism"
/// house rule, asserted on the progress struct rather than the real-socket
/// file's own `/metrics` counter (see this module's own doc for why that
/// counter is genuinely unreachable here, not merely untested).
fn run_metrics_reflect_a_completed_retention_cycle(seed: u64) {
    let retention = Duration::from_secs(5);
    let mut cluster = SimCluster::new_with_segment_janitor_retention(seed, 1, 1, retention);
    let table = "t";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let (status, body) = put_item(&mut cluster, 0, table, "p1");
    assert_eq!(status, 200, "seed={seed}: PutItem(p1) failed: {body}");
    cluster.drive_stream_seal(0);

    // Capture epoch 0 right after its own seal (this module's own "timing
    // gotcha" — see `two_phase_expiry`'s identical fix) — a small
    // `retention` can let the object be deleted mid the SECOND
    // `drive_stream_seal`'s own opaque `OP_BUDGET` window otherwise.
    let (tablet, epoch) = first_sealed(&cluster.metadata(0), table);
    assert_eq!(epoch, 0, "seed={seed}");

    let (status, body) = put_item(&mut cluster, 0, table, "p2");
    assert_eq!(status, 200, "seed={seed}: PutItem(p2) failed: {body}");
    cluster.drive_stream_seal(0);

    poll_run_for(
        &mut cluster,
        Duration::from_secs(60),
        "epoch 0's row was never fully removed",
        |c| !c.metadata(0).stream_shards.contains_key(&(tablet, epoch)),
    );

    let progress = cluster.segment_janitor_progress(0);
    assert!(
        progress.orphans_deleted_total >= 1,
        "seed={seed}: the leader's own progress must record the reclaim: {progress:?}"
    );
    assert_eq!(progress.last_error, None, "seed={seed}: {progress:?}");
}

#[test]
fn metrics_reflect_a_completed_retention_cycle() {
    run_metrics_reflect_a_completed_retention_cycle(env_seed(0x5E64_7001));
}

#[test]
fn metrics_reflect_a_completed_retention_cycle_over_seeds() {
    for i in 0..5 {
        run_metrics_reflect_a_completed_retention_cycle(0x5E64_7100 + i);
    }
}

// ---------------------------------------------------------------------------
// ADR 0050 Train B rung 6: the retired-tablet rule
// ---------------------------------------------------------------------------

/// Splits `table`'s sole tablet via [`SimCluster::grow_stream`] and drives
/// the fork/cutover to convergence (root retired, exactly two `Active`
/// children) — this fixture's own analogue of the real-socket file's
/// `grow_and_await_cutover` (which drives `/admin/stream/grow`, an HTTP
/// surface this fixture has no listener for). Returns the retired root's
/// own former tablet id. **No manual pre-split seal is needed** (unlike
/// the real-socket file's own `await_chain_len` setup, which relies on a
/// periodic seal loop this fixture never runs) — the cutover driver's own
/// streams-final-seal loop (`index_drain::inplace_split_driver_tick`,
/// [`SimCluster::drive_inplace_split_cutover`]'s own primitive) runs
/// automatically as part of every poll iteration below, sealing whatever
/// is pending as the root's own (single) final epoch.
fn grow_and_await_cutover(cluster: &mut SimCluster, table: &str) -> TabletId {
    let root = tablet_of_table(cluster, table);
    let results = cluster.grow_stream(0, table);
    assert_eq!(results.len(), 1, "seed={}: {results:?}", cluster.seed());
    assert!(
        matches!(results[0].1, crate::ClientResponse::PutOk),
        "seed={}: grow_stream must genuinely split the sole tablet: {:?}",
        cluster.seed(),
        results[0]
    );

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    const STEP: Duration = Duration::from_millis(100);
    let budget = Duration::from_secs(120);
    let seed = cluster.seed();
    let mut elapsed = Duration::ZERO;
    loop {
        for &n in &nodes {
            cluster.drive_inplace_split_cutover(n);
        }
        let converged = nodes.iter().all(|&n| {
            let meta = cluster.metadata(n);
            !meta.tablets.contains_key(&root)
                && meta
                    .tablets
                    .iter()
                    .filter(|(_, t)| t.serves_table(table))
                    .count()
                    == 2
        });
        if converged {
            return root;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: cutover of {table} did not converge within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// The mark half of the retired-tablet rule: a retired split parent's
/// sealed shards expire by ORDINARY retention — never the drop-table
/// retention-zero rule, which keys on the TABLE's schema (still live via
/// the children), not on tablet presence. This fixture's own default
/// (generous, 3600s) retention makes this easy to prove: cutover
/// convergence itself (every poll iteration burns a full [`OP_BUDGET`]
/// per node via [`SimCluster::drive_inplace_split_cutover`]) consumes at
/// most a few hundred seconds of virtual time — comfortably under 3600s —
/// so the retired root's rows must still be present and unexpired right
/// after convergence.
fn run_retired_parents_shards_are_not_reaped_early(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "rt";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    for i in 0..8 {
        let (status, body) = put_item(&mut cluster, 0, table, &format!("r{i:04}"));
        assert_eq!(status, 200, "seed={seed}: PutItem(r{i:04}) failed: {body}");
    }

    let root = grow_and_await_cutover(&mut cluster, table);

    let meta = cluster.metadata(0);
    let rows: Vec<_> = meta
        .stream_shards
        .range((root, 0)..=(root, u64::MAX))
        .collect();
    assert!(
        !rows.is_empty(),
        "seed={seed}: the retired root must still have its sealed shards cataloged"
    );
    for ((_, epoch), row) in rows {
        assert!(
            !row.expired,
            "seed={seed}: a retired parent's shard (epoch {epoch}) must expire by \
             ordinary retention, never be reaped early as dropped-table work"
        );
    }
}

#[test]
fn retired_parents_shards_are_not_reaped_early() {
    run_retired_parents_shards_are_not_reaped_early(env_seed(0x5E64_8001));
}

#[test]
fn retired_parents_shards_are_not_reaped_early_over_seeds() {
    for i in 0..5 {
        run_retired_parents_shards_are_not_reaped_early(0x5E64_8100 + i);
    }
}

/// The removal half: past retention, a retired parent's rows are removed
/// **including its final (max-epoch) shard** — the max-epoch pin exists
/// only for a LIVE tablet; a retired tablet can never seal again, so
/// nothing pins its final row. A modest (30s) retention: cutover
/// convergence's own `drive_inplace_split_cutover` polling already burns
/// many multiples of that in virtual time, so no additional manual
/// timing is needed beyond an ordinary converged-or-timeout poll for the
/// removal itself.
fn run_retired_parents_final_shard_expires_by_retention(seed: u64) {
    // `grow_and_await_cutover`'s own convergence polling calls
    // `SimCluster::drive_inplace_split_cutover` on every node every
    // iteration, each burning a full `OP_BUDGET` (12s) per node —
    // confirmed empirically (not guessed) to converge in exactly ONE such
    // iteration for this scenario's own 8-item/3-node shape, across six
    // different seeds (`DEBUG cutover converged ... iters=1
    // approx_virtual_secs=36.1`, checked directly while diagnosing this
    // constant's own sizing) — so the retired root's own final seal lands
    // somewhere in `[0s, 36.1s)` of virtual time, worst case right at the
    // THIRD node's own call window. `retention` (50s) is chosen
    // comfortably larger than that worst case specifically so `final_row`
    // (read via a plain synchronous `cluster.metadata(0)` call, no
    // further virtual time elapsing, immediately after `grow_and_await_
    // cutover` returns) is ALWAYS still present to capture — a smaller
    // retention risked the final shard being marked, object-deleted, AND
    // fully removed (the retired-tablet rule needs no max-epoch pin to
    // clear at all — see this module's own `grow_and_await_cutover` doc)
    // somewhere INSIDE that same opaque convergence loop, before this
    // scenario ever got to read it back: confirmed live, a `_over_seeds`
    // run at 30s retention hit exactly this, `unwrap_or_else` firing
    // because `stream_shards` was already fully empty by the time cutover
    // itself converged.
    let retention = Duration::from_secs(50);
    let mut cluster = SimCluster::new_with_segment_janitor_retention(seed, 3, 3, retention);
    let table = "re";

    let (status, body) = create_streamed_table(&mut cluster, 0, table);
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    for i in 0..8 {
        let (status, body) = put_item(&mut cluster, 0, table, &format!("e{i:04}"));
        assert_eq!(status, 200, "seed={seed}: PutItem(e{i:04}) failed: {body}");
    }

    let root = grow_and_await_cutover(&mut cluster, table);

    let meta = cluster.metadata(0);
    let final_row = meta
        .stream_shards
        .range((root, 0)..=(root, u64::MAX))
        .next_back()
        .map(|(_, row)| row.clone())
        .unwrap_or_else(|| panic!("seed={seed}: the retired root sealed at least one shard"));

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    poll_run_for_with_step(
        &mut cluster,
        Duration::from_secs(5),
        Duration::from_secs(150),
        "the retired root's rows (final epoch included) were never removed",
        |c| {
            nodes.iter().all(|&n| {
                c.metadata(n)
                    .stream_shards
                    .range((root, 0)..=(root, u64::MAX))
                    .next()
                    .is_none()
            })
        },
    );

    let stored = cluster.segment_store().stored_ids();
    assert!(
        !stored.contains(&final_row.object_id),
        "seed={seed}: the final shard's own object must be reclaimed too: {stored:?}"
    );
}

#[test]
fn retired_parents_final_shard_expires_by_retention() {
    run_retired_parents_final_shard_expires_by_retention(env_seed(0x5E64_9001));
}

#[test]
fn retired_parents_final_shard_expires_by_retention_over_seeds() {
    for i in 0..5 {
        run_retired_parents_final_shard_expires_by_retention(0x5E64_9100 + i);
    }
}
