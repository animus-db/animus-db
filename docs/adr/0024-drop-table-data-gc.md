# ADR 0024 — Drop-table data GC

- **Status:** Accepted
- **Date:** 2026-08-06
- **Builds on:** [ADR 0023](0023-table-scoped-tablets.md) (table-scoped tablets),
  [ADR 0017](0017-per-tablet-raft-data-plane.md) (per-tablet Raft groups),
  [ADR 0013](0013-replicated-schemas.md) (the schema catalog)

## Context

Before this ADR, `DROP TABLE` (CQL and the admin dashboard's
`/admin/data/drop-table`) removed only the table's **schema** from the replicated
catalog. Everything else leaked forever: the table's tablets stayed in the
replicated tablet map, their per-tablet Raft groups kept running (electing,
heartbeating, holding their engines open), and their on-disk artifacts — the LSM
`db-`/`db-t{id}-` files and each group's `raftkv.wal` — remained on every replica.
ADR 0023 listed "drop-table teardown" as the unfinished rollout item; ADR 0023's
table-scoped tablets are also what make teardown *tractable*: a table's data is
exactly the engines of `tablets_for_table(table)`, never interleaved with other
tables' rows.

## Decision

Dropping a table garbage-collects its data in two halves, following the repo's
established *decision-in-replicated-state, timing-in-a-per-node-loop* shape:

- **The metadata half — `MetaCommand::DropTableTablets { table }`**
  (`animus-control`): removes **every** tablet scoped to `table` from
  `Metadata.tablets`, along with its placement policy (mirroring the
  `MergeTablets` cleanup), in **one apply** so no replica ever observes a table
  half-dropped. Idempotent (`NoOp` when the table has no tablets). The real drop
  sink, `ClientCtx::drop_table`, proposes `DropTableSchema` then
  `DropTableTablets` and waits for both to leave replicated metadata.
  `drop_table_schema` stays the schema-only primitive because CQL
  `ALTER TABLE` uses it for drop-then-recreate — an ALTER must never GC data.
  The command is in the cross-process relay allowlist (`is_relayable_command`),
  so a drop issued on a follower-connected node commits on the control leader.

- **The local half — the per-node `cp_gc_loop`** (`animusd`): the exact dual of
  `cp_join_host_loop`. Each node polls replicated `Metadata.tablets`; for any
  tablet in its per-node `minted` claim set that is **absent from the map**, it
  reclaims everything local, in a crash-convergent order:
  1. **Unregister** its own handle from the edge registry
     (`unregister_raftkv(tablet, member)`, matched by the handle env's member id
     — in `--cluster N` the edge is shared, so a node must remove *its* handle,
     not whichever is first). No handle registered means the stand-up path is
     mid-flight — skip and retry a later tick, never GC a group mid-standup.
  2. **Stop the group**: `RaftKvNode::shutdown()` (new) asks the driver loop to
     exit between full persist+apply passes; the GC waits on `is_stopped()`
     before touching any file, and re-registers + retries on timeout.
  3. **Delete the artifacts** through the group env's `Disk` seam: every file
     with the engine prefix (`db-` for the first/bootstrap tablet on the main
     raftkv env, `db-t{id}-` for a sibling-hosted tablet) plus the group's
     `raftkv.wal*`, enumerated via the new `Disk::list`. A sibling env then gets
     `ProdEnv::shutdown_tasks()` (aborts its accept loop + tasks **without**
     draining the shared sibling listener pool — a full `shutdown` would kill
     the unclaimed slots future splits need).
  4. **Prune the durable `cp-hosted` marker** (so a restart no longer re-hosts)
     and release the `minted` claim last.

### New primitives this rides on

- `Disk::list() -> Vec<String>` on the `Env` seam (sim: the node's BTreeMap key
  band; prod: a non-recursive `read_dir` of the env's own dir). Deletion goes
  through the seam, so teardown is sim-testable like the rest of the system.
- `RaftKvNode::shutdown()/is_halted()/is_stopped()`: a graceful, deterministic
  driver halt (an `AtomicBool` observed at the loop top, `stopped` latched on
  exit). The reconfigure loop exits with it.
- `ProdEnv::shutdown_tasks()`: per-sibling task teardown that leaves the shared
  pool intact.

## Why absence-from-the-map is a sound trigger

A node mints (claims) a tablet only after **applying** its `CreateTablet`, and
`metadata()` exposes only durable applied state — so a recovered control replica
always contains every tablet its node legitimately hosts. Absence therefore means
a committed drop. The `last_applied() == 0` guard skips the pre-recovery window
where default (empty) metadata would read as "everything dropped".

**Drop + GC are convergent, not one-shot.** Two benign transients resolve on
later ticks / the next restart:

- A replica that was down during the drop restarts, re-hosts the tablet from its
  marker/engine, then its GC loop reclaims it once its control replica catches up
  past the drop.
- A restarted control replica **re-applies its log from the start**, passing
  through historical map states in which the dropped tablet still exists; the
  join-host loop may briefly re-host an *empty* group for it (its files are
  already gone, and routing consults the current map, so it serves nothing).
  Replay reaches the drop; the GC loop reclaims the zombie. Tablet ids are never
  reused (`next_tablet_id` is monotonic), so a late reclaim can never collide
  with a re-created table — a new same-named table gets a fresh tablet id.

## Consequences / limits

- **What is reclaimed:** the tablet map + policy entries, the running groups, the
  engine + Raft WAL files on every replica, the `cp-hosted` marker entries and
  `minted` claims, the edge registry handles.
- **What intentionally leaks (small, bounded):** the dropped tablet's members in
  `Metadata.cp_member_addrs` (a few strings; harmless — nothing routes to a
  tablet that is not in the map), a GC'd sibling's claimed pool slot (the pool is
  per-process and sized generously; slots are not returnable by design), and the
  empty `sib-<id>/` directory (the `Disk` seam deletes files, not dirs).
- Rows of a schema-less table written before ADR 0023 into the legacy unscoped
  tablet (`table: None`) are not GC'd — that tablet serves every table and is
  never scoped to one. Table-scoped tablets are the norm since ADR 0023.
- DynamoDB still has no `DeleteTable` wire operation; the dashboard's
  `/admin/data/drop-table` and CQL `DROP TABLE` are the drop entry points, and
  both now GC.

## Tests

- `animus-control` `meta.rs`: `DropTableTablets` removes the table's tablets
  (split children included) + policies in one apply, leaves other tables and the
  legacy tablet alone, no-ops on re-apply.
- `animus-cp-data` `tests/shutdown.rs` (SimEnv, deterministic): a halted follower
  stops applying while the live majority proceeds; a halted leader's survivors
  re-elect and keep serving.
- `animus-sim` / `animus-env`: `Disk::list` is per-node, sorted, non-recursive,
  reflects `remove`.
- `animusd` `tests/drop_table_gc.rs` (ProdEnv, end-to-end): single node — write,
  split, drop; tablets leave the map, both groups' files (main env + sibling) are
  deleted, the marker is pruned, the admin view empties, a restart resurrects
  nothing (waiting out the replay transient above), and a fresh table still
  serves. Three nodes per-process — **every replica** reclaims its own files off
  the replicated drop, no cross-node teardown message.
