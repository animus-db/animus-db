# ADR 0077 — Force-new-configuration recovery after permanent loss of a voter majority

- **Status:** Accepted (2026-10-10) — phase 1 (control plane) implemented; phase 2 (tablet groups) design only
- **Date:** 2026-10-10
- **Origin:** issue #1178 and `docs/production-readiness.md` E-2 ("whether an unsafe-recovery tool is needed"). `docs/runbook/control-plane-quorum-loss.md` said there was none and the only answer to losing two of three control voters was to rebuild.
- **Amends:** none. **Depends on:** ADR 0003 (determinism), ADR 0009 (control-plane Raft, the 2026-09-15 wiped-voter / cluster-check amendments), ADR 0037 (runtime membership change), ADR 0038 (`Metadata` apply task and its system-keyspace mirror), ADR 0059 (backup catalog lives in `Metadata`), ADR 0073 (upgrade compatibility).

## Context

A Raft group whose voter majority is permanently gone cannot elect a leader or commit a membership change, so no in-band operation (`control-remove --force` included, which only skips a reachability guard) can help. The state is still on the survivors' disks. Every comparable system ships an explicit, loud, offline escape hatch for this (etcd `--force-new-cluster`, TiKV `unsafe-recover`, CockroachDB `debug recover`). Without one, beta has no answer to "two of three control voters died" other than rebuild-from-export.

## Decision

### 1. Offline only, one survivor, explicit acknowledgement

`animusd recover-control --config FILE --node I [--dir DIR] [--encryption-key PATH] [--acknowledge-data-loss]` runs against the data directory of **one surviving control voter** while its process is stopped.

- Without `--acknowledge-data-loss` it prints the plan (recorded voters, log extent, term change, what is kept, what may be lost) and writes nothing.
- It refuses if a node appears to be running on the directory. `animusd` has no data-directory lock, so the guard is the node's own internal listen address: the tool binds it for the whole run (a running node already owns it; a node started meanwhile cannot bind). A node configured with a different address on the same directory is not detected; the runbook says to stop the process first.
- It refuses a node that is not a voter in its own recovered configuration, a data-only node, a missing or empty WAL (nothing is created or modified), and a WAL that does not decode.
- It is never run automatically and there is no admin-API form: the thing it does can discard acknowledged writes.

### 2. The rewrite reuses existing record shapes

`animus_control::recover::{plan, apply}` (generic over `RaftCore<C, S>`, so tablet groups can reuse it) back up the WAL to `raft.wal.pre-force-new-config.<term>`, then append two ordinary records: a `WalRecord::Hard` raising the term by `RECOVERY_TERM_JUMP` (2^16) over the highest term the survivor knows, and a `WalRecord::Append` of a no-op entry that **carries the configuration `{survivor}`** (no learners) at that term. This is exactly what `change_membership` appends, so `RaftCore::recovered` computes `config = {survivor}` from it unmodified. On restart the survivor wins a one-vote election above that term and commits its entire log.

The apply is idempotent (a WAL already naming the node as sole voter is a no-op) and an interrupted apply leaves the original intact (the backup is written first; then both records go in one append followed by one fsync, so a crash leaves either the original or a torn tail the normal recovery cuts back).

### 3. What is preserved, what is lost

- **Preserved:** every entry in the survivor's durable log, **whether or not it knew the entry was committed**. Entries it had appended but never saw committed are treated as committed from now on. Its system-keyspace `Metadata` mirror is untouched; it stays consistent because the rewrite only appends past the log and the mirror's `_applied_index` watermark (ADR 0038) can only cover entries already in the log.
- **Lost:** any write the old group acknowledged to a client that this survivor never received (it was acknowledged by a majority that did not include it). The tool cannot tell which writes those are; the plan reports how many tail entries have unknown commit status. **Choose the survivor with the highest log** (compare `/admin/raft` `commit_index`, or the plan's "last index", on each candidate before running anything).

### 4. Fencing stale voters, and why they must be wiped first

The other old voters must be **wiped** (data directory emptied) before they run again, then re-added through the supported path (`control-add`: learner, catch-up, promote). A voter restarted on its old disk keeps state the recovered group does not have and must never be re-admitted into it.

If one comes back un-wiped anyway, the new configuration entry fences it on contact: its term is above anything the old group can have reached, so the stale voter adopts it and steps down, and a campaign is answered by the leader's explicit `RaftMsg::Removed` notice (issue #1061), leaving it a non-voter. This is best effort. Two or more stale voters that cannot reach the survivor can still elect each other in the old configuration, which is why the procedure orders "wipe, then start". A wiped voter resolves the ADR 0009 cluster check as fresh and rejoins as a learner. The recovered survivor needs no cluster check: its WAL is non-empty, so it takes the recovered path, never the genesis-vs-wiped-restart one.

### 5. `Metadata` consequences

`Metadata` keeps the old `members` and node records for the dead voters, and the tablet replica sets still name them. After recovery the failure detector marks the missing nodes down and the usual repair/placement machinery reacts (re-replicating tablets whose data nodes are gone; data nodes that are still alive keep serving). The operator should decommission nodes that are never coming back (ADR 0032). The backup catalog (ADR 0059), schema catalog, credentials and tablet map survive in the survivor's `Metadata` as of its log; backups taken in the lost tail are not in the catalog (their objects remain in the store).

### 6. ADR 0073 classification

No new durable format and no cross-node variant: the rewrite appends existing `Hard` and `Append` records, and a config-bearing entry is an existing shape. So there is no version tag, golden fixture or `Gate` to add. (The accessor `RaftNode::removed_by_leader` is test/diagnostic surface only.) The backup file is a copy of an existing-format WAL.

### 7. Phase 2: tablet groups (design only, not built)

A tablet group that lost its majority can use the same `plan`/`apply` on a replica's per-tablet WAL, but the surrounding steps differ and are deferred to a follow-up issue: the tool must address a tablet's own engine/WAL (shared WAL, ADR 0028), the control plane's replica set for the tablet must be corrected afterwards (`CasTabletReplicas`) and linearizability across a tablet's lost tail must be stated per table. Until then the runbook's answer for a tablet is restore from backup/PITR.

## Consequences

- The runbook gains an actual procedure; E-2 moves to Met for the control plane.
- Data loss is possible and explicit, never silent.
- Proof: `animus-control` `tests/it/force_new_configuration.rs` (seeded `SimEnv`, `ANIMUS_FORCE_RECOVER_SEEDS`, replay with `ANIMUS_SEED`): 3 and 5 voters, a compacted WAL, a lagging survivor (acked writes lost), an uncommitted tail (kept), a stale voter fenced, a wiped voter rejoining, an interrupted apply, and a negative control showing a lone survivor cannot serve without the tool. `animusd` `control_recover` unit tests cover the CLI-side refusals.
