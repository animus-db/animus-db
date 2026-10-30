# ADR 0077 — Force-new-configuration recovery after permanent loss of a voter majority

- **Status:** Accepted (2026-10-10) — phase 1 (control plane) implemented; phase 2 (tablet groups) design accepted 2026-10-30 (amendment), not built
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

## Amendment (2026-10-30): phase 2 design, recovering a tablet group that lost its majority (issue #1273)

**Status:** design accepted by the maintainer, **not built**. This replaces the sketch in section 7. Implementation follows in three PRs (below). The recovery stays a data-loss operation and keeps the `DataLossAcknowledged` token and the explicit acknowledgement flag of section 1.

Facts checked against the code while writing this (so the design does not rest on prose that lags):

- `recover::plan` and `recover::apply` take an `Env` and a **file name**, read it with `env.read` and decode it with the *untagged* `PersistedState::decode_with_extent` / `PersistedState::recover`. They are generic over `C, S` already, but know nothing of a tablet id or of the `SWL1`-tagged framing.
- The shared WAL is `animus_control::shared_wal::SharedWal<C, S>` (the file is `animus_cp_data::SHARED_WAL` = `raftkv.wal.shared`), **not** in `animus-cp-data`. `SharedWal::open` seeds a per-tablet `group_tails` cache from the file; `recovered_state(tablet)` replays one tablet's records from that cache; `append_tagged(env, file, tablet, &[WalRecord])` appends tagged records and updates the cache in the same critical section. The per-group file (`wal_file(stream)`) is the other layout; `DEFAULT_SHARED_WAL` is `true`, so the shared file is the normal case.
- `MetaCommand::CasTabletReplicas { tablet, expected_epoch, replicas }` applies only if the tablet's epoch equals `expected_epoch`, then bumps it. It is committed by the **control** group, so a tablet that cannot elect a leader does not block it.
- The host reconciler's per-group stop is `Reconciler::teardown(tablet, release)`. It unregisters the group, calls `node.shutdown()`, waits up to `RECLAIM_STOP_GRACE` for `is_stopped()`, closes the stream, and then `finish_teardown` **erases the engine files and the WAL**. Only the first half is reusable here; the erase is exactly what recovery must not do.

### 8. Online, one tablet group at a time (decision 1)

The offline-only rule of section 1 is relaxed for tablet groups. A node that holds many groups cannot be stopped to fix one of them (the other groups on it are healthy and some of them may be the survivors' only quorum for other tablets). The survivor's host:

1. **Stops only the target group** using the stop half of `teardown`: unregister, `shutdown()`, wait for `is_stopped()`, `close_stream`. It does **not** run `finish_teardown`'s erase or `confirm_torn_down`, and it **suppresses re-hosting** of that tablet for the duration (a new per-tablet "recovering" mark in the reconciler's local state, so `plan` does not emit `Host` while the rewrite runs). This is a new reconciler hook, not an existing path; it is the main code addition of the host side.
2. Rewrites the group's WAL (section 9).
3. Clears the mark so the next `plan` re-hosts the group, which replays the rewritten WAL like any restart.

Healthy tablets on the node keep serving throughout. If the stop does not complete within a bound (the existing `stopping` parking applies), the recovery fails with nothing written.

Consequence: the rewrite must go through the **live node's own `SharedWal` handle** (`append_tagged`), because that object caches every tablet's tail and a second writer on the file would desynchronise the cache from the disk. So the tablet form is an **admin action on the running node**, not a second process on the directory; the section 1 statement "there is no admin-API form" is amended to: *there is still no unauthenticated or automatic form; the tablet form is a gated admin action (same gating as other destructive admin actions), requires the acknowledgement parameter, and mints `DataLossAcknowledged` only from it.* Whether the control-plane form of section 1 should also gain an online variant is **open** and not part of this work.

### 9. Order: rewrite the survivor first, then correct the control plane (decision 2)

1. **WAL first.** Back up, then append the term-bumping `Hard` and the config-bearing no-op `Append` naming the survivor as sole voter (sections 2-3, unchanged), then restart the group.
2. **Then** commit `CasTabletReplicas { tablet, expected_epoch, replicas: [survivor] }` on the control plane, which keeps its own majority. The CLI/admin action reads the tablet's current epoch from `Metadata` immediately before, and reports (does not retry blindly) an epoch mismatch.

The reverse order is refused by design: with the control plane already saying `{survivor}`, the reconciler would try a membership change on a group whose WAL still names three voters and that cannot commit, and it would also treat the two dead replicas as removed while the survivor could still be re-hosted with the old config. With the WAL first, the survivor is a one-voter group by the time the control plane agrees, so the usual machinery (`reconfigure_step`, `replan_repair`) then grows it back to the policy RF by adding learners and promoting them, exactly as for any under-replicated tablet. Until the second step commits, the survivor serves as a one-voter group while `Metadata` still lists dead replicas: that window is harmless (the failure detector already marks them down) and is the state an interrupted run is left in. Re-running is safe: the WAL step is idempotent (`already_single_voter`), and the CAS is re-derived from the current epoch.

Stale replicas of the old group are fenced by the term jump as in section 4, and the two lost replicas' data must be wiped before they return; the reconciler's release path erases a replica that `Metadata` no longer lists, which covers it once the CAS lands.

### 10. Shared WAL: back up the whole file, then tagged-append (decision 3)

Before the tagged append, copy the **entire** shared WAL file to `<file>.pre-force-new-config.<new_term>` (the existing backup naming, which `apply` already produces by copying whatever `file` it is given). It is not an extract of the target tablet's records: a whole-file copy is simple and complete, and an extract would be a new file shape that would itself need trusting and a reader. The cost is a copy of one file's size per recovery, and the backup holds other tablets' records too (the runbook says so and says to delete it once the group is healthy). The copy is taken through the live `SharedWal` handle's lock so it is a consistent point (no round half-written).

### 11. A WAL sink abstraction in `recover` (decision 4)

`recover::{plan, apply}` currently read and append a whole untagged file. They gain a small **sink** trait (PR 1) with exactly the operations the rewrite needs: read the target group's recovered records (and the valid extent), copy the backup, append the two records durably. Two implementations:

- **plain file** (the control WAL and a per-group tablet WAL): today's behaviour, behaviour-preserving, with the control-plane tests as the regression net;
- **tagged shared-WAL**: `recovered_state(tablet)` for the read, whole-file copy for the backup, `append_tagged(tablet, ...)` for the append.

`plan_from_state` and the `PlanChanged` / `already_single_voter` / `NotAVoter` / `EmptyState` checks are shared unchanged. The tablet's initial-voters argument comes from the tablet's replica set in `Metadata`.

**ADR 0073.** There is **no new durable format**: the sink writes the existing `WalRecord::Hard` and `WalRecord::Append` records, framed by the existing `SWL1` tag (current version) or the plain framing, by the existing encoders (`encode_tagged_record`, `encode_record`). So no version tag, golden fixture or `legacy` decoder is added. The format rules still bind in one way: the sink must use the **current** encoders for the survivor's own version (it never writes an older version), and a survivor whose WAL carries a pre-baseline or unknown `SWL1` version is refused by `SharedWal::open` already (the group fails to open; the recovery reports it). The backup file is a copy of an existing-format file.

### 12. Cross-tablet transactions (ADR 0018), approved: allow and document the risk (decision 5)

The lost tail can contain entries of cross-tablet transactions (ADR 0018): a transaction's intent on the recovered tablet, or the **transaction record** if the recovered tablet is the coordinator. After recovery the survivor holds a consistent prefix of its own log (everything it received), but a healthy tablet may hold the other half of a transaction.

- **Procedure.** After the group restarts, the tool lists the **unresolved intents** on the survivor and resolves each by asking the coordinator tablet for the transaction's status, through the existing foreign-intent path (`resolve_intent_given_status` fed by a `TxnStatus` query to the record owner). Committed: the intent commits at its timestamp. Aborted: it aborts. **Pending** is waited out by the normal resolver. If the coordinator record cannot be found (it lived in the lost tail of a *recovered* coordinator, or the coordinator's record is gone), the intent **aborts**, by the same rule as ADR 0018 section 2b (an absent record plus a stale intent is aborted).
- **Risk, stated plainly.** A multi-tablet transaction that was committed (acknowledged to its client) but whose record or intent was in the lost tail can lose its writes **on the recovered tablet only**, while its writes on the healthy tablets persist. That is a partial transaction, atomicity is violated for those ids, and the tool cannot prevent it. A single-tablet write and a transaction entirely inside the recovered tablet's received prefix are unaffected.
- **Mitigation.** The tool prints the **ids of every transaction whose intent it aborted** (and, where the coordinator is a healthy tablet, every id whose status it resolved), with the tablet, so the operator can reconcile by hand. The runbook (updated in PR 2) states this risk next to the existing lost-tail warning, and says to quiesce multi-item transactions before the procedure if the operator can.
- **Interaction with ADR 0018's 2026-10-27 amendment.** A Settled stub is reaped after `TXN_SETTLED_REAP_AGE`; the old amendment argues no live intent can reference a reaped record. Recovery does not weaken that, but the tool must distinguish "coordinator answered Aborted/Committed" from "no record" in what it prints.

**Open (not decided here):** (a) whether the tool should refuse by default when it finds unresolved intents whose coordinator is itself the recovered tablet, requiring a second flag; (b) whether to report intents already resolved before recovery but whose resolve was in the lost tail (the resolved marker, #1243/#1251, is durable state and should make them idempotent, but this has not been shown for a rewrite); (c) the exact admin route and gating; (d) whether the recovering mark should survive a node restart in the middle of the procedure (the safe default is yes, kept in the control plane or a local marker file, undecided).

### 13. ADR 0073 classification of new cross-node surface

- WAL records and the shared-WAL tag: unchanged, see section 11. Class L (node-local), no `Gate`.
- `CasTabletReplicas`: an existing, already-gated-or-ungated command used with an existing shape; no new variant. Class G unchanged; no new `Gate`.
- The admin action and the reconciler's recovering mark: node-local (L). The mark, if persisted (open (d)), is a new node-local file and would need a version tag and fixture at that point.
- The intent-resolution step uses existing client requests (`TxnStatus` to the record owner) and existing `KvCommand::TxnResolve`; no new cross-node variant.

If the implementation finds it needs any new cross-node variant or field, that is a new G-class surface and must name its `Gate` before it lands.

### 14. Implementation plan: three PRs under issue #1273

The #1273 issue is **not** closed by the doc PR. (Item 3 of the root `CLAUDE.md` prefers one PR per workstream; the maintainer chose three here because each has an independent test surface. The sink is separately mergeable and green.)

1. **WAL sink.** The sink trait and its two implementations in `animus_control::recover`; tablet-id-aware `plan`/`apply` for the tagged shared WAL; unit and `SimEnv` tests (existing control-plane force-new-config tests stay green as the regression net; a shared-WAL cell with many tablets: only the target's tail changes, the others are byte-identical in `recovered_state`).
2. **Operator command.** The reconciler's stop-without-erase and recovering mark, the gated admin action, the `CasTabletReplicas` step, the intent-resolution step and txn-id report, the `animus` CLI verb, and the runbook (`tablet-unavailable.md` row for permanent majority loss; the txn risk of section 12 next to the lost-tail warning).
3. **SimCluster corpus cell.** The fault-injection cell below, under a new `ANIMUS_*` seed knob documented in the root `CLAUDE.md` table, with negative controls.

### 15. Test plan

A `SimCluster` (`animusd`) cell over `LsmEngine`, seeded and replayable with `ANIMUS_SEED`, shared WAL on:

1. 3 nodes, RF 3, several tablets, a DynamoDB-wire workload including single-item writes and multi-item transactions that straddle the target tablet and a healthy one.
2. Kill 2 of the 3 replicas of the target tablet permanently, with a follower lagging (so there is a real lost tail); the survivor is the one with the highest log.
3. Run the recovery through the real path of PR 2 (stop one group, rewrite, CAS, re-host, resolve intents).
4. Assert: **no acknowledged write outside the lost tail is lost** (the oracle records, per acked write, whether it reached the survivor's log); the tablet serves reads and writes again; repair re-grows it to RF; **healthy tablets on the survivor node served throughout** (no write to them failed during the recovery); **the printed affected transaction ids equal the oracle's set of acked transactions whose writes are missing on the recovered tablet only**; every other transaction is atomic.
5. Negative controls (each asserts the oracle FAILS): control-plane CAS before the WAL rewrite (the reverse order wedges); a recovery that erases instead of rewriting; skipping intent resolution (leaves stuck intents).
6. Cells: lagging survivor, uncommitted tail kept, interrupted apply (re-run), the coordinator tablet is the recovered one, and a stale replica returning un-wiped (fenced).

The proof of sections 9 and 12 is these cells; until they exist the amendment is a design, not a claim.
