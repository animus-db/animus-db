# Resource bounds and overload (R-01 sub-track d)

As-built record for the overload semantics ADR 0074 §2 fixes: every queue on a
path reachable from an untrusted peer is bounded, and overload is a prompt,
typed, retryable refusal, never an unbounded queue and never a silent hang.
Status per criterion lives in `docs/production-readiness.md` (D-4 to D-8).

## 1. What is implemented

| Bound | Where | Default | Setting | Refusal | Counter |
|---|---|---|---|---|---|
| DynamoDB listener concurrent connections | `animusd::dynamo::serve` | 4096 | `overload.max_connections` / `--max-connections N` | HTTP 503 `ServiceUnavailable`, `Connection: close` (plain TCP); closed outright on a TLS listener | `overload_shed_conn_cap` |
| Node-wide in-flight DynamoDB requests | `animusd::dynamo::handle_conn`, around `dispatch` | 2048 | `overload.max_inflight_requests` / `--max-inflight N` | HTTP 503 `ServiceUnavailable`, connection kept | `overload_shed_admission` |
| Admin listener and console listener, each | `admin::serve`, `console::serve` | 256 | `overload.max_admin_connections` (config file only) | HTTP 503 text; closed outright on TLS | `overload_shed_admin_conn_cap` |
| Client-protocol and intra listener, each | `serve_requests` | 16384 | `overload.max_peer_connections` (config file only) | connection closed (framed protocol, peer reconnects) | `overload_shed_peer_conn_cap` |
| Refusal tasks themselves | `overload::shed_connection` | 64 | fixed | beyond 64 the refused connection is dropped without a response | none |

Zero is rejected for every setting: the DynamoDB port has no "unlimited"
mode (`OverloadSection::validate`). `overload` is a per-node section of a
`nodes[]` entry in the cluster config (`RoleAddrs::overload`, absent means
all defaults, not serialized when absent so the `cluster-config` v1 fixture is
unchanged). The flags apply to `--config/--node`, `data --config`, `data
--seed` and `join`; they are refused with `--cluster N` and
`--cluster-control/--cluster-data` (no per-node config entry). A flag and the
same config field set together is a hard error, like `--tls-*`.

Mechanics:

- `overload::CountGate` is an atomic counter handing out RAII permits. It never
  blocks or queues. A connection permit is held for the connection's whole
  life (so the cap bounds live connections); the in-flight permit is held for
  `dispatch` only. `/metrics` is exempt from admission control so a shedding
  node stays scrapeable.
- Per-connection pipelining is bounded at 1 by construction: `handle_conn`
  reads, executes and answers one request before reading the next.
- A refused connection never gets a task from the accept loop. It goes to a
  bounded refusal task that writes the 503, half-closes, drains the peer's
  unread request for up to 250 ms (so the close is a FIN, not an RST that
  could destroy the response), and exits.
- ADR 0065 per-table throttling is unchanged (`ProvisionedThroughputExceeded
  Exception`, 400). A node-level shed is deliberately a different code.
- When ADR 0074's observability counters (`dynamo_requests_total`,
  `dynamo_responses_5xx`) land, a shed 503 should count as a 5xx; the shed path
  in `handle_conn` is the place to bump them.

Tests: `crates/animusd/tests/overload.rs` (real `ProdEnv`, real TCP):
over-cap connections get a prompt 503 and the listener recovers when held
connections drop; 24 concurrent clients against `max_inflight_requests=2` see
only 200 and 503 `ServiceUnavailable`, the counter moves, and the node serves
again after the load stops. `overload::tests` pins `CountGate` exactness under
8-thread contention. All waits are converged-or-timeout polls.

Worst-case memory for the DynamoDB wire is now a computable bound per node:
`max_connections x MAX_BODY (1 MiB)` of buffered request plus the working set
of `max_inflight_requests` executing requests (each bounded by the 1 MiB
response page cap below). Default: about 4 GiB of request buffer in the
adversarial case, which is why the knob exists; lower it on small nodes.

Known gap: an idle or slow client holds its connection slot indefinitely (there
is no header-read or keep-alive idle timeout), so a slowloris client can fill
the cap. The cap turns that from unbounded memory into a bounded refusal, but a
connection idle timeout is the follow-up that closes it.

## 2. Memory audit (D-6)

| Path | What is bounded | Bound | Status |
|---|---|---|---|
| `animus_node::http::MAX_BODY`, `read_http_request` | header block and body of one HTTP request (dynamo, admin, console) | 1 MiB; `Content-Length` above it is rejected before the body is read | Bounded. Test: `animus-node` http unit tests |
| DynamoDB `Query`/`Scan` page | evaluated item data per page | 1 MiB (`MAX_QUERY_SCAN_PAGE_BYTES`) | Bounded, coordinator-side only (see follow-up 3) |
| `BatchGetItem` | keys per call, response size | 100 keys, 16 MiB (`BATCH_GET_MAX_KEYS`, `MAX_BATCH_GET_RESPONSE_BYTES`) | Bounded |
| `BatchWriteItem` | items, request size | 25 items, 16 MiB | Bounded at decode |
| `TransactWriteItems`/`TransactGetItems` | actions, bytes | 100 actions, 4 MiB | Bounded |
| Client/intra framed protocol (`animus_node::codec`, `read_frame`) | one frame | `MAX_FRAME_LEN` 64 MiB, checked before allocation | Bounded (64 MiB x `max_peer_connections` is large; follow-up 4) |
| Per-listener connection count | tasks and buffers per listener | section 1 | Bounded (new) |
| In-flight DynamoDB requests | concurrent executing requests | section 1 | Bounded (new) |
| Raft `InstallSnapshot` | one chunk | 64 KiB (`SNAPSHOT_CHUNK_BYTES`), streamed chunked | Bounded |
| Raft `AppendEntries` | entries per message | 512 (`MAX_APPEND_ENTRIES_BATCH`) | Bounded |
| `ProdEnv` inbox per stream (demux) | queued frames and bytes per stream | `InboxCap`; oldest dropped past it, `demux_frames_dropped_overflow` | Bounded |
| `ProdEnv` internal wire, `read_frames` | one frame's payload | **none**: allocates `vec![0; len]` for a peer-supplied `u32` length (up to 4 GiB) | **Bug, see section 4** |
| `ProdEnv` accept-to-pump channel (`mpsc::unbounded_channel`, `spawn_accept`) | frames between socket readers and the demux pump | unbounded channel, drained by one pump task into the capped demux | Not an independent bound; follow-up 5 |
| `SharedWal`/WAL group commit | pending append batch | one round per drain, size follows the Raft batch bound | Bounded transitively by `MAX_APPEND_ENTRIES_BATCH` |
| Streams `GetRecords` | records per call | `Limit` (AWS max 1000) and page size | Bounded by decode |
| Export/import jobs | in-memory chunk | `EXPORT_CHUNK_ROWS` 1000 rows per object; per-line import decode | Bounded per chunk; whole-job object count is not |

No other `unbounded_channel`/`mpsc::unbounded` exists outside tests
(`grep -rn unbounded_channel crates/*/src`). Std/other channels in the tree are
`oneshot`.

## 3. Disk-full (D-7): implemented (R-01 (d), issue #1185)

**Status: implemented in simulation; not yet proven on a real size-limited
filesystem** (see Residuals). Before this change an ENOSPC on a WAL `append`/
`sync` hit an `assert!` in `animus_cp_data::persist_wal` /
`animus_control::node::persist_wal` and silently killed that group's consensus
task until a process restart (the node still looked healthy). Now:

1. **Classification.** `animus_env::is_storage_full(&io::Error)` names
   `ErrorKind::StorageFull` at the `Disk` seam, so every layer matches the same
   thing and `SimEnv`'s `DiskConfig::set_enospc_prob` injector exercises it.
2. **Suspect WAL, never retried.** On ENOSPC in a persist round the group calls
   `PersistProgress::mark_suspect`. A suspect WAL is never appended to again and
   its `fsync` is never retried on the old descriptor (fsyncgate: after a
   failed `fsync` the kernel may have dropped the dirty pages, and a torn
   partial append may sit in the file). The round is never marked durable, so
   nothing it covers is applied, visible or acked. A leader in this state
   (or with a stalled apply task) hands leadership off, see "Leader step-down
   (issue #1219)" below.
3. **Recovery without a restart** (`persist_round::recover_suspect_wal`). The
   persist future probes on `env.sleep` with exponential backoff (50 ms up to a
   2 s cap, so a disk that stays full for minutes is not hammered). Each probe
   takes the group's `wal_lock`, skips if a staged compaction rewrite is in
   flight, drains whatever the core owes, captures `RaftCore::wal_image()`
   (snapshot + hard state + the whole in-memory log, a superset of everything any
   failed or stranded round tried to write) and writes it as a **fresh file**:
   `Disk::replace` on the per-group path (a failed replace removes its `.tmp`
   sibling so the space can return), `SharedWal::compact_group` on the shared-WAL
   path. Only after that succeeds does it `mark_durable_through`, declare every
   drained round durable and clear the suspect flag; ENOSPC on the rewrite keeps
   probing, any other error is still a hard failure. No persisted-format change
   (the rewritten file is an ordinary WAL; `scripts/check-format-fixtures.sh`
   passes untouched). A compaction rewrite that hits ENOSPC also marks suspect,
   and compaction is skipped while suspect.
4. **Shared WAL.** `SharedWal` carries a `needs_rewrite` flag armed only by an
   ENOSPC append/sync failure; while armed every `Append` is refused with a
   StorageFull error until a `Compact` succeeds, so a healthy sibling tablet
   cannot stack bytes after the suspect tail.
5. **Wire.** A suspect group refuses writes before proposing with
   `decide::STORAGE_FULL_REFUSAL` ("StorageFull: ...; retry"), mapped by
   `map_throttleable_error` (and `read_should_retry`) to a 503
   `WireError::service_unavailable`, the same house `; retry` suffix as every
   transient refusal. The write and 2PC retry loops stop on
   `is_storage_full_refusal` instead of spinning to a timeout, and each refusal
   bumps `Metric::OverloadStorageFull` (`overload_storage_full`). Reads of
   already-applied state continue.
6. **Admin.** `/admin/health` gains `storage_full`, `storage_full_control` and
   `storage_full_tablets` (degraded signal; the status code deliberately does not
   flip, because pulling the node out of rotation would also take its reads
   away), and `/admin/raftkv` gains a per-group `storage_full` field.
7. **Tests.** The raftkv corpus gains an ENOSPC family
   (`crates/animus-test/tests/it/raftkv_linearizable.rs`, knob
   `ANIMUS_DISK_FULL_SEEDS`): full disk on every replica, on the leader only, and
   a flaky disk (30% per op), each opening and closing a window mid-workload. It
   asserts the linearizability oracle (no acked write lost or duplicated), that
   progress resumes after the window with no restart, and seed determinism.

### LSM-engine ENOSPC (issue #1218)

ENOSPC inside `LsmEngine` no longer panics the apply task:

1. **Classification.** `StorageError::StorageFull` (built from
   `animus_env::is_storage_full` at every disk-seam error site) is the
   recoverable class; its contract is that the failed operation changed nothing
   durable or visible, so the identical call may be retried.
2. **WAL commit.** A failed group commit applies nothing and surfaces
   `StorageFull` to every writer in the lost batch. After an ENOSPC the segment
   is cut back (`replace`) to its last known-durable length before the next
   batch rides it, so a short write left by the failed `append` can never sit in
   front of an acked record. (Only ENOSPC arms this; other errors are unchanged.)
3. **Flush and compaction** fail cleanly: a failed flush leaves the memtable and
   WAL untouched, a failed compaction leaves its inputs authoritative, and both
   remove their partial/complete-but-unreferenced outputs (seqs are only
   consumed by the manifest swap, so the retry reuses them). Inline post-write
   maintenance ENOSPC is **deferred, not surfaced**: the write that triggered it
   is already durable and applied, so it must not fail; the next write retries
   the maintenance. `flush_now`/`compact_now` and the background-maintenance
   backpressure error still return `StorageFull`.
4. **Apply pause.** The apply task's engine handle (`animus-cp-data`
   `apply_stall::StallingEngine`) retries any `StorageFull` call on the `Env`
   clock instead of panicking. The task is blocked inside that one call, so
   nothing is lost, duplicated or reordered. While paused,
   `RaftKvNode::is_storage_full()` is true, so `animusd` refuses new writes with
   the existing 503 `StorageFull` and `/admin/health` reports `storage_full`;
   the flag clears when the call succeeds, with no restart. On shutdown a paused
   call raises `apply_stopped` and parks (no panic).
5. **Tests.** `animus-storage` `tests/it/lsm_disk_full.rs` (WAL commit applies
   nothing and retries, torn-tail repair, flush/compaction ENOSPC leave no
   orphans and retry, deferred inline maintenance; depth
   `ANIMUS_LSM_DISK_FAULT_SEEDS`), and the disk-full corpus now also runs over
   `LsmEngine<SimEnv>` (three representative cells always on; the whole corpus
   under `ANIMUS_RAFTKV_LSM=1`).

Not covered: engine reads and other non-apply engine users (read path, TTL
reaper, reconciler) still propagate the error to their caller, and a
`ProdEnv` size-limited filesystem test is still outstanding.

### Leader step-down (issue #1219)

A StorageFull tablet leader now hands leadership to a replica with free disk, so
the group stays writable through a leader-only disk-full window instead of
refusing every write until space returns.

1. **Mechanism.** The per-tablet consensus loop (`animus-cp-data`) feeds
   `RaftKvNode::is_storage_full()` into the core every pass
   (`RaftCore::set_storage_full`). A storage-full **leader** calls
   `RaftCore::storage_full_step_down`, which arms the existing
   `transfer_leadership` (Raft section 3.10, `TimeoutNow`) toward the voter with
   the highest `match_index` (at least `commit_index`, the arm gate), then wakes
   the loop so `TimeoutNow` ships at once. No new wire message, no persisted
   state.
2. **No ping-pong.** A storage-full node never starts a pre-vote or an election
   and declines `TimeoutNow` (the same gate as `state_machine_behind`): it
   could not persist the term bump anyway, and winning would put leadership back
   on a node that refuses writes. It recovers its WAL in place and campaigns
   normally again once space returns.
3. **No permanent leaderlessness.** A transfer only arms; the leader keeps
   leading until a target actually wins. Since #1228 it arms only when a
   **majority of the other voters** reported healthy (`check_pending == false`,
   which includes "not storage-full") on an ack within one election timeout
   (`RaftCore::healthy_followers`), after a one-election-timeout settle from
   entering the full state, and never toward a voter that reported it cannot
   vote (`transfer_leadership` refuses it for every caller, including the G-01
   preferred-leader step). With fewer healthy voters nobody could win an
   election or commit under any leader, so the leader **stays leader**, serving
   reads and refusing writes; stepping down would only have left the group
   leaderless (F-1). A target that is itself full still declines `TimeoutNow`,
   the transfer aborts at its one-election-timeout deadline, and the retry (after
   a two-election-timeout cooldown) rotates to the next voter. When space returns
   on any node the group converges. The control-plane group is unchanged: it
   never calls `set_storage_full`.
4. **Quiesced groups.** A group that is storage-full vetoes quiescence, arming
   a transfer un-quiesces the leader, and both the apply task's ENOSPC stall
   transition and each refused write (`record_storage_full_refusal`) wake the
   consensus loop, so a parked leader still steps down.
5. **Follower side (issue #1228: frozen acks).** A follower whose WAL is suspect
   still must never vouch for an entry it could not persist, but it no longer
   goes silent either. Before #1228 the failed round gated every later
   `AppendEntriesResp`, even a bare heartbeat's, so a full leader lost all
   follower contact; combined with a step-down that could not see which voters
   were full, an every-replica-full group could lose its leader (F-1). Now the
   follower's ack is **frozen at its own durable index**
   (`RaftCore::handle_append_entries` clamps `match_index` to `durable_index`
   while storage-full) and ships immediately (the consensus loop lets an
   `AppendEntriesResp{success}` with `match_index <= durable_index` out ahead of
   the round, while the WAL is suspect). The leader therefore keeps hearing
   from it (leadership, `peer_last_contact`, ReadIndex probes), the commit index
   can never advance on an entry the follower did not persist, and the ack's
   `check_pending` flag (now also true while storage-full) tells the leader it
   cannot vote. Rejects, vote grants and any ack claiming more than its durable
   prefix are still held until the rewrite lands. Pinned by the
   `a_full_followers_ack_is_frozen_at_its_durable_index` unit test, the
   `raftkv_disk_full_follower_acks_nothing_it_could_not_persist` sim test and the
   `fresh_full_acks` check of the all-full cells.
6. **Tests.** `animus-control` `tests/it/storage_full_step_down.rs` (pure core),
   `animus-cp-data` `tests/it/quiescence.rs` (viii) (quiesced leader), and the
   disk-full corpus: every cell runs a probe writer outside the Elle history and
   the `LeaderDiskFull` cells assert probe writes are acked **inside** the window
   (past a 1.5 s grace), plus linearizability; the all-replica `DiskFull` cells
   still assert recovery after the window.

### Every replica full (issue #1228, F-1 / F-3)

With every replica of a group full the contract is: **the established leader
keeps leading, serves linearizable and eventual reads of what the group already
holds, and refuses every write promptly with a named 503 `StorageFull`** (no
timeouts); a leader that dies in that window is not replaced until space
returns (below). The mechanism is the frozen follower ack and the
healthy-quorum step-down described above, plus two read-path changes:

1. **Linearizable reads at the committed floor.** A linearizable read needs a
   committed `ReadCeiling` above its timestamp (ADR 0018 section 2, 500 ms of
   cover per proposal); a storage-full leader cannot commit a new one. Once the
   ceiling lapses it serves at the highest version its engine holds
   (`RaftKvNode::read_serve_ts`), after the usual ReadIndex barrier whose index
   for a full leader is its first-term entry (the engine may be paused short of
   the commit index and never reach it): every acknowledged write is at or below
   that floor and every later write, on any leader, is minted above anything it
   applied or witnessed. It does not propose ceilings (they could never commit,
   and each would grow the in-memory log and stall the read a full
   `READ_TIMEOUT`).
2. **Eventual reads without a current leader.** `stale_read_ready` accepts a
   storage-full replica that has had leader contact in this process's life even
   when it currently knows no leader, and even with its apply paused (its engine
   is then an in-order prefix of the log; only a half-installed snapshot is
   excluded). A full
   node never campaigns, so after the leader's death nothing would otherwise
   re-establish the "knows a leader" condition.
3. **Leader death while every replica is full.** Not replaced: electing needs a
   durable term bump and self-vote and durable grants, and a node that cannot
   write cannot make a vote it could safely forget never to have cast (see ADR
   0074's 2026-10-06 amendment for why a full node winning "write-refusing" is
   rejected). The group serves eventual reads from every survivor, loses
   nothing, and elects as soon as space returns on a quorum. A node restarted
   *during* the window serves no eventual read until it hears a leader.
4. **Tests.** Unit: `animus-control` `tests/it/storage_full_step_down.rs`
   (frozen ack, healthy quorum, stale report, transfer guard). Corpus:
   `raftkv_disk_full_all_replicas_keep_leadership_and_serve_reads[_lsm]` (RF3 and
   RF5), `raftkv_disk_full_step_down_requires_a_healthy_quorum`,
   `raftkv_disk_full_all_replicas_leader_crash_*`, `quiescence.rs` (ix); wire:
   `animusd` `sim_cluster_dynamo_disk_full`; real: `chaos_disk_full` phase 2.

### Residuals (not done; file as issues)

- ~~LSM engine ENOSPC is not handled.~~ Handled (issue #1218), see "LSM-engine
  ENOSPC" below.
- ~~No leader step-down.~~ Done (issue #1219), see "Leader step-down" below.
- ~~`spawned_task_panics` is not exported, and `/admin/health` does not fail on a
  panicked consensus task.~~ Done (issue #1220): `spawned_task_panics` (any
  spawned task) and `consensus_task_panics` (a `Spawner::spawn_critical` task: the
  control Raft driver and `Metadata` apply loop, each CP-data group's driver and
  apply loop) are `Metric`s; a nonzero `consensus_task_panics` makes
  `/admin/health` return 503 with a `consensus_task_panics` field (a dead consensus
  loop is never restarted, so only a restart repairs it; `/admin/live` is
  unchanged). Alerts `AnimusConsensusTaskPanicked` / `AnimusBackgroundTaskPanicked`
  in `deploy/observability/animus-alerts.yml`; test
  `crates/animusd/tests/consensus_task_panic_health.rs`.
- ~~No `ProdEnv` test on a size-limited filesystem.~~ Done (issue #1221):
  `chaos_disk_full` in the real-process chaos harness (`docs/chaos.md`, "Disk full
  on real filesystems"), per-node tmpfs mounts, CI job `chaos-disk-full`. It found
  defects, now all fixed: **F-1** reads were not reliably served while every node was
  full and **F-3** the 503 refusal raced leadership loss (both closed by #1228, see
  "Every replica full"), and **F-2** a disk-full window with 2PC ops on left an
  unresolved intent that blocked a key after space returned (a split cut a txn
  record off its anchor; `docs/chaos.md`).
- SimEnv injects ENOSPC on reads as well, so a `StopRestart` during a 100%
  window would read an empty WAL; the corpus never combines the two.

## 4. Findings to triage (not fixed here)

1. **Unbounded allocation from a peer frame** (`animus-env/src/prod.rs`,
   `read_frames`): `from_len` and `len` are peer-supplied `u32`s fed straight to
   `vec![0u8; n]`. A peer that can reach the internal port (any host when TLS is
   off) can force a 4 GiB allocation per frame and per connection. The internal
   port is peer-facing and unauthenticated unless mutual TLS is configured. Fix:
   cap `len` (the largest legitimate frame is a 64 KiB snapshot chunk plus
   overhead, or a 512-entry append) and `from_len` (node ids are short) and drop
   the connection above it. Needs its own PR and test.
2. ~~Disk-full is a silent group death~~ fixed for the WAL path and the LSM-engine path (section 3).
3. `Query`/`Scan` byte cap is applied at the coordinator after the per-tablet
   scan RPC returns, so one tablet round trip can still materialize more than a
   page of raw pairs (already noted in `crates/animusd/CLAUDE.md`).
4. Client/intra protocol: 64 MiB frame bound times `max_peer_connections`
   (16384) is a large theoretical buffer; consider a lower per-frame bound for
   the client port.
5. The `ProdEnv` accept-to-pump `unbounded_channel`: one pump drains it, and the
   demux behind it is capped, but a stalled pump (lock contention) would let it
   grow. Replace with a bounded channel and drop-oldest accounting.
6. No idle/slow-header timeout on any HTTP listener (slot exhaustion by a slow
   client, section 1).
7. Admin and console listeners are unauthenticated and their per-request
   handlers are not covered by the in-flight bound (only the connection cap).
