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

## 3. Disk-full (D-7): what happens today, and the design

Traced on a real node (`ProdEnv`):

1. `ProdEnv::append` and `sync` return `std::io::Error`; ENOSPC surfaces as
   `ErrorKind::StorageFull`. Nothing in `animus-env`, `animus-storage`,
   `animus-cp-data` or `animus-control` inspects it. There is no named
   `StorageFull` error anywhere outside the `SimEnv` fault injector.
2. The per-tablet Raft consensus loop persists with `animus_cp_data::persist_wal`
   (and the `SharedWal` variant), the control plane with
   `animus_control::node::persist_wal`. On an `append` or `sync` error while the
   group is not `halted`, both `assert!` (a hard panic): "a real durability
   fault on a live leader (crash-stop-before-ack)".
3. A panic inside a task spawned through `env.spawn_task` is caught by
   `ProdEnv` (issue #939), logged ("spawned task panicked"), counted on the env
   and the process stays up. There is no `panic = abort` in any profile.

Consequences:

- **Never acks an unsynced write.** The round is drained from the core but
  `mark_durable_through`/`complete_drain` never run, so the entry is never
  durable, never applied, never visible, never acked. This half of ADR 0074
  holds today, by crash-stop.
- **Not a named error, not a 503 naming StorageFull.** The client sees its
  request time out and then the generic transient `ServiceUnavailable` (HTTP
  503) produced by an exhausted retry budget, with no mention of storage.
- **The group is dead until restart.** The consensus-loop task has exited. That
  replica neither acks nor heartbeats (the rest of the group elects around it if
  a quorum remains). Space returning does not revive it; only a process restart
  (which replays the WAL) does. Compaction or an LSM flush that hits ENOSPC on
  the apply task is a separate path with the same expect/assert shape.
- **The node looks healthy.** `/admin/live` and `/admin/health` stay 200 (the
  process and the control leader belief are fine), and the spawned-task panic
  count is a `ProdEnv` inherent method (`spawned_task_panics`), not exported by
  `/metrics`. An operator sees a log line only. Reads of already-applied state
  keep working from the surviving apply task, but a leader whose loop died
  serves nothing consistent.
- **Control-plane WAL has the same shape**, and is shared by the whole node,
  so ENOSPC there kills control Raft participation for the node.

Why this PR stops at the audit and a design: the recovery ADR 0074 requires
("resumes without a restart, nothing acked lost or duplicated") cannot be done by
mapping an error. The records of a failed round were already drained out of the
core (`drain_for_round`), the WAL file may hold a torn partial append that a
retry must not stack onto, and after a failed `fsync` the kernel may have
dropped the dirty pages, so retrying `fsync` on the same descriptor and trusting
it is wrong (fsyncgate). It needs a redesign of the persist round, not a patch.

Design (not implemented):

1. Add `animus_env::DiskError`-style classification: `ErrorKind::StorageFull`
   becomes a named `StorageFull` kind at the `Disk` seam boundary, so every layer
   can match it and `SimEnv`'s existing ENOSPC injector exercises the same match.
2. Persist round becomes fallible and re-queueable. On an append/sync error the
   driver (a) never marks the round durable, (b) restores the drained records to
   the front of the core's pending queue (new `RaftCore::requeue_unpersisted`),
   (c) records `storage_full` on the group and steps a leader down (and makes a
   follower not ack), and (d) marks the WAL file "suspect".
3. Recovery: a suspect WAL is never appended to again. When free space is
   available (probe: a small staged write plus sync on a scratch file, polled
   by the driver on `env.sleep`), the driver truncates the WAL back to the last
   fully synced offset it tracked (`SyncMarkerState` already tracks synced
   rounds) via `replace`/`stage_replace`, or rewrites it whole from the core's
   in-memory log, then retries the requeued round on the new file. Never retry
   `fsync` on the old descriptor and assume success.
4. Wire: `StorageFull` maps to `WireError::service_unavailable("StorageFull: ...
   ; retry")` (503, house suffix) at the same sites that map a transient refusal
   today (`map_throttleable_error`, `cp_kind_write_item`), and bumps a new
   `overload_storage_full` counter. `/admin/health` reports a degraded
   `storage_full` field while any hosted group is suspect.
5. Tests: a `SimEnv` cell (extending the raftkv corpus, `DiskConfig::
   set_enospc_prob` windows) asserting no acked write is lost or duplicated and
   that writes resume without restart after the window; then a `ProdEnv` test on a
   size-limited filesystem (tmpfs mount, needs `CAP_SYS_ADMIN`, so CI-only).
   Note the current corpus excludes ENOSPC injection precisely because of the
   panic in item 2 above (`animus-test/CLAUDE.md`).
6. Small related fix: export `spawned_task_panics` through the metrics seam
   (needs the `Env` trait or a metric slot fed by the spawn wrapper) and make
   `/admin/health` fail when a consensus-loop task has panicked.

## 4. Findings to triage (not fixed here)

1. **Unbounded allocation from a peer frame** (`animus-env/src/prod.rs`,
   `read_frames`): `from_len` and `len` are peer-supplied `u32`s fed straight to
   `vec![0u8; n]`. A peer that can reach the internal port (any host when TLS is
   off) can force a 4 GiB allocation per frame and per connection. The internal
   port is peer-facing and unauthenticated unless mutual TLS is configured. Fix:
   cap `len` (the largest legitimate frame is a 64 KiB snapshot chunk plus
   overhead, or a 512-entry append) and `from_len` (node ids are short) and drop
   the connection above it. Needs its own PR and test.
2. **Disk-full is a silent group death** (section 3).
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
