# ADR 0080 — Overload admission on the client path and the transport: group-aware admission, a propose bound, bounded queues, a node memory budget

- **Status:** Proposed (2026-10-15). Docs only: no implementation code lands with this ADR. Defaults are **provisional** until the calibration step of the implementation plan.
- **Date:** 2026-10-15
- **Origin:** issue #1296 (companion to #1295 / ADR 0079); the two follow-ups PR #1317 (issue #1304) left open; ADR 0074 section 2 ("every queue is bounded"); maintainer order "ADR first, code after".
- **Amends:** none. **Depends on:** ADR 0003 (determinism), ADR 0009 (control-plane Raft, 2026-10-14 pipelining amendment), ADR 0016/0017 (CP data plane), ADR 0026 (stream addressing, inbox cap), ADR 0044/0048 (quiescence, heartbeat batching), ADR 0055 (replica-local reads), ADR 0065 (per-table throttling), ADR 0073 (upgrade compatibility), ADR 0074 (exit criteria), ADR 0078 (campaign governor, `LoadMonitor`), ADR 0079 (awake cap, permit pools, shared `lag -> factor` helper).
- **Not in scope:** the L0 write stall and compaction IO limit (#1300), per-table throttling defaults (ADR 0065 stays opt-in), the `Query`/`Scan` byte cap pushed into the per-tablet RPC (resource-bounds finding 4.3, see "Out of scope"), forwarded-request connection pooling.

## Context

ADR 0078 bounds the election feedback loop and ADR 0079 bounds how much a node keeps awake and how much background work it runs. Neither bounds **foreground load**: how much client work a node accepts, how much a single Raft group may queue, and how much the transport may buffer when a consumer is slow. Those are exactly the cases where a node is already overloaded, so an unbounded queue there turns a slow node into a dead one (the #1199 shape, on the client side).

### Evidence

All numbers from `docs/density-measurements.md` section H (issue #1304, single `ProdEnv` runs, noise about 25 %) unless stated.

- **Quadratic resend (fixed, PR #1317).** `LsmEngine`, 700 groups, 600 puts/s of 4 KiB over 50 groups: 2.62 GB RSS (71 MB at start), 80.7k committed-but-unapplied entries, 450-700 MB of encoded frames parked in 11-17k send tasks. #1317 capped the leader at 4 entries in flight per peer: 0.81 GB, 35.7k backlog, all 700 groups led. **Not fixed:** the open-loop rig still queues past the node's apply capacity (about 400-450 puts/s here), growing about 1.4 GB at 150 s, because `ProposeResult::Accepted` has no admission control. With a closed loop (`ANIMUS_BP_LIVE_WRITE_WINDOW=64`) the plateau is 0.55 GB, and the closed-loop floor (0.4-0.55 GB: allocator retention, LSM flush/compaction buffers, log plus apply clones) needs a node-wide budget. This ADR is where #1317 sent both.
- **Load moves the awake ceiling.** About 0.8-1.0 ms of process CPU per RF-3 write (MemoryEngine, 128 B), about 2 ms with `LsmEngine` and 4 KiB; 1000 puts/s costs the equivalent of 800-1000 awake replicas (section E3). Admission that is blind to load shares CPU with the election machinery ADR 0078 protects.
- **Issue #1296 findings** (re-grepped at `5df2f273`, below). Reasoning about client retry amplification is inferred, not measured.

### What exists today (grepped, not assumed)

Client path:

- **One flat node gate.** `CountGate` (`animusd/src/overload.rs:36`) caps in-flight DynamoDB requests at `max_inflight_requests` = 2048 (`config.rs:41`); `handle_conn` takes the permit at `dynamo.rs:622` and holds it across the whole `dispatch`. The refusal is 503 `ServiceUnavailable`, counted `overload_shed_admission`. The gate runs **before** the request is decoded, so it cannot see the target table, tablet or group state. It is not scaled by `LoadMonitor` lag.
- **A request on a leaderless group holds its slot for up to `CLIENT_TIMEOUT` (10 s, `lib.rs:2436`).** `cp_route` (`forwarding.rs:225`) polls `resolve_cp_route` every `SCHEMA_POLL_INTERVAL` (50 ms, `dynamo.rs:211`) until the shared `deadline`; the write loop (`write_path.rs:576-660`) then retries a `"; retry"` error with a 50 ms sleep until `retry_until`. Both spend from one budget, but nothing short-circuits it when the group is known to have been leaderless for seconds. Every such request keeps a node slot; the client times out and retries, and the gate stays saturated.
- **No per-group bound.** No `Semaphore` exists in `animusd` (grep), and the only counters are the node-wide ones. A hot or stalled group can take all 2048 slots, starving healthy groups on the same node (inferred; no test shows it).
- **Eventual reads are not the problem.** `ConsistentRead: false` is served from any replica without leadership (ADR 0055); only leader-requiring operations (writes, consistent reads, 2PC) park on `cp_route`.
- **Forwarded requests have no in-flight bound.** The intra listener has `max_peer_connections` = 16384 (`lib.rs:15973`) and `handle_connection` serves one request per connection at a time (`lib.rs:16161`), so up to 16384 forwarded operations can execute concurrently on a leader, with no cost bound and no per-group bound. Each forward dials a **new TCP connection** (`lib.rs:19578`, `relay_request_with_timeout`).
- **Admin and console handlers** are bounded by connection count only (finding 4.7); no idle or slow-header timeout on any HTTP listener (finding 4.6).

Raft and propose:

- **`RaftCore::propose` checks only role and transfer** (`animus-control/src/raft.rs:4288`). The data-plane choke point is `gated_propose` (`animus-cp-data/src/lib.rs:3788`), which checks only the ADR 0073 feature gate. Nothing bounds uncommitted or committed-unapplied entries or bytes per group. Log retention is capped in entries (`COMPACT_RETENTION_CAP_ENTRIES` 4096), not bytes. #1317's `inflight_sent` bounds the *wire* (4 entries per peer), not the log.
- **`ProposeResult` has two variants** (`Accepted`, `NotLeader`; `raft.rs:911`), is in-memory only (no serde), and is referenced at about 213 sites. Several call sites turn any non-`Accepted` result into a string without the house `"; retry"` suffix (`write_path.rs:804`: `"kind write not accepted: {other:?}"`), which `read_should_retry` treats as terminal and the wire edge as `InternalServerError`.
- **Precedent for a pre-propose refusal:** `refuse_if_storage_full` (`StorageFull:` prefix plus `"; retry"`; `is_storage_full_refusal` stops the inline retry loops from spinning on it).

Transport (`animus-env/src/prod.rs`):

- **One task per outbound frame, uncounted.** `send_stream` (`:1130`) spawns a task per frame (`:1176`) bounded by `SEND_TIMEOUT` (2 s, `:1337`) and waits on a per-peer-address `tokio::Mutex` slot (`:1959`). The task count is rate x 2 s with no cap (11-17k seen in section H), each holding its payload.
- **One connection per peer carries every class.** Heartbeats, votes and 64 KiB snapshot chunks queue FIFO behind the same slot lock, so a stuck bulk write delays a vote by up to `SEND_TIMEOUT`.
- **Unbounded accept-to-pump channel** (`mpsc::unbounded_channel`, `:1827`; resource-bounds finding 4.5). The receive-side demux is capped (`InboxCap`: 8 MiB / 4096 frames per stream, drop-oldest, `enforce_inbox_cap` `:928`), but **reserved streams are exempt** (`:929`, `is_reserved_stream`), which includes the heartbeat batch stream.
- **`HeartbeatInbox` is an unbounded `VecDeque`** (`animus-cp-data/src/heartbeat_batch.rs:162`): a group whose consumer is slow accumulates one heartbeat per 50 ms flush forever. The batched frame per destination grows with the groups per destination, with no split.
- The transport sees only `(to, stream, bytes)`; Raft messages are `serde_json` inside the payload, so **the transport cannot classify a frame**. A class has to be supplied by the sender.

Doc drift found while grepping (to be fixed in a separate docs PR, see Open question 11): `docs/production-readiness.md` D-4, D-5, D-6, D-8 say "Not met" although `docs/resource-bounds.md` records the connection cap, in-flight gate, memory audit and shed counters as landed; `docs/runbook/overload-and-throttling.md` still says admission control is "Not implemented (pending R-01(d))".

## Decision

Five mechanisms, all **node-local** (class L, see the classification). One refusal vocabulary (below). One lag input: ADR 0079 decision 9's shared `lag -> factor` helper (`load_monitor::scale_by_lag`) drives every cap here.

### 1. Leader propose bound: `ProposeResult::Busy`

At `gated_propose` (the data-plane choke point, under the `core` lock so the check and the append are atomic), refuse a **client-origin** command when the group's backlog exceeds a bound. Backlog = `last_log_index - engine_applied_index` (uncommitted plus committed-but-unapplied entries) and the payload bytes of those entries (a small ring of `(index, bytes)` trimmed by the apply watermark).

- `propose_backlog_max_entries` = **512**, `propose_backlog_max_bytes` = **4 MiB** per group, both lag-scaled (`scale_by_lag`: half at 30 ms, a quarter at 100 ms, minimum 8 entries / 256 KiB). Basis: section H's closed loop at a window of 64 held a flat 0.55 GB; 512 is 8x that window and 4 MiB of 4 KiB values is 1024 entries, so entries bind first for small items and bytes for large ones. `0` disables each (today's behaviour).
- New variant `ProposeResult::Busy { backlog_entries, backlog_bytes }`. **Busy is a definite refusal**: nothing was appended, so unlike a timeout it can never have taken effect; callers may retry or fail without an idempotency question. The control-plane group never refuses (the bound lives in `animus-cp-data`, not `RaftCore::propose`).
- **Admission classes, exhaustive match, no `_` arm** (the `required_gate` pattern): `KvCommand::admission_class()` returns `Client` (kind writes, 2PC stage: bounded at the limit), `Internal` (TTL reaper deletes, index drain, MREC apply, change-log trim: bounded at **2x** the limit, so housekeeping yields to clients), or `Liveness` (2PC commit/abort/resolve decisions, `ReadCeiling`, `SeedBatch` for restore: **never refused**, because they are what drains the backlog; refusing a resolve behind a refused stage is a deadlock). A new `KvCommand` variant fails to compile until classified.
- **Wire form:** the refusal travels as a string on the existing `ClientResponse::Error` path, `"Busy: this tablet's apply backlog is over its bound ...; retry"`, following the `StorageFull:` convention (`decide::BUSY_REFUSAL`, `is_busy_refusal`). The inline retry loops (`cp_kind_write_raw_bounded`, the item/batch/txn loops) **stop on it** like they stop on storage-full and return it to the edge, which maps it at the terminal return (lesson 2026-09-20). No new wire variant, no new relayed code (see classification).
- **Audit:** every `ProposeResult` site gets reviewed; the `other => Err("... not accepted: {other:?}")` arms (`write_path.rs:804` and about six like it across `animusd`) must route `Busy` to the refusal above, or `Busy` surfaces as a terminal 500. A negative control covers it.

### 2. Client admission that sees group state

Three layers, outermost first.

**2a. The node gate becomes lag-aware.** `max_inflight_requests` stays 2048 and the 503 `ServiceUnavailable` refusal stays. Its effective size is `scale_by_lag(2048, lag)` (1024 at 30 ms, 512 at 100 ms, floor 256), refreshed by the `LoadMonitor` the way it refreshes `K_eff` and `A_eff` (an `attach_admission` beside `attach_awake`/`attach_budget`). `/metrics`, `/admin/health`, `/admin/live` stay exempt.

**2b. Leaderless fast-fail (frees the slot).** `cp_route` keeps a per-tablet `unresolved_since` (set when `resolve_cp_route` first fails, cleared when it resolves). For a **leader-requiring** operation:
- unresolved for less than `leaderless_wait` (default **2 s**): wait as today (an election is normally shorter; ADR 0078's worst-case detection bound is 10 x `election_base` = 1.5 s on LAN defaults, so 2 s covers a loaded failover);
- unresolved for `leaderless_wait` or more: **refuse at once**, with no polling and no slot held, `"NoLeader: tablet T has had no reachable leader for Ns; retry"`, mapped to 503 `ServiceUnavailable`. It recovers on its own: `resolve_cp_route` is a cheap local read, so the first request after a leader appears resolves normally.
- The WAN profile scales the wait with the group's own election base (`max(2 s, 10 x base)`), like ADR 0078's stretch cap.
- Replaces the 10 s per-attempt park with a 2 s park plus an O(1) refusal. `CLIENT_TIMEOUT` is unchanged for requests that did find a leader.

**2c. Per-group in-flight.** An edge-side counter per tablet, `group_inflight_max` = **256** (one eighth of the node gate), counting requests parked on that tablet (waiting for a leader, a forward reply or a confirm) on this node. Over it: `"Busy: too many in-flight requests for tablet T; retry"`. This stops one hot or stalled group taking every slot. The leader's propose bound (decision 1) covers the same group from the other side, including forwarded requests, so the edge counter needs no cross-node signal.

**2d. Forwarded requests.** A separate `CountGate` on the intra listener's `Forwarded` execution, `max_forwarded_inflight` = **4096** (config-file only, like `max_peer_connections`), answering `ClientResponse::Error("Busy: ...; retry")` without executing. It is separate from the client gate so a flood of client work cannot starve a leader of the forwards that complete it, and vice versa.

**Refusal vocabulary (DynamoDB-faithful).** "Slow down" and "try later or elsewhere" are different signals, and AWS SDKs treat them differently: a throttle code (`ThrottlingException`, `ProvisionedThroughputExceededException`, `RequestLimitExceeded`) lets adaptive retry mode reduce the client's rate, while a 5xx only backs off.

| Condition | Status / code | Why |
|---|---|---|
| Node gate full (2a); leaderless fast-fail (2b); forwarded gate full | 503 `ServiceUnavailable` (unchanged for 2a) | The server's condition, not the caller's rate (the distinction `WireError::service_unavailable` already documents). Counted as 5xx so the 5xx-ratio alert sees overload. |
| Propose `Busy` (1); per-group in-flight (2c) | 400 `ThrottlingException` | A hot group is a caller-rate signal: retry with backoff, and adaptive clients slow down. Not `ProvisionedThroughputExceededException`: that code stays reserved for configured capacity (ADR 0065). |
| `TransactWriteItems` hitting either | `TransactionCanceledException`, reason `ThrottlingError` | The reason code the throttle path already emits (`dynamo.rs:5901`). |
| `BatchWriteItem` / `BatchGetItem` hitting either | the refused items in `UnprocessedItems` / `UnprocessedKeys` | The AWS shape; verify against ADR 0065's batch handling in the implementation. |

Never `InternalServerError`, never a hang to `CLIENT_TIMEOUT`. The mapping is a pure step at the edge's terminal return (lesson 2026-09-20): the internal `"; retry"` loop decisions do not change.

**Quiesced groups (composition with ADR 0079).** ADR 0079 makes the awake cap soft because a refusal path did not exist; this ADR supplies one. When `lag >= 100 ms` (the `A/4` regime) **and** `awake >= A_eff`, a client write that would *wake* a quiesced group (not one to an already awake group) is refused with `Busy`, so the working set keeps being served and a cold-wake storm is shed. Below that regime the cap stays soft exactly as in ADR 0079. Counted separately (`overload_shed_cold_wake`). This is Open question 4.

### 3. Bounded transport queues, per message class

The sender supplies a class (`Network::send_stream_classed`, defaulting to `Normal`; in-memory only, no wire field). A per-peer outbox replaces task-per-frame: **one writer task per peer** drains bounded per-class queues in strict priority order, with the existing `SEND_TIMEOUT`-bounded dial and write.

| Class | Examples | Policy | Bound |
|---|---|---|---|
| `Vote` | `PreVote`, `RequestVote`, their responses, `TimeoutNow` | Strict priority, never dropped for capacity; on overflow drop-**oldest** (a stale vote is useless) | 4096 frames per peer |
| `Heartbeat` | batched heartbeat frames, bare-heartbeat acks | Priority above bulk; **coalesce**: a newer batch for the same destination replaces the queued one | 1 frame per destination (plus any acks, 4096) |
| `Replicate` | `AppendEntries` with entries, their acks | **Supersede per (group, kind)**: a newer append anchored at the same acknowledged position replaces the older (the sender re-anchors at the ack, per #1317, so a drop is a delayed retransmit, never a gap); beyond the byte bound drop-oldest | 8 MiB and 4096 frames per peer (the inbox's own numbers) |
| `Bulk` | `InstallSnapshot` chunks, segment-store traffic | **Block the producer, do not drop**: the snapshot/segment sender takes a byte credit and yields when none is free (it is already a polled loop under ADR 0079's permit pool); on its **own TCP connection** | 4 MiB credit per peer |

- **Heartbeats and votes cannot be starved** by replication or bulk: they sit above both in the drain order, and `Bulk` uses a second connection so a stuck bulk write cannot hold the control slot. This is the transport half of ADR 0078's rule that the *responder* must stay live (lesson 2026-10-11): a dropped or delayed grant costs an election window, and #1297 showed 29,099 grants arriving after the window had lapsed.
- **The outbox is a pure, `Env`-seam-clean struct in `animus-env`** (sync, `BTreeMap`, time passed in), so `SimEnv` and `ProdEnv` share it and it is property-testable. A drop is counted per class (`transport_outbox_dropped_{class}`); outbox bytes are a gauge.
- **Accept-to-pump channel** (`prod.rs:1827`) becomes a bounded channel (16384 frames) whose reader **awaits** when full, so TCP backpressure reaches the sender, whose outbox is bounded. Receive-side policy stays class-blind (the receiver cannot classify; adding a class byte to the frame header would be a wire change, rejected below).
- **`HeartbeatInbox`** holds at most **8** messages per group, drop-oldest (heartbeats are idempotent; 8 flushes is 400 ms of consumer lag). Reserved streams stay exempt from `InboxCap`, but the exemption now has a named bound at each of the two reserved consumers that can lag.
- **Heartbeat batch frames** are split at 8192 entries per frame (a receiver already handles any number of frames).

### 4. Node memory budget

A node-wide `QueuedBytes` budget `B`, a shared `Arc` atomic counter with `try_reserve(n)`/RAII release, **charging only what this ADR bounds**: propose-backlog bytes (decision 1), transport outbox bytes (3), per-stream inbox bytes (the demux already counts them) and in-flight request bodies (2). It does **not** charge `LsmEngine` memtables or compaction/flush buffers: the engine has its own gates and charging them would put storage policy in the transport (Open question 8).

- `B` = **256 MiB** by default (`--queued-bytes-budget`), enabled out of the box: "every queue is bounded" (ADR 0074) cannot depend on a flag being set. The operator sets it to **25 % of the pod memory limit**.
- **Soft watermark at 70 %:** the node gate and the group bounds shrink by one lag-step (as if lag were 30 ms). **Hard at 100 %:** a client-class reservation fails and the request is refused (`Busy`). `Internal` and `Liveness` classes charge but are never refused, so `QueuedBytes` can exceed `B` by their bounded amount; the bound is therefore `B` plus the `Internal`/`Liveness` allowance, stated in the admin output.
- **Composition with ADR 0079.** `A` (awake replicas x per-core) protects **CPU** from idle awake groups; `B` protects **memory** from written groups. They multiply on different axes (a node can be under `A` and over `B` with 50 hot groups, as the section H rig was) and neither changes the other's number. They share one lag input and one reduction rule, so both tighten together. Per-group bounds stay fixed (a hot group cannot reserve more than `B / 8` of the budget: `min(4 MiB, B / 8)`), so `B` is the real ceiling and the number of simultaneously hot groups is bounded by `B / (typical backlog)`, not by `A`.
- **Sizing floor.** Section H's closed-loop floor is 0.4-0.55 GB RSS from allocator retention and engine buffers that `B` does not charge. A pod needs `floor + B + working set`; the operator warns (condition, not an error) when `memory limit < 1 GiB`. This is the number the measurement said was missing; the ProdEnv measurement in the test plan replaces the estimate.

### 5. Residual bounds that belong here

From `docs/resource-bounds.md` section 4: **4.6** `header_read_timeout` = 10 s and `keepalive_idle_timeout` = 120 s on the HTTP listeners (the SDK pools keep idle connections for under that, so a closed idle connection is retried by the SDK); **4.7** a separate `max_admin_inflight` = 32 gate on admin and console handlers, `/admin/health`, `/admin/live` and `/metrics` exempt; **4.4** a lower inbound frame cap on the *client* listener only (16 MiB; the intra listener keeps 64 MiB). **4.3** (scan byte cap pushed into the per-tablet RPC) is out of scope: it adds a request field, which is a cross-node surface.

### Knobs (summary)

| Knob (flag / config section) | Default (provisional) | Meaning |
|---|---|---|
| `propose_backlog_max_entries` / `_bytes` | 512 / 4 MiB per group | decision 1; `0` disables |
| `max_inflight_requests` | 2048 (unchanged), lag-scaled | decision 2a |
| `leaderless_wait` | 2 s | decision 2b |
| `group_inflight_max` | 256 | decision 2c; `0` disables |
| `max_forwarded_inflight` | 4096 (config file) | decision 2d |
| `queued_bytes_budget` | 256 MiB (operator: 25 % of memory limit) | decision 4 |
| (constants) | outbox per-class bounds, `HeartbeatInbox` 8, accept channel 16384, header/idle timeouts, soft watermark 70 % | change by ADR/code |

`overload` is already a per-node section of `nodes[]` (`RoleAddrs::overload`, not serialized when absent); the new fields are `Option`s in `OverloadSection` with the same rules (zero is rejected for the gates; `0` means "off" only where stated). All fixed numbers, none from `available_parallelism`.

### Metrics

Additive `Metric` variants (no-op under sim): `OverloadShedBusy`, `OverloadShedNoLeader`, `OverloadShedGroupInflight`, `OverloadShedForwarded`, `OverloadShedColdWake`, `OverloadShedBudget`, `CpProposeBacklogEntriesHwm`/`BytesHwm`, `TransportOutboxDropped{Class}`, `TransportOutboxBytes`, `QueuedBytes` (gauge + hwm), `HeartbeatInboxDropped`. `/admin/health` gains `queued_bytes`, `queued_bytes_budget`, `busy_groups` as **advisory** fields: per ADR 0078 decision 4, sustained shedding never fails the health check or the roll gate. The shed counters join the 5xx-ratio alert's inputs.

## ADR 0073 compatibility classification

Everything here is **L (node-local)**. No new `KvCommand`, no replicated `Metadata` field, no WAL or snapshot record, no key layout, no new `ClientRequest`/`ClientResponse` variant, no new frame header field, no change to any message's meaning.

| Item | Class | Reason |
|---|---|---|
| `ProposeResult::Busy`, admission classes | L | In-memory enum, never serialized; `admission_class()` is a local classification, not a `required_gate`. |
| `Busy:` / `NoLeader:` refusal strings | L, with a defined mixed-version reading | They ride the existing `ClientResponse::Error` string. An N-1 edge receiving `"Busy: ...; retry"` sees an ordinary `"; retry"` error and retries inline until its own deadline (today's behaviour, not a wedge); an N-1 leader never emits it. No new relayed `WireError` code is minted on the forwarded hop, so the relayed-error allowlist (`RELAYABLE_WIRE_ERROR_CODES`) is untouched. |
| Edge mapping to `ThrottlingException` / `ThrottlingError` | L | Rendered at the node the client talks to. |
| Node/group/forwarded gates, `leaderless_wait`, lag scaling | L | Decide what *this* node accepts. |
| Per-peer outbox, classes, second bulk connection | L | The class is a send-side argument. A receiver accepts frames on any connection and routes by the unchanged `(from, stream)` header, so an N-1 receiver is unaffected; a second TCP connection per peer needs no handshake change. |
| Heartbeat batch split, `HeartbeatInbox` and accept-channel bounds | L | The receiver already handles any number of batch frames. |
| `QueuedBytes` budget | L | In-memory counter. |
| New `overload` config fields, operator spec fields | F-adjacent, additive | Optional, `skip_serializing_if`; the frozen `ClusterConfig` v1 fixture is untouched (ADR 0078 / 0079 precedent). |
| `Metric` variants, admin JSON | not a format | Additive. |

No `Gate`, no cluster-version bump, no `required_gate` row, no golden fixture. **Would become G:** a typed `retry-after` field on `ClientResponse::Error` (rejected below), a class byte in the frame header, or the scan-byte pushdown (out of scope). Rolling-upgrade note: the first node on the new binary can enforce everything immediately; a roll's per-node health wait is unaffected because the new fields are advisory.

## Test plan

Every distributed behaviour gets a seeded, fault-injecting simulation; negative controls assert the oracle **fails** with the mechanism off (lessons 2026-10-04, 2026-10-06, 2026-10-14: each control must bite on an observable the mutation removes, with a workload that makes the mechanism matter). The existing `Simulator::set_cpu_cost` models the apply capacity and a slow peer; no new sim primitive is proposed.

**Tier 0, pure/property.** Admission class exhaustiveness; the backlog ring (bytes and entries, trimmed by the apply watermark); `Busy` leaves the log untouched; `Liveness` is never refused and `Internal` at 2x only; outbox: strict priority, coalesce/supersede, per-class bounds, a `Vote` is never behind more than one in-progress frame, the producer-block credit returns on drop; `QueuedBytes` reserve/release (RAII, no leak across cancellation); `leaderless_wait` state machine; `scale_by_lag` reuse; the `Busy` string is never `read_should_retry`-inline-spun.

**Tier 1, `SimEnv` corpus, new `animus-cp-data` `tests/it/overload_admission_corpus.rs`, knob `ANIMUS_OVERLOAD_SEEDS` (default 1), `ANIMUS_OVERLOAD_CELL=<substring>`.** RF-3 groups, a proposer 2x the modelled apply capacity, the raftkv linearizability oracle (a `Busy`-refused write is a definite failure; an acked write must survive), plus: backlog never exceeds bound + slack; goodput stays at least 80 % of apply capacity (no collapse); ADR 0078's oracles (leaders retained, bounded `max_term`) hold; the backlog drains and writes are admitted within a bound after load stops. Cells:
1. steady overload; 2. overload + leader kill; 3. overload + one stalled follower; 4. overload + partition heal; 5. one leaderless group + load on others (healthy groups keep at least 95 % of their rate, refusals for the leaderless group return within `leaderless_wait` + one RTT); 6. leaderless-then-recovered (the first request after a leader appears is served); 7. quiesced hot-wake under lag; 8. transaction stage refused while resolves of earlier txns are still admitted (no deadlock); 9. outbox: bulk flood does not delay votes or heartbeats beyond one frame (term churn bounded); 10. `HeartbeatInbox` under a stalled consumer stays at 8.
**Negative controls (each must fail its oracle):** (a) bounds off: backlog and bytes grow past the bound; (b) `Busy` applied to `Liveness` commands: a txn never resolves; (c) `Busy` treated as `Accepted`: a lost-write oracle failure; (d) no fast-fail: healthy-group throughput collapses in cell 5; (e) outbox without class priority: term churn or lost leaders under the bulk flood; (f) heartbeat coalescing off: inbox grows; (g) `Busy` surfaced as a terminal error (the `other =>` arm): an SDK-style retrying client sees a 500.

**Tier 2, cluster, `animusd` `sim_cluster_overload`** over `SimCluster`'s LSM backend and the DynamoDB wire, `C17`-style metric lines: open-loop clients with SDK-like retry (3 attempts, exponential backoff) against capped nodes. Assert only `ServiceUnavailable`/`ThrottlingException`/`ThrottlingError` ever appear (never 500, never a response later than the bound), acked writes durable, and a **retry-storm cell** (the #1199 pattern on the client side): the node gate's occupancy by requests to a leaderless group stays near zero, where the control with fast-fail off saturates it.

**Tier 3, `ProdEnv`.** (i) Extend the density rig (`election_backpressure_liveness.rs`) with the section H LSM open-loop command for 150 s: acceptance is RSS flat (no growth between 60 s and 150 s; target at most 1.0 GB), all 700 groups led, shed fraction and accepted-write p99 reported; this replaces the "about 0.4-0.55 GB" estimate in the budget sizing. (ii) `animusd` `tests/overload_admission.rs` (real TCP): error codes under saturation, refusal latency, recovery. (iii) A stalled-peer `ProdEnv` test (a listener that never reads): outbox bytes and writer-task count stay under the cap, other peers' heartbeats keep flowing, no panic. All waits are converged-or-timeout polls.

## Alternatives considered

- **A pre-propose `refuse_if_busy()` (the storage-full pattern) instead of a `ProposeResult` variant.** It needs no change to the 213 sites, but the check and the append are not atomic: concurrent proposers each pass the check and overshoot by the number of concurrent proposers. The variant is checked under the `core` lock and the compiler finds the sites. Open question 3.
- **One typed `Retry-After` / `Busy` variant on `ClientResponse`.** Cleaner than a string, but a new variant on the forwarded hop is a cross-node surface (class G, a new `Gate` at cluster version 6, an exhaustive `required_gate` row and a mixed-version cell). The string convention already carries `StorageFull` and `FROZEN_REFUSAL`. Rejected for now; revisit if more refusal kinds appear.
- **A class byte in the frame header so the receiver can prioritise.** Class G (an old receiver misreads the header). Rejected; receiver-side backpressure is class-blind and sender-side priority carries the guarantee.
- **Hold the node permit only for execution, acquired after routing.** Needs the request decoded before admission, which moves parsing outside the gate; the 2 s park plus an O(1) refusal frees the slot in the cases that matter without restructuring `handle_conn`.
- **Drop-newest on the outbox.** Starves exactly the message a node most needs (the latest vote or ack); drop-oldest and supersede keep the freshest information.
- **A hard cap on a replicated, cluster-wide admission rate.** Puts consensus on the data path's failure path and needs a gate; the resources are one node's CPU and memory.
- **Charging engine buffers to `QueuedBytes`.** Puts storage policy into the admission layer and double-counts the engine's own memtable backpressure; left to the storage ADR (#1300).
- **Making ADR 0079's awake cap hard everywhere.** A quiesced tablet would become unwritable on any busy node; the refusal is limited to the severe-lag regime (Open question 4).

## Implementation plan

One implementation PR on one branch (repo operating mode), ordered commits, each green on all gates. This ADR is its own docs-only PR first.

1. **Calibration harness first** (test knobs only): open-loop LSM rig with apply backlog, shed and RSS output, a stalled-peer rig; record in `docs/density-measurements.md` and fix the defaults.
2. **Pure primitives:** `KvCommand::admission_class`, the backlog ring, `QueuedBytes`, the pure `PeerOutbox`, `leaderless` state; Tier 0.
3. **Leader propose bound:** `ProposeResult::Busy`, the `Busy:` refusal, the retry-loop exits, audit of every site; cells 1-4, 8, controls (a)-(c), (g).
4. **Edge admission:** leaderless fast-fail, per-group in-flight, lag-scaled node gate, forwarded gate, error mapping (including txn and batch shapes); cells 5-7, controls (d); Tier 2.
5. **Transport:** `HeartbeatInbox` bound and batch split, bounded accept channel, the per-peer outbox with classes and the bulk connection; cells 9-10, controls (e), (f); Tier 3 (iii).
6. **Node budget:** `QueuedBytes` wiring, operator derivation, admin fields, ADR 0079 composition, Tier 3 (i)-(ii).
7. **Residuals and docs:** idle/header timeouts, admin in-flight, client frame cap; ADR to Accepted with an as-built note; ADR 0074/0078/0079 cross-references; `docs/resource-bounds.md`, `docs/runbook/overload-and-throttling.md`, crate guides, `docs/roadmap.md`; check `website/` claims.

The docs-drift fix (Open question 11) is a separate small PR and does not gate this one.

## Open questions for the maintainer

1. **Which code for a `Busy` / per-group refusal: 503 `ServiceUnavailable` for everything, or 400 `ThrottlingException` for group-level conditions?** Recommended: `ThrottlingException` for `Busy` and the per-group cap (a hot group is a caller-rate signal and lets adaptive SDK retry slow down), `ServiceUnavailable` for the node gate, leaderless and forwarded gates (the server's condition, visible to the 5xx-ratio alert), `ThrottlingError` as the transaction reason. The cost is that group-level overload is invisible to the 5xx alert, so the shed counters join it.
2. **Are the provisional defaults acceptable as starting points** (backlog 512 entries / 4 MiB per group, `leaderless_wait` 2 s, `group_inflight_max` 256, `max_forwarded_inflight` 4096, `B` 256 MiB)? Recommended: yes, to be replaced by the calibration step; they are single-run-derived and flagged as such.
3. **`ProposeResult::Busy` variant, or a pre-propose check?** Recommended: the variant. Atomic under the `core` lock; the compiler plus a grep audit find the 213 sites; the pre-check overshoots under concurrency.
4. **Refuse a write that would wake a quiesced group when lag >= 100 ms and `awake >= A_eff`?** Recommended: yes, only in that regime; it is the refusal path ADR 0079 said it lacked, and below it the cap stays soft so a quiesced tablet is never made unwritable by ordinary load. The risk is a tablet unwritable for the duration of a severe lag episode, bounded by recovery.
5. **Replace task-per-frame with a per-peer outbox now, or ship only a global cap on in-flight send tasks first?** Recommended: the outbox, in one PR with a commit order that lands the simple cap first. The cap alone fixes memory but not priority, and heartbeat starvation is the failure ADR 0078 cares about.
6. **A second TCP connection per peer for the `Bulk` class?** Recommended: yes. Class L, one extra socket per peer, removes head-of-line blocking of votes behind a stuck snapshot write.
7. **Memory budget on by default at a fixed 256 MiB, or off unless configured (as the awake cap needs `--cores`)?** Recommended: on by default; the operator derives 25 % of the memory limit. A bound that exists only when configured does not meet ADR 0074's "every queue is bounded".
8. **Should `QueuedBytes` charge `LsmEngine` memtable and compaction buffers?** Recommended: no, here. Size the pod for `floor + B + working set` (floor 0.4-0.55 GB, measured) and leave engine memory policy to the storage ADR (#1300).
9. **HTTP listener `header_read_timeout` 10 s and `keepalive_idle_timeout` 120 s?** Recommended: yes. Provisional; confirm against the AWS SDK pool idle defaults before fixing.
10. **String refusal convention or a typed `Retry-After` on the forwarded hop?** Recommended: the string convention now (class L, no Gate). Revisit with a typed field and `Gate` at cluster version 6 only if a third refusal kind needs structure.
11. **Fix the documentation drift in a separate docs PR** (`docs/production-readiness.md` D-4, D-5, D-6, D-8, `docs/runbook/overload-and-throttling.md`, roadmap R-01(d) items #1220/#1221)? Recommended: yes, before implementation step 7, so the as-built note edits a true baseline.
12. **Client-listener frame cap 16 MiB (finding 4.4), and defer 4.3 (scan byte pushdown) to its own PR?** Recommended: yes to both; 4.3 adds a request field and needs its own classification.
