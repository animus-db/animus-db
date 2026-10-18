# Overload, throttling and ServiceUnavailable

Conventions are in [README.md](README.md). Related alert pages:
[throttling.md](throttling.md), [errors-5xx.md](errors-5xx.md).

## What exists today

| Mechanism | Behaviour | Source |
|---|---|---|
| Per-table throttling (ADR 0065) | A token bucket per tablet; refill = the table's provisioned capacity divided by its current tablet count; burst capacity = 300 s of that share. Exhausted: single-item ops fail with `ProvisionedThroughputExceededException` (HTTP 400, SDK-retryable); `BatchWriteItem`/`BatchGetItem` return the throttled items in `UnprocessedItems`/`UnprocessedKeys`; transactions cancel with a `ThrottlingError` reason. | `animus-dynamo/src/wire.rs`, `animusd/src/{write_path,read_path}.rs` |
| `ServiceUnavailable` (HTTP 503) | The server's own retry budget ran out on a **transient** refusal (split cutover freeze, leadership chase). It is not a capacity signal. | `WireError::service_unavailable` |
| `InternalServerError` (HTTP 500) | Internal failure, for example no quorum for the tablet. | `error_status` in `dynamo.rs` |
| Request size caps | 1 MiB HTTP body cap on the edge; AWS-faithful item/page/batch limits (ADR 0072). | `animus-node/src/http.rs`, `animus_dynamo::limits` |
| Inbox cap | Per-stream inbound frame queue is bounded, drop-oldest ([network.md](network.md)). | `demux_*` metrics |

**Connection and in-flight limits exist (R-01(d), ADR 0074 section 2).** The
DynamoDB listener caps concurrent connections (`overload.max_connections`, 4096)
and node-wide in-flight requests (`overload.max_inflight_requests`, 2048); past
either, the node answers 503 `ServiceUnavailable` at once and never queues, and
`overload_shed_conn_cap` / `overload_shed_admission` count it (the admin, console,
client and intra listeners have their own caps: `overload_shed_admin_conn_cap`,
`overload_shed_peer_conn_cap`). Pipelining is bounded at 1 per connection. See
[Change the limits](#change-the-limits) and `docs/resource-bounds.md`. What is
**not** bounded yet is the data path behind admission: a node whose disk or apply
path falls behind still queues proposals on a leader, and a leaderless tablet parks
requests for the client budget (ADR 0080 proposes the fixes). A saturated node can
therefore still slow down and time out; clients that retry make it worse
(`client_requests_abandoned` counts requests whose client left first). Throttling is **off by default**:
a table is `PAY_PER_REQUEST` (unthrottled) unless it has its own
`ProvisionedThroughput` or the cluster default below is set.

## Diagnose

```sh
curl -s http://<dyn>/metrics | grep -E '^(overload_shed_[a-z_]+|throttled_reads|throttled_writes|client_requests_abandoned|cp_proposals_rejected_not_leader|cp_read_barriers_timed_out) '
curl -s http://<admin>/admin/metrics | jq '.throttle, .request_rates'   # per tablet: tokens left, rates, throttled counts
```

`throttle[]` has per tablet `read_tokens`, `write_tokens`, `read_rate`,
`write_rate`, `read_throttled`, `write_throttled`. Tokens near 0 with rising
throttled counts is a real capacity limit: the whole table (all tablets) or one
hot tablet. `request_rates` shows which tablets take the writes.

1. **Throttled** (400 `ProvisionedThroughputExceededException`, message
   "table `T` exceeds its provisioned write capacity"): raise the limit, split the hot
   tablet, spread keys, or let clients back off. Expected behaviour, not a fault.
2. **503 `ServiceUnavailable`**: first check the `overload_shed_*` counters: a
   rising `overload_shed_conn_cap` or `overload_shed_admission` is the node's own
   connection or in-flight limit (raise it only if CPU and disk have headroom, else add
   nodes); otherwise check for a split or leader change in progress.
   Clients should retry. Sustained: [tablet-unavailable.md](tablet-unavailable.md).
3. **Latency up, no throttles**: resource saturation (CPU, disk); see
   [disk-full.md](disk-full.md) and [capacity-planning.md](capacity-planning.md);
   add nodes (growth is online: [node-replace.md](node-replace.md) A step 2) so
   placement can rebalance.

## Change the limits

Per table (preferred; replicated, any node accepts it):

```sh
curl -s -X POST http://<admin>/admin/data/dynamo -d '{"op":"UpdateTable","payload":{"TableName":"T","BillingMode":"PROVISIONED","ProvisionedThroughput":{"ReadCapacityUnits":1000,"WriteCapacityUnits":500}}}'
curl -s -X POST http://<admin>/admin/data/dynamo -d '{"op":"UpdateTable","payload":{"TableName":"T","BillingMode":"PAY_PER_REQUEST"}}'   # remove throttling
```

(`aws dynamodb update-table` against the DynamoDB port does the same, signed.)
Cluster-wide default for tables without their own setting: `animusd` flags
`--throttle-read-units N --throttle-write-units N`, or the config file's
`cluster_settings.throttle_read_units` / `throttle_write_units` (one way, not
both; restart required). The Kubernetes operator's CRD does not expose these.
`POST /admin/throttle/defaults {"read_units":N,"write_units":N}` sets them on
**one node only, at runtime, not persisted** (a test hook).

A provisioned table is also pre-split to a throughput-derived minimum tablet count
(ADR 0067: `ceil(RCU/3000 + WCU/1000)`; `--tablet-max-read-units` /
`--tablet-max-write-units` tune the ceilings). Auto-split for size is off unless
`--auto-split-bytes` / `cluster_settings.auto_split_bytes` (operator
`spec.autoSplitBytes`) is set; write-rate splitting is `--auto-split-ops-rate`.

Per node (not replicated; restart required): the `overload` section of the node's
entry in the cluster config (`max_connections`, `max_inflight_requests`,
`max_admin_connections`, `max_peer_connections`) or the `animusd` flags
`--max-connections N` / `--max-inflight N`. Zero is rejected, there is no
"unlimited" setting, and a flag plus the same config field is a startup error.

Note the ADR 0065 consequence: eventually-consistent reads are admitted per
replica, so aggregate read admission can reach RF times the per-tablet share.

## Refusals from ADR 0080 (propose bound, admission, transport)

A node now refuses work it cannot absorb instead of queueing it. Read the
`overload_shed_*` counters on `/metrics` to see which bound fired:

| Counter | Client sees | Means | Do |
|---|---|---|---|
| `overload_shed_busy` | 400 `ThrottlingException` (item in `UnprocessedItems` for a batch) | a tablet leader's appended-but-unapplied backlog hit its bound: the disk or apply path is the bottleneck | clients back off (the SDK does); check disk latency and `/admin/health` `queued_bytes_hwm`; do not raise the bound first |
| `overload_shed_group_inflight` | 400 `ThrottlingException` | 256 requests are parked on one tablet from this node | a hot or stuck tablet; check `tablet-unavailable.md`; consider a split |
| `overload_shed_no_leader` | 503 `ServiceUnavailable` | a tablet has had no leader for `leaderless_wait_ms` (2 s) | [tablet-leaderless.md](tablet-leaderless.md) |
| `overload_shed_admission` | 503 | the node gate (shrinks with scheduling lag) is full | CPU starvation: `election_lag_ms` on `/admin/health`; add nodes or cores |
| `overload_shed_cold_wake` | 400 `ThrottlingException` | a write would wake a quiesced group while lag is severe and awake is at the cap | wait; reduce write fan-out across idle tablets |
| `overload_shed_forwarded`, `overload_shed_budget` | forwarded refusal / 400 | forwarded gate or the node `QueuedBytes` budget is full | check `queued_bytes` against `queued_bytes_budget` |
| `overload_shed_admin_inflight` | 503 from `/admin/*` | 32 admin calls are running; probes are exempt | stop the scraper hammering expensive admin routes |

Transport: `transport_outbox_dropped_{vote,heartbeat,replicate}` and
`transport_outbox_bytes` show a stalled peer being bounded; votes and
heartbeats keep priority over bulk snapshot traffic. The node budget is
`overload.queued_bytes_budget` (`--queued-bytes-budget`; the operator sets 25 %
of `spec.resources.limits.memory`). Size a pod for the measured floor plus that
budget plus engine working memory (`docs/density-measurements.md`).

## Verified

Setting a table to 1 RCU / 1 WCU on a local dev cluster and writing 400 items in
a tight loop produced the first throttle after the 300-token burst (item 303), the
exact error body above, `throttled_writes 97` on `/metrics` and the matching
`write_throttled` in `/admin/metrics`.

## Maturity

The throttle path is covered by simulation and real-thread tests; the dev-cluster
check above was run by the author. The connection and in-flight caps are proven
over real TCP (`crates/animusd/tests/overload.rs`); no soak behaviour of a saturated
real node has been measured (pending R-01(a)/(b), B-01).
