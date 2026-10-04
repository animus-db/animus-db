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

**Not implemented (pending R-01(d)): connection limits, admission control, a global
concurrency cap, and a defined overload response when a node is saturated.** A
node under more load than it can serve slows down and times out; clients that retry
make it worse (`client_requests_abandoned` counts requests whose client left
first). Do not assume a bounded queue exists. Throttling is **off by default**:
a table is `PAY_PER_REQUEST` (unthrottled) unless it has its own
`ProvisionedThroughput` or the cluster default below is set.

## Diagnose

```sh
curl -s http://<dyn>/metrics | grep -E '^(throttled_reads|throttled_writes|client_requests_abandoned|cp_proposals_rejected_not_leader|cp_read_barriers_timed_out) '
curl -s http://<admin>/admin/metrics | jq '.throttle, .request_rates'   # per tablet: tokens left, rates, throttled counts
```

`throttle[]` has per tablet `read_tokens`, `write_tokens`, `read_rate`,
`write_rate`, `read_throttled`, `write_throttled`. Tokens near 0 with rising
throttled counts is a real capacity limit: the whole table (all tablets) or one
hot tablet. `request_rates` shows which tablets take the writes.

1. **Throttled** (400 `ProvisionedThroughputExceededException`, message
   "table `T` exceeds its provisioned write capacity"): raise the limit, split the hot
   tablet, spread keys, or let clients back off. Expected behaviour, not a fault.
2. **503 `ServiceUnavailable`**: check for a split or leader change in progress;
   clients should retry. Sustained: [tablet-unavailable.md](tablet-unavailable.md).
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

Note the ADR 0065 consequence: eventually-consistent reads are admitted per
replica, so aggregate read admission can reach RF times the per-tablet share.

## Verified

Setting a table to 1 RCU / 1 WCU on a local dev cluster and writing 400 items in
a tight loop produced the first throttle after the 300-token burst (item 303), the
exact error body above, `throttled_writes 97` on `/metrics` and the matching
`write_throttled` in `/admin/metrics`.

## Maturity

The throttle path is covered by simulation and real-thread tests; the dev-cluster
check above was run by the author. No overload or soak behaviour of a saturated
real node has been measured (pending R-01(a)/(b)/(d), B-01).
