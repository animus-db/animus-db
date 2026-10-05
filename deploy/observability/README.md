# AnimusDB observability kit

Prometheus alert and recording rules, a Grafana dashboard and the SLO
definitions for an AnimusDB cluster (production-readiness sub-track R-01 (f);
exit criteria F-2 to F-5). Everything here is checked against the metrics the
code really exports: see [The metrics-exist check](#the-metrics-exist-check).

| File | What it is |
|------|------------|
| `animus-alerts.yml` | 7 recording rules plus 31 alert rules. Every alert has a `runbook_url`. `promtool check rules` passes. |
| `animus-dashboard.json` | Grafana 10+ dashboard (import it, pick the Prometheus datasource). |

## How animusd exposes metrics

* **Scrape path:** `GET /metrics` on the node's **DynamoDB listener** (the
  `dynamo` port, ADR 0015), plain text, `name value` per line, no `HELP`/`TYPE`
  lines (Prometheus ingests them as untyped), no labels. It is served ahead of
  the SigV4 gate, so no credentials are needed. With TLS on (ADR 0064, server-only
  on `dynamo`) scrape over `https` with the cluster CA.
* **`/admin/metrics` is not a Prometheus endpoint.** It is the admin API's JSON
  view of the same counters (plus `is_leader`, per-tablet rates), used by the
  dashboard/CLI. Prometheus must scrape `/metrics`.
* **Names have no namespace prefix** (`cp_commits`, not `animus_cp_commits`).
  They come from the closed `Metric` enum in `crates/animus-env/src/metrics.rs`;
  every name is always present (value `0` until recorded). Recording rules we
  define use the `animus:` prefix to stay distinguishable.
* **Per node, summed across the node's role sinks.** Counters reset on restart,
  so use `rate()`/`increase()`. Several "counters" are level gauges
  (`cp_groups_quiesced`, `cp_groups_refused_as_voter`, `stream_*_backlog*`,
  `stream_hot_bytes`, `change_log_trim_blocked`, `demux_queued_*`,
  `spawned_task_handles_tracked`, `control_is_leader`); the enum docs say which.

Minimal scrape config (Kubernetes pods created by the operator; adjust the port
to `base_port + 2`, the `dynamo` offset):

```yaml
scrape_configs:
  - job_name: animusd
    metrics_path: /metrics
    kubernetes_sd_configs: [{role: pod}]
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
        regex: animusdb
        action: keep
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: dynamo
        action: keep
```

Load the rules with `rule_files: [animus-alerts.yml]`. The kubelet PVC and
cert-manager alerts need those exporters' metrics and are the only rules that
use non-animusd metrics.

## SLOs

Scope: the client-facing DynamoDB wire. SLO window is 30 days.

### Availability

* **SLI:** `1 - animus:dynamo_request_errors:ratio_*`, where the ratio is
  `dynamo_responses_5xx / dynamo_requests_total` (recording rules in
  `animus-alerts.yml`). A request counts as bad only if the server answered
  5xx (`InternalServerError`, `ServiceUnavailable`). Throttles
  (`ProvisionedThroughputExceededException`) and validation or auth errors
  are 4xx: they are the contract working and do not burn the budget.
* **Objective:** 99.9% good over 30 days, for every op class below. (Per-op-class
  availability needs an op label the exposition does not have yet; see gaps.
  Until then one objective covers all classes.)
* **Alerts:** multiwindow burn rate: `AnimusErrorBudgetFastBurn` (14.4x over
  1h and 5m, page) and `AnimusErrorBudgetSlowBurn` (6x over 6h, ticket), plus
  the plain `AnimusHighServerErrorRate` (>1% for 5m).
* **Supporting SLIs for the strongly-consistent paths:**
  `animus:linearizable_read_barrier_failures:ratio_rate5m` (ConsistentRead
  barrier timeouts) and `animus:write_proposals_not_leader:ratio_rate5m`.

### Latency (p99 per op class)

| Op class | Examples | p99 target | Status |
|----------|----------|-----------|--------|
| Point read, eventually consistent | `GetItem` default | provisional, pending B-01 numbers | Not measurable yet |
| Point read, linearizable | `GetItem` with `ConsistentRead: true` | provisional, pending B-01 numbers | Not measurable yet |
| Single-item write | `PutItem`, `UpdateItem`, `DeleteItem` | provisional, pending B-01 numbers | Not measurable yet |
| Query / Scan page | `Query`, `Scan` | provisional, pending B-01 numbers | Not measurable yet |
| Transactions | `TransactWriteItems`, `TransactGetItems` | provisional, pending B-01 numbers | Not measurable yet |

Every latency target is **provisional pending B-01 numbers**: no benchmark
baseline exists to anchor a number honestly, and none is invented here.
Latency SLIs are also **not measurable from the exposition today** because
animusd exports no latency histogram (gap 1). When it does, the recording
rules to add are `histogram_quantile(0.99, sum by (le, op_class)
(rate(<latency_bucket>[5m])))` per class, and a `>` target alert per class with
`for: 10m`. Until then, measure p99 with a client-side or blackbox probe per
op class and put the target in this table.

## Alerts and signals

| Concern | Alert(s) | Metric(s) |
|---------|----------|-----------|
| Node down / scrape absent | `AnimusNodeDown`, `AnimusScrapeAbsent` | `up` |
| Control plane leaderless or quorum at risk | `AnimusControlPlaneLeaderless`, `AnimusQuorumToleranceExhausted`, `AnimusControlPlaneElectionChurn`, `AnimusControlPlaneMultipleLeaders`, `AnimusMemberDeclaredDown` | `control_is_leader`, `control_elections_started`, `control_failure_detector_down`, `up` |
| Tablet group leaderless | `AnimusTabletGroupLeaderless`, `AnimusWritesBouncingOffNonLeaders`, `AnimusLinearizableReadsTimingOut` | `cp_route_fanout_exhausted`, `cp_proposals_rejected_not_leader`, `cp_read_barriers_timed_out` |
| Under-replicated / data at risk | `AnimusReplicaRefusedAsVoter`, `AnimusEngineOpenFailed`, `AnimusEngineRebuildFailed`, `AnimusReplicaNeedsSnapshotRepeated`, `AnimusStreamRepairBacklog` | `cp_groups_refused_as_voter`, `cp_engine_*`, `stream_repair_backlog` |
| 5xx / ServiceUnavailable | `AnimusHighServerErrorRate`, burn-rate alerts | `dynamo_responses_5xx`, `dynamo_requests_total` |
| Throttling | `AnimusThrottlingHigh` | `throttled_reads`, `throttled_writes` |
| Backlog | `AnimusStreamSealBacklog`, `AnimusStreamSealFailures`, `AnimusChangeLogTrimBlocked`, `AnimusTxnRecoveryStuck` | `stream_seal_backlog_ms`, `change_log_trim_blocked`, `cp_txn_*` |
| Disk | `AnimusDataVolumeFillingUp`, `AnimusDataVolumeAlmostFull` | kubelet `kubelet_volume_stats_*` (third party) |
| Cert expiry | `AnimusTlsCertificateExpiringSoon` | cert-manager (third party) |

### Gaps: signals with no animusd metric today

These are deliberately not faked with invented names. Each needs a metric
added through the `animus-env` seam.

1. **Latency histograms** (per op class). No timing is recorded at all, so no
   p99, and the latency SLO is unmeasurable from the exposition.
2. **Per-op-class request/error counters** (`op` label). The seam is a closed
   enum of unlabeled counters; `dynamo_requests_total` and
   `dynamo_responses_5xx` are aggregate over all ops.
3. **Per-tablet replica health:** number of leaderless, under-replicated or
   quorum-lost tablet groups, and per-group replica counts. Leaderless is
   approximated by `cp_route_fanout_exhausted`, under-replicated by
   `cp_groups_refused_as_voter` and the engine-loss counters. `/admin/raftkv`
   has the per-group truth but it is JSON, not scrapable. A control-plane
   `Metadata`-derived gauge (tablets with fewer Active replicas than policy RF)
   is the right fix.
4. **WAL and compaction backlog:** no level for pending compaction bytes, L0
   file count, WAL bytes outstanding or memtable pressure. Only activity
   counters exist (`storage_flushes`, `storage_compactions`,
   `storage_wal_segment_rotations`).
5. **Disk usage from the node itself:** animusd exports none; the disk alerts
   rely on kubelet or node_exporter.
6. **Certificate expiry:** animusd exports no `not_after` for its own TLS
   material; the alert relies on cert-manager and cannot see a manually
   provisioned `Secret`.
7. **Request-path saturation:** no in-flight request gauge or queue depth for
   the client listener.
8. **Metric typing and prefix:** no `HELP`/`TYPE` and no `animus_` namespace,
   so Prometheus treats everything as untyped, and the names can collide with
   other exporters in a shared Prometheus.

## Runbook pages

Every alert links to `docs/runbook/<page>.md` (sub-track (e) writes them).
Planned pages: `node-down`, `quorum-risk`, `control-plane-leader`,
`tablet-leaderless`, `replica-health`, `engine-recovery`, `txn-recovery`,
`errors-5xx`, `throttling`, `auth`, `network`, `stream-backlog`, `disk-space`,
`cert-expiry`.

## The metrics-exist check

`crates/animusd/src/sim_cluster_admin.rs::metric_references_exist_in_exposition`
(runs in the `animusd --lib` nextest tier on every push) boots a `SimCluster`,
renders the real `ClientCtx::metrics_text()` exposition (the string `GET
/metrics` serves) and `/admin/metrics`, asserts they and `Metric::ALL` agree,
then scans `docs/`, `website/` and this directory:

* In this directory (strict) every identifier whose first `_` segment is a
  metric family prefix must be an exposed metric. Recording rules contain `:`
  and are skipped.
* In `docs/` and `website/` a backticked or `<code>` identifier with a family
  prefix, on a line that mentions metric/counter/gauge/Prometheus/scrape or in
  the first cell of a `Metric` table, must be exposed, or be listed with a
  reason in the test's `NOT_A_METRIC` allowlist.

Renaming or removing a metric therefore fails CI until every reference follows.
