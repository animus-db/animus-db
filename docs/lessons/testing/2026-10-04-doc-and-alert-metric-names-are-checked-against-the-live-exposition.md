# Check doc/alert metric names against the live exposition, not a source grep

Alert rules and dashboards that name metrics silently rot: a rename or an
invented name just makes an alert never fire. R-01 (f) found the real
exposition has **no `animus_` prefix** (names like `cp_commits`), and that
`/admin/metrics` is JSON while Prometheus scrapes `/metrics` on the DynamoDB
port; the first draft assumption (`animus_*` tokens, `/admin/metrics`
scrape) was wrong on both counts, which only reading the handler showed.

The check (`sim_cluster_admin::metric_references_exist_in_exposition`) renders
the real `metrics_text()` from a `SimCluster`, asserts it equals `Metric::ALL`
(+ the `control_is_leader` gauge) and `/admin/metrics`, then scans files.
Scanning prose is noisy (147 backticked `cp_*`/`data_*` identifiers are Rust
names), so docs/website are matched only when quoted AND on a
metric/counter/gauge/Prometheus line or in a `Metric` table's first cell,
with an allowlist carrying reasons; the alert kit directory is matched
strictly. Verify such a checker by adding a bogus name and seeing it fail.
