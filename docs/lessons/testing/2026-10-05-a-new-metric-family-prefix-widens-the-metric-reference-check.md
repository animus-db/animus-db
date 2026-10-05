# A new metric family prefix widens the metric-reference check's "looks like a metric" net

`sim_cluster_admin::metric_references_exist_in_exposition` flags any backticked
identifier in `docs/`, `website/` and `deploy/observability/` whose first `_`
segment is a prefix of some exposed metric. Adding the first metrics under a new
prefix (`cluster_gate_*`) made unrelated identifiers that start with that word
(an ADR-cited test name `cluster_wide_throttle_...`) fail the check. The
exposition/registry equality half passed; only the doc-scan false positive
tripped. Fix: add the identifier to `NOT_A_METRIC` with a reason. When appending
metrics under a brand-new first segment, run the check immediately, and expect
this allowlist edit.
