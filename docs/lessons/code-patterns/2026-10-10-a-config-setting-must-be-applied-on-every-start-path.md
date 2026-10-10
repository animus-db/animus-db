# A config setting must be applied on every start path, in one shared place

**Issue #1241.** `run_bound_node`/`run_bound_node_with` never called
`with_max_region_rtt`, so a bound node silently kept
`DEFAULT_MAX_REGION_RTT` while `run_node*` honoured
`cluster_settings.max_region_rtt_ms`. The same gap had already bitten
`with_mrec` once (`tests/mrec_peer_transport.rs`).

**What to do.** A builder knob fed from `ClusterConfig` is easy to forget on
the Nth entry point. Apply all config-derived builder settings in one helper
(`apply_cluster_settings_to_bound`) that the shared start half calls, and add a
new knob there rather than at each caller. Pin it with a cheap in-crate test
that binds on `:0`, applies the helper and asserts the builder's effective
value; no cluster bring-up is needed to catch a missing call.
