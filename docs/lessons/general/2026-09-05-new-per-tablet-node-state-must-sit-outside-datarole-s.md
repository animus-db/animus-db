# New per-tablet node state must sit outside `DataRole`'s `Option` if any `SimEnv` `ClientCtx` fixture is expected to exercise it (ADR 0065, W-08 step 2)

`ChangeRateTracker`/`RequestRateTracker` (W-09, ADR 0034) both live inside
`DataRole` — a reasonable, established precedent, since a control-only node
never hosts a tablet and `DataRole` is exactly the "fields that only make
sense with a data role" grouping. ADR 0065's per-tablet token bucket
(`ThrottleTracker`) looked like the obvious third member of that family
until its own testing requirement was checked against this crate's actual
`SimEnv` fixtures: `simenv_client_ctx_tests`, `two_node_relay_tests`, and
`sim_cluster.rs` (the fixture the ADR's own testing section names —
"a `SimEnv` virtual-clock test... exercised over the D1 `SimCluster`
fixture") all construct every node's `ClientCtx` with `data: None` — none
of them build a `DataRole` at all, since nothing they drive
(`cp_kind_write_raw`/`cp_get`/`cp_scan`) needs one. A `DataRole`-gated
`ThrottleTracker` would have compiled fine and passed every existing test,
then been silently unreachable from the one corpus this ADR's testing
section explicitly requires — a design mistake no compiler or existing
test would have caught, only discovered by tracing exactly which fixtures
the new testing requirement names and checking what fields they populate.
The fix was to place `throttle`/`throttle_defaults` directly on `ClientCtx`
itself (mirroring the W-10 precedent that already moved `segment_store`/
`backup_store` off `DataRole` for the analogous "a control-only leader must
still reach this" reason) rather than inside `DataRole`. **General form**:
before copying an existing field's placement inside a gated `Option` group,
check what the *new* field's own testing requirement actually needs
constructed — a precedent's placement was correct for what it had to prove
at the time, not necessarily for what the next field needs to prove.
