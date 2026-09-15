# A Raft group *forming or re-forming* (no live leader) needs the full voter config; only a *new spare joining a led group* starts as a non-voter — and the restart signal is on-disk data, not the epoch.

**A Raft group *forming or re-forming* (no live leader) needs the full voter config;
only a *new spare joining a led group* starts as a non-voter — and the restart
signal is on-disk data, not the epoch.** WAL recovery does **not** restore voter
status from a non-voter `all_nodes` start, so a node re-hosting a tablet it already
has data for must pass the **full** config explicitly. Gating on epoch misfired: a
split bumps the original replicas' epoch, so a post-restart re-host of a split
parent looked like a "join" → non-voter → no election. Use `latest_version() > 0`
(engine has data ⟹ re-forming) as the signal. (ADR 0023, originally `animusd`
`cp_join_host`; since ADR 0031 PR4 the decision lives on unchanged as
`TabletFacts::has_data` in `animus_cp_data::host` — gathered by
`Reconciler::gather_facts` via `StorageScope::has_data`, the shared-engine
successor to `latest_version()`.)
