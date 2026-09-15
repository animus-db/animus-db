# Superseded by ADR 0031 (per-node `ClusterEdgeState`, tablet-host reconciler)

ADR 0031 made `--cluster N`'s in-process `ClusterEdgeState` genuinely
per-node (PR2), then replaced four independent `ProdEnv` polling loops
(`cp_join_host_loop`, `cp_gc_loop`, `cp_reconfigure_loop`, plus their
per-node bookkeeping — `minted`, `pending_release`) with one pure planner
(`animus_cp_data::host::plan`, PR3) and one event-driven per-node executor
(`animus_cp_data::host::Reconciler`, driven by `animusd::
tablet_host_reconciler_loop`, PR4). The loops, the `minted`/`pending_release`
fields, and the fixed 500ms/150ms polling cadences these entries describe no
longer exist in `animusd`; see `crates/animus-cp-data/CLAUDE.md`'s "Per-node
tablet-host reconciler" section for the replacement.
