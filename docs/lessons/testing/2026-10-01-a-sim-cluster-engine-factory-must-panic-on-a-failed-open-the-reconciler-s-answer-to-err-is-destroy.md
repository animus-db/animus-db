# A `SimCluster` engine factory must panic on a failed open, because the reconciler's answer to an `Err` open is destroy-and-reopen.

**A `SimCluster` engine factory must panic on a failed open, because the
reconciler's answer to an `Err` open is destroy-and-reopen.** Giving
`SimCluster` an `LsmEngine` backend for the ADR 0073 tier-2 upgrade corpus,
the obvious `EngineFactory::open` returns the engine's `Err`. But
`host::Reconciler::ensure_engine` treats that `Err` as "treat the engine as
lost": it destroys the tablet's files and opens a fresh empty engine (issue
#554), and Raft then repopulates it. A truncated SSTable or a bad transcode
therefore looks like a clean wipe plus a slow catch-up, which is exactly the
masking the tier-1 strict-open lesson warns about, now one layer down from the
test's own `.expect`. The sim factory (`SimLsmTabletFactory`) panics instead
(`strict open of the tablet N LSM engine failed`), and the corpus turns the
panic out of `Simulator::run_for` into the verdict via `catch_unwind`. The
control-plane system-keyspace engine is opened with `.expect` for the same
reason.

Two related sim-fixture facts found building it: `SimCluster::crash` only
*mutes* a node (its tasks stay alive and could still write the disk), so a
whole-cluster stop that must hand quiet disks to a transcoder needs
`Simulator::stop` on every node first (`SimCluster::simulator()`), with
`SimCluster::restart` doing the rebuild afterwards; and a `block_on` engine
open under a `sync_delay` disk config waits on a timer nothing fires, so the
fault disk config is armed only after bring-up and reset to default before the
restart.
