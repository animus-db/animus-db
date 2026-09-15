# When replacing N polling loops with one event-driven watch, inventory the consumers whose watch source structurally never fires before deleting the polls — the periodic fallback arm is load-bearing for them, not a safety net; and any guard that gates the new unified loop must be keyed on *every* node-type's own signal, or it permanently blocks the type it wasn't written for.

**When replacing N polling loops with one event-driven watch, inventory the
consumers whose watch source structurally never fires before deleting the
polls — the periodic fallback arm is load-bearing for them, not a safety
net; and any guard that gates the new unified loop must be keyed on *every*
node-type's own signal, or it permanently blocks the type it wasn't written
for.** Wiring the ADR 0031 PR4 reconciler trigger
(`select!(metadata_watch.changed(..), sleep(500ms))`), two growth-node (ADR
0030) hazards were only visible by asking "for which consumer does the
watch never fire": (1) a growth node's own control raft never advances (a
permanent non-voter of a group it never replicates), so `metadata_watch`
never wakes it — only the fallback tick ever drives its reconciler, reading
the `remote_metadata_sync_loop` mirror via `effective_metadata()`; deleting
the old fixed-period loops without the fallback would have silently frozen
every grown node's tablet hosting forever, with zero errors. (2) The
pre-recovery guard the old GC loop used (`raft.last_applied() == 0` → skip,
so a default-empty pre-recovery `Metadata` doesn't read as "everything
dropped") is keyed on exactly the signal a growth node never raises — so
the unified loop's guard had to become `last_applied() == 0 && remote
mirror is empty`, or the same guard that protects a normal node's restart
would have blocked a growth node's reconciler from ever ticking at all.
Also: after any watch-arm wake, coalesce to the source's freshest value
(`watch.latest()`) rather than the value the future resolved with — a
burst of commits under bulk load must collapse into one reconcile tick,
not one per applied entry. (`animusd::tablet_host_reconciler_loop`,
`RECONCILE_FALLBACK_INTERVAL`; `tests/cluster_growth.rs` is the regression
that proves the growth node still functions.)
