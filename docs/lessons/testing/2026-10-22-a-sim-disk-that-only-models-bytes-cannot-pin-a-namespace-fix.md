# A sim disk that only models bytes cannot pin a namespace fix

PR #27 fixed `ProdEnv` not fsyncing the parent directory after creating or renaming a
file, but no sim test could pin it: `SimEnv` kept a created-but-never-synced file as an
empty entry and never un-did a `remove`, so the bug was invisible under simulation.
`docs/sim-coverage.md` carried it as a GAP until the sim modelled the namespace.

What generalizes:

- When a production fix is about an OS durability rule (dir fsync, rename ordering),
  extend the sim to model **exactly what the contract permits and no more**, mirroring
  the production memo (`dir_synced`: only the *first* sync of a name fsyncs the dir),
  or the sim becomes stronger than prod and raises false failures.
- Make the new default behaviour RNG-free and trace-silent (a never-synced file
  vanishing) so existing seeds and pinned digests do not move; put the adversarial
  part (an unsynced `remove` un-happening) behind a `DiskConfig` knob that must be
  armed before the workload. Do the new crash step *after* existing RNG draws.
- Every new crash model needs a negative control that re-introduces the old behaviour
  as a knob (`set_sync_skips_dir_fsync`) and asserts the oracle FAILS. Without it a
  green crash corpus proves nothing.
- Higher layers (LsmEngine, SharedWal) already passed the new model at 60 and 30
  seeds: their recovery tolerates orphans and rebuilds from replace-made manifests.
