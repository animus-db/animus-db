# Superseded by ADR 0028

**Superseded by ADR 0028** (single-command, control-plane-only split — the
data-plane `KvCommand::Split`/`propose_split` this entry describes is deleted;
a metadata-level `SplitTablet` is now the *entire* operation). Retained for
historical record. **Metadata-level dedup of a proposal only picks one *winner* — it does not stop
other legitimate callers from still invoking a side-effecting state-machine
command, which must therefore be idempotent at APPLY time, not just deduped at
the propose layer.** In `--cluster N`, every node's auto-split loop shares one
`ClusterEdgeState`, so multiple nodes could independently observe the same
over-threshold tablet and each call `propose_split`; the control plane's
`SplitTablet` metadata command dedups which proposal wins the *metadata*
race, but nothing stopped a second `Split` command from also landing in the
committed CP-group Raft log. Re-applying it recomputed the handoff from
storage — now empty, since the first application had already tombstoned the
range — and re-fired the split hook with an empty handoff, which could win
the mint race and silently seed the new tablet with **no data** (a silent
flake with zero logged errors, `tablet_auto_splits_when_it_grows`, ~1-in-3 to
1-in-10 standalone). Fix: make `Split` apply idempotent (a persistent
`already_split` flag; every application after the first is a no-op) —
replay-safe and failover-safe by construction, not a patch for one race. Any
command carrying a hook/side-effect (not a plain value write) that more than
one caller can legitimately propose needs this. (PR #30.)
