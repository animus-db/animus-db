# Wiring fault primitives into a per-group corpus needs `set_*_for` scoping, and running a corpus at its own nightly depth for the first time can surface an unrelated pre-existing failure (ADR 0061 Decision 3, `txn_serializable.rs`)

Porting `raftkv_linearizable.rs`'s ADR 0061 Decision 3 fault-primitives
wiring (`DiskConfig::set_fsync_lie_prob`, a compound `NetConfig`,
`Simulator::set_clock_drift_for`) to `txn_serializable.rs` needed one real
adaptation, not a copy-paste: the raftkv corpus is a single Raft group, so
its `Nemesis::FsyncLie` sets `DiskConfig` **globally**
(`Simulator::set_disk_config`); the txn corpus has 3 *independent* tablet
groups sharing one `Simulator`, so a global disk fault would lie to every
group at once regardless of which one a scenario means to target —
`set_disk_config_for`/`set_clock_drift_for` (per-node, not the bare
`set_disk_config`/no per-node clock-drift equivalent needed by the
single-group corpus) are the right primitives once a corpus has more than
one independently-faulted unit sharing a `Simulator`. The general form:
when porting a fault-wiring pattern from a single-topology corpus to a
multi-topology one, re-derive which scope (`_for`, whole-`Simulator`, or
per-link) is correct for the *new* corpus's own shape — don't assume the
source corpus's scope choice transfers, since a single-group corpus never
had a reason to need anything narrower than global. `heal_all` resetting
these per-node overrides individually (not just the global `NetConfig`) is
the same "a fired fault must not outlive its window" trap the raftkv PR's
own `heal_all` already documents, just for a `_for`-scoped fault instead of
a global one — resetting the global default does nothing for a per-node
override that was never routed through the global setter at all.

Separately: `ANIMUS_TXN_SEEDS=40` (the depth `corpus-deep.yml`'s existing
nightly tier already configures for this corpus) surfaced a **pre-existing,
unrelated** `check_kind_consistency` divergence on `main`, with no new
fault involved — `compound_lossy_and_anchor_kill_s25` (seed
`8035380114809936673`, a `Lossy` + `AnchorLeaderKill` cell) reproduces
identically on the unmodified checkout. This corpus's own default depth is
1, so nightly's `=40` had evidently not actually been run to completion
against this specific check before — a fault-primitives PR that happens to
validate at that depth is not the same thing as nightly CI having done so.
The general lesson: a corpus's documented "held green at depth K" claim is
only as trustworthy as the last time someone actually ran it at K and
looked at the result — treat validating a change at a corpus's own stated
nightly depth as an opportunity to check that claim is still true, not just
a formality for the change at hand, and when it turns out not to be, that
is a real, separate finding (this repo's "incidental bug gets its own PR"
convention) — report it plainly rather than quietly dropping the depth or
excluding the offending cell to get a clean run.

**Resolved (issue #488):** this divergence was diagnosed as a test-harness
bug, not a production one — `Topology::start` shared one `MemoryEngine`
across a group's 3 replicas; see the Testing section's "cheap to clone,
clones share state" entry for the full mechanism and the fix. The corpus
is green at `ANIMUS_TXN_SEEDS=40` (including `check_kind_consistency`)
with each replica given its own engine.
