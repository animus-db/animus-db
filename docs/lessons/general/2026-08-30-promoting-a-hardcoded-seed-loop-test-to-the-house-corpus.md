# Promoting a hardcoded-seed-loop test to the house corpus shape: the seed's literal value was never load-bearing, and default-depth coverage is *expected* to shrink (`lsm_crash.rs`/`lsm_disk_faults.rs`, ADR 0061 rung B1)

Converting `animus-storage`'s two `LsmEngine` fault tests
(`tests/lsm_crash.rs`, `tests/lsm_disk_faults.rs`) from hand-rolled
`for seed in [0xA1u64, 0xB2, ...]` loops onto `animus_test::corpus`'s
standard `Scenario`/`seed_expand` shape surfaced two things worth stating
plainly, since both look like regressions at a glance and aren't.

**The specific magic-number seeds were never part of what the tests
prove.** Every scenario in both files asserts a generic invariant ("every
acked write survives a crash", "a corrupted block/record surfaces a clean
error, never silent loss") that must hold for *any* interleaving — none of
them compare against an expected value computed from that particular seed.
So replacing `[0xA1, 0xB2, 0xC3, 7, 42, 1337]` with `corpus::name_seed(name)`
per newly-named cell changes which specific interleavings get exercised but
proves exactly the same thing; there was no need to preserve the literal
hex constants, and doing so would have fought the house doctrine ("a
scenario's seed is always a deterministic function of its own name," see
`animus-test/src/corpus.rs`'s module doc) for no benefit. Where the
original loop crossed a real *structural* axis with its own seed list
(`torn_wal_tail_crash_recovers_all_acked_writes`'s `corrupt: bool`,
`corrupted_manifest_fails_open_cleanly`'s corruption `offset`), each
combination became its own named cell (`..._corrupt`, `..._offset_4`, …)
sharing one extracted body function — the axis is what deserves a name,
the seed list crossing it doesn't.

**Converting a file that unconditionally ran N seeds every push into a
corpus at default depth 1 is a deliberate reduction in per-push seed count,
not a coverage regression** — it's the entire point of the house corpus
doctrine, whose default is *always* 1 (see the root `CLAUDE.md`'s
knob table and every existing corpus in this repo), with seed-sweeping
depth pushed to `corpus-deep.yml`'s nightly tier (`=40`) instead of paid on
every push. Before this conversion, `injected_wal_errors_surface_and_
lose_no_acked_write` ran 6 seeds unconditionally on every `cargo test`;
after, it runs 1 by default and 40 nightly — fewer per-push runs, far more
nightly ones, exactly like every other corpus in the repo. A task framed as
"preserve current behavior/coverage, must not regress existing CI" is
about outcomes (the test still builds, still passes, still exercises the
same fault-injection mechanism), not about the literal per-push seed count
— conflating the two would mean no hardcoded-seed-loop test could ever be
promoted to the standard shape without also being read as a downgrade.
- **When a value can be produced two ways — an explicit config field and a
  positional/minted default — grep the field's read sites and make sure at
  least one fixture in the suite actually diverges the two.** The
  `--config FILE --node I` entry points bound each node under the minted
  `config::node_id(index)` (`"n{index}"`) instead of the config entry's own
  `id` field; every fixture in the repo built ids with the same minting
  convention, so `addrs.id == node_id(index)` held everywhere by
  coincidence and the wrong read was invisible. The first config with
  operator-style ids (`"{cluster}-{ordinal}"`) then failed in the most
  silent way possible: each node's *claimed* identity was absent from its
  own genesis voter set, `is_voter()` was false on every node, and no one
  ever *started* an election — nothing to log, nothing to time out, just a
  cluster that never elects. Regression:
  `crates/animusd/tests/config_node_identity.rs` (a config whose ids
  deliberately do not follow the minting convention).
- **An accept loop must never treat a transient `accept()` error as
  fatal.** `ProdEnv`'s listener task returned on any `accept()` error —
  one transient `EMFILE`/`ECONNABORTED` during a bootstrap burst and the
  node was permanently deaf to inbound connections while looking otherwise
  healthy (observed live: 2 of 3 nodes deafened during a DNS-lag
  bootstrap window). Retry with a short backoff instead; any future accept
  loop at a process boundary gets the same review scrutiny.
