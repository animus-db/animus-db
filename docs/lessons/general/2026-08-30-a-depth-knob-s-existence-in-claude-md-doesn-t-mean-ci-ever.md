# A depth knob's existence in `CLAUDE.md` doesn't mean CI ever runs it deep — `corpus-deep.yml` needs its own explicit entry per corpus (2026-08-30, `ANIMUS_RAFTKV_SEEDS`)

`raftkv_linearizable.rs` — the flagship Elle-checked linearizability corpus
for the per-tablet CP data plane (ADR 0017) — had a documented
`ANIMUS_RAFTKV_SEEDS` depth knob (and a second, `ANIMUS_RAFTKV_LSM=1`, for
the `LsmEngine<SimEnv>` tier) held green at depth 20/×10 in
`crates/animus-test/CLAUDE.md`, but `.github/workflows/corpus-deep.yml`'s
nightly job never referenced either env var. Every push still ran the
corpus at its default depth (1 = the frozen byte-identical set) via the
normal `gates` job, so the deep-seed proof this corpus exists for was
simply never exercised in CI — the crate guide's "held green at depth N"
line documents a `cargo test` invocation someone ran by hand at some point,
not a standing CI guarantee. **A corpus's env-var depth knob and its
`corpus-deep.yml` matrix entry are two separate things that must both
exist** — adding the knob (and proving it works locally) is not the same
as wiring it into the nightly tier, and nothing fails loudly when the
wiring is missing; the gap is silent unless someone reads the workflow
file against the crate guide's knob table. When adding a new
fault-injection corpus with a depth knob, add its `corpus-deep.yml` step
in the same change (or a tracked follow-up), not "later." Also worth
naming: a corpus that gates an entire alternate code path behind an
off-by-default env var (`ANIMUS_RAFTKV_LSM=1`, the durable
`LsmEngine<SimEnv>` tier vs. the always-on `MemoryEngine` one) needs its
own nightly step distinct from that corpus's plain depth-scaled step, not
just a deeper seed count on the same invocation — the two exercise
different storage-engine code, not just "more of the same" scenarios.
