# Real-thread `ProdEnv` liveness/concurrency tests and deterministic `SimEnv` suites need different CI contracts, and the split enforcing that must be structural, not a hand-maintained list (issues #280/#286, 2026-08-18).

**Real-thread `ProdEnv` liveness/concurrency tests and deterministic
`SimEnv` suites need different CI contracts, and the split enforcing that
must be structural, not a hand-maintained list (issues #280/#286,
2026-08-18).** `ci.yml`'s single `gates` job ran the whole workspace's
`cargo test` on a 2-vCPU shared runner; the real-thread tests (`animusd`'s
66 integration binaries + its real-thread `--lib` tests, plus 6 scattered
binaries in `animus-control`/`animus-cp-data`/`animus-storage`/
`animus-consensus`) blow their timing/convergence budgets under
contention from everything else the job builds/runs, starving a different
victim almost every run. Splitting them into a separate, bounded-retry
`prod-liveness` job needed a way to keep them out of the deterministic
`gates` job's plain `cargo test` that can't silently drift as tests are
added later — a YAML exclude list naming test files by hand is exactly
the kind of "two places must agree" hazard this file already warns about
generally. Fix: give each of the 4 non-`animusd` crates an opt-in,
default-off `prod-heavy` Cargo feature and declare each real-thread
binary as an explicit `[[test]] required-features = ["prod-heavy"]`
target — a plain `cargo test` (no `--features`) then skips building or
running them automatically (verified directly: `cargo test -p
animus-storage` builds and runs every other binary in the crate but
neither `lsm_concurrent` nor `idle_engine_cost`, both of which build and
run once `--features prod-heavy` is passed), while `clippy --all-features`
still type-checks them on every push. A new real-thread test added later
without this treatment simply runs inside `gates` and re-creates the
starvation — the fix converts "don't forget to exclude it" into a
compile-visible property (an un-gated binary is *always* in the plain
`cargo test` run) instead of a rule someone has to remember. The retried
tier is a stopgap for runner-class starvation, not a waiver: a test
failing both attempts still blocks merge, per this file's standing
"a flaky `ProdEnv` integration test is a real bug" rule.
(`.github/workflows/ci.yml`, `crates/{animus-control,animus-cp-data,
animus-storage,animus-consensus}/Cargo.toml`.)
