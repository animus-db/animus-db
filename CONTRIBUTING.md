# Contributing to AnimusDB

Thanks for your interest. AnimusDB is foundational distributed-systems code; we
optimize for clarity and testability over completeness.

## Build & test

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
cargo deny check          # licenses + advisories (install: cargo install cargo-deny)
```

All of the above must be green in CI before a PR merges.

## The non-negotiable rule: determinism

System code must be deterministic. Concretely, in any crate other than
`animus-env` (the `ProdEnv` implementation) and tests:

- **No wall clock.** No `std::time::Instant::now`, `SystemTime::now`, or
  `tokio::time` directly — get time from `Env::clock()`.
- **No raw task spawning.** No `tokio::spawn` / `std::thread` — use
  `Env::spawner()`.
- **No real I/O.** No `std::net`, `std::fs`, `tokio::net`, `tokio::fs` — go
  through `Env::network()` / `Env::disk()`.
- **No unseeded randomness.** No `rand::thread_rng` / `OsRng` — use `Env::rng()`.
- **No `HashMap` / `HashSet` in logic.** Their iteration order is
  nondeterministic. Use `BTreeMap` / `BTreeSet`. This is lint-enforced via
  `clippy.toml`.

See [ADR 0003](docs/adr/0003-deterministic-simulation.md). Every distributed
behavior must land with a fault-injecting simulation test that is reproducible
from a seed.

## Working style

- **One milestone / logical change per PR.** Keep diffs reviewable.
- **When a decision changes, update the relevant ADR in the same PR.**
- Prefer thin end-to-end vertical slices a simulator can exercise over a
  "finished" subsystem in isolation.

## Developer Certificate of Origin (DCO)

Every commit must be signed off, certifying the [DCO](https://developercertificate.org/):

```sh
git commit -s -m "your message"
```

This adds a `Signed-off-by: Your Name <you@example.com>` trailer.

## Contributor License Agreement (CLA)

AnimusDB is licensed under the AGPL-3.0 and is also offered under a commercial
licence on request (see [ADR 0007](docs/adr/0007-agpl-cla.md)). For your
contribution to ship in both, you also accept the
[AnimusDB Contributor License Agreement](CLA.md). You keep the copyright in
your work; the agreement grants the project a licence to distribute it, and
commits the project to keeping it available under the AGPL.

Signing takes one comment, once per GitHub account:

1. Open your pull request. If you have not signed yet, the `CLA` workflow
   comments with what to do, and the required `cla` check stays red.
2. Read [CLA.md](CLA.md), then post this exact comment on the pull request:

   ```
   I have read the AnimusDB CLA and I hereby sign it.
   ```

3. The workflow records your signature (GitHub account, date, agreement
   version, link to your comment) on the public `cla-signatures` branch and
   re-runs the failed CI jobs, so the check turns green without a new push.

Everyone who authored a commit in the PR signs, not just the person who opened
it. Commits must be authored by an e-mail address linked to your GitHub
account, or they cannot be matched to your signature. Commits written by a
coding assistant on your behalf are covered by your own signature
(`.github/cla.json` lists the assistant e-mails). The mechanism lives in
`.github/workflows/cla.yml`, the `cla` job of `.github/workflows/ci.yml`, and
`.github/scripts/cla.js`.

## Code of Conduct

Participation is governed by our [Code of Conduct](CODE_OF_CONDUCT.md).
