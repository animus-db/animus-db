# Contributing to CustosDB

Thanks for your interest. CustosDB is foundational distributed-systems code; we
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
`custos-env` (the `ProdEnv` implementation) and tests:

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

To preserve future licensing optionality (see
[ADR 0007](docs/adr/0007-agpl-cla.md)), contributions also require accepting the
project CLA. A CLA bot will comment on your first PR with a one-time signing link.

## Code of Conduct

Participation is governed by our [Code of Conduct](CODE_OF_CONDUCT.md).
