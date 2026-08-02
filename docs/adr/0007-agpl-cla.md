# ADR 0007 — AGPL-3.0 + CLA

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

We want AnimusDB to be genuinely open source while protecting against a managed
service provider taking the code, running it as a hosted database, and returning
nothing to the project. We also want to keep future licensing options open (for
example, offering a commercial license) without having to track down every past
contributor.

## Decision

- License the project under **AGPL-3.0-only**. The AGPL's network-use clause
  closes the "SaaS loophole": anyone offering AnimusDB as a network service must
  offer their modified source to users of that service.
- Require every contribution to carry a **DCO sign-off** (`git commit -s`) *and*
  to be covered by a **Contributor License Agreement**, enforced by a CLA bot on
  pull requests.

## Consequences

- Hosted/managed use must reciprocate source changes, aligning incentives with
  the project.
- The CLA preserves relicensing optionality (e.g. a future dual license) because
  contributors grant the project the necessary rights up front.
- AGPL and the CLA add friction for some contributors and forbid some corporate
  uses; we accept this as the cost of the protections above.
- Dependencies must be license-compatible with AGPL redistribution; this is
  enforced by `cargo-deny` (see `deny.toml`).
