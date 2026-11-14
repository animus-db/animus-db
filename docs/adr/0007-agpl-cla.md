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

## Amendment (2026-11-14): commercial licence on request, and the CLA as built

Until this date only the DCO half of the decision was enforced (`dco.yml`);
no CLA text existed and no bot asked for it, so nothing actually consolidated
the rights the commercial option depends on.

- **Licensing.** AnimusDB stays AGPL-3.0-only for everyone, and is **also
  available under a commercial licence on request**, for organisations whose
  policy forbids AGPL software. The website's licensing page says so.
- **The agreement** is [`CLA.md`](../../CLA.md), version 1.0: a copyright and
  patent licence to the Licensor (not an assignment; contributors keep their
  copyright), broad enough to sublicense under commercial terms, plus a
  commitment that every accepted contribution stays available under the AGPL
  or another OSI-approved licence. It needs legal review before first use.
- **Signing** is a pull-request comment with a fixed sentence, recorded by
  `.github/workflows/cla.yml` (`pull_request_target` + `issue_comment`,
  running `main`'s code only, never the PR's) as a JSON entry on the orphan
  `cla-signatures` branch, keyed by GitHub account id. No third-party CLA
  service or app holds the records.
- **Enforcement** is the `cla` job in `ci.yml`, aggregated by `gates` (PR
  only, like `dco`), so an unsigned PR cannot enter the merge queue. Who must
  sign: the PR author and every linked commit author, except the Licensor's
  accounts and bots; commits whose author e-mail belongs to a coding assistant
  (`.github/cla.json` `assistantEmails`, matched by e-mail because
  `noreply@anthropic.com` resolves to an unrelated GitHub account) are covered
  by the PR author's signature. A commit by an e-mail linked to no GitHub
  account blocks the PR.
- **Prior contributions** are by the Licensor or by coding assistants working
  for it, so there is no backlog of external contributions to clear.
