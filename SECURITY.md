# Security Policy

AnimusDB is **pre-alpha** (see [README.md](README.md)). It is not yet
recommended for production data, but we take vulnerability reports seriously
and handle them as described here. The release and support policy is in
[docs/release.md](docs/release.md).

## Supported versions

| Version | Supported |
|---------|-----------|
| Latest tagged release (`vX.Y.Z[-pre]`) | Yes: security fixes land here |
| `main` (unreleased) | Best effort: fixes land on `main` first |
| Any earlier release | No, until a post-1.0 support window is defined |

While the project is pre-1.0, only the **most recent release** is supported.
Fixes are shipped as a new release; we do not backport to older pre-1.0
versions. Once 1.0 ships this table will name the supported minor lines
(see docs/release.md, "Support window").

## Reporting a vulnerability

**Please do not open a public issue, pull request, or discussion for a
suspected vulnerability.**

Report it privately through GitHub's private vulnerability reporting:

1. Go to <https://github.com/animus-db/animus-db/security/advisories/new>
   (repository **Security** tab, **Report a vulnerability**).
2. Describe the issue, the affected version or commit, reproduction steps (a
   deterministic-simulation seed or a script is ideal), and the impact you
   expect.

This opens a private advisory visible only to you and the maintainers. If you
cannot use GitHub for any reason, open a minimal public issue that says only
"I have a security report and need a private channel" (no details) and a
maintainer will arrange one.

## What to expect

These are targets, not contractual SLAs, for a small maintainer team:

| Step | Target |
|------|--------|
| Acknowledgement of your report | within 3 business days |
| Initial assessment (severity, affected versions, accept/decline) | within 7 days |
| Status updates while a fix is in progress | at least every 14 days |
| Fix released and advisory published | within 90 days of the report; sooner for high severity |

We coordinate disclosure with you: we will agree a publication date, credit you
in the advisory (unless you prefer to remain anonymous), and request a CVE
through the GitHub advisory when the issue warrants one. If a report is
declined as not a vulnerability, we will explain why.

## Scope

In scope: the `animusd` node, the `animus` CLI, the `animus-operator`
controller, their container images, and release artifacts. Examples: a
remotely triggerable crash or data-corruption bug, authentication or
authorization bypass (SigV4 gate, credential catalog, TLS handling), disclosure
of data at rest or in transit, and supply-chain weaknesses in our release
process.

Out of scope: findings that require access the documented deployment model
already treats as trusted (for example the unauthenticated admin port, which
is documented as trusted-network-only, ADR 0020), denial of service by
resource exhaustion from an authenticated, authorized client, and issues in
unreleased experimental branches.

## Verifying what you run

Release binaries and container images are signed with
[cosign](https://docs.sigstore.dev/) keyless signing and carry SBOM and
build-provenance attestations. See docs/release.md, "Verifying a release",
for the exact commands.
