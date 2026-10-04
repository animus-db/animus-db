# Changelog

All notable changes to AnimusDB are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
binary's version follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
as described in [docs/release.md](docs/release.md). On-disk and wire format
versions (ADR 0073) are versioned separately.

No version has been released yet. Each release section is generated from
commit history with `git cliff` (see `cliff.toml` and docs/release.md) and
prepended below at release time; the `release.yml` workflow reuses the same
section as the GitHub release notes.

## [Unreleased]

### Added

- Release engineering: workspace version baseline `0.1.0-alpha.0`,
  `animusd --version` / `animus --version`, tag-driven `release.yml` (multi-arch
  binaries, SBOM, cosign keyless signatures, build provenance), multi-arch
  signed container images with SBOM and provenance attestations,
  `SECURITY.md`, and the release/support policy in `docs/release.md`.
