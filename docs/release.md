# Releasing AnimusDB

This page is the **mechanism** of a release: how versions are numbered and
bumped, what the release workflows produce, how to verify them, the
supported-platform matrix, and the deprecation policy. The release *policy*
(what 1.0 requires, signing and SBOM commitments, exit criteria) is fixed by
[ADR 0074](adr/0074-production-readiness-exit-criteria.md) and
[production-readiness.md](production-readiness.md); the on-disk and wire
compatibility rules are [ADR 0073](adr/0073-upgrade-compatibility.md). Where
this page and those disagree, they win.

## Two independent version axes

| Axis | What it versions | Where it lives | Bumped when |
|------|------------------|----------------|-------------|
| **Binary version** (SemVer) | The `animusd`, `animus`, `animus-operator` builds and the container images | `[workspace.package] version` in `Cargo.toml`; git tag `vX.Y.Z[-pre]`; `animusd --version` | A release is cut |
| **Format version tags** (ADR 0073) | Each durable or wire format (`control-wal`, `shared-wal`, `lsm-wal`, `raftkv-wal`, `cluster-config`, ...) | A `"v"` field / magic per format, with a golden fixture per version | A format changes, in the PR that changes it, independent of any release |

They never imply one another. A release can ship no format change, and a
format version can bump in a PR long before the release that carries it. What
a release *must* do is satisfy ADR 0073's guarantee: a newer binary reads
everything an older post-baseline binary wrote (every post-baseline version
stays readable forever, and existing fixtures are never edited), enforced by
`scripts/check-format-fixtures.sh` and the upgrade-restart corpora. The
cluster's recorded node `build` string is the binary version
(`CARGO_PKG_VERSION`); it is informational and is not consulted for any
compatibility decision (that is the cluster version and format tags, ADR 0073
Phase 2).

## Binary versioning

AnimusDB follows [Semantic Versioning 2.0.0](https://semver.org/) for the
binary, with pre-1.0 semantics made explicit:

* **Pre-1.0 (`0.y.z`)**. Nothing is promised stable except what ADR 0073
  already promises (durable format readability, and, once Phases 2-3 land, wire
  compatibility and rolling upgrades). Within `0.y`, a **minor** bump (`0.y`
  to `0.(y+1)`) may change CLI flags, config keys, admin routes, metrics,
  and behavior; a **patch** bump (`0.y.z` to `0.y.(z+1)`) is for fixes only.
  A breaking change to anything in the deprecation table below still follows
  that table, so it is announced a release ahead even pre-1.0 where feasible.
* **Pre-release identifiers** (`-alpha.N`, `-beta.N`, `-rc.N`). The current
  baseline is **`0.1.0-alpha.0`**, matching the project's "pre-alpha" status
  (README). Pre-release tags are GitHub *pre-releases*, never get the
  `latest` image tag, and never get a `major.minor` image tag. The `alpha`
  to `beta` transition is gated by the exit criteria in ADR 0074; do not bump
  to `-beta.0` by hand.
* **1.0.0 and after**. Once 1.0.0 ships: **major** = an incompatible change to
  a public interface (DynamoDB wire behavior, CLI/config/admin API, Kubernetes
  CRD) or dropping support for a previously supported platform; **minor** =
  backwards-compatible features; **patch** = backwards-compatible fixes.
  Formats still follow ADR 0073 regardless (a format change is a new tag plus
  fixture, never a major bump by itself).

Why `0.1.0-alpha.0` rather than keeping `0.0.0`: `0.0.0` is the cargo "never
versioned" placeholder, sorts below every real version, and could not carry a
pre-release meaning; the first real build must be distinguishable. `0.1.0`
rather than `0.0.1` because `0.0.x` conventionally means "every release is
breaking and unrelated", and the `-alpha.N` suffix carries that signal more
precisely. The number is only compared by humans and by package tooling: no
code branches on it (grep for `CARGO_PKG_VERSION`: it is recorded as the node's
`build` string and printed by `--version`).

All workspace crates inherit the version (`version.workspace = true`), so one
edit in `Cargo.toml` bumps everything. The operator image's CRD/example
manifests under `deploy/` reference the `:latest` image tag (a moving tag);
pin to a release tag or digest for anything you care about.

## Changelog

`CHANGELOG.md` is [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
format. The `Unreleased` section is regenerated from commit history by
[git-cliff](https://git-cliff.org/) using `cliff.toml`:

```sh
git cliff --unreleased                                # preview the next section
git cliff --unreleased --tag vX.Y.Z --prepend CHANGELOG.md   # at release time
```

`cliff.toml` understands both Conventional Commit prefixes (`feat(scope):`,
`fix(scope):`) and the plain imperative subjects older commits use; merge
commits and `docs`/`test`/`ci`/`build`/`chore` commits are omitted. Hand-edit
the generated section for clarity before committing (it is a changelog, not a
log dump): call out breaking changes, deprecations, and any format-version
bumps explicitly. The `release.yml` workflow reuses `git cliff --latest` as
the GitHub release notes.

## Release checklist

1. **Green `main`.** All per-push gates, plus the latest nightly
   `corpus-deep.yml` run, are green (root `CLAUDE.md`, "Green is an
   invariant"). A release is never cut from a red `main`.
2. **Decide the version** per the rules above. Confirm the format-compat story:
   if any durable/wire format changed since the last release, its ADR 0073
   checklist (new tag, fixture, legacy decoder) is complete and
   `scripts/check-format-fixtures.sh` passes.
3. **Release PR**: bump `[workspace.package] version` in `Cargo.toml`;
   run `cargo build --workspace` (regenerates `Cargo.lock`, never edit it by
   hand) and `cargo deny check`; prepend the changelog section
   (`git cliff --unreleased --tag vX.Y.Z --prepend CHANGELOG.md`, then edit);
   update the supported-platform matrix below if it changed; update
   `deploy/operator/` image tags if you pin them. Merge it (DCO sign-off
   required, no AI-attribution trailers).
4. **Tag** the merge commit: `git tag -s vX.Y.Z[-pre] -m "vX.Y.Z[-pre]"` and
   push it. The tag must equal `v` + the `Cargo.toml` version; `release.yml`
   fails fast otherwise.
5. **Watch the workflows**: `release.yml` (binaries, SBOM, signatures,
   provenance, GitHub release) and `image.yml` (multi-arch images, cosign
   signature, SBOM and provenance attestations). Both are triggered by the
   same tag.
6. **Verify** the release as a consumer would (next section), on a machine
   with no access to the build.
7. **Announce**: update the website/README status text if the maturity claim
   changed (the `website/` pages are part of the documentation); if the
   release carries a deprecation or format bump, say so in the notes.
8. If anything went wrong *after* publishing: do not move or delete the tag.
   Fix forward with the next patch/pre-release; mark a bad GitHub release as a
   pre-release or edit its notes, and publish a GitHub security advisory if it
   is a vulnerability ([SECURITY.md](../SECURITY.md)).

## What a release produces

On a `vX.Y.Z[-pre]` tag push:

**`release.yml`** (`.github/workflows/release.yml`)

* `animusdb-<version>-linux-{amd64,arm64}.tar.gz`, each containing `animusd`,
  `animus`, `LICENSE`, `README.md`. Built with `cargo build --release
  --locked` inside `rust:1.96-bookworm` (the Dockerfile's builder image, so
  the glibc floor is 2.36, matching the container runtime stage). amd64 builds
  on `ubuntu-latest`, arm64 natively on `ubuntu-24.04-arm` (no emulation).
* `animusdb-<version>-animusd.cdx.json` and `...-animus.cdx.json`: CycloneDX
  1.5 dependency SBOMs generated by `cargo-cyclonedx` from the committed
  `Cargo.lock`.
* `SHA256SUMS` over the tarballs and SBOMs.
* A cosign keyless signature bundle (`<file>.sigstore.json`) for every file
  above.
* A GitHub build-provenance attestation (`actions/attest-build-provenance`)
  for the tarballs and `SHA256SUMS`.
* A GitHub release (marked pre-release when the version has a `-` suffix) whose
  notes are the changelog section.

**`image.yml`** (`.github/workflows/image.yml`)

* `ghcr.io/animus-db/animusd` and `ghcr.io/animus-db/animus-operator`, as
  multi-arch (`linux/amd64`, `linux/arm64`) manifest lists. arm64 is built
  only on tag pushes; `main` pushes remain amd64-only.
* Tags: `vX.Y.Z[-pre]`, `X.Y.Z[-pre]`, `X.Y` (final releases only), `sha-<full
  sha>`, and `latest` on `main` only. **Pin by digest** for anything that
  matters: tags are mutable, signatures and attestations are bound to the
  digest.
* A cosign keyless signature on the manifest-list digest, a CycloneDX SBOM
  attestation (`anchore/sbom-action` + `actions/attest-sbom`), and a build
  provenance attestation (`actions/attest-build-provenance`), all pushed to the
  registry beside the image.

Actions are pinned by major-version tag, consistent with the other workflows
in the repo. Pinning third-party actions to full commit SHAs (with Dependabot
updates) is tracked as part of the supply-chain hardening in ADR 0074.

## Verifying a release

The signing identity is the release workflow, recorded in the Sigstore
transparency log; there is no key to distribute. Replace `TAG` with the tag
(for example `v0.1.0-alpha.0`).

```sh
# Binaries: verify the signature bundle against the workflow identity.
cosign verify-blob \
  --bundle animusdb-VERSION-linux-amd64.tar.gz.sigstore.json \
  --certificate-identity-regexp '^https://github.com/animus-db/animus-db/\.github/workflows/release\.yml@refs/tags/TAG$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  animusdb-VERSION-linux-amd64.tar.gz
sha256sum -c SHA256SUMS --ignore-missing

# Binaries: GitHub provenance attestation.
gh attestation verify animusdb-VERSION-linux-amd64.tar.gz --repo animus-db/animus-db

# Images: resolve the digest, then verify signature and attestations.
digest=$(docker buildx imagetools inspect ghcr.io/animus-db/animusd:TAG \
          --format '{{json .Manifest}}' | jq -r .digest)
cosign verify "ghcr.io/animus-db/animusd@${digest}" \
  --certificate-identity-regexp '^https://github.com/animus-db/animus-db/\.github/workflows/image\.yml@refs/tags/TAG$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
gh attestation verify "oci://ghcr.io/animus-db/animusd@${digest}" --repo animus-db/animus-db
```

The same applies to `animus-operator`. A mismatched identity, an unsigned
digest, or a signature that does not appear in the transparency log means
**do not run it**: report it per [SECURITY.md](../SECURITY.md).

## Deprecation policy

Pre-1.0, the goal is that no change surprises an operator who reads the release
notes. The table says how each surface is allowed to change.

| Surface | Compatibility promise | How a change is made |
|---------|----------------------|----------------------|
| Durable formats (WALs, snapshots, manifests, config `"v"`, ...) | **Forever readable** by newer binaries (ADR 0073). Old decoders and fixtures are never deleted. | A new version tag + new fixture + legacy decoder in the PR that changes it. There is no deprecation window because there is no removal. Writing an old version may stop; reading it never does. |
| Wire formats (internal Raft/network, client protocol, handshake) | Compatible across the supported upgrade path **once** ADR 0073 Phase 2/3 land; until then only whole-cluster stop, upgrade, restart is supported. | Behind the replicated cluster version / feature gate (Phase 2). A wire change before then is allowed but must be called out in the release notes. |
| DynamoDB wire behavior (ADR 0006, 0072) | Follows AWS-defined behavior; deviations are documented in `website/compatibility.html`. | Behavior that was wrong is fixed in a release with a changelog entry. New operations are additive. Service limits are compiled-in and AWS-faithful, with no unleashed mode (ADR 0072). |
| CLI flags, config file keys, admin/HTTP routes, metric names, `AnimusCluster` CRD fields | Best effort pre-1.0; stable once 1.0 ships. | Deprecate for **at least one minor release** (pre-1.0: one minor, post-1.0: one minor and at least 90 days): keep the old name working, log a deprecation warning at use, document it in the release notes and `CHANGELOG.md` under "Deprecated". Remove in a later minor (pre-1.0) or the next major (post-1.0), listed under "Removed". A rename is an add + deprecate, not a replace. |
| Supported platforms (table below) | Dropping a platform is announced one release ahead. | Changelog "Removed" entry; after 1.0 only in a major release. |

An emergency removal for a security reason may skip the window; the advisory
and release notes say so.

## Supported platforms

What is stated below is what the project builds, tests, and will take bug
reports for. "Tested" names the CI that exercises it; anything else is
best effort.

| Dimension | Supported | Notes |
|-----------|-----------|-------|
| Operating system | **Linux** (glibc) | Release binaries and images target glibc 2.36 (Debian 12 "bookworm") or newer. Other OSes (macOS, Windows) are not supported for running a node; the simulator-based test suites are developer-run on Linux. musl/Alpine is not supported (no musl builds are produced). |
| Kernel | Linux **5.10 or newer** | The code uses no kernel features beyond ordinary POSIX file and socket calls (`fsync`, `fdatasync`-class durability, `rename`, `O_APPEND`, TCP). 5.10 is the oldest LTS line still maintained when this was written; CI runs on current GitHub-hosted Ubuntu kernels. Older kernels may work but are untested. |
| CPU architecture | `x86_64` (`linux/amd64`), `aarch64` (`linux/arm64`) | Both build and are smoke-started natively in CI on tag builds. The deterministic-simulation corpora and the `kind` e2e run on amd64 only; arm64 has not yet had soak or chaos testing. |
| Container runtime | Images based on `debian:bookworm-slim`, non-root user | `animusd` writes only to its mounted data directory. |
| Data filesystem | Local **ext4** or **xfs** on a block device, mounted read-write | See "Durability requirements" below. Other local journaling filesystems may work; they are untested. |
| Not supported for the data directory | NFS, SMB, FUSE/object-store mounts, `tmpfs`/RAM disks (data is not durable), overlayfs upper layers, and any device or hypervisor setting that acknowledges writes before they are durable (write cache without flush/FUA support) | Use a persistent volume, not the container's writable layer. |
| Kubernetes (operator, ADR 0060) | **1.32, 1.33, 1.34** | Exercised by the `kind` e2e: the default node image (currently v1.34, tied to `KIND_VERSION`) on every operator/deploy change, and v1.33 and v1.32 via `e2e-kind-k8s-compat` on pushes to `main`. Older versions are untested. `kind` is the only distribution tested; managed distributions (EKS, GKE, AKS, OpenShift) are expected to work but are not covered. |

### Durability requirements

The storage engine's contract (ADR 0004, ADR 0008) is "an ack means fsynced",
and the production `Disk` implementation (`animus-env`, `ProdEnv`) relies on
exactly the following from the filesystem and device; a platform that cannot
provide them is unsupported:

* **`fsync(2)` on a file** durably persists the file's bytes (`File::sync_all`
  after the write; WAL group commit issues one per round).
* **`fsync(2)` on the containing directory** durably persists a *namespace*
  change (file creation, `rename`). `ProdEnv` fsyncs the whole parent chain up
  to the data directory after creating a file and after an atomic replace
  (`sync_parents`); POSIX does not guarantee a created or renamed entry
  survives power loss without it. ext4 and xfs honor this.
* **Atomic `rename(2)`** within the data directory (used for manifest and
  whole-file replacement: write temp, fsync, rename, fsync directory).
* **Appends** are ordered (`O_APPEND`) and a torn tail after power loss is
  detected and handled by the WAL formats (sync markers, ADR 0073 amendments);
  the filesystem must not reorder an fsynced append before earlier fsynced
  data.
* **The device honors flush.** Disks, RAID controllers and virtual disks must
  not acknowledge `fsync` from a volatile cache. This cannot be checked by the
  software; cloud network block volumes (EBS, PD, Azure Disk) and local NVMe
  with power-loss protection are the intended targets.

Mount options or shims that weaken these (`nobarrier`, `eatmydata`-style
fsync suppression) are unsupported.

## Support window

Pre-1.0, only the most recent release is supported (see
[SECURITY.md](../SECURITY.md)); fixes ship as a new release, not a backport.
A longer support window (named supported minor lines, overlap during
upgrades) is part of the 1.0 criteria, and ADR 0073's "every post-baseline
version stays readable forever" bounds what an upgrade can break regardless.
