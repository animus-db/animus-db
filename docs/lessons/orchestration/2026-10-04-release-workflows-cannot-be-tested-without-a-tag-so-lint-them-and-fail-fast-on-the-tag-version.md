# A tag-triggered release workflow cannot be exercised on a branch, so lint it and make it fail fast

**Context (R-01 g, release engineering).** `release.yml` and the signing half
of `image.yml` only run on a `v*` tag push, need `id-token: write`, and publish
immutable artifacts (a Sigstore log entry, a GitHub release, GHCR tags). None
of that can be rehearsed on a PR, so the first real run is the test.

**What to do instead of hoping:**

* Run `actionlint` (pip `actionlint-py`) over `.github/workflows/` and parse
  every `fromJSON(...)` matrix literal with a JSON parser; both catch the
  classes of mistake (bad expression, bad matrix shape, wrong context) that
  would otherwise surface as a failed release.
* Make the first job check the invariant the rest depends on: the tag must be
  `v` + the workspace `version` in `Cargo.toml`, before any build or signing
  step. A mismatch should cost seconds, not a published bad release.
* Never reuse or move a release tag to retry; fix forward with the next
  version. State plainly in the PR which behavior is unvalidated until the
  first tag.

**Two non-obvious gotchas found building it:**

* `docker buildx imagetools create` joins per-platform images into a manifest
  list but drops buildkit's per-platform attestation manifests, so
  `provenance`/`sbom` on the per-platform build push are lost. Push by digest
  with those off, then attach SBOM and provenance as attestations (and a
  cosign signature) to the final *index* digest in the merge job.
* Path filters on `on.push` are not evaluated for tag pushes, so adding
  `tags: ["v*"]` beside `paths:` is safe; but a `pull_request` run never has
  registry credentials, so the push, sign and attest steps must be gated on
  `github.event_name != 'pull_request'`.
