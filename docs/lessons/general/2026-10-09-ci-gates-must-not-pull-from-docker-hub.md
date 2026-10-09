# CI gates must not pull from Docker Hub

On 2026-10-09 two merge-queue runs of a Markdown-only PR failed `lint` and
`s3-real-endpoint` in a row. Neither failure touched the change: the
`EmbarkStudios/cargo-deny-action@v2` step builds its own Docker image `FROM` a
Docker Hub `rust:alpine` base on every run ("Docker build failed", three
retries, cargo-deny never ran), and `s3-real-endpoint` `docker run`s RustFS
and the AWS CLI from Docker Hub (exit 125, the pull). The same jobs had passed
on the same commit half an hour earlier.

**Why it matters:** a required gate that depends on Docker Hub inherits its
outages and anonymous pull limits, and the merge queue then rejects every PR,
whatever it changes. A re-run does not help while the registry is degraded.

**How to apply:** a required CI job gets its tools as release binaries
(`taiki-e/install-action`, version-pinned) and its service images from a
registry GitHub's runners reach reliably (ghcr.io, ECR Public), pinned to a tag
whose digest matches the Docker Hub one. A Docker-based action
(`runs.using: docker` with `image: Dockerfile`) rebuilds from its base image on
every run, so check an action's `action.yml` before adding it to a gate.
