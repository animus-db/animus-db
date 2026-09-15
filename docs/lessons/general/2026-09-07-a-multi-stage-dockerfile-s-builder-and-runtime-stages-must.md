# A multi-stage Dockerfile's builder and runtime stages must name the same Debian release, and the publish workflow must start the image, not just build it (S-07e's e2e-kind-webhook leg, the `runtime-operator` image)

**What happened.** The `runtime-operator` image had been built and
published since S-07a without anything ever *running* it: the operator e2e
ran the controller out-of-cluster with `cargo run`. The first in-cluster
use (ADR 0070's `E2E_WEBHOOK=1` leg deploying `--webhook-only`) died on
startup with `/lib/x86_64-linux-gnu/libc.so.6: version GLIBC_2.39 not
found (required by animus-operator)`. The builder stage was the bare
`rust:1.96` tag, which follows Debian's current stable (trixie, glibc
2.41); both runtime stages are `debian:bookworm-slim` (glibc 2.36). A
binary links against the builder's glibc and binds the newest symbol
version available there; run it on an older glibc and the dynamic loader
refuses before `main`. `animusd`, built in the very same `cargo build`
invocation, kept working only because it happens not to use any symbol
newer than 2.36 — so "the other image from this Dockerfile works" proved
nothing.

**Why it generalizes.** Two rules. (1) A floating base tag like
`rust:<version>` is not a pin: its Debian release moves when Debian's does,
and the failure surfaces only in whichever binary first touches a newer
symbol, possibly months after the tag moved. Builder and runtime must name
the same release explicitly (`rust:1.96-bookworm` with
`debian:bookworm-slim`) and be bumped together. (2) A CI job that builds
and publishes an image without starting it is not a gate for the image;
`docker build` succeeding says nothing about the loader. The publish
workflow now loads each image locally and runs it far enough to print
something only our code prints (`animusd --help`'s usage line, `animus-
operator crd`'s CRD document) before the push step, so a glibc skew, a
missing shared library, or a wrong entrypoint fails the workflow instead
of the first user.
