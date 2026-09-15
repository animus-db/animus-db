# A committed generated manifest is only as current as the last hand run of its generator — pin it with a test that regenerates and compares (`deploy/operator/crd.yaml`, S-04 PR 3, 2026-09-06)

`deploy/operator/crd.yaml` is what `scripts/e2e-kind.sh` and every operator
user apply to a real API server, and it is *generated* from the Rust spec
type by `animus-operator crd`. S-04 PR 3 added `spec.s3` to
`AnimusClusterSpec`, added the field to the e2e's sample cluster, ran every
Rust gate green — and the first real `kind` run failed at `kubectl apply`
with `strict decoding error: unknown field "spec.s3"`, because nobody had
re-run the generator and nothing checked. The unit tests could not catch it:
they exercise the Rust type, never the committed YAML, and the only consumer
of the YAML is an e2e that cannot run in this sandbox. **Rule**: any file
that is checked in *and* generated from code gets a test that regenerates
it in-process and asserts byte equality with the committed copy (here
`crates/animus-operator/tests/crd_manifest_pinned.rs`), with the refresh
command in the assertion message — so a spec change fails `cargo test`
locally, not the one CI job that needs a cluster. The general form: the
gate that guards a generated artifact must live in the same gate set as
the change that invalidates it.
