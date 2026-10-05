# Two sim traps found closing out ADR 0073 Phase 2

1. **`SimEnv::metrics()` is a per-(sim, node) sink, not the sink a node exports.**
   In `ProdEnv` they are the same object, so code that sets a metric through
   `env.metrics()` and a scrape that reads the node's aggregated sinks agree; in
   `SimCluster` they do not, and a test that asserts the scraped value sees 0
   while the code "works". Route anything a test must observe through the one
   handle the exporter reads (`ClientCtx::exported_metrics()`), in the writer
   and the reader alike, and assert through the scrape (`SimCluster::metric`).

2. **A simulated old binary must decode the bytes, not trust the emitter's
   classification.** A capped decode that rejects on `required_gate()` can only
   catch emitters whose classification is right, so an "ungated field"
   negative control (a payload the classifier does not know) passes vacuously.
   Let the cap read what the bytes need (`content_gate`) and keep the
   classification as the emitter's separate claim; then the control fails the
   oracle for the right reason, and a mutation that blinds the cap to the field
   fails exactly that control.

Also: a cfg'd enum variant (`Gate::Synthetic`, `cfg(any(test, feature =
"sim-versions"))`) is safe only while no other crate matches the enum
exhaustively, because Cargo features unify across the workspace; check with
`cargo check --workspace --all-targets` before relying on it.
