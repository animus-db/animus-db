# A `#[cfg(test)]`-gated field or method is invisible to an external `tests/*.rs` integration binary — a test-only injection point needs a genuinely `pub` hook

Building S-05 PR 1's `dynamo_export.rs` e2e suite, the natural instinct was
to reuse the crate's existing `#[cfg(test)] test_ctx` pattern (`animusd`'s
`Node` already carries one, gated behind `#[cfg(test)]`, for other
in-crate test needs) to inject a fake S3 store factory into a running
`Node`. That doesn't work for a file under `tests/`: each file there
compiles as its own separate integration-test crate linked against the
*library* crate built without `--cfg test` (only the harness binary itself
gets `cfg(test)`), so any item gated `#[cfg(test)]` in the library simply
doesn't exist from an integration test's point of view — not a visibility
error, a "no such field" compile error that looks like a typo until you
remember the two are different compilation units. The fix was a genuinely
public, always-compiled method whose only real-world purpose is a test
injection point: `Node::set_export_store_factory(&self, factory:
ExportStoreFactory)`, storing the factory behind `Arc<Mutex<..>>` so
swapping it in place is visible to every already-cloned per-connection
`ClientCtx` sharing that `Arc`. **General form**: when a `tests/*.rs` file
needs to inject or override library-internal state, `#[cfg(test)]` is not
available to you at all — the hook must be unconditionally compiled (and
named/documented as a test-only knob in its own doc comment so a reader
doesn't mistake it for a real runtime feature), not merely `pub(crate)`
widened.
