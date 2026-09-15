# When several agents edit the same workspace in parallel, a workspace-wide `cargo build`/`clippy` failure needs its error message read, not just its exit code, before deciding whose slice broke it

**When several agents edit the same workspace in parallel, a
workspace-wide `cargo build`/`clippy` failure needs its error message
read, not just its exit code, before deciding whose slice broke it**
(TTL catalog slice, ADR 0051, 2026-08-19). Building `--workspace
--all-targets` while sibling agents have half-finished edits elsewhere
in the tree routinely fails for reasons that have nothing to do with
your own change — e.g. an `error[E0004]: non-exhaustive patterns` on
`animus-dynamo`'s `Operation` enum while implementing a `MetaCommand`
addition in `animus-control` is a different agent's wire-adapter slice
mid-edit, not a fallout from the `MetaCommand`/schema change. Confirm
scope by grepping the error for your own new symbol names and by
building/testing your own crate in isolation (`cargo build -p <crate>
--all-targets`, `cargo test -p <crate>`) as the real gate — that must be
genuinely green — and report the cross-crate failure verbatim rather
than "fixing" code another agent is still writing.
