# A clean `git merge` can leave two adjacent `let metrics = ...` bindings that silently shadow shared state — run the tests, not just the compiler

Merging `origin/main` (which hoisted `let metrics = MetricsHandle::recording()`
above `spawn_accept` in `ProdEnv::bind_with_tls_and_key`) into a branch that
had added its own identical `let metrics = ...` just before `spawn_pump`
auto-merged with no conflict markers. It compiled and passed clippy: the
second binding just shadowed the first, so `Inner` stored a different
metrics sink than the one the accept loop recorded into.
`prod::tests::accept_refuses_*` (asserting `NetworkHandshakeRefused`
incremented) failed, three tests, and only there.

After any merge touching a constructor both sides edited, grep the merged
function for duplicate bindings of the same name (`grep -n "let metrics"`)
and run the crate's tests before trusting a conflict-free merge. Shadowing
of a `let` is never a compiler or clippy error here.
