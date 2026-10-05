# `let _ = op().await` in a spawned startup task turns a bounded timeout into a permanent, silent failure

**Context:** issue #1230. animusd's node self-registration was
`tokio::spawn(async move { let _ = ctx.register_node(..).await; })`.
`register_node` gives up after `SCHEMA_COMMIT_TIMEOUT` (10 s); a pod that
started before the control group had a leader timed out once, the error was
discarded, and the node stayed unregistered (no `node_addrs`, empty labels)
for the life of the process, with nothing logged. It surfaced as a flaky
`e2e-kind-encryption` phase, and was first misdiagnosed as a polling problem
in the test (see `testing/2026-10-04-e2e-pod-ready-does-not-mean-registered.md`).

**Lesson:** a bounded wait is only safe when its caller retries or surfaces
the failure. A one-shot startup announcement must be a retry-until-settled
loop (bounded backoff, `env.sleep`, a log line per failure), living in an
`E: Env`-generic function so a `SimCluster` test can partition the node from
the control quorum for longer than the timeout and poll for convergence. And
when the claim is not idempotent against a removal (`RegisterNode` cannot tell
a fresh claim from a re-registration of a just-decommissioned identity), the
loop must stop on first sight of its own entry in the node's view, never
propose blind forever. When you find one `let _ = ...await;` on a startup
path, grep for its siblings (here `admin_add_member` on the growth and
data-only paths had the same shape).
