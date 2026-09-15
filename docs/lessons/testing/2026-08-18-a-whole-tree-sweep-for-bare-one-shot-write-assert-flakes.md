# A whole-tree sweep for "bare one-shot write assert" flakes (issue #278 item 7) should grep for an existing local retry helper (`async fn put`/ `put_until_ok`/`put_retry`-shaped) *before* writing new retry logic at each site

**A whole-tree sweep for "bare one-shot write assert" flakes (issue #278
item 7) should grep for an existing local retry helper (`async fn put`/
`put_until_ok`/`put_retry`-shaped) *before* writing new retry logic at each
site** — the same file class (a real-socket `ProdEnv` integration test)
very often already has a hand-rolled bounded-retry helper sitting right
next to one or more still-unprotected raw `ClientRequest::Put`/`PutBatch`/
`Delete` call sites that simply never got routed through it (an omission
from whoever added the later call site, not a missing mechanism) — the fix
is usually "call the existing helper" or "extract the existing inline
retry loop into one," never a parallel reimplementation. The specific
class this sweep targeted: issue #268's fix made the CP data plane's
confirm-loop futility case fail FAST instead of burning a slow timeout,
which turned every bare one-shot write assert across the test tree into a
latent flake under runner contention (a transient `"; retry"`-class error
that used to be masked by a 10s stall now surfaces in milliseconds). The
canonical retry shape (`crates/animusd/tests/split_build.rs::put`): a
bounded-deadline loop retrying on ANY `ClientResponse::Error`, panicking on
a non-`Error` unexpected reply or deadline expiry — puts are idempotent, so
a later read-back is still the real assertion. Two judgment calls that
matter when sweeping: a deliberate negative test (asserting a write IS
refused, e.g. delete-against-a-never-created-table) must stay one-shot; a
test whose whole point is single-shot latency/behavior (e.g.
`cluster_split.rs::single_shot_first_write_through_control_node_succeeds`,
which exists specifically to prove the *forwarder's own* internal
election-wait backoff makes ONE client call succeed with no client-side
retry) must not be loosened into a retry loop, or it stops testing what it
says it tests.
