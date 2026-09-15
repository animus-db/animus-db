# A same-address restart test that rebinds N nodes sequentially multiplies a single node's port-TOCTOU exposure by N — the fix is a bigger retry budget AND fewer nodes, not just one of the two.

**A same-address restart test that rebinds N nodes sequentially multiplies
a single node's port-TOCTOU exposure by N — the fix is a bigger retry
budget AND fewer nodes, not just one of the two.** Building the ADR 0035
PR6 full-split-cluster restart test (stop control trio + data fleet, rebind
every node on its own dir/address, assert recovery), `Address already in
use` recurred under `cargo test --workspace`-level contention even after
raising the single-node restart bound (`support::restart_same_addrs`'s 5s)
to 30s, because this test does the rebind race *five times* in one run
instead of once. First ruled out a lingering-`TIME_WAIT` explanation by
checking the vendored `mio` source directly (`mio::net::TcpListener::bind`
already sets `SO_REUSEADDR`), confirming every failure really was another
process's live bind on that exact port, not a socket-close-ordering bug in
`Node::shutdown()`. Fix was two changes together: raise the per-node bound
further (60s — a full-outage restart is rare enough that patience is
cheap) *and* shrink the fleet this specific test needs to rebind (one data
node instead of two — replication/HA across multiple data nodes is
already covered by other tests in the same file, so this test only needs
to prove "every process comes back", not "multiple data replicas each
come back"). Also added a diagnostic (`ss -ltnp`, best-effort) attached to
the panic message so a *future* recurrence carries forensic evidence
(who holds the port) instead of just "address already in use" — cheap
insurance for a flake that is inherently hard to reproduce on demand.
**General rule: when a bounded-retry mitigation for a known race is
ported into a test that repeats the racy operation multiple times per
run, the exposure is multiplicative — widen the bound AND look for a way
to do the operation fewer times, don't just widen the bound.**
(`animusd/tests/split_cluster.rs::full_split_cluster_restart_recovers_metadata_and_data`.)
**Correction (see the next entry): the "not a socket-close-ordering bug in
`Node::shutdown()`" conclusion above was wrong.** Checking that `mio` sets
`SO_REUSEADDR` only rules out *TIME_WAIT*-reuse contention; it says nothing
about a **live** listener still bound by this *same* process, which is
exactly what a not-yet-unwound aborted task looks like from the outside —
externally indistinguishable from "another process is squatting on it."
The 60s bound + smaller fleet made the symptom rare enough to ship, but the
test kept flaking under `--workspace` load months later until the actual
mechanism below was found and fixed at the source.
