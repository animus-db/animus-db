# A fixture whose op helper auto-advances virtual time must size per-op cost against the PER-CALL refill, not the nominal configured rate (ADR 0065, W-08 step 3)

`SimCluster::spawn_and_capture` (the helper every `put`/`get`/`delete`/
`scan` call goes through) advances the simulator's virtual clock by up to
`OP_BUDGET` (12s) per call, regardless of how quickly the operation itself
actually resolves — a deliberate design so a route/propose/confirm loop
inside one op call can never itself hang the corpus. This is invisible to
an ordinary op sequence, but it is directly load-bearing for a token-bucket
throttle test: at a naively "obviously enough" rate like 1 unit/sec, a
12-second-per-call refill hands back ~12 units between every single op —
so a test whose per-op cost is anywhere near that (e.g. a ~10-RCU read on a
40 KiB item) never nets any drain at all; `sim_cluster_throttle.rs`'s first
draft of its read-throttle test admitted all 60 of 60 attempted reads with
zero refusals, not because the throttle mechanism was broken but because
the fixture's own per-call refill was outrunning the configured debit. The
fix was sizing the test's own item at 512 KiB (~128 RCU/consistent-read),
clearing the per-call refill by an order of magnitude rather than merely
exceeding the nominal rate on paper; the write-throttle test's own ~100
KiB/~100 WCU item was already, by luck, well clear of the identical
12-WCU/call refill. **General form**: when a fixture's own driving loop
advances a deterministic simulator's clock by a fixed budget per call (for
liveness/hang-prevention reasons unrelated to what the test is actually
proving), any rate-based assertion built on that fixture must size its
per-op cost against that PER-CALL budget, not just against the nominal
rate under test — read the fixture's own op-driving helper before picking
"a rate that should obviously throttle."
