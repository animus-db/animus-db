# Enforce a log entry's feature gate where it is proposed, not where it is sent

ADR 0073 Phase 2 wants every emit site to refuse a value whose feature gate is
closed. The tempting place for Raft traffic is the send site: compute
`msg.required_gate()` (for `AppendEntries`, the join of its entries' gates) and
refuse if closed. That is unsound, for three reasons that all come from "what the
sender's `ClusterFeatures` says" being a view of the **applied** state:

1. A leader's applied view lags its log. The entry being replicated may be newer
   than anything the handle has seen.
2. The era-start `ReportNodeVersion` entries are, by design, proposed (and
   shipped) while `Gate::Era` is still closed; they open it only when they apply.
3. A new leader resends entries an earlier leader proposed under a gate that was
   open then. Gates only ever open, so the entry is legitimate, but the new
   leader's own handle may not have caught up.

So the send site checks only the message **envelope** (`RaftMsg::envelope_gate`,
`KvWire::envelope_gate`), and entry gates are enforced where the entry is created:
the one `propose` choke point (`propose_gated` in `animus-control`, `gated_propose`
in `animus-cp-data`). The one intended exception (era start) is a separately named
function, not a flag, so a grep finds every bypass.

Two details that also bit: a proposer that reacts to the applied cache can race the
handle that the apply task feeds just after publishing it, so the closed-gate slow
path re-reads the cache before refusing; and the handle's `update` must be
monotonic because two feeders race and an older view must never close a gate.

How to catch it: `animus-control/tests/it/gate_enforcement.rs` runs the whole era
start / upkeep / finalize path with `debug_assert!` live and asserts zero violations.
