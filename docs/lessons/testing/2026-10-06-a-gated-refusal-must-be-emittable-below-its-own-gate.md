# A "not yet" reply must not be gated on the feature it refuses

**What happened (G-d M3).** `MrecApply`'s response was classed G (`MrecReplication`) as a
whole, including the `Refused { retryable }` variant a receiver sends when its own cluster
version is below the gate. The `SimWorld` receiver test passed because `PeerBridge` calls
the handler directly; the first real-socket test panicked ("gate violation on
client_response"), because the gated encoder refuses (and a debug build asserts on) emitting
a class-G value below the gate. The node that most needs to answer was the one that could not.

**Rule.** For a gated request, the reply that says "this gate is closed here" (and any other
whole-request refusal) is `Base`; only replies that carry the feature's content need the gate.
When the gated-encoder seam is bypassed by a sim shortcut, add one real-framing test per class-G
request/response pair, including the closed-gate path. Same test also found that
`Node::bind`-then-`run_bound_node` never installed config-derived settings (the start half had
no `with_*` call): a new node-local setting needs a test through every entry point, not just
`run_node`.
