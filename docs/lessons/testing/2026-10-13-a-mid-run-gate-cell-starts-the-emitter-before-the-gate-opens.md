# A gate cell proves the interesting thing when the emitter is already waiting

The earlier gate cells (`txn_seal`, `mrec`) check two states: before the finalize the
old behaviour, after it the new one. For a *background* emitter (a loop that reconciles
configuration into `Metadata`) that misses the real failure modes: a loop that proposes
once at start-up and gives up, or one that proposes through the closed gate (a debug
assert, or a wedged older voter). `release4_to_release5_node_capacity_gate` starts the
production loop (`SimClusterHandle::sync_node_capacity`, on the data-only node, whose
view is a mirror) *before* the cluster reaches the gate, keeps it running through a
half-rolled cluster with the finalize refused by name, asserts that nothing was proposed
or applied and the task has not finished, and then lets the finalize open the gate and
asserts the same task completes with the value on every replica.

The negative control (`negative_control_node_capacity_gate_emitted_early`) bypasses the
propose-site gate with `propose_meta_ungated` and requires the capped decode to wedge
exactly the older voter; it proves the oracle (including the new "capacity applied below
version 5" check in `instant_violations`) can fail. Add the new gate's content shapes to
`sim_versions::content_gate` as well, so a mis-classified emitter still trips the cap.
