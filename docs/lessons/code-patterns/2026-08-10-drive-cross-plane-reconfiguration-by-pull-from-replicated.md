# Drive cross-plane reconfiguration by *pull from replicated state*, not a new push command — it keeps the dependency edge one-way and the seam testable.

**Drive cross-plane reconfiguration by *pull from replicated state*, not a new
push command — it keeps the dependency edge one-way and the seam testable.**
Wiring the control plane to reconfigure a per-tablet Raft KV group on a node
failure (ADR 0017 C), a control→data "reconfigure now" message would have forced
`animus-control` to depend on `animus-cp-data` (a cycle — data already depends on
control for `RaftCore`) and to track each group's leader. Instead the decision
already lives in replicated `Metadata` (the placement reconciler's epoch-CAS), so
each group **leader pulls** its tablet's desired voter set and reconfigures
*itself* (`reconfigure_step` + `spawn_reconfigure_loop`) — no reverse dependency,
no leader-reporting needed for the trigger, and the data side takes the metadata
source as a **closure** (`Fn() -> Option<BTreeSet<NodeId>>`) so the crate stays
decoupled from the control-plane driver type. Mirrors the proven `reconcile_loop`
split: decision pure + elsewhere, timing in the loop. Reconfigure toward a target
**one single-server step per tick** (the `change_membership` contract), letting a
multi-server move converge over successive ticks rather than failing.
