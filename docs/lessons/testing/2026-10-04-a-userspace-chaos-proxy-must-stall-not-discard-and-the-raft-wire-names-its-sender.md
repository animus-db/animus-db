# A userspace chaos proxy must stall, never discard, and the Raft wire names its sender

**Context.** R-01 (b) real-cluster chaos (`crates/animusd/tests/chaos_support/proxy.rs`,
`docs/chaos.md`): real `animusd` processes, partitions and delay injected with a Rust TCP proxy
instead of `tc`/netns so it runs unprivileged on a CI runner.

**Lessons.**
- **A cut link must hold bytes, not drop them.** The internal wire is length-prefixed frames
  (`[from_len][from][stream][len][payload]`). Discarding a chunk mid-stream and later resuming
  desynchronises the framing; the receiver then reads garbage lengths. That tests the harness, not
  the database. Model a cut as "stop reading" (TCP backpressure builds, bytes resume in order on
  heal, like packet loss + retransmit) or as an explicit connection reset.
- **A static peer book cannot be redirected through a proxy by config alone.** `peer_sync_loop`
  overlays `Metadata.node_addrs[*].internal` onto the static book every 200 ms, and a node
  self-registers its *own bind address*. The way through without product changes is the existing
  `advertise_host` knob: the node binds `127.0.0.1:P`, advertises `127.0.77.(i+1):P`, and the proxy
  listens on that alias address (all of 127/8 is loopback on Linux). Per-node config files list the
  *other* nodes at their proxy addresses so the pre-registration window is covered too.
- **Per-link granularity is available for free on the Raft port.** The first frame after the
  handshake preamble carries the sender's node id, so the proxy can sniff it per connection (hold
  the first bytes until the sender is known, so a cut link cannot leak its first frame) and cut
  per directed `(src, dst)` pair: one-way and asymmetric partitions work. The `intra` forwarding RPC
  names no sender, so it can only be cut per destination; document that a minority node can still
  forward its own clients outward.
- **Record a trace of every non-200 op, not just the history.** The oracle verdict says *what*
  broke; which node served the op and what the server said (a `ConditionalCheckFailedException` on
  an unconditional `UpdateItem`, a `TransactionCanceledException`) is what let the first finding be
  pinned to aborted transactions within minutes.
- **A chaos run is a distribution, not a pass/fail.** The first violation appeared once in ~12
  smoke runs of the same seed; the harness therefore saves history, events, the op trace and every
  node log on failure, and the docs say a replay re-runs the same *faults*, not the same
  execution (ADR 0003: determinism is `SimEnv`-only).
