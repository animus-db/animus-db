# Drive a supervisor CLI against a scripted fake server, and pin the parity by fixture

**Context.** `animus cluster roll` (ADR 0073 P3-B) reads three admin endpoints and must
agree with the server's own `roll.remaining` / roll-health. The server's view builder is
`pub(crate)` in `animusd`, so a CLI test cannot call it, and a real multi-node ProdEnv
cluster is too heavy for a per-push CLI test.

**What worked.** A ~40-line `std::net::TcpListener` fake in `tests/roll_cli.rs` serving
canned bodies shaped exactly like the server's (copy the field names from
`cluster_version_view`, put the server's own `roll.remaining` in the fixture), driven by the
real `animus` binary. It pins exit codes, `--json`, `--timeout`, the poll loop (a handler
that flips from not-ok to ok on the Nth poll) and POST bodies, and the parity assertion is
"the CLI's restart order equals the fixture's `roll.remaining`". The real-listener test
(`admin_real_listener.rs`) then proves the field names against the real endpoints, so the
fixture cannot silently drift from the server.

**Gotchas.** (1) The last node rolled is the *former* control leader, so a `--finalize` that
POSTs to the address it was given gets a 409; resolve the leader's admin address (leader id
from `/admin/raft`, address from `/admin/status` `node_addrs`). The 409 hint is the client
address, not the admin one. (2) When judging "is THIS node done" with a shared state machine,
hold every other node back as old, or an unrelated in-flight node (sorted first by id)
masks the answer. (3) A single-node real cluster cannot hand off control leadership, so
`roll plan` is correctly refused there: assert the refusal, not success.
