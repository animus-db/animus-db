# Splitting a peer/address book by role must still satisfy any consumer that legitimately spans roles — enumerate cross-role wiring before assuming "role A's book" and "role B's book" are each other's complement.

**Splitting a peer/address book by role must still satisfy any consumer that
legitimately spans roles — enumerate cross-role wiring before assuming
"role A's book" and "role B's book" are each other's complement.**
Decoupling `animusd`'s single `peer_book()` into `control_peer_book()` (ADR
0035 PR2) surfaced that a **data**-role node's `raftkv` env is not a pure
data-role consumer: `heartbeat_loop` (ADR 0012 failure detection) runs *on*
that env and sends `RaftMsg::Heartbeat` to the **control** ids — so a
future data-only node whose `raftkv` env peer book was installed as
`raftkv_peer_book()` alone would have the control ids simply absent from
its own book, and every heartbeat would have nowhere to route, silently
killing failure detection for the whole data fleet with no error anywhere
(`set_peers` with a missing entry just drops the send — no panic, no log).
The fix is not a new book, just documentation + a test proving it: the
correct book for that env is the **union** (`raftkv_peer_book() ∪
control_peer_book()`, i.e. `peer_book()` itself) — call this out explicitly
in the narrower book's doc comment, and add a unit test that demonstrates
the negative (the narrow book alone lacks the ids a real consumer needs)
before asserting the union has them. General check when splitting a
previously-unified resource by role/tier: for each new narrower view, ask
"does anything that conceptually belongs to the *other* side still need to
read this one" — a cross-cutting concern (heartbeats, tracing, metrics) is
exactly where this hides, because it rides on a role's transport without
being that role's own data. (`animusd::config::{control_peer_book,
raftkv_peer_book}`.)
