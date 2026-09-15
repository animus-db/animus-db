# Durable-before-visible: never expose state a crash could lose.

**Durable-before-visible: never expose state a crash could lose.** A node must
not make a committed entry client-visible (readable / ack-returnable) until it is
fsynced. The control plane enforces this with a `durable_index` watermark the
driver advances *after* `env.sync(WAL)`, gating `apply`
(`min(commit_index, durable_index)`) — so `metadata()`, and any proposer waiting
on it, only sees durable state (ADR 0009; mirrors `animus-data` `ack_durability`
and `animus-consensus` `persist_then_ship`). Two consequences worth remembering:
a core/component driven by hand must **simulate the fsync** (advance the
watermark) or its applied state never moves; and gating *follower* visibility on
the follower's own fsync **widens cross-node replication races** — a read on a
follower right after a create on the leader must wait for the definition to
replicate to that node (`await_table_*`), not assume the leader's ack made it
visible everywhere.
