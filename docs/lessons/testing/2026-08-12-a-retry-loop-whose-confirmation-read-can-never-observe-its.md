# A retry loop whose confirmation read can never observe its own success is a resurrection cannon.

**A retry loop whose confirmation read can never observe its own success is
a resurrection cannon.** The same investigation's production half:
`register_node`'s propose-then-confirm loop confirmed via
`metadata_fresh()`, which on a growth/non-voting node structurally never
advances (ADR 0030: its local Raft log doesn't move) — so every growth
node's one-shot self-registration re-proposed an already-committed
`RegisterNode` blindly for the whole `SCHEMA_COMMIT_TIMEOUT`, and a
drain+remove landing inside that window let a stale re-propose recreate
the just-removed member (apply can't tell a stale duplicate from a fresh
claim), which a live heartbeat then promoted straight back to `Active`.
Two generalizable checks: (a) for every propose-and-await confirmation,
ask "can *this caller's* read path ever observe the effect?" — a
confirmation source that is correct for one node shape (voter) can be
structurally blind on another (growth mirror); (b) an idempotent-looking
re-propose is not idempotent across an intervening *delete* — retry loops
for claim-style commands must stop as soon as any read shows the claim
ever existed, not only when the freshest read does. (`animusd::
register_node`'s `effective_metadata()` fallback.)
