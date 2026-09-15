# Superseded by ADR 0028

**Superseded by ADR 0028**: the `cp-hosted` durable marker this entry
describes is deleted — every tablet on a node now shares one `LsmEngine`,
opened once at node start, so there is no per-tablet "which engines exist
here" question left to answer; a restart just re-discovers every tablet to
host from replicated `Metadata`. Retained for historical record. **Which physical engines a node hosts is *local* durable state — a marker file,
not derivable from replicated `Metadata`.** Re-hosting a node's per-tablet CP
groups after a restart (ADR 0017 #2) can't be driven purely off the replicated
tablet map: that map records placement in **stable base node ids**, not which
co-resident `sib-<id>/db-t{id}-` engines actually exist on *this* node. So
`animusd` writes a small durable `cp-hosted` marker (per `raftkv` env) when it
stands up a split tablet's group, and reads it at start to re-host (recover the
engine + WAL). Bonus: pre-populating the per-node mint-guard (`minted`) from that
marker *before* starting the parent group gives **split crash-idempotency** — the
parent re-applying its committed `Split` on WAL recovery finds the tablet already
hosted and won't mint the sibling twice. (A genuinely-local durable record is fine;
the "prefer a live read of the durable layer" caution is about *stale derived
caches*, which this is not.)
