# A small pure bridge struct (mirroring `StreamDescription`'s existing precedent) is the right way to hand a distributed-system layer's real state into a pure crate's response encoder, without adding a dependency the crate doesn't already have.

**A small pure bridge struct (mirroring `StreamDescription`'s existing
precedent) is the right way to hand a distributed-system layer's real
state into a pure crate's response encoder, without adding a dependency
the crate doesn't already have.** `wire::TtlDescription` (ADR 0051) is
filled in by `animusd`, which holds the replicated catalog's actual TTL
configuration; `animus-dynamo` never needs `animus_control` types to
render `DescribeTimeToLive`'s JSON. Before inventing a new response-input
shape, check whether an existing sibling (`StreamDescription`,
`index_statuses`'s side-channel) already establishes the pattern — it
usually does, and matching it keeps the crate's encoders uniform.
(`crates/animus-dynamo/src/wire.rs`.)
