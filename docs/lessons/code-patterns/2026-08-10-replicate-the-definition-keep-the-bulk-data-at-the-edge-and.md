# Replicate the *definition*, keep the *bulk data* at the edge — and split them cleanly.

**Replicate the *definition*, keep the *bulk data* at the edge — and split them
cleanly.** When promoting per-process state to the control plane (ADR 0013), move
only the small, must-agree *shape* (e.g. a secondary-index definition: name/keys/
projection) into replicated `Metadata`; leave the large derived *data* (the index
entries) edge-local, rebuilt from observed writes. Make the edge reconcile its
in-memory machinery *from* the replicated definitions (a `sync_indexes`-style
method that preserves entries on an unchanged shape, clears on a changed one) so a
restart recovers the shape from Raft, not local memory. Additive `MetaCommand`
variants + a `#[serde(default)]` new field keep older snapshots/consumers working.
(Found replicating DynamoDB GSI/LSI definitions; `animus-control` `schema.rs`.)
