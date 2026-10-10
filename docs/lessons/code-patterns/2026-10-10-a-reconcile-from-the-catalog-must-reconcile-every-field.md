# A "sync from the authoritative source" helper must sync every field, not just the one it was written for

**Issue #1245.** After `DeleteTable` + `CreateTable` of the same name with a
different key schema, every node that had already registered the old table
failed `GetItem`/`PutItem` with ``missing key attribute `id` `` forever.

**Why it happened.** `SchemaRegistry::sync_indexes` is the per-request
reconcile of a node's process-local registry against the replicated catalog.
It was written to resync *indexes*, so for an already-registered table it
replaced only the index set and ignored the incoming key `schema`. The first
registration won for the life of the process.

**What to do.** When a cache is reconciled from an authoritative source, the
reconcile must overwrite every field the source owns (here: key schema, and
the legacy `sort_key_optional` flag that only applies to undeclared tables),
and its test must change a field other than the one it was named for. A
name that is reusable (drop then re-create) is the case that exposes it; pin
it with a SimCluster test that touches the old entry on every node before the
re-create (`sim_cluster_dynamo_recreate_schema.rs`).
