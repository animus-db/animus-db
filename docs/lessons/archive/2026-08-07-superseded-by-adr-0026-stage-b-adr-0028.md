# Superseded by ADR 0026 Stage B / ADR 0028

**Superseded by ADR 0026 Stage B / ADR 0028**: a tablet's CP group member id
is now simply its base `raftkv` id (stream-addressed, not a derived
`NodeId`), so the base↔member translation this entry describes no longer
exists. Retained for historical record. **Keep the replicated tablet map in stable base node ids; translate to per-tablet
group member ids at the edge.** A tablet's Raft *group member ids* differ from the
node's base id (a split tablet uses `base + tablet*STRIDE` so co-resident groups
get distinct inboxes), but failure-detection and placement speak **base ids**. So
`Metadata.tablets[t].replicas` stays base ids, and the data-plane reconfigure loop
translates with one function (`cp_members_for`) so its `desired` set matches the
running group's `config()` exactly — no spurious reconfigure churn, and no need to
reconcile the map to derived ids. The bootstrap tablet is the identity case
(member == base); only split tablets derive.
