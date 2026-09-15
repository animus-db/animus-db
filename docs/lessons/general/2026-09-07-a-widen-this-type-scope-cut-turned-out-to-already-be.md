# A "widen this type" scope cut turned out to already be widened — trace the type before believing an ADR's own prior estimate (ADR 0069 "As-built: cluster store", issue #680)

ADR 0069's PR 2 scope-cut paragraph said encrypting the default `cluster`
segment/backup store would mean "widening `ClusterSegmentStore`'s own
concrete type parameter... a larger, structurally separate change (every
call site that pattern-matches the `Cluster` variant would need to thread
a second generic parameter through `animus-cp-data`'s own
`cluster_segment_store` module)." That estimate was never actually
checked against the type's own definition — `ClusterSegmentStore<E: Env,
S: SegmentStore + Clone + Send + Sync + 'static>` was **already** generic
over its local building block `S`; nothing inside `animus-cp-data` ever
named it concretely to `FsSegmentStore` — only `animusd`'s own two
construction sites (`SegmentStoreHandle::Cluster`/`BackupStoreHandle::
Cluster`'s field type) did. Closing the gap was therefore a single new
`animusd`-local type (`LocalSegmentStore`, a two-arm `Plain`/`Encrypted`
enum delegating `SegmentStore`) occupying that existing parameter — zero
changes to `animus-cp-data`, and `SegmentStoreHandle::Cluster`/
`BackupStoreHandle::Cluster` stayed one variant each (never the
`EncryptedCluster` fourth-variant shape the task's own design guidance
flagged as the less-preferred fallback). General lesson: an ADR's own
"this would be a bigger change" scope-cut reasoning is a snapshot of what
looked true at the time it was written, not a fact to inherit uncritically
into a follow-up — before accepting a documented "needs widening a type
parameter across N call sites" estimate, `grep`/read the type's own
definition first. The actual size of a widening is frequently smaller
than a prior pass estimated, especially when (as here) the type in
question was already built generic for an unrelated reason (here,
`ClusterSegmentStore` being generic over `S` was originally so a `SimEnv`
test could pair it with `SimSegmentStore` — a design decision made for
testability that happened to also make this later encryption task cheap).

A second, narrower lesson from the same change: a node's `Disk`-seam
marker directory (`<node dir>/internal`, PR 1's own check) and its
`SegmentStore`-seam marker directories (`<node dir>/segments`,
`<node dir>/backups`, PR 2's) are **sibling** subdirectories of one
`--dir`, not the same directory — `Disk::list()`'s own non-recursive
listing never sees into a sibling, so the two loud-refusal mechanisms
genuinely operate independently on disk even though one `--encryption-key`
covers both. This mattered for testing the new mechanism in isolation: a
naive attempt to build a "key against an existing plaintext cluster
store" fixture by copying a real node's whole data directory hit the
Disk-seam's own refusal first (a real node that has ever written any
WAL/engine content already carries the Disk-seam's own marker the moment
a key is first configured, since both seams share the same node
lifecycle in the common case) — masking the very check the test meant to
prove. The fix was to construct a target directory carrying **only** the
`segments`/`backups` subdirectory's own content (real objects, or a
copied marker file), with no top-level marker and no `internal/`
content at all — which a `Disk::list()` scan of the (from its own point
of view) genuinely fresh top-level directory sails through regardless,
reaching the new check specifically. Worth remembering for any future
directory-marker-based loud refusal sharing a parent `--dir` with an
existing one: know exactly which subdirectory each check scans before
assuming a "restart with X" test naturally isolates the mechanism you
think it does.
