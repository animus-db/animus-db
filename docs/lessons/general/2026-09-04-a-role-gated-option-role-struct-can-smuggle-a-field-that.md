# A role-gated `Option<Role>` struct can smuggle a field that was never actually role-specific — check each field's own real dependency before believing the "no role, no handle" framing (W-10, ADR 0043 §A9)

`animusd::DataRole` bundled `segment_store: SegmentStoreHandle`/
`backup_store: BackupStoreHandle` alongside genuinely data-role-specific
fields (`rmw_lock`, `raftkv_metrics`, `base_id`, `stream_seal_knobs`) —
reasonable at the time (ADR 0043's sealer PR built the store handle
alongside the rest of the data-plane assembly a combined/data-only node
already needed), but it meant every downstream doc, and the segment
janitor / backup janitor / backup completion / PITR janitor loops
themselves, inherited a blanket "control-only leader has no data role, so
no `SegmentStoreHandle`" framing that was never actually true at the
*handle*'s own level. Tracing what each handle really needs found nothing
tablet-shaped at all: `SegmentStoreHandle`/`BackupStoreHandle` need only
`ProdEnv` (for the K-replicated `Cluster` variant's own network I/O) and a
`ControlHandle` (to read `Metadata` for placement candidates) — both of
which a control-only node already has. The actual reason a control-only
node was never chosen as a replica *target* is a completely separate,
already-correct mechanism (`animus-control::meta.rs`'s `RegisterNode` apply
arm never claims `Metadata::members` for a `role == "control"`
registration) — nothing about *hosting* the handle depends on *being
hosted at* by anyone else's handle.

The fix was mechanical once the real dependency was traced: promote
`segment_store`/`backup_store` out of the role-gated `Option<DataRole>`
onto `ClientCtx` itself (always provisioned, on every node shape), leaving
`DataRole` holding only the fields that genuinely need a hosted tablet or
the dynamo listener. Four separate background loops' own "control-only
leader can only mark, never reclaim" scope-gap paragraphs — independently
documented in `animusd/CLAUDE.md`, ADR 0043 §A9, and ADR 0059 §3/§9 — all
closed from this one change, because they'd all inherited the same
mis-scoped blocker rather than each having a genuinely independent
dependency on hosting a tablet.

**General form**: when a background loop's own doc says "X can't run
because this node has no data role," check whether X's actual dependency
(an `Env`, a `ControlHandle`, a piece of `Metadata`) is really gated by
that role, or whether it only *lives inside* a role-gated struct for
assembly-time convenience. A field that needs nothing tablet-shaped can
usually be promoted to the top level and provisioned everywhere, closing
every downstream consumer's inherited gap at once — cheaper and more
honest than fixing each consumer's own symptom independently, and a strong
signal to look for whenever more than one unrelated background loop cites
the identical "no data role" reason for a gap.
