# Porting a `DRIVER_APPLIED` cutover onto a state machine that ISN'T a unit placeholder is a smaller change than it looks — the generic core needs zero edits.

**Porting a `DRIVER_APPLIED` cutover onto a state machine that ISN'T a
unit placeholder is a smaller change than it looks — the generic core
needs zero edits.** ADR 0038 PR3 flipped `Metadata` (a large, real struct
with actual business logic — the control plane's whole `Metadata::apply`)
to `StateMachine::DRIVER_APPLIED = true`, expecting to need a new unit
placeholder type the way `animus-cp-data`'s `KvState` is one. It didn't:
`RaftCore<C, S>`'s `metadata: S` field is only ever touched by the
trait-impl's `apply()` (never called once `DRIVER_APPLIED`) and the
now-dead `WalRecord::Snapshot { metadata: S, .. }` embedding (a harmless,
never-populated `Metadata::default()` for a `DRIVER_APPLIED` plane, exactly
as trivial as a real unit type) — so `Metadata` doubles as both "the real
struct an external apply task privately owns" and "the harmless generic
`S` parameter satisfying `RaftCore`'s trait bounds," with no new type and
no `raft.rs` edits beyond deleting the now-meaningless
`RaftCore<MetaCommand, Metadata>::{metadata, members, placement_view}`
inherent methods. **When porting a `DRIVER_APPLIED` cutover, check whether
the "real" state machine can just BE the generic `S` before reaching for a
placeholder type — the core's own genericity (ADR 0016) was already built
to make this a no-op.**
