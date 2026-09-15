# Partition work by disjoint crate ownership — exactly one owner per shared crate/file.

**Partition work by disjoint crate ownership — exactly one owner per shared
crate/file.** The assembly points (`animusd`, `animus-control`) are
chokepoints; if several agents must touch `animusd`, split by *file*
(`dynamo.rs` / `admin.rs` / `lib.rs`) and expect a small `lib.rs` merge.
