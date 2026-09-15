# A cached per-node handle derived from replicated state needs an explicit re-sync step for every way that state can change in place — "it was correct when constructed" is not "it stays correct."

**A cached per-node handle derived from replicated state needs an explicit re-sync step for every way that state can change in place — "it was correct when constructed" is not "it stays correct."** (Mechanism superseded by ADR 0031 PR4 — the reconciler's planner now emits an explicit `NarrowScope` action instead of a per-tick patch-up. Archived in `docs/engineering-lessons-archive.md`.)
