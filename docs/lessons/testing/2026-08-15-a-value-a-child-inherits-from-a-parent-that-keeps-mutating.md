# A value a child inherits from a parent that keeps mutating must be frozen at the inheritance event, never derived live from the parent's current state.

**A value a child inherits from a parent that keeps mutating must be
frozen at the inheritance event, never derived live from the parent's
current state.** (Mechanism — `Metadata::stream_split_basis`, the zero-copy
split's watermark inheritance — deleted in ADR 0050 Train B rung 7:
copy-based split children are born with empty change logs, so no consumer
offset crosses a split at all, the strictly stronger successor invariant.
Full entry archived verbatim in `docs/engineering-lessons-archive.md`.)
