# A retry loop keyed on a resource id must recheck the resource still exists — a precondition that only checks its own transient state silently assumes the resource itself is immortal.

**A retry loop keyed on a resource id must recheck the resource still exists — a precondition that only checks its own transient state silently assumes the resource itself is immortal.** (Found in the pre-ADR-0028 `auto_split_loop` pending-retry map, since deleted; archived in `docs/engineering-lessons-archive.md`.)
