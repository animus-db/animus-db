# An "abandon and forget" exit from a retry loop must still leave the cooldown state a *fresh* attempt would have set — otherwise the resource is eligible again on the very next tick, not after backing off.

**An "abandon and forget" exit from a retry loop must still leave the cooldown state a *fresh* attempt would have set — otherwise the resource is eligible again on the very next tick, not after backing off.** (Found in the pre-ADR-0028 `auto_split_loop` abandon path, since removed; archived in `docs/engineering-lessons-archive.md`.)
