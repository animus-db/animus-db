# Before extracting a flagged "untested pure function," check whether it's already a thin call-through to a pure/tested implementation elsewhere.

**Before extracting a flagged "untested pure function," check whether it's
already a thin call-through to a pure/tested implementation elsewhere.**
`next_free_tablet_id` looked like animusd's problem (the audit flagged the
*caller*, `trigger_split`) but the allocator itself was already pure and
unit-tested in `animus-control::Metadata` — nothing to extract, just a
caller that wasn't using it (fixed separately in PR #21). (PR #33.)
