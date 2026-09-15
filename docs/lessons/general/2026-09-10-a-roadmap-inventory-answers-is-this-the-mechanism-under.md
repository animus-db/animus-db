# A roadmap inventory answers "is this the mechanism under investigation" and a ground-truth investigation must separately answer "does this file call that mechanism" — a file can score yes on the second while its own tests are about something else entirely (ADR 0061 rung M opener, `join_data_seed_settings_reach.rs`)

C-13's roadmap entry inventoried four seed/join files; a fifth,
`join_data_seed_settings_reach.rs`, was missing from it even though every
one of its four tests genuinely calls the real join entry points
(`run_node_join`/`run_node_join_with_settings`/`run_node_data_join_with_
settings`) — confirmed by reading each test's own setup helper, not by
its filename. The omission was not a mistake about which files exist; it
was a category confusion between two different questions a roadmap
inventory and a ground-truth opener need to ask separately. A roadmap
inventory, built to size a future rung's *scope*, reasonably asks only
"is this file's own asserted subject the mechanism I'm scoping" — and by
that test, this file's four tests are about per-node knob threading
(shared-WAL layout, `quiesce_after`, encryption-key-at-rest) surviving a
join, not about the join mechanism itself, so it looked out of scope and
got left out. A ground-truth opener, whose job is the complete file
inventory a later PR ladder will draw from, has to ask a second, distinct
question first — "does this file's own code path actually call the
mechanism under investigation" — before it can even reach the first
question at all. This file scores yes on the second question (it belongs
in the ground-truth scope) and mostly no on the first (its own tests
aren't about discovery/claim, so most of them turn out permanent for an
unrelated, real-disk reason once actually converted). The general rule:
when building a rung's ground-truth inventory, grep for every real call
site of the mechanism first, independent of any existing roadmap
inventory's own file list — a roadmap inventory that only answers the
narrower "is this file's own subject the thing I'm scoping" question can
legitimately, silently omit a file that calls the mechanism for an
unrelated reason, and only a full re-grep from source catches it.
