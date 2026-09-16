# A single-caller exemption from a safety gate may not cover every party to the same operation

Found fixing issue #945 (`corpus-deep` red on `inplace_split_reconciler_corpus`,
`heartbeat_batch_corpus`), root-caused to PR #907 (issue #900's tablet-group
boot-time cluster check, `animus-cp-data`).

## The pattern

A new safety gate is ambiguous only because a caller hasn't yet proven
itself safe by construction. One caller already carries a flag that proves
exactly that (`campaign_immediately` — ADR 0058 Train 2 rung 4's
deterministic split-child leader, set **only** for the replica the caller
has already proven, by construction, is the parent's own leader at the
fork). The fix exempts that flag's caller from the gate and stops there.

This is incomplete whenever the "proven safe by construction" premise
actually applies to more than just the one caller that happens to carry the
flag. Here, `materialize_split_child` hosts *every* replica of a split
child from the identical, single, fork-time construction — a brand new
`TabletId` minted once from the parent's own committed entry, so **no**
replica of that child can possibly be an established voter's wiped disk
(the tablet didn't exist before the fork). That safety argument holds for
every replica of the child, not just the one that also happens to campaign
immediately. But only the campaigning replica's own gate got exempted —
every other replica of the same child still ran the full check, and (since
the check also refuses to *grant* a real vote while pending) that blocked
the campaigning replica's own "instant" leadership win behind a probe round
trip, defeating the very optimization the exemption was written for.

## The generalizable rule

When adding an exemption to a new safety/ambiguity gate keyed on a
caller-supplied flag, ask **why** the flag's holder is exempt — usually
"this caller can prove, by construction, that the ambiguous case can't
apply to it." Then check: does that same proof apply to any other party
that participates in the identical construction, just without happening to
carry the flag? If so:

- The gate needs a **second**, independently-set flag/parameter for "this
  is provably safe by construction" that is orthogonal to whatever
  behavioral flag (here, "should I also campaign") triggered the review in
  the first place. Conflating the two (as if "provably safe" and "should
  behave specially" are the same bit) is what causes the gap — they happen
  to coincide for exactly one caller, and that coincidence is what makes
  the bug easy to miss in review and in a shallow-depth test run.
- Every OTHER party sharing the same "proven fresh"/"proven safe" premise
  needs the new flag set too, via its own constructor/call site — grep for
  every caller of the construction the flag's holder is one instance of
  (here: every `RaftKvNode::start_*` call inside the one function,
  `materialize_split_child`, that ever sets `campaign_immediately`).
- A regression test for this class of gap can't just exercise the exempted
  caller in isolation — it needs to exercise the exempted caller **and**
  its non-exempted siblings from the same real operation together, since
  the bug is specifically about how they interact (one grants; one
  refuses).

See `crates/animus-cp-data/CLAUDE.md`'s issue #945 note and ADR 0017's
2026-09-16 amendment for the concrete fix (`skip_cluster_check` split out
from `campaign_immediately`, set for every replica
`materialize_split_child` hosts, not just the campaigning one).
