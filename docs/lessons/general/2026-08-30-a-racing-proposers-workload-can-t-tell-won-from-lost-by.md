# A racing-proposers workload can't tell "won" from "lost" by presence alone — it has to confirm by content (`animus-control`'s `control_corpus.rs`, ADR 0061 rung B1 sibling)

Building `control_corpus.rs` — a new seed-depth corpus for `animus-control`'s
own machinery (the ADR 0038 apply task, the schema-catalog exclusivity
guarantee), modeled on `raftkv_linearizable.rs`'s harness shape — the
schema-race workload's first draft had each racer's confirm loop treat "the
table now exists" as its own success signal. That's the exact
`ProposeResult::Accepted`-isn't-apply-time-truth trap the entry above already
names, but with an extra twist specific to a **race**: here `Accepted` isn't
even ambiguous about *whether* the command took effect (a single proposer's
own straggler command, `CreateTablet`-fixture-style) — it's ambiguous about
*whose* content took effect, since `CreateTableSchema` rejects outright on an
existing name (first-committer-wins, not idempotent-on-identical the way
`RegisterNode`'s CAS is) and TWO different proposers can each see "yes, a
schema for this table now exists" as true. A confirm loop that stops
retrying the instant presence flips true will, for the losing racer, log a
false "I won" — exactly the durability check's "this confirmed effect must
survive" assertion firing on content that was never actually this
proposer's own. The fix: read the table's *actual* schema back and compare
it, structurally, against the exact value this proposer proposed —
`Some(existing) if *existing == schema => won`, `Some(_) => lost, stop
retrying (nothing left to retry against a name that already belongs to
someone else)`, `None => keep trying`. General lesson: **when a workload
races N proposers for one identity and the state machine's own accept rule
is "first-committer-wins, reject the rest" rather than idempotent-on-match,
"does the effect exist" is the wrong confirm predicate — it has to be "does
the effect that exists match MINE," or a losing proposer misreports itself
as a winner and a durability check built on that misreport is checking a
claim nobody should have made.**
