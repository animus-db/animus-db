# "Am I already in your committed config?" can never distinguish a genesis race from a genuinely established restart — only real, granted participation can

Found as the actual root cause behind PR #902's own persistent CI failure,
after the `next_deadline()`/resend-timing fix (sibling entry) turned out
insufficient on its own: the boot-time wiped-voter check (issue #667,
`RaftCore::begin_cluster_check`) used `config.contains(&self.id)` as its
one disambiguating signal between "this identity was already an
established voter and its disk was wiped" (refuse) and "this identity has
never had voting rights here before" (safe, an ordinary ADR 0060 join).

## The trap: a genesis config contains every founder from birth, by construction

`config.contains` is a sound signal for an ADR 0060 *runtime* join — a
node added to an already-running cluster genuinely isn't in anyone's
config until the `change_membership` that adds it commits. It is
**unconditionally true, from the very first committed entry onward**, for
every ordinary N-node genesis founder — a genesis config is defined by
listing every founder up front. The moment ANY majority of founders elects
a leader and commits anything (even the leader's own no-op), every other
founder's committed config already names every slower, still-checking
founder too — because it always did. From the slower founder's own point
of view, this is **byte-for-byte indistinguishable** from probing a
genuinely established, long-running cluster whose disk-wiped voter it
happens to be. Waiting for every peer to answer (the sibling "single-peer
evidence" fix, applied earlier this same day) does not help: it only
delays the same false conclusion until every peer has independently
raced ahead, which a real, CPU-starved, staggered `ProdEnv` bring-up does
routinely.

## The fix: ask about real participation, not committee membership

The actual distinguishing fact is not "does your config already know my
name" but "have you ever personally witnessed me cast a real, durable
vote." A node that has never campaigned or voted (which a still-checking
founder structurally cannot do — voting/campaigning is gated on
`!cluster_check_pending`) has sent no peer any evidence of a forgettable
vote, regardless of what its config says about it. `RaftMsg::
ClusterProbeResp` gained `ever_heard_from_prober`, tracked in a new
per-core `heard_from` set marked **only** at the specific events that
represent a genuinely durable, forgettable vote: a candidate's own
self-vote (`handle_request_vote`, unconditional — the candidate voted for
itself the instant it issued the request, regardless of our own grant
decision), a **granted** vote we receive (`handle_vote_resp`, `granted:
true` only), and a peer proving it previously won a real election by
sending `AppendEntries`/`InstallSnapshot` as leader.

## The gotcha inside the fix: a REJECTED vote is not participation, and marking it broke the fix on its own first draft

The first implementation marked `heard_from` broadly — any non-probe
message received from a peer, in one place, at the top of the dispatch
function. This reproduced the exact bug it was meant to fix, on the very
next attempt with a 2-node cluster: node B (still checking) honestly
**rejects** node A's `RequestVote` once A resolves and starts campaigning
first (a still-checking node still answers vote requests, just always with
`granted: false`). That rejection is real wire traffic sent BY B, and the
broad `heard_from` marking counted it as evidence B had "participated" —
so when B later probed A, A's own `heard_from` (built from processing B's
earlier rejection) wrongly reported "yes, I've heard from B before,"
recreating the false "established" verdict. A rejection sets no durable
state on the rejecter's side (`voted_for` is never written for a message
we decline) — it proves nothing forgettable happened, and marking it as
if it did defeated the entire point of the new signal. The fix had to be
narrowed to the three specific sites above, each chosen because it is
exactly where a `voted_for` write (the state a disk wipe can lose) would
have durably happened on the SENDER's side, never on the receiver's mere
act of answering.

## The generalizable rule

When a boolean/set-membership signal is meant to answer "has X ever
mattered here," audit it against every event that could set it and ask,
for each: does this event represent a durable state change on the
SUBJECT's side, or merely a reaction the OBSERVER performed? A broad "any
non-trivial message counts" heuristic will almost always over-count by
including replies, rejections, and other observer-side reactions that
prove nothing about the subject's own durable state — and the failure
mode this produces (a false positive reintroducing the exact bug the
signal exists to prevent) may not surface until the very next adversarial
timing, not the first test run. Prove the narrowed definition with a
scenario built specifically to exercise the excluded case (here: a
still-pending node rejecting an already-resolved peer's vote request) as
well as the included ones, not just the original failing scenario.
