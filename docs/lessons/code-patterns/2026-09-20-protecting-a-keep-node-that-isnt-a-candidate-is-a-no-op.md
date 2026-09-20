# Protecting a "keep this node" decision only works if the node is also in the candidate pool

Found implementing issue #928's fully general fix: a healthy replica's
member merely marked `Down` by a failure-detector false positive (a GC
pause, a scheduler hiccup — never an actual failure) had its replica
evicted and a full rebuild triggered on the very next `reconcile_loop` tick,
because `reconcile_placement` fed the fresh `Active`-only candidate pool
straight to `replan_repair` every time, with nothing to distinguish "this
member has been gone for a while" from "this member missed one heartbeat."

## The trap: a side-channel "protect this id" set looks like it should work

The obvious-looking fix is a set of ids to protect, threaded past
`replan_repair` as extra context: "here's the desired replica set, and here
are the ids you must not drop even if they don't look eligible." That
doesn't work with `animus_placement::choose` (the shared core of
`select_replicas`/`replan`/`replan_repair`), and the reason generalizes past
this one function.

`choose(eligible, must_keep, policy)` seeds its output from `must_keep`
**only for ids that are also present in `eligible`** — the candidate/domain
map it was actually handed:

```rust
for node in must_keep {
    if chosen.len() >= rf { break; }
    if let Some(domain) = domain_of.get(node) {   // <- must be IN eligible
        chosen.push(node.clone());
        ...
    }
}
```

A `must_keep` id absent from `eligible` is silently skipped — not an error,
not a special case, just quietly dropped as if it had never been asked for.
So passing a "protect node C" instruction through any side channel that
isn't *also* a candidate entry does nothing at all: `choose` never sees a
reason to prefer it over any other candidate, because as far as it's
concerned, C was never a candidate in the first place. `replan`/
`replan_repair` build their own `must_keep` this same way (a filter over
`current` against the eligible pool), so the same trap applies one level up
too: keeping a node's *id* in some bookkeeping structure buys nothing unless
that same node also has a `Candidate` entry in the list actually passed to
`choose`.

**The generalizable rule**: when a decision function's contract is
"choose from *this* candidate pool," a "keep/protect this one anyway"
requirement has to be satisfied by **putting a candidate entry for it in
that same pool**, not by a parallel data structure the decision function
never reads. Before wiring a protection mechanism through any layered
selection/scoring/planning function, check where its own "is this thing
even eligible" gate lives, and confirm the protected identity survives that
gate — a keep-set that never intersects the eligible set is a silent no-op,
and unlike most no-ops it won't error or log anything; it will simply look
like the feature was never implemented once tested against real input.

## The fix: augment the candidate pool, per call site, not the identity set

`reconcile_placement` (`animus-control`) now builds its candidate list per
tablet: the shared `active_candidates(members)` (unchanged, still `Active`
only) plus a fresh `Candidate` for each of *that tablet's own* current
replicas named in the driver's `recently_down` set (a member observed
`Down` for less than `node::REPAIR_DWELL`). Passing an augmented candidate
list — not a side-channel keep-id set — is what makes `replan_repair`'s own
`must_keep` computation (itself derived from `current ∩ eligible`) actually
retain the protected member.

**A second, independent trap this same fix had to avoid: over-eager
protection recruiting the protected node elsewhere.** Once you're willing to
inject a "not really eligible, but protect it" candidate into a pool, the
tempting shortcut is to add it to the *shared* candidate list every tablet's
own repair call reads from — one line, looks harmless. It isn't: a member
merely dwelling (Down, not yet past the repair threshold) would then become
available as a *fresh placement destination* for some completely unrelated,
under-replicated tablet it has never hosted — recruiting a suspect node into
new duty is a much worse failure mode than the one being fixed. The
augmentation has to be scoped to exactly "is this candidate already a
replica of the tablet under consideration right now," computed fresh per
call, never hoisted into the pool every caller shares. Any protection
mechanism shaped as "add X to the eligible set so it's kept" needs the same
scoping question asked explicitly: kept *where*, and nowhere else it
wasn't already.

## The general pattern this is one instance of: suspect vs. dead

The underlying shape — a cheap, fast liveness signal (a failure detector's
`Down` transition, on the order of a few heartbeat intervals) driving an
expensive, disruptive action (evicting a replica and triggering a full
rebuild) — recurs anywhere a system reacts to liveness at all. The fix here
is the same Cockroach/TiKV-shaped split ADR 0062 §2's directed-Placing phase
already used for an identical false-positive class one layer up
(`SPLIT_PLACING_RETARGET_DWELL`/`SPLIT_PLACING_RETARGET_DWELL_ACHIEVED`):
keep the fast signal (`Down`) exactly as fast and cheap as it needs to be
for every OTHER consumer (a dashboard, an admin read, the detector's own
logic), and gate only the one expensive *consumer* action behind a second,
slower, independently-tracked "has this been true continuously for long
enough to act on" threshold. Don't widen the fast signal's own timeout to
satisfy the slow consumer — that blunts every fast consumer that legitimately
wanted the quick answer. When reviewing a new consumer of any fast liveness
signal that takes an expensive, hard-to-undo action on it, ask explicitly:
does this action need its own dwell, separate from the signal's own
timeout, and is that dwell state properly scoped (per-subject, reset on a
recovery observation, cleared on a driver takeover) rather than reusing
whatever timing state the signal's own detector happens to carry?
