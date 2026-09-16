# A generic core-level safety fix does not wire itself into every driver that instantiates the core

Found closing issue #900, the CP data plane's own instance of issue #667's
wiped-voter double-vote hazard.

## The trap: "the mechanism already exists generically" is not the same claim as "every consumer calls it"

Issue #667 fixed a P0 Raft safety hazard entirely inside `animus-control`'s
generic, sync `RaftCore<C, S>` (ADR 0009's 2026-09-15 amendments):
`begin_cluster_check`/`handle_cluster_probe`/`handle_cluster_probe_resp`, the
new `cluster_check_pending`/`cluster_check_refused`/`heard_from` state, and
the vote/campaign gating in `start_pre_vote`/`start_election`/
`handle_request_vote`. `animus-cp-data`'s tablet-group node (`RaftKvNode`)
instantiates that exact same generic core with `C = KvCommand` — so it is
tempting to assume the fix "just works" for tablet groups too, and in one
real sense it does: the safety *mechanism itself*, once invoked, behaves
identically regardless of which `Command` type parameterizes the core, with
zero cp-data-specific reimplementation needed. `animus-cp-data::codec`'s
hand-rolled binary `RaftMsg` framing even already had working
`ClusterProbe`/`ClusterProbeResp` encode/decode arms, forced into existence
by `RaftMsg`'s exhaustive match — so cross-crate wire compatibility was
never in question either.

**None of that means the mechanism ever actually engaged.** `RaftCore::
begin_cluster_check` is not called automatically by `RaftCore::new` or by
anything inside the core itself — it is an explicit call a *driver* must
make, at boot, when its own WAL replay comes back empty. `animus-control::
node::drive` makes that call. `animus-cp-data`'s own `drive` function —
despite instantiating the identical `RaftCore<C, S>` — never did. A wiped
tablet-group voter therefore came back as an ordinary, fully-eligible
`RaftCore::new()` follower, exactly as unsafe as a wiped control-plane
voter was before issue #667, even though every generic building block issue
#667 added was already sitting there, unused, one function call away.

## The generalizable rule

When a safety fix lands entirely inside a shared, generic core that several
drivers instantiate, treat "is the mechanism generically correct" and "does
every driver actually invoke it" as two separate questions, and audit the
second one explicitly — grep every driver/boot-path function that
constructs the generic core from an empty/fresh state, not just the one the
original bug report named. A fix that is 100% correct at the core level can
still leave every *other* instantiation exactly as vulnerable as before,
and nothing about "the core already has the fix" will surface that in
review unless someone specifically checks each call site. Here, the check
was cheap once framed this way: `animus-cp-data`'s own `codec.rs` had
already left a comment on its `ClusterProbe`/`ClusterProbeResp` arms
pointing out exactly this gap ("this crate's `RaftKvNode` never calls
`RaftCore::begin_cluster_check`") — the fix was findable by reading a
comment that was already there, not by discovering anything new about the
core.

## A second, narrower lesson: a caller-trusted "this is definitely fresh" flag is a legitimate, narrow bypass — but only when it is provably narrower than the general check

Wiring the check in naively (unconditionally, for every empty-state boot)
would have been *safe* but would have silently regressed a real, separate,
ADR-documented optimization: ADR 0058 Train 2 rung 4's `campaign_immediately`
flag lets the parent's own leader at an in-place split fork campaign
synchronously, before the driver loop ever selects on a timer, to win the
race against the child group's own cold randomized election timeout. Running
the new cluster check unconditionally would gate that synchronous
`campaign_now` call on `cluster_check_pending` (since the check has almost
certainly not resolved yet at that exact synchronous instant), silently
degrading "wins the race against the cold timeout" to "waits out the cold
timeout anyway" on every single split. The fix instead skips the check
specifically when `campaign_immediately` is set, because that flag is
*already*, by construction, a narrower and equally sound freshness proof:
it is set by exactly one caller (`materialize_split_child`), exactly once,
only for a replica that caller has already established is a genuine fork
participant — never settable by a restart. The general rule this
illustrates: when a new, more general safety check would regress an
existing, narrower, already-proven-safe fast path, look for whether the
fast path's own precondition is *itself* sufficient evidence for the thing
the new check exists to establish — if so, bypassing the general check for
that one caller is not a hole, it is reusing evidence that already existed.

## A third, expected (not surprising) side effect: boot-path entropy desync strikes again

Exactly as it did for issue #667's own control-plane fix (see the sibling
entries dated the same day), adding a new `env.next_u64()` draw to a boot
path that previously drew none reshuffles every later random draw in that
`SimEnv` run. One pre-existing fixed-seed test
(`crates/animus-cp-data/tests/read_index.rs::
linearizable_read_succeeds_after_a_full_membership_rotation`) needed its
seed re-pinned as a direct result — not because the fix was wrong, but
because the test's hard-coded voter-removal order silently depended on a
specific node winning the initial election, and the reshuffled entropy
changed which node that was. Any change to a boot path's own message/RNG
shape needs the same treatment given the control-plane fix: re-run every
fixed-seed test that boots a fresh replica, not just the ones that
exercise the new mechanism directly, before concluding the corpus is
unaffected.
