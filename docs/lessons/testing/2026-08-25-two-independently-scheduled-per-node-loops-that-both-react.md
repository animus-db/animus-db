# Two independently-scheduled per-node loops that both react to the same underlying event, on different signals with different latencies, can race in a way `SimEnv` won't show you (2026-08-25, ADR 0058 Train 2 rung 3, the `animusd`-level in-place-split driver).

**Two independently-scheduled per-node loops that both react to the same
underlying event, on different signals with different latencies, can race
in a way `SimEnv` won't show you (2026-08-25, ADR 0058 Train 2 rung 3, the
`animusd`-level in-place-split driver).** The in-place split fork
(`KvCommand::SplitTablet`, Stages 1-3) is deliberately **not** a control-plane
commit — the whole point of the in-place design is that minting the two
child Raft groups happens inside the CP-data group itself, off the control
plane's critical path. That left two consumers of "the fork happened" with
very different reaction times: the per-node tablet-host reconciler
(`animus-cp-data::host`) only wakes on `metadata_watch()` (which never fires
for an in-place fork, since nothing changed in `Metadata`) or its own
500ms fallback poll, while the new `animusd`-level cutover driver polls
every 200ms via the existing change-consumer loop and, seeing no vetoes
configured, proposed `CutoverSplit` almost immediately. `CutoverSplit`
deletes the parent's `Metadata` row — the reconciler's only durable memory
that a child tablet is a split product needing `MaterializeSplitChild`
(clone the parent's engine slice, bootstrap with the fork's superset voter
set) rather than an ordinary fresh `Host`. On any replica whose reconciler
hadn't yet run *its own* post-fork tick before the cutover committed, the
child was silently hosted via the ordinary path instead — empty engine,
and (because ordinary non-initial-formation hosting deliberately excludes
self from the voter list, for the "join an already-led group as a quiet
non-voter" case) a **different, wrong voter config per replica** — a
scenario that reads as pure success (no error, no panic) until you notice
the group can no longer elect a leader and every write it took is gone.
`SimEnv` corpora for the fork and for the reconciler each passed in
isolation because each drives only one of the two loops; the bug only
exists in the gap *between* two loops that a `ProdEnv` end-to-end test with
a real paced writer across the fork→cutover window could show. Fixed
entirely on the slow side — no protocol/replicated-command change: the
reconciler fast-polls (50ms) for as long as any tablet cluster-wide carries
an unresolved split intent (a fact every fork participant observes
identically, well before Stage 3 applies, so all replicas speed up
together), and the driver additionally requires a small local
materialization-settle margin *and* direct confirmation via its own
`hosted_groups()` before ever proposing cutover. **General rule**: when two
loops key off "the same underlying fact changed" but one of them learns it
from a slow/optional signal (a poll fallback, an absent notification) and
the other from a fast/mandatory one, audit what the fast side can delete or
invalidate before the slow side ever gets a chance to look — and prove it
with a real-clock test that races them, since a single-loop sim corpus
cannot expose an inter-loop race by construction.
