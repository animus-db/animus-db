# `Simulator::crash`+`restart` is not a stand-in for a real process restart — a "does recovery actually rebuild from disk" test needs `stop` + a fresh constructor (`animus-control`'s `control_corpus.rs`, PR③)

`animus-sim`'s `Simulator::crash` mutes a node (drops its un-synced disk +
volatile inbox, keeps its tasks alive but silent) and `restart` un-mutes and
re-arms those SAME still-live tasks — the node's `RaftCore`, driver loop,
and (for `animus-control`) ADR 0038's async apply task never actually stop
running; they just stop being able to send/receive for a while. This is the
right primitive for "the node was briefly unreachable/froze," and it's what
every pre-existing `LeaderKill`/`FollowerKill`-shaped nemesis in this
repo's corpora uses. It is the WRONG primitive for proving a genuine
crash-recovery code path — anything gated on "a fresh process just started
and has to rebuild its in-memory state from durable disk" (here:
`meta_apply_loop`'s `mirror::rebuild_metadata_from_engine` call plus
reseeding its `engine_applied` watermark from the engine's own
`syskv::applied_index_key()`, rather than trusting `core.last_applied()`)
never runs at all under crash+restart, because the in-memory state that
code path exists to rebuild was never actually destroyed. Proving that path
needs `Simulator::stop` (removes the node's tasks entirely — genuinely
nothing left running under that node id) followed by constructing a BRAND
NEW driver/node that reopens the SAME retained durable-engine handle
(mirroring `tests/restart.rs`'s pattern: keep the `MemoryEngine` alive in a
variable outside the node so a later `RaftNode::start` can re-clone it,
exactly like a real disk surviving a process exit). **General lesson: when
a fault nemesis is meant to exercise "recovery from durable state after a
restart," check whether the simulator primitive it uses actually destroys
the in-memory state the recovery code is supposed to rebuild — a
crash-and-resume primitive that merely mutes/re-arms the same live task
will make the test pass for the wrong reason (nothing to recover, so
nothing can prove the recovery path works) rather than for the right one.**

A second, sharper gotcha discovered building this nemesis: `Simulator::stop`
does **not** clear a `crashed` flag a prior `Simulator::crash` on the same
node id set — the two are independent pieces of state (`crashed: BTreeSet`
vs. simply having no owned tasks). A cell that ever crashes a node and then
later `stop`s + reconstructs it on the same id, without an intervening
`Simulator::restart` (which is what actually clears `crashed`), ends up with
a fresh `RaftNode` whose every outbound send is silently swallowed forever
by the stale mute — with no panic, no error, just an isolated node that
looks alive but never converges, i.e. exactly the shape of bug this session
already burned time on once before with a different composition (see the
sibling entry on issue #421 above). The fix is a fixed compose order —
`crash; stop; restart` (the `restart` call clears the mute even though
there are no tasks left for it to re-arm) — *before* constructing the fresh
node. This repo's `control_corpus.rs` has no cell that currently composes
`StopRestart` with a prior crash on the same id, but the defensive order is
applied unconditionally in `Group::stop_node` anyway, since it costs
nothing and rules the entire hazard class out by construction rather than
by "don't do that" convention.
