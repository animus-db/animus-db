# A sim's fault primitive must be at least as harsh as the real-world event it claims to model — never more forgiving

`animus-sim`'s `Simulator::stop(node)` is documented as modelling a real
process exit, and its own cleanup (drop the node's tasks, its volatile
inbox, its un-synced disk buffer) reads as complete. It wasn't: it never
touched the shared `(time, seq)` timeline, so a `Deliver` event already
scheduled for that node with a future `deliver_at` survived the call. When
that event's deadline arrived, `fire_event` found the target neither
`crashed` nor partitioned (`stop` sets neither) and pushed the envelope into
whatever inbox existed under that node id at that moment — including a
**fresh incarnation's** inbox constructed after the stop, since
`Simulator::env` only does `.entry(..).or_default()` rather than checking
for a still-registered generation. A real process exit drops its open TCP
connections; this made the simulated one strictly *more forgiving*, so
every test using `stop` to model cold-restart recovery (the `StopRestart`
nemesis, `crash`+`stop`+reconstruct) was quietly weaker than its own claim
whenever a message happened to be in flight at the stop instant — the
failure mode this primitive existed to catch (a restarted process observing
state that shouldn't have survived) could not reproduce, because the sim's
own fault model was leaking correctness the real world doesn't grant.

**General rule for any deterministic-sim fault primitive** (crash, stop,
partition, pause, disk-tear — this repo's whole `animus-sim` vocabulary):
before trusting a primitive to stand in for a real-world failure mode, trace
through *every* piece of shared state a message or event in flight could be
sitting in at the moment the primitive fires — not just the state the
primitive's own doc lists as "volatile" (inbox, disk buffer), but anything
else with its own independent lifetime (a scheduled timeline entry, a
pending retry, a buffered ack). A primitive that cleans up 90% of what a
real failure destroys is not "close enough" — it silently converts every
test built on it from "proves the recovery path is correct" to "proves the
recovery path is correct assuming nothing was ever in flight," which is a
materially weaker and usually unstated claim. The fix pattern here (a
one-time removal of matching timeline entries at the moment the fault
fires, not a standing mute) generalizes too: prefer "clean up what already
exists right now" over "remember to suppress future state," since the
latter needs its own clearing discipline (see the sibling lesson on `stop`
not clearing `crash`'s `crashed` flag) and is an easy source of the exact
opposite bug — an over-broad mute that silently muzzles a *fresh*
incarnation's own traffic.

See `crates/animus-sim/CLAUDE.md`'s `stop` section and
`crates/animus-sim/tests/stop_semantics.rs` (issue #836) for the concrete
instance.
