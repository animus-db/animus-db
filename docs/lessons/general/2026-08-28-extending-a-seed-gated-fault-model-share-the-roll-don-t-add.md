# Extending a seed-gated fault model: share the roll, don't add a parallel one; and a "frozen node" fault needs the primitive it freezes to know who owns it (ADR 0061 rung B2/B3)

Adding `DiskConfig::set_enospc_prob` (an ENOSPC-distinguishable disk fault,
alongside the pre-existing generic `error_prob`) was tempting to implement
as a second, independent RNG roll — draw once for "does the generic error
fire", draw again for "does ENOSPC fire". That would have been wrong even
though both draws would individually be deterministic: it changes the
**number** of RNG draws `inject_disk_fault` makes for a config that only
ever set `error_prob` (now it would draw for ENOSPC too, even at threshold
0 — no, gating each draw on its own threshold avoids that specific case —
but it still changes draw *count* the moment both are configured together,
and more importantly it's a second source of truth for "did a fault fire
at all" that has to agree with the first). The fix that actually preserves
byte-identity for every existing config is **one shared roll, two
buckets**: draw once, and check `roll < enospc_threshold` then `roll <
enospc_threshold + error_threshold`. With the new knob at its default (0),
the combined check degenerates to *exactly* the original single comparison
against `error_threshold` — not merely "the same number of draws", the
identical draw feeding an identical comparison. The general form: when a
new fault variant is meant to be mutually exclusive with an existing one on
the same op, don't reach for "another independent `if` with another
independent draw" — fold it into the existing roll as an additional bucket,
so the old knob's byte-identity proof needs no new argument at all (it's
still "the same draw, the same comparison" — just with a widened, but
default-empty, lower bucket carved out of the front of the range).

Implementing `Simulator::pause(node, dur)` (alive-but-frozen: no timer
fires, no send leaves, no delivery lands, until a bounded resume instant)
surfaced a structural gap: the simulator's `Sleep` future — the thing that
actually schedules a `Timer` event on the shared timeline — had never once
needed to know *which node* it belonged to, because nothing before this had
a reason to treat one node's timers differently from another's. A `pause`
that must defer only the *paused* node's timers needs exactly that
ownership, so `Sleep` gained a `node: NodeId` field (populated from
`SimEnv`'s own `self.node_id`, already on hand at the `Clock::sleep` call
site) and `SimState` gained `timer_owner: BTreeMap<TimerId, NodeId>`,
written once, the first time a `Sleep` actually schedules a timeline entry
(not on every poll — a `sleep` that resolves immediately never needs an
owner, since there's no timer to ever defer). The general lesson: a new
fault that targets "state belonging to one node" can require retrofitting
ownership tracking onto a primitive that has lived fine without it for
years, because nothing about that primitive's own job ever needed to
distinguish nodes — the fault is what introduces the need, not a
pre-existing design gap. Look for this before assuming a new fault is a
pure additive knob: does it need to single out state by node/owner that the
mechanism it's modifying currently treats uniformly?

Two smaller traps hit while building the corpus for these: (1) a spawned
async block that does `out.lock().unwrap().push(some_async_call().await)`
in one expression fails `Send` bound checking (`spawn_task` requires `Send
+ 'static`) because the temporary `MutexGuard` from `.lock()` is deemed
live across the `.await` even though it's logically dropped before the
push's argument is evaluated — split it into `let x = foo().await; guard =
lock(); push(x);` on two statements, the same fix this crate's own
`append`/`sync` doc comments already document for *production* code, which
turns out to bite test code identically. (2) A test asserting "an fsync
that lies still loses the *second* write but the first, genuinely synced
write survives" needs the fault **installed after** the first write's
already-successful sync, not from the start of the simulator run — with
`fsync_lie_prob` set globally from t=0, *every* sync lies, including the
one meant to be the test's trusted baseline, and the failure surfaces as
"everything is gone", easy to misdiagnose as the fault implementation being
too aggressive rather than the test's own setup being wrong. The general
form (already documented elsewhere in this log for disk error injection,
worth restating for a new fault kind): when a test needs a "this part is
real, only the later part is faulty" baseline, install the fault config
*after* the baseline operations run on the same `Simulator`, not before.

Building the ADR 0061 rung B4 failure minimizer surfaced a lesson worth
generalizing beyond this one facility: **an ADR's Consequences section can
promise something that isn't literally buildable as stated, and the fix is
to amend the ADR honestly rather than force-build the literal words.** ADR
0003 promised "shrinking a failure to a minimal seed becomes possible" —
but a `SimEnv` run is a pure function of an *opaque* seed by design (that
opacity is the whole point of a seed), so no seed is "smaller" than
another and there is nothing to shrink *to*. Two different seeds are two
unrelated executions, not a big and a small version of the same one. The
actually-buildable, actually-useful thing was minimizing a failing
scenario's own *parameters* while holding its seed fixed — a different
target than the ADR's own words named. Don't let a stale promise's exact
phrasing constrain the design of the thing that fulfills its *intent*;
when you build the right thing under a different name than the original
promise used, say so explicitly in the doc you're amending (a reader
diffing "shrinking" against what shipped needs the mapping spelled out, not
left to be inferred), rather than silently reinterpreting old prose to
have meant what you built.

A second, more mechanical lesson from the same task: **when validating a
new "fires on failure, does nothing when green" diagnostic path (a
minimizer, an auto-triage tool, a report generator gated on an assertion
failing), a synthetic in-memory predicate proves the algorithm but not the
wiring** — the wiring has to be exercised against a real failure from the
real harness it's attached to, which for an already-green corpus means
either fabricating one temporarily (edit in a forced `result.ok = false`
for one named case, run the real corpus loop with the new tool enabled,
observe, then revert the edit before committing — never leave the forced
failure in the tree) or, better where it fits, constructing a *genuine*
regression: a real scenario whose real, deterministically-computed outcome
is the failure you want to minimize, without needing an actual production
bug or weakening any real assertion. `raftkv_linearizable.rs`'s shrink demo
uses the latter (`read_pct = 100` makes the workload structurally never
write, so `ok_writes == 0` is a genuine simulator-derived fact, not a
fabricated boolean) — it's the same trick `negative_control.rs` uses for
the Elle checker (hand-built histories the checker *must* reject), applied
one layer up: prove a piece of test *infrastructure* against a
deliberately-constructed-but-real case, not a toy stand-in and not a wish
that a production bug will conveniently exist to test against.
