# A witness point that reads back engine state is structurally weaker than one that scans committed log entries, and a uniform clock-skew knob can never expose a differential-skew bug (2026-09-09, issue #804)

A real multi-node `ProdEnv` cluster hard-panicked `assert_ts_monotonic`
("did not strictly exceed the last applied ... the witnessing chain is
broken") under ordinary client load — no fault injection, no crash, no
corruption. ADR 0018 §2's witnessing chain lists four fold-in points, and
they look equivalent on a skim: WAL recovery, every received
`AppendEntries`, snapshot install, group start. They are not. The first
two (`witness_append_entries`, WAL replay) scan committed **log entries**
directly, via `command_ts`, which returns a command's `ts` unconditionally
— whether or not that entry's own apply wrote anything. The other two
instead read back `StorageEngine::latest_version()` — the engine's own
highest **written** MVCC version. Those two views coincide only under an
assumption nobody had stated because it happened to hold for a long time:
that every committed, applied entry writes a row at its own `ts`. CAS and
condition semantics deliberately violate it by design — a `Cas` whose
`expected` never matches, a condition-failed `KindBatch`/`KindEval`, an
aborted transaction all commit a real, monotonicity-checked `ts` that
`latest_version()` never moves for. Once compaction truncates such an
entry out of the WAL, the log-scanning witnesses can no longer see it
either, and the two engine-reading witnesses — snapshot install and group
start — silently undercount from then on.

**Lesson (a), the general one**: when auditing every entry point of a
monotonicity/ordering invariant, the entry points are not automatically
equivalent just because they are "witnessing the same thing" in prose.
Ask, for each one, "does this read back what was *decided*, or what was
*written*?" — a CAS/condition/OCC mechanism that can decide "no-op" is
exactly the kind of thing that makes those two diverge. A marker/watermark
mechanism that itself lives on the "written" side (`ceiling.rs`'s durable
`ReadCeiling` marker, the model this fix generalized into `hwm.rs`) is a
cheap, proven way to pull a "written" read back into agreement with the
"decided" one, in place of auditing (and maintaining, forever) a "does
every apply arm remember to bump the version" invariant by hand.

**Lesson (b)**: this crate's clock-skew fault injection
(`Simulator::set_clock_skew_for`, exercised by e.g.
`animus-test/tests/txn_serializable.rs`) had always applied the **same**
skew to every replica of a group. That knob's existence gave false
confidence that clock-skew-adjacent bugs were covered — but a uniform
skew can only ever test a group's clock reading unrealistically fast/slow
relative to a *wall* clock, never one replica's clock reading low
**relative to what the group has actually committed**, which is exactly
what this bug needed to reproduce (each `ProdEnv` node samples its own
independent `Instant::now()` base at bind, so real clusters see
sub-2-second *differential* skew between nodes routinely). The regression
(`crates/animus-cp-data/tests/hlc_differential_skew.rs`) assigns each
replica of a 3-node group its own, distinct, fixed skew — deliberately
*after* the initial leader election, since skew is a pure read-side
offset with no bearing on that race, so assigning it up front would need
to guess the eventual winner instead of reading it off after the fact.
**A fault-injection knob's own coverage is bounded by how it is invoked,
not by its mere existence** — before trusting "we have a clock-skew
corpus" as evidence a clock-skew class of bug is covered, check whether
every call site drives the *shape* of skew (uniform vs. differential,
here) the specific bug needs, not just skew of some kind.

Fixed two ways, one per weak witness site (`crates/animus-cp-data/src/
lib.rs`, `hwm.rs`, `codec.rs`): the `InstallSnapshot` image now carries
the sender's own running high-water mark in a header field (codec version
`29`), since the image's per-kind row scan deliberately excludes every
engine-global marker and so could never have carried it as a row; the
local-restart path gets a durable per-tablet marker (`hwm.rs`,
`ceiling.rs`'s own mechanism generalized to every ts-bearing entry) written
at compaction time, which durably raises the engine's global high-water
mark by the same `merge`-into-`manifest.max_version` path any real row
write already uses — so the pre-existing group-start witness needed no
code change at all. See ADR 0018's 2026-09-09 amendment and this crate's
own `CLAUDE.md` (Key invariants, "Witnessing" bullet) for the full
account.

**Correction, same day (post-review): the first pass of both fixes above
closed the log-order-witnessing half but left two narrower gaps a review
caught before merge — both now closed, and both are the same lesson
twice.** (1) The `InstallSnapshot` install side only ever `hlc.witness`ed
the header value into the receiving replica's **in-memory** `Hlc` — never
durably. That is fine for the live process, but a receiver that itself
restarts before its own next compaction has nothing to re-derive the mark
from: `install_engine_image` now ALSO `merge`s the receiver's own
`hwm.rs` marker, in the identical `merge_batch` as the installed rows
(same crash-atomicity argument issue #554 already established for the
applied-watermark marker riding alongside). (2) The image-building
`engine_image` call only ever passed the apply task's raw, in-memory
`max_applied_ts` as the header — which resets to `None` on every restart
of THIS task, "including after a restart" (`apply_and_compact`'s own
doc), until the first qualifying entry it processes *this lifetime*.
A sender that restarted since its own last compaction, then is
immediately asked for a snapshot before applying anything new, shipped
`None` even though its own durable `hwm.rs` marker (written at that
earlier compaction) had already raised `storage.latest_version()` to the
true mark — the fix folds `hlc::unpack(storage.latest_version())` into
the header alongside the running `max_applied_ts`, `max`-ing the two.
**The general lesson**: witnessing something in memory and writing it
durably to the specific replica that will need to re-derive it later are
two different claims, and a value sourced from a per-task counter that is
documented to reset to `None` on every restart can never be trusted alone
as "the truth" — it must always be maxed against whatever the same
task's own durable engine state already proves, exactly the same
"resettable counter vs. durable engine read" shape lesson (a) above
already names, just one level deeper (a sender's OWN header-building
step, not only the two original witness points). Regression:
`crates/animus-cp-data/tests/hlc_differential_skew.rs`'s
`receiver_installs_the_durable_high_water_mark_not_just_the_rows` and
`sender_restart_with_nothing_applied_since_still_ships_the_true_high_
water_mark`, both confirmed red with their own fix hunk reverted and
green with it restored.
