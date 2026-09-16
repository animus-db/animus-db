# `!is_leader()` proves this node stopped leading, never that an accepted entry is lost — issue #911

Found root-causing issue #601's own flake (see that PR's — #914's — body
and the 2026-09-15 note on a mechanistic counter still needing an explained
tolerance): 50 real `ProdEnv` runs of a 200-item `BatchWriteItem` showed the
batched phase's own Raft-proposal count was *usually* exactly
`ceil(N/BATCH_WRITE_MAX_ITEMS)`, but occasionally one higher — never on the
single-item path, in 6000+ sampled writes.

## The bug

`ClientCtx::cp_kind_raw_local`'s confirm loop (`crates/animusd/src/
write_path.rs`) probed by value equality alone, and — once
`decide::confirm_wait_is_futile` returned `true` — did one more value check
and gave up with a retryable `"; retry"` error. That predicate's own doc is
explicit about the deal it strikes: its `!is_leader()` clause is safe
*only* because "the accepted entry may yet commit under the new leader (**a
retry is then a harmless idempotent duplicate**)". That premise holds for
every other caller of `confirm_wait_is_futile` in this file, because their
retry re-proposes a plain value with no side effect beyond itself.

It does not hold for a raw `KindBatch`. A second, distinct accepted entry
for the identical logical write mints its own fresh HLC `ts`
(`RaftKvNode::mint_pushed`, called fresh inside `put_kind_batch` on every
propose), and ADR 0049's `materialize_derived` keys every change-log/marker
record by `prefix || hlc::pack(ts) || ordinal`. Two accepted entries for
one client write therefore produce two physically distinct `KIND_CHANGE`
rows — a genuine duplicate DynamoDB Streams / GSI-backfill-drain event for
a write the client made exactly once. The base/LSI rows themselves stay
harmless (last-write-wins on identical bytes), which is exactly why this
shipped invisibly for a long time: nothing about the *served* value ever
looked wrong, only the amplification (a wasted WAL/replicate/apply round,
plus a real downstream event duplication) did — and that only showed up as
an off-by-one **proposal count**, discovered only because an unrelated fix
(#914, closing #601) happened to start asserting on that exact counter.

`!is_leader()` firing does not mean the entry is lost. Raft only ever
truncates a log tail that *conflicts* with a new leader's own log, never
one that matches it, so an entry that already reached a majority (or that
this same node re-reaches once re-elected with an intact log tail) commits
and applies under whoever leads next — and `kind_batch_outcome`/
`local_get_kind` are plain **local** reads that need no leadership at all,
so the very node that just stopped leading can keep watching its own entry
resolve. The value-equality probe this loop used, though, needs the exact
same bytes to still be the *current* value — which fails not only when
this node loses leadership, but also (with no leadership change at all)
whenever a second, unrelated, later write to the identical key legitimately
overwrites the first one's bytes before this loop gets to check. Both are
"my own entry provably applied, but the plain value read says otherwise."

## The fix, and why the naive one doesn't close it

`poll_probe` (the sibling confirm primitive `cp_batch_local`/
`cp_kind_eval_local` already use) closes an *adjacent* false-positive class
this same way: it trusts a recorded `Applied` outcome as a confirm only
when the outcome's own term matches the proposer's `accepted_term`
(`classify_kind_batch_outcome`, issue #334). Wiring `cp_kind_raw_local` to
the identical check is necessary but, on its own, **not sufficient** — a
naive port that still falls through to "declare it superseded" the instant
`confirm_wait_is_futile` fires, exactly mirroring `poll_probe`'s own
structure, changes nothing for the specific fact-combination this bug
lives in: `kind_batch_outcome` is `None` (nothing has been decided at this
index by anyone, anywhere, yet) at the exact moment `!is_leader()` also
fires. `classify_kind_batch_outcome(None, ..)` is `Inconclusive` under
either the old or the new code, so a straight copy-the-pattern fix is a
no-op for the case that actually matters.

The real fix has to change what counts as "futile" for this one caller:
`!is_leader()` alone is no longer treated as proof of loss. Only two
signals are — both leadership-independent, both already derivable locally:
`engine_applied_index` has passed the accepted index with no matching
outcome/value (something *else* already resolved this slot), or a
**different term** is recorded at this exact index (a provable
truncation). "Nothing decided yet" now just keeps waiting, bounded by the
same `CLIENT_TIMEOUT` deadline the loop always had — never a wider one, and
never an unbounded hang, since a genuinely orphaned entry (decided by no
one, ever) still gives up on schedule.

## Reproducing it deterministically was the hard part, and the easy
## construction generalizes further than the bug report did

The literal scenario named in the issue — a transient term bump from a
missed heartbeat deadline, with the entry still recoverable under whoever
leads next — proved empirically very hard to pin in `SimEnv`. Both
`Simulator::pause` (which defers a paused node's own timers *and* every
message addressed to it to one batched resume instant) and a plain
`partition`/`heal` toggle converge atomically in this implementation:
whichever single message first reveals the higher term to the stepped-down
node also, in the same `AppendEntries`, carries full commit-index catch-up
(no backtracking needed, since a *recoverable* scenario means the logs
already match) — so both the buggy and the fixed code see success on the
very next check, and the race window that a real, multi-threaded `ProdEnv`
can hit (a genuine gap between an in-memory role update and a separately-
scheduled async apply task actually catching up under real CPU contention)
never opens up in `SimEnv`'s single-threaded, drain-to-fixpoint event loop.
A sweep of 180 (seed, network-latency, pause-duration) combinations at
10µs polling granularity found zero.

The same false-negative class has a second, much easier door in, though:
`confirm_wait_is_futile`'s *other* clause (`engine_applied_index >=
accepted_index`) fires identically whether the index was decided by a
leadership change or by an entirely unrelated, later write to the same
key — no faults, no leadership change, no timing race required. Two
back-to-back writes to one key on a single, healthy, single-leader group
reproduce the exact fact-combination (my own entry genuinely applied, at
my own term, but the *current* value is someone else's) with full
determinism. The regression test (`cp_kind_raw_local_outcome_confirm_tests`,
`crates/animusd/src/write_path.rs`) uses this door, not the leadership-churn
one the issue itself led with — proving the identical mechanism the fix
actually closes, just via the concurrent-writer instance of it rather than
the term-bump instance, which remains real and code-verified (Raft's own
indirect-commit-of-a-prior-term rule) but not independently pinned by a
`SimEnv` test in this change.

**The generalizable lesson**: when a shared decision helper's own doc names
the specific harm class it accepts as a tradeoff ("a retry here is
harmless" / "this fallback is idempotent" / similar), that acceptance is
scoped to the callers it was written against — a new caller inherits the
helper's *code*, never automatically its *safety argument*. Re-derive
whether the tradeoff still holds for the new call site before reusing the
predicate as-is; if it doesn't, the fix is not "wire in the same identity
check everyone else uses" by rote, but working out which specific ambiguity
that identity check needs to resolve *for this caller* — index-alone vs.
(index, term) closes one shape of ambiguity ("whose entry is this"); it
does not, by itself, close "has anyone decided this index at all yet."
