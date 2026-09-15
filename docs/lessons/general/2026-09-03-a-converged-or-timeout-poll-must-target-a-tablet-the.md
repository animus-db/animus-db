# A converged-or-timeout poll must target a tablet the property actually holds for, not "whichever one a lookup happened to return" (2026-09-03, issue #580)

`streams_e2e.rs::auto_split_mid_stream_with_live_consumer_across_every_node`
(D8) panicked once on `main` — "the parent tablet never sealed at least one
shard (timed out after 20s)" — then passed in 8s on the very next run, with
no code change in between. The test picks a `(child, parent)` pair by
scanning the table's currently-active tablets (`tablets_for`, ascending by
`TabletId` — ids are minted monotonically and never reused, ADR 0022) for
the first one that has a `split_lineage` entry, then polls
`meta.stream_shards` for `parent` to show at least one sealed shard.

That poll assumes every split's parent seals something before cutover. It
doesn't: `inplace_split_driver_tick`'s own final seal
(`index_drain::seal_now`, `crates/animusd/src/index_drain.rs`) implements
ADR 0043 §A3's "never seal an empty segment" rule — `Ok(None)` the instant
`pending_changes()` is empty — and `CutoverSplit` proposes regardless of
whether anything got sealed. With a 2048-byte auto-split threshold and 40
items, a **child** minted by the in-place fork inherits data (bytes) but
not pending change-log records, and can legitimately exceed the threshold
and split again (cascade) having received zero *new* writes since its own
birth — such a tablet reaches its own `CutoverSplit` with nothing ever
sealed for it. `tablets_for`'s ascending order normally shields the test
from this (any surviving direct child of the true root sorts below every
later-minted grandchild, so the scan finds a root-parented pair first) —
but if *both* of the root's own direct children happen to cascade away
before the poll runs, every remaining active tablet is a grandchild, and
the scan's first match resolves to an intermediate parent that never sealed
anything. Rare, timing-dependent, and exactly reproduces a pass-then-fail
flip with no code change: whether both children cascade before the lineage
scan's window is a race against the test's own write loop, not against
anything the production code guarantees.

The fix (`crates/animusd/tests/streams_e2e.rs`) targets the property at the
one tablet the test actually knows is safe: the table's original tablet,
captured (`root`, via `tablets_for`) right after `CreateTable` and before
any write — the tablet the byte-threshold trigger fires against, and so
the one tablet in any lineage this run produces that is *guaranteed* to
have taken real writes (and, under `tiny_seal_knobs()`'s `seal_bytes: 1`,
to have sealed them almost immediately) before it ever splits. The
`(child, parent)` scan is untouched — it still picks an arbitrary lineage
pair for the downstream `ParentShardId`/chain-walk checks, which is a
narrower, pre-existing assumption (that whichever `parent` it lands on
happens to have sealed) not the target of this fix; a doubly-cascaded pick
could in principle still trip *those* checks, and is left as a known,
undocumented-until-now edge case rather than folded into this fix's scope.

**General form**: a `await_true`/converged-or-timeout poll that asserts
some property of "a tablet/entity a lookup returned" is only as sound as
the guarantee that the *specific* thing the lookup can return actually has
that property in every reachable state — not just the state the lookup's
author had in mind when writing it. When a selection is itself
data-dependent (here: which lineage entry a `BTreeMap`/`Vec` scan happens to
land on, itself downstream of how a cascade played out), prefer asserting
against a fixed, provably-safe anchor captured before the nondeterminism
begins, over trusting whichever instance a generic "find the first match"
scan returns. This is the same shape as the `HashMap`-vs-`BTreeMap`
determinism rule one level up the stack: an assertion's target must not be
allowed to vary with timing the test doesn't control, or a fixed-deadline
wait becomes a coin flip dressed up as a bug report.

Companion fix, `crates/animusd/src/index_drain.rs`: the swallowed
`tracing::debug!` in this same split-driver arm (`inplace_split_driver_
tick`'s caller, `change_consumer_loop`) is now `tracing::warn!` carrying
the tablet's epoch and state alongside the error — a `debug!` there is
invisible not just at the real `animusd` binary's own default log level
(`otel::init_tracing`'s `RUST_LOG`-less fallback is `"info"`) but *always*
in this crate's own `ProdEnv` integration test binaries, which — checked
for this fix, not assumed — wire up no tracing subscriber at all (only
`main.rs` calls `otel::init_tracing`; no `tests/*.rs` file or `tests/
support` does). So today this warning still won't appear in a `cargo test
-p animusd --test streams_e2e` run's own output either way — `warn!` is
still the right level (it's the correct signal for a genuinely-anomalous
condition, and it costs nothing to be already-correct the day a subscriber
does get wired into these tests, which `docs/engineering-lessons.md`'s own
"a routed operation's error policy must be identical" precedent (issue
#572, immediately above) suggests should probably happen — a separate,
not-yet-scoped follow-up, not fixed here).

**Follow-up, issue #588 (2026-09-04): the "known, undocumented-until-now
edge case" this entry named above (a doubly-cascaded `(child, parent)`
pick tripping the downstream `ParentShardId` check) is what actually
happened next, on `main`, twice.** Not a double-fork of the same parent
(a hypothesis this issue's own triage text initially favored, going as far
as naming the branch after it) — `BeginSplitInPlace`/`CutoverSplit`'s
epoch-CAS + state gate make a second fork of an already-forked-away parent
structurally impossible, confirmed by direct code reading, not just
assumed. Reproduced instead in ~1-in-10 real `ProdEnv` runs with a fully
legitimate, single-fork-per-parent lineage tree: an intermediate tablet
split again having sealed *nothing of its own* (exactly this entry's own
"a child... can legitimately reach its own CutoverSplit with nothing ever
sealed for it" case), so `stream_shard_parent_id` returned `None` for
every one of its descendants — permanently, since `parents_final_epoch`
is frozen once, at cutover, and never revisited. Root-caused with a
diagnostic `eprintln!` of `split_lineage` dropped into a scratch copy of
this exact test (removed before the real fix landed) — the printed tree
showed a clean, well-formed cascade with **no** duplicate parent entries,
which is what closed out the double-fork hypothesis in minutes rather than
hours of re-reading the CAS logic looking for a hole that wasn't there.
Fixed at the source of the derivation, not the test: `Metadata::
stream_shard_parent_id` now walks past a never-sealed intermediate
ancestor to the nearest one that DID seal (`docs/adr/
0058-learner-replicas-in-place-split.md`'s Fork F9 entry has the full
mechanism) — `None` means only "no ancestor anywhere in the chain ever
sealed," never "the immediate parent happens to be one that didn't." The
test itself still needed a companion fix (`streams_e2e.rs`): it used to
assert the child's rendered `ParentShardId` names the *immediate* `parent`
tablet id the lineage scan happened to land on — after the derivation fix,
that is no longer always the same tablet, so the assertion now derives its
own expectation via `meta.stream_shard_parent_id(child, 0)` (the same
function production code calls) rather than assuming it equals
`parent.0`. **General form**: when an accessor derives a value by walking
one hop of a chain and stops the moment that hop has nothing to say, ask
whether "nothing to say" is actually possible mid-chain (not just at the
true end) before trusting a one-hop `?`/early-return to mean "there is no
answer" — a legitimate empty link partway through a chain is a different
fact than a legitimate empty chain, and conflating them stops a derivation
that should recurse.
