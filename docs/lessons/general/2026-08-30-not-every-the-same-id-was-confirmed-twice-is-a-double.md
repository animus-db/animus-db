# Not every "the same id was confirmed twice" is a double-assignment — check whether the racing proposals were content-identical first (`animus-control`'s `control_corpus.rs`, `AllocatorRace`, PR② of the control-corpus stack)

Building `Workload::AllocatorRace`'s invariant #4 (allocator injectivity),
the first draft's `check_allocator_injectivity` had two parts: (1) a
content-aware sampler (`Shared::sample_tablets`) that flags a `TabletId`
observed with two *disagreeing* fingerprints, and (2) a second check that
every `TabletId` a client's own confirm loop reported as applied
(`confirmed_tablet_ids`) was pairwise distinct. Part (2) immediately failed
the fault-free baseline: every `AllocatorRace` client races
`MetaCommand::CreateTablet` for the identical shared table, so every
racer's proposal is byte-identical **except for the candidate tablet id**
(same table name, same range, same replica set). Before any proposal has
committed, several racers legitimately read the same stale
`next_free_tablet_id()` and each proposes with that same candidate id — and
once the tablet that actually lands carries that id, EVERY one of those
racers correctly observes "the tablet that now exists carries my own
candidate id" and calls `confirm_tablet_id`. That is not a bug: there is no
meaningful sense in which "whose literal `CreateTablet` call committed" is
distinguishable when the content besides the id is identical — multiple
racers correctly recognizing the identical, single, real assignment is
expected, not a double-assignment. The fix was to delete part (2) entirely
and rely solely on the content-aware sampler, which is strictly the
stronger and correct check (it only flags a *disagreeing* fingerprint for
the same id, never a repeated agreement). Contrast this with
`BeginSplit`'s own phase of the same workload, where each racer's split key
is deliberately distinct per proposer index — there, a "confirm by content"
check (the child's actual range boundary matching MY split key, not just
presence of my candidate ids) is exactly right, and is what the racing-
proposers lesson above (`won`-vs-`lost` by content, not presence) already
prescribes. **General lesson: when a race's confirm signal is "this
proposal's content == what committed," first ask whether every racer's
proposal *could be* content-identical except for the field the race is
actually about — if so, a raw "confirmed exactly once" assertion over that
field alone is checking a stronger, false property; the real invariant is
"no two DIFFERENT contents were ever attributed to the same identity,"
which only a fingerprint/content comparison (not an occurrence count) can
state correctly.**
