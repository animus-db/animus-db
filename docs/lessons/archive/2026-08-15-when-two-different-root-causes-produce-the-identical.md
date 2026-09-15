# When two different root causes produce the identical observable absence, don't try to reconstruct which one happened from the remaining state — record an explicit signal at the moment the distinction is still known.

**When two different root causes produce the identical observable absence,
don't try to reconstruct which one happened from the remaining state —
record an explicit signal at the moment the distinction is still known.**
Wiring tablet merge (ADR 0033, the data-plane dual of ADR 0028's split), a
per-node reconciler observing "a tablet I used to host vanished from the
replicated tablet map" must react completely differently depending on
*why*: merged into a sibling (tear the group down, but the data is still
live — a survivor now serves it on the same shared engine, so **never
erase**) vs. the whole table dropped (tear down **and erase** — nothing is
left to serve that range). Both produce the exact same absence from
`Metadata.tablets`, and the tempting inference — "does some other tablet's
range now cover mine, so it must be a merge survivor" — is unsound: two
different tables' still-unsplit tablets can have byte-identical default
ranges (`KeyRange::whole()`), and by the time the reconciler is deciding
what to do, the vanished tablet's own table identity is gone from view too
(it's not in the map anymore), so there's no way to disambiguate a
same-table survivor from an unrelated table's coincidentally-matching
tablet. The fix was a tiny, explicit, **permanently-retained** replicated
marker (`Metadata::merged_tablets: BTreeSet<TabletId>`, ADR 0033) set at
the one moment the distinction is unambiguous (the `MergeTablets` apply
itself, which knows exactly which tablet it just absorbed) — cheap because
tablet ids are never reused (so the marker never needs pruning and can
never resurrect a wrong decision for a later id), and correct by
construction instead of by inference. **General check when a planner reacts
to "X disappeared" from a coarser view: are there multiple legitimate
reasons X can disappear that demand different actions, and if so, is there
actually enough information left in the coarser view at decision time to
tell them apart — or does the distinguishing fact need to be captured
explicitly, closer to where it was still known, even at the cost of a
small permanent marker?** (`animus-control::Metadata::merged_tablets`;
`animus-cp-data::host::{HostAction::Absorb, MetadataView::merged}`.)
