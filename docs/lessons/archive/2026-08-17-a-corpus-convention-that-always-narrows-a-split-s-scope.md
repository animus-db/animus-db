# A corpus convention that always narrows a split's scope synchronously with (or before) the `SplitTablet` apply can only ever test the FIXED ordering — it structurally cannot express the real race where the local, un-replicated scope-narrow lags the control-plane commit.

**A corpus convention that always narrows a split's scope synchronously
with (or before) the `SplitTablet` apply can only ever test the FIXED
ordering — it structurally cannot express the real race where the local,
un-replicated scope-narrow lags the control-plane commit.** Found
2026-08-15 investigating the D8 duplication flake above (the mirror-image
DUPLICATION direction of #216's own loss bug — same watermark machinery,
opposite symptom): `stream_lineage_corpus.rs`'s `scenario_split_mid_
stream`/`scenario_split_then_parent_seals_first` both call
`n.narrow_scope(..)` on the parent's nodes in the test script itself,
strictly before (or as part of the same step as) applying
`MetaCommand::SplitTablet` — modelling `animus_cp_data::host::HostAction::
NarrowScope` as if it always lands atomically with the control commit
that triggers it. In production it does not: `SplitTablet` commits only
to the control Raft; `RaftKvNode::narrow_scope` is a separate, local,
per-node action the tablet-host reconciler applies only once it next
notices the metadata change (event-driven watch or a 500ms fallback) —
and nothing synchronizes that against a *different* background loop
(`animusd::index_drain::change_consumer_loop`'s 200ms seal tick) reading
the same tablet's still-wide `pending_changes()` in the meantime. The
parent's seal in that window physically captures records that, per the
metadata just committed, already belong to the split-off child; the
child's own first seal — whose watermark is `Metadata::stream_split_
basis`, deliberately frozen *before* that racing parent seal (the exact
mechanism #216 added to fix the loss direction) — has no way to learn the
parent already covered them, and re-seals the same physical records
(never deleted by a seal, only by a later trim) into its own epoch 0:
the same packed HLC delivered twice, caught by `verify_lineage`'s
`seen_hlcs` set once a scenario actually drives the two seals in this
order (`scenario_split_then_parent_reseals_before_scope_narrows`, a new
cell proving the mechanism). General form: when a corpus's own helper
always performs two steps of a protocol in the same call/in a fixed
order because "that's how the test drives it," check whether production
ever lets them land in the *other* order or with a delay between them —
a convention baked into every existing scenario can hide an entire bug
class from hundreds of seeds, exactly as it did here (and as the sibling
`dueling_seals_orphan_hot_range` cell's own two-snapshot scripting had to
be added by hand for the *other* seal-store race the ordinary corpus
couldn't reach either). **Fixed 2026-08-15**: `seal_now`, the hot-trim
arm, and the open-shard hot-read path (`animusd::index_drain`) now fence
their `pending_changes()` candidate set to the tablet's current
metadata-declared range (`in_declared_range`, ADR 0028's write-fence
idiom applied to the seal arm's read side) — see ADR 0043 §A4's own
"split-seal range-fence amendment" for the full as-built writeup.
