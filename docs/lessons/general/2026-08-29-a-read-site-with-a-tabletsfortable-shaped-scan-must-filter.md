# A read site with a `tablets_for_table`-shaped scan must filter to the current authoritative owner explicitly (ADR 0059/0050, `BeginBackup` pinning)

`BeginBackup`'s apply arm derived its `pinned_tablets` list from
`self.tablets_for_table(table)` with no state filter — reasonable-looking,
since for an in-place-split table (ADR 0058, the default) every row
`tablets_for_table` returns really is the current authoritative owner of
its slice of the key range. It stops being true the moment a *copy-based*
split (ADR 0050, still selectable via `--split-mode copy`) is mid-flight:
`BeginSplit` mints both children as `Building` rows in `meta.tablets`
immediately, long before `CutoverSplit` ever flips authority to them, and
the parent stays `Splitting` — still serving every read/write — for the
whole build/tail window. A `tablets_for_table` scan during that window
returns THREE rows covering one range at once, and a caller that doesn't
know to filter treats the `Building` children as if they were as
authoritative as the parent: pinning (or otherwise processing) all three
double-counts the range and points at rows that are both unroutable
(`topology::tablet_for_key` excludes `Building`) and an incomplete copy.
The general rule this generalizes to: any new read site built on
`tablets_for_table` (or an equivalent whole-table tablet scan) must decide,
explicitly, which `TabletState`s are "the current authoritative owner" for
its purpose — for split lineage that's `Active`/`Splitting`, never
`Building` — rather than assuming the scan itself already excludes
not-yet-live rows. The existing `is_relayable_command`/`cp_serve_forwarded`
"grep every gating match site" lesson above is the write-side sibling of
this same failure mode: a set-returning primitive whose membership changes
mid-lifecycle needs every caller to state its own filter, because the
primitive can't know which callers only want the settled members.
