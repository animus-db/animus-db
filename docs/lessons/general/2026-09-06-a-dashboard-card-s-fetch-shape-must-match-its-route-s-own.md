# A dashboard card's fetch shape must match its route's OWN gating, not the previous route's precedent (docs/roadmap.md U-07, `GET /admin/ttl`)

`GET /admin/backup-store` (the first U-07 route) is control-plane-leader-
gated — the backup janitor only ever runs on the current control leader,
so every other node's own `JanitorProgress` is honestly `Idle` forever.
`dashboard_core.js::loadAll()` correctly fetches it once against `SEED`
(this console's own node), the same "one shared, cluster-wide answer"
shape `backups`/`restores` already use, with a comment explaining exactly
why a per-node fan-out would be pointless there.

`GET /admin/ttl` (the second route, this PR) is a different shape
entirely: the TTL reaper runs on **every** node, self-gated per tablet
(`TtlScanHost::led_tablets`), so each node's own `TtlReaperProgress` is a
genuinely different, independently meaningful answer — which tablets it
leads, what it has deleted, its own resume cursor. Copying the first
route's SEED-only fetch here would have been the wrong precedent to
follow: the card would have silently shown only the console's own node's
reaper activity as if it spoke for the whole cluster, with no error and no
signal that anything was missing (`loadAll()`'s `.catch(() => null)`
degrades gracefully per node, but a single SEED fetch has no such
per-node granularity to degrade *into* in the first place). The fix was to
add `/admin/ttl` to the *existing per-node* `Promise.all` array
(`dashboard_core.js`'s own `nodes.map(async (addr) => {...})` fan-out that
already fetches `config`/`raft`/`raftkv`/`txns`/`health`/`metrics` per
node) rather than a second single fetch, and to render one row per
reachable node instead of one shared card.

The generalizable rule: before wiring a new admin route's data into the
dashboard, check that route's own **gating** (control-plane-leader-only,
per-tablet-leader-only, or genuinely node-local/always-answered) against
whichever *fetch shape* — single SEED fetch vs. per-node fan-out — the
most recently added, superficially similar route happens to use. The two
remaining U-07 routes this PR's own commit didn't touch make the same
choice differently again: `/admin/gc` (`segment_janitor_loop`) is
control-plane-leader-only like the backup janitor, so it wants the SEED-
only shape; `/admin/segment-store` (placement per shard, already inside
`ClusterSegmentStore`) is genuinely per-node local state, so it wants the
per-node fan-out shape like this one. Never assume the previous PR's own
`STATE.<field>` idiom is the template to copy — re-derive the fetch shape
from the route's own semantics every time.
