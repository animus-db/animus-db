# A mode flag derived from "this map is non-empty" can be pruned off; use a sticky marker

**Context:** ADR 0073 Phase 2 (P2-A) first defined the version era as
`!Metadata::node_versions.is_empty()`. The apply-corpus test
(`version_apply_corpus`) found the flaw while checking "era-on is monotonic":
`RemoveMember` prunes a node's record, so removing the last reporter turned the
era back off while `cluster_version` stayed above 1 — which would let a Phase 1
binary rejoin and wedge on era-only entity kinds/variants.

**Lesson:** a one-way mode must be derived from a *sticky* marker, never from
the emptiness of a collection other commands prune. The fix reuses an existing
field as the marker: stored `cluster_version` `0`/absent = era off, the first
applied `ReportNodeVersion` sets it to an explicit `1`, nothing resets it. That
keeps era-0 bytes identical (the reason emptiness was tempting) with no new
field. The mirror must then carry the era-on `1` too, or rebuild and delta
replay disagree with direct apply.

**Related generalization:** a per-node attribute stored on a row that a
whole-row-replacing command rewrites (`UpsertMember`) is erased by unrelated
traffic, and a node with no row at all (control-only voters: `node_addrs` only)
is invisible to "every row" checks. Use a separate map and define the required
set as the union of every registry that can name a node.
