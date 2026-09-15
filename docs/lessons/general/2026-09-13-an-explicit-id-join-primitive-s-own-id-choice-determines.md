# An explicit-id join primitive's own `id` choice determines whether `restart`/`crash` stay usable on it afterward (ADR 0061 rung M, C-13 PR 4)

Building the explicit-`--id` claim branch (`SimCluster::join_via_seed_with_
explicit_id`, the counterpart of `join_via_seed_with_role`'s self-mint arm)
looked at first like a pure superset of the self-mint path — the caller
supplies `id` instead of drawing one from `NodeId::mint`, everything else
(discovery, the single claim-or-collide call, the shared `finish_join`
assembly tail) is identical. It very nearly is, with one load-bearing
exception: `SimCluster::restart`/`crash` both derive `id = nid(node)` from
the node's own INDEX, unconditionally, unrelated to whatever identity that
node actually claimed at join time. A self-minted joiner already violates
this (documented as permanently out of scope by the self-mint arm's own
doc) — its real id is a 22-char base64url string, never `nid(index)`. An
explicit-id joiner is NOT automatically exempt from the same mismatch: an
explicit id is only safe to `restart`/`crash` afterward if the CALLER
happened to choose `id == nid(that node's own about-to-be-assigned index)`
— i.e. an index-derived id, not an arbitrary operator-chosen string. This
is easy to get right by accident (this rung's own new sim scenario always
passes `nid(cluster.node_count())`) and easy to get wrong silently if a
future caller ever passes a free-form explicit id and then tries to
`restart` the result — `restart`'s own `assert_eq!(role, NodeRole::Data,
...)` guard would still pass (role-based, unrelated to id shape), but the
rebuilt node would come up under the WRONG id (`nid(node)`, not whatever
was actually claimed), silently diverging from the real `Metadata` row —
a bug that manifests as a mysteriously "un-promotable" node, not a panic
naming the real cause. **The general rule**: any fixture method that lets
a caller supply an identity FOR a resource whose OTHER methods derive that
same identity a different way (here: from the resource's own positional
index) needs its own doc calling out the exact condition under which the
two derivations agree — "it happens to work" is not the same as "it is
safe to rely on," and the fix is a documented precondition, not a runtime
assertion that would fire too late to explain itself. Confirmed the fix is
adequate here specifically because this rung's own new scenario is the
ONLY caller of the new method that also calls `restart`/`crash` afterward
— a documented precondition, not an enforced one, is only acceptable when
every actual caller at the time of writing already satisfies it and the
doc makes the condition impossible to miss for the next one.
