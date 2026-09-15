# A commit-wait convergence check on a mergeable/settable field must test membership, not whole-value equality (roadmap W-06, `TagResource`/`UntagResource`)

`SetTableTtl`/`SetTableStream`/`UpdateContinuousBackups`'s commit-wait loops
(propose → poll `metadata_fresh()` → compare against the value just
proposed → retry-or-succeed) all get away with a whole-value equality check
(`meta.table_ttl(table) == spec.as_ref()`) because each of those fields is a
**replace**: the whole `Option<Spec>` is what the command sets, so "did my
value land" and "does the field now equal what I proposed" are the same
question.

A tag set is different: `TagResource`/`UntagResource` **merge** into
`TableSchema::tags` (insert/remove specific keys), so two independent
callers tagging the *same table* with *different keys* are both legitimate
and don't conflict. A commit-wait loop that captured "the whole map I
expect after my write" up front and compared for exact equality would spin
past its deadline the moment a concurrent, unrelated tag mutation landed in
between — the field is real and correct, just not byte-identical to the
snapshot taken before proposing. The fix: check only that **the keys this
call cares about** now hold the wanted values (`tags.iter().all(|(k, v)|
current.get(k) == Some(v))` for `TagResource`, `tag_keys.iter().all(|k|
!current.contains_key(k))` for `UntagResource`) — the same "did *my* effect
take hold" question the whole-equality checks answer for a replace-shaped
field, expressed correctly for a merge-shaped one. Generalizes to any future
replicated command that merges into a collection rather than replacing a
scalar/struct field: reach for the SetTableTtl precedent's exact-equality
convergence check only when the command truly replaces the field wholesale.
