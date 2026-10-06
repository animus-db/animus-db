# A new field inside an existing command's JSON blob needs a content-dependent gate

`WriteSchema.mrec` and `KindEvalOp::Replicate` ride as `serde_json` blobs inside the
existing `KvCommand::KindEval`/`KindEvalBatch`/`TxnStage`. An older binary's serde
silently *ignores* the unknown `mrec` field (no `deny_unknown_fields`), so it would
apply an MREC write as an unstamped ordinary one: no decode error, silent divergence,
which is worse than the wedge a new variant causes. A per-variant `required_gate` row
cannot see this, so the row became content-dependent (fold the entries / pending writes
and join to the gate when the content is present), enforced at the single propose
choke point. The same trap applies to a new enum *value* in an existing field
(`MultiRegionConsistency::Eventual` through `ConvertTableToGlobal`): classify by
content and reject it at apply as well. In the sim, let the capped decode name the new
shapes by content (`content_gate`) so a mis-classified emitter still trips it; verify
by downgrading the rows to `Base` and watching both positive cells fail.
