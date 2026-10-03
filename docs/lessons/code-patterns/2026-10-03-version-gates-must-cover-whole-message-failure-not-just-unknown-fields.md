# Version gates: unknown serde variants fail the whole enclosing message, so "additive" is not safe for enums

**Found while:** designing ADR 0073 Phase 2 (cluster version / feature gates).

The workspace has no `deny_unknown_fields`, so an older reader silently ignores
a new *field* (safe only if absence means the old behavior). A new enum
*variant* is different: serde fails the whole value, and our receivers drop it
(control/raftkv: "undecodable ... dropped"; client wire: connection torn down).
One new `MetaCommand` inside an `AppendEntries` therefore drops the entire
batch and wedges replication silently. Rule: new variants are emitted only
behind a gate, enforced by an exhaustive `required_gate()` match with no `_`
arm so the compiler forces every new variant to name its gate.

Related: never branch on a gate at apply time; a tablet replica cannot read
`Metadata` and mirrors lag, so apply must depend only on entry bytes and
state-machine state. Gate at the proposer with a new variant/flag instead.
