# Adding a variant to a replicated config command that, unlike its closest precedent, mints no identity label changes its idempotency rule, not just its payload shape

**Adding a variant to a replicated config command that, unlike its
closest precedent, mints no identity label changes its idempotency
rule, not just its payload shape** — modeling DynamoDB TTL's
`MetaCommand::SetTableTtl` directly on `SetTableStream` would have made
a same-attribute re-enable an error, because `SetTableStream` rejects
re-enabling specifically to protect its minted `label` from going
stale. TTL has no label to protect, so the correct rule is the opposite:
re-enabling with the same value is a no-op, and changing the value in
place (no disable/re-enable round trip) is `Applied` — both are real,
legal DynamoDB operations. When cloning the shape of an existing
replicated command for a new field, check *why* each of its rejects
exists before copying it, not just what the reject is guarding on the
surface.
