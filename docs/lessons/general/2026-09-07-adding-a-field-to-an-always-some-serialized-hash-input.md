# Adding a field to an always-`Some`-serialized hash-input struct rolls every deployed cluster's pods, even ones that never use the new feature — give the new field `skip_serializing_if`, not its predecessors (ADR 0069, S-03 PR 3, `desired::statefulset::RestartRelevantConfig`)

`desired::statefulset::RestartRelevantConfig` (S-07d) is hashed
(`restart_relevant_config_hash`, FNV-1a 64 over its JSON encoding) into a
pod-template annotation that triggers a `StatefulSet` rolling restart the
moment it changes. Its existing fields (`tls: Option<TlsSection>`,
`cluster_settings: Option<ClusterSettings>`) have no `serde(
skip_serializing_if)` at all — `None` always serializes as an explicit
`"tls":null`. Adding `encryption_key_path: Option<String>` (S-03 PR 3) the
same way would have meant every spec's JSON projection gained a new key —
`"encryption_key_path":null` on every cluster that has never heard of
`spec.encryptionKeySecretName` — changing the hash, and therefore rolling
every already-deployed cluster's pods on the very next operator upgrade,
for a feature those clusters don't use and nothing about their own config
actually changed.

**Fix**: give only the *new* field `#[serde(skip_serializing_if =
"Option::is_none")]`. A spec with the field unset then serializes
byte-identically to the pre-PR JSON shape (verified directly: the pinned
`config_hash_pinned_for_a_fixed_fixture` literal needed no update),
so the blast radius of the upgrade shrinks to exactly the clusters that
actually set the new field — which is the only population for whom a
restart is doing real work. Retrofitting the same attribute onto the
*existing* fields (`tls`/`cluster_settings`) would itself change their
hash contribution and cause the identical unwanted restart, so don't —
the fix is additive-only, applied at the moment a field is introduced,
never backfilled onto siblings whose hash contribution is already
load-bearing for real clusters.

**General form**: before adding a field to any struct whose serialized
form feeds a change-detection hash with real infrastructure consequences
(a pod restart, a cache invalidation, a re-sync), ask whether the struct's
existing shape always serializes every field (no `skip_serializing_if`)
or only present ones. In the "always" case, a plain new field changes the
hash for *every* input, not just inputs that use the new feature — make
the new field itself opt out of serialization when absent, so adopting a
feature (not merely upgrading the tool that supports it) is what triggers
the consequence.
