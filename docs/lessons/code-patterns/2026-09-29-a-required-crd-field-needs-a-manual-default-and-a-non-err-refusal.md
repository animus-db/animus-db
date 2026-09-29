# A required CRD version field: manual `Default`, schema `required:`, and a non-`Err` refusal

Adding a required, no-serde-default `schemaVersion` to `AnimusClusterSpec`
(ADR 0073 Phase 0 E, layer 2) has three non-obvious parts:

- A derived `Default` would give `schema_version: 0`, an instantly-invalid
  spec, silently breaking the ~25 `..Default::default()` test literals'
  intent. Replace the derive with a manual `Default` using the
  `CONTENT_SCHEMA_VERSION` constant; struct literals without a spread must
  name the field.
- Omitting `#[serde(default)]` is what makes schemars list the field under
  `required:` (and `#[schemars(range(min = 1))]` adds `minimum`); the pinned
  `deploy/operator/crd.yaml` must be regenerated, and every checked-in CR
  manifest (example.yaml, the e2e heredoc) must gain the field or the API
  server rejects it. Do not rely on the schema alone: serde and the shared
  validator also reject.
- A reconciler refusal must return `Action::await_change()` after setting a
  status condition, never `Err`: `error_policy` would requeue with backoff on
  an input that cannot change until the spec is edited.
