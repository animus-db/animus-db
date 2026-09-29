# Dropping `#[serde(default)]` on an `Option` field does not make it required

**Context.** ADR 0073 Phase 0 (workstream C, layer 3) removed pre-baseline compat from
`txn::TxnWrite` by deleting `#[serde(default)]` from its `stage_marker`/`pending`
`Option` fields. A test that removed the field from the JSON and expected a decode error
still decoded fine.

**Lesson.** serde's derive already treats a *missing* `Option<T>` field as `None` with no
attribute at all, so deleting `#[serde(default)]` from an `Option` changes nothing. To make
such a field genuinely required, use `#[serde(deserialize_with = "f")]` where `f` just calls
`Option::<T>::deserialize` (a field with `deserialize_with` loses the implicit missing-`Option`
default). Always write the negative test (field removed -> `Err`) — the deletion looks
sufficient in review and is a silent no-op.

**Also.** A wire/image codec that is `pub(crate)` cannot be fixture-tested from `tests/`;
put those fixture tests in-crate (`#[cfg(test)]`) and share the "every variant" sample
builders with the round-trip unit tests rather than duplicating them.
