# `kube`'s `DynamicObject` is the right tool for a CRD-adjacent resource from an API group `k8s-openapi` doesn't ship (ADR 0064, S-01 commit 3)

Building a `cert-manager.io/v1` `Certificate` from `animus-operator` hit
the same problem any operator managing a non-core, non-`k8s-openapi`
resource will hit: there is no typed Rust struct for it anywhere in the
dependency graph, and vendoring cert-manager's own Rust types (if they
even exist as a published crate) is a heavier dependency than the one
object this operator ever creates warrants. `kube::core::DynamicObject`
(`ApiResource::from_gvk` for the type descriptor, `.data(serde_json::
json!({...}))` for the actual spec) is built exactly for this: a
`serde_json::Value` payload plus enough metadata (`TypeMeta`/
`ObjectMeta`) to round-trip through `kube::Api::namespaced_with` and
server-side-apply like any typed object. The trade-off is real but
narrow — no compile-time field checking on the `Certificate`'s own spec
shape, same as this crate's pre-existing hand-maintained `animusd::config`
mirror already accepts for a different reason (see this crate's own
`CLAUDE.md`) — and it's the right one for a single, simple object rather
than pulling in or hand-rolling a whole second CRD's typed bindings.

**General form**: before reaching for a full typed-struct dependency (or
writing one by hand) for a foreign CRD your own operator only ever
creates/patches one shape of, check whether `kube::core::DynamicObject` +
a `serde_json::json!` literal covers it — it usually does, and it keeps
the dependency graph from growing for a resource you don't own the schema
of anyway.
