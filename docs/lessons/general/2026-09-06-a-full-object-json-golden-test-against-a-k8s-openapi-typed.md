# A full-object JSON golden test against a `k8s-openapi` typed resource must include `apiVersion`/`kind` — its `Serialize` impl always injects them (S-07c, `PodDisruptionBudget`)

Writing `desired::poddisruptionbudget`'s golden shape test by copying the
existing full-object JSON-golden pattern from `desired::cluster_config`'s
`three_node_golden_config` (`serde_json::to_value(&built)` compared
against a `serde_json::json!{...}` literal via `assert_eq!`), the first
run failed on a diff that had nothing to do with the field under test:
the serialized value carried `"apiVersion": "policy/v1"` and `"kind":
"PodDisruptionBudget"` that the hand-written `expected` literal never
mentioned. `cluster_config`'s own golden test never hits this because
`ClusterConfig` is a hand-rolled, `#[derive(Serialize)]` plain struct with
no such fields — it was the wrong precedent to copy from for a test that,
this time, serializes an actual `k8s-openapi` top-level resource type
(`PodDisruptionBudget`, `NetworkPolicy`, `StatefulSet`, …) rather than a
crate-local mirror type. Every `k8s-openapi` resource implementing
`Resource` hand-writes its own `Serialize` (not `#[derive(Serialize)]`)
specifically to always emit `apiVersion`/`kind` from
`<Self as Resource>::{API_VERSION,KIND}` ahead of `metadata`/`spec`/
`status` — this is why every *other* builder test in this crate
(`networkpolicy.rs`, `statefulset.rs`, `services.rs`) asserts on individual
typed fields (`np.spec.unwrap().ingress`, …) rather than a full
`serde_json::to_value` diff: doing so sidesteps this entirely, at the cost
of not pinning the object's complete shape in one place.

**General form**: before writing a full-object JSON/YAML golden test
against any `k8s-openapi` (or other library-owned, hand-serialized)
top-level type, either (a) check that type's own `Serialize` impl for
extra always-emitted fields the value's own Rust struct doesn't obviously
suggest (`apiVersion`/`kind` here; a real API server also injects fields
like `metadata.creationTimestamp` on `Deserialize` that a locally-built,
never-sent object simply won't carry, so those don't bite a *build-side*
golden test the way `apiVersion`/`kind` do), or (b) follow this crate's
own dominant pattern instead — assert on the specific typed fields under
test, not the whole serialized object — which is both immune to this
class of surprise and, per `crates/animus-operator/CLAUDE.md`'s own
"pure builder" design, usually what the test actually needs to pin.
