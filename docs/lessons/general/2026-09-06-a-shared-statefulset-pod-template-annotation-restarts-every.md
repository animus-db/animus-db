# A shared `StatefulSet` pod-template annotation restarts *every* pod — hash only what a running pod cannot pick up live, never what changes on every routine scale (S-07d, the `animusdb.io/config-hash` annotation)

S-07d's groundwork (`desired::statefulset::CONFIG_HASH_ANNOTATION`) bakes a
content hash of the generated cluster config into the `StatefulSet`'s pod
template so an already-running `animusd` — which reads `cluster.json`
once at container start and never again — actually gets restarted when a
config-affecting spec change (`spec.tls`, `spec.s3`/`backupStore`/
`segmentStore`, `spec.controlNodes`, …) needs it to. The mechanism is
correct, but its *first* version hashed the raw generated `ConfigMap`
`data` map wholesale — every key, including `cluster.json`'s full `nodes`
array. That array grows/shrinks on a plain `spec.nodes` scale (one
`RoleAddrs` entry appended or removed, every *existing* entry untouched
per `cluster_config`'s own `scale_up_config_append_preserves_existing_
entries_byte_for_byte` invariant), so a routine scale-up changed the hash
and rolled *every already-running pod* — not just the new one — even
though nothing about their own boot-time config changed: a running
`animusd` never rereads the node list at all, it learns of new/changed
peers only through replicated `Metadata` (ADR 0030 self-registration).
This shipped straight through review because every *unit* test for the
annotation (`config_hash_is_stable_for_an_unchanged_spec`,
`config_hash_changes_when_control_nodes_changes`) held `spec.nodes`
constant — nothing pinned the "nodes-only scale must be a no-op for this
hash" property, so nothing caught it locally. It surfaced only in
`e2e-kind-tls`'s scale phase (3 → 4 nodes): the unwanted rollout evicted
`e2e-0`/`e2e-1`/`e2e-2` out from under the script's own `kubectl
port-forward` mid-check, failing the post-scale `GetItem` with a "lost
connection to pod" — a CI-only failure with no local repro until the
scale step was actually exercised against a real `StatefulSet` controller.

The fix (same day) replaced "hash the whole `ConfigMap`" with "hash a
typed *restart-relevant projection*" built straight from
`AnimusClusterSpec` (`desired::statefulset::restart_relevant_projection`):
the `control_nodes` role-split threshold, the full `entrypoint.sh` text
(already node-count-independent — `entrypoint_script` takes only `spec`),
the `cluster_settings` section, and whether TLS is wired — explicitly
never `spec.nodes`, node count, or any per-node id/address/
`advertise_host`. Regression coverage added the property the first
version lacked directly: `config_hash_is_unchanged_by_a_nodes_only_
scale_{up,down}` (3 ↔ 4 nodes, mirroring the exact e2e scenario) alongside
the existing "changes when it should" cases, and
`config_hash_pinned_for_a_fixed_fixture` pins the hash *value* so a future
change to the hash function or the projection's field set shows up as an
explicit, reviewable diff rather than silently rolling every deployed
cluster's pods on the next release. The hash itself also moved off
`std::collections::hash_map::DefaultHasher` to an inline FNV-1a 64 for the
same "no silent bit-pattern change" reason — `DefaultHasher` carries no
cross-Rust-release stability guarantee, so an operator rebuilt with a
newer toolchain could roll every cluster's pods for no operator-visible
reason.

**General form**: a hash (or any other single scalar) baked into a shared
`StatefulSet`/`Deployment` pod template to force a restart necessarily
restarts *every* replica on *any* input change — there is one pod
template, not one per pod. Before hashing "the generated config" wholesale,
ask specifically which of its fields a running process actually rereads
live vs. only at boot, and — separately — which of its fields change on
inputs that have nothing to do with an individual pod's own boot-time
config (a node/replica count is the recurring example: it changes on
every routine scale, and an already-running process in a
self-registering/gossip-style cluster typically never needs to know about
it from a config file at all). Anything in the second category has no
business in the hash, no matter how naturally it falls out of "just hash
the whole generated artifact" — and the regression test that would have
caught it is specifically "hold the scale-sensitive input constant across
its own natural range and assert the hash doesn't move," not just
"assert the hash changes when it obviously should."
