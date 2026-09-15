# An out-of-cluster Kubernetes controller cannot reach a pod's own network address at all — an e2e that never exercises that code path proves nothing about it (`animus-operator`, admin-port reachability groundwork)

`scripts/e2e-kind.sh` runs the operator **out-of-cluster**
(`cargo run -p animus-operator -- run` against the runner's local
kubeconfig) — the documented local-iteration shape, not a shortcut taken
only in CI. From there, neither a pod's headless-`Service` DNS name
(`<pod>.<svc>.<ns>.svc.cluster.local`) nor its `10.244.x.x` pod IP is
routable: both addresses live on the cluster's own pod network, which a
process outside the cluster (a laptop, a CI runner) has no route to at
all — not a firewall rule to punch through, an address space that simply
isn't reachable from outside. `crate::admin_client::drain_and_remove_node`
(the scale-down member-drain sequence) dials exactly one of those two
addresses directly, and had done so since it was first written — the bug
was there from day one, just never triggered, because the e2e smoke never
forced a scale-down. It surfaced only once a second admin-port consumer
(S-07d's `spec.controlNodes` growth sequence) needed the *same* dial
during its own e2e leg and got "could not reach any control ordinal" on
every attempt.

**General form**: for a controller/agent that can run either in-cluster or
out-of-cluster (a deliberately supported local-iteration mode, not just a
dev convenience), any code path that dials a workload's own pod-network
address — not the Kubernetes API server itself — is untested by an
in-cluster-only assumption baked into review, and an e2e suite that
*always* happens to run the same way (always out-of-cluster, say) proves
that path never at all rather than proving it works. The fix here was
structural, not a network-plumbing workaround: reach the pod through the
Kubernetes API server's **pod-proxy subresource** (`GET`/`POST
/api/v1/namespaces/{ns}/pods/{scheme}:{pod}:{port}/proxy{path}`) instead
of dialing it directly — the API server is the one address a Kubernetes
client reaches identically in every deployment shape, so routing through
it (rather than adding a second, direct-dial-repair mechanism) closes the
gap for every current and future admin-port consumer at once, the same
"structural fix over a self-repair loop" preference ADR 0060's own Part 1
(stable pod DNS names over IP-repair polling) already established. See
ADR 0060's "operator admin access through the API server pod proxy"
amendment for the full design, and `crates/animus-operator/CLAUDE.md`'s
`admin_client.rs` entry for the mechanism. The narrower, second lesson:
when a fake test double (`crate::fakes::FakeAdminClient`) sits *below* the
`AdminOps` trait boundary rather than mocking a real socket, changing how
a real implementor reaches its target (direct dial vs. API-server proxy)
needs no change to the fake or to any test built on it at all — the seam
was drawn at exactly the right altitude for this kind of transport swap.
