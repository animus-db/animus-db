# A `NetworkPolicyPeer` with no `namespaceSelector` is scoped to the policy's own namespace, never the peer pod's real one (issue #857)

`animus-operator`'s admin-ingress `NetworkPolicy` rule used a bare
`podSelector` to admit the operator's own pods to a cluster's admin port.
Kubernetes `NetworkPolicy` semantics make that scoping implicit and easy to
miss: a `NetworkPolicyPeer` with only a `podSelector` set matches pods *in
the `NetworkPolicy` object's own namespace* — here, the `AnimusCluster`'s
namespace — never wherever the labeled pod actually lives. The operator
runs in its own dedicated namespace, distinct from every `AnimusCluster`'s
namespace in the documented deployment topology, so the rule was a silent
no-op: it compiled, applied cleanly, and passed its own test (which only
ever asserted the `podSelector`'s `matchLabels`, never checking for a
`namespaceSelector` at all) while admitting nothing in a real cluster.
Masked entirely under the default `--admin-access proxy` mode (admin calls
go through the API server's own pod-proxy subresource, never sourced from
an operator pod), and would only have surfaced as a real outage under the
also-supported `--admin-access direct` mode — exactly the kind of gap that
survives review and every existing test because the two things needed to
notice it (a specific deployment topology, a specific config flag) rarely
line up in the same test run. **The general rule**: any peer you build for
a `NetworkPolicy`/`NetworkPolicyPeer` that names a pod living in a
*different* namespace from the policy's own subject needs an explicit
`namespaceSelector` ANDed with the `podSelector` — never assume workload
identity to be enough. When one rule in a builder module already gets this
right (`dns_peer`'s `kube-system` scoping did, here), that itself is a
signal to check every *other* peer in the same file for the same
requirement, not just to write the new one correctly — see
`crates/animus-operator/CLAUDE.md`'s own "NetworkPolicy admin-port ingress
needs a namespaceSelector" section for the fix and how the operator's
namespace is identified.
