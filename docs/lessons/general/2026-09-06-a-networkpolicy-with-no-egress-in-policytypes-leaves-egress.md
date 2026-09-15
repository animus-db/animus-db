# A `NetworkPolicy` with no `Egress` in `policyTypes` leaves egress completely open, regardless of what an `egress:` list would say

Writing S-04 PR 3's egress rules for `animus-operator`'s generated
`NetworkPolicy` (`desired/networkpolicy.rs`) was a reminder that
Kubernetes `NetworkPolicy` semantics are per-*direction*, not per-object:
a policy that never names `Egress` in `spec.policyTypes` is a no-op for
egress traffic on that pod selector, full stop — it doesn't matter whether
`spec.egress` is present, empty, or omitted, and it doesn't matter how
restrictive `spec.ingress` is. This repo's own operator had exactly that
shape for a long time (`policyTypes: [Ingress]` only, since the policy was
originally written before egress was ever a concern), which is precisely
what `docs/roadmap.md`'s S-04 item flagged as "egress unrestricted by
omission." **The generalizable check**: when adding an egress rule to an
existing `NetworkPolicy` builder (here or anywhere else), verify
`policyTypes` names `Egress` in the same change — a rule appended to
`spec.egress` alone silently does nothing if that list update is missed,
and nothing in the API server, `kubectl apply`, or a type-checked
`k8s-openapi` struct catches the omission; only a real cluster (or reading
the NetworkPolicy semantics doc closely) reveals it.
