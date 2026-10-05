# A Ready pod does not mean its background registration has committed

**Context:** G-01 stage G-a added an `e2e-kind.sh` assertion that every member
in `/admin/status` carries the node's topology labels. It was a one-shot check
right after the dynamo pod's `/admin/health` went 200, and `e2e-kind-encryption`
(run 37201459207) failed with e2e-0 still `labels: {}` while e2e-1/e2e-2 were
labelled and every pod already carried the resolved annotations.

**Lesson:** animusd's `RegisterNode` runs as a background task after the
listeners are up, and the replica serving `/admin/status` can lag the leader.
Readiness is not a barrier for it. Any e2e assertion on replicated metadata
written by a node's own startup tasks (labels, addresses, membership) is an
eventual property: poll it with `wait_for` (converged-or-timeout) and print the
last observed body on timeout. Do not write "by the time a pod is Ready X is
committed" in a comment without a mechanism that guarantees it.
