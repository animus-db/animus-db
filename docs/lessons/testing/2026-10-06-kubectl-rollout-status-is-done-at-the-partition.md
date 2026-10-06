# `kubectl rollout status` is "done" at the partition, not at the end of the roll

**What happened.** Once the operator gated every pod-template change behind an operator-owned
StatefulSet `partition` (ADR 0073 Phase 3, D7/D8), the kind e2e's controlNodes-growth phase
became intermittent. It waited with `kubectl rollout status statefulset/...` and then
port-forwarded to `e2e-1`, which then failed with "admin port-forward against pod e2e-1 never
became ready".

**Why.** `rollout status` on a StatefulSet with `rollingUpdate.partition = N` reports success
as soon as the ordinals `>= N` are updated. The operator applies the template with
`partition = replicas` and lowers it one ordinal per healthy observation, so `rollout status`
returns while most pods are still waiting to be restarted. The script then raced the pod it
had just picked.

**Rule.** When something other than the StatefulSet controller owns the partition, wait for
the whole roll to finish. That means the partition is back at 0, `observedGeneration` is at
least `generation`, `currentRevision == updateRevision`, and `updatedReplicas` and
`readyReplicas` both equal `spec.replicas`. `scripts/e2e-kind.sh`'s `statefulset_fully_rolled`
checks exactly this.
