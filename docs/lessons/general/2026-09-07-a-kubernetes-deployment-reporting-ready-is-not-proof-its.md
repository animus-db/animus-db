# A Kubernetes Deployment reporting `Ready` is not proof its Service is routable yet (2026-09-07, issue #704, e2e-kind S-07e)

`scripts/e2e-kind.sh`'s webhook leg waited on `kubectl rollout status
deployment/e2e-operator-webhook` and then, in the very next phase, applied
the `ValidatingWebhookConfiguration` and fired the `kubectl patch` the API
server's admission call must reject. `rollout status` proves the pod passed
readiness — nothing about the layer above it: the Service's own
Endpoints/EndpointSlice being populated and kube-proxy having programmed
the ClusterIP rule are a separate, asynchronous propagation the Deployment
resource has no visibility into. In `kind` this is routinely about a second,
which was enough for the diagnostics to catch the Deployment and pod both
`1/1 Ready` at the same instant the API server's own admission call got
`dial tcp <clusterip>:443: connect: connection refused`, surfaced by
`failurePolicy: Fail` as an `InternalError` the test misread as "the
webhook ran and didn't reject". The fix: a converged-or-timeout poll on
`kubectl get endpoints <svc> -o jsonpath='{.subsets[*].addresses[*].ip}'`
being non-empty before ever registering the webhook config, plus a bounded
retry on the assertion probe itself (both the rejection and the acceptance
patch) that only re-fires while the error text is the dial-failure shape
(`failed calling webhook` / `connection refused` / `InternalError`) and
still fails immediately, with the original message, on any other error —
this is the same "poll the exact predicate the following assertion needs,
not a weaker stand-in that merely correlates with it" shape as the #421
entry above, one layer up the Kubernetes object graph: a Deployment's
`Ready` condition correlates with its Service being routable, but is not
that fact.
