# A readiness probe gates on what the pod serves, not on cluster-wide health (issue #1274)

The operator's `readinessProbe` pointed at `/admin/health`, which 503s with no
recent control-plane leader. The client Service routes only to Ready pods, so a
control-plane quorum loss emptied the Service even though per-tablet Raft
groups (a separate quorum) kept serving. Rule: a readiness signal answers "can
THIS pod serve THIS Service's traffic" (synced routing metadata, no dead
consensus loop), never "is the whole cluster healthy"; keep the cluster-health
route for dashboards and runbooks. Corollary for rollouts: a probe path lives
in the pod template next to the image, so a new route is only safe once every
image the operator may render it against serves it; a 404 probe keeps a pod
NotReady forever. Test a readiness route in the state it exists for (quorum
lost, metadata synced), not just the happy path.
