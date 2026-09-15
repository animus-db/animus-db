# Turning TLS on for a probed port changes the prober's contract too, and no sandbox without a real cluster can see that (ADR 0064's first real `e2e-kind-tls` CI run)

ADR 0064 commit 3 made `animus-operator`'s `StatefulSet` mount TLS material
and wire it into `animusd` when `spec.tls` is set — server-only TLS on the
`admin` port, among others. It did **not** touch the `StatefulSet`'s own
`readinessProbe`/`livenessProbe`, which the same commit's own builder had
always pointed at `http://:$ADMIN_PORT/admin/health` with no `scheme`
field, because nothing in the unit-test suite (or in review) treats the
kubelet as a *consumer* of the admin port the way `animus-operator`'s admin
client or `animusd`'s own dashboard are — it's a platform component
issuing plaintext HTTP requests from outside the process entirely. The
result: every pod's admin listener now spoke TLS-only, the kubelet kept
probing plain HTTP, `rustls` rejected the plaintext bytes as
`InvalidMessage(InvalidContentType)` on the server side once per probe
period forever, and every pod stayed `NotReady` and got restart-looped —
found only on the **first real run** of the `e2e-kind-tls` CI job, because
this repo's own dev sandbox cannot bring up a `kind` cluster at all
(`CAP_SYS_RESOURCE`), so nothing short of real CI could ever have exercised
a real kubelet probing a real TLS-enabled pod. The fix needed no CA
plumbed into the kubelet: its `scheme: HTTPS` probe mode already skips
certificate verification, so flipping the `HTTPGetAction.scheme` alongside
`spec.tls` was the whole change. **General form**: when a change turns TLS
on for a port, enumerate every *out-of-process* consumer of that port, not
just the ones this codebase's own clients dial — a kubelet health check, a
cloud load balancer's health check, a service mesh sidecar's own probe are
all separate contracts with the port that a design review focused on "who
in our code dials this" will walk right past, and a sandbox that cannot
run the real orchestrator cannot catch the gap before CI does.
