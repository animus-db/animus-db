# An observation that triggers a destructive action must fail to "unknown" (issue #1274)

The operator's "observed switch" read any `404` from `GET /admin/ready` as "old
binary" and reverted the readiness probe path, which is a pod-template change
and so a rolling restart. Through the API-server pod proxy a pod that does not
exist yet (a scale-up ordinal, a pod not created during bootstrap) also answers
404, so ordinary scale-up and bootstrap rolled every pod (killed port-forwards,
curl 52) and the extra latency of probing unready pods delayed the topology
annotations animusd waits for at boot. Rules: classify only the answer you can
positively attribute (probe pods that are Running with an IP; a `pods "x" not
found` 404 is not the route's 404); bound the observation's time; and never let
a best-effort optimisation sit in front of other reconcile steps (topology now
runs first, independent of probing).
