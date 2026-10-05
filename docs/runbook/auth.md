# Authentication and authorization denials

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alert: `AnimusAuthDeniedSpike`
(`rate(auth_denied[5m]) + rate(auth_unknown_key[5m]) > 1` for 10m).

When `--dynamo-auth` (or a replicated credential catalog) is on, the DynamoDB
port requires SigV4. `auth_unknown_key` counts requests whose access key id is
in neither the catalog nor the static map (`UnrecognizedClientException`);
`auth_denied` counts valid signatures whose policy forbids the operation class
or table (`AccessDeniedException`); `auth_rotated_secret_used` (not alerting)
counts use of a rotated secret inside its grace window.

First checks: who is calling? (request logs/source IPs; the node does not log
caller identity per request in metrics). List credentials (ids, policy, enabled,
rotation state, never secrets): `curl -s http://<admin-addr>/admin/credentials`
(CLI: `animus admin credentials <admin-addr>`). A spike after a rotation means a
client still uses the old secret past its grace window. A revoked or disabled
credential, or a policy change, explains denials. Rotate:
`POST /admin/credentials/rotate {"id":"...","new_secret":"...","grace_secs":N}`;
revoke: `POST /admin/credentials/revoke {"id":"..."}`; redefine:
`POST /admin/credentials {"id","secret","policy"?,"enabled"?}`. The admin port
itself has no authentication; if denials come from a scanner, restrict network
access to the DynamoDB port.

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
