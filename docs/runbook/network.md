# Network, handshake and inbox problems

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alerts: `AnimusHandshakeRefused`
(`rate(net_handshake_refused[5m]) + rate(client_handshake_refused[5m]) > 0`
for 10m), `AnimusInboxOverflow` (`rate(demux_frames_dropped_overflow[5m]) > 0`
for 10m).

**Handshake refused.** Every internal and client-protocol connection starts with
a version preamble (ADR 0073 Phase 0). A refusal means the peer speaks an
incompatible wire version (two different builds in one cluster: unsupported,
see [upgrade.md](upgrade.md)), or it is not an AnimusDB peer at all (a load
balancer health check or scanner hitting the internal/client/intra port with
plain TCP or HTTP; note the admin and DynamoDB ports are HTTP and do not use
the preamble). Also check TLS: a plaintext peer against a TLS port, or the
reverse, fails at handshake ([cert-rotation.md](cert-rotation.md)). Find the
instance with the non-zero rate and compare its binary version and `tls`
settings with its peers.

**Inbox overflow.** The oldest inbound frames for one internal stream id are being
dropped because no consumer drains it: usually frames for a tablet group the
node does not host (a reconcile lag or a removed replica). `GET
/admin/debug/inboxes` on the instance lists the inboxes. It is bounded by
design and self-heals once the group is hosted or the sender stops; if it
persists, see [replica-health.md](replica-health.md).

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
