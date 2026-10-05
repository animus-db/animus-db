# TLS certificate expiring

Alert entry point (rules in `deploy/observability/animus-alerts.yml`, R-01(f)). Conventions (`<admin-addr>`, the control leader, `GET /metrics` on the DynamoDB port) are in [README.md](README.md).

Alert: `AnimusTlsCertificateExpiringSoon`
(`certmanager_certificate_expiration_timestamp_seconds{name=~".*-tls"} - time()
< 14 days` for 1h).

**A renewed certificate is not a rotated certificate.** `animusd` reads its
PEM files once at startup and never reloads them (ADR 0064 Decision 6). When
cert-manager renews, it updates the `Secret` in place; the running pods keep
presenting the old certificate until they restart, and fail handshakes when it
expires. Operator-managed pods are also **not** restarted by a `Secret`
content change. So the response to this alert is a rolling restart after the
renewal has landed: follow [cert-rotation.md](cert-rotation.md). If the alert is firing
because renewal itself failed, fix cert-manager first (`kubectl describe
certificate <name>-tls`).

## Maturity

Derived from the alert expression and the metric documentation in `crates/animus-env/src/metrics.rs`; not exercised against a real incident.
