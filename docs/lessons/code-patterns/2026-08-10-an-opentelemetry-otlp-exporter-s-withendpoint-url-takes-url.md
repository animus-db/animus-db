# An `opentelemetry-otlp` exporter's `.with_endpoint(url)` takes `url` as the exact, final request URL — it does *not* append the OTLP signal path (`/v1/traces`) the way the SDK's own env-var resolution does for the generic `OTEL_EXPORTER_OTLP_ENDPOINT`.

**An `opentelemetry-otlp` exporter's `.with_endpoint(url)` takes `url` as the
exact, final request URL — it does *not* append the OTLP signal path
(`/v1/traces`) the way the SDK's own env-var resolution does for the generic
`OTEL_EXPORTER_OTLP_ENDPOINT`.** Reading that env var by hand and forwarding it
straight into `.with_endpoint(..)` (ADR 0027's `animusd::otel` seam) silently
posted every span export to the endpoint's bare root (`POST /`) instead of
`POST /v1/traces` — a real collector would 404 this with zero indication it was
a config bug, not a network one, since the exporter reports one generic
`HttpClient.NetworkError` regardless of cause. Either let the builder resolve
the endpoint itself (don't call `.with_endpoint(..)` at all — it then reads
`OTEL_EXPORTER_OTLP_ENDPOINT`/`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and appends
the signal path correctly), or reproduce the append by hand if the endpoint must
be threaded explicitly for testability (`animus_db` did the latter, so a test
seam could pass an arbitrary receiver address without `unsafe`-mutating process
env). Caught by decoding the exporter's actual protobuf payload in
`animusd/tests/otel_tracing.rs`, not by the exporter reporting success.
