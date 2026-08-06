# ADR 0027 — OpenTelemetry-compatible distributed tracing

- **Status:** Accepted
- **Date:** 2026-08-06

## Context

ADR 0015 gave AnimusDB a deterministic-safe **metrics** seam — aggregate
counters, useful for "how often" questions. It does not help answer "what
happened to *this* request," especially once a request crosses a **forwarded
cross-process hop** (ADR 0017 #3b): a client writes to a node that doesn't host
the tablet's CP group leader, that node forwards the op to the leader's node
over a fresh connection, and the two nodes' logs today have no way to be
joined. Several real postmortems recorded in root `CLAUDE.md` are exactly this
shape — a `ClientRequest::Forwarded` handling gap, a leader-hint id-space
mismatch between a tablet's stable base node id and its derived Raft member
id, a shared-vs-per-node `--cluster N` edge state — and in each case, being
able to see one connected trace spanning both nodes would have cut the
diagnosis time substantially.

`tracing`/`tracing-subscriber` are already workspace dependencies. Usage today
is ad hoc: scattered `warn!`/`debug!`/`error!` calls at production-edge error
paths in `animus-env`, `animus-control`, `animus-cp-data`, `animus-consensus`,
and `animusd`. Exactly one process-wide subscriber exists
(`crates/animusd/src/main.rs`), so these calls are silent no-ops everywhere
else (in particular, under `SimEnv` — no test installs a subscriber, so this
carries no determinism risk today).

The user chose an **OpenTelemetry-compatible** setup over a bespoke
ring-buffer/`/admin/logs` surface: traces should be exportable to any standard
OTLP backend (Jaeger, Tempo, etc.), so a forwarded write shows up as one
distributed trace, viewable in existing tooling.

This ADR must decide what ADR 0015 deliberately left out (it is metrics-only,
and explicitly deferred latency/histograms): a narrative, per-request,
cross-process tracing seam. It builds on 0015's doctrine (additive, safe by
default, no `SimEnv` impact) rather than replacing it — metrics stay the
aggregate-counter seam; tracing is for point-in-time request narrative and
distributed correlation.

## Decision

### Layering: OpenTelemetry lives in `animusd` only

Only `animusd` depends on the `opentelemetry`/`opentelemetry_sdk`/
`opentelemetry-otlp`/`tracing-opentelemetry` crates or knows about trace-context
propagation. Every other crate (`animus-env`, `animus-control`,
`animus-cp-data`, `animus-consensus`, and any crate instrumented later) only
ever calls the plain `tracing` facade (`#[instrument]`, `span!`, `event!`) —
unaware that a subscriber might bridge those spans to OpenTelemetry. This
mirrors how `MetricsHandle` keeps `ProdEnv`-only recording out of
`animus-sim`: the determinism-critical crates stay dependency-light, and the
bridge lives in exactly one place — `animusd`'s subscriber construction
(`src/otel.rs`).

Cross-process propagation crosses the wire as a **plain `Option<String>`**
(the W3C `traceparent` value) on the one `ClientRequest` variant that matters
most for this — `Forwarded` — so no other crate's types need to know an
`opentelemetry` type exists.

### Export is opt-in, no-op by default

No collector is required to run a node or the test suite — the same "safe
absence" doctrine as `MetricsHandle::noop()`. The OTLP exporter activates only
when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (the OTel-standard variable); unset,
`animusd` behaves exactly as before this ADR (stdout `fmt` layer only, zero
export attempts). The W3C Trace Context *propagator* is registered
unconditionally (a pure header codec, no I/O, no network) so the
inject/extract code path is the same whether or not export is configured —
with no OTel layer attached, a span carries no valid OTel context, so
injection naturally produces no `traceparent` and propagation is a no-op.

### Transport: OTLP/HTTP (protobuf)

`opentelemetry-otlp`'s `http-proto` transport, not `grpc-tonic` — fewer
transitive dependencies (no `tonic`/protobuf-codegen toolchain), broadly
accepted by every major collector/backend. Revisit only if a target collector
requires gRPC specifically.

### Cross-process trace-context propagation

`ClientRequest::Forwarded` gains a `traceparent: Option<String>` field
alongside its existing `request` payload. At the forward site (`cp_forward`/
`relay`) the current span's OTel context, if any, is injected into a W3C
`traceparent` string via the registered propagator; at the receiving side
(`cp_serve_forwarded`) it is extracted and set as the parent of the span
handling the forwarded request — so a forwarded write shows up as one trace
spanning both nodes' spans in an OTel backend. `ProposeSchema` and `CpSplit`
relay (the other two request variants that flow through the same `relay()`
primitive) do not carry a `traceparent` yet — same mechanism, deferred to a
follow-up since they are lower-value (schema DDL and split triggers are rare,
operator-driven paths, not the hot request path the postmortems above were
about).

`request_id`/`traceparent` is **server-log-only**: it is never echoed back
into `ClientResponse`, nor into the DynamoDB/CQL wire responses. Simpler, and
those two wire formats have external client-compatibility constraints far
beyond this internal protocol's. Revisit if operators ask for it.

### Conventions

- **`#[instrument]`** at connection/request-handler entry points and Raft
  propose/apply entry points; always `skip` large/binary/non-`Debug` arguments
  (`Vec<u8>` keys/values, `TcpStream`) — this is an observe-only seam, it must
  never accidentally log full payloads.
- **Manual `event!`/`warn!`/`debug!`** stay at existing leaf/error sites (a
  dropped undecodable message, a connection-accept failure) — these are
  occurrences, not lifetimes, so they don't need a span of their own.
- **Field vocabulary**: `node_id`, `tablet_id`, `term`, `role`
  (`"control"`/`"raftkv"`) as custom span attributes — consistent names across
  crates so a `grep`/structured-log query can join fields across a forwarded
  hop.
- **`EnvFilter` targets**: rely on `tracing`'s default `module_path!()` target
  (e.g. `animus_control::node`) rather than a hand-rolled taxonomy — it's free,
  already unique per module, and sufficient for `RUST_LOG` scoping
  (`RUST_LOG=animus_cp_data=debug,animus_control=info`).

### Version-skew risk

`opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`, and
`tracing-opentelemetry` churn fast and break across each other's versions.
Pin an exact, verified-mutually-compatible set (resolved via `cargo add` at
implementation time, not hand-typed) rather than loose semver ranges; re-verify
compatibility before bumping any one of the four.

### Shutdown

The OTLP exporter batches spans on a background thread (the `http-proto` +
`reqwest-blocking-client` transport, the crate's default). `SdkTracerProvider`
is threaded out of `otel::init_tracing` and `.shutdown()` is called from the
existing graceful-shutdown path (`Node::shutdown_graceful`/Ctrl-C handling in
`main.rs`) so in-flight spans are flushed before the process exits, rather than
silently dropped.

## Non-goals

- No `tokio-console` / async-runtime instrumentation.
- No replacing `MetricsHandle` — aggregate counters and per-request tracing
  are complementary, not overlapping.
- No touching `animus-sim`'s own `Simulator::trace()`/`trace_lines()` — that is
  a **separate**, pre-existing deterministic virtual-time event log used by
  many test assertions (e.g. `animus-sim/tests/determinism.rs`'s
  byte-identical-trace guarantee). It is unrelated to the `tracing` crate and
  must not be confused with it.
- No `/admin/logs` ring buffer or runtime-reloadable `EnvFilter` in this ADR —
  the user chose OpenTelemetry export as the live-debug mechanism; those
  remain independently-scoped future options if ever needed.
- No `request_id`/`traceparent` echoed into any client-visible response.

## Consequences

- A forwarded write across two nodes now produces one joined trace in any OTLP
  backend, directly addressing the diagnosis gap the forwarding-bug postmortem
  class exposed.
- `animusd`'s dependency tree grows meaningfully (the OTLP/HTTP export stack);
  no other crate is affected.
- Full Raft-internals and wire-edge (CQL/Dynamo) span instrumentation, and
  `ProposeSchema`/`CpSplit` trace-context propagation, are follow-up work, not
  landed in the same change as this ADR — tracked as open items below rather
  than blocking the core (forwarded-request tracing) deliverable.

## Follow-up (not yet done)

- Instrument control-plane Raft election/apply and CP-data-plane per-tablet
  Raft propose/apply/snapshot paths with spans.
- Add `tracing` to `animus-cql`/`animus-dynamo` and instrument wire-level
  decode.
- Extend `traceparent` propagation to `ProposeSchema` and `CpSplit` relay.
- `animus-storage`/`animus-placement` tracing for flush/compaction/residency
  narrative (lower priority — these already have ADR 0015 metric counters).
