//! OpenTelemetry-compatible distributed tracing (ADR 0027).
//!
//! Scoped to this crate only: every other crate stays on the plain `tracing`
//! facade and never sees an `opentelemetry` type. Export is opt-in — with no
//! `OTEL_EXPORTER_OTLP_ENDPOINT` set, [`init_tracing`] installs only the
//! existing stdout `fmt` layer, and every OTel call below becomes a no-op (a
//! span with no OpenTelemetry layer attached carries no valid OTel context, so
//! injecting it produces no `traceparent`).

use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Installs the process-wide `tracing` subscriber: stdout `fmt` output
/// (unchanged from before this ADR) plus, when `OTEL_EXPORTER_OTLP_ENDPOINT`
/// is set and non-empty, an OTLP/HTTP span exporter tagged with
/// `instance_id` (a `--config/--node` run's node index, or a `--cluster`
/// run's process-level label — see `main.rs`). Returns the tracer provider so
/// the caller can flush it on shutdown — `None` if OTLP export isn't
/// configured (the default; no collector is ever required to run a node or
/// the test suite).
pub fn init_tracing(instance_id: &str) -> Option<SdkTracerProvider> {
    init_tracing_with_endpoint(instance_id, resolved_endpoint().as_deref())
}

/// The OTLP endpoint tracing export currently resolves to — the same
/// `OTEL_EXPORTER_OTLP_ENDPOINT` read [`init_tracing`] performs (empty
/// filtered out the same way), factored out so the admin `/admin/config`
/// view (ADR 0020, `AdminInfo::otlp_endpoint`) can report it without
/// duplicating the lookup. `None` when export isn't configured — the same
/// meaning [`init_tracing`] gives an unset/empty var.
pub fn resolved_endpoint() -> Option<String> {
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty())
}

/// The implementation behind [`init_tracing`], taking the OTLP endpoint as an
/// explicit parameter rather than reading it from the process environment —
/// the seam integration tests use to drive a real exporter against a local
/// receiver without mutating global process state (`std::env::set_var` is
/// `unsafe` and this workspace forbids `unsafe_code` outright). Most callers
/// want [`init_tracing`]; this exists for that one reason.
pub fn init_tracing_with_endpoint(
    instance_id: &str,
    endpoint: Option<&str>,
) -> Option<SdkTracerProvider> {
    // The W3C Trace Context codec used by `current_traceparent`/
    // `set_parent_traceparent` below — registered unconditionally (a pure
    // header codec, no I/O) so cross-hop propagation compiles and runs the
    // same way whether or not export is actually configured.
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    let provider = endpoint.map(|endpoint| build_tracer_provider(endpoint, instance_id));
    let otel_layer = provider
        .clone()
        .map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer("animusd")));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    provider
}

fn build_tracer_provider(endpoint: &str, instance_id: &str) -> SdkTracerProvider {
    // `OTEL_EXPORTER_OTLP_ENDPOINT` is the generic (not signal-specific) OTLP
    // env var, which per spec gets the `/v1/traces` signal path appended to a
    // bare collector `host:port` — reproduced by hand here since `endpoint` is
    // passed explicitly (see `init_tracing_with_endpoint`) rather than left
    // for the exporter to resolve from the process environment itself.
    let endpoint = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .expect("OTEL_EXPORTER_OTLP_ENDPOINT must be a valid OTLP/HTTP endpoint");
    let resource = Resource::builder()
        .with_service_name("animusdb")
        .with_attribute(KeyValue::new("service.instance.id", instance_id.to_owned()))
        .build();
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(provider.clone());
    provider
}

/// A single-key W3C `traceparent` carrier. ADR 0027 keeps this minimal —
/// `tracestate` is not propagated — so a small dedicated struct is clearer and
/// avoids a `HashMap` for the one key that matters.
#[derive(Default)]
struct TraceParentCarrier(Option<String>);

impl Injector for TraceParentCarrier {
    fn set(&mut self, key: &str, value: String) {
        if key == "traceparent" {
            self.0 = Some(value);
        }
    }
}

impl Extractor for TraceParentCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        if key == "traceparent" {
            self.0.as_deref()
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        if self.0.is_some() {
            vec!["traceparent"]
        } else {
            Vec::new()
        }
    }
}

/// The current span's W3C `traceparent`, if tracing export is active and the
/// span carries a valid OTel context — `None` otherwise (the no-op default
/// when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, or when called outside any
/// span). Call this at the point a request is about to be forwarded to
/// another node, and carry the result on the wire.
pub fn current_traceparent() -> Option<String> {
    let cx = tracing::Span::current().context();
    let mut carrier = TraceParentCarrier::default();
    global::get_text_map_propagator(|propagator| propagator.inject_context(&cx, &mut carrier));
    carrier.0
}

/// Sets `traceparent` as the parent context of `span`, so the span handling a
/// forwarded request joins the originating node's trace instead of starting a
/// disconnected one. Must be called before `span` is entered — a no-op
/// (logged at debug) if tracing export isn't active or the span has already
/// started.
pub fn set_parent_traceparent(span: &tracing::Span, traceparent: &str) {
    let carrier = TraceParentCarrier(Some(traceparent.to_owned()));
    let cx = global::get_text_map_propagator(|propagator| propagator.extract(&carrier));
    if let Err(err) = span.set_parent(cx) {
        tracing::debug!(%err, "could not set span parent from propagated traceparent");
    }
}
