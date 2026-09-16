# `SdkTracerProvider::force_flush`/`shutdown` block the calling OS thread until the exporter's HTTP call completes — call them via `spawn_blocking`, never directly inside an async fn on a `#[tokio::test]`'s default current-thread runtime.

**`SdkTracerProvider::force_flush`/`shutdown` block the calling OS thread until
the exporter's HTTP call completes — call them via `spawn_blocking`, never
directly inside an async fn on a `#[tokio::test]`'s default current-thread
runtime.** The default runtime has exactly one worker thread; blocking it
synchronously starves every other task scheduled on it, including a test's own
in-process receiver task waiting to `accept()`/`read()` the very HTTP request
the flush is trying to send — a same-process instance of the "don't hold a lock
across `.await`" deadlock family, just with a blocking call standing in for the
lock. The symptom is a flush that hangs for its full timeout and then reports a
generic network error, which reads exactly like a broken exporter rather than a
starved runtime. (`animusd/tests/otel_tracing.rs`.)
