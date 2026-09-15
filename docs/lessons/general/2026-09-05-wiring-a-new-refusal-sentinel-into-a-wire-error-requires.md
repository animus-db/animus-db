# Wiring a new refusal sentinel into a wire error requires auditing EVERY error-mapping call site, not just the primary write path (ADR 0065, W-08 step 3)

Enforcing per-table throttling (`ThrottleTracker::check_write`/`check_read`)
needed a plain `Result<_, String>` sentinel (`dynamo::THROTTLE_WRITE_
REFUSAL`/`THROTTLE_READ_REFUSAL`) to propagate up from deep inside
`write_path.rs`/`read_path.rs` to `dynamo.rs`, where it has to render as
`ProvisionedThroughputExceededException` rather than a generic `500`. The
obvious place to add that mapping (`map_throttleable_error`) is the single
call site the throttled write path most directly feeds — but a
`String`-typed error has no compiler-visible identity, so nothing forced an
audit of every OTHER place a `.map_err(|e| internal(&e))`-shaped closure
converts the identical error type into a wire response. Five separate call
sites needed the fix, found only by grepping every `internal(&e)`-shaped
mapping in `dynamo.rs`, not by inspection of the "main" path:
`fast_marker_write`'s own `cp_kind_write_raw` error mapping, `raw_quorum_
read`, `native_scan`, and two LSI-scan pagination call sites
(`paginated_table_examine`'s `cp_scan_kind_table` call, `paginated_kind_
examine_one`'s `cp_scan_kind` call). One of these (`fast_marker_write`) was
caught only by a real integration test getting an unexpected `500` instead
of a `400` — the other four were found by then auditing every structurally
identical mapping pattern rather than trusting that the first fix was
complete. **General form**: when a new internal sentinel error needs a
wire-level translation, grep every `.map_err`/error-conversion call site
that touches the same underlying `Result<_, String>`-shaped function family,
not just the one test that happened to exercise the sentinel first — a
string-typed error carries no type-system signal that a translation step
was skipped, so the compiler will not find the other four sites for you.
