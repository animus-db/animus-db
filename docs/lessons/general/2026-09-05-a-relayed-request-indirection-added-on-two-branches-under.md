# A relayed-request indirection added on two branches under confusingly similar names leaves a stale free function as silent dead code (merging S-01 onto ADR 0061 rung C3d)

`main` grew a `ClientCtx.relay: R` field (ADR 0061 rung C3d) so every
cross-node relay call in `forwarding.rs`/`read_path.rs`/`schema.rs` goes
through `self.relay.relay(..)` instead of the free `relay_request`/
`relay_request_with_timeout` functions directly. Independently, `S-01`'s
own TLS commit had threaded a `tls: Option<&TlsMaterial>` parameter through
those same two free functions and every one of their call sites. Reconciling
the two (per this repo's own review guidance: the TLS dial belongs in
`AnimusdRelayClient` — the one `RelayClient` implementor — not at each
generic call site) meant moving `self.tls.as_ref()` off every call site and
into `AnimusdRelayClient::relay`'s own body, which already called
`relay_request_with_timeout` directly. That left the *other*, timeout-less
wrapper — `relay_request(addr, request, tls) { relay_request_with_timeout(
addr, request, CLIENT_TIMEOUT, tls) }` — with zero remaining callers
anywhere in the crate, `pub(crate)` and therefore invisible to any
downstream crate's own dead-code lint, and *already* dead on the
pre-merge `S-01` branch tip itself (confirmed by grepping that commit's
own tree) — an unrelated, same-shaped `RemoteControlClient::relay()`
accessor S-01 had added for `remote_metadata_watch_loop` (a method on a
*different* type, coincidentally named identically to the field this
merge introduced) had already made it redundant, and nothing had rebuilt
that one file with `-D warnings` since. Caught only by an actual `cargo
check -p animusd --lib` after resolving the textual conflicts, not by
inspection: `cargo build`/`cargo test` don't run the same dead-code
diagnostics the moment a function keeps at least one internal caller from
before the refactor, and a purely textual merge can leave a fully
well-typed function with nothing left calling it. **General form**: after
resolving a conflict that redirects a call site from one relay/dispatch
primitive to another, grep every other call site of the primitive being
abandoned before assuming it still earns its keep — a same-named-but-
unrelated indirection on the other side of the merge can make a function
look load-bearing right up until the compiler's own unused-function
warning says otherwise.
