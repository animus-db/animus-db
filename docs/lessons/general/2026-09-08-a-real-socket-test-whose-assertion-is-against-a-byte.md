# A real-socket test whose assertion is against a byte-identical compile-time-constant asset does not need its sim fixture to reproduce the serving mechanism at all (ADR 0061 rung H, C-08 PR 7, 2026-09-08)

Converting `crates/animusd/tests/dashboard_endpoint.rs`'s render-only
tests to `SimCluster` siblings hit an apparent blocker: the web
dashboard's static assets (`GET /admin/ui/*` — the shell HTML, every
per-view `.js`) are served from `animusd`'s own `handle_conn` **before**
its `AdminHost` dispatch table is ever reached (`animus_node::admin`'s
own module doc says so explicitly), and `SimCluster::admin` calls
straight into `animus_node::admin::dispatch` — never `handle_conn` — so
it structurally cannot fetch `/admin/ui/*` at all. The naive read of this
gap is "these tests can't be converted without teaching `SimCluster` to
speak real HTTP framing," the same shape of blocker several earlier rungs
in this series treated as a genuine `ProdEnv`-only residual (a control-
only/data-only role split, a real TLS handshake, real SSTable I/O).

**It is not that kind of blocker here, and the difference is worth
naming**: every one of these assets is a `pub(crate) const &str =
include_str!(...)` — a **compile-time constant**, fixed at build time,
with zero request-time computation between the constant and the HTTP
response body (`crate::admin::static_asset` is a lookup table from path to
constant, not a renderer). A test asserting `served_body.contains("some
marker")` is really asserting `HTML.contains("some marker")` one
indirection removed — the HTTP round trip contributes nothing to the
assertion's own truth value, only to how far from the constant the test
happens to read it. Reading `crate::dashboard::{HTML, CORE_JS, ...}`
directly, in-process, is therefore not a *narrower* proof than fetching it
over a real socket would have been: it is the exact same bytes, checked
by the exact same `.contains(...)`, minus a serving mechanism the
assertion never needed reproduced in the first place. (The live JSON
routes these same tests also check — `/admin/txns`, `/admin/status`, the
U-07 observability routes — are the opposite case, genuinely computed
per-request from live replicated state, and stayed on `SimCluster::admin`
exactly as every other converted-test route in this series does.)

**General form**: before spending fixture effort reproducing a serving
mechanism (framing, routing, a dispatch table) to make a real-socket
test's assertion reachable from a sim fixture, check what the assertion
is actually checking. If it is asserting against a value that is fixed at
compile time and identical regardless of how it was fetched — a static
asset, a schema-derived constant, anything `include_str!`/`const`-shaped
— reading that value directly is a legitimate, not-narrower conversion,
and the "can't reproduce the serving mechanism" framing that would
otherwise keep the whole test on `ProdEnv` doesn't apply to that half of
the test at all. It still applies, unmodified, to any other half of the
same test that legitimately depends on live, per-request, or per-process
state.
