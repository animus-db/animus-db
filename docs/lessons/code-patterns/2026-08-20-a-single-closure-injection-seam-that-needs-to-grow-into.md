# A single-closure injection seam that needs to grow into several fallible, parameterized operations should become a small trait, not a pile of more closures — but the widening must be audited to stay in *shape* only, never in *kind* (2026-08-20, ADR 0052 PR3, the Data Console's Config tab).

**A single-closure injection seam that needs to grow into several
fallible, parameterized operations should become a small trait, not a
pile of more closures — but the widening must be audited to stay in
*shape* only, never in *kind* (2026-08-20, ADR 0052 PR3, the Data
Console's Config tab).** PR2 gave `console.rs` a `TableSnapshotFn`
(`Arc<dyn Fn() -> Vec<TableSummary>>`) as its one seam into `lib.rs`'s
cluster-aware world — exactly right for one parameterless, infallible
read. PR3 needed six more operations (a per-table detail read plus five
mutations), each needing a table name/request body and able to fail.
Bolting five more `Arc<dyn Fn...>` fields onto `serve`'s signature would
have worked mechanically but obscured the one property that actually
matters here: that every operation's signature is still built only from
plain owned types the seam itself declares, never the richer type the
other side of the boundary actually has in hand. An `async_trait` trait
(`ConsoleBackend`) makes that property easy to see and easy to keep
honest at every call site — one `impl` block, one place to check that no
method accepts or returns a cluster/schema-catalog type — where five
separate closures would have made the same audit five separate
fly-by-eye checks. The general form: when a seam must grow, prefer
promoting it to a trait over multiplying its closures, but the reason to
prefer the trait is auditability of the type boundary, not the trait
keyword itself — a trait whose methods leak the richer type back through
is no safer than the closures would have been. (`crates/animusd/src/
console.rs`, `crates/animusd/src/lib.rs`.)
