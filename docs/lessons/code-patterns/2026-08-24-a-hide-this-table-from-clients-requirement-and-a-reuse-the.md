# A "hide this table from clients" requirement and a "reuse the existing per-table TTL reaper" requirement can be mutually exclusive under a hidden-table naming convention that predates the second requirement (2026-08-24, ADR 0018's `ClientRequestToken` amendment).

**A "hide this table from clients" requirement and a "reuse the existing
per-table TTL reaper" requirement can be mutually exclusive under a
hidden-table naming convention that predates the second requirement
(2026-08-24, ADR 0018's `ClientRequestToken` amendment).** The obvious
design for an internal, client-invisible table was to reuse the `$`-
separated hidden-table convention a materialized GSI/LSI already uses
(`animus_dynamo::index::index_table_name`) — it looked like exactly the
"invisible internal table" primitive needed. It structurally cannot work
for a table that also needs the ADR 0051 TTL reaper: `Metadata::apply`'s
`CreateTableSchema` arm rejects any `$`-containing name outright (so a
`$`-named table never gets a `Metadata.schemas` entry at all), and the
reaper's own per-tick sweep requires **both** a `table_ttl` entry **and**
a `table_schema` entry before it will scan a table — so a `$`-named table
could never be TTL-enabled even if the first guard were relaxed. The fix
was not to weaken either guard (the `$` guard is exactly what keeps a
hidden index table's identity collision-free) but to pick an ordinary,
schema-registered table name instead, and grow the *visibility* story
(`is_internal_table_name`, checked at every client-facing entry point)
as its own, separate mechanism. **General form**: before reaching for an
existing "hide this from normal traffic" convention to solve a *new*
hiding requirement, check what else that convention's own definition
structurally excludes the hidden thing from — a convention built to
answer one question (avoid a name collision) can silently foreclose an
unrelated later question (participate in a background sweep) that never
came up when it was designed, and the foreclosure is in the *existing*
guards, not a new one you'd think to look for.
