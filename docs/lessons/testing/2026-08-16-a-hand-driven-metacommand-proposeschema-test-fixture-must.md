# A hand-driven `MetaCommand::ProposeSchema` test fixture must target the INTRA port, not the client port, since ADR 0047's listener cutover

**A hand-driven `MetaCommand::ProposeSchema` test fixture must target the
INTRA port, not the client port, since ADR 0047's listener cutover** — a
fresh test file (`f11_split_alignment.rs`) sent `ClientRequest::
ProposeSchema(CreateTableSchema{..})` to a node's `client_addr()` (the
shape every pre-0047 test used, and still the right address for
`Put`/`Get`/`SplitTablet`, which stayed `Surface::Public`) and got back
`Error("propose_schema is a cluster-internal request; send it to this
node's intra port")` — `handle_request` refuses any `Surface::Intra`
request (`ProposeSchema` among them, `surface_of`'s own match arm) on the
client listener outright. `index_backfill.rs` already carries the fix
as a one-line comment ("ADR 0047: `ProposeSchema` is intra-only") right
above its own `intra_addr()`-targeted calls, but nothing greppable ties
that convention to the wire type itself, so a fresh test file rediscovers
it the hard way. **General rule**: when hand-driving a `ClientRequest`
variant in a new test, check `surface_of`'s match arms (`lib.rs`) for
which listener it's actually gated to — `Surface::Public` variants
(`Put`/`Get`/`Scan`/`Delete`/`Txn`/`SplitTablet`/`Status`) work on
`client_addr()`; every `Surface::Intra` variant (`ProposeSchema`,
`Forwarded`, `KindWrite`/`KindScan`, the `Txn*` internal RPCs, etc.) needs
`intra_addr()` instead — the error message names the fix, but only once
you've already hit it. (2026-08-16, `growth/1-f11-fence`.)
