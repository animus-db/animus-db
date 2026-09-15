# A lock-free "is there anything to check" fast path belongs on the per-node shared handle, not the per-connection context struct (ADR 0066 §1, S-02 step 3)

Wiring the SigV4 gate to consult the replicated credential catalog (ADR
0066) needed a cost-avoidance guard: a cluster with SigV4 auth configured
purely for its static bootstrap map (no catalog rows at all — every
pre-ADR-0066 deployment) must not start paying `ClientCtx::
effective_metadata()`'s deep `Metadata` clone on every gated DynamoDB
request just because the gate now also has a catalog to check. The
obvious place to cache "does the catalog have any rows" is a new field on
`ClientCtx` itself, following this crate's own well-worn precedent for a
config-derived per-request fact (`dynamo_auth`, `tls`, …) — but `ClientCtx`
is `Clone`d onto every connection and constructed at ~18 literal
`ClientCtx { .. }` sites across `src/`/`tests/` (the same
`error[E0063]`-enumerated fan-out root `CLAUDE.md`'s port-addition entry
describes), so a new mutable field there means auditing all 18 for how to
seed and keep it live.

`ClientCtx::edge: ClusterEdgeState<E>` already exists specifically to be
the one per-node, `Arc`-internally-shared handle every cloned `ClientCtx`
on that node points at — and it has exactly one constructor,
`ClusterEdgeState::new()`, called from ~9 sites, every one of which already
just wants "a fresh edge state," with no per-call-site knowledge the new
field would need. Putting the `Arc<AtomicBool>` fast-path flag there
instead — `has_catalog_credentials()`/`set_has_catalog_credentials()` — cut
the fan-out from ~18 sites (each needing a real per-site decision: "what
should this ClientCtx's own value even be?") to zero: every existing
`ClusterEdgeState::new()` call already produces the correct default
(`false`), and the three places that recompute it
(`ClientCtx` construction, `index_drain::change_consumer_loop`'s own
per-tick metadata refresh, and immediately after a `PutCredential`/
`RevokeCredential` commit) call a two-line accessor method rather than
touching a struct literal at all.

**General form**: before adding a new field to a struct with many
construction sites to buy a cheap fast-path check, look for an existing
`Arc`-shared handle already threaded through every one of those sites for
an unrelated reason (a routing table, a registry, an edge-state bundle) —
if its own constructor count is much smaller than the struct's, the flag
belongs there, not on the widely-constructed struct itself. This is the
inverse of the port/field-addition lesson (where the fan-out is the point,
because every site genuinely needs its own value) — the tell is whether
every one of the many sites would just want the same default.
