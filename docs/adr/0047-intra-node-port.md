# ADR 0047 — The intra-cluster port: splitting node-to-node traffic off the client port

- **Status:** Accepted — implemented (this stack: `intra/1-plumbing` →
  `intra/2-cutover` → `intra/3-join-docs`). **Amended by [ADR 0053](
  0053-dynamodb-only-drop-cql.md) (2026-08-22):** the CQL wire adapter this
  ADR's port-stride formula includes is dropped; see the amendment note at
  the end of this document.
- **Date:** 2026-08-16

## Context

Every node-to-node `ClientRequest` that isn't the raw `ProdEnv`/Raft-wire
transport (`internal`) — `Forwarded` CP relays, `ProposeSchema` schema-DDL
relay, `WatchMetadata` long-polls, `JoinInfo` discovery, and every
internal-only forwarding payload (`KindWrite`/`KindScan`/`ForceSeal`/
`StreamHotRead`/`ClearBackfillCursor`/`KindWriteItem`/the six 2PC `Txn*`
RPCs) rode the same `client` port an external DynamoDB/CQL-adjacent caller
also dials. Those internal-only variants were already refused when sent
*bare* (not wrapped in `Forwarded`) — a pre-existing, orthogonal "wrong
envelope" rule — but nothing stopped an external caller from reaching them
at all, and `WatchMetadata`/`JoinInfo`/`ProposeSchema` were reachable
completely unrefused.

Animus's target deployment (Guillaume, 2026-08-16) is Kubernetes via an
**operator**: seed/node-to-node traffic is cluster-internal by design
(internal Services), and only the DynamoDB/CQL wire edges are meant to be
reachable from outside the cluster. A single client port conflates two
audiences with very different trust levels, and gives the operator's
Service topology no port-level way to express "this traffic never needs to
leave the cluster." This ADR gives node-to-node `ClientRequest` traffic its
own **intra** port and makes the client port refuse to serve it.

## Decision

### Plumbing (`intra/1-plumbing`)

`RoleAddrs`/`NodeAddrs` gain a sixth field, `intra: SocketAddr`/`String`, no
serde default — matching `internal`/`client`'s own no-default convention (a
clean break; no live deployments to keep back-compat with). Port stride:
`base_port + 6*i + {internal:0, client:1, dynamo:2, cql:3, admin:4,
intra:5}` (was `5*i`). All three deployment shapes (combined, control-only,
data-only) bind and carry the listener (`Node::bind`/`bind_control`/
`bind_data`, the `Bound*` structs, an `intra_addr()` accessor mirroring the
existing ones) — there is no shape that skips it: a control-only node
receives `ProposeSchema` relays and serves `WatchMetadata` long-polls; a
data-only node originates both.

### The cutover (`intra/2-cutover`)

Two small, deliberately separate 2-variant enums:

```rust
enum ListenerKind { Client, Intra }   // which listener a connection came in on
enum Surface { Public, Intra }        // where a variant may be received bare
```

Kept as two distinct types rather than one shared enum, because the
refusal rule is **asymmetric**: refuse only when `listener == Client &&
surface_of(request) == Intra`. The reverse combination (`Intra` listener
serving a `Public` variant) is fine — intra is the more-trusted network
segment, and neither port has authentication yet at this milestone, so it
transparently accepting ordinary client-shaped ops too is not a privilege
escalation. **`Intra` is a superset of `Public`, not a disjoint
partition** — this is deliberate; a future reader should not "fix" it into
a second refusal layer.

One exhaustive free function, `surface_of(&ClientRequest) -> Surface`
(beside `request_kind`, same free-function convention), is the single
source of truth for the classification — no wildcard arm, so a new
`ClientRequest` variant anywhere is a compile error here until classified.
As shipped, at this stack's tip (25 variants, including `KindWriteItem`
from the already-landed U3 stack):

| `Surface::Public` (8) | `Surface::Intra` (17) |
|---|---|
| `Status`, `Put`, `PutBatch`, `Get`, `Scan`, `Delete`, `Txn`, `SplitTablet` | `Forwarded`, `ProposeSchema`, `JoinInfo`, `WatchMetadata`, `KindWrite`, `KindScan`, `GetSnapshot`, `ForceSeal`, `StreamHotRead`, `ClearBackfillCursor`, `KindWriteItem`, `TxnPrepare`, `TxnDecide`, `TxnResolve`, `TxnStatus`, `TxnRecordView`, `TxnVerify` |

`Status` is genuinely dual-purpose (served on both surfaces, no asterisk):
side-effect-free, grants no authority, and is needed on `client` (the
`animus-cli` `status` command) as well as on `intra` (the join-bootstrap's
own bare `Status` poll, `RemoteControlClient::metadata_fresh`).

`Forwarded` is classified `Intra` unconditionally, regardless of its boxed
inner payload — no recursion needed, since any `Forwarded` envelope is
node-to-node by construction. The twelve internal-only forwarding payloads
are listed explicitly too, even though `Forwarded`'s own classification
already makes them transitively unreachable via the client listener —
leaving that implicit would mean "is this variant reachable on the client
port" depends on reasoning about two gates together instead of this one
table being the complete answer on its own.

One guard clause at the top of `handle_request`, before its existing
~160-line match (untouched, byte-for-byte):

```rust
if listener == ListenerKind::Client && surface_of(&request) == Surface::Intra {
    return ClientResponse::Error(format!(
        "{} is a cluster-internal request; send it to this node's intra port",
        request_kind(&request)
    ));
}
```

`serve_clients`/`handle_client` merge into `serve_requests`/
`handle_connection`, parameterized by `ListenerKind` and threaded straight
through to `handle_request` — a copy becomes a parameterization, not a
fork. `spawn_common_tail` spawns two instantiations of the same function
(one per listener) plus a new `intra_route_sync_loop`.

**Layering rule** for the client-protocol family specifically (this does
**not** extend to `dynamo.rs`/`cql.rs`/`admin.rs`, which already have their
own established shared-`http.rs` shape and are untouched here):

```
transport/framing (read_frame/write_frame)
  → per-listener adapter (serve_requests/handle_connection,
    parameterized by ListenerKind, never forked)
    → one dispatch/use-case layer (handle_request + the single
      surface_of table)
      → ClientCtx primitives (cp_*/propose_schema/relay/cp_forward)
```

Any future new surface on this same protocol should follow this shape:
extend the parameterization, not fork the adapter or grow a second
classification table.

**Scope precision**: `surface_of`'s exhaustiveness retires the standing
"grep every gating site" lesson (root `CLAUDE.md`) **only for the
client-vs-intra reachability axis**. It does **not** touch
`is_relayable_command` (whether a `MetaCommand` may ride the
`ProposeSchema` relay envelope) or `cp_serve_forwarded`'s own match
(whether real handling exists for a forwarded payload) — both stay exactly
as grep-dependent as before; unrelated axes, not solved by this change.

### The hint-field conflation, and the parallel-hint design

Retargeting the machine-relay address resolvers surfaced a pre-existing
conflation: `ControlHandle::leader_addr_hint()`/`RemoteControlClient.
leader_hint` backed **three** consumers wanting different address flavors
off one field — `propose_schema`'s relay preference (machine-to-machine,
needs intra), `not_leader_error`'s human-facing "retry on {addr}" message
surfaced by `admin_drain`/`admin_remove_member` over the admin HTTP
endpoint (must stay client — a human operator dials it), and `admin.rs`'s
dashboard `leader_hint` display (same, explicitly documented as "the
client-API address"). A naive repoint would have silently broken the two
human-facing consumers on any `ControlHandle::Remote` node.

**Decision: add a parallel hint, don't repoint the existing one.**
`ClientResponse::Status`/`MetadataDelta` gain `intra_leader_hint:
Option<(NodeId, SocketAddr)>` (`#[serde(default)]`, matching `leader_hint`'s
own robustness pattern), populated server-side from the same
`self.control.leader()` id, resolved through a new `ClientCtx::intra_addr`
lookup instead of `route_addr`. `RemoteControlClient` tracks a second
`(NodeId, SocketAddr)` hint the identical way, exposed as
`ControlHandle::intra_leader_addr_hint()`. `propose_schema`'s relay/
broadcast tiers, `remote_metadata_watch_loop`'s dial candidates, and the
growth-node mirror's own seed-building all switch to the new hint plus a
parallel `intra_route`/`ClientCtx::intra_addr`/`intra_route_snapshot`
(mirroring `client_route`/`route_addr`/`route_snapshot` exactly).
`not_leader_error` and `admin.rs`'s dashboard display are **untouched**.

**Standing rule**: machine relay → `intra_leader_hint`; anything a human
reads → `leader_hint`. Nothing in the type system enforces this — a future
reader must not assume the two are interchangeable just because they carry
the same shape.

### `intra_route` needed a real static seed, not an empty one

The first attempt mirrored `route_sync_loop`'s shape but started
`ctx.intra_route` from an **empty** static seed, on the theory that the
sync loop's 200ms overlay of `Metadata.node_addrs[*].intra` would converge
it fast enough — true for most consumers, which tolerate "not yet known,
retry." It is not true for the growth-node/join-node mirror's own
seed-building (`BoundNode::start_with_streams`'s `ctx.intra_addr(id)` call,
feeding `remote_metadata_sync_loop`): that call runs **synchronously** at
ctx-construction time, and the loop it feeds captures its `seeds` argument
once, by value, at spawn time — an empty seed there is permanent, not
transient. Fixed by threading `intra_route: BTreeMap<NodeId, SocketAddr>`
as a full sibling parameter through every `start_with*`/`start_control_with`/
`start_data_with*` signature and call site (mirroring `client_route`
exactly), including through the join path: `ClientResponse::JoinInfo` gains
an `intra_route` field, threaded through `discover_join_info`/
`finish_combined_join`/`finish_data_join`.

### A missed retargeting site, found by a real test failure

`resolve_cp_route`'s own "no local replica of this tablet" fallback — the
very first blind-forward guess a node with zero local replicas of a tablet
makes — still read `route_snapshot()` (client addresses) after every named
resolver (`cp_leader_hint`, `other_tablet_replica_addr`, `propose_schema`'s
relay) had been retargeted. It wasn't visible by code review; it surfaced
as a real `cluster_split.rs` test failure (a control node forwarding a
write): `Error("forwarded is a cluster-internal request; send it to this
node's intra port")`. Fixed to `intra_route_snapshot()`. See
`docs/engineering-lessons.md` for the general form of this lesson.

### Join semantics (`intra/3-join-docs`)

**`--seed ADDR` now names the seed's intra address, not its client
address — no client-side exception carved out.** Joining is fundamentally
a cluster-membership action (the joiner is about to become an internal
`ProdEnv`/Raft peer too), so there is no genuine "external client"
character to this traffic; classifying it as intra is the honest
classification, not a compromise, and keeps the client listener's refusal
set free of asterisks. The chicken-and-egg concern resolves cleanly: the
operator supplies the seed's intra address directly (the same mechanism as
today, a different port number they already have — the Kubernetes operator
wires it from the same Service DNS/IP it always used), and every
subsequent hop stays on intra once `JoinInfo`'s reply carries
`intra_route`.

## Consequences

- Every deployment shape binds and serves a sixth port; `Node::shutdown()`
  now frees six ports, not five.
- A mis-pointed dial to the wrong listener degrades to an ordinary
  `ClientResponse::Error`, never silent corruption — every existing retry
  loop (`join_request`/`poll_seeds_for`, `cp_forward`) already treats an
  error reply as "this candidate didn't help, try the next one / time out
  loudly."
- `--seed`'s documented meaning changes; ~15 pre-existing test files that
  drove a bare `ProposeSchema`/`WatchMetadata`/`Forwarded`-wrapped RPC
  directly against a node's client address as a test-setup shortcut needed
  retargeting to the intra port — a wider blast radius than initially
  scoped; see `docs/engineering-lessons.md`.
- Slightly higher port-exhaustion pressure under `cargo test --workspace`
  parallelism (6 ports/node instead of 5) — the existing `free_addrs`
  port-TOCTOU retry dance widens proportionally; no new failure mode
  observed in practice during this stack's own gates.
- No authentication on either port yet — this ADR is a port-level
  segmentation only; a later milestone.

## New regression

`crates/animusd/tests/intra_port_split.rs`, over a real 3-process cluster:
a tablet **follower**'s own **client** port refuses a bare `Forwarded{..}`
and a representative internal variant (`KindWrite`) — a pure listener+
surface check, independent of leadership; the tablet **leader**'s own
**intra** port serves the identical `Forwarded{Get}` request end-to-end,
one hop, returning the real committed value — proving the intra port is
genuinely wired to `cp_serve_forwarded`, not just unblocked.

## Amendment (2026-08-22, ADR 0053)

[ADR 0053](0053-dynamodb-only-drop-cql.md) drops the CQL wire adapter and
its port entirely. This ADR's own port-stride formula above (`base_port +
6*i + {internal:0, client:1, dynamo:2, cql:3, admin:4, intra:5}`) and
[ADR 0052](0052-data-console-port.md)'s later seven-slot extension (`+
console:6`) are both historical at this point — as of ADR 0053 the stride
is six again, but composed differently than it was here: `base_port + 6*i
+ {internal:0, client:1, dynamo:2, admin:3, intra:4, console:5}`. Every
slot from `admin` onward renumbers down by one; this is an in-process
constant only (root `CLAUDE.md`'s no-back-compat policy), not a wire
format any client depends on across the change. The body above is kept
as originally written — it was correct when this ADR shipped — and
should be read alongside ADR 0052's and ADR 0053's own amendment notes
for the current layout.
