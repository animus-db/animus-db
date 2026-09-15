# A strictly-typed admin-API address field (`SocketAddr`) silently forecloses Kubernetes-native automation over it — check the field's own type, not just its wire shape (S-07d, `POST /admin/control/member/add`)

Automating ADR 0037's control-voter-growth admin path from
`animus-operator` (S-07d) hit a real, pre-existing gap only visible once
an actual Kubernetes call site needed it: `POST /admin/control/
member/add`'s request body (`admin::AddControlMemberReq`) types `addr` as
`std::net::SocketAddr`, which `serde`'s `FromStr`-backed deserialization
can only ever parse from a literal IP:port — never a DNS name. That is
exactly the wrong shape for a Kubernetes pod, whose only *stable* identity
across a restart is its per-ordinal DNS name (`RoleAddrs::
advertise_host`); its IP is not stable at all. Every *other* address
surface this same codebase already built for exactly this reason
(`RoleAddrs`/`ClientResponse::JoinInfo`/`ProdEnv::merge_peer`/`set_peers`)
is deliberately string-typed — `member/add`'s own `addr` field is the one
outlier, because it was built for the bare-metal CLI's `animus admin
control-add`, where a literal IP is exactly right (that deployment shape
binds real host IPs, never `0.0.0.0`). Nothing about `member/add`'s own
*doc comment* or its *wire shape* (a plain JSON string either way) hints
at this — only reading the actual Rust field type on the server side
(`SocketAddr`, not `String`) reveals it, and only because a Kubernetes
call site's own address is a hostname.

The workaround landed (`resolve_control_dial_addr`, reading the pod's live
`status.podIP` via the Kubernetes API rather than resolving the hostname
via DNS) is deliberately narrow and self-healing (the promoted node's own
startup self-registration republishes its real, hostname-based address
moments later — see ADR 0060's S-07d amendment for the full account), but
it is a workaround, not a fix — the real fix (accepting a `String` addr
the way `ProdEnv::merge_peer` already does) belongs in `animusd`, out of
`animus-operator`'s own scope (that crate has no dependency on `animusd`).

**General form**: before wiring a new automated caller (an operator, a
controller, any Kubernetes-native client) against an *existing* admin/RPC
API that predates that caller's own deployment shape, check every address
field's actual Rust type on the server side, not just its JSON shape or
its doc comment — a `SocketAddr` (or any other strictly-typed, IP-only
field) is a signal the API was designed for a deployment shape where a
literal IP is stable, which a Kubernetes pod's is not. Grep for the type,
don't infer it from the wire.
