# A "handle has no local authority for this" fallback should return `Option`/`Result`, not a silently-empty collection — even when the empty collection is currently harmless.

**A "handle has no local authority for this" fallback should return
`Option`/`Result`, not a silently-empty collection — even when the empty
collection is currently harmless.** `ControlHandle::Remote::config()`
(no local `RaftCore`, ADR 0035) answered a bare `BTreeSet::new()` — correct
in isolation (a data-only node genuinely knows nothing at start-up), but it
made "I have never learned anything yet" and "I have learned the group has
zero voters" the same bit pattern. That distinction stayed free as long as
nothing downstream branched on it — the moment ADR 0037 PR2 needed `Remote`
to echo a *discovered* live voter set learned over the wire, the existing
return type had no slot for "unknown", forcing a signature change anyway
(to `Option<BTreeSet<NodeId>>`) that a wider `-D warnings` blast radius
would have caught immediately if the type had been honest from the start.
General check before shipping a `Remote`/mirror/no-local-authority arm of
any handle: if this value could later be *learned* rather than *known
outright*, return `Option` (or a dedicated "unknown" variant) now, even if
every current caller would `.unwrap_or_default()` it anyway — the cost of
being wrong is a signature-widening PR later that touches every call site,
not a graceful extension. (`animusd` `control_handle.rs::ControlHandle::
config`, `RemoteControlClient::control_voters`.)
