//! Thin `animusd`-side glue for the `ControlHandle`/`RemoteControlClient`
//! seam (ADR 0035 PR1/PR4/PR5).
//!
//! **Moved to `animus-node` and genericized over `E: Env`/`R: RelayClient`
//! by ADR 0061 rung C3c** (the third 2026-08-28 amendment): the full
//! design/doc lives on `animus_node::control_handle::{ControlHandle,
//! RemoteControlClient}` now — this file is left with exactly what's
//! `animusd`-specific: the concrete type aliases instantiating both at
//! `E = ProdEnv`, and [`AnimusdRelayClient`], the [`RelayClient`]
//! implementor `RemoteControlClient::metadata_fresh` relays its `Status`
//! fetch through (rung C3b). Every pre-existing `ControlHandle`/
//! `RemoteControlClient` call site elsewhere in this crate (`lib.rs`,
//! `admin.rs`) keeps compiling unchanged against these aliases.

use std::time::Duration;

use animus_env::ProdEnv;
use animus_node::host::RelayClient;
use animus_node::{ClientRequest, ClientResponse};

use crate::relay_request_with_timeout;

/// This node's control-plane access — see `animus_node::control_handle::
/// ControlHandle`'s own doc for the full `Local`/`Remote` design. Bound
/// here at this crate's own `E = ProdEnv` and `R = AnimusdRelayClient`.
pub(crate) type ControlHandle =
    animus_node::control_handle::ControlHandle<ProdEnv, AnimusdRelayClient>;

/// A data-only node's access to the separately-deployed control plane —
/// see `animus_node::control_handle::RemoteControlClient`'s own doc. Bound
/// here at this crate's own `R = AnimusdRelayClient`.
pub(crate) type RemoteControlClient =
    animus_node::control_handle::RemoteControlClient<AnimusdRelayClient>;

/// This node's [`RelayClient`] implementor (ADR 0061 rung C3b) — a thin,
/// zero-sized wrapper over this crate's own **unchanged**
/// [`relay_request_with_timeout`](crate::relay_request_with_timeout): still
/// a fresh `TcpStream` dial per call, still on the `intra`/`client` ports,
/// still framed via [`crate::write_frame`]/[`crate::read_frame`] (which
/// themselves now call `animus_node::codec`'s C3a pure functions — see that
/// crate's `CLAUDE.md`). `tokio::time::timeout` stays entirely in this
/// impl, never in the trait or a default method it could provide: `animus-
/// node` cannot even name `tokio::time::timeout` (no `tokio` dependency at
/// all, and this crate's `disallowed_methods` lint would refuse it there
/// even if the dependency existed — see that crate's own `CLAUDE.md`'s "no
/// tokio" invariant).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AnimusdRelayClient;

#[async_trait::async_trait]
impl RelayClient for AnimusdRelayClient {
    async fn relay(
        &self,
        addr: String,
        request: &ClientRequest,
        timeout: Duration,
    ) -> ClientResponse {
        relay_request_with_timeout(addr, request, timeout).await
    }
}
