//! [`SimRelayClient`] — a **sim-only**, [`Network`](animus_env::Network)-backed
//! [`RelayClient`] implementor (ADR 0061 rung C3d, the third 2026-08-28
//! amendment).
//!
//! Rung C3b gave `animusd` a `RelayClient` capability trait over its own
//! **unchanged** `relay_request`/`relay_request_with_timeout` (still a raw
//! `TcpStream` dial on the `intra`/`client` ports — production never
//! changes). That leaves a `SimEnv`-only cluster with no way to actually
//! reach a peer: `ClientCtx<SimEnv, R>`'s forwarding/relay/schema-broadcast
//! call sites can be driven generically, but nothing implements `R` over
//! the `Network` seam. This module is that second implementor — the thing
//! that actually lets several `ClientCtx<SimEnv, SimRelayClient<SimEnv>>`
//! instances, each bound to its own `SimEnv` node id, talk to each other
//! inside one [`Simulator`](animus_sim::Simulator) run. It is what Phase D's
//! `SimCluster` (ADR 0061 rung D1) is built on.
//!
//! # Why not literally the same wire `animusd` uses
//!
//! `Network` (ADR 0026) is fire-and-forget `send_stream`/single-consumer
//! `recv_stream` with **no built-in request/response correlation** — relay
//! is a synchronous call/await RPC, and two concurrent relay calls to one
//! peer sharing a stream cannot match replies to callers without a
//! `req_id`. That correlation machinery already has a proven shape in this
//! codebase: `animus_cp_data::cluster_segment_store`'s `req_id` +
//! `Pending` slots, polled via `env.sleep()` rather than a tokio
//! `oneshot` (a `SimEnv` caller has no tokio runtime to hand one to). This
//! module copies that shape wholesale rather than inventing a new one —
//! see [`Pending`]'s doc for the one-to-one mapping.
//!
//! Also unlike production relay (which dials `intra`/`client`, a real
//! socket address), a sim node has no host:port at all — see "Address
//! convention" below for what a `String` addr means here instead.
//!
//! # Stream allocation (ADR 0026)
//!
//! `(node, stream)` is single-consumer, so every distinct protocol
//! instance sharing a node needs its own reserved `stream` id. The full
//! allocation, gathered by grepping every existing reserved-stream constant
//! in the workspace (each one documents this same table, or a pointer to
//! it, beside its own definition):
//!
//! | Stream | Owner | Value |
//! |---|---|---|
//! | [`animus_env::PRIMARY_STREAM`] | every pre-multiplexing protocol (the control-plane Raft group; a non-split CP tablet's Raft group) | `0` |
//! | a CP data-plane tablet's own Raft group (post-split, or any tablet once it needs a distinct stream) | `animus-cp-data`'s host reconciler, `animusd`'s `RaftKvNode` wiring | `tablet.0` (the [`TabletId`](animus_tablet::TabletId)'s own `u64`, "stream = tablet.0" — see `animus_cp_data::lib`'s host-reconciler doc) |
//! | [`RELAY_STREAM`] (this module) | [`SimRelayClient`]'s own request/reply traffic | `u64::MAX - 2` |
//! | `animus_cp_data::cluster_segment_store::BACKUP_SEGMENT_STREAM`'s sibling, `animus_cp_data::backup::BACKUP_SEGMENT_STREAM` | the on-demand backup store's `ClusterSegmentStore` | `u64::MAX - 1` |
//! | `animus_cp_data::cluster_segment_store::SEGMENT_STREAM` | the DynamoDB Streams segment store's `ClusterSegmentStore` | `u64::MAX` |
//!
//! `RELAY_STREAM` sits at `u64::MAX - 2`: disjoint from `PRIMARY_STREAM`
//! (0), from every plausible `tablet.0` (small, sequential, minted by the
//! control plane — nowhere near `u64::MAX`), and from the two pre-existing
//! `u64::MAX`/`u64::MAX - 1` reservations one below the ceiling.
//!
//! # Address convention
//!
//! [`RelayClient::relay`]'s `addr: String` is production's client-API/intra
//! socket address (`"host:port"`). A `SimEnv` node has no such thing — its
//! only address is its [`NodeId`], which is itself fundamentally a string
//! (`NodeId::as_str`/`Display` round-trip through
//! [`NodeId::new_unchecked`]). So under this implementor, **`addr` is
//! exactly `NodeId::to_string()`** — no separate `String -> NodeId` lookup
//! table, no parsing that can fail. A test or fixture that wants a route
//! table entry pointing at a sim node writes `id.to_string()` as the
//! address, precisely the same way `animusd`'s real `client_route`/
//! `intra_route` maps hold a real socket address string — `SimCluster`
//! (ADR 0061 rung D1) must populate `ClientCtx::client_route`/`intra_route`
//! the same way for this to keep working unmodified.
//!
//! # One stream, two roles, exactly like `cluster_segment_store`
//!
//! `(node, stream)` is single-consumer (ADR 0026), so a node can run at
//! most one receive loop on [`RELAY_STREAM`] — but that one node is both a
//! **client** (sending `relay()` calls out, awaiting replies) and a
//! **server** (answering another node's `relay()` calls) on the identical
//! stream, and **both roles need that one receive loop**: a reply to this
//! node's own outbound call arrives on exactly the same stream an inbound
//! request would, so there is no way to be "client-only" and skip running
//! it. Rather than two streams demultiplexed by direction, this module
//! follows `cluster_segment_store::serve_loop`'s existing precedent: one
//! wire enum ([`RelayWire`]) carrying both request and reply variants, one
//! receive loop ([`serve_loop`]) that dispatches on which variant arrived —
//! a `Request` gets handed to whatever handler [`SimRelayClient::serve`]
//! has installed (or a fixed "no handler installed" error if none has) and
//! answered in place; a `Reply` gets stashed into this node's own
//! [`Pending`] map for whichever earlier `relay()` call is still waiting on
//! that `req_id`. [`SimRelayClient::new`] therefore spawns this loop
//! unconditionally — [`serve`](SimRelayClient::serve) only ever *installs
//! a handler* into it, never starts or stops it, so it may be called at any
//! point after construction (including never, for a node that only ever
//! calls out and is fine answering every inbound request with that fixed
//! error).

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{BoxFuture, Env, EnvExt, NodeId};
use async_trait::async_trait;

use crate::host::RelayClient;
use crate::wire::{ClientRequest, ClientResponse};

/// This module's reserved [`Network`](animus_env::Network) stream — see the
/// module doc's "Stream allocation" table for the full allocation and why
/// this particular value was picked.
pub const RELAY_STREAM: u64 = u64::MAX - 2;

/// How often [`SimRelayClient::relay`]'s wait loop polls its own
/// [`Pending`] slot for an arrived reply. Small relative to any realistic
/// `timeout` (`CLIENT_TIMEOUT`-scale, seconds under `SimEnv`'s virtual
/// clock) — this only bounds how finely a reply's *virtual*-time arrival is
/// observed, not real wall-clock cost.
const RELAY_POLL: Duration = Duration::from_millis(5);

/// The wire message [`serve_loop`] speaks on [`RELAY_STREAM`] — `serde_json`
/// over the `Network`'s `Vec<u8>` payload, the same "define and
/// (de)serialize your own message type" convention every other layer
/// riding `Network` directly uses (`animus_cp_data::cluster_segment_store`'s
/// `SegmentWire`, `RaftKvNode`'s own wire). `req_id` is a per-sender,
/// monotonically increasing counter (see [`Pending::next_req_id`]) — the
/// same shape `cluster_segment_store`'s own `req_id` uses, not drawn off
/// [`animus_env::Rng`] (a counter can never collide within one sender's own
/// lifetime; a random draw could, and would also perturb every other
/// seeded draw a test relies on for its own reproducibility).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum RelayWire {
    /// An outbound [`RelayClient::relay`] call, addressed to the receiving
    /// node's own [`serve_loop`] handler. `Box`ed (clippy's
    /// `large_enum_variant`, not a design choice): `ClientRequest` and
    /// `ClientResponse` are both large, deeply-nested enums, so boxing each
    /// payload keeps `RelayWire` itself small regardless of which variant
    /// is live.
    Request {
        req_id: u64,
        request: Box<ClientRequest>,
    },
    /// The handler's answer, routed back to the original caller's node
    /// (`Envelope::from` on the `Request` this replies to) and matched by
    /// `req_id` against that caller's own [`Pending`] slot.
    Reply {
        req_id: u64,
        response: Box<ClientResponse>,
    },
}

fn encode(msg: &RelayWire) -> Vec<u8> {
    serde_json::to_vec(msg).expect("RelayWire always encodes")
}

/// Outstanding-request bookkeeping for one [`SimRelayClient`] — the
/// `req_id`/`Pending`-slot correlation pattern `animus_cp_data::
/// cluster_segment_store` already established, copied here rather than
/// reinvented (see the module doc). A request's slot is `None` from
/// [`register`](Self) until a matching [`RelayWire::Reply`] arrives, then
/// `Some(response)`; `relay()` polls it away, and `drop_pending` removes it
/// — for good on a reply, or on giving up at `timeout`.
///
/// **The `req_id` correlation property this buys**: a reply that arrives
/// after `relay()` has already given up and removed the slot lands in
/// [`stash`](Self) against a `req_id` with no slot — a silent no-op (the
/// `if let Some(slot) = ..` below), never mistaken for the answer to some
/// *later* request, because `req_id` is minted fresh (monotonically) for
/// every call and never reused.
#[derive(Debug, Default)]
struct Pending {
    next_req_id: u64,
    slots: BTreeMap<u64, Option<ClientResponse>>,
}

fn next_req_id(pending: &Mutex<Pending>) -> u64 {
    let mut p = pending.lock().expect("sim relay pending poisoned");
    let id = p.next_req_id;
    p.next_req_id += 1;
    id
}

fn register_pending(pending: &Mutex<Pending>, req_id: u64) {
    pending
        .lock()
        .expect("sim relay pending poisoned")
        .slots
        .insert(req_id, None);
}

/// Stash a reply into its slot, if one is still waiting — silently dropped
/// if not (see [`Pending`]'s doc for why that's the correct, not merely
/// tolerated, behavior).
fn stash_reply(pending: &Mutex<Pending>, req_id: u64, response: ClientResponse) {
    let mut p = pending.lock().expect("sim relay pending poisoned");
    if let Some(slot) = p.slots.get_mut(&req_id) {
        *slot = Some(response);
    }
}

/// Take this slot's reply if one has arrived, without removing the slot
/// itself (the caller removes it via [`drop_pending`] once it is done —
/// either because it got an answer, or because it gave up).
fn take_reply(pending: &Mutex<Pending>, req_id: u64) -> Option<ClientResponse> {
    pending
        .lock()
        .expect("sim relay pending poisoned")
        .slots
        .get_mut(&req_id)
        .and_then(Option::take)
}

fn drop_pending(pending: &Mutex<Pending>, req_id: u64) {
    pending
        .lock()
        .expect("sim relay pending poisoned")
        .slots
        .remove(&req_id);
}

/// The dispatch [`SimRelayClient::serve`] installs — an inbound
/// [`ClientRequest`] in, a [`ClientResponse`] out. Type-erased (`dyn`, not a
/// second generic parameter on [`SimRelayClient`]) so the always-running
/// [`serve_loop`] spawned by [`SimRelayClient::new`] has one concrete
/// function to call regardless of whether — or when — `serve` ever installs
/// a real one.
type Handler = dyn Fn(ClientRequest) -> BoxFuture<'static, ClientResponse> + Send + Sync;

/// The reply [`serve_loop`] gives an inbound request when no handler has
/// been installed yet (or ever) — never a hang, never a panic, just a
/// plain, honest refusal the caller's own `relay()` sees as an ordinary
/// [`ClientResponse::Error`].
fn no_handler_installed(addr_hint: &NodeId) -> ClientResponse {
    ClientResponse::Error(format!(
        "sim relay: node {addr_hint} has no handler installed (SimRelayClient::serve was \
         never called there)"
    ))
}

/// A sim-only, [`Network`](animus_env::Network)-backed [`RelayClient`]
/// implementor — see the module doc for the full design (stream
/// allocation, address convention, the one-stream-two-roles shape).
///
/// Cheap to clone (an `E` handle plus two `Arc`s), the same shape every
/// other `RelayClient` implementor and every `Env` handle itself already
/// has — required by [`RelayClient`]'s own `Clone + Send + Sync + 'static`
/// supertrait bound.
#[derive(Clone)]
pub struct SimRelayClient<E: Env> {
    env: E,
    pending: Arc<Mutex<Pending>>,
    handler: Arc<Mutex<Option<Arc<Handler>>>>,
}

impl<E: Env> SimRelayClient<E> {
    /// Build a relay client bound to `env`'s own node id and immediately
    /// spawn its single [`RELAY_STREAM`] receive loop (`env.spawn_task`) —
    /// see the module doc's "one stream, two roles" section for why this
    /// cannot be deferred to [`serve`](Self::serve): a reply to this node's
    /// *own* outbound `relay()` calls arrives on the identical stream an
    /// inbound request would, so the loop is required for outbound calls
    /// to ever resolve at all, not only to answer inbound ones.
    ///
    /// **Call this at most once per node id.** `(node, stream)` is
    /// single-consumer (ADR 0026): a second `SimRelayClient::new` for the
    /// same node would spawn a second receive loop racing the first for
    /// the same inbox, silently splitting delivery between them — the
    /// identical contract `ClusterSegmentStore::start`'s own doc states for
    /// its own serving task. One `SimRelayClient` per node, exactly like
    /// one `ClientCtx` per node.
    #[must_use]
    pub fn new(env: E) -> Self {
        let client = Self {
            env: env.clone(),
            pending: Arc::new(Mutex::new(Pending::default())),
            handler: Arc::new(Mutex::new(None)),
        };
        let loop_env = env.clone();
        let pending = Arc::clone(&client.pending);
        let handler = Arc::clone(&client.handler);
        env.spawn_task(serve_loop(loop_env, pending, handler));
        client
    }

    /// Install `handler` as this node's dispatch for inbound
    /// [`RelayClient::relay`] calls from other nodes — every decoded
    /// [`ClientRequest`] the always-running [`serve_loop`] (spawned by
    /// [`new`](Self::new)) receives from here on is answered through it.
    ///
    /// May be called at any point after construction, including never (an
    /// inbound request then gets [`no_handler_installed`]'s fixed error,
    /// never a hang) — this only *installs* the dispatch, it does not start
    /// or stop the receive loop itself, which is already running
    /// regardless. Calling it again replaces the previously installed
    /// handler (last write wins); no production or test fixture in this
    /// codebase does that today, but nothing here forbids it.
    ///
    /// `handler` is a plain closure over whatever state the caller needs
    /// (typically a cloned `ClientCtx<E, R>`) — this module has and needs
    /// no concept of `ClientCtx`; the caller (`animusd`'s `forwarding::
    /// handle_relayed_request`) supplies the actual dispatch.
    pub fn serve<H, Fut>(&self, handler: H)
    where
        H: Fn(ClientRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ClientResponse> + Send + 'static,
    {
        let boxed: Arc<Handler> = Arc::new(move |req| Box::pin(handler(req)));
        *self.handler.lock().expect("sim relay handler poisoned") = Some(boxed);
    }
}

async fn serve_loop<E: Env>(
    env: E,
    pending: Arc<Mutex<Pending>>,
    handler: Arc<Mutex<Option<Arc<Handler>>>>,
) {
    loop {
        let envelope = env.recv_stream(RELAY_STREAM).await;
        let msg = match serde_json::from_slice::<RelayWire>(&envelope.payload) {
            Ok(msg) => msg,
            Err(err) => {
                tracing::warn!(?err, "undecodable sim-relay message dropped");
                continue;
            }
        };
        match msg {
            RelayWire::Request { req_id, request } => {
                // Clone the installed handler (if any) out from under the
                // lock before awaiting it — never hold a `std::sync::Mutex`
                // guard across an `.await` (this crate's own convention,
                // mirrored throughout `animusd`).
                let installed = handler.lock().expect("sim relay handler poisoned").clone();
                let response = match installed {
                    Some(h) => h(*request).await,
                    None => no_handler_installed(&env.node_id()),
                };
                let reply = encode(&RelayWire::Reply {
                    req_id,
                    response: Box::new(response),
                });
                env.send_stream(envelope.from, RELAY_STREAM, reply).await;
            }
            RelayWire::Reply { req_id, response } => {
                stash_reply(&pending, req_id, *response);
            }
        }
    }
}

#[async_trait]
impl<E: Env> RelayClient for SimRelayClient<E> {
    /// See the module doc's "Address convention": `addr` is parsed as
    /// exactly `NodeId::to_string()` — infallible, since [`NodeId`] is
    /// fundamentally a string
    /// ([`NodeId::new_unchecked`] is the exact inverse of `Display`).
    ///
    /// Races the [`Pending`] poll against `timeout` measured on `env.now()`
    /// (never a wall clock — ADR 0003), exactly the shape
    /// `cluster_segment_store`'s own put/fetch/delete waits use. A reply
    /// that never arrives (a dropped/partitioned send, or a peer that never
    /// calls [`SimRelayClient::serve`]) times out cleanly into a
    /// [`ClientResponse::Error`] — this never hangs past `timeout`, and
    /// never panics.
    async fn relay(
        &self,
        addr: String,
        request: &ClientRequest,
        timeout: Duration,
    ) -> ClientResponse {
        let target = NodeId::new_unchecked(addr);
        let req_id = next_req_id(&self.pending);
        register_pending(&self.pending, req_id);
        let payload = encode(&RelayWire::Request {
            req_id,
            request: Box::new(request.clone()),
        });
        self.env.send_stream(target, RELAY_STREAM, payload).await;

        let deadline = self.env.now().saturating_add(timeout);
        loop {
            if let Some(response) = take_reply(&self.pending, req_id) {
                drop_pending(&self.pending, req_id);
                return response;
            }
            let remaining = deadline.duration_since(self.env.now());
            if remaining.is_zero() {
                drop_pending(&self.pending, req_id);
                return ClientResponse::Error(format!(
                    "sim relay: timed out waiting for a reply to req_id={req_id}"
                ));
            }
            self.env.sleep(RELAY_POLL.min(remaining)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use animus_env::nid;
    use animus_sim::{SimEnv, Simulator};

    use super::*;

    /// Round-trip: node B relays a request to node A, which is serving,
    /// and gets back the exact reply the handler produced.
    #[test]
    fn request_reply_round_trip_between_two_sim_nodes() {
        let mut sim = Simulator::new(0x5E1A_0001);
        let env_a: SimEnv = sim.env(nid(0));
        let env_b: SimEnv = sim.env(nid(1));
        let relay_a = SimRelayClient::new(env_a.clone());
        let relay_b = SimRelayClient::new(env_b.clone());

        relay_a.serve(|req| async move {
            match req {
                ClientRequest::Status => ClientResponse::PutOk,
                other => ClientResponse::Error(format!("unexpected: {other:?}")),
            }
        });

        let slot: Arc<Mutex<Option<ClientResponse>>> = Arc::new(Mutex::new(None));
        let out = Arc::clone(&slot);
        env_b.spawn_task(async move {
            let resp = relay_b
                .relay(
                    nid(0).to_string(),
                    &ClientRequest::Status,
                    Duration::from_secs(5),
                )
                .await;
            *out.lock().expect("slot poisoned") = Some(resp);
        });

        sim.run_for(Duration::from_secs(2));
        assert_eq!(
            slot.lock().expect("slot poisoned").take(),
            Some(ClientResponse::PutOk),
            "seed={}",
            sim.seed()
        );
    }

    /// A partitioned peer never answers — `relay()` must time out into a
    /// clean `ClientResponse::Error`, never hang past its own `timeout`.
    #[test]
    fn a_partitioned_peer_times_out_instead_of_hanging() {
        let mut sim = Simulator::new(0x5E1A_0002);
        let env_a: SimEnv = sim.env(nid(0));
        let env_b: SimEnv = sim.env(nid(1));
        // Node A is fully willing to answer — the point of this test is
        // that the *partition* is what stops the reply, not a missing
        // handler (that "never installs a handler" case is a distinct,
        // deliberately-supported shape — see `SimRelayClient::serve`'s doc
        // — not what this test is about).
        sim.partition_pair(nid(0), nid(1));
        let relay_a = SimRelayClient::new(env_a);
        relay_a.serve(|_req| async move { ClientResponse::PutOk });
        let relay_b = SimRelayClient::new(env_b.clone());

        let slot: Arc<Mutex<Option<ClientResponse>>> = Arc::new(Mutex::new(None));
        let out = Arc::clone(&slot);
        env_b.spawn_task(async move {
            let resp = relay_b
                .relay(
                    nid(0).to_string(),
                    &ClientRequest::Status,
                    Duration::from_millis(200),
                )
                .await;
            *out.lock().expect("slot poisoned") = Some(resp);
        });

        sim.run_for(Duration::from_secs(2));
        let got = slot.lock().expect("slot poisoned").take();
        assert!(
            matches!(got, Some(ClientResponse::Error(_))),
            "expected a timeout error, got {got:?} (seed={})",
            sim.seed()
        );
    }

    /// A reply that arrives only after its own request's `relay()` call
    /// already gave up and dropped the pending slot must not be mistaken
    /// for the answer to a *later* request reusing the map — proven here
    /// by driving a first, doomed-to-time-out call to completion, then
    /// issuing a second call and confirming it gets its own, correct
    /// answer rather than any stale leftover.
    #[test]
    fn a_late_reply_after_timeout_is_not_matched_to_a_later_request() {
        let mut sim = Simulator::new(0x5E1A_0003);
        let env_a: SimEnv = sim.env(nid(0));
        let env_b: SimEnv = sim.env(nid(1));
        let relay_a = SimRelayClient::new(env_a);
        // Node A answers every request with a distinct counter-tagged
        // value, so a stale first answer is trivially distinguishable from
        // the second call's real one.
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let counter = Arc::clone(&counter);
            relay_a.serve(move |_req| {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                async move { ClientResponse::Error(format!("answer-{n}")) }
            });
        }
        let relay_b = SimRelayClient::new(env_b.clone());

        // First call: a short timeout the (slow, but not truly dead) peer
        // still answers — but only after `relay()` has already given up.
        // `RELAY_POLL` runs on virtual time, so a 1ms timeout against a
        // same-process, same-tick handler would race — instead this proves
        // the property directly: force the slot to already be dropped by
        // using a zero timeout, then confirm a normal-timeout second call
        // still gets its own, fresh answer.
        let first = {
            let relay_b = relay_b.clone();
            let slot: Arc<Mutex<Option<ClientResponse>>> = Arc::new(Mutex::new(None));
            let out = Arc::clone(&slot);
            env_b.spawn_task(async move {
                let resp = relay_b
                    .relay(nid(0).to_string(), &ClientRequest::Status, Duration::ZERO)
                    .await;
                *out.lock().expect("slot poisoned") = Some(resp);
            });
            slot
        };
        sim.run_for(Duration::from_millis(50));
        assert!(
            matches!(
                first.lock().expect("slot poisoned").take(),
                Some(ClientResponse::Error(_))
            ),
            "first call must have already timed out (seed={})",
            sim.seed()
        );

        let second: Arc<Mutex<Option<ClientResponse>>> = Arc::new(Mutex::new(None));
        let out = Arc::clone(&second);
        env_b.spawn_task(async move {
            let resp = relay_b
                .relay(
                    nid(0).to_string(),
                    &ClientRequest::Status,
                    Duration::from_secs(5),
                )
                .await;
            *out.lock().expect("slot poisoned") = Some(resp);
        });
        sim.run_for(Duration::from_secs(2));
        assert_eq!(
            second.lock().expect("slot poisoned").take(),
            Some(ClientResponse::Error("answer-1".into())),
            "the second call must resolve to its OWN reply (answer-1), never a stale \
             leftover from the first, timed-out call (seed={})",
            sim.seed()
        );
    }

    /// Several outstanding requests to the same peer, issued concurrently,
    /// each resolve to their own caller's reply — the `req_id` correlation
    /// property under concurrency, not just in sequence.
    #[test]
    fn concurrent_outstanding_requests_resolve_to_the_right_callers() {
        let mut sim = Simulator::new(0x5E1A_0004);
        let env_a: SimEnv = sim.env(nid(0));
        let env_b: SimEnv = sim.env(nid(1));
        let relay_a = SimRelayClient::new(env_a);
        relay_a.serve(|req| async move {
            let ClientRequest::Get { key, .. } = req else {
                return ClientResponse::Error("unexpected".into());
            };
            // Echo the key back inside the value, so each caller can prove
            // it got its own answer.
            ClientResponse::Value(Some(key))
        });
        let relay_b = SimRelayClient::new(env_b.clone());

        let mut slots = Vec::new();
        for i in 0u8..8 {
            let relay_b = relay_b.clone();
            let slot: Arc<Mutex<Option<ClientResponse>>> = Arc::new(Mutex::new(None));
            let out = Arc::clone(&slot);
            env_b.spawn_task(async move {
                let resp = relay_b
                    .relay(
                        nid(0).to_string(),
                        &ClientRequest::Get {
                            key: vec![i],
                            table: "t".into(),
                            stale: false,
                        },
                        Duration::from_secs(5),
                    )
                    .await;
                *out.lock().expect("slot poisoned") = Some(resp);
            });
            slots.push((i, slot));
        }

        sim.run_for(Duration::from_secs(2));
        for (i, slot) in slots {
            assert_eq!(
                slot.lock().expect("slot poisoned").take(),
                Some(ClientResponse::Value(Some(vec![i]))),
                "request {i} must resolve to its own reply (seed={})",
                sim.seed()
            );
        }
    }
}
