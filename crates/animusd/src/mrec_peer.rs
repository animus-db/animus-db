//! MREC peer-cluster plumbing (ADR 0075 section 4.3, G-01 stage G-d M3): the
//! node-local view of `cluster_settings.{region, peers, allow_insecure_peers,
//! mrec_max_clock_skew_ms}` ([`MrecConfig`]) and the **cross-cluster
//! transport seam** ([`PeerClient`]) with its real implementation
//! ([`ProdPeerClient`]).
//!
//! # Transport
//!
//! A peer cluster is reached on its nodes' **intra** ports (any peer data
//! node accepts a frame and routes it by its own tablet layout, see
//! `mrec_receiver`). The cross-cluster authentication is ADR 0064 **mutual
//! TLS** on that port: each side's `--tls-ca` bundle must contain the other
//! side's CA (a peer's optional `tls_ca` is additionally trusted when
//! verifying *that peer's* server certificate). There is no new listener and
//! no per-request signing in v1. A node with no TLS refuses to dial a peer
//! (and its receiver refuses to accept a peer frame) unless
//! `allow_insecure_peers` is set, which is for dev and simulation only.
//!
//! The wire shape is one intra-only request family,
//! [`ClientRequest::MrecApply`] / [`ClientResponse::MrecApply`], **class G**
//! gated on `Gate::MrecReplication` (see `animus-node`'s `wire.rs`). A peer
//! cluster on a binary that predates it tears the connection down on the
//! unknown variant (ADR 0073 section 3); that surfaces here as an ordinary
//! transport error and the shipper backs off. A peer on a new binary whose
//! cluster version has not reached the gate answers `Refused { retryable }`.
//!
//! # The seam
//!
//! [`PeerClient::call`] is bytes in, bytes out (`serde_json` of
//! [`MrecApplyRequest`]/[`MrecApplyResponse`]) so the sim's `PeerBridge`
//! (`sim_world.rs`) and this crate's real client are interchangeable; `to` is
//! an index into the configured peer list ([`MrecConfig::peers`]).

#![allow(
    dead_code,
    reason = "the M4 shipper is this module's first production caller; M3 ships the config view, \
              the seam and the real client, exercised by tests"
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use animus_env::TlsMaterial;
use animus_node::host::RelayClient;
use animus_node::{ClientRequest, ClientResponse, MrecApplyRequest, MrecApplyResponse};
use futures::future::BoxFuture;

use crate::config::{
    ClusterConfig, ClusterSettings, DEFAULT_MREC_MAX_CLOCK_SKEW_MS, PeerCluster, TlsSection,
};
use crate::control_handle::AnimusdRelayClient;

/// Why a cross-cluster call failed (as opposed to the peer answering).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerError {
    /// No reply within the caller's timeout (lost, partitioned, or too slow).
    Timeout,
    /// The connection or the exchange failed (peer down, handshake refused, an
    /// older binary that does not know the request).
    Transport(String),
    /// Refused locally before anything was sent: no TLS and not
    /// `allow_insecure_peers`, an unknown peer index, or a malformed payload.
    Refused(String),
}

/// The peer-client seam: what MREC's shipper calls. `to` indexes
/// [`MrecConfig::peers`] (the sim's bridge: a cluster index).
pub(crate) trait PeerClient: Send + Sync {
    /// Send `payload` (an [`MrecApplyRequest`] as JSON) to peer `to` and await
    /// its response (an [`MrecApplyResponse`] as JSON).
    fn call(
        &self,
        to: usize,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> BoxFuture<'static, Result<Vec<u8>, PeerError>>;
}

/// Most `MrecApply` batches one node's receiver executes concurrently; one
/// more is answered `Retry` for every record (no ack, no cursor move) rather
/// than queued, so a flood from a peer cannot starve this cluster's own
/// traffic.
pub(crate) const MREC_MAX_INFLIGHT_APPLIES: usize = 64;

/// The node-local MREC view of `cluster_settings` (static, never replicated;
/// the same on every node of the cluster). `Default` is "no MREC": no region,
/// no peers, which makes the receiver refuse every frame.
#[derive(Clone, Debug)]
pub(crate) struct MrecConfig {
    /// This cluster's own region name.
    pub(crate) region: Option<String>,
    /// The configured peer clusters, in declaration order (the index is the
    /// `to` of [`PeerClient::call`]).
    pub(crate) peers: Vec<PeerCluster>,
    /// `cluster_settings.allow_insecure_peers`.
    pub(crate) allow_insecure: bool,
    /// `cluster_settings.mrec_max_clock_skew_ms` (default 500).
    pub(crate) max_clock_skew_ms: u64,
    /// Receiver concurrency gauge (see [`MREC_MAX_INFLIGHT_APPLIES`]).
    pub(crate) inflight: Arc<AtomicUsize>,
}

impl Default for MrecConfig {
    fn default() -> Self {
        MrecConfig {
            region: None,
            peers: Vec::new(),
            allow_insecure: false,
            max_clock_skew_ms: DEFAULT_MREC_MAX_CLOCK_SKEW_MS,
            inflight: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl MrecConfig {
    /// The view of a (validated) `cluster_settings` section.
    pub(crate) fn from_settings(settings: Option<&ClusterSettings>) -> Self {
        let Some(s) = settings else {
            return Self::default();
        };
        MrecConfig {
            region: s.region.clone(),
            peers: s.peers.clone(),
            allow_insecure: s.allow_insecure_peers.unwrap_or(false),
            max_clock_skew_ms: s
                .mrec_max_clock_skew_ms
                .unwrap_or(DEFAULT_MREC_MAX_CLOCK_SKEW_MS),
            inflight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The view of a whole cluster config.
    pub(crate) fn from_cluster(config: &ClusterConfig) -> Self {
        Self::from_settings(config.cluster_settings.as_ref())
    }

    /// Whether `region` is a configured peer of this cluster.
    pub(crate) fn is_peer(&self, region: &str) -> bool {
        self.peers.iter().any(|p| p.region == region)
    }

    /// The peer index of `region` (its [`PeerClient::call`] `to`).
    pub(crate) fn peer_index(&self, region: &str) -> Option<usize> {
        self.peers.iter().position(|p| p.region == region)
    }

    /// Whether cross-cluster traffic may flow on a node that does (`true`) or
    /// does not (`false`) have TLS configured: mutual TLS on the intra port is
    /// the cross-cluster authentication, so plaintext is refused unless
    /// `allow_insecure_peers` (dev/sim) is set.
    pub(crate) fn transport_allowed(&self, tls_configured: bool) -> bool {
        tls_configured || self.allow_insecure
    }
}

/// The refusal text for a plaintext node, shared by the client and the
/// receiver so both name the same remedy.
pub(crate) const INSECURE_PEER_REFUSAL: &str = "cross-cluster MREC traffic requires TLS (mutual TLS on the intra port, ADR 0064); \
     configure TLS or set cluster_settings.allow_insecure_peers for dev/sim";

/// The real [`PeerClient`]: dials a peer's intra endpoints with this node's
/// own mutual-TLS material (plus the peer's own `tls_ca` when configured),
/// through the same gated framing every intra relay uses.
#[derive(Clone)]
pub(crate) struct ProdPeerClient {
    cfg: Arc<MrecConfig>,
    relay: AnimusdRelayClient,
    /// Per-peer-index dial material for peers that name their own `tls_ca`.
    peer_tls: Arc<BTreeMap<usize, TlsMaterial>>,
}

impl ProdPeerClient {
    /// Build the client. `node_tls` is this node's own TLS material (the one
    /// the intra relay uses) and `node_tls_section` the config it was loaded
    /// from, needed only to derive a per-peer connector for a peer that names
    /// a `tls_ca`.
    ///
    /// # Errors
    /// A peer names a `tls_ca` but this node has no TLS, or the CA file cannot
    /// be loaded.
    pub(crate) fn new(
        cfg: Arc<MrecConfig>,
        node_tls: Option<TlsMaterial>,
        node_tls_section: Option<&TlsSection>,
        features: animus_control::version::ClusterFeatures,
    ) -> std::io::Result<Self> {
        let mut peer_tls = BTreeMap::new();
        for (i, peer) in cfg.peers.iter().enumerate() {
            let Some(ca) = &peer.tls_ca else { continue };
            let (Some(base), Some(section)) = (&node_tls, node_tls_section) else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "peer `{}` names a tls_ca but this node has no TLS configured",
                        peer.region
                    ),
                ));
            };
            let loaded = section
                .to_tls_config()
                .load_with_extra_client_ca(Some(std::path::Path::new(ca)))?;
            let mut material = base.clone();
            material.connector = loaded.connector;
            peer_tls.insert(i, material);
        }
        Ok(ProdPeerClient {
            relay: AnimusdRelayClient::new(node_tls, features),
            cfg,
            peer_tls: Arc::new(peer_tls),
        })
    }
}

impl PeerClient for ProdPeerClient {
    fn call(
        &self,
        to: usize,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> BoxFuture<'static, Result<Vec<u8>, PeerError>> {
        let this = self.clone();
        Box::pin(async move {
            if !this.cfg.transport_allowed(this.relay.tls.is_some()) {
                return Err(PeerError::Refused(INSECURE_PEER_REFUSAL.into()));
            }
            let Some(peer) = this.cfg.peers.get(to) else {
                return Err(PeerError::Refused(format!("no peer with index {to}")));
            };
            let request: MrecApplyRequest = serde_json::from_slice(&payload)
                .map_err(|e| PeerError::Refused(format!("malformed MrecApply payload: {e}")))?;
            let request = ClientRequest::MrecApply(request);
            // One overall budget split across the endpoints, tried in order:
            // any peer data node can accept the frame.
            let per_endpoint = timeout / u32::try_from(peer.endpoints.len().max(1)).unwrap_or(1);
            let relay = match this.peer_tls.get(&to) {
                Some(material) => {
                    AnimusdRelayClient::new(Some(material.clone()), this.relay.features.clone())
                }
                None => this.relay.clone(),
            };
            let mut last = PeerError::Timeout;
            for endpoint in &peer.endpoints {
                match relay.relay(endpoint.clone(), &request, per_endpoint).await {
                    ClientResponse::MrecApply(resp) => {
                        return serde_json::to_vec(&resp)
                            .map_err(|e| PeerError::Transport(e.to_string()));
                    }
                    ClientResponse::Error(e) if e == crate::RELAY_HOP_TIMEOUT => {
                        last = PeerError::Timeout;
                    }
                    ClientResponse::Error(e) => last = PeerError::Transport(e),
                    other => {
                        last = PeerError::Transport(format!("unexpected reply: {other:?}"));
                    }
                }
            }
            Err(last)
        })
    }
}

/// Decode a peer's reply bytes (a convenience for the shipper and the tests).
///
/// # Errors
/// The bytes are not an [`MrecApplyResponse`].
pub(crate) fn decode_response(bytes: &[u8]) -> Result<MrecApplyResponse, PeerError> {
    serde_json::from_slice(bytes)
        .map_err(|e| PeerError::Transport(format!("malformed MrecApply response: {e}")))
}
