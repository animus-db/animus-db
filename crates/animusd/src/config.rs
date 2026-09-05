//! Cluster configuration for per-process deployment.
//!
//! A [`ClusterConfig`] lists every node's addresses. One `animusd` process runs
//! one node by index: it binds *its* listeners at the configured addresses and
//! learns every peer's address from the same config.
//!
//! **ADR 0040 PR1 (one identity per node)**: a node has exactly **one**
//! [`NodeId`], carried on one internal `ProdEnv` — the control-plane Raft
//! rides stream 0 (`PRIMARY_STREAM`), every per-tablet Raft group its own
//! stream (`stream = tablet_id >= 1`, ADR 0026). There is no more
//! `control_id`/`raftkv_id` arithmetic (`RAFTKV_ID_BASE`/`synthetic_control_id_for`
//! are gone) — a node's id is just its config index (or a minted string in a
//! later PR of this stack). This is a **clean break**: fresh clusters only, no
//! wire/WAL back-compat with a pre-ADR-0040 deployment.
//!
//! **ADR 0035 (control plane as a separate deployment)** adds [`NodeRole`]: a
//! [`RoleAddrs`](crate::RoleAddrs) entry declares whether it runs the control
//! role, the data role, or both (`Both`, the default and — until that ADR —
//! the *only* shape).

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use animus_env::NodeId;
#[cfg(test)]
use animus_env::nid;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};

use crate::RoleAddrs;

/// The conventional (unpadded) node id for config index `index` — `"n{index}"`
/// (ADR 0040 PR3: `NodeId` is now a validated string, not an arithmetic
/// `u64`). This is the *default minting convention* every generator in this
/// module uses; it is **not** re-derived from a loaded config at runtime —
/// once a config exists, a node's true identity is its own [`RoleAddrs::id`]
/// field (which may have been zero-padded by [`ClusterConfig::generate`] for
/// `n >= 10`, or hand-edited by an operator). Kept as a free function because
/// dozens of tests use it purely to predict what id a freshly `generate`d
/// small (`n < 10`, hence unpadded) cluster assigns index `i` — see
/// `minted_id` for the width-aware variant `generate`/`generate_split`
/// actually embed.
#[must_use]
pub fn node_id(index: usize) -> NodeId {
    NodeId::new_unchecked(format!("n{index}"))
}

/// The id `generate`/`generate_split` actually embed for index `i` out of
/// `total` nodes: `"n{i}"`, zero-padded to the width of `total - 1` once
/// `total >= 10` — otherwise byte-identical to [`node_id`]. Zero-padding
/// keeps ids in **lexicographic == numeric** order (`"n10" < "n2"` otherwise,
/// ADR 0040 §6 call-out #7); below 10 nodes there is nothing to reorder, so
/// the common small test/dev cluster keeps the plain, already-relied-upon
/// `"n{i}"` convention.
fn minted_id(i: usize, total: usize) -> NodeId {
    if total >= 10 {
        let width = (total.saturating_sub(1)).to_string().len();
        NodeId::new_unchecked(format!("n{i:0width$}"))
    } else {
        node_id(i)
    }
}

/// Which role(s) a [`RoleAddrs`] entry runs (ADR 0035).
///
/// `Both` is the default and was, before ADR 0035, the *only* shape every
/// node could take — so an old config (or one that never sets this field)
/// deserializes as `Both` and behaves exactly as it always has. `Control` and
/// `Data` describe the two split-deployment shapes ADR 0035 targets
/// (`animusd control` / `animusd data`, PR3/PR4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Runs only the control-plane Raft (metadata: membership, tablet map,
    /// schema catalog, node address book) + placement/detector + client/admin
    /// endpoints. No storage engine, no `raftkv`/data-plane traffic.
    Control,
    /// Runs only the data plane: the shared LSM engine, the per-tablet Raft
    /// groups (stream-addressed on the same internal env), the tablet-host
    /// reconciler, and the client/DynamoDB/admin edges. No local control
    /// `RaftCore` — in the split deployment (PR4) this reads `Metadata` from
    /// a polled mirror of the control deployment instead.
    Data,
    /// Runs both roles in one process — today's only shape, and still the
    /// default: combined mode (`--cluster N`, `--config`/`--node`, growth,
    /// join) is unaffected by ADR 0035's config additions.
    #[default]
    Both,
}

impl NodeRole {
    /// Whether this role includes the control plane (`Control` or `Both`).
    #[must_use]
    pub fn has_control(self) -> bool {
        matches!(self, NodeRole::Control | NodeRole::Both)
    }

    /// Whether this role includes the data plane (`Data` or `Both`).
    #[must_use]
    pub fn has_data(self) -> bool {
        matches!(self, NodeRole::Data | NodeRole::Both)
    }
}

/// The client DynamoDB port's SigV4 credential store (ADR 0057): a static
/// `access_key_id → secret_access_key` map, loaded either from a
/// [`ClusterConfig`]'s own `dynamo_auth` section or from the file named by a
/// config-less startup mode's `--dynamo-auth PATH` flag (same JSON shape
/// either way — `{"credentials": {"AKID": "secret", ...}}`). Deliberately
/// minimal (ADR 0057's non-goals): no rotation, no dynamic credential API, no
/// secret-at-rest protection — the secret sits in plaintext in a config file
/// that is already trusted operator input.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DynamoAuthConfig {
    /// `access_key_id → secret_access_key`. A `BTreeMap` (ADR 0003
    /// determinism rules — no `HashMap` in logic, even config-shaped logic).
    pub credentials: BTreeMap<String, String>,
}

impl DynamoAuthConfig {
    /// A present `dynamo_auth` section with an empty credential map is a
    /// misconfiguration (every request would be rejected as an unknown
    /// access key, indistinguishable from auth simply being broken) — reject
    /// it at load time rather than at the first client request.
    ///
    /// # Errors
    /// Returns a message if `credentials` is empty.
    pub fn validate(&self) -> Result<(), String> {
        if self.credentials.is_empty() {
            return Err(
                "dynamo_auth is present but its credentials map is empty — omit the section \
                 entirely to disable auth, or list at least one access_key_id/secret_access_key \
                 pair"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// This node's TLS material (ADR 0064, S-01 commit 2) — a per-node
/// [`RoleAddrs`] field (unlike [`DynamoAuthConfig`], which is a single
/// cluster-wide credential map) because the cert/key files it names are
/// always this node's own, never shared across the cluster; only `ca_path`
/// is (conventionally) the same file on every node. `#[serde(default)]` on
/// [`RoleAddrs::tls`] so an absent section (every pre-ADR-0064 config)
/// deserializes as `None` — plain TCP on every port, byte-for-byte today's
/// behavior.
///
/// Same three-file shape as `animus_env::TlsConfig` (`cert_path`,
/// `key_path`, `ca_path`) but declared independently here rather than
/// reusing that type directly: this section also has to exist, and
/// round-trip through `serde_json`, in builds of `animusd` that don't
/// enable `animus-env`'s `prod` feature (`animus-env`'s TLS types live
/// behind that feature, ADR 0061 rung C0) — `animusd`'s own binary always
/// enables `prod`, but the config *type* shouldn't have to assume that.
/// `TlsSection::ca_path` is `Option`, matching `animus_env::TlsConfig`'s
/// own shape (mutual TLS on `internal`/`intra` requires it; server-only
/// TLS on `client`/`dynamo`/`admin`/`console` never reads it) —
/// `ClusterConfig::validate_tls` is what actually enforces "every node has
/// TLS or none does" across a whole config; per-port `ca_path` presence is
/// enforced port-by-port in `Node::bind*` (mutual ports error without one).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsSection {
    /// PEM file: this node's own certificate (leaf, optionally followed by
    /// intermediates).
    pub cert_path: PathBuf,
    /// PEM file: this node's private key.
    pub key_path: PathBuf,
    /// PEM file: the cluster CA certificate(s) — required for the mutual
    /// `internal`/`intra` ports; unused for the server-only `client`/
    /// `dynamo`/`admin`/`console` ports.
    #[serde(default)]
    pub ca_path: Option<PathBuf>,
}

impl TlsSection {
    /// Build the `animus-env` [`animus_env::TlsConfig`] this section
    /// describes (a plain field-for-field copy — the two types exist
    /// independently only so this crate's own config type doesn't have to
    /// assume `animus-env`'s `prod` feature is on just to round-trip
    /// through `serde_json`, see this type's own doc; `animusd` itself
    /// always enables that feature).
    #[must_use]
    pub fn to_tls_config(&self) -> animus_env::TlsConfig {
        animus_env::TlsConfig {
            cert_path: self.cert_path.clone(),
            key_path: self.key_path.clone(),
            ca_path: self.ca_path.clone(),
        }
    }
}

/// Cluster-wide operational knobs reachable from a [`ClusterConfig`] file
/// (S-06) — previously these reached only `animusd --cluster N`'s in-process
/// dev CLI flags (`run_in_process_cluster`/`run_in_process_split_cluster`),
/// never a per-process real deployment (`--config`/`--node`, `animusd
/// control`, `animusd data`). Every field is `#[serde(default)]` so an
/// absent `cluster_settings` section (every pre-S-06 config) deserializes
/// with every knob defaulting exactly as omitting its CLI flag already
/// does. Units and semantics mirror the corresponding `animusd` CLI flag
/// byte-for-byte (`main.rs`'s own module doc) — a `_secs` field is the
/// flag's raw seconds value, never a [`std::time::Duration`], so this type
/// stays trivially `Serialize`/`Deserialize`.
///
/// **Not every field applies on every deployment shape** — a node applies
/// only the subset its own role can act on, and silently ignores the rest
/// (the same config file is commonly deployed to every process in a
/// cluster, control-only and data-only alike, so an inapplicable field
/// present in it is normal, not a misconfiguration):
/// - `orphan_sweep_after_secs` only matters to a node that runs a local
///   control `RaftNode` (a combined `--config`/`--node` node or `animusd
///   control`) — a data-only node has none and ignores it.
/// - `stream_retention_secs` only matters to whichever node happens to be
///   the control-plane **leader** (the segment janitor's own gate) — a
///   data-only node's `ControlHandle` is always `Remote`, so it never runs
///   that loop and ignores the field too.
/// - The other eight fields (`auto_split_bytes`, `auto_split_change_rate`,
///   `auto_split_ops_rate`, `quiesce_after_secs`, `stream_seal_bytes`,
///   `stream_seal_age_secs`, `throttle_read_units`, `throttle_write_units`)
///   apply to any data-hosting node (combined or data-only).
///
/// A CLI flag naming the same knob **and** a config file's `cluster_
/// settings` section setting it is a hard startup error, checked field by
/// field — the identical "specify it one way, not both" shape
/// `apply_dynamo_auth_flag` (ADR 0057) already uses for `dynamo_auth`,
/// never a silent precedence rule (see `main.rs`'s
/// `resolve_cluster_settings`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClusterSettings {
    /// `--auto-split-bytes B` (ADR 0034): a led tablet's own scoped bytes
    /// threshold that triggers an auto-split.
    #[serde(default)]
    pub auto_split_bytes: Option<u64>,
    /// `--auto-split-change-rate RATE` (ADR 0042 §14): a streamed led
    /// tablet's own smoothed change-append rate (bytes/sec) threshold that
    /// also triggers an auto-split.
    #[serde(default)]
    pub auto_split_change_rate: Option<u64>,
    /// `--auto-split-ops-rate RATE` (W-09, ADR 0034 amendment): a led
    /// tablet's own smoothed **write** request rate (ops/sec) threshold
    /// that also triggers an auto-split — unlike `auto_split_change_rate`,
    /// this applies to any table, not just a streamed one.
    #[serde(default)]
    pub auto_split_ops_rate: Option<u64>,
    /// `--orphan-sweep-after SECS` (ADR 0040 PR6): the control-plane
    /// leader's own never-activated-registration reclaim grace period; `0`
    /// disables the sweep.
    #[serde(default)]
    pub orphan_sweep_after_secs: Option<u64>,
    /// `--quiesce-after SECS` (ADR 0044 phase-1 PR7 / ADR 0048): the
    /// idle-before-quiescing grace period for a data-plane CP group; `0`
    /// disables quiescence entirely.
    #[serde(default)]
    pub quiesce_after_secs: Option<u64>,
    /// `--stream-seal-bytes B` (ADR 0042 §13): the DynamoDB Streams
    /// sealer's size trigger.
    #[serde(default)]
    pub stream_seal_bytes: Option<u64>,
    /// `--stream-seal-age SECS` (ADR 0042 §13): the DynamoDB Streams
    /// sealer's age trigger.
    #[serde(default)]
    pub stream_seal_age_secs: Option<u64>,
    /// `--stream-retention SECS` (ADR 0042 §13 / ADR 0043 §A9): the segment
    /// janitor's own retention grace period.
    #[serde(default)]
    pub stream_retention_secs: Option<u64>,
    /// `--throttle-read-units N` (ADR 0065 §5(a), W-08 step 4): the
    /// cluster-wide default read-capacity-units budget applied to any table
    /// that has not set its own `ProvisionedThroughput` — seeds `ClientCtx`'s
    /// [`ThrottleDefaults`](crate::ThrottleDefaults) at node start. `None`
    /// (the default) means `PAY_PER_REQUEST` — no throttling — byte-for-byte
    /// unchanged from before ADR 0065.
    #[serde(default)]
    pub throttle_read_units: Option<u64>,
    /// `--throttle-write-units N` (ADR 0065 §5(a), W-08 step 4): the
    /// write-capacity-units sibling of
    /// [`throttle_read_units`](Self::throttle_read_units).
    #[serde(default)]
    pub throttle_write_units: Option<u64>,
}

/// A whole-cluster configuration shared (identically) by every node's process.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Per-node listen addresses, indexed by node index.
    pub nodes: Vec<RoleAddrs>,
    /// The client DynamoDB port's SigV4 credential store (ADR 0057) —
    /// `None` (every existing config) disables auth entirely, byte-identical
    /// to pre-ADR-0057 behavior. See [`DynamoAuthConfig`]'s own doc.
    #[serde(default)]
    pub dynamo_auth: Option<DynamoAuthConfig>,
    /// Cluster-wide operational knobs (auto-split, quiesce, orphan-sweep,
    /// stream-seal — S-06) — `None` (every pre-S-06 config) leaves every
    /// knob at its CLI-flag-omitted default, byte-identical to before this
    /// section existed. See [`ClusterSettings`]'s own doc for field-by-field
    /// applicability and the CLI-flag-conflict contract.
    #[serde(default)]
    pub cluster_settings: Option<ClusterSettings>,
}

impl ClusterConfig {
    /// Generate a **combined-mode** config for `n` nodes on `host`, assigning
    /// each node six consecutive ports starting at `base_port` (node `i`
    /// uses `base_port + 6*i .. +6`): internal, client, dynamo, admin
    /// (the admin/debug interface, ADR 0020), intra (the cluster-internal RPC
    /// port, ADR 0047), console (the DynamoDB-shaped data app, ADR 0052).
    /// Every node is [`NodeRole::Both`] — see [`generate_split`] for the
    /// split-deployment shape.
    ///
    /// [`generate_split`]: Self::generate_split
    #[must_use]
    pub fn generate(n: usize, host: IpAddr, base_port: u16) -> Self {
        let nodes = (0..n)
            .map(|i| {
                let p = |role: u16| SocketAddr::new(host, base_port + (i as u16) * 6 + role);
                RoleAddrs {
                    id: minted_id(i, n),
                    role: NodeRole::Both,
                    internal: p(0),
                    client: p(1),
                    dynamo: p(2),
                    admin: p(3),
                    intra: p(4),
                    console: p(5),
                    advertise_host: None,
                    tls: None,
                }
            })
            .collect();
        Self {
            nodes,
            dynamo_auth: None,
            cluster_settings: None,
        }
    }

    /// Generate a **split-deployment** config (ADR 0035 target topology):
    /// `control_n` control-only nodes followed by `data_n` data-only nodes,
    /// all on `host` starting at `base_port`. Each node still gets a full
    /// six-port block (same stride as [`generate`](Self::generate)) so the
    /// two generators stay trivially comparable.
    #[must_use]
    pub fn generate_split(control_n: usize, data_n: usize, host: IpAddr, base_port: u16) -> Self {
        let total = control_n + data_n;
        let nodes = (0..total)
            .map(|i| {
                let p = |role: u16| SocketAddr::new(host, base_port + (i as u16) * 6 + role);
                let role = if i < control_n {
                    NodeRole::Control
                } else {
                    NodeRole::Data
                };
                RoleAddrs {
                    id: minted_id(i, total),
                    role,
                    internal: p(0),
                    client: p(1),
                    dynamo: p(2),
                    admin: p(3),
                    intra: p(4),
                    console: p(5),
                    advertise_host: None,
                    tls: None,
                }
            })
            .collect();
        Self {
            nodes,
            dynamo_auth: None,
            cluster_settings: None,
        }
    }

    /// Number of nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the cluster is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The indices of nodes that run the control role (`Control` or `Both`).
    #[must_use]
    pub fn control_indexes(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, a)| a.role.has_control())
            .map(|(i, _)| i)
            .collect()
    }

    /// The indices of nodes that run the data role (`Data` or `Both`) — the
    /// ADR 0035 dual of [`control_indexes`](Self::control_indexes).
    #[must_use]
    pub fn data_indexes(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, a)| a.role.has_data())
            .map(|(i, _)| i)
            .collect()
    }

    /// The control-plane Raft membership: the ids of nodes that actually run
    /// the control role. In combined mode (every node `Both`) this is
    /// unchanged — `0..len()`.
    #[must_use]
    pub fn control_ids(&self) -> Vec<NodeId> {
        self.control_indexes()
            .into_iter()
            .map(|i| self.nodes[i].id.clone())
            .collect()
    }

    /// The ids of nodes that actually run the data role — the universe from
    /// which a CP tablet group's replica set is drawn (ADR 0017 #3a), and the
    /// set `bootstrap` auto-registers as `Active` data members. In combined
    /// mode this is unchanged — every node's id.
    #[must_use]
    pub fn data_ids(&self) -> Vec<NodeId> {
        self.data_indexes()
            .into_iter()
            .map(|i| self.nodes[i].id.clone())
            .collect()
    }

    /// The whole cluster's internal peer address book: every node's id → its
    /// one internal `ProdEnv` address (ADR 0040 PR1 — one identity per node,
    /// replacing the separate `control_peer_book`/`raftkv_peer_book`/
    /// `peer_book` triad ADR 0035 grew: with one shared env per node there is
    /// only one internal network to build a peer book for). Every node —
    /// control-only, data-only, or combined — contributes its own entry, since
    /// every role needs the internal env (control Raft, per-tablet Raft
    /// groups, and failure-detection heartbeats all ride it).
    #[must_use]
    pub fn peer_book(&self) -> BTreeMap<NodeId, String> {
        self.nodes
            .iter()
            .map(|a| {
                (
                    a.id.clone(),
                    crate::advertised_addr(a.advertise_host.as_deref(), a.internal),
                )
            })
            .collect()
    }

    /// Serialize to pretty JSON.
    ///
    /// # Panics
    /// Never in practice (the config is plain serializable data).
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("config serializes")
    }

    /// Parse from JSON.
    ///
    /// # Errors
    /// Returns a `serde_json` error if the text is not a valid config, if
    /// two entries claim the same [`RoleAddrs::id`] (ADR 0040 PR3: ids are
    /// now explicit and must be unique — a duplicate is a hard load-time
    /// error, not a silently-shadowed entry), if a present `dynamo_auth`
    /// section has an empty credentials map (ADR 0057 — see
    /// [`DynamoAuthConfig::validate`]), or if only some nodes carry a `tls`
    /// section (ADR 0064 — see [`Self::validate_tls`]).
    pub fn from_json(text: &str) -> serde_json::Result<Self> {
        let cfg: Self = serde_json::from_str(text)?;
        let mut seen = std::collections::BTreeSet::new();
        for n in &cfg.nodes {
            if !seen.insert(n.id.clone()) {
                return Err(serde_json::Error::custom(format!(
                    "duplicate node id {:?} in config",
                    n.id
                )));
            }
        }
        if let Some(auth) = &cfg.dynamo_auth {
            auth.validate().map_err(serde_json::Error::custom)?;
        }
        cfg.validate_tls().map_err(serde_json::Error::custom)?;
        Ok(cfg)
    }

    /// ADR 0064's "a cluster is either all-TLS or all-plain on the internal
    /// wire" rule: every node's `tls` section is present, or none is.
    /// `internal`/`intra` are mutual-TLS-only ports shared by a Raft
    /// group's own peers (the control group, and every per-tablet group) —
    /// a group with some members dialing in plaintext and others requiring
    /// a handshake would either silently drop half its peers or accept
    /// unauthenticated connections from the other half, neither of which is
    /// a coherent security posture (see ADR 0064 Decision 3). The
    /// client/admin/console ports have no such constraint (each is
    /// independently server-only TLS, checked instead at bind time per
    /// node) — only this cross-node internal-wire rule needs a whole-config
    /// check.
    ///
    /// # Errors
    /// Returns a message naming the first node whose `tls` presence
    /// disagrees with the first node in the list, unless every node agrees.
    pub fn validate_tls(&self) -> Result<(), String> {
        let mut nodes = self.nodes.iter();
        let Some(first) = nodes.next() else {
            return Ok(());
        };
        let first_has_tls = first.tls.is_some();
        for n in nodes {
            if n.tls.is_some() != first_has_tls {
                return Err(format!(
                    "mixed TLS configuration: node {:?} {} a tls section but node {:?} {} \
                     one — a cluster must be all-TLS or all-plain on the internal wire (ADR 0064)",
                    n.id,
                    if n.tls.is_some() { "has" } else { "has no" },
                    first.id,
                    if first_has_tls { "has" } else { "has no" },
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_config_has_distinct_sequential_ports() {
        let cfg = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 7000);
        assert_eq!(cfg.len(), 3);
        assert_eq!(cfg.nodes[0].internal.port(), 7000);
        assert_eq!(cfg.nodes[0].client.port(), 7001);
        assert_eq!(cfg.nodes[0].dynamo.port(), 7002);
        assert_eq!(cfg.nodes[0].admin.port(), 7003);
        assert_eq!(cfg.nodes[0].intra.port(), 7004);
        assert_eq!(cfg.nodes[0].console.port(), 7005);
        assert_eq!(cfg.nodes[1].internal.port(), 7006);
        assert_eq!(cfg.nodes[2].internal.port(), 7012);
    }

    #[test]
    fn generated_config_is_combined_mode() {
        let cfg = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 7000);
        assert!(cfg.nodes.iter().all(|a| a.role == NodeRole::Both));
        assert_eq!(cfg.control_ids(), vec![nid(0), nid(1), nid(2)]);
        assert_eq!(cfg.data_ids(), vec![nid(0), nid(1), nid(2)]);
    }

    #[test]
    fn peer_book_covers_every_node() {
        let cfg = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 7000);
        let book = cfg.peer_book();
        assert_eq!(book.len(), 3, "3 nodes, one identity/address each");
        // No `advertise_host` set (`generate` never sets one) — every entry
        // is the bind address's own `host:port` string.
        let port = |s: &str| s.rsplit(':').next().unwrap().parse::<u16>().unwrap();
        assert_eq!(port(&book[&node_id(1)]), 7006);
        assert_eq!(port(&book[&node_id(0)]), 7000);
        assert_eq!(port(&book[&node_id(2)]), 7012);
        // Client / dynamo / admin / intra / console addresses are
        // intentionally absent from the internal book (external client
        // channels, not the network).
        assert!(!book.values().any(|a| port(a) == 7001)); // client (node 0)
        assert!(!book.values().any(|a| port(a) == 7002)); // dynamo (node 0)
        assert!(!book.values().any(|a| port(a) == 7003)); // admin (node 0)
        assert!(!book.values().any(|a| port(a) == 7004)); // intra (node 0)
        assert!(!book.values().any(|a| port(a) == 7005)); // console (node 0)
    }

    #[test]
    fn json_round_trips() {
        let cfg = ClusterConfig::generate(2, "10.0.0.1".parse().unwrap(), 9000);
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.nodes[1].internal, cfg.nodes[1].internal);
    }

    #[test]
    fn ids_follow_convention() {
        assert_eq!(node_id(2), nid(2));
        assert_eq!(node_id(0), nid(0));
    }

    #[test]
    fn node_role_defaults_to_both_and_gates_correctly() {
        assert_eq!(NodeRole::default(), NodeRole::Both);
        assert!(NodeRole::Both.has_control());
        assert!(NodeRole::Both.has_data());
        assert!(NodeRole::Control.has_control());
        assert!(!NodeRole::Control.has_data());
        assert!(!NodeRole::Data.has_control());
        assert!(NodeRole::Data.has_data());
    }

    #[test]
    fn mixed_topology_derivations_are_role_scoped() {
        // 2 control-only + 3 data-only nodes, indices 0..5.
        let cfg = ClusterConfig::generate_split(2, 3, "127.0.0.1".parse().unwrap(), 8000);
        assert_eq!(cfg.len(), 5);
        assert_eq!(cfg.control_indexes(), vec![0, 1]);
        assert_eq!(cfg.data_indexes(), vec![2, 3, 4]);
        assert_eq!(cfg.control_ids(), vec![node_id(0), node_id(1)]);
        assert_eq!(cfg.data_ids(), vec![node_id(2), node_id(3), node_id(4)]);
    }

    #[test]
    fn peer_book_covers_every_node_on_a_mixed_topology() {
        let cfg = ClusterConfig::generate_split(2, 3, "127.0.0.1".parse().unwrap(), 8000);
        let book = cfg.peer_book();
        assert_eq!(
            book.len(),
            5,
            "every node, regardless of role, gets one entry"
        );
        for i in 0..5 {
            assert!(book.contains_key(&node_id(i)));
        }
    }

    #[test]
    fn generated_ids_are_zero_padded_at_ten_or_more_nodes() {
        // ADR 0040 §6 call-out #7: `"n10" < "n2"` lexicographically, so a
        // generated config of >= 10 nodes must zero-pad to keep id order
        // matching index order.
        let cfg = ClusterConfig::generate(11, "127.0.0.1".parse().unwrap(), 7000);
        let ids: Vec<String> = cfg.nodes.iter().map(|n| n.id.to_string()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "lexicographic id order must match index order");
        assert_eq!(cfg.nodes[0].id.to_string(), "n00");
        assert_eq!(cfg.nodes[10].id.to_string(), "n10");
    }

    #[test]
    fn below_ten_nodes_ids_stay_unpadded() {
        // Below the zero-pad threshold, `generate` must stay byte-identical
        // to the long-standing `node_id`/`nid` convention every existing
        // test relies on.
        let cfg = ClusterConfig::generate(9, "127.0.0.1".parse().unwrap(), 7000);
        assert_eq!(cfg.nodes[0].id, node_id(0));
        assert_eq!(cfg.nodes[8].id, node_id(8));
        assert_eq!(cfg.nodes[8].id.to_string(), "n8");
    }

    #[test]
    fn duplicate_ids_are_rejected_at_load() {
        let mut cfg = ClusterConfig::generate(2, "127.0.0.1".parse().unwrap(), 7000);
        cfg.nodes[1].id = cfg.nodes[0].id.clone();
        let err = ClusterConfig::from_json(&cfg.to_json())
            .expect_err("a config with two entries claiming the same id must be rejected");
        assert!(err.to_string().contains("duplicate node id"));
    }

    #[test]
    fn dynamo_auth_defaults_to_none_and_round_trips_absent() {
        // ADR 0057: a generated config carries no `dynamo_auth` section, and
        // it survives a JSON round trip as `None` — byte-identical to
        // pre-ADR-0057 behavior.
        let cfg = ClusterConfig::generate(2, "127.0.0.1".parse().unwrap(), 7000);
        assert!(cfg.dynamo_auth.is_none());
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        assert!(parsed.dynamo_auth.is_none());

        // An old-shaped config JSON with no `dynamo_auth` key at all must
        // still parse (the field is additive via `#[serde(default)]`, never
        // a breaking requirement on an existing config file).
        let bare = serde_json::json!({ "nodes": cfg.nodes }).to_string();
        let parsed = ClusterConfig::from_json(&bare).unwrap();
        assert!(parsed.dynamo_auth.is_none());
    }

    #[test]
    fn dynamo_auth_with_credentials_round_trips() {
        let mut cfg = ClusterConfig::generate(1, "127.0.0.1".parse().unwrap(), 7000);
        let mut credentials = BTreeMap::new();
        credentials.insert("AKIDEXAMPLE".to_string(), "secret".to_string());
        cfg.dynamo_auth = Some(DynamoAuthConfig { credentials });
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        let auth = parsed.dynamo_auth.expect("dynamo_auth survives round trip");
        assert_eq!(auth.credentials.get("AKIDEXAMPLE").unwrap(), "secret");
    }

    #[test]
    fn empty_dynamo_auth_credentials_rejected_at_load() {
        let mut cfg = ClusterConfig::generate(1, "127.0.0.1".parse().unwrap(), 7000);
        cfg.dynamo_auth = Some(DynamoAuthConfig {
            credentials: BTreeMap::new(),
        });
        let err = ClusterConfig::from_json(&cfg.to_json())
            .expect_err("an empty dynamo_auth credentials map must be rejected at load");
        assert!(err.to_string().contains("credentials map is empty"));
    }

    // --- `cluster_settings` (S-06) ---------------------------------------

    #[test]
    fn cluster_settings_defaults_to_none_and_round_trips_absent() {
        // A generated config carries no `cluster_settings` section, and it
        // survives a JSON round trip as `None` — byte-identical to
        // pre-S-06 behavior.
        let cfg = ClusterConfig::generate(2, "127.0.0.1".parse().unwrap(), 7000);
        assert!(cfg.cluster_settings.is_none());
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        assert!(parsed.cluster_settings.is_none());

        // An old-shaped config JSON with no `cluster_settings` key at all
        // must still parse (`#[serde(default)]`, never a breaking
        // requirement on an existing config file).
        let bare = serde_json::json!({ "nodes": cfg.nodes }).to_string();
        let parsed = ClusterConfig::from_json(&bare).unwrap();
        assert!(parsed.cluster_settings.is_none());
    }

    #[test]
    fn cluster_settings_round_trips_every_field() {
        let mut cfg = ClusterConfig::generate(1, "127.0.0.1".parse().unwrap(), 7000);
        cfg.cluster_settings = Some(ClusterSettings {
            auto_split_bytes: Some(1_000_000),
            auto_split_change_rate: Some(500),
            auto_split_ops_rate: Some(200),
            orphan_sweep_after_secs: Some(120),
            quiesce_after_secs: Some(10),
            stream_seal_bytes: Some(4_194_304),
            stream_seal_age_secs: Some(3600),
            stream_retention_secs: Some(86_400),
            throttle_read_units: Some(50),
            throttle_write_units: Some(25),
        });
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        assert_eq!(parsed.cluster_settings, cfg.cluster_settings);
    }

    #[test]
    fn cluster_settings_partial_section_defaults_the_rest() {
        // A hand-written config that sets only one field must not require
        // every other field — each is independently `#[serde(default)]`.
        let cfg = ClusterConfig::generate(1, "127.0.0.1".parse().unwrap(), 7000);
        let text = serde_json::json!({
            "nodes": cfg.nodes,
            "cluster_settings": { "auto_split_bytes": 2_000_000 }
        })
        .to_string();
        let parsed = ClusterConfig::from_json(&text).unwrap();
        let settings = parsed
            .cluster_settings
            .expect("a present section parses even with only one field set");
        assert_eq!(settings.auto_split_bytes, Some(2_000_000));
        assert_eq!(settings.quiesce_after_secs, None);
        assert_eq!(settings.orphan_sweep_after_secs, None);
        assert_eq!(settings.stream_seal_bytes, None);
        assert_eq!(settings.stream_seal_age_secs, None);
        assert_eq!(settings.stream_retention_secs, None);
        assert_eq!(settings.auto_split_change_rate, None);
        assert_eq!(settings.auto_split_ops_rate, None);
        assert_eq!(settings.throttle_read_units, None);
        assert_eq!(settings.throttle_write_units, None);
    }

    // --- `tls` (ADR 0064, S-01 commit 2) ----------------------------------

    fn tls_section(tag: &str) -> TlsSection {
        TlsSection {
            cert_path: format!("/etc/animusd/tls/{tag}.cert.pem").into(),
            key_path: format!("/etc/animusd/tls/{tag}.key.pem").into(),
            ca_path: Some("/etc/animusd/tls/ca.pem".into()),
        }
    }

    #[test]
    fn tls_defaults_to_none_and_round_trips_absent() {
        // A generated config carries no `tls` section on any node, and it
        // survives a JSON round trip as `None` — byte-identical to
        // pre-ADR-0064 behavior.
        let cfg = ClusterConfig::generate(2, "127.0.0.1".parse().unwrap(), 7000);
        assert!(cfg.nodes.iter().all(|n| n.tls.is_none()));
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        assert!(parsed.nodes.iter().all(|n| n.tls.is_none()));

        // An old-shaped node JSON object with no `tls` key at all must
        // still parse (`#[serde(default)]`, never a breaking requirement).
        let bare = serde_json::json!({ "nodes": cfg.nodes }).to_string();
        let parsed = ClusterConfig::from_json(&bare).unwrap();
        assert!(parsed.nodes.iter().all(|n| n.tls.is_none()));
    }

    #[test]
    fn tls_section_round_trips_with_and_without_ca_path() {
        let mut cfg = ClusterConfig::generate(2, "127.0.0.1".parse().unwrap(), 7000);
        cfg.nodes[0].tls = Some(tls_section("n0"));
        // `ca_path` is `Option` on the type itself (server-only TLS on the
        // client/dynamo/admin/console ports never reads it) — a section
        // with no CA must still round-trip even though this whole-config
        // path never validates per-port ca_path presence.
        cfg.nodes[1].tls = Some(TlsSection {
            cert_path: "/etc/animusd/tls/n1.cert.pem".into(),
            key_path: "/etc/animusd/tls/n1.key.pem".into(),
            ca_path: None,
        });
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        assert_eq!(parsed.nodes[0].tls, cfg.nodes[0].tls);
        assert_eq!(parsed.nodes[1].tls, cfg.nodes[1].tls);
        assert!(parsed.nodes[1].tls.as_ref().unwrap().ca_path.is_none());
    }

    #[test]
    fn validate_tls_accepts_all_nodes_plain_or_all_nodes_tls() {
        let plain = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 7000);
        plain.validate_tls().expect("all-plain must validate");

        let mut all_tls = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 7000);
        for (i, node) in all_tls.nodes.iter_mut().enumerate() {
            node.tls = Some(tls_section(&format!("n{i}")));
        }
        all_tls.validate_tls().expect("all-tls must validate");
        // Also reachable through `from_json`, which calls `validate_tls`
        // itself (this is what makes a mixed config a *load-time* error,
        // not something discovered only at the first failed handshake).
        ClusterConfig::from_json(&all_tls.to_json()).expect("all-tls config file must load");
    }

    #[test]
    fn validate_tls_rejects_a_mixed_config() {
        let mut cfg = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 7000);
        cfg.nodes[1].tls = Some(tls_section("n1"));
        let err = cfg
            .validate_tls()
            .expect_err("one node with tls and two without must be rejected");
        assert!(err.contains("mixed TLS configuration"), "{err}");

        // And the same rejection happens automatically at `from_json` load
        // time — an operator never gets past parsing a mixed config file.
        let load_err = ClusterConfig::from_json(&cfg.to_json())
            .expect_err("from_json must reject a mixed-tls config file");
        assert!(load_err.to_string().contains("mixed TLS configuration"));
    }

    #[test]
    fn validate_tls_is_a_no_op_on_an_empty_node_list() {
        let cfg = ClusterConfig {
            nodes: vec![],
            dynamo_auth: None,
            cluster_settings: None,
        };
        cfg.validate_tls()
            .expect("no nodes means nothing to disagree");
    }
}
