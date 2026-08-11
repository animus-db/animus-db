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

use animus_env::NodeId;
#[cfg(test)]
use animus_env::nid;
use serde::{Deserialize, Serialize};

use crate::RoleAddrs;

/// The node id for config index `index`. Ids are still plain `u64`s in this
/// PR (ADR 0040 PR1 only unifies the *identity*, not the type — see the ADR)
/// — a node's id is simply its position in the config.
#[must_use]
pub fn node_id(index: usize) -> NodeId {
    NodeId::new(index as u64)
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
    /// reconciler, and the client/DynamoDB/CQL/admin edges. No local control
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

/// A whole-cluster configuration shared (identically) by every node's process.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Per-node listen addresses, indexed by node index.
    pub nodes: Vec<RoleAddrs>,
}

impl ClusterConfig {
    /// Generate a **combined-mode** config for `n` nodes on `host`, assigning
    /// each node five consecutive ports starting at `base_port` (node `i` uses
    /// `base_port + 5*i .. +5`): internal, client, dynamo, cql, admin (the
    /// admin/debug interface, ADR 0020). Every node is [`NodeRole::Both`] —
    /// see [`generate_split`] for the split-deployment shape.
    ///
    /// [`generate_split`]: Self::generate_split
    #[must_use]
    pub fn generate(n: usize, host: IpAddr, base_port: u16) -> Self {
        let nodes = (0..n)
            .map(|i| {
                let p = |role: u16| SocketAddr::new(host, base_port + (i as u16) * 5 + role);
                RoleAddrs {
                    role: NodeRole::Both,
                    internal: p(0),
                    client: p(1),
                    dynamo: p(2),
                    cql: p(3),
                    admin: p(4),
                }
            })
            .collect();
        Self { nodes }
    }

    /// Generate a **split-deployment** config (ADR 0035 target topology):
    /// `control_n` control-only nodes followed by `data_n` data-only nodes,
    /// all on `host` starting at `base_port`. Each node still gets a full
    /// five-port block (same stride as [`generate`](Self::generate)) so the
    /// two generators stay trivially comparable.
    #[must_use]
    pub fn generate_split(control_n: usize, data_n: usize, host: IpAddr, base_port: u16) -> Self {
        let total = control_n + data_n;
        let nodes = (0..total)
            .map(|i| {
                let p = |role: u16| SocketAddr::new(host, base_port + (i as u16) * 5 + role);
                let role = if i < control_n {
                    NodeRole::Control
                } else {
                    NodeRole::Data
                };
                RoleAddrs {
                    role,
                    internal: p(0),
                    client: p(1),
                    dynamo: p(2),
                    cql: p(3),
                    admin: p(4),
                }
            })
            .collect();
        Self { nodes }
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
        self.control_indexes().into_iter().map(node_id).collect()
    }

    /// The ids of nodes that actually run the data role — the universe from
    /// which a CP tablet group's replica set is drawn (ADR 0017 #3a), and the
    /// set `bootstrap` auto-registers as `Active` data members. In combined
    /// mode this is unchanged — every node's id.
    #[must_use]
    pub fn data_ids(&self) -> Vec<NodeId> {
        self.data_indexes().into_iter().map(node_id).collect()
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
    pub fn peer_book(&self) -> BTreeMap<NodeId, SocketAddr> {
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, a)| (node_id(i), a.internal))
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
    /// Returns a `serde_json` error if the text is not a valid config.
    pub fn from_json(text: &str) -> serde_json::Result<Self> {
        serde_json::from_str(text)
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
        assert_eq!(cfg.nodes[0].cql.port(), 7003);
        assert_eq!(cfg.nodes[0].admin.port(), 7004);
        assert_eq!(cfg.nodes[1].internal.port(), 7005);
        assert_eq!(cfg.nodes[2].internal.port(), 7010);
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
        assert_eq!(book[&node_id(1)].port(), 7005);
        assert_eq!(book[&node_id(0)].port(), 7000);
        assert_eq!(book[&node_id(2)].port(), 7010);
        // Client / dynamo / cql / admin addresses are intentionally absent
        // from the internal book (external client channels, not the network).
        assert!(!book.values().any(|a| a.port() == 7001)); // client (node 0)
        assert!(!book.values().any(|a| a.port() == 7002)); // dynamo (node 0)
        assert!(!book.values().any(|a| a.port() == 7003)); // cql (node 0)
        assert!(!book.values().any(|a| a.port() == 7004)); // admin (node 0)
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
}
