//! Cluster configuration for per-process deployment.
//!
//! A [`ClusterConfig`] lists every node's five listen addresses (the control +
//! **raftkv** internal `ProdEnv` roles + the client API + the DynamoDB HTTP and
//! CQL endpoints). One `animusd` process runs one node by index: it binds *its*
//! listeners at the configured addresses and learns every peer's address from the
//! same config.
//!
//! Node ids follow a fixed convention derived from the node's index, so all
//! processes agree without listing ids explicitly: control `i`, raftkv `300 + i`.
//! v1 (ADR 0019) is CP-only — the leaderless AP `data`/`coord` roles are gone.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

use crate::RoleAddrs;

/// Id offset for a node's **leaderful CP** per-tablet Raft role (ADR 0017 #3a) —
/// the data plane. A node hosting a CP tablet group runs a
/// [`RaftKvNode`](animus_raftdata::RaftKvNode) on this id (its own `ProdEnv`/inbox,
/// distinct from the control role). Offset well above the control ids so the two
/// roles never collide.
pub const RAFTKV_ID_BASE: NodeId = 300;

/// The control-plane Raft id for node `index`.
#[must_use]
pub fn control_id(index: usize) -> NodeId {
    index as NodeId
}
/// The leaderful CP per-tablet Raft id for node `index` (ADR 0017 #3a).
#[must_use]
pub fn raftkv_id(index: usize) -> NodeId {
    RAFTKV_ID_BASE + index as NodeId
}

/// A whole-cluster configuration shared (identically) by every node's process.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Per-node listen addresses, indexed by node index.
    pub nodes: Vec<RoleAddrs>,
}

impl ClusterConfig {
    /// Generate a config for `n` nodes on `host`, assigning each node five
    /// consecutive ports starting at `base_port` (node `i` uses
    /// `base_port + 5*i .. +5`): control, client, dynamo, cql, raftkv.
    #[must_use]
    pub fn generate(n: usize, host: IpAddr, base_port: u16) -> Self {
        let nodes = (0..n)
            .map(|i| {
                let p = |role: u16| SocketAddr::new(host, base_port + (i as u16) * 5 + role);
                RoleAddrs {
                    control: p(0),
                    client: p(1),
                    dynamo: p(2),
                    cql: p(3),
                    raftkv: p(4),
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

    /// The control-plane Raft membership (all control ids).
    #[must_use]
    pub fn control_ids(&self) -> Vec<NodeId> {
        (0..self.nodes.len()).map(control_id).collect()
    }

    /// The leaderful CP per-tablet Raft ids (one per node) — the universe from
    /// which a CP tablet group's replica set is drawn (ADR 0017 #3a).
    #[must_use]
    pub fn raftkv_ids(&self) -> Vec<NodeId> {
        (0..self.nodes.len()).map(raftkv_id).collect()
    }

    /// The peer address book every node installs: each node's control and raftkv
    /// ids mapped to their addresses. (Client/dynamo/cql addresses are external
    /// client channels, not part of the internal network.)
    #[must_use]
    pub fn peer_book(&self) -> BTreeMap<NodeId, SocketAddr> {
        let mut book = BTreeMap::new();
        for (i, addrs) in self.nodes.iter().enumerate() {
            book.insert(control_id(i), addrs.control);
            book.insert(raftkv_id(i), addrs.raftkv);
        }
        book
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
        assert_eq!(cfg.nodes[0].control.port(), 7000);
        assert_eq!(cfg.nodes[0].client.port(), 7001);
        assert_eq!(cfg.nodes[0].dynamo.port(), 7002);
        assert_eq!(cfg.nodes[0].cql.port(), 7003);
        assert_eq!(cfg.nodes[0].raftkv.port(), 7004);
        assert_eq!(cfg.nodes[1].control.port(), 7005);
        assert_eq!(cfg.nodes[2].raftkv.port(), 7014);
    }

    #[test]
    fn peer_book_covers_all_internal_roles() {
        let cfg = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 7000);
        let book = cfg.peer_book();
        assert_eq!(
            book.len(),
            6,
            "3 nodes x 2 internal roles (control + raftkv)"
        );
        // Conventional ids resolve to the right ports.
        assert_eq!(book[&control_id(1)].port(), 7005);
        assert_eq!(book[&raftkv_id(0)].port(), 7004);
        assert_eq!(book[&raftkv_id(2)].port(), 7014);
        // Client / dynamo / cql addresses are intentionally absent from the
        // internal book (they are external client channels, not the network).
        assert!(!book.values().any(|a| a.port() == 7001)); // client (node 0)
        assert!(!book.values().any(|a| a.port() == 7002)); // dynamo (node 0)
        assert!(!book.values().any(|a| a.port() == 7003)); // cql (node 0)
    }

    #[test]
    fn json_round_trips() {
        let cfg = ClusterConfig::generate(2, "10.0.0.1".parse().unwrap(), 9000);
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.nodes[1].raftkv, cfg.nodes[1].raftkv);
    }

    #[test]
    fn ids_follow_convention() {
        assert_eq!(control_id(2), 2);
        assert_eq!(raftkv_id(0), RAFTKV_ID_BASE);
        assert_eq!(raftkv_id(2), RAFTKV_ID_BASE + 2);
    }
}
