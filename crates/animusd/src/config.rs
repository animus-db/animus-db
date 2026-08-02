//! Cluster configuration for per-process deployment.
//!
//! A [`ClusterConfig`] lists every node's six listen addresses (control / data
//! / coord internal `ProdEnv` roles + the client API + the DynamoDB HTTP and CQL
//! endpoints) plus the quorum sizes. One `animusd` process runs one node by
//! index: it binds *its* listeners at the configured addresses and learns every
//! peer's address from the same config.
//!
//! Node ids follow a fixed convention derived from the node's index, so all
//! processes agree without listing ids explicitly: control `i`, data `100 + i`,
//! coord `200 + i`.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

use crate::RoleAddrs;

/// Id offset for a node's data-replica role.
pub const DATA_ID_BASE: NodeId = 100;
/// Id offset for a node's coordinator role.
pub const COORD_ID_BASE: NodeId = 200;

/// The control-plane Raft id for node `index`.
#[must_use]
pub fn control_id(index: usize) -> NodeId {
    index as NodeId
}
/// The data-replica id for node `index`.
#[must_use]
pub fn data_id(index: usize) -> NodeId {
    DATA_ID_BASE + index as NodeId
}
/// The coordinator id for node `index`.
#[must_use]
pub fn coord_id(index: usize) -> NodeId {
    COORD_ID_BASE + index as NodeId
}

/// A whole-cluster configuration shared (identically) by every node's process.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Per-node listen addresses, indexed by node index.
    pub nodes: Vec<RoleAddrs>,
    /// Read quorum size.
    pub r: usize,
    /// Write quorum size.
    pub w: usize,
}

impl ClusterConfig {
    /// Generate a config for `n` nodes on `host`, assigning each node six
    /// consecutive ports starting at `base_port` (node `i` uses
    /// `base_port + 6*i .. +6`): control, data, coord, client, dynamo, cql.
    /// Quorum defaults to a majority (`R = W > N/2`).
    #[must_use]
    pub fn generate(n: usize, host: IpAddr, base_port: u16) -> Self {
        let nodes = (0..n)
            .map(|i| {
                let p = |role: u16| SocketAddr::new(host, base_port + (i as u16) * 6 + role);
                RoleAddrs {
                    control: p(0),
                    data: p(1),
                    coord: p(2),
                    client: p(3),
                    dynamo: p(4),
                    cql: p(5),
                }
            })
            .collect();
        let majority = n / 2 + 1;
        Self {
            nodes,
            r: majority,
            w: majority,
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

    /// The control-plane Raft membership (all control ids).
    #[must_use]
    pub fn control_ids(&self) -> Vec<NodeId> {
        (0..self.nodes.len()).map(control_id).collect()
    }

    /// The tablet replica set (all data ids).
    #[must_use]
    pub fn data_ids(&self) -> Vec<NodeId> {
        (0..self.nodes.len()).map(data_id).collect()
    }

    /// The `(control, data, coord)` ids for node `index`.
    #[must_use]
    pub fn role_ids(&self, index: usize) -> (NodeId, NodeId, NodeId) {
        (control_id(index), data_id(index), coord_id(index))
    }

    /// The peer address book every node installs: each node's control, data, and
    /// coord ids mapped to their addresses. (Client addresses are not part of
    /// the internal network.)
    #[must_use]
    pub fn peer_book(&self) -> BTreeMap<NodeId, SocketAddr> {
        let mut book = BTreeMap::new();
        for (i, addrs) in self.nodes.iter().enumerate() {
            book.insert(control_id(i), addrs.control);
            book.insert(data_id(i), addrs.data);
            book.insert(coord_id(i), addrs.coord);
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
        assert_eq!(cfg.r, 2);
        assert_eq!(cfg.w, 2);
        assert_eq!(cfg.nodes[0].control.port(), 7000);
        assert_eq!(cfg.nodes[0].client.port(), 7003);
        assert_eq!(cfg.nodes[0].dynamo.port(), 7004);
        assert_eq!(cfg.nodes[0].cql.port(), 7005);
        assert_eq!(cfg.nodes[1].control.port(), 7006);
        assert_eq!(cfg.nodes[2].coord.port(), 7014);
    }

    #[test]
    fn peer_book_covers_all_internal_roles() {
        let cfg = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 7000);
        let book = cfg.peer_book();
        assert_eq!(book.len(), 9, "3 nodes x 3 internal roles");
        // Conventional ids resolve to the right ports.
        assert_eq!(book[&control_id(1)].port(), 7006);
        assert_eq!(book[&data_id(1)].port(), 7007);
        assert_eq!(book[&coord_id(2)].port(), 7014);
        // Client / dynamo / cql addresses are intentionally absent from the
        // internal book (they are external client channels, not the network).
        assert!(!book.values().any(|a| a.port() == 7003)); // client (node 0)
        assert!(!book.values().any(|a| a.port() == 7004)); // dynamo (node 0)
        assert!(!book.values().any(|a| a.port() == 7005)); // cql (node 0)
    }

    #[test]
    fn json_round_trips() {
        let cfg = ClusterConfig::generate(2, "10.0.0.1".parse().unwrap(), 9000);
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.nodes[1].data, cfg.nodes[1].data);
        assert_eq!((parsed.r, parsed.w), (cfg.r, cfg.w));
    }

    #[test]
    fn role_ids_follow_convention() {
        let cfg = ClusterConfig::generate(1, "127.0.0.1".parse().unwrap(), 7000);
        assert_eq!(cfg.role_ids(0), (0, DATA_ID_BASE, COORD_ID_BASE));
    }
}
