//! Cluster configuration for per-process deployment.
//!
//! A [`ClusterConfig`] lists every node's addresses. One `animusd` process runs
//! one node by index: it binds *its* listeners at the configured addresses and
//! learns every peer's address from the same config.
//!
//! Node ids follow a fixed convention derived from the node's index, so all
//! processes agree without listing ids explicitly: control `i`, raftkv `300 + i`.
//! v1 (ADR 0019) is CP-only — the leaderless AP `data`/`coord` roles are gone.
//!
//! **ADR 0035 (control plane as a separate deployment)** adds [`NodeRole`]: a
//! [`RoleAddrs`](crate::RoleAddrs) entry now declares whether it runs the
//! control role, the data role, or both (`Both`, the default and — until this
//! ADR — the *only* shape). This module's job is the **config layer only**:
//! expressing and deriving from that per-node role. Actually assembling a
//! control-only or data-only *process* (skipping the unused listeners/engine
//! entirely) is later work in the ADR 0035 PR stack (PR3/PR4); today every
//! `animusd` entry point still runs in combined mode, where every node is
//! `Both` and nothing here changes behavior — the id scheme
//! (`control_id(i) = i`, `raftkv_id(i) = 300 + i`) is unchanged, ids are still
//! index-derived, and a mixed-role `ClusterConfig` id-derives exactly the same
//! way, just filtered by which nodes actually declare each role.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

use crate::RoleAddrs;

/// Id offset for a node's **leaderful CP** per-tablet Raft role (ADR 0017 #3a) —
/// the data plane. A node hosting a CP tablet group runs a
/// [`RaftKvNode`](animus_cp_data::RaftKvNode) on this id (its own `ProdEnv`/inbox,
/// distinct from the control role). Offset well above the control ids so the two
/// roles never collide.
pub const RAFTKV_ID_BASE: NodeId = 300;

/// The control-plane Raft id for node `index`. This is the **id scheme**, not a
/// claim that `index` actually runs the control role — check
/// [`RoleAddrs::role`]/[`NodeRole::has_control`] (or go through
/// [`ClusterConfig::control_ids`]) for that. ADR 0035 doesn't change the
/// scheme, only the assumption that every index runs every role.
#[must_use]
pub fn control_id(index: usize) -> NodeId {
    index as NodeId
}
/// The leaderful CP per-tablet Raft id for node `index` (ADR 0017 #3a). As with
/// [`control_id`], this is the id scheme, not a role claim — see
/// [`ClusterConfig::raftkv_ids`].
#[must_use]
pub fn raftkv_id(index: usize) -> NodeId {
    RAFTKV_ID_BASE + index as NodeId
}

/// Which role(s) a [`RoleAddrs`] entry runs (ADR 0035).
///
/// `Both` is the default and was, before ADR 0035, the *only* shape every
/// node could take — so an old config (or one that never sets this field)
/// deserializes as `Both` and behaves exactly as it always has. `Control` and
/// `Data` describe the two split-deployment shapes ADR 0035 targets
/// (`animusd control` / `animusd data`, PR3/PR4): a `Control` node needs only
/// `control`/`client`/`admin` addresses; a `Data` node needs
/// `client`/`dynamo`/`cql`/`raftkv`/`admin` (no `control`). See
/// [`RoleAddrs`](crate::RoleAddrs)'s doc for how the address fields encode
/// this (the role-gated ones, `control` and `raftkv`, are `Option`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Runs only the control-plane Raft (metadata: membership, tablet map,
    /// schema catalog, node address book) + placement/detector + client/admin
    /// endpoints. No storage engine, no `raftkv` env, no DynamoDB/CQL edges.
    Control,
    /// Runs only the data plane: the shared LSM engine, the `raftkv` env +
    /// per-tablet Raft groups, the tablet-host reconciler, and the
    /// client/DynamoDB/CQL/admin edges. No local control `RaftCore` — in the
    /// eventual split deployment (PR4) this reads `Metadata` from a polled
    /// mirror of the control deployment instead.
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
    /// each node six consecutive ports starting at `base_port` (node `i` uses
    /// `base_port + 6*i .. +6`): control, client, dynamo, cql, raftkv, admin
    /// (the admin/debug interface, ADR 0020). Every node is [`NodeRole::Both`]
    /// (unchanged since before ADR 0035) — see [`generate_split`] for the
    /// split-deployment shape.
    ///
    /// [`generate_split`]: Self::generate_split
    #[must_use]
    pub fn generate(n: usize, host: IpAddr, base_port: u16) -> Self {
        let nodes = (0..n)
            .map(|i| {
                let p = |role: u16| SocketAddr::new(host, base_port + (i as u16) * 6 + role);
                RoleAddrs {
                    role: NodeRole::Both,
                    control: Some(p(0)),
                    client: p(1),
                    dynamo: p(2),
                    cql: p(3),
                    raftkv: Some(p(4)),
                    admin: p(5),
                }
            })
            .collect();
        Self { nodes }
    }

    /// Generate a **split-deployment** config (ADR 0035 target topology):
    /// `control_n` control-only nodes followed by `data_n` data-only nodes,
    /// all on `host` starting at `base_port`. Each node still gets a full
    /// six-port block (same stride as [`generate`](Self::generate) — dynamo/
    /// cql/raftkv ports on a control node, and the control port on a data
    /// node, simply go unused rather than being omitted from the port range),
    /// so the two generators stay trivially comparable and a config can be
    /// edited from one shape toward the other by hand.
    ///
    /// This is additive config-layer scaffolding for the PR3/PR4 control-only
    /// / data-only entry points — nothing in this PR (PR2) actually assembles
    /// a process from a `Control`- or `Data`-only [`RoleAddrs`] (every real
    /// entry point today still requires both a control and a raftkv address,
    /// i.e. `Both`); wiring a `gen-config --control-nodes/--data-nodes` CLI
    /// flag onto this is left to PR3, once there's a process shape to
    /// generate a config *for*.
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
                    role,
                    control: role.has_control().then(|| p(0)),
                    client: p(1),
                    dynamo: p(2),
                    cql: p(3),
                    raftkv: role.has_data().then(|| p(4)),
                    admin: p(5),
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

    /// The control-plane Raft membership: the control ids of nodes that
    /// actually run the control role. In combined mode (every node `Both`,
    /// the only shape before ADR 0035) this is unchanged — `0..len()`.
    #[must_use]
    pub fn control_ids(&self) -> Vec<NodeId> {
        self.control_indexes().into_iter().map(control_id).collect()
    }

    /// The leaderful CP per-tablet Raft ids of nodes that actually run the
    /// data role — the universe from which a CP tablet group's replica set
    /// is drawn (ADR 0017 #3a), and (ADR 0035) the set `bootstrap`
    /// auto-registers as `Active` data members. In combined mode this is
    /// unchanged — every node's `raftkv_id`.
    #[must_use]
    pub fn raftkv_ids(&self) -> Vec<NodeId> {
        self.data_indexes().into_iter().map(raftkv_id).collect()
    }

    /// The control-role peer address book: each control-role node's control
    /// id → its control address (ADR 0035's split of [`peer_book`]).
    ///
    /// **A data-role node's `raftkv` env also needs this book, unioned with
    /// [`raftkv_peer_book`](Self::raftkv_peer_book) — not
    /// `raftkv_peer_book()` alone.** `heartbeat_loop`
    /// (`animus_control::node::heartbeat_loop`) runs on a data node's
    /// `raftkv` env and sends `RaftMsg::Heartbeat` to the **control** ids
    /// (unchanged by ADR 0035 — see `BoundNode::start_with`'s
    /// `raftkv_hb_env`), so a data-only node whose `raftkv` env peer book was
    /// installed as `raftkv_peer_book()` alone would have nowhere to route
    /// its own heartbeats: the control ids simply aren't in that book (see
    /// its doc). The book a data-only node's `raftkv` env must actually
    /// install is `raftkv_peer_book() ∪ control_peer_book()` — i.e.
    /// [`peer_book`] itself, exactly the combined-mode book every node
    /// installs on **both** envs today, now also the correct one for a
    /// future data-only node's single (`raftkv`-only) env. See
    /// `a_data_nodes_raftkv_env_needs_the_union_not_raftkv_peer_book_alone`
    /// below for the concrete failure this would otherwise cause.
    ///
    /// [`peer_book`]: Self::peer_book
    #[must_use]
    pub fn control_peer_book(&self) -> BTreeMap<NodeId, SocketAddr> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, a)| a.role.has_control())
            .filter_map(|(i, a)| a.control.map(|addr| (control_id(i), addr)))
            .collect()
    }

    /// The data-role peer address book: each data-role node's raftkv id → its
    /// raftkv address (ADR 0035's split of [`peer_book`]) — the internal
    /// per-tablet-Raft network among data nodes only. **Deliberately excludes
    /// the control ids** — see [`control_peer_book`](Self::control_peer_book)'s
    /// doc for why a data-only node's `raftkv` env must install that book too
    /// (its own failure-detector heartbeat target), not just this one.
    ///
    /// [`peer_book`]: Self::peer_book
    #[must_use]
    pub fn raftkv_peer_book(&self) -> BTreeMap<NodeId, SocketAddr> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, a)| a.role.has_data())
            .filter_map(|(i, a)| a.raftkv.map(|addr| (raftkv_id(i), addr)))
            .collect()
    }

    /// The peer address book every **combined-mode** node installs on both
    /// its internal envs: the union of [`control_peer_book`](Self::control_peer_book)
    /// and [`raftkv_peer_book`](Self::raftkv_peer_book). (Client/dynamo/cql
    /// addresses are external client channels, not part of the internal
    /// network.) Unchanged in shape from before ADR 0035 for an
    /// all-`Both` config; now correctly omits a control-only node's absent
    /// `raftkv` address and a data-only node's absent `control` address.
    ///
    /// This is also the book a **data-only** node's single `raftkv` env
    /// needs (see [`control_peer_book`](Self::control_peer_book)'s doc) — a
    /// control-only node's control env instead needs only
    /// [`control_peer_book`](Self::control_peer_book) on its own, since it
    /// never heartbeats or replicates over a `raftkv` env it doesn't have.
    #[must_use]
    pub fn peer_book(&self) -> BTreeMap<NodeId, SocketAddr> {
        let mut book = self.control_peer_book();
        book.extend(self.raftkv_peer_book());
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
        assert_eq!(cfg.nodes[0].control.unwrap().port(), 7000);
        assert_eq!(cfg.nodes[0].client.port(), 7001);
        assert_eq!(cfg.nodes[0].dynamo.port(), 7002);
        assert_eq!(cfg.nodes[0].cql.port(), 7003);
        assert_eq!(cfg.nodes[0].raftkv.unwrap().port(), 7004);
        assert_eq!(cfg.nodes[0].admin.port(), 7005);
        assert_eq!(cfg.nodes[1].control.unwrap().port(), 7006);
        assert_eq!(cfg.nodes[2].raftkv.unwrap().port(), 7016);
    }

    #[test]
    fn generated_config_is_combined_mode() {
        let cfg = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 7000);
        assert!(cfg.nodes.iter().all(|a| a.role == NodeRole::Both));
        assert_eq!(cfg.control_ids(), vec![0, 1, 2]);
        assert_eq!(cfg.raftkv_ids(), vec![300, 301, 302]);
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
        assert_eq!(book[&control_id(1)].port(), 7006);
        assert_eq!(book[&raftkv_id(0)].port(), 7004);
        assert_eq!(book[&raftkv_id(2)].port(), 7016);
        // Client / dynamo / cql / admin addresses are intentionally absent from
        // the internal book (they are external client channels, not the network).
        assert!(!book.values().any(|a| a.port() == 7001)); // client (node 0)
        assert!(!book.values().any(|a| a.port() == 7002)); // dynamo (node 0)
        assert!(!book.values().any(|a| a.port() == 7003)); // cql (node 0)
        assert!(!book.values().any(|a| a.port() == 7005)); // admin (node 0)
    }

    #[test]
    fn json_round_trips() {
        let cfg = ClusterConfig::generate(2, "10.0.0.1".parse().unwrap(), 9000);
        let parsed = ClusterConfig::from_json(&cfg.to_json()).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.nodes[1].raftkv, cfg.nodes[1].raftkv);
    }

    /// Back-compat: the pre-ADR-0035 JSON shape (no `role` field, `control`/
    /// `raftkv` given as plain addresses rather than `Option`-shaped) must
    /// still load, and must mean combined mode — every field defaults
    /// (`role` → `Both`) or deserializes straight into `Some(..)` for
    /// `control`/`raftkv` (a JSON address value deserializes into
    /// `Option<SocketAddr>` as `Some` the same way it always deserialized
    /// into a bare `SocketAddr`; `serde(default)` only matters if the field
    /// is missing entirely, which an old *written* config's `control`/
    /// `raftkv` never were).
    #[test]
    fn old_json_shape_without_role_loads_as_combined_mode() {
        let old_json = r#"{
            "nodes": [
                {
                    "control": "127.0.0.1:7000",
                    "client": "127.0.0.1:7001",
                    "dynamo": "127.0.0.1:7002",
                    "cql": "127.0.0.1:7003",
                    "raftkv": "127.0.0.1:7004",
                    "admin": "127.0.0.1:7005"
                }
            ]
        }"#;
        let cfg = ClusterConfig::from_json(old_json).expect("old shape parses");
        assert_eq!(cfg.len(), 1);
        assert_eq!(cfg.nodes[0].role, NodeRole::Both);
        assert_eq!(
            cfg.nodes[0].control,
            Some("127.0.0.1:7000".parse().unwrap())
        );
        assert_eq!(cfg.nodes[0].raftkv, Some("127.0.0.1:7004".parse().unwrap()));
        assert_eq!(cfg.control_ids(), vec![0]);
        assert_eq!(cfg.raftkv_ids(), vec![RAFTKV_ID_BASE]);
    }

    /// An even-older shape (predating `dynamo`/`cql`/`raftkv`/`admin`
    /// entirely, back-compat already covered by their own
    /// `default_ephemeral_addr`) still loads too, now also picking up the
    /// new `role` default.
    #[test]
    fn oldest_json_shape_missing_optional_fields_loads() {
        let ancient_json = r#"{
            "nodes": [
                { "control": "127.0.0.1:7000", "client": "127.0.0.1:7001" }
            ]
        }"#;
        let cfg = ClusterConfig::from_json(ancient_json).expect("ancient shape parses");
        assert_eq!(cfg.nodes[0].role, NodeRole::Both);
        assert_eq!(
            cfg.nodes[0].control,
            Some("127.0.0.1:7000".parse().unwrap())
        );
        assert!(
            cfg.nodes[0].raftkv.is_some(),
            "defaults to an ephemeral addr, not None"
        );
    }

    #[test]
    fn ids_follow_convention() {
        assert_eq!(control_id(2), 2);
        assert_eq!(raftkv_id(0), RAFTKV_ID_BASE);
        assert_eq!(raftkv_id(2), RAFTKV_ID_BASE + 2);
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
        assert_eq!(cfg.control_ids(), vec![control_id(0), control_id(1)]);
        assert_eq!(
            cfg.raftkv_ids(),
            vec![raftkv_id(2), raftkv_id(3), raftkv_id(4)]
        );
        // Role-gated addresses are present only where the role calls for them.
        for i in 0..2 {
            assert!(cfg.nodes[i].control.is_some());
            assert!(cfg.nodes[i].raftkv.is_none());
        }
        for i in 2..5 {
            assert!(cfg.nodes[i].control.is_none());
            assert!(cfg.nodes[i].raftkv.is_some());
        }
    }

    #[test]
    fn peer_books_split_by_role_on_a_mixed_topology() {
        let cfg = ClusterConfig::generate_split(2, 3, "127.0.0.1".parse().unwrap(), 8000);
        let control_book = cfg.control_peer_book();
        let raftkv_book = cfg.raftkv_peer_book();
        assert_eq!(control_book.len(), 2, "only the 2 control-role nodes");
        assert_eq!(raftkv_book.len(), 3, "only the 3 data-role nodes");
        assert!(control_book.contains_key(&control_id(0)));
        assert!(control_book.contains_key(&control_id(1)));
        assert!(
            !control_book.contains_key(&control_id(2)),
            "node 2 is data-only"
        );
        assert!(raftkv_book.contains_key(&raftkv_id(2)));
        assert!(raftkv_book.contains_key(&raftkv_id(3)));
        assert!(raftkv_book.contains_key(&raftkv_id(4)));
        assert!(
            !raftkv_book.contains_key(&raftkv_id(0)),
            "node 0 is control-only"
        );
        // The union view still covers every role-address that exists.
        let union = cfg.peer_book();
        assert_eq!(union.len(), 5);
    }

    /// A data-only node's `raftkv` env hosts both its per-tablet CP Raft
    /// groups (needs other data nodes' `raftkv` addresses,
    /// [`ClusterConfig::raftkv_peer_book`]) **and** the ADR 0012
    /// failure-detector `heartbeat_loop`, which sends `RaftMsg::Heartbeat` to
    /// the **control** ids over that same env
    /// (`animus_control::node::heartbeat_loop`, wired on the `raftkv` env in
    /// `BoundNode::start_with`). So installing `raftkv_peer_book()` alone as
    /// that env's peer book would leave every control id unreachable from
    /// it — the heartbeat has nowhere to route, and failure detection goes
    /// silently dead for the whole data fleet, with no error anywhere (a
    /// `set_peers` with a missing entry just drops the send). The book a
    /// data-only node's `raftkv` env must actually install is the union —
    /// exactly [`ClusterConfig::peer_book`], not `raftkv_peer_book()` on its
    /// own.
    #[test]
    fn a_data_nodes_raftkv_env_needs_the_union_not_raftkv_peer_book_alone() {
        let cfg = ClusterConfig::generate_split(2, 3, "127.0.0.1".parse().unwrap(), 8000);
        let raftkv_only = cfg.raftkv_peer_book();
        // The control ids a data node must heartbeat are absent from
        // `raftkv_peer_book()` alone...
        assert!(!raftkv_only.contains_key(&control_id(0)));
        assert!(!raftkv_only.contains_key(&control_id(1)));
        // ...but present in the union a data-only node's `raftkv` env should
        // actually be given (mirroring how `peer_book()` is built).
        let mut data_node_raftkv_env_book = raftkv_only;
        data_node_raftkv_env_book.extend(cfg.control_peer_book());
        assert!(data_node_raftkv_env_book.contains_key(&control_id(0)));
        assert!(data_node_raftkv_env_book.contains_key(&control_id(1)));
        assert!(data_node_raftkv_env_book.contains_key(&raftkv_id(2)));
        assert!(data_node_raftkv_env_book.contains_key(&raftkv_id(3)));
        assert!(data_node_raftkv_env_book.contains_key(&raftkv_id(4)));
        assert_eq!(data_node_raftkv_env_book, cfg.peer_book());
    }
}
