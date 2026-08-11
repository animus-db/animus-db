//! Phase 1 / A2 (v1 plan, ADR 0013): **cross-process schema-DDL relay**. In a
//! one-process-per-node deployment, a schema command (`CreateTableSchema` /
//! `SetTableMode` / …) issued to a node that is **not** the control-plane leader
//! is relayed (via `ClientRequest::ProposeSchema`) to the leader's node so it
//! commits + replicates — instead of timing out (the prior behavior, where a
//! follower had no leader handle to propose on). The relay is **gated** to
//! schema-catalog commands: a membership/placement command is rejected.
//!
//! Real TCP/time → polls with timeouts.

use std::net::SocketAddr;
use std::time::Duration;

use animus_env::nid;
use animusd::{
    ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, ReplicationMode, TableSchema,
    read_frame,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Bring up an `n`-node per-process cluster (each node its own edge state).
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, animusd::ClusterConfig) {
    // Documented port-TOCTOU retry: `free_addrs` releases the probed ports before
    // `run_node` rebinds them, so a concurrent test binary can steal one —
    // re-allocate fresh ports and retry the whole bring-up as a unit.
    let mut brought_up = None;
    'attempts: for attempt in 0..16 {
        let addrs = support::free_addrs(n * 5);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[5 * i],
                client: addrs[5 * i + 1],
                dynamo: addrs[5 * i + 2],
                cql: addrs[5 * i + 3],
                admin: addrs[5 * i + 4],
            })
            .collect();
        let config = animusd::ClusterConfig { nodes: nodes_cfg };
        let mut nodes = Vec::new();
        for i in 0..n {
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    for node in &nodes {
                        node.shutdown_graceful().await;
                    }
                    sleep(Duration::from_millis(50)).await;
                    continue 'attempts;
                }
            }
        }
        brought_up = Some((nodes, config));
        break;
    }
    let (nodes, config) =
        brought_up.expect("could not bring up cluster after retries (ports kept getting stolen)");
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
    (nodes, config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn schema_ddl_on_a_follower_is_relayed_to_the_leader() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;

    // The control-plane leader, and a *different* node to issue DDL against.
    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();
    let follower_client = config.nodes[follower].client;

    // Issue a schema create against the FOLLOWER. Pre-A2 this would time out (the
    // follower has no local leader handle to propose on); now it relays to the
    // leader. Retry while a leader settles.
    let create = MetaCommand::CreateTableSchema {
        table: "ddl_t".into(),
        schema: TableSchema::simple("id", ColumnType::String),
    };
    timeout(Duration::from_secs(20), async {
        loop {
            let resp = call(
                follower_client,
                ClientRequest::ProposeSchema(create.clone()),
            )
            .await;
            // The schema commits + replicates back to every node regardless of
            // which node accepted the relay; gate on this node's replicated view.
            if nodes[follower].metadata().has_table_schema("ddl_t") {
                return;
            }
            let _ = resp;
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued DDL did not commit via relay in 20s");

    // It replicated to *every* node.
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.metadata().has_table_schema("ddl_t"),
            "table schema missing on node {i} after follower-relayed DDL"
        );
    }

    // Also flip its mode to CP via the follower (another schema command) and see it
    // replicate — exercises the relay for `SetTableMode` too.
    let set_cp = MetaCommand::SetTableMode {
        table: "ddl_t".into(),
        mode: ReplicationMode::Cp,
    };
    timeout(Duration::from_secs(20), async {
        loop {
            let _ = call(
                follower_client,
                ClientRequest::ProposeSchema(set_cp.clone()),
            )
            .await;
            if nodes
                .iter()
                .all(|n| n.metadata().table_mode("ddl_t") == ReplicationMode::Cp)
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued SetTableMode did not replicate in 20s");

    // The atomic `ALTER TABLE` primitive (`ReplaceTableSchema`) relays too — the
    // gating allowlist (`is_relayable_command`) must include it, or a
    // follower-connected ALTER silently times out (works only when the connected
    // node happens to be the control leader — the documented bimodal relay flake).
    let mut extended = TableSchema::simple("id", ColumnType::String);
    extended
        .columns
        .push(animusd::ColumnDef::new("age", ColumnType::Number));
    extended.mode = ReplicationMode::Cp; // preserve the mode set above
    let replace = MetaCommand::ReplaceTableSchema {
        table: "ddl_t".into(),
        schema: extended.clone(),
    };
    timeout(Duration::from_secs(20), async {
        loop {
            let _ = call(
                follower_client,
                ClientRequest::ProposeSchema(replace.clone()),
            )
            .await;
            if nodes.iter().all(|n| {
                n.metadata()
                    .table_schema("ddl_t")
                    .is_some_and(|s| s == &extended)
            }) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued ReplaceTableSchema (atomic ALTER) did not replicate in 20s");
    // The replacement was in place: the table kept a schema throughout (spot-check
    // the final state on every node — no drop-then-recreate window exists at all
    // with a single command).
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.metadata().has_table_schema("ddl_t"),
            "table schema missing on node {i} after atomic ALTER"
        );
    }

    // Gate: a non-schema (membership/placement) command must be rejected by the
    // relay, on any node — this is not a general "propose anything" surface.
    let bad = call(
        config.nodes[leader].client,
        ClientRequest::ProposeSchema(MetaCommand::UpsertMember {
            node: nid(999),
            labels: std::collections::BTreeMap::new(),
            status: animusd::NodeStatus::Active,
        }),
    )
    .await;
    assert!(
        matches!(bad, ClientResponse::Error(_)),
        "a non-schema command must be rejected by the relay, got {bad:?}"
    );
    // And it really did not take effect (member 999 was never registered).
    assert!(
        !nodes[leader].metadata().members.contains_key(&nid(999)),
        "rejected command must not have been applied"
    );

    for n in &nodes {
        n.shutdown();
    }
}
