//! Phase 1 / A2 (v1 plan, ADR 0013): **cross-process schema-DDL relay**. In a
//! one-process-per-node deployment, a schema command (`CreateTableSchema` /
//! `CreateTableIndex` / …) issued to a node that is **not** the control-plane
//! leader is relayed (via `ClientRequest::ProposeSchema`) to the leader's node
//! so it commits + replicates — instead of timing out (the prior behavior,
//! where a follower had no leader handle to propose on). The relay is
//! **gated** to schema-catalog commands: a membership/placement command is
//! rejected.
//!
//! Real TCP/time → polls with timeouts.

use std::net::SocketAddr;
use std::time::Duration;

use animus_env::nid;
use animusd::{
    ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, TableSchema, read_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
        };
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
    // ADR 0047: `ProposeSchema` is intra-only.
    let follower_client = config.nodes[follower].intra;

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

    // The atomic `ALTER TABLE` primitive (`ReplaceTableSchema`) relays too — the
    // gating allowlist (`is_relayable_command`) must include it, or a
    // follower-connected ALTER silently times out (works only when the connected
    // node happens to be the control leader — the documented bimodal relay flake).
    let mut extended = TableSchema::simple("id", ColumnType::String);
    extended
        .columns
        .push(animusd::ColumnDef::new("age", ColumnType::Number));
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
        // ADR 0047: `ProposeSchema` is intra-only.
        config.nodes[leader].intra,
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
        n.shutdown_graceful().await;
    }
}

/// ADR 0042/0043's stream-shard catalog relay decision:
/// `MetaCommand::SealStreamShard` (a tablet leader's own seal commit, which
/// may run on any data node, not necessarily one connected to the control
/// leader) is relayable — issued against a follower-connected node, it must
/// still land and replicate. `MetaCommand::ExpireStreamShards` (the segment
/// janitor's own reclaim, a control-plane-leader-only background loop with
/// no structural need for a relay path) is deliberately **excluded** — the
/// relay rejects it outright, even sent straight to the leader, mirroring
/// the `RemoveMember` gate test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn stream_shard_catalog_relay_allows_seal_but_not_expire() {
    use animus_control::{StreamSpec, StreamViewType};
    use animus_tablet::TabletId;

    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;

    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();
    // ADR 0047: `ProposeSchema` is intra-only.
    let follower_client = config.nodes[follower].intra;

    // Register a table + enable a stream first (SealStreamShard's own label
    // validation needs a schema entry to license the label) — through the
    // SAME follower-connected node, exercising the relay for both.
    let create = MetaCommand::CreateTableSchema {
        table: "stream_relay_t".into(),
        schema: TableSchema::simple("id", ColumnType::String),
    };
    timeout(Duration::from_secs(20), async {
        loop {
            let _ = call(
                follower_client,
                ClientRequest::ProposeSchema(create.clone()),
            )
            .await;
            if nodes[follower]
                .metadata()
                .has_table_schema("stream_relay_t")
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued CreateTableSchema did not commit in 20s");

    let enable = MetaCommand::SetTableStream {
        table: "stream_relay_t".into(),
        spec: Some(StreamSpec {
            view_type: StreamViewType::NewAndOldImages,
            label: "relay-L1".into(),
        }),
    };
    timeout(Duration::from_secs(20), async {
        loop {
            let _ = call(
                follower_client,
                ClientRequest::ProposeSchema(enable.clone()),
            )
            .await;
            if nodes[follower]
                .metadata()
                .table_stream("stream_relay_t")
                .is_some()
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued SetTableStream did not commit in 20s");

    // The regression: `SealStreamShard`, issued against the FOLLOWER, must
    // relay to the leader and replicate everywhere — the exact bimodal-flake
    // shape the house lesson on `is_relayable_command` warns about.
    let seal = MetaCommand::SealStreamShard {
        table: "stream_relay_t".into(),
        label: "relay-L1".into(),
        tablet: TabletId(1),
        epoch: 0,
        view_type: StreamViewType::NewAndOldImages,
        hlc_range: (0, 100),
        count: 1,
        seal_wall_ms: 1_700_000_000_000,
        replicas: vec![nid(10), nid(11)],
        object_id: "stream_relay_t/relay-L1/1/0/test".to_owned(),
    };
    timeout(Duration::from_secs(20), async {
        loop {
            let _ = call(follower_client, ClientRequest::ProposeSchema(seal.clone())).await;
            if nodes[follower]
                .metadata()
                .stream_shard_watermark(TabletId(1))
                .is_some()
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued SealStreamShard did not commit via relay in 20s");
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.metadata().stream_shard_watermark(TabletId(1)),
            Some(100),
            "node {i}: SealStreamShard's catalog row did not replicate"
        );
    }

    // `ExpireStreamShards` is deliberately NOT relayable — rejected by the
    // gate even sent straight to the leader (mirroring `RemoveMember`'s own
    // gate test above), since its only intended caller (the segment
    // janitor) never needs a relay path at all.
    let expire = call(
        // ADR 0047: `ProposeSchema` is intra-only.
        config.nodes[leader].intra,
        ClientRequest::ProposeSchema(MetaCommand::ExpireStreamShards {
            rows: vec![(TabletId(1), 0)],
            remove: false,
        }),
    )
    .await;
    assert!(
        matches!(expire, ClientResponse::Error(_)),
        "ExpireStreamShards must be rejected by the relay, got {expire:?}"
    );
    // And it really did not take effect (the row is still unmarked).
    assert!(
        !nodes[leader]
            .metadata()
            .stream_shard_chain("stream_relay_t", "relay-L1", TabletId(1))
            .next()
            .is_some_and(|(_, row)| row.expired),
        "rejected ExpireStreamShards must not have marked the row"
    );

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)` —
/// mirrors `dynamo_schema.rs`'s identical helper (this file only needs it
/// for the one TTL regression below, so it isn't worth sharing via
/// `support`).
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to dynamo");
    let request = format!(
        "POST / HTTP/1.1\r\n\
         Host: animus\r\n\
         X-Amz-Target: {target}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read full response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

/// ADR 0051's own instance of the `is_relayable_command` regression class
/// this whole file exists for: `UpdateTimeToLive` issued against a DynamoDB
/// listener on a node that is **not** the control-plane leader must still
/// commit — `MetaCommand::SetTableTtl` must be on the relay allowlist, or
/// this times out on exactly this shape (works only when the connected
/// node happens to be the leader).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn update_time_to_live_on_a_follower_is_relayed_to_the_leader() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;

    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();

    // Create the table against the leader (already covered elsewhere) so
    // the regression below is scoped to `UpdateTimeToLive` alone.
    let create = MetaCommand::CreateTableSchema {
        table: "ttl_relay_t".into(),
        schema: TableSchema::simple("id", ColumnType::String),
    };
    timeout(Duration::from_secs(20), async {
        loop {
            let _ = call(
                config.nodes[leader].intra,
                ClientRequest::ProposeSchema(create.clone()),
            )
            .await;
            if nodes[leader].metadata().has_table_schema("ttl_relay_t") {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("CreateTableSchema did not commit in 20s");

    // The regression: `UpdateTimeToLive`, issued against the FOLLOWER's own
    // DynamoDB listener, must relay to the leader and replicate everywhere.
    let follower_dynamo = config.nodes[follower].dynamo;
    let (status, body) = timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(
                follower_dynamo,
                "DynamoDB_20120810.UpdateTimeToLive",
                r#"{"TableName":"ttl_relay_t","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
            )
            .await;
            if status == 200 {
                return (status, body);
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued UpdateTimeToLive did not commit via relay in 20s");
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("\"AttributeName\":\"expiresAt\""), "{body}");
    assert!(body.contains("\"Enabled\":true"), "{body}");

    // It replicated to *every* node's own replicated catalog.
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            n.metadata()
                .table_ttl("ttl_relay_t")
                .map(|t| t.attribute_name.as_str()),
            Some("expiresAt"),
            "node {i}: TTL spec missing after follower-relayed UpdateTimeToLive"
        );
    }

    // `DescribeTimeToLive` against the (different) follower reads the same
    // committed spec back — a pure catalog read, no relay involved.
    let (status, body) = dynamo(
        follower_dynamo,
        "DynamoDB_20120810.DescribeTimeToLive",
        r#"{"TableName":"ttl_relay_t"}"#,
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("\"TimeToLiveStatus\":\"ENABLED\""), "{body}");
    assert!(body.contains("\"AttributeName\":\"expiresAt\""), "{body}");

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// ADR 0059 §3 (Train 1 PR④)'s own instance of this file's regression
/// class: `DeleteBackup` issued against a DynamoDB listener on a node that
/// is **not** the control-plane leader must still commit —
/// `MetaCommand::MarkBackupDeleted` must be on the relay allowlist, or this
/// times out on exactly this shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn delete_backup_on_a_follower_is_relayed_to_the_leader() {
    let dir = tempfile::tempdir().unwrap();
    let (nodes, config) = bring_up(3, dir.path()).await;

    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();
    let leader_dynamo = config.nodes[leader].dynamo;
    let follower_dynamo = config.nodes[follower].dynamo;

    let (status, body) = dynamo(
        leader_dynamo,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"backup_relay_t","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "body: {body}");

    let (status, body) = dynamo(
        leader_dynamo,
        "DynamoDB_20120810.CreateBackup",
        r#"{"TableName":"backup_relay_t","BackupName":"relay-backup-1"}"#,
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    let created: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    let backup_arn = created["BackupDetails"]["BackupArn"]
        .as_str()
        .expect("BackupArn")
        .to_owned();

    // Wait for the backup to become AVAILABLE before deleting it (matching
    // real AWS's own contract — this file's regression is about the
    // relay, not about racing a still-`CREATING` backup).
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes[leader]
                .metadata()
                .backup(&backup_arn)
                .is_some_and(|row| row.status == animus_control::BackupStatus::Available)
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("backup did not become AVAILABLE in 20s");

    // The regression: `DeleteBackup`, issued against the FOLLOWER's own
    // DynamoDB listener, must relay to the leader and commit.
    let (status, body) = timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(
                follower_dynamo,
                "DynamoDB_20120810.DeleteBackup",
                &format!(r#"{{"BackupArn":"{backup_arn}"}}"#),
            )
            .await;
            if status == 200 {
                return (status, body);
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued DeleteBackup did not commit via relay in 20s");
    assert_eq!(status, 200, "body: {body}");
    assert!(
        body.contains("\"BackupStatus\":\"DELETED\""),
        "body: {body}"
    );

    // It replicated to *every* node's own replicated catalog — a
    // converged-or-timeout poll, never a one-shot assert (a node's own
    // local apply can genuinely lag the commit by a beat).
    for (i, n) in nodes.iter().enumerate() {
        timeout(Duration::from_secs(20), async {
            loop {
                if n.metadata()
                    .backup(&backup_arn)
                    .map(|row| row.status.clone())
                    == Some(animus_control::BackupStatus::Expired)
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "node {i}: backup not marked Expired within 20s of follower-relayed DeleteBackup"
            )
        });
    }

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}
