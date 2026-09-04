//! The secondary-index backfill-completion aggregator end to end (ADR 0045
//! §4): `index_backfill_loop` reads `Metadata::index_backfill` and flips a
//! table's index from `Creating` to `Active` once every one of the table's
//! *currently live* tablets has reported a finished scan. The seeder that
//! actually populates `index_backfill` in production doesn't exist yet (a
//! later PR) — every proposal here is hand-driven via
//! `ClientRequest::ProposeSchema`, mirroring how `stream_janitor.rs`/
//! `schema_ddl_relay.rs` test their own control-leader-only aggregators
//! against hand-driven `MetaCommand`s.
//!
//! Real TCP/time throughout — every eventual property is a
//! converged-or-timeout poll; every negative ("must not flip yet") property
//! is a bounded polling window that fails immediately on a premature flip
//! rather than a fixed sleep followed by one assertion.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::{IndexDef, IndexKind, IndexProjection, IndexStatus};
use animus_tablet::{Epoch, KeyRange, TabletId};
use animusd::{
    ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, TableSchema, read_frame,
};
use tokio::time::{sleep, timeout};

mod support;

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    // `support::connect_retry` rides out the port-TOCTOU/listener-not-yet-
    // accepting window a plain `TcpStream::connect` can lose the instant
    // `bring_up` returns (issue #592) — see that helper's own doc.
    let mut stream = support::connect_retry(addr).await;
    animusd::write_frame(&mut stream, &req).await.expect("send");
    read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// Bring up an `n`-node per-process combined cluster (each node its own edge
/// state) — duplicated from `schema_ddl_relay.rs` rather than shared, per
/// this codebase's own "sibling test modules keep their own fixtures
/// independent" convention (`stream_janitor.rs`'s doc comment).
async fn bring_up(n: usize, dir: &Path) -> (Vec<Node>, animusd::ClusterConfig) {
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
                advertise_host: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
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

/// A `Creating` GSI definition — the shape a populated-table `UpdateTable`
/// would propose (a later PR); the exact key attributes don't matter to this
/// aggregator, which never reads them.
fn creating_index(name: &str) -> IndexDef {
    IndexDef {
        name: name.to_owned(),
        kind: IndexKind::Global,
        hash_attribute: "email".to_owned(),
        sort_attribute: None,
        projection: IndexProjection::All,
        status: IndexStatus::Creating,
        hash_attribute_type: None,
        sort_attribute_type: None,
    }
}

fn index_status(nodes: &[Node], table: &str, index: &str) -> Option<IndexStatus> {
    nodes[0]
        .metadata()
        .table_indexes(table)
        .iter()
        .find(|i| i.name == index)
        .map(|i| i.status)
}

async fn await_index_status(
    nodes: &[Node],
    table: &str,
    index: &str,
    want: IndexStatus,
    secs: u64,
) {
    timeout(Duration::from_secs(secs), async {
        loop {
            if nodes.iter().all(|n| {
                n.metadata()
                    .table_indexes(table)
                    .iter()
                    .any(|i| i.name == index && i.status == want)
            }) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "index {table}/{index} did not reach {want:?} within {secs}s (last seen: {:?})",
            index_status(nodes, table, index)
        )
    });
}

/// Poll for `secs`, failing immediately the moment the index's status is
/// observed to have left `Creating` — the converged-or-timeout shape for a
/// negative property (see the module doc): a fixed sleep followed by one
/// assertion could miss a flip that happened and reverted, but nothing here
/// ever reverts a status, so this window is exactly as strong as a fixed
/// sleep while also failing fast on the common case.
async fn assert_no_premature_flip(nodes: &[Node], table: &str, index: &str, window: Duration) {
    let deadline = tokio::time::Instant::now() + window;
    loop {
        for n in nodes {
            let status = n
                .metadata()
                .table_indexes(table)
                .iter()
                .find(|i| i.name == index)
                .map(|i| i.status);
            assert_eq!(
                status,
                Some(IndexStatus::Creating),
                "index {table}/{index} flipped prematurely"
            );
        }
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn index_backfill_converges_to_active_once_every_tablet_reports() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(3, dir.path()).await;
    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    // ADR 0047: `ProposeSchema` is intra-only.
    let client = config.nodes[leader].intra;

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableSchema {
            table: "bf_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
    )
    .await;
    timeout(Duration::from_secs(10), async {
        loop {
            if nodes[0].metadata().has_table_schema("bf_t") {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CreateTableSchema did not commit in 10s");

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
            table: "bf_t".into(),
            index: creating_index("by_email"),
        }),
    )
    .await;
    await_index_status(&nodes, "bf_t", "by_email", IndexStatus::Creating, 10).await;

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTablet {
            tablet: TabletId(500),
            table: Some("bf_t".into()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        }),
    )
    .await;
    timeout(Duration::from_secs(10), async {
        loop {
            if nodes[0].metadata().tablets.contains_key(&TabletId(500)) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CreateTablet did not commit in 10s");

    // A table's second (and later) tablet only ever comes from a split
    // (ADR 0023/0044) — mint a genuine sibling so this test exercises a
    // real ≥2-tablet completion set, not a degenerate single-tablet one.
    // This is topology scaffolding on an empty table only (ADR 0062: an
    // in-place split's children inherit the parent's own replicas
    // verbatim — the parent here has none, so the empty `Vec::new()`s
    // below are still correct) — nothing here exercises the fork itself.
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::BeginSplitInPlace {
            parent: TabletId(500),
            expected_epoch: Epoch::INITIAL,
            split_key: b"m".to_vec(),
            children: [(TabletId(501), Vec::new()), (TabletId(502), Vec::new())],
        }),
    )
    .await;
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CutoverSplit {
            parent: TabletId(500),
            expected_epoch: Epoch::INITIAL.next(),
            cutover_wall_ms: 1_000,
        }),
    )
    .await;
    timeout(Duration::from_secs(10), async {
        loop {
            if nodes[0].metadata().tablets.contains_key(&TabletId(501)) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("split round did not commit in 10s");

    // Only one of the two (post-cutover) tablets reports — must not flip.
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::MarkIndexBackfilled {
            table: "bf_t".into(),
            index: "by_email".into(),
            tablet: TabletId(501),
        }),
    )
    .await;
    assert_no_premature_flip(&nodes, "bf_t", "by_email", Duration::from_millis(1500)).await;

    // The second (and last) tablet reports — must now converge to `Active`.
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::MarkIndexBackfilled {
            table: "bf_t".into(),
            index: "by_email".into(),
            tablet: TabletId(502),
        }),
    )
    .await;
    await_index_status(&nodes, "bf_t", "by_email", IndexStatus::Active, 10).await;

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// The split-during-backfill shape named in the plan's own risk list,
/// simulated without a real seeder: a tablet appears (via a genuine
/// `SplitTablet`, ADR 0044's only source of a table's later tablets) after
/// every *previously existing* tablet has already reported — the aggregator
/// must re-read the *live* tablet map fresh every tick and refuse to flip
/// until the new arrival reports too.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_tablet_that_appears_before_the_flip_blocks_it_until_it_also_reports() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(3, dir.path()).await;
    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    // ADR 0047: `ProposeSchema` is intra-only.
    let client = config.nodes[leader].intra;

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableSchema {
            table: "bf_split_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
    )
    .await;
    timeout(Duration::from_secs(10), async {
        loop {
            if nodes[0].metadata().has_table_schema("bf_split_t") {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CreateTableSchema did not commit in 10s");

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
            table: "bf_split_t".into(),
            index: creating_index("by_email"),
        }),
    )
    .await;
    await_index_status(&nodes, "bf_split_t", "by_email", IndexStatus::Creating, 10).await;

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTablet {
            tablet: TabletId(600),
            table: Some("bf_split_t".into()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        }),
    )
    .await;
    timeout(Duration::from_secs(10), async {
        loop {
            if nodes[0].metadata().tablets.contains_key(&TabletId(600)) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CreateTablet did not commit in 10s");

    // The only tablet reports — with just one tablet total, this alone would
    // already satisfy completion, so a real production seeder's split race
    // would need to land its new sibling *before* this report is even
    // proposed to test anything; this test instead orders the split itself
    // right after, so the completion set genuinely grows mid-flight.
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::MarkIndexBackfilled {
            table: "bf_split_t".into(),
            index: "by_email".into(),
            tablet: TabletId(600),
        }),
    )
    .await;

    // A new tablet appears (a real split) before any tick could have
    // observed "every current tablet reported" as anything but momentarily
    // true — from this point on, completion needs the child too. Topology
    // scaffolding only (empty table; ADR 0062's verbatim-replicas rule is
    // trivially satisfied since the parent has none).
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::BeginSplitInPlace {
            parent: TabletId(600),
            expected_epoch: Epoch::INITIAL,
            split_key: b"m".to_vec(),
            children: [(TabletId(601), Vec::new()), (TabletId(602), Vec::new())],
        }),
    )
    .await;
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CutoverSplit {
            parent: TabletId(600),
            expected_epoch: Epoch::INITIAL.next(),
            cutover_wall_ms: 1_000,
        }),
    )
    .await;
    timeout(Duration::from_secs(10), async {
        loop {
            if nodes[0].metadata().tablets.contains_key(&TabletId(601)) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("split round did not commit in 10s");

    // Neither child has reported — must not flip, however long the retired
    // parent's own (now-irrelevant) report has had to be observed.
    assert_no_premature_flip(
        &nodes,
        "bf_split_t",
        "by_email",
        Duration::from_millis(1500),
    )
    .await;

    // One child reports — still must not flip (its sibling hasn't).
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::MarkIndexBackfilled {
            table: "bf_split_t".into(),
            index: "by_email".into(),
            tablet: TabletId(601),
        }),
    )
    .await;
    assert_no_premature_flip(
        &nodes,
        "bf_split_t",
        "by_email",
        Duration::from_millis(1500),
    )
    .await;

    // The second child reports — now it converges.
    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::MarkIndexBackfilled {
            table: "bf_split_t".into(),
            index: "by_email".into(),
            tablet: TabletId(602),
        }),
    )
    .await;
    await_index_status(&nodes, "bf_split_t", "by_email", IndexStatus::Active, 10).await;

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// The named control-only-leader regression (the plan's own risk list): this
/// aggregator touches only replicated `Metadata`, unlike the segment
/// janitor's later phases — so, unlike that loop's documented
/// control-only-leader gap, a **pure** control-only leader (no data role at
/// all in the whole deployment) must still be able to drive the flip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_only_leader_drives_the_flip() {
    let dir = support::panic_safe_tempdir();
    let (control_nodes, data_nodes, config) = support::bring_up_split(1, 0, dir.path()).await;
    assert!(data_nodes.is_empty(), "test premise: no data role anywhere");
    support::await_leader(&control_nodes).await;
    // ADR 0047: `ProposeSchema` is intra-only.
    let client = config.nodes[0].intra;

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableSchema {
            table: "bf_control_only_t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
    )
    .await;
    timeout(Duration::from_secs(10), async {
        loop {
            if control_nodes[0]
                .metadata()
                .has_table_schema("bf_control_only_t")
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CreateTableSchema did not commit in 10s");

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTableIndex {
            table: "bf_control_only_t".into(),
            index: creating_index("by_email"),
        }),
    )
    .await;
    await_index_status(
        &control_nodes,
        "bf_control_only_t",
        "by_email",
        IndexStatus::Creating,
        10,
    )
    .await;

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::CreateTablet {
            tablet: TabletId(700),
            table: Some("bf_control_only_t".into()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        }),
    )
    .await;
    timeout(Duration::from_secs(10), async {
        loop {
            if control_nodes[0]
                .metadata()
                .tablets
                .contains_key(&TabletId(700))
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CreateTablet did not commit in 10s");

    call(
        client,
        ClientRequest::ProposeSchema(MetaCommand::MarkIndexBackfilled {
            table: "bf_control_only_t".into(),
            index: "by_email".into(),
            tablet: TabletId(700),
        }),
    )
    .await;
    await_index_status(
        &control_nodes,
        "bf_control_only_t",
        "by_email",
        IndexStatus::Active,
        10,
    )
    .await;

    for n in control_nodes.iter().chain(data_nodes.iter()) {
        n.shutdown_graceful().await;
    }
}
