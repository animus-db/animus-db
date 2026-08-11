//! **Data-only seed/join** (ADR 0035 PR5, `animusd data --seed`): a data node
//! starts knowing only its own five addresses (no `control` address — it is
//! data-only) plus a seed list, discovers a running **split** deployment's
//! pre-existing control group from the seed's `JoinInfo` reply, and joins as
//! a genuine `ControlHandle::Remote` member with **no local control
//! `RaftCore` at all** — the data-only dual of `tests/seed_join.rs`'s
//! combined-mode join, reusing the exact same `discover_join_info`/
//! `check_join_collision` helpers under the hood (see
//! `animusd::run_node_data_join`'s doc).
//!
//! Covers, over real TCP/time (a converged-or-timeout poll, never a fixed
//! sleep):
//! - the joined node self-registers (relayed `admin_add_member`, since a
//!   data-only node can never satisfy `propose_schema`'s local-leader
//!   branch) and is promoted `Active` by the unmodified ADR 0012 heartbeat/
//!   failure-detector chain, with **zero** operator admin calls from this
//!   test;
//! - the placement rebalancer (ADR 0029) eventually places a real tablet
//!   replica on it — seeded across several independent tables (the
//!   documented lesson: with too few tables the pre-join cluster can already
//!   be at `max - min <= 1` and the rebalancer never moves anything, so this
//!   seeds enough tables that the joined node is guaranteed to gain at least
//!   one real replica);
//! - reads and writes work through the joined node's own client address for
//!   the specific table it's confirmed to actually replicate, round-tripping
//!   both ways against the pre-existing data nodes.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, Node, StorageBackend, read_frame};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;
use support::{await_data_nodes_active, await_leader, bring_up_split};

const TABLES: [&str; 3] = ["datajoin0", "datajoin1", "datajoin2"];

async fn call(addr: SocketAddr, req: ClientRequest) -> Option<ClientResponse> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    animusd::write_frame(&mut stream, &req).await.ok()?;
    read_frame(&mut stream).await.ok()?
}

/// Try every client address in `clients` (round-robin) until one accepts the
/// write.
async fn put(clients: &[SocketAddr], table: &str, key: &[u8], value: &[u8], secs: u64) {
    let mut last: Option<ClientResponse> = None;
    let w = async {
        loop {
            for &c in clients {
                let resp = call(
                    c,
                    ClientRequest::Put {
                        key: key.to_vec(),
                        value: value.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await;
                if let Some(ClientResponse::PutOk) = &resp {
                    return;
                }
                last = resp;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(secs), w)
        .await
        .unwrap_or_else(|_| {
            panic!("write of {table}/{key:?} never committed; last reply: {last:?}")
        });
}

async fn await_value(clients: &[SocketAddr], table: &str, key: &[u8], want: &[u8], secs: u64) {
    let p = async {
        loop {
            for &c in clients {
                if let Some(ClientResponse::Value(Some(v))) = call(
                    c,
                    ClientRequest::Get {
                        key: key.to_vec(),
                        table: table.to_string(),
                    },
                )
                .await
                    && v == want
                {
                    return;
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    };
    timeout(Duration::from_secs(secs), p)
        .await
        .unwrap_or_else(|_| panic!("key {table}/{key:?} never read back as {want:?}"));
}

/// One HTTP/1.0 GET to the admin endpoint; returns the parsed JSON body.
async fn admin_get(addr: SocketAddr, path: &str) -> serde_json::Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (_head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"))
}

/// Every member's status, `raftkv_id -> "Active"/"Down"/...`, from
/// `/admin/status` (mirrors `tests/seed_join.rs::member_statuses`).
async fn member_statuses(admin_addr: SocketAddr) -> std::collections::BTreeMap<u64, String> {
    let v = admin_get(admin_addr, "/admin/status").await;
    v["members"]
        .as_object()
        .expect("members is an object")
        .iter()
        .map(|(id, m)| {
            (
                id.parse().expect("member id key is numeric"),
                m["status"].as_str().expect("status is a string").to_owned(),
            )
        })
        .collect()
}

/// A table whose tablet currently lists `raftkv_id` as a replica, if any —
/// mirrors `tests/seed_join.rs::table_with_replica`'s doc verbatim: the
/// rebalancer only ever proposes a move while it improves the *global*
/// imbalance, so a through-only-the-joined-node write must target a table
/// this actually returns, not an arbitrary one.
async fn table_with_replica(admin_addr: SocketAddr, raftkv_id: u64) -> Option<String> {
    let v = admin_get(admin_addr, "/admin/status").await;
    v["tablets"]
        .as_object()
        .expect("tablets is an object")
        .values()
        .find_map(|t| {
            let has_replica = t["replicas"]
                .as_array()
                .expect("replicas is an array")
                .iter()
                .filter_map(serde_json::Value::as_u64)
                .any(|r| r == raftkv_id);
            has_replica
                .then(|| t["table"].as_str().map(str::to_owned))
                .flatten()
        })
}

/// Join a fresh data-only node against `seeds` (port-TOCTOU mitigation) —
/// see `support::join_data_fresh_deadline`.
async fn join_data_fresh(
    seeds: &[SocketAddr],
    index: usize,
    dir: &Path,
    backend: StorageBackend,
) -> Node {
    support::join_data_fresh_deadline(seeds, index, dir, backend, support::JOIN_DEADLINE).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn data_node_joins_a_split_cluster_via_seed_and_gets_a_rebalanced_replica() {
    let dir = tempfile::tempdir().unwrap();

    // 1. Bring up a split cluster (3 control-only + 2 data-only) and wait for
    // both the control deployment's own leader and the two data nodes'
    // promotion to `Active`.
    let (control_nodes, data_nodes, config) = bring_up_split(3, 2, dir.path()).await;
    await_leader(&control_nodes).await;
    let existing_data_raftkv_ids: Vec<animus_env::NodeId> =
        (3..5).map(animusd::config::node_id).collect();
    await_data_nodes_active(&control_nodes, &existing_data_raftkv_ids).await;

    // 2. Create several independent tables and write through the existing
    // data nodes — several, not one, so the pre-join cluster is genuinely
    // imbalanced (see this file's module doc / `table_with_replica`'s doc for
    // why one table isn't enough to guarantee the rebalancer moves anything
    // onto the node this test is about to join).
    let data_clients: Vec<SocketAddr> = data_nodes.iter().map(Node::client_addr).collect();
    for table in TABLES {
        put(&data_clients, table, b"k0", b"v0", 30).await;
    }

    // 3. Join a THIRD data-only node — no expanded config, just the client
    // addresses of every already-running node (control *and* data) as seeds,
    // mirroring how an operator would point a new data node at a live
    // cluster without knowing in advance which nodes are control vs. data.
    let mut seeds: Vec<SocketAddr> = control_nodes.iter().map(Node::client_addr).collect();
    seeds.extend(data_clients.iter().copied());
    let join_index = config.len();
    let join_raftkv_id = animusd::config::node_id(join_index);
    let joined = join_data_fresh(&seeds, join_index, dir.path(), StorageBackend::Memory).await;

    // 4. It becomes an Active member with zero operator admin calls from
    // this test (the relayed `admin_add_member` self-registration inside
    // `BoundDataNode::start_data_with`).
    let control_admin: Vec<SocketAddr> = control_nodes.iter().map(Node::admin_addr).collect();
    let promoted = async {
        loop {
            let statuses = member_statuses(control_admin[0]).await;
            if statuses.get(&join_raftkv_id).map(String::as_str) == Some("Active") {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), promoted)
        .await
        .unwrap_or_else(|_| panic!("joined data node never promoted to Active"));

    // 5. The rebalancer (ADR 0029) eventually places a real tablet replica
    // on it — the data-plane hosted-voters signal, not just a `Metadata`
    // member row.
    let hosted_table: String = {
        let discover = async {
            loop {
                if let Some(table) = table_with_replica(control_admin[0], join_raftkv_id).await {
                    return table;
                }
                sleep(Duration::from_millis(300)).await;
            }
        };
        timeout(Duration::from_secs(90), discover)
            .await
            .unwrap_or_else(|_| panic!("joined data node never gained a tablet replica"))
    };

    // 6. Reads and writes work through the joined node's own client address
    // for `hosted_table` (the one it's confirmed to actually replicate), and
    // round-trip both ways against the pre-existing data nodes — proving the
    // joined node is a genuine participant in the CP data path, not just a
    // registered-but-inert member.
    put(&[joined.client_addr()], &hosted_table, b"k1", b"v1", 30).await;
    await_value(&data_clients, &hosted_table, b"k1", b"v1", 30).await;
    put(&data_clients, &hosted_table, b"k2", b"v2", 30).await;
    await_value(&[joined.client_addr()], &hosted_table, b"k2", b"v2", 30).await;

    for node in control_nodes
        .iter()
        .chain(data_nodes.iter())
        .chain(std::iter::once(&joined))
    {
        node.shutdown_graceful().await;
    }
}
