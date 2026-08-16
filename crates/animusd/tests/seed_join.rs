//! **Seed/join startup** (ADR 0032 PR2): a new node starts knowing only its own
//! six addresses plus a *seed* list (the client addresses of already-running
//! nodes) — no expanded `ClusterConfig` listing every node up front, unlike
//! `run_node_growth`/ADR 0030's own test (`tests/cluster_growth.rs`). It
//! contacts a seed for `ClientRequest::JoinInfo`, discovers the pre-growth
//! control group + the live peer/route/admin address books, and becomes a
//! real ADR 0030 data-plane growth member with zero operator admin calls (no
//! `POST /admin/member/add` needed — `start_with`'s growth-node block
//! self-registers).
//!
//! Real TCP/time — polls with generous timeouts, not deterministic assertions.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use animusd::{
    ClientRequest, ClientResponse, ClusterConfig, Node, RoleAddrs, StorageBackend, read_frame,
};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

const TABLES: [&str; 3] = ["seedjoin0", "seedjoin1", "seedjoin2"];

/// Bring up the initial `n`-node config core (port-TOCTOU mitigation) — see
/// `support::bring_up_deadline`.
async fn bring_up(n: usize, dir: &Path) -> (Vec<Node>, ClusterConfig) {
    support::bring_up_deadline(n, dir, support::JOIN_DEADLINE).await
}

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|node| !node.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(30), ready)
        .await
        .expect("cluster did not bootstrap within 30s");
}

/// Join a fresh node with newly-allocated addresses (port-TOCTOU mitigation)
/// — see `support::join_fresh_deadline`. Returns the node, the addresses it
/// actually bound, and the data dir it used — the rejoin test needs all
/// three to reuse exactly.
async fn join_fresh(
    seeds: &[SocketAddr],
    index: usize,
    dir: &Path,
    backend: StorageBackend,
) -> (Node, RoleAddrs, PathBuf) {
    support::join_fresh_deadline(seeds, index, dir, backend, support::JOIN_DEADLINE).await
}

/// Rejoin at the same index/addresses/dir as a previous `join_fresh` call,
/// retrying the rebind briefly — the same port-TOCTOU mitigation
/// `support::restart_same_addrs` uses for a same-address restart: a
/// just-freed port can be stolen momentarily by another test binary's
/// `free_addrs` probe, and a same-address rejoin cannot route around it by
/// re-allocating (reusing the exact addresses/dir is the point of the test).
async fn rejoin_same(
    seeds: &[SocketAddr],
    index: usize,
    addrs: RoleAddrs,
    dir: &Path,
    backend: StorageBackend,
) -> Node {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match animusd::run_node_join(
            seeds.to_vec(),
            Some(animusd::config::node_id(index)),
            addrs.clone(),
            dir,
            backend,
            std::collections::BTreeMap::new(),
        )
        .await
        {
            Ok(node) => return node,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "rejoin on the same index/addresses/dir did not rebind: {e}"
                );
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, serde_json::Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\n\
         Host: animus\r\n\
         Content-Type: application/json\r\n\
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
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let value: serde_json::Value = serde_json::from_str(payload).expect("admin body is JSON");
    (status, value)
}

/// Every member's status, `raftkv_id -> "Active"/"Down"/...`, from
/// `/admin/status`.
async fn member_statuses(
    admin_addr: SocketAddr,
) -> std::collections::BTreeMap<animus_env::NodeId, String> {
    let (_s, v) = admin(admin_addr, "GET", "/admin/status", None).await;
    v["members"]
        .as_object()
        .expect("members is an object")
        .iter()
        .map(|(id, m)| {
            (
                id.parse().expect("member id key is a valid NodeId"),
                m["status"].as_str().expect("status is a string").to_owned(),
            )
        })
        .collect()
}

/// A table whose tablet currently lists `raftkv_id` as a replica, if any —
/// used to pick a **specific** table for a through-only-the-joined-node write
/// (step 5): rebalancing (ADR 0029) only ever proposes a move while it
/// improves the *global* imbalance, so with a handful of tables/tablets it
/// commonly moves just enough replicas to reach `max - min <= 1` and stops —
/// there is no guarantee every table's tablet ends up with a replica on the
/// joined node, only that *at least one* eventually does. A node that is not
/// a replica of a given table's tablet at all resolves its
/// CP route via a fallback that picks *some known* replica rather than the
/// tablet's actual current leader (`resolve_cp_route`'s no-local-replica
/// branch, `topology::decide_cp_route`'s `Forward`-to-any-known-route case) —
/// a distinct, cross-hop routing case `cp_serve_forwarded` never retries
/// around (it forwards at most one hop) and not what this test means to
/// exercise, so step 5 must pick a table this returns, not any/every table.
async fn table_with_replica(
    admin_addr: SocketAddr,
    raftkv_id: &animus_env::NodeId,
) -> Option<String> {
    let (_s, v) = admin(admin_addr, "GET", "/admin/status", None).await;
    v["tablets"]
        .as_object()
        .expect("tablets is an object")
        .values()
        .find_map(|t| {
            let has_replica = t["replicas"]
                .as_array()
                .expect("replicas is an array")
                .iter()
                .filter_map(|r| r.as_str())
                .any(|r| r == raftkv_id.as_str());
            has_replica
                .then(|| t["table"].as_str().map(str::to_owned))
                .flatten()
        })
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn node_joins_via_seed_with_no_expanded_config() {
    let dir = tempfile::tempdir().unwrap();

    // 1. Bring up a 3-node config core; create tables + write through it.
    let (core_nodes, core_config) = bring_up(3, dir.path()).await;
    await_bootstrap(&core_nodes).await;
    let core_clients: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.client).collect();
    let core_admin: Vec<SocketAddr> = core_config.nodes.iter().map(|a| a.admin).collect();
    for table in TABLES {
        put(&core_clients, table, b"k0", b"v0", 30).await;
    }

    // 2. Join a 4th node passing ONLY the core's client addresses as seeds —
    // no expanded config object anywhere in this test.
    let join_index = core_config.len();
    let join_raftkv_id = animusd::config::node_id(join_index);
    let (joined, joined_addrs, joined_dir) = join_fresh(
        &core_clients,
        join_index,
        dir.path(),
        StorageBackend::default(),
    )
    .await;

    // 3. It becomes an Active member with no admin call from this test at all
    // (the growth-node self-registration inside `start_with`, ADR 0032 PR2).
    let promoted = async {
        loop {
            let statuses = member_statuses(core_admin[0]).await;
            if statuses.get(&join_raftkv_id).map(String::as_str) == Some("Active") {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    };
    timeout(Duration::from_secs(20), promoted)
        .await
        .unwrap_or_else(|_| panic!("joined node never promoted to Active"));

    // 4. Rebalancing (ADR 0029) eventually places a real tablet replica on it
    // — the data-plane hosted-voters signal, not just a `Metadata` member
    // row. Rebalancing only ever proposes a move while it improves the
    // *global* imbalance and stops once `max - min <= 1`, so with a handful
    // of tables it may move just one table's tablet onto the joined node,
    // not every one — `hosted_table` is whichever table `table_with_replica`
    // finds (see its own doc), the one this test's step 5 must use for the
    // through-only-the-joined-node checks.
    let hosted_table: String = {
        let discover = async {
            loop {
                if let Some(table) = table_with_replica(core_admin[0], &join_raftkv_id).await {
                    return table;
                }
                sleep(Duration::from_millis(300)).await;
            }
        };
        timeout(Duration::from_secs(90), discover)
            .await
            .unwrap_or_else(|_| panic!("joined node never gained a tablet replica"))
    };

    // 5. Reads and writes work through the joined node's own client address
    // (for `hosted_table`, the one it's confirmed to actually replicate —
    // see `table_with_replica`'s doc for why this test must not pick an
    // arbitrary table here)...
    put(&[joined.client_addr()], &hosted_table, b"k1", b"v1", 30).await;
    await_value(&[joined.client_addr()], &hosted_table, b"k1", b"v1", 30).await;
    // ...and through an original core node, both directions: a write via a
    // core node is readable back through the joined node (forward-in), and a
    // write via the joined node (above) is readable through a core node
    // (forward-out, implicitly proven since both `put`/`await_value` calls
    // above round-trip through the CP group regardless of which address
    // served them).
    put(&core_clients, &hosted_table, b"k2", b"v2", 30).await;
    await_value(&[joined.client_addr()], &hosted_table, b"k2", b"v2", 30).await;
    await_value(&core_clients, &hosted_table, b"k2", b"v2", 30).await;

    // 6. `/admin/peers` on an original core node lists the joined node's admin
    // address (ADR 0032 PR1's union, now exercised by a node that joined via
    // seed/join rather than an expanded-config growth node).
    let peers_include_joined = async {
        loop {
            let (status, body) = admin(core_admin[0], "GET", "/admin/peers", None).await;
            if status == 200 {
                let listed: Vec<String> = body["admin_addrs"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                if listed.contains(&joined_addrs.admin.to_string()) {
                    return;
                }
            }
            sleep(Duration::from_millis(200)).await;
        }
    };
    timeout(Duration::from_secs(20), peers_include_joined)
        .await
        .unwrap_or_else(|_| panic!("an original node's /admin/peers never listed the joined node"));

    // 7. Collision: joining at the SAME index with DIFFERENT addresses must
    // fail loudly (the collision guard), and the cluster stays unharmed.
    let collision_addrs = {
        let raw = support::free_addrs(6);
        RoleAddrs {
            id: join_raftkv_id.clone(),
            role: animusd::config::NodeRole::Both,
            internal: raw[0],
            client: raw[1],
            dynamo: raw[2],
            cql: raw[3],
            admin: raw[4],
            intra: raw[5],
        }
    };
    let collision_result = animusd::run_node_join(
        core_clients.clone(),
        Some(join_raftkv_id.clone()),
        collision_addrs,
        &dir.path().join("collision"),
        StorageBackend::default(),
        std::collections::BTreeMap::new(),
    )
    .await;
    let err = match collision_result {
        Ok(_) => panic!("joining at an already-claimed index with different addresses must fail"),
        Err(e) => e,
    };
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::AlreadyExists,
        "unexpected collision-guard error: {err}"
    );
    // Cluster unharmed: the joined node's own earlier writes are still readable.
    await_value(&[joined.client_addr()], &hosted_table, b"k1", b"v1", 10).await;
    await_value(&core_clients, &hosted_table, b"k1", b"v1", 10).await;

    // 8. Rejoin: shut the joined node down, then join again at the same
    // index/addresses/dir — recovers and serves, no collision-guard error
    // (an identical address book is a rejoin, not a conflict).
    joined.shutdown();
    sleep(Duration::from_millis(200)).await;
    let rejoined = rejoin_same(
        &core_clients,
        join_index,
        joined_addrs,
        &joined_dir,
        StorageBackend::default(),
    )
    .await;
    await_value(&[rejoined.client_addr()], &hosted_table, b"k1", b"v1", 30).await;
    await_value(&[rejoined.client_addr()], &hosted_table, b"k2", b"v2", 30).await;

    rejoined.shutdown();
    for node in core_nodes {
        node.shutdown();
    }
}
