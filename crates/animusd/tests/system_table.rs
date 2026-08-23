//! `GET /admin/system-table` (plan-syskv-ui, an ADR 0038 addendum) — the
//! read-only system-keyspace browse surface, end to end over real TCP
//! (`ProdEnv`), as an operator or the dashboard's browse section would hit
//! it.
//!
//! Two scenarios, each its own single-node combined-mode process (a single
//! voter self-elects immediately, so bring-up is fast and this file doesn't
//! need `support::free_addrs`'s multi-node port-TOCTOU retry dance):
//!
//! - `system_table_lists_every_seeded_entity_kind` — seeds every
//!   [`animus_control::syskv::EntityKind`] via the client protocol (a plain
//!   `Put` auto-provisions a tablet, `ProposeSchema` reaches every other
//!   `MetaCommand` this plane can mirror, a copy-based split round produces
//!   an (ADR 0050 fork F9) `SplitLineage` row; `member`/
//!   `node_addrs` come from the
//!   bootstrapped node's own self-registration — ADR 0040 PR4 retired the
//!   ADR 0036 allocator's dedicated `NodeIdAlloc` ledger kind along with the
//!   allocator itself, since `MetaCommand::RegisterNode`'s claim lives
//!   entirely in the already-mirrored `member`/`node_addrs` kinds, no
//!   separate ledger needed), then asserts every kind appears with the
//!   exact value shape the plan's decode table specifies, plus the `kind`
//!   query filter narrowing correctly.
//! - `system_table_pagination_is_gapless_and_duplicate_free` — seeds many
//!   `tablet` rows (one plain `Put` per distinct table auto-provisions one),
//!   then walks the forward-only pager at a small `limit` and diffs the
//!   concatenated pages against one unlimited fetch.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use animus_control::PlacementPolicy;
use animus_env::nid;
use animus_tablet::TabletId;
use animusd::{ClientRequest, ClientResponse, ColumnType, MetaCommand, Node, TableSchema};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

/// Bring up a single combined-mode node — a 1-of-1 Raft group self-elects on
/// its first tick, so this is fast and needs no port-TOCTOU retry loop (a
/// fresh `tempfile::tempdir()` + freshly-bound `:0` ports per call is enough
/// isolation for one node).
async fn bring_up_one(dir: &std::path::Path) -> Node {
    let addrs = free_addrs(6);
    let node_cfg = animusd::RoleAddrs {
        id: animusd::config::node_id(0),
        role: animusd::config::NodeRole::Both,
        internal: addrs[0],
        client: addrs[1],
        dynamo: addrs[2],
        admin: addrs[3],
        intra: addrs[4],
        console: addrs[5],
    };
    let config = animusd::ClusterConfig {
        nodes: vec![node_cfg],
    };
    animusd::run_node(&config, 0, dir.join("node-0"))
        .await
        .expect("start single node")
}

/// Reserve `count` free loopback ports (bind :0, read addr, release).
fn free_addrs(count: usize) -> Vec<SocketAddr> {
    let ls: Vec<std::net::TcpListener> = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter().map(|l| l.local_addr().unwrap()).collect()
}

async fn await_bootstrap(node: &Node) {
    timeout(Duration::from_secs(20), async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("single node did not bootstrap in 20s");
}

/// One HTTP/1.0 GET to the admin endpoint; returns `(status, parsed JSON)`.
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, Value) {
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
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let value: Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

async fn call(addr: SocketAddr, req: ClientRequest) -> ClientResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    animusd::write_frame(&mut stream, &req).await.expect("send");
    animusd::read_frame(&mut stream)
        .await
        .expect("read")
        .expect("a reply")
}

/// A plain `Put`, retried on ANY `ClientResponse::Error` reply for up to
/// 20s: a put is idempotent, and the first write against a fresh table
/// right after bootstrap can legitimately race the tablet-host reconciler
/// (documented in `docs/engineering-lessons.md`'s "CP write-forward path
/// has no retry-on-not-the-leader-here" entry) or hit the confirm-loop
/// futility-retry shape (issue #268).
async fn put_retry(addr: SocketAddr, table: &str, key: &[u8], value: &[u8]) -> ClientResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let resp = call(
            addr,
            ClientRequest::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                table: table.to_string(),
            },
        )
        .await;
        match resp {
            ClientResponse::PutOk => return resp,
            ClientResponse::Error(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            other => return other,
        }
    }
}

/// Poll `/admin/status` until `pred(&status)` holds, or panic after 15s —
/// `ProposeSchema`'s `PutOk` only means "reached the leader's log", never
/// "committed" (see `ClientCtx::propose_schema`'s doc), so every seed step
/// below confirms via the replicated view, not the reply alone.
async fn await_status<F: Fn(&Value) -> bool>(addr: SocketAddr, pred: F, what: &str) -> Value {
    timeout(Duration::from_secs(15), async {
        loop {
            let (_, status) = admin_get(addr, "/admin/status").await;
            if pred(&status) {
                return status;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} did not converge in 15s"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn system_table_lists_every_seeded_entity_kind() {
    timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();
        let node = bring_up_one(dir.path()).await;
        await_bootstrap(&node).await;
        // ADR 0047: this file seeds several intra-only entity kinds via bare
        // `ProposeSchema` — dial the intra port (which also happily serves
        // the plain `Put`/`Scan`/`SplitTablet` calls this test otherwise
        // makes, since intra is a superset of client).
        let client = node.intra_addr();
        let admin = node.admin_addr();

        let (s, syst0) = admin_get(admin, "/admin/system-table").await;
        assert_eq!(s, 200);
        assert_eq!(
            syst0["available"], true,
            "a combined node has a system keyspace to browse: {syst0}"
        );
        let applied0 = syst0["applied_index"].as_u64().expect("applied_index");

        // ---- Tablet + Counter: a plain Put auto-provisions a tablet -------
        let put = put_retry(client, "kv", b"a", b"v").await;
        assert!(matches!(put, ClientResponse::PutOk), "{put:?}");
        await_status(
            admin,
            |s| s["tablets"].as_object().is_some_and(|t| !t.is_empty()),
            "tablet auto-provisioned",
        )
        .await;

        // ---- Schema ---------------------------------------------------------
        let resp = call(
            client,
            ClientRequest::ProposeSchema(MetaCommand::CreateTableSchema {
                table: "orders".to_string(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
        )
        .await;
        assert!(matches!(resp, ClientResponse::PutOk), "{resp:?}");
        await_status(
            admin,
            // `Metadata::schemas` is a `SchemaCatalog` (a thin `{"tables":
            // {..}}` wrapper around the map, not a bare map itself).
            |s| s["schemas"]["tables"].get("orders").is_some(),
            "schema committed",
        )
        .await;

        // ---- Policy (on the bootstrap tablet, id 1) --------------------------
        let resp = call(
            client,
            ClientRequest::ProposeSchema(MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("p", 1)),
            }),
        )
        .await;
        assert!(matches!(resp, ClientResponse::PutOk), "{resp:?}");
        await_status(
            admin,
            |s| s["policies"].get("1").is_some(),
            "policy committed",
        )
        .await;

        // ---- CpMemberAddr (legacy) --------------------------------------------
        let resp = call(
            client,
            ClientRequest::ProposeSchema(MetaCommand::RegisterCpAddr {
                id: nid(9999),
                addr: "127.0.0.1:1".to_string(),
                tablet: Some(TabletId(1)),
            }),
        )
        .await;
        assert!(matches!(resp, ClientResponse::PutOk), "{resp:?}");
        await_status(
            admin,
            |s| s["cp_member_addrs"].get("n9999").is_some(),
            "cp_member_addr committed",
        )
        .await;

        // ---- SplitLineage: a copy-based split round --------------------------
        // ADR 0050: Begin+Cutover on the (empty) bootstrap tablet — this
        // test only asserts the SYSTEM keyspace's entity kinds (including
        // `split_lineage`), never post-split data placement.
        let meta = node.metadata();
        let source = animus_tablet::TabletId(1);
        let expected_epoch = meta.tablets[&source].epoch;
        let replicas = meta.tablets[&source].replicas.clone();
        let new_id = meta.next_free_tablet_id();
        let cmd = animus_control::MetaCommand::BeginSplit {
            parent: source,
            expected_epoch,
            split_key: b"m".to_vec(),
            children: [
                (new_id, replicas.clone()),
                (animus_tablet::TabletId(new_id.0 + 1), replicas),
            ],
        };
        assert!(
            node.propose_meta(cmd),
            "the node's control handle must accept the harness begin-split proposal"
        );
        assert!(
            node.propose_meta(animus_control::MetaCommand::CutoverSplit {
                parent: source,
                expected_epoch: expected_epoch.next(),
                cutover_wall_ms: 1_000,
            }),
            "the node's control handle must accept the harness cutover proposal"
        );
        let after_split = await_status(
            admin,
            |s| s["tablets"].as_object().is_some_and(|t| t.len() >= 2),
            "split committed",
        )
        .await;
        let sibling_id: u64 = after_split["tablets"]
            .as_object()
            .expect("tablets map")
            .keys()
            .filter_map(|k| k.parse::<u64>().ok())
            .find(|&id| id != 1)
            .expect("a sibling tablet exists after split");

        // ---- every seeded kind eventually shows up in the browse surface ----
        const EXPECT_KINDS: [&str; 8] = [
            "tablet",
            "member",
            "schema",
            "policy",
            "node_addrs",
            "counter",
            "cp_member_addr",
            // ADR 0050 fork F9: the cutover above froze this lineage row.
            "split_lineage",
        ];
        let full = timeout(Duration::from_secs(15), async {
            loop {
                let (s, syst) = admin_get(admin, "/admin/system-table?limit=1000").await;
                assert_eq!(s, 200);
                assert_eq!(syst["available"], true);
                let items = syst["items"].as_array().cloned().unwrap_or_default();
                let kinds: BTreeSet<&str> = items
                    .iter()
                    .map(|it| it["kind"].as_str().expect("kind is a string"))
                    .collect();
                if EXPECT_KINDS.iter().all(|k| kinds.contains(k)) {
                    return syst;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("every seeded entity kind appears within 15s");

        let applied_after = full["applied_index"].as_u64().expect("applied_index");
        assert!(
            applied_after > applied0,
            "applied_index advances as commands land: {applied0} -> {applied_after}"
        );
        assert_eq!(
            full["kind_filter"],
            Value::Null,
            "no kind filter was requested: {full}"
        );
        assert_eq!(
            full["truncated"], false,
            "limit=1000 comfortably covers this tiny keyspace: {full}"
        );

        let items = full["items"].as_array().expect("items array");
        let find_one = |kind: &str| {
            items
                .iter()
                .find(|it| it["kind"] == kind)
                .unwrap_or_else(|| panic!("missing a {kind} row: {items:?}"))
        };
        let find = |kind: &str, id: &str| {
            items
                .iter()
                .find(|it| it["kind"] == kind && it["id"] == id)
                .unwrap_or_else(|| panic!("missing {kind}/{id} row: {items:?}"))
        };

        // ---- per-kind value-shape asserts (mirrors mirror.rs exactly) --------
        let tablet_item = find_one("tablet");
        assert!(
            tablet_item["value"].is_object(),
            "tablet value is JSON passthrough: {tablet_item}"
        );
        assert!(
            tablet_item["id"].is_string(),
            "a numeric kind's id renders as a decimal string: {tablet_item}"
        );
        assert!(tablet_item["version"].is_u64());

        let member_item = find_one("member");
        assert!(member_item["value"].is_object());
        assert!(member_item["id"].is_string());

        let schema_item = find("schema", "orders");
        assert!(schema_item["value"].is_object());

        let policy_item = find("policy", &sibling_id.to_string());
        assert!(policy_item["value"].is_object());

        let node_addrs_item = find_one("node_addrs");
        assert!(node_addrs_item["value"].is_object());

        // ADR 0050 fork F9: the cutover above froze `split_lineage[sibling_id]`
        // naming parent tablet 1 — a JSON entity (unlike the retired
        // split_parent raw-TabletId rendering).
        let split_lineage_item = find("split_lineage", &sibling_id.to_string());
        assert_eq!(
            split_lineage_item["value"]["parent"], 1,
            "split_lineage's value names the retired parent tablet: {split_lineage_item}"
        );

        let counter_item = find("counter", "next_tablet_id");
        assert!(
            counter_item["value"].is_u64(),
            "counter value is a raw u64 rendered as a JSON number: {counter_item}"
        );

        let cp_addr_item = find("cp_member_addr", "n9999");
        assert!(cp_addr_item["value"].is_object());

        // ---- kind filter narrows correctly -----------------------------------
        let (s, filtered) = admin_get(admin, "/admin/system-table?kind=schema&limit=1000").await;
        assert_eq!(s, 200);
        assert_eq!(filtered["kind_filter"], "schema");
        let filtered_items = filtered["items"].as_array().expect("items array");
        assert!(
            filtered_items.iter().all(|it| it["kind"] == "schema"),
            "kind=schema returns only schema rows: {filtered_items:?}"
        );
        assert!(
            filtered_items.iter().any(|it| it["id"] == "orders"),
            "the seeded orders schema is among them: {filtered_items:?}"
        );

        // An unrecognized kind is a clean 400, not a silently-empty filter.
        let (s, bad) = admin_get(admin, "/admin/system-table?kind=not-a-real-kind").await;
        assert_eq!(s, 400, "unrecognized kind is rejected: {bad}");

        node.shutdown_graceful().await;
    })
    .await
    .expect("system_table_lists_every_seeded_entity_kind timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn system_table_pagination_is_gapless_and_duplicate_free() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let node = bring_up_one(dir.path()).await;
        await_bootstrap(&node).await;
        // ADR 0047: this file seeds several intra-only entity kinds via bare
        // `ProposeSchema` — dial the intra port (which also happily serves
        // the plain `Put`/`Scan`/`SplitTablet` calls this test otherwise
        // makes, since intra is a superset of client).
        let client = node.intra_addr();
        let admin = node.admin_addr();

        // Auto-provision a dozen distinct tablets (one plain Put per table) —
        // plenty of rows to page through at a small limit, without needing
        // schema/policy/keyspace/split/merge machinery this test doesn't care
        // about.
        const N_TABLES: usize = 12;
        for i in 0..N_TABLES {
            let put = put_retry(client, &format!("t{i}"), b"k", b"v").await;
            assert!(matches!(put, ClientResponse::PutOk), "{put:?}");
        }
        await_status(
            admin,
            |s| {
                s["tablets"]
                    .as_object()
                    .is_some_and(|t| t.len() >= N_TABLES)
            },
            "every table's tablet provisioned",
        )
        .await;

        // The oracle: one unlimited (well past this tiny keyspace's size) scan.
        let (s, oracle) = admin_get(admin, "/admin/system-table?limit=1000").await;
        assert_eq!(s, 200);
        assert_eq!(
            oracle["truncated"], false,
            "the oracle scan isn't itself truncated: {oracle}"
        );
        let oracle_items: Vec<Value> = oracle["items"].as_array().cloned().expect("items");
        assert!(
            oracle_items.len() >= N_TABLES,
            "at least one row per provisioned tablet: {}",
            oracle_items.len()
        );

        // Walk the forward-only pager at a small limit and concatenate.
        let mut paged_items: Vec<Value> = Vec::new();
        let mut after: Option<String> = None;
        let mut pages = 0usize;
        loop {
            pages += 1;
            assert!(
                pages <= oracle_items.len() + 2,
                "pager should terminate promptly"
            );
            let mut path = "/admin/system-table?limit=3".to_string();
            if let Some(a) = &after {
                path.push_str("&after=");
                path.push_str(&urlencode(a));
            }
            let (s, page) = admin_get(admin, &path).await;
            assert_eq!(s, 200);
            let items = page["items"].as_array().cloned().expect("items");
            paged_items.extend(items);
            if page["truncated"] == true {
                after = Some(
                    page["next_after"]
                        .as_str()
                        .expect("truncated page carries next_after")
                        .to_string(),
                );
            } else {
                assert!(
                    page["next_after"].is_null(),
                    "an untruncated page has no next_after: {page}"
                );
                break;
            }
        }

        // Gapless + duplicate-free: the same (kind, id) pairs, in the same
        // order, as the one unlimited oracle scan.
        let key_of = |it: &Value| {
            (
                it["kind"].as_str().expect("kind is a string").to_string(),
                it["id"].to_string(),
            )
        };
        let oracle_keys: Vec<_> = oracle_items.iter().map(key_of).collect();
        let paged_keys: Vec<_> = paged_items.iter().map(key_of).collect();
        assert_eq!(
            paged_keys, oracle_keys,
            "paginated walk reproduces the oracle scan exactly, in order"
        );
        let dedup: BTreeSet<_> = paged_keys.iter().cloned().collect();
        assert_eq!(
            dedup.len(),
            paged_keys.len(),
            "no (kind, id) pair repeats across pages"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("system_table_pagination_is_gapless_and_duplicate_free timed out");
}

/// Percent-encode a base64url cursor for the `?after=` query parameter (it
/// can contain `-`/`_`, both already query-safe, but keep this honest about
/// what a real client would do).
fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
