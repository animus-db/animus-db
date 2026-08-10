//! AnimusDB operator/client CLI (`animus`).
//!
//! Usage:
//!   animus status <node-addr>
//!   animus put    <node-addr> <table> <key> <value>
//!   animus get    <node-addr> <table> <key>
//!   animus admin  <subcommand> <admin-addr> [args...]
//!
//! `status`/`put`/`get` are a thin plain-TCP client over a node's request/reply
//! API. `admin` talks the HTTP/JSON admin interface (ADR 0020) on a node's
//! **admin** address (as printed by `animusd`), printing the JSON response. See
//! [`ADMIN_USAGE`] for the subcommands.

use std::process::ExitCode;
use std::time::Duration;

use animusd::{ClientRequest, ClientResponse, read_frame, write_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("animus: {msg}");
            eprintln!(
                "\nusage:\n  animus status <node-addr>\n  animus put <node-addr> <table> <key> <value>\n  animus get <node-addr> <table> <key>\n{ADMIN_USAGE}"
            );
            ExitCode::FAILURE
        }
    }
}

const ADMIN_USAGE: &str = "  admin <subcommand> <admin-addr> [args]:\n    \
    config|status|raft|raftkv|metrics|health <admin-addr>\n    \
    lsm|wal <admin-addr> [tablet]\n    \
    wal-segment <admin-addr> <seg> [tablet]\n    \
    key <admin-addr> <key> [tablet]\n    \
    split <admin-addr> <tablet> <split-key>\n    \
    merge <admin-addr> <left> <right>\n    \
    flush|compact <admin-addr> <tablet>\n    \
    reconfigure <admin-addr> <tablet> <voter,voter,...>\n    \
    drain <admin-addr> <node-id>\n    \
    drain-status <admin-addr> <node-id>\n    \
    remove <admin-addr> <node-id>\n    \
    decommission <admin-addr> <node-id>";

async fn run(args: &[String]) -> Result<(), String> {
    let cmd = args.first().map(String::as_str).ok_or("missing command")?;
    if cmd == "admin" {
        return run_admin(&args[1..]).await;
    }
    let addr = args.get(1).ok_or("missing <node-addr>")?;

    let request = match cmd {
        "status" => ClientRequest::Status,
        "put" => {
            // Every key names a table (ADR 0023): `put <addr> <table> <key> <value>`.
            let table = args.get(2).ok_or("put needs <table>")?;
            let key = args.get(3).ok_or("put needs <key>")?;
            let value = args.get(4).ok_or("put needs <value>")?;
            ClientRequest::Put {
                key: key.clone().into_bytes(),
                value: value.clone().into_bytes(),
                table: table.clone(),
            }
        }
        "get" => {
            // `get <addr> <table> <key>`.
            let table = args.get(2).ok_or("get needs <table>")?;
            let key = args.get(3).ok_or("get needs <key>")?;
            ClientRequest::Get {
                key: key.clone().into_bytes(),
                table: table.clone(),
            }
        }
        other => return Err(format!("unknown command `{other}`")),
    };

    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("cannot connect to {addr}: {e}"))?;
    write_frame(&mut stream, &request)
        .await
        .map_err(|e| format!("send failed: {e}"))?;
    let response: ClientResponse = read_frame(&mut stream)
        .await
        .map_err(|e| format!("recv failed: {e}"))?
        .ok_or("node closed the connection without replying")?;

    print_response(&response);
    if matches!(response, ClientResponse::Error(_)) {
        return Err("operation failed".into());
    }
    Ok(())
}

/// The `admin` subcommand group: speak the HTTP/JSON admin interface (ADR 0020)
/// on a node's admin address. `args[0]` is the subcommand, `args[1]` the address.
async fn run_admin(args: &[String]) -> Result<(), String> {
    let sub = args
        .first()
        .map(String::as_str)
        .ok_or("admin needs a subcommand")?;
    let addr = args.get(1).ok_or("admin needs <admin-addr>")?;
    let arg = |i: usize| args.get(i).map(String::as_str);
    // The optional trailing `tablet` for the storage GET routes.
    let tablet_q = |i: usize| arg(i).map_or(String::new(), |t| format!("?tablet={t}"));

    // `decommission` is a multi-step composite (drain → poll drain-status →
    // remove), not a single request/response — handled separately from the
    // generic one-shot dispatch below (ADR 0032 PR3).
    if sub == "decommission" {
        let node = arg(2).ok_or("decommission needs <node-id>")?;
        return run_decommission(addr, node).await;
    }

    let (method, path, body): (&str, String, Option<String>) = match sub {
        "config" => ("GET", "/admin/config".into(), None),
        "status" => ("GET", "/admin/status".into(), None),
        "raft" => ("GET", "/admin/raft".into(), None),
        "raftkv" => ("GET", "/admin/raftkv".into(), None),
        "metrics" => ("GET", "/admin/metrics".into(), None),
        "health" => ("GET", "/admin/health".into(), None),
        "lsm" => ("GET", format!("/admin/storage/lsm{}", tablet_q(2)), None),
        "wal" => ("GET", format!("/admin/storage/wal{}", tablet_q(2)), None),
        "wal-segment" => {
            let seg = arg(2).ok_or("wal-segment needs <seg>")?;
            let tablet = arg(3).unwrap_or("1");
            (
                "GET",
                format!("/admin/storage/wal/segment?seg={seg}&tablet={tablet}"),
                None,
            )
        }
        "key" => {
            let key = arg(2).ok_or("key needs <key>")?;
            let tablet = arg(3).unwrap_or("1");
            (
                "GET",
                format!("/admin/storage/key?key={key}&tablet={tablet}"),
                None,
            )
        }
        "split" => {
            let tablet: u64 = arg(2)
                .ok_or("split needs <tablet>")?
                .parse()
                .map_err(|_| "tablet must be a number")?;
            let split_key = arg(3).ok_or("split needs <split-key>")?;
            let body = serde_json::json!({"tablet": tablet, "split_key": split_key}).to_string();
            ("POST", "/admin/tablet/split".into(), Some(body))
        }
        "merge" => {
            let left: u64 = arg(2)
                .ok_or("merge needs <left>")?
                .parse()
                .map_err(|_| "left must be a number")?;
            let right: u64 = arg(3)
                .ok_or("merge needs <right>")?
                .parse()
                .map_err(|_| "right must be a number")?;
            let body = serde_json::json!({"left": left, "right": right}).to_string();
            ("POST", "/admin/tablet/merge".into(), Some(body))
        }
        "flush" | "compact" => {
            let tablet: u64 = arg(2)
                .ok_or("needs <tablet>")?
                .parse()
                .map_err(|_| "tablet must be a number")?;
            let body = serde_json::json!({"tablet": tablet}).to_string();
            ("POST", format!("/admin/storage/{sub}"), Some(body))
        }
        "reconfigure" => {
            let tablet: u64 = arg(2)
                .ok_or("reconfigure needs <tablet>")?
                .parse()
                .map_err(|_| "tablet must be a number")?;
            let voters: Result<Vec<u64>, _> = arg(3)
                .ok_or("reconfigure needs <voter,voter,...>")?
                .split(',')
                .map(|v| v.trim().parse::<u64>())
                .collect();
            let voters = voters.map_err(|_| "voters must be comma-separated node ids")?;
            let body = serde_json::json!({"tablet": tablet, "voters": voters}).to_string();
            ("POST", "/admin/raftkv/reconfigure".into(), Some(body))
        }
        "drain" => {
            let node: u64 = arg(2)
                .ok_or("drain needs <node-id>")?
                .parse()
                .map_err(|_| "node id must be a number")?;
            let body = serde_json::json!({"node": node}).to_string();
            ("POST", "/admin/drain".into(), Some(body))
        }
        "drain-status" => {
            let node = arg(2).ok_or("drain-status needs <node-id>")?;
            (
                "GET",
                format!("/admin/member/drain-status?node={node}"),
                None,
            )
        }
        "remove" => {
            let node: u64 = arg(2)
                .ok_or("remove needs <node-id>")?
                .parse()
                .map_err(|_| "node id must be a number")?;
            let body = serde_json::json!({"node": node}).to_string();
            ("POST", "/admin/member/remove".into(), Some(body))
        }
        other => return Err(format!("unknown admin subcommand `{other}`")),
    };

    let (status, response) = http_call(addr, method, &path, body).await?;
    println!("{response}");
    if !(200..300).contains(&status) {
        return Err(format!("admin request failed (HTTP {status})"));
    }
    Ok(())
}

/// The operator's whole decommission flow (ADR 0032 PR3), as a single
/// command: `POST /admin/drain` → poll `GET /admin/member/drain-status` until
/// draining has actually converged (no tablet still references the member,
/// and it isn't mid-service) → `POST /admin/member/remove`. All three requests
/// go to `addr`, which must be the **control-plane leader's** admin port —
/// both `/admin/drain` and `/admin/member/remove` are deliberately
/// local-leader-only, not relayed (see `is_relayable_command`'s doc in
/// `animusd`), so this fails loudly with the same "not the control-plane
/// leader" error a bare `drain`/`remove` call would if pointed at a follower.
async fn run_decommission(addr: &str, node: &str) -> Result<(), String> {
    let node: u64 = node.parse().map_err(|_| "node id must be a number")?;
    let drain_body = serde_json::json!({"node": node}).to_string();
    let (status, resp) = http_call(addr, "POST", "/admin/drain", Some(drain_body)).await?;
    if !(200..300).contains(&status) {
        return Err(format!("drain failed (HTTP {status}): {resp}"));
    }
    println!("draining node {node}...");

    let status_path = format!("/admin/member/drain-status?node={node}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let (status, resp) = http_call(addr, "GET", &status_path, None).await?;
        if !(200..300).contains(&status) {
            return Err(format!("drain-status failed (HTTP {status}): {resp}"));
        }
        let v: serde_json::Value = serde_json::from_str(&resp)
            .map_err(|e| format!("malformed drain-status response: {e}"))?;
        let tablets_remaining = v.get("tablets_remaining").and_then(|x| x.as_u64());
        let node_status = v.get("status").and_then(|x| x.as_str()).unwrap_or("?");
        println!(
            "  drain-status: status={node_status} tablets_remaining={}",
            tablets_remaining.map_or_else(|| "?".to_string(), |n| n.to_string())
        );
        if tablets_remaining == Some(0) && node_status != "Active" {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "node {node} did not finish draining within 120s \
                 (status={node_status}, tablets_remaining={tablets_remaining:?})"
            ));
        }
        sleep(Duration::from_millis(500)).await;
    }

    let remove_body = serde_json::json!({"node": node}).to_string();
    let (status, resp) = http_call(addr, "POST", "/admin/member/remove", Some(remove_body)).await?;
    if !(200..300).contains(&status) {
        return Err(format!("remove failed (HTTP {status}): {resp}"));
    }
    println!("node {node} removed; safe to stop the process");
    Ok(())
}

/// Issue a single HTTP/1.0 request to the admin endpoint and return its
/// `(status, body)`. A minimal hand-rolled client (the admin server is a
/// hand-rolled HTTP/1.1 edge); `Connection: close` makes a one-shot read to EOF.
async fn http_call(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<String>,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("cannot connect to {addr}: {e}"))?;
    let body = body.unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.0\r\n\
         Host: {addr}\r\n\
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
        .map_err(|e| format!("send failed: {e}"))?;
    stream.flush().await.ok();
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("recv failed: {e}"))?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or("malformed HTTP response from admin endpoint")?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or("malformed status line from admin endpoint")?;
    Ok((status, body.to_string()))
}

fn print_response(response: &ClientResponse) {
    match response {
        ClientResponse::Status { metadata: meta, .. } => {
            println!("members: {}", meta.members.len());
            for (id, member) in &meta.members {
                println!("  node {id}: {:?}", member.status);
            }
            println!("tablets: {}", meta.tablets.len());
            for (id, t) in &meta.tablets {
                let end = t
                    .range
                    .end
                    .as_ref()
                    .map_or_else(|| "∞".to_string(), |e| show(e));
                println!(
                    "  tablet {}: [{}, {}) epoch {} replicas {:?}",
                    id.0,
                    show(&t.range.start),
                    end,
                    t.epoch.0,
                    t.replicas
                );
            }
        }
        ClientResponse::PutOk => println!("OK"),
        ClientResponse::Value(Some(v)) => println!("{}", show(v)),
        ClientResponse::Value(None) => println!("(not found)"),
        ClientResponse::Pairs(pairs) => {
            for (k, v) in pairs {
                println!("{}\t{}", show(k), show(v));
            }
        }
        ClientResponse::Error(e) => println!("error: {e}"),
        // Join discovery (ADR 0032 PR2): consumed programmatically by
        // `animusd join`'s startup, not requested by any CLI subcommand —
        // printed raw if one ever surfaces here.
        ClientResponse::JoinInfo {
            control_ids,
            peers,
            client_route,
            admin_addrs,
        } => {
            println!("control ids: {control_ids:?}");
            println!("peers: {peers:?}");
            println!("client route: {client_route:?}");
            println!("admin addrs: {admin_addrs:?}");
        }
        // Cluster-allocated member id (ADR 0036): consumed programmatically
        // by `animusd join`/`data --seed`'s no-`--node` startup path, not
        // requested by any CLI subcommand of its own — printed raw if one
        // ever surfaces here (mirroring `JoinInfo` above).
        ClientResponse::NodeIdAllocated { node } => {
            println!("allocated node id: {node}");
        }
    }
}

/// Render bytes as UTF-8 if possible, else as a debug string.
fn show(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| format!("{bytes:?}"))
}
