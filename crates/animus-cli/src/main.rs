//! AnimusDB operator/client CLI (`animus`).
//!
//! Usage:
//!   animus status <node-addr>
//!   animus put    <node-addr> <table> <key> <value>
//!   animus get    <node-addr> <table> <key>
//!   animus get-eventual <node-addr> <table> <key>
//!   animus admin  <subcommand> <admin-addr> [args...]
//!
//! `get` is linearizable (ReadIndex on the tablet's leader); `get-eventual` is
//! the ADR 0055 eventually-consistent read — DynamoDB's `ConsistentRead:
//! false` — served from any replica's applied state, which is what makes it
//! the hand-driven way to observe replica lag on a live cluster.
//!
//! `status`/`put`/`get` are a thin plain-TCP client over a node's request/reply
//! API. `admin` talks the HTTP/JSON admin interface (ADR 0020) on a node's
//! **admin** address (as printed by `animusd`), printing the JSON response. See
//! [`ADMIN_USAGE`] for the subcommands.

// ADR 0003 / ADR 0061 Decision 4 (rung B5): this binary is a real network
// client talking to a live, already-running cluster over actual sockets — it
// is never `E: Env`-generic and has no simulated counterpart to keep in sync
// with, so its polling loops' `Instant::now()`/`sleep()` deadlines are the
// correct tool, not a determinism hole. One file-level allow rather than
// repeating the same reason at each of this file's poll-loop call sites.
#![allow(
    clippy::disallowed_methods,
    reason = "animus-cli is a real-socket client CLI outside the Env seam, not system logic (ADR 0003); see ADR 0061 Decision 4"
)]

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
                "\nusage:\n  animus status <node-addr>\n  animus put <node-addr> <table> <key> <value>\n  animus get <node-addr> <table> <key>\n  animus get-eventual <node-addr> <table> <key>\n{ADMIN_USAGE}"
            );
            ExitCode::FAILURE
        }
    }
}

const ADMIN_USAGE: &str = "  admin <subcommand> <admin-addr> [args]:\n    \
    config|status|raft|raftkv|metrics|health <admin-addr>\n    \
    peers|txns|backups|restores|control-members|storage-control <admin-addr>\n    \
    lsm|wal <admin-addr> [tablet]\n    \
    wal-segment <admin-addr> <seg> [tablet]\n    \
    key <admin-addr> <key> [tablet]\n    \
    storage-scan <admin-addr> [--tablet <id>] [--start <key>] [--limit <n>]\n    \
    system-table <admin-addr> [--kind <kind>] [--limit <n>] [--after <cursor>]\n    \
    split <admin-addr> <tablet> <split-key>\n    \
    stream-grow <admin-addr> <table>\n    \
    flush|compact <admin-addr> <tablet>\n    \
    reconfigure <admin-addr> <tablet> <voter,voter,...>\n    \
    drain <admin-addr> <node-id>\n    \
    drain-status <admin-addr> <node-id>\n    \
    remove <admin-addr> <node-id>\n    \
    decommission <admin-addr> <node-id> [--force-control-remove]\n    \
    control-add <leader-admin-addr> <new-node-admin-addr>                    (self-minted id)\n    \
    control-add <leader-admin-addr> <node-id> <new-node-admin-addr>         (operator-supplied id)\n    \
    control-remove <leader-admin-addr> <node-id> [--force]\n    \
    control-grow <leader-admin-addr> <node-id> <admin-addr> [<node-id> <admin-addr>...]";

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
        // `get <addr> <table> <key>` is linearizable; `get-eventual` is the
        // ADR 0055 cheap read (`ConsistentRead: false`) — any replica's own
        // applied state, no ReadIndex barrier, no leader hop. Same shape
        // otherwise, so they share one arm.
        cmd @ ("get" | "get-eventual") => {
            let table = args.get(2).ok_or("get needs <table>")?;
            let key = args.get(3).ok_or("get needs <key>")?;
            ClientRequest::Get {
                key: key.clone().into_bytes(),
                table: table.clone(),
                stale: cmd == "get-eventual",
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

    // `decommission` is a multi-step composite (drain → poll drain-status →
    // remove), not a single request/response — handled separately from the
    // generic one-shot dispatch below (ADR 0032 PR3).
    if sub == "decommission" {
        let node = arg(2).ok_or("decommission needs <node-id>")?;
        let force_control_remove = arg(3) == Some("--force-control-remove");
        return run_decommission(addr, node, force_control_remove).await;
    }

    // `control-add`/`control-remove`/`control-grow` (ADR 0037 PR3) are the
    // control-plane-membership counterparts of `decommission`: multi-step
    // orchestration (an address lookup + a catch-up poll, or a sequential
    // one-at-a-time loop), not a single request/response, so they too are
    // handled before the generic one-shot dispatch below.
    //
    // `control-add` disambiguates its two forms by **arity** (ADR 0037
    // hardening trio's PR3, locked decision — no `--auto` flag): exactly one
    // trailing arg is the self-minted-id form (`<new-node-admin-addr>`
    // only); exactly two is the operator-supplied-id form (`<node-id>
    // <new-node-admin-addr>`), unchanged from before this PR.
    if sub == "control-add" {
        let rest = &args[2..];
        return match rest.len() {
            1 => run_control_add_allocated(addr, &rest[0]).await,
            2 => run_control_add(addr, &rest[0], &rest[1]).await,
            _ => Err(
                "control-add needs <new-node-admin-addr> (self-minted id) or \
                 <node-id> <new-node-admin-addr> (operator-supplied id)"
                    .into(),
            ),
        };
    }
    if sub == "control-remove" {
        let node = arg(2).ok_or("control-remove needs <node-id>")?;
        let force = arg(3) == Some("--force");
        return run_control_remove(addr, node, force).await;
    }
    if sub == "control-grow" {
        let pairs = &args[2..];
        if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
            return Err(
                "control-grow needs one or more <node-id> <new-node-admin-addr> pairs".into(),
            );
        }
        return run_control_grow(addr, pairs).await;
    }

    let (method, path, body) = admin_request(sub, args)?;

    let (status, response) = http_call(addr, method, &path, body).await?;
    println!("{response}");
    if !(200..300).contains(&status) {
        return Err(format!("admin request failed (HTTP {status})"));
    }
    Ok(())
}

/// Build the `(method, path, body)` for the flat one-shot admin routes —
/// pulled out of [`run_admin`] as a pure function (no socket I/O) so the
/// argument parsing is unit-testable. `args` is the full admin-subcommand
/// argument list (`args[0]` the subcommand, `args[1]` the admin address, as
/// in [`run_admin`]) — `decommission`/`control-add`/`control-remove`/
/// `control-grow` are multi-step orchestration handled by [`run_admin`]
/// itself before this is ever called, so they never reach here.
fn admin_request(
    sub: &str,
    args: &[String],
) -> Result<(&'static str, String, Option<String>), String> {
    let arg = |i: usize| args.get(i).map(String::as_str);
    // The optional trailing `tablet` for the storage GET routes.
    let tablet_q = |i: usize| arg(i).map_or(String::new(), |t| format!("?tablet={t}"));

    Ok(match sub {
        "config" => ("GET", "/admin/config".into(), None),
        "status" => ("GET", "/admin/status".into(), None),
        "raft" => ("GET", "/admin/raft".into(), None),
        "raftkv" => ("GET", "/admin/raftkv".into(), None),
        "metrics" => ("GET", "/admin/metrics".into(), None),
        "health" => ("GET", "/admin/health".into(), None),
        "peers" => ("GET", "/admin/peers".into(), None),
        "txns" => ("GET", "/admin/txns".into(), None),
        "backups" => ("GET", "/admin/backups".into(), None),
        "restores" => ("GET", "/admin/restores".into(), None),
        "control-members" => ("GET", "/admin/control/members".into(), None),
        "storage-control" => ("GET", "/admin/storage/control".into(), None),
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
        // All three params are optional server-side (`tablet` defaults to 1,
        // `start` to the beginning of the tablet, `limit` to 50) — `--flag`
        // form rather than positional, since there is no single mandatory
        // leading arg to anchor trailing positionals on the way the
        // `lsm`/`wal`/`key` routes' single optional `[tablet]` does.
        "storage-scan" => {
            let mut q = Vec::new();
            if let Some(v) = flag_value(args, "--tablet") {
                q.push(format!("tablet={v}"));
            }
            if let Some(v) = flag_value(args, "--start") {
                q.push(format!("start={v}"));
            }
            if let Some(v) = flag_value(args, "--limit") {
                q.push(format!("limit={v}"));
            }
            (
                "GET",
                format!("/admin/storage/scan{}", join_query(&q)),
                None,
            )
        }
        // `kind`/`limit`/`after` are all optional server-side too (ADR 0038
        // addendum's `system_table` handler) — same `--flag` shape as
        // `storage-scan` above.
        "system-table" => {
            let mut q = Vec::new();
            if let Some(v) = flag_value(args, "--kind") {
                q.push(format!("kind={v}"));
            }
            if let Some(v) = flag_value(args, "--limit") {
                q.push(format!("limit={v}"));
            }
            if let Some(v) = flag_value(args, "--after") {
                q.push(format!("after={v}"));
            }
            (
                "GET",
                format!("/admin/system-table{}", join_query(&q)),
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
        "stream-grow" => {
            let table = arg(2).ok_or("stream-grow needs <table>")?;
            let body = serde_json::json!({"table": table}).to_string();
            ("POST", "/admin/stream/grow".into(), Some(body))
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
            let voters: Vec<&str> = arg(3)
                .ok_or("reconfigure needs <voter,voter,...>")?
                .split(',')
                .map(str::trim)
                .collect();
            let body = serde_json::json!({"tablet": tablet, "voters": voters}).to_string();
            ("POST", "/admin/raftkv/reconfigure".into(), Some(body))
        }
        "drain" => {
            let node = arg(2).ok_or("drain needs <node-id>")?;
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
            let node = arg(2).ok_or("remove needs <node-id>")?;
            let body = serde_json::json!({"node": node}).to_string();
            ("POST", "/admin/member/remove".into(), Some(body))
        }
        other => return Err(format!("unknown admin subcommand `{other}`")),
    })
}

/// Look up a `--name value` pair anywhere in `args` (order-independent,
/// unlike the positional `arg`/`tablet_q` closures above — `storage-scan`
/// and `system-table` have several independent optional params with no
/// natural positional order to anchor on). Returns the value verbatim
/// (un-percent-encoded, matching every other query value this file already
/// builds by hand — e.g. `key`'s `key={key}` — since the admin server's own
/// `query_param` percent-decodes on the way in).
fn flag_value<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

/// Join `k=v` pieces into a `?`-prefixed query string, or `""` if there are none.
fn join_query(pieces: &[String]) -> String {
    if pieces.is_empty() {
        String::new()
    } else {
        format!("?{}", pieces.join("&"))
    }
}

/// The operator's whole decommission flow (ADR 0032 PR3, extended by ADR 0037
/// PR4 for a combined node that is also a **live** control-plane voter), as a
/// single command: an optional control-voter pre-check/two-phase removal (see
/// below), then `POST /admin/drain` → poll `GET /admin/member/drain-status`
/// until draining has actually converged (no tablet still references the
/// member, and it isn't mid-service) → `POST /admin/member/remove`. Every
/// request goes to `addr`, which must be the **control-plane leader's** admin
/// port — `/admin/drain`, `/admin/member/remove`, and both
/// `/admin/control/member/*` actions are deliberately local-leader-only, not
/// relayed (see `is_relayable_command`'s doc in `animusd`), so this fails
/// loudly with the same "not the control-plane leader" error a bare
/// `drain`/`remove`/`control-remove` call would if pointed at a follower.
///
/// **Combined-node-is-a-control-voter flow (ADR 0037 PR4, plan §7/§8):**
/// `animusd`'s own `admin_remove_member` refuses the final `/admin/member/
/// remove` step outright while `node` itself (ADR 0040 PR1: one identity per
/// node — there is no more separate control id to derive) is a *current,
/// live* control-plane voter (`ClientCtx::admin_remove_member`'s doc) — that
/// server-side check is the actual authority. This flow adds a **friendlier,
/// fail-fast** CLI-side pre-check so an operator doesn't drain a node for two
/// minutes only to have the final step refused: it asks `GET
/// /admin/control/members` up front and, if `node` is listed as a live
/// voter:
/// - without `force_control_remove`: refuses immediately with a clear
///   message naming the two-phase path, before ever touching `/admin/drain`;
/// - with `force_control_remove`: runs the control-plane-membership removal
///   first (`run_control_remove`, which itself arms a leadership transfer if
///   `node` happens to be the control leader — see that function's doc),
///   polls `/admin/control/members` until the live voter set no longer lists
///   it (bounded, since a transfer can take a few election-timeout rounds
///   under real scheduling), and only then falls through to the *unchanged*
///   drain → drain-status → remove flow below.
///
/// If `/admin/control/members` itself is unreachable (e.g. an old `animusd`
/// binary predating ADR 0037 — the endpoint didn't exist), this pre-check is
/// skipped entirely and the flow proceeds exactly as it did before this PR:
/// the server-side `admin_remove_member` refusal (if `node` really is a live
/// control voter) still surfaces at the final `remove` step, just later and
/// after an unnecessary drain — a graceful degrade, not a silent skip of the
/// real safety check.
async fn run_decommission(
    addr: &str,
    node: &str,
    force_control_remove: bool,
) -> Result<(), String> {
    // Unreachable / non-200 (e.g. an old binary with no such route, or a
    // follower's admin port before the caller even knows who leads):
    // skip the pre-check and let the ordinary flow's own final `remove`
    // step surface the authoritative refusal, if any.
    if let Ok((200, resp)) = http_call(addr, "GET", "/admin/control/members", None).await {
        let is_live_voter = serde_json::from_str::<serde_json::Value>(&resp)
            .ok()
            .and_then(|v| v.get("voters").cloned())
            .and_then(|v| v.as_array().cloned())
            .is_some_and(|voters| voters.iter().any(|x| x.as_str() == Some(node)));
        if is_live_voter {
            if !force_control_remove {
                return Err(format!(
                    "node {node} is a current control-plane voter; \
                     decommissioning it requires removing it from the control \
                     group first. Retry with `--force-control-remove`, or run \
                     `animus admin control-remove {addr} {node}` yourself first"
                ));
            }
            println!("node {node} is a control voter; removing it from the control group first...");
            // `--force-control-remove` does NOT imply `--force`: these are
            // separate, independently-explicit escape hatches (see
            // `run_control_remove`'s doc). If the removal itself is refused
            // by the liveness guard, the operator must retry with `animus
            // admin control-remove <addr> <node> --force` explicitly.
            run_control_remove(addr, node, false).await?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let (status, resp) = http_call(addr, "GET", "/admin/control/members", None).await?;
                if status != 200 {
                    return Err(format!(
                        "control/members failed while polling for {node}'s \
                         removal (HTTP {status}): {resp}"
                    ));
                }
                let still_voter = serde_json::from_str::<serde_json::Value>(&resp)
                    .ok()
                    .and_then(|v| v.get("voters").cloned())
                    .and_then(|v| v.as_array().cloned())
                    .is_some_and(|voters| voters.iter().any(|x| x.as_str() == Some(node)));
                if !still_voter {
                    println!(
                        "node {node} is no longer a control voter; \
                         proceeding with decommission..."
                    );
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!(
                        "node {node} was still a live control voter 30s \
                         after control-remove; retry"
                    ));
                }
                sleep(Duration::from_millis(200)).await;
            }
        }
    }

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

/// `animus admin control-add <leader-admin-addr> <node-id> <new-node-admin-addr>`
/// (ADR 0037 PR3, the **operator-supplied-id** form — see [`run_admin`]'s
/// arity dispatch and [`run_control_add_allocated`] for the allocator-minted
/// sibling): grow the control group by one voter. This CLI speaks in
/// **admin** addresses everywhere else, so `<new-node-admin-addr>` is that —
/// not the internal control-Raft address `POST /admin/control/member/add`'s
/// wire payload actually wants. This resolves the difference itself: a `GET
/// /admin/config` against the new node's own admin port doubles as the
/// "confirm it's up" liveness check the runbook wants and yields its
/// `control` address, which then goes into the add request to the **leader**.
/// Finally polls the **new node's own** `/admin/control/members` until it
/// reports itself a voter — mirroring `run_decommission`'s
/// poll-to-convergence shape (bounded, no fixed sleep-and-hope).
async fn run_control_add(
    leader_admin_addr: &str,
    node: &str,
    new_node_admin_addr: &str,
) -> Result<(), String> {
    let (status, resp) = http_call(new_node_admin_addr, "GET", "/admin/config", None).await?;
    if !(200..300).contains(&status) {
        return Err(format!(
            "could not reach the new node's admin port {new_node_admin_addr} \
             (HTTP {status}): {resp}"
        ));
    }
    let cfg: serde_json::Value = serde_json::from_str(&resp)
        .map_err(|e| format!("malformed /admin/config response: {e}"))?;
    let control_addr = cfg["control"].as_str().ok_or(
        "the new node's /admin/config has no `control` address \
         (is it a control-role or combined-mode node?)",
    )?;

    let body = serde_json::json!({"node": node, "addr": control_addr}).to_string();
    let (status, resp) = http_call(
        leader_admin_addr,
        "POST",
        "/admin/control/member/add",
        Some(body),
    )
    .await?;
    if !(200..300).contains(&status) {
        return Err(format!("control/member/add failed (HTTP {status}): {resp}"));
    }
    println!("added control voter {node} ({control_addr}); waiting for it to catch up...");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, resp) = http_call(new_node_admin_addr, "GET", "/admin/control/members", None)
            .await
            .unwrap_or((0, String::new()));
        if status == 200
            && let Ok(v) = serde_json::from_str::<serde_json::Value>(&resp)
            && v["voters"]
                .as_array()
                .is_some_and(|vs| vs.iter().any(|x| x.as_str() == Some(node)))
        {
            println!("node {node} is now a control voter");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "node {node} did not report itself a control voter within 60s"
            ));
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// `animus admin control-add <leader-admin-addr> <new-node-control-addr>`
/// (ADR 0037 hardening trio's PR3, the **self-minted-id** form since ADR 0040
/// PR4 — 2 args, disambiguated by arity in [`run_admin`]'s dispatch, locked
/// decision: no `--auto` flag). Unlike [`run_control_add`]'s operator-
/// supplied form, there is no id yet to look a running node up by, so this
/// skips the `GET /admin/config` liveness/address-resolution step entirely:
/// `addr` goes straight into the request as the new voter's internal
/// control-Raft listen address, and the control plane self-mints a fresh id
/// (`NodeId::mint`, `POST /admin/control/member/add` with `node` omitted),
/// then registers `addr` for it and adds it as a voter — same one-call
/// semantics as the operator-supplied form, just with the id decided
/// server-side. Prints the minted id and returns — there is no known admin
/// port to poll for catch-up convergence (the physical process at `addr` may
/// not even be running yet by design: the operator's next step is to start
/// it there with `--id <minted-id>` — e.g. `animusd join --seed <any-node>
/// --id <minted-id> --base-port <port>` — at which point it starts
/// replicating like any other freshly-added voter).
async fn run_control_add_allocated(
    leader_admin_addr: &str,
    new_node_control_addr: &str,
) -> Result<(), String> {
    let body = serde_json::json!({"addr": new_node_control_addr}).to_string();
    let (status, resp) = http_call(
        leader_admin_addr,
        "POST",
        "/admin/control/member/add",
        Some(body),
    )
    .await?;
    if !(200..300).contains(&status) {
        return Err(format!("control/member/add failed (HTTP {status}): {resp}"));
    }
    let v: serde_json::Value = serde_json::from_str(&resp)
        .map_err(|e| format!("malformed control/member/add response: {e}"))?;
    let node = v["node"]
        .as_str()
        .ok_or("control/member/add response missing `node`")?;
    println!(
        "minted control voter id {node} for {new_node_control_addr}; \
         start the new node's process there with --id {node} to complete the join"
    );
    Ok(())
}

/// `animus admin control-remove <leader-admin-addr> <node-id> [--force]`
/// (ADR 0037 PR3, `--force` added by the hardening-trio's quorum-guard
/// liveness fix): a thin wrap over `POST /admin/control/member/remove`,
/// printing the server's `warning` field (ADR 0037 §2's deliberately-
/// allowed-but-risky quorum-loss cases) to stderr rather than swallowing it —
/// mirroring `remove`'s existing print-then-check-status shape. `--force`
/// bypasses the server's liveness-aware quorum-loss guard (refuse if fewer
/// than a majority of the *resulting* voters are reachable) — it is **not**
/// implied by `decommission --force-control-remove`, a deliberately separate
/// flag: that one only says "run control-remove as part of decommission,"
/// never "and skip control-remove's own safety checks" (see
/// `run_decommission`'s doc and `animusd::ClientCtx::admin_remove_control_member`).
async fn run_control_remove(
    leader_admin_addr: &str,
    node: &str,
    force: bool,
) -> Result<(), String> {
    let body = serde_json::json!({"node": node, "force": force}).to_string();
    let (status, resp) = http_call(
        leader_admin_addr,
        "POST",
        "/admin/control/member/remove",
        Some(body),
    )
    .await?;
    println!("{resp}");
    if !(200..300).contains(&status) {
        return Err(format!("control/member/remove failed (HTTP {status})"));
    }
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&resp)
        && let Some(w) = v["warning"].as_str()
    {
        eprintln!("warning: {w}");
    }
    Ok(())
}

/// `animus admin control-grow <leader-admin-addr> <node-id> <new-node-admin-addr>
/// [<node-id> <new-node-admin-addr>...]` (ADR 0037 PR3): the "3→5" composite —
/// `RaftCore::change_membership` is single-server-at-a-time (ADR 0017 C), so
/// growing by more than one voter is a **sequential** loop of
/// [`run_control_add`] calls, each waiting for its own catch-up before the
/// next is even proposed (a second concurrent change would be rejected as
/// "already in flight" anyway). `pairs` is `args[2..]`, already validated
/// non-empty and even-length by the caller.
async fn run_control_grow(leader_admin_addr: &str, pairs: &[String]) -> Result<(), String> {
    for chunk in pairs.chunks(2) {
        let node = &chunk[0];
        let new_node_admin_addr = &chunk[1];
        run_control_add(leader_admin_addr, node, new_node_admin_addr).await?;
    }
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
        // Reply to the internal `GetSnapshot` RPC (ADR 0018 §2, torn-pair-fix
        // stack PR2) — never requested by any CLI subcommand (`get` only
        // ever sends a bare `Get`); printed raw if one ever surfaces here,
        // mirroring `JoinInfo`/`MetadataDelta` below.
        ClientResponse::Unresolved => println!("(unresolved: transaction in flight)"),
        ClientResponse::Pairs(pairs) => {
            for (k, v) in pairs {
                println!("{}\t{}", show(k), show(v));
            }
        }
        ClientResponse::Error(e) => println!("error: {e}"),
        // Internal evaluate-at-leader write RPC replies (ADR 0046 U3):
        // consumed programmatically by `ClientCtx::cp_kind_write_item`'s own
        // caller (`dynamo.rs`'s `PutItem`/`DeleteItem`/`UpdateItem`/
        // `BatchWriteItem` handlers) and by tests driving the client
        // protocol directly — not requested by any CLI subcommand of its
        // own, mirroring `JoinInfo`/`MetadataDelta` above.
        ClientResponse::KindWriteOk {
            old,
            new,
            collection_bytes,
        } => {
            println!("kind write ok: old={old:?} new={new:?}");
            // The item-collection size bound the leader priced (ADR 0006's
            // `ItemCollectionMetrics`), when the reply carried one.
            if let Some(bytes) = collection_bytes {
                println!("collection bytes (upper bound): {bytes}");
            }
        }
        ClientResponse::ConditionFailed => println!("condition failed"),
        // Internal TxnResolve RPC reply (ADR 0018 §3/§6, torn-pair-fix
        // stack PR2): consumed programmatically by `txn_resolve_participant_retrying`,
        // not requested by any CLI subcommand of its own — printed raw if
        // one ever surfaces here, mirroring `KindWriteOk` above.
        ClientResponse::TxnResolved { outcome } => println!("txn resolved: {outcome:?}"),
        // Join discovery (ADR 0032 PR2): consumed programmatically by
        // `animusd join`'s startup, not requested by any CLI subcommand —
        // printed raw if one ever surfaces here.
        ClientResponse::JoinInfo {
            control_ids,
            peers,
            client_route,
            intra_route,
            admin_addrs,
        } => {
            println!("control ids: {control_ids:?}");
            println!("peers: {peers:?}");
            println!("client route: {client_route:?}");
            println!("intra route: {intra_route:?}");
            println!("admin addrs: {admin_addrs:?}");
        }
        // Incremental `WatchMetadata` reply (ADR 0038 PR5): consumed
        // programmatically by `RemoteControlClient`'s mirror sync, not
        // requested by any CLI subcommand of its own — printed raw if one
        // ever surfaces here (mirroring `JoinInfo` above).
        ClientResponse::MetadataDelta {
            writes, watermark, ..
        } => {
            println!(
                "metadata delta: {} write(s) up to watermark {watermark}",
                writes.len()
            );
        }
        // Multi-participant transaction replies (ADR 0018 §2/PR4): consumed
        // programmatically by `ClientCtx::cp_txn`'s own coordinator logic
        // and by tests driving the client protocol directly — not
        // requested by any CLI subcommand of its own yet (that's tracked
        // for a later PR, alongside the Dynamo `TransactWriteItems`
        // surface). Printed raw if one ever surfaces here, mirroring
        // `JoinInfo`/`MetadataDelta` above.
        ClientResponse::TxnCommitted { commit_ts } => {
            println!("txn committed at {commit_ts:?}");
        }
        ClientResponse::TxnPrepared { txn_id, ts, .. } => {
            println!("txn {txn_id:?} prepared at {ts:?}");
        }
        ClientResponse::TxnDecided { outcome } => {
            println!("txn decided: {outcome:?}");
        }
        ClientResponse::TxnStatusReply { status } => {
            println!("txn status: {status:?}");
        }
        // Internal recovery RPCs (ADR 0018 §2/PR5) — never requested by any
        // CLI subcommand; printed raw if one ever surfaces here.
        ClientResponse::TxnRecordViewReply { view } => {
            println!("txn record view: {view:?}");
        }
        ClientResponse::TxnVerifyReply { staged } => {
            println!("txn verify: staged={staged}");
        }
    }
}

/// Render bytes as UTF-8 if possible, else as a debug string.
fn show(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| format!("{bytes:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `admin_request`'s `args` mirrors `run_admin`'s: `[0]` is the
    /// subcommand, `[1]` the admin address — both unused by `admin_request`
    /// itself (routing happens in `run_admin`; `arg(2)`/`flag_value` never
    /// look at index 0 or 1) but kept as placeholders so `arg(2)` lines up
    /// with `rest[0]`, matching real call sites.
    fn args(rest: &[&str]) -> Vec<String> {
        ["sub", "addr"]
            .into_iter()
            .chain(rest.iter().copied())
            .map(String::from)
            .collect()
    }

    #[test]
    fn flat_gets_with_no_params_route_to_their_fixed_path() {
        let cases = [
            ("peers", "/admin/peers"),
            ("txns", "/admin/txns"),
            ("backups", "/admin/backups"),
            ("restores", "/admin/restores"),
            ("control-members", "/admin/control/members"),
            ("storage-control", "/admin/storage/control"),
        ];
        for (sub, expected_path) in cases {
            let (method, path, body) = admin_request(sub, &args(&[])).unwrap();
            assert_eq!(method, "GET", "sub={sub}");
            assert_eq!(path, expected_path, "sub={sub}");
            assert_eq!(body, None, "sub={sub}");
        }
    }

    #[test]
    fn storage_scan_with_no_flags_has_no_query_string() {
        let (method, path, body) = admin_request("storage-scan", &args(&[])).unwrap();
        assert_eq!(method, "GET");
        assert_eq!(path, "/admin/storage/scan");
        assert_eq!(body, None);
    }

    #[test]
    fn storage_scan_passes_through_its_flags_regardless_of_order() {
        let (_, path, _) = admin_request(
            "storage-scan",
            &args(&["--limit", "10", "--tablet", "3", "--start", "abc"]),
        )
        .unwrap();
        assert_eq!(path, "/admin/storage/scan?tablet=3&start=abc&limit=10");
    }

    #[test]
    fn storage_scan_supports_a_single_flag_alone() {
        let (_, path, _) = admin_request("storage-scan", &args(&["--start", "k1"])).unwrap();
        assert_eq!(path, "/admin/storage/scan?start=k1");
    }

    #[test]
    fn system_table_with_no_flags_has_no_query_string() {
        let (method, path, body) = admin_request("system-table", &args(&[])).unwrap();
        assert_eq!(method, "GET");
        assert_eq!(path, "/admin/system-table");
        assert_eq!(body, None);
    }

    #[test]
    fn system_table_passes_through_kind_limit_after() {
        let (_, path, _) = admin_request(
            "system-table",
            &args(&["--kind", "Tablet", "--limit", "25", "--after", "cursor1"]),
        )
        .unwrap();
        assert_eq!(
            path,
            "/admin/system-table?kind=Tablet&limit=25&after=cursor1"
        );
    }

    #[test]
    fn system_table_supports_kind_alone() {
        let (_, path, _) = admin_request("system-table", &args(&["--kind", "Policy"])).unwrap();
        assert_eq!(path, "/admin/system-table?kind=Policy");
    }

    #[test]
    fn preexisting_arms_are_unchanged_by_the_refactor() {
        let (method, path, body) = admin_request("lsm", &args(&[])).unwrap();
        assert_eq!(method, "GET");
        assert_eq!(path, "/admin/storage/lsm");
        assert_eq!(body, None);

        let (_, path, _) = admin_request("lsm", &args(&["7"])).unwrap();
        assert_eq!(path, "/admin/storage/lsm?tablet=7");

        let (_, path, _) = admin_request("key", &args(&["mykey"])).unwrap();
        assert_eq!(path, "/admin/storage/key?key=mykey&tablet=1");

        let (method, path, body) = admin_request("split", &args(&["5", "somekey"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/tablet/split");
        assert_eq!(
            body,
            Some(r#"{"split_key":"somekey","tablet":5}"#.to_string())
        );
    }

    #[test]
    fn unknown_subcommand_is_an_error() {
        assert!(admin_request("no-such-thing", &args(&[])).is_err());
    }

    #[test]
    fn flag_value_finds_the_value_following_its_name_anywhere_in_args() {
        let a = args(&["--a", "1", "--b", "2"]);
        assert_eq!(flag_value(&a, "--b"), Some("2"));
        assert_eq!(flag_value(&a, "--missing"), None);
    }
}
