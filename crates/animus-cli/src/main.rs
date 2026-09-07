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
use std::sync::Arc;
use std::time::Duration;

use animus_env::MaybeTlsStream;
use animusd::{ClientRequest, ClientResponse, read_frame, write_frame};
use rustls_pki_types::pem::PemObject;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> ExitCode {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    // `--tls-ca PATH` (ADR 0064, S-01 commit 2) may appear anywhere in the
    // argument list — extracted up front, before any subcommand's own
    // positional parsing runs, so it never collides with a subcommand's own
    // argument shape. Server-only TLS (this CLI never presents a client
    // certificate): it verifies the node it talks to, on both the
    // client-protocol and admin ports.
    let tls = match extract_tls_ca(&mut args)
        .and_then(|ca| ca.as_deref().map(build_tls_connector).transpose())
    {
        Ok(tls) => tls,
        Err(msg) => {
            eprintln!("animus: {msg}");
            return ExitCode::FAILURE;
        }
    };
    match run(&args, tls.as_ref()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("animus: {msg}");
            eprintln!(
                "\nusage:\n  animus [--tls-ca PATH] status <node-addr>\n  animus [--tls-ca PATH] put <node-addr> <table> <key> <value>\n  animus [--tls-ca PATH] get <node-addr> <table> <key>\n  animus [--tls-ca PATH] get-eventual <node-addr> <table> <key>\n{ADMIN_USAGE}"
            );
            ExitCode::FAILURE
        }
    }
}

/// Pull `--tls-ca PATH` out of `args` (in place), wherever it appears —
/// returns its value, or `None` if the flag was never given.
///
/// # Errors
/// A message if `--tls-ca` is given with no following value.
fn extract_tls_ca(args: &mut Vec<String>) -> Result<Option<String>, String> {
    let Some(pos) = args.iter().position(|a| a == "--tls-ca") else {
        return Ok(None);
    };
    let value = args
        .get(pos + 1)
        .cloned()
        .ok_or("--tls-ca requires a PATH argument")?;
    args.remove(pos + 1);
    args.remove(pos);
    Ok(Some(value))
}

/// Build a **server-only** TLS client config trusting `ca_path` (ADR 0064,
/// S-01 commit 2) — this CLI verifies the node it dials but never presents
/// a client certificate of its own (it is not a cluster member; mutual TLS
/// is only for the internal/intra ports). Independent of `animus_env::
/// TlsConfig::load` (which always builds a *mutual* `ClientConfig`) for
/// exactly that reason.
///
/// # Errors
/// A message if the file cannot be read, contains no certificate, or
/// `rustls` rejects the resulting root store.
fn build_tls_connector(ca_path: &str) -> Result<tokio_rustls::TlsConnector, String> {
    let bytes = std::fs::read(ca_path).map_err(|e| format!("reading --tls-ca {ca_path}: {e}"))?;
    let certs = rustls_pki_types::CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parsing --tls-ca {ca_path}: {e}"))?;
    let mut root_store = rustls::RootCertStore::empty();
    for cert in certs {
        root_store
            .add(cert)
            .map_err(|e| format!("--tls-ca {ca_path}: {e}"))?;
    }
    if root_store.is_empty() {
        return Err(format!("no certificates found in --tls-ca {ca_path}"));
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("building TLS client config: {e}"))?
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

/// Dial `addr`, optionally through `tls` (ADR 0064, S-01 commit 2) — `None`
/// is a plain `TcpStream` (byte-for-byte unchanged); `Some` runs a
/// server-only TLS handshake first, deriving the `ServerName` to verify the
/// peer against from exactly the address string dialed
/// (`animus_env::tls::server_name_for`) — the node's certificate SAN must
/// cover whatever string `addr` names it by.
async fn maybe_tls_connect(
    addr: &str,
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<MaybeTlsStream, String> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("cannot connect to {addr}: {e}"))?;
    match tls {
        None => Ok(MaybeTlsStream::Plain(stream)),
        Some(connector) => {
            let server_name = animus_env::tls::server_name_for(addr)
                .map_err(|e| format!("invalid TLS server name for {addr}: {e}"))?;
            let tls_stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|e| format!("TLS handshake with {addr} failed: {e}"))?;
            Ok(MaybeTlsStream::Tls(Box::new(tls_stream.into())))
        }
    }
}

const ADMIN_USAGE: &str = "  admin <subcommand> <admin-addr> [args]:\n    \
    config|status|raft|raftkv|metrics|health <admin-addr>\n    \
    peers|txns|backups|restores|backup-store|ttl-reaper|gc|segment-store|control-members|storage-control <admin-addr>\n    \
    lsm|wal <admin-addr> [tablet]\n    \
    wal-segment <admin-addr> <seg> [tablet]\n    \
    key <admin-addr> <key> [tablet]\n    \
    storage-scan <admin-addr> [--tablet <id>] [--start <key>] [--limit <n>]\n    \
    system-table <admin-addr> [--kind <kind>] [--limit <n>] [--after <cursor>]\n    \
    credentials <admin-addr>\n    \
    credentials-put <admin-addr> <id> <secret> [--enabled true|false] \
    [--policy-tables all|name1,name2|prefix:pfx1,pfx2] [--policy-ops read,write,ddl,streams,backup]\n    \
    credentials-rotate <admin-addr> <id> <new-secret> <grace-secs>\n    \
    credentials-revoke <admin-addr> <id>\n    \
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
    control-grow <leader-admin-addr> <node-id> <admin-addr> [<node-id> <admin-addr>...]\n    \
    control-transfer <leader-admin-addr> <node-id>\n    \
    backup-create <admin-addr> <table> <backup-name>\n    \
    backup-delete <admin-addr> <backup-arn>\n    \
    restore <admin-addr> <backup-arn> <target-table>\n    \
    pitr-enable|pitr-disable <admin-addr> <table>\n    \
    ttl <admin-addr> <table> <attribute> [--disable]\n    \
    stream <admin-addr> <table> <NEW_IMAGE|OLD_IMAGE|NEW_AND_OLD_IMAGES|KEYS_ONLY|off>\n    \
    export-create <admin-addr> <table-arn> <s3-bucket> [s3-prefix]\n    \
    export-describe <admin-addr> <export-arn>\n    \
    export-list <admin-addr> [table-arn]\n    \
    import-create <admin-addr> <table> <s3-bucket> [s3-prefix] [--gzip|--none] \
    --pk name:TYPE [--sk name:TYPE]\n    \
    import-describe <admin-addr> <import-arn>\n    \
    import-list <admin-addr> [table-arn]";

async fn run(args: &[String], tls: Option<&tokio_rustls::TlsConnector>) -> Result<(), String> {
    let cmd = args.first().map(String::as_str).ok_or("missing command")?;
    if cmd == "admin" {
        return run_admin(&args[1..], tls).await;
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

    let mut stream = maybe_tls_connect(addr, tls).await?;
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
async fn run_admin(
    args: &[String],
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<(), String> {
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
        return run_decommission(addr, node, force_control_remove, tls).await;
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
            1 => run_control_add_allocated(addr, &rest[0], tls).await,
            2 => run_control_add(addr, &rest[0], &rest[1], tls).await,
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
        return run_control_remove(addr, node, force, tls).await;
    }
    if sub == "control-grow" {
        let pairs = &args[2..];
        if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
            return Err(
                "control-grow needs one or more <node-id> <new-node-admin-addr> pairs".into(),
            );
        }
        return run_control_grow(addr, pairs, tls).await;
    }

    let (method, path, body) = admin_request(sub, args)?;

    let (status, response) = http_call(addr, method, &path, body, tls).await?;
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
        // `GET /admin/backup-store` (ADR 0059 §1/§3, roadmap U-07): store
        // config, object counts, and the backup janitor's own live phase.
        "backup-store" => ("GET", "/admin/backup-store".into(), None),
        // `GET /admin/ttl` (ADR 0051, roadmap U-07): the TTL reaper's own
        // live phase/cursor/counters plus every TTL-enabled table. Named
        // `ttl-reaper`, not the bare `ttl` its route would suggest —
        // roadmap U-08(ii) plans a `ttl` *dynamo-proxy* wrapper
        // (`UpdateTimeToLive`/`DescribeTimeToLive` via `/admin/data/
        // dynamo`) in this same subcommand namespace, and this GET arm
        // must not claim that name first.
        "ttl-reaper" => ("GET", "/admin/ttl".into(), None),
        // `GET /admin/gc` (ADR 0042 §10/ADR 0043 §A9, roadmap U-07): the
        // DynamoDB Streams segment janitor's own live orphan-sweep phase
        // and counters (control-plane-leader-only, exactly like
        // `backup-store` above).
        "gc" => ("GET", "/admin/gc".into(), None),
        // `GET /admin/segment-store` (ADR 0043 §A7b, roadmap U-07): this
        // node's own configured stream-segment store, the shard→replica
        // placement it sees (`cluster` kind only), and a bounded local
        // object count/bytes.
        "segment-store" => ("GET", "/admin/segment-store".into(), None),
        "control-members" => ("GET", "/admin/control/members".into(), None),
        // `POST /admin/control/transfer {to}` (ADR 0020/0037, roadmap U-05):
        // a single request/response, unlike `control-add`/`control-remove`/
        // `control-grow` above — the server itself does the bounded arm-
        // and-poll, so this is a flat one-shot route like `remove`/`drain`,
        // not `run_admin`'s own multi-step orchestration. Still targets the
        // leader's own admin address (local-control-leader-only, not
        // relayed), same as every other `control-*` action here.
        "control-transfer" => {
            let node = arg(2).ok_or("control-transfer needs <node-id>")?;
            let body = serde_json::json!({"to": node}).to_string();
            ("POST", "/admin/control/transfer".into(), Some(body))
        }
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
        // The replicated credential catalog (ADR 0066 §1/§2/§6) — never
        // echoes a secret in its own output; see `print_response`/
        // `http_call`'s plain "print the server's JSON verbatim" contract,
        // which is safe here precisely because `GET /admin/credentials`'s
        // own response never carries one (`animusd::admin::
        // credential_row_redacted`).
        "credentials" => ("GET", "/admin/credentials".into(), None),
        "credentials-put" => {
            let id = arg(2).ok_or("credentials-put needs <id>")?;
            let secret = arg(3).ok_or("credentials-put needs <secret>")?;
            let enabled: bool = match flag_value(args, "--enabled") {
                Some(v) => v
                    .parse()
                    .map_err(|_| "--enabled must be `true` or `false`")?,
                None => true,
            };
            let mut body = serde_json::json!({"id": id, "secret": secret, "enabled": enabled});
            if let Some(policy) = build_policy_body(args)? {
                body["policy"] = policy;
            }
            ("POST", "/admin/credentials".into(), Some(body.to_string()))
        }
        "credentials-rotate" => {
            let id = arg(2).ok_or("credentials-rotate needs <id>")?;
            let new_secret = arg(3).ok_or("credentials-rotate needs <new-secret>")?;
            let grace_secs: u64 = arg(4)
                .ok_or("credentials-rotate needs <grace-secs>")?
                .parse()
                .map_err(|_| "grace-secs must be a number")?;
            let body = serde_json::json!({
                "id": id,
                "new_secret": new_secret,
                "grace_secs": grace_secs,
            })
            .to_string();
            ("POST", "/admin/credentials/rotate".into(), Some(body))
        }
        "credentials-revoke" => {
            let id = arg(2).ok_or("credentials-revoke needs <id>")?;
            let body = serde_json::json!({"id": id}).to_string();
            ("POST", "/admin/credentials/revoke".into(), Some(body))
        }
        // Dynamo-proxy wrappers (roadmap U-08(ii)): each is a thin POST to
        // `/admin/data/dynamo` (`{op, payload}`, ADR 0021) reusing the exact
        // wire shapes `animusd`'s own dashboard already sends for these same
        // actions (`dashboard_backups.js`/`dashboard_browser.js`) — no new
        // route and no proxy allow-list change (`animusd::admin::
        // action_data_dynamo` has none beyond the bare-name Streams-vs-item
        // disambiguation, and none of these six ops are Streams ops).
        "backup-create" => {
            let table = arg(2).ok_or("backup-create needs <table>")?;
            let name = arg(3).ok_or("backup-create needs <backup-name>")?;
            let body = serde_json::json!({
                "op": "CreateBackup",
                "payload": {"TableName": table, "BackupName": name},
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        "backup-delete" => {
            let arn = arg(2).ok_or("backup-delete needs <backup-arn>")?;
            let body = serde_json::json!({
                "op": "DeleteBackup",
                "payload": {"BackupArn": arn},
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        "restore" => {
            let arn = arg(2).ok_or("restore needs <backup-arn>")?;
            let target = arg(3).ok_or("restore needs <target-table>")?;
            let body = serde_json::json!({
                "op": "RestoreTableFromBackup",
                "payload": {"TargetTableName": target, "BackupArn": arn},
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        // `pitr-enable`/`pitr-disable` share one arm — both build the
        // identical `UpdateContinuousBackups` shape, differing only in the
        // boolean (mirroring the dashboard's own `togglePitr`).
        "pitr-enable" | "pitr-disable" => {
            let table = arg(2).ok_or_else(|| format!("{sub} needs <table>"))?;
            let enabled = sub == "pitr-enable";
            let body = serde_json::json!({
                "op": "UpdateContinuousBackups",
                "payload": {
                    "TableName": table,
                    "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": enabled},
                },
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        // `ttl` (bare — `ttl-reaper` above claimed the diagnostic GET's own
        // name for exactly this reason): enables by default; `--disable`
        // (a fourth, positional-flag arg, mirroring `--force`/
        // `--force-control-remove` elsewhere in this file) disables. AWS
        // requires `AttributeName` on a disable call too (naming the
        // attribute being disabled), so this CLI form takes it either way
        // rather than trying to look up the current one over a second round
        // trip.
        "ttl" => {
            let table = arg(2).ok_or("ttl needs <table>")?;
            let attr = arg(3).ok_or("ttl needs <attribute>")?;
            let disable = arg(4) == Some("--disable");
            let body = serde_json::json!({
                "op": "UpdateTimeToLive",
                "payload": {
                    "TableName": table,
                    "TimeToLiveSpecification": {"Enabled": !disable, "AttributeName": attr},
                },
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        // `stream`: `off` disables (no `StreamViewType` — mirrors the
        // dashboard's own `disableStream`); any of DynamoDB's four real view
        // types enables/changes it. Validated client-side so a typo becomes
        // a clear CLI error instead of a wire-level `ValidationException`.
        "stream" => {
            let table = arg(2).ok_or("stream needs <table>")?;
            let view = arg(3)
                .ok_or("stream needs <NEW_IMAGE|OLD_IMAGE|NEW_AND_OLD_IMAGES|KEYS_ONLY|off>")?;
            let spec = if view == "off" {
                serde_json::json!({"StreamEnabled": false})
            } else {
                const VIEW_TYPES: &[&str] =
                    &["NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES", "KEYS_ONLY"];
                if !VIEW_TYPES.contains(&view) {
                    return Err(format!(
                        "stream view type must be one of NEW_IMAGE|OLD_IMAGE|\
                         NEW_AND_OLD_IMAGES|KEYS_ONLY|off, got `{view}`"
                    ));
                }
                serde_json::json!({"StreamEnabled": true, "StreamViewType": view})
            };
            let body = serde_json::json!({
                "op": "UpdateTable",
                "payload": {"TableName": table, "StreamSpecification": spec},
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        // S3 export (ADR 0068, S-05) — same dynamo-proxy-wrapper shape as
        // the backup/restore/PITR/TTL/stream group above:
        // `export-create` takes the source table's own ARN directly (this
        // adapter's own convention: `arn:aws:dynamodb:animus:0:table/
        // <name>`, printed by `DescribeTable`/`CreateTable`), never a bare
        // table name — matching real AWS's own
        // `aws dynamodb export-table-to-point-in-time --table-arn` shape,
        // and sidestepping ARN construction here (this crate has no
        // `animus-dynamo` dependency to build one with).
        "export-create" => {
            let table_arn = arg(2).ok_or("export-create needs <table-arn>")?;
            let bucket = arg(3).ok_or("export-create needs <s3-bucket>")?;
            let mut payload = serde_json::json!({"TableArn": table_arn, "S3Bucket": bucket});
            if let Some(prefix) = arg(4) {
                payload["S3Prefix"] = serde_json::Value::String(prefix.to_string());
            }
            let body = serde_json::json!({
                "op": "ExportTableToPointInTime",
                "payload": payload,
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        "export-describe" => {
            let export_arn = arg(2).ok_or("export-describe needs <export-arn>")?;
            let body = serde_json::json!({
                "op": "DescribeExport",
                "payload": {"ExportArn": export_arn},
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        "export-list" => {
            let mut payload = serde_json::Map::new();
            if let Some(table_arn) = arg(2) {
                payload.insert(
                    "TableArn".to_string(),
                    serde_json::Value::String(table_arn.to_string()),
                );
            }
            let body = serde_json::json!({
                "op": "ListExports",
                "payload": payload,
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        // S3 import (ADR 0068 §6, S-05 PR 2) — the mirror-image trio of the
        // export group above, over `ImportTable`/`DescribeImport`/
        // `ListImports`. `import-create` takes a bare **table name** (not
        // an ARN, unlike `export-create`) since the target table does not
        // exist yet — there is no ARN to name it by until this call
        // creates it — and builds a minimal `TableCreationParameters` from
        // `--pk NAME:TYPE`/`--sk NAME:TYPE` (no GSI/throughput flags yet;
        // this wrapper covers the common single-key-schema case, matching
        // `export-create`'s own "common case, not every field" scope note).
        "import-create" => {
            let table = arg(2).ok_or("import-create needs <table>")?;
            let bucket = arg(3).ok_or("import-create needs <s3-bucket>")?;
            // `[<prefix>]` is positional (mirrors `export-create`), but
            // this subcommand also has bare flags after it — an arg
            // starting with `--` here means the prefix was omitted.
            let prefix = arg(4).filter(|a| !a.starts_with("--"));
            let gzip = !args.iter().any(|a| a == "--none");
            let pk = flag_value(args, "--pk")
                .ok_or("import-create needs --pk NAME:TYPE (e.g. --pk id:S)")?;
            let (pk_name, pk_type) = pk
                .split_once(':')
                .ok_or("--pk must be NAME:TYPE (e.g. id:S)")?;
            let mut attribute_definitions = vec![serde_json::json!({
                "AttributeName": pk_name, "AttributeType": pk_type,
            })];
            let mut key_schema = vec![serde_json::json!({
                "AttributeName": pk_name, "KeyType": "HASH",
            })];
            if let Some(sk) = flag_value(args, "--sk") {
                let (sk_name, sk_type) = sk
                    .split_once(':')
                    .ok_or("--sk must be NAME:TYPE (e.g. ts:N)")?;
                attribute_definitions.push(serde_json::json!({
                    "AttributeName": sk_name, "AttributeType": sk_type,
                }));
                key_schema.push(serde_json::json!({
                    "AttributeName": sk_name, "KeyType": "RANGE",
                }));
            }
            let mut source = serde_json::json!({"S3Bucket": bucket});
            if let Some(prefix) = prefix {
                source["S3KeyPrefix"] = serde_json::Value::String(prefix.to_string());
            }
            let payload = serde_json::json!({
                "S3BucketSource": source,
                "InputFormat": "DYNAMODB_JSON",
                "InputCompressionType": if gzip { "GZIP" } else { "NONE" },
                "TableCreationParameters": {
                    "TableName": table,
                    "AttributeDefinitions": attribute_definitions,
                    "KeySchema": key_schema,
                },
            });
            let body = serde_json::json!({
                "op": "ImportTable",
                "payload": payload,
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        "import-describe" => {
            let import_arn = arg(2).ok_or("import-describe needs <import-arn>")?;
            let body = serde_json::json!({
                "op": "DescribeImport",
                "payload": {"ImportArn": import_arn},
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        "import-list" => {
            let mut payload = serde_json::Map::new();
            if let Some(table_arn) = arg(2) {
                payload.insert(
                    "TableArn".to_string(),
                    serde_json::Value::String(table_arn.to_string()),
                );
            }
            let body = serde_json::json!({
                "op": "ListImports",
                "payload": payload,
            })
            .to_string();
            ("POST", "/admin/data/dynamo".into(), Some(body))
        }
        other => return Err(format!("unknown admin subcommand `{other}`")),
    })
}

/// Build the `{"tables": {"kind": ..., ...}, "ops": [...]}` policy body
/// `animusd`'s `POST /admin/credentials` expects (ADR 0066 §1/§6) from
/// `--policy-tables`/`--policy-ops` — `Ok(None)` when neither flag is
/// given (the server then defaults to `Policy::allow_all()`). Giving only
/// one flag fills the other in with its own `allow_all()`-shaped default
/// (every table, or every class but `admin`) rather than requiring both —
/// an operator narrowing just one axis shouldn't have to spell out the
/// other. `--policy-tables` is `all` (every table), a bare comma-separated
/// list of exact table names, or `prefix:` followed by a comma-separated
/// list of prefixes; `--policy-ops` is a comma-separated list of
/// `read`/`write`/`ddl`/`streams`/`backup`/`admin`.
fn build_policy_body(args: &[String]) -> Result<Option<serde_json::Value>, String> {
    let tables_flag = flag_value(args, "--policy-tables");
    let ops_flag = flag_value(args, "--policy-ops");
    if tables_flag.is_none() && ops_flag.is_none() {
        return Ok(None);
    }
    let tables = match tables_flag {
        None | Some("all") => serde_json::json!({"kind": "all"}),
        Some(v) if v.starts_with("prefix:") => {
            let prefixes: Vec<&str> = v
                .trim_start_matches("prefix:")
                .split(',')
                .filter(|s| !s.is_empty())
                .collect();
            if prefixes.is_empty() {
                return Err("--policy-tables prefix:... needs at least one prefix".into());
            }
            serde_json::json!({"kind": "prefixes", "prefixes": prefixes})
        }
        Some(v) => {
            let names: Vec<&str> = v.split(',').filter(|s| !s.is_empty()).collect();
            if names.is_empty() {
                return Err("--policy-tables needs at least one table name".into());
            }
            serde_json::json!({"kind": "names", "names": names})
        }
    };
    let ops: Vec<&str> = match ops_flag {
        None => vec!["read", "write", "ddl", "streams", "backup"],
        Some(v) => v.split(',').filter(|s| !s.is_empty()).collect(),
    };
    if ops.is_empty() {
        return Err("--policy-ops needs at least one op".into());
    }
    Ok(Some(serde_json::json!({"tables": tables, "ops": ops})))
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
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<(), String> {
    // Unreachable / non-200 (e.g. an old binary with no such route, or a
    // follower's admin port before the caller even knows who leads):
    // skip the pre-check and let the ordinary flow's own final `remove`
    // step surface the authoritative refusal, if any.
    if let Ok((200, resp)) = http_call(addr, "GET", "/admin/control/members", None, tls).await {
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
            run_control_remove(addr, node, false, tls).await?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let (status, resp) =
                    http_call(addr, "GET", "/admin/control/members", None, tls).await?;
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
    let (status, resp) = http_call(addr, "POST", "/admin/drain", Some(drain_body), tls).await?;
    if !(200..300).contains(&status) {
        return Err(format!("drain failed (HTTP {status}): {resp}"));
    }
    println!("draining node {node}...");

    let status_path = format!("/admin/member/drain-status?node={node}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let (status, resp) = http_call(addr, "GET", &status_path, None, tls).await?;
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
    let (status, resp) =
        http_call(addr, "POST", "/admin/member/remove", Some(remove_body), tls).await?;
    if !(200..300).contains(&status) {
        return Err(format!("remove failed (HTTP {status}): {resp}"));
    }
    println!("node {node} removed; safe to stop the process");
    Ok(())
}

/// Pulls the new voter's internal control-Raft dial address out of a `GET
/// /admin/config` response body (`animusd::admin::config_view`'s JSON
/// shape). Since ADR 0040 PR1 merged the old top-level `control`/`raftkv`
/// address pair into one `addrs.internal` field, that is the key path to
/// read — **not** a top-level `control` field, which no longer exists at
/// all (see `run_control_add`'s own doc for the incident this fixes: that
/// stale read had silently broken the operator-supplied-id form of
/// `control-add` since the ADR 0040 PR1 rename, with nothing exercising the
/// path to catch it). Factored out as a pure, unit-testable function
/// specifically so this key path can be pinned against a captured real
/// response shape without opening a socket.
fn internal_addr_from_admin_config(cfg: &serde_json::Value) -> Result<String, String> {
    cfg["addrs"]["internal"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| {
            "the new node's /admin/config has no `addrs.internal` address \
             (is it a control-role or combined-mode node?)"
                .to_string()
        })
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
/// internal address (via [`internal_addr_from_admin_config`]), which then
/// goes into the add request to the **leader**.
/// Finally polls the **new node's own** `/admin/control/members` until it
/// reports itself a voter — mirroring `run_decommission`'s
/// poll-to-convergence shape (bounded, no fixed sleep-and-hope).
async fn run_control_add(
    leader_admin_addr: &str,
    node: &str,
    new_node_admin_addr: &str,
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<(), String> {
    let (status, resp) = http_call(new_node_admin_addr, "GET", "/admin/config", None, tls).await?;
    if !(200..300).contains(&status) {
        return Err(format!(
            "could not reach the new node's admin port {new_node_admin_addr} \
             (HTTP {status}): {resp}"
        ));
    }
    let cfg: serde_json::Value = serde_json::from_str(&resp)
        .map_err(|e| format!("malformed /admin/config response: {e}"))?;
    let control_addr = internal_addr_from_admin_config(&cfg)?;
    let control_addr = control_addr.as_str();

    let body = serde_json::json!({"node": node, "addr": control_addr}).to_string();
    let (status, resp) = http_call(
        leader_admin_addr,
        "POST",
        "/admin/control/member/add",
        Some(body),
        tls,
    )
    .await?;
    if !(200..300).contains(&status) {
        return Err(format!("control/member/add failed (HTTP {status}): {resp}"));
    }
    println!("added control voter {node} ({control_addr}); waiting for it to catch up...");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, resp) = http_call(
            new_node_admin_addr,
            "GET",
            "/admin/control/members",
            None,
            tls,
        )
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
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<(), String> {
    let body = serde_json::json!({"addr": new_node_control_addr}).to_string();
    let (status, resp) = http_call(
        leader_admin_addr,
        "POST",
        "/admin/control/member/add",
        Some(body),
        tls,
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
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<(), String> {
    let body = serde_json::json!({"node": node, "force": force}).to_string();
    let (status, resp) = http_call(
        leader_admin_addr,
        "POST",
        "/admin/control/member/remove",
        Some(body),
        tls,
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
async fn run_control_grow(
    leader_admin_addr: &str,
    pairs: &[String],
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<(), String> {
    for chunk in pairs.chunks(2) {
        let node = &chunk[0];
        let new_node_admin_addr = &chunk[1];
        run_control_add(leader_admin_addr, node, new_node_admin_addr, tls).await?;
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
    tls: Option<&tokio_rustls::TlsConnector>,
) -> Result<(u16, String), String> {
    let mut stream = maybe_tls_connect(addr, tls).await?;
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
            ("backup-store", "/admin/backup-store"),
            ("ttl-reaper", "/admin/ttl"),
            ("gc", "/admin/gc"),
            ("segment-store", "/admin/segment-store"),
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

    // --- Pre-existing one-shot mutating arms (roadmap C-04 E2): these seven
    // arms — `drain`/`drain-status`/`remove`/`reconfigure`/`flush`/`compact`/
    // `stream-grow` — predate both U-08(i) and U-08(ii) and had no coverage
    // of their own until now. Same shape as every other `admin_request` test
    // in this module: happy path + the argument-error paths the parser
    // already has, no new validation added.

    #[test]
    fn stream_grow_posts_the_table() {
        let (method, path, body) = admin_request("stream-grow", &args(&["orders"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/stream/grow");
        assert_eq!(body, Some(r#"{"table":"orders"}"#.to_string()));
    }

    #[test]
    fn stream_grow_needs_a_table() {
        assert!(admin_request("stream-grow", &args(&[])).is_err());
    }

    #[test]
    fn flush_posts_the_tablet() {
        let (method, path, body) = admin_request("flush", &args(&["5"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/storage/flush");
        assert_eq!(body, Some(r#"{"tablet":5}"#.to_string()));
    }

    #[test]
    fn compact_posts_the_tablet() {
        let (method, path, body) = admin_request("compact", &args(&["5"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/storage/compact");
        assert_eq!(body, Some(r#"{"tablet":5}"#.to_string()));
    }

    #[test]
    fn flush_and_compact_need_a_tablet() {
        assert!(admin_request("flush", &args(&[])).is_err());
        assert!(admin_request("compact", &args(&[])).is_err());
    }

    #[test]
    fn flush_and_compact_reject_a_non_numeric_tablet() {
        assert!(admin_request("flush", &args(&["not-a-number"])).is_err());
        assert!(admin_request("compact", &args(&["not-a-number"])).is_err());
    }

    #[test]
    fn reconfigure_posts_the_tablet_and_voter_list() {
        let (method, path, body) = admin_request("reconfigure", &args(&["5", "n1,n2,n3"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/raftkv/reconfigure");
        assert_eq!(
            body,
            Some(r#"{"tablet":5,"voters":["n1","n2","n3"]}"#.to_string())
        );
    }

    #[test]
    fn reconfigure_trims_whitespace_around_each_voter() {
        let (_, _, body) = admin_request("reconfigure", &args(&["5", "n1, n2 , n3"])).unwrap();
        assert_eq!(
            body,
            Some(r#"{"tablet":5,"voters":["n1","n2","n3"]}"#.to_string())
        );
    }

    #[test]
    fn reconfigure_needs_a_tablet_and_a_voter_list() {
        assert!(admin_request("reconfigure", &args(&[])).is_err());
        assert!(admin_request("reconfigure", &args(&["5"])).is_err());
    }

    #[test]
    fn reconfigure_rejects_a_non_numeric_tablet() {
        assert!(admin_request("reconfigure", &args(&["not-a-number", "n1,n2"])).is_err());
    }

    /// A malformed voter list — here, a trailing comma — is not rejected by
    /// this parser: `split(',')` yields a trailing empty string, which
    /// passes straight through as a voter named `""`. Documented as a
    /// regression pin of the parser's actual (permissive) behavior, not as
    /// an endorsement of it — validating voter *identifiers* is the
    /// server's job, same as every other admin route here.
    #[test]
    fn reconfigure_a_trailing_comma_in_the_voter_list_produces_an_empty_voter() {
        let (_, _, body) = admin_request("reconfigure", &args(&["5", "n1,n2,"])).unwrap();
        assert_eq!(
            body,
            Some(r#"{"tablet":5,"voters":["n1","n2",""]}"#.to_string())
        );
    }

    #[test]
    fn drain_posts_the_node() {
        let (method, path, body) = admin_request("drain", &args(&["n5"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/drain");
        assert_eq!(body, Some(r#"{"node":"n5"}"#.to_string()));
    }

    #[test]
    fn drain_needs_a_node_id() {
        assert!(admin_request("drain", &args(&[])).is_err());
    }

    #[test]
    fn drain_status_is_a_flat_get_querying_the_node() {
        let (method, path, body) = admin_request("drain-status", &args(&["n5"])).unwrap();
        assert_eq!(method, "GET");
        assert_eq!(path, "/admin/member/drain-status?node=n5");
        assert_eq!(body, None);
    }

    #[test]
    fn drain_status_needs_a_node_id() {
        assert!(admin_request("drain-status", &args(&[])).is_err());
    }

    #[test]
    fn remove_posts_the_node() {
        let (method, path, body) = admin_request("remove", &args(&["n5"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/member/remove");
        assert_eq!(body, Some(r#"{"node":"n5"}"#.to_string()));
    }

    #[test]
    fn remove_needs_a_node_id() {
        assert!(admin_request("remove", &args(&[])).is_err());
    }

    #[test]
    fn unknown_subcommand_is_an_error() {
        assert!(admin_request("no-such-thing", &args(&[])).is_err());
    }

    #[test]
    fn control_transfer_builds_a_post_naming_the_target_node() {
        let (method, path, body) = admin_request("control-transfer", &args(&["n2"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/control/transfer");
        assert_eq!(body, Some(r#"{"to":"n2"}"#.to_string()));
    }

    #[test]
    fn control_transfer_needs_a_node_id() {
        assert!(admin_request("control-transfer", &args(&[])).is_err());
    }

    /// Regression for the control-add `/admin/config` field issue: the old
    /// code read a top-level `control` field, removed by ADR 0040 PR1's
    /// `control`/`raftkv` → `addrs.internal` merge. A response carrying
    /// *only* the legacy shape must fail with a clear error, not silently
    /// resolve to nothing/panic.
    #[test]
    fn internal_addr_from_admin_config_rejects_the_removed_legacy_shape() {
        let legacy = serde_json::json!({"control": "127.0.0.1:9001"});
        let err = internal_addr_from_admin_config(&legacy)
            .expect_err("a `control`-only body must not resolve — that field is gone");
        assert!(
            err.contains("addrs.internal"),
            "error should name the field it actually looked for: {err}"
        );
    }

    /// The current real shape (`animusd::admin::config_view`, ADR 0040
    /// PR1): `addrs.internal` is where the new voter's internal
    /// control-Raft dial address actually lives. Captured field-for-field
    /// from that function's own `json!({ .. })` literal (a subset — only
    /// the fields this helper's key path touches need be present).
    #[test]
    fn internal_addr_from_admin_config_reads_the_current_shape() {
        let current = serde_json::json!({
            "role": "combined",
            "node_id": "n0",
            "control_ids": ["n0"],
            "addrs": {
                "internal": "127.0.0.1:9001",
                "client": "127.0.0.1:9002",
                "dynamo": "127.0.0.1:9003",
                "admin": "127.0.0.1:9004",
            },
            "peers": {},
        });
        assert_eq!(
            internal_addr_from_admin_config(&current).unwrap(),
            "127.0.0.1:9001"
        );
    }

    /// `AdminInfo::internal_addr` is modeled as `Option<SocketAddr>`
    /// (`animusd::lib.rs`'s own doc: `None` only for a node with no internal
    /// role at all, which "doesn't occur in practice") — so `addrs.internal`
    /// can in principle serialize as JSON `null`, not just be absent or a
    /// string. Must be treated the same as "no address available", never as
    /// a literal `"null"` string.
    #[test]
    fn internal_addr_from_admin_config_rejects_a_null_internal_addr() {
        let no_internal = serde_json::json!({"addrs": {"internal": null}});
        assert!(internal_addr_from_admin_config(&no_internal).is_err());
    }

    #[test]
    fn credentials_list_is_a_flat_get() {
        let (method, path, body) = admin_request("credentials", &args(&[])).unwrap();
        assert_eq!(method, "GET");
        assert_eq!(path, "/admin/credentials");
        assert_eq!(body, None);
    }

    #[test]
    fn credentials_put_defaults_enabled_true_and_omits_policy() {
        let (method, path, body) =
            admin_request("credentials-put", &args(&["AKID1", "s3cr3t"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/credentials");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["id"], "AKID1");
        assert_eq!(v["secret"], "s3cr3t");
        assert_eq!(v["enabled"], true);
        assert!(v.get("policy").is_none(), "no policy flags given");
    }

    #[test]
    fn credentials_put_never_echoes_the_secret_in_the_path() {
        // The secret must travel only in the POST body, never the URL (which
        // could end up in a proxy/access log) — a cheap regression against a
        // future refactor that moves it into a query string by mistake.
        let (_, path, _) = admin_request("credentials-put", &args(&["AKID1", "s3cr3t"])).unwrap();
        assert!(!path.contains("s3cr3t"));
    }

    #[test]
    fn credentials_put_disabled_flag() {
        let (_, _, body) = admin_request(
            "credentials-put",
            &args(&["AKID1", "s3cr3t", "--enabled", "false"]),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["enabled"], false);
    }

    #[test]
    fn credentials_put_invalid_enabled_flag_is_an_error() {
        assert!(
            admin_request(
                "credentials-put",
                &args(&["AKID1", "s3cr3t", "--enabled", "maybe"]),
            )
            .is_err()
        );
    }

    #[test]
    fn credentials_put_policy_tables_all() {
        let (_, _, body) = admin_request(
            "credentials-put",
            &args(&[
                "AKID1",
                "s3cr3t",
                "--policy-tables",
                "all",
                "--policy-ops",
                "read,write",
            ]),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["policy"]["tables"], serde_json::json!({"kind": "all"}));
        assert_eq!(v["policy"]["ops"], serde_json::json!(["read", "write"]));
    }

    #[test]
    fn credentials_put_policy_tables_names() {
        let (_, _, body) = admin_request(
            "credentials-put",
            &args(&[
                "AKID1",
                "s3cr3t",
                "--policy-tables",
                "orders,customers",
                "--policy-ops",
                "read",
            ]),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(
            v["policy"]["tables"],
            serde_json::json!({"kind": "names", "names": ["orders", "customers"]})
        );
    }

    #[test]
    fn credentials_put_policy_tables_prefix() {
        let (_, _, body) = admin_request(
            "credentials-put",
            &args(&[
                "AKID1",
                "s3cr3t",
                "--policy-tables",
                "prefix:tenant-a-,tenant-b-",
            ]),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(
            v["policy"]["tables"],
            serde_json::json!({"kind": "prefixes", "prefixes": ["tenant-a-", "tenant-b-"]})
        );
        // `--policy-ops` was omitted: defaults to the `allow_all()` shape.
        assert_eq!(
            v["policy"]["ops"],
            serde_json::json!(["read", "write", "ddl", "streams", "backup"])
        );
    }

    #[test]
    fn credentials_rotate_body() {
        let (method, path, body) =
            admin_request("credentials-rotate", &args(&["AKID1", "newsecret", "3600"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/credentials/rotate");
        assert_eq!(
            body,
            Some(r#"{"grace_secs":3600,"id":"AKID1","new_secret":"newsecret"}"#.to_string())
        );
    }

    #[test]
    fn credentials_rotate_needs_a_numeric_grace_secs() {
        assert!(
            admin_request("credentials-rotate", &args(&["AKID1", "newsecret", "soon"]),).is_err()
        );
    }

    #[test]
    fn credentials_revoke_body() {
        let (method, path, body) = admin_request("credentials-revoke", &args(&["AKID1"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/credentials/revoke");
        assert_eq!(body, Some(r#"{"id":"AKID1"}"#.to_string()));
    }

    // --- Dynamo-proxy wrappers (roadmap U-08(ii)) -------------------------

    #[test]
    fn backup_create_posts_the_real_create_backup_shape() {
        let (method, path, body) =
            admin_request("backup-create", &args(&["orders", "nightly-1"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "CreateBackup");
        assert_eq!(
            v["payload"],
            serde_json::json!({"TableName": "orders", "BackupName": "nightly-1"})
        );
    }

    #[test]
    fn backup_create_needs_table_and_name() {
        assert!(admin_request("backup-create", &args(&[])).is_err());
        assert!(admin_request("backup-create", &args(&["orders"])).is_err());
    }

    #[test]
    fn backup_delete_posts_the_real_delete_backup_shape() {
        let arn = "arn:aws:dynamodb:us-east-1:000000000000:table/orders/backup/01234";
        let (method, path, body) = admin_request("backup-delete", &args(&[arn])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "DeleteBackup");
        assert_eq!(v["payload"], serde_json::json!({"BackupArn": arn}));
    }

    #[test]
    fn backup_delete_needs_a_backup_arn() {
        assert!(admin_request("backup-delete", &args(&[])).is_err());
    }

    #[test]
    fn restore_posts_the_real_restore_table_from_backup_shape() {
        let arn = "arn:aws:dynamodb:us-east-1:000000000000:table/orders/backup/01234";
        let (method, path, body) =
            admin_request("restore", &args(&[arn, "orders-restored"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "RestoreTableFromBackup");
        assert_eq!(
            v["payload"],
            serde_json::json!({"TargetTableName": "orders-restored", "BackupArn": arn})
        );
    }

    #[test]
    fn restore_needs_backup_arn_and_target_table() {
        assert!(admin_request("restore", &args(&[])).is_err());
        assert!(admin_request("restore", &args(&["arn:aws:..."])).is_err());
    }

    #[test]
    fn pitr_enable_posts_update_continuous_backups_true() {
        let (method, path, body) = admin_request("pitr-enable", &args(&["orders"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "UpdateContinuousBackups");
        assert_eq!(
            v["payload"],
            serde_json::json!({
                "TableName": "orders",
                "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": true},
            })
        );
    }

    #[test]
    fn pitr_disable_posts_update_continuous_backups_false() {
        let (_, _, body) = admin_request("pitr-disable", &args(&["orders"])).unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(
            v["payload"]["PointInTimeRecoverySpecification"]["PointInTimeRecoveryEnabled"],
            false
        );
    }

    #[test]
    fn pitr_enable_and_disable_need_a_table() {
        assert!(admin_request("pitr-enable", &args(&[])).is_err());
        assert!(admin_request("pitr-disable", &args(&[])).is_err());
    }

    #[test]
    fn ttl_enable_posts_update_time_to_live_true() {
        let (method, path, body) = admin_request("ttl", &args(&["orders", "expiresAt"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "UpdateTimeToLive");
        assert_eq!(
            v["payload"],
            serde_json::json!({
                "TableName": "orders",
                "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expiresAt"},
            })
        );
    }

    #[test]
    fn ttl_disable_flag_posts_enabled_false_with_the_same_attribute() {
        let (_, _, body) =
            admin_request("ttl", &args(&["orders", "expiresAt", "--disable"])).unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(
            v["payload"],
            serde_json::json!({
                "TableName": "orders",
                "TimeToLiveSpecification": {"Enabled": false, "AttributeName": "expiresAt"},
            })
        );
    }

    #[test]
    fn ttl_needs_table_and_attribute() {
        assert!(admin_request("ttl", &args(&[])).is_err());
        assert!(admin_request("ttl", &args(&["orders"])).is_err());
    }

    #[test]
    fn stream_enable_posts_update_table_with_stream_specification() {
        let (method, path, body) =
            admin_request("stream", &args(&["orders", "NEW_AND_OLD_IMAGES"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "UpdateTable");
        assert_eq!(
            v["payload"],
            serde_json::json!({
                "TableName": "orders",
                "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
            })
        );
    }

    #[test]
    fn stream_off_disables_with_no_view_type() {
        let (_, _, body) = admin_request("stream", &args(&["orders", "off"])).unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(
            v["payload"],
            serde_json::json!({
                "TableName": "orders",
                "StreamSpecification": {"StreamEnabled": false},
            })
        );
    }

    #[test]
    fn stream_rejects_an_unknown_view_type() {
        let err = admin_request("stream", &args(&["orders", "NOT_A_REAL_VIEW"]))
            .expect_err("an invalid view type must be rejected client-side");
        assert!(err.contains("NOT_A_REAL_VIEW"), "{err}");
    }

    #[test]
    fn stream_needs_table_and_view_type() {
        assert!(admin_request("stream", &args(&[])).is_err());
        assert!(admin_request("stream", &args(&["orders"])).is_err());
    }

    // --- S3 export (ADR 0068, S-05) ---------------------------------------

    #[test]
    fn export_create_posts_the_real_export_table_shape() {
        let table_arn = "arn:aws:dynamodb:animus:0:table/orders";
        let (method, path, body) =
            admin_request("export-create", &args(&[table_arn, "my-bucket"])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "ExportTableToPointInTime");
        assert_eq!(
            v["payload"],
            serde_json::json!({"TableArn": table_arn, "S3Bucket": "my-bucket"})
        );
    }

    #[test]
    fn export_create_includes_an_optional_s3_prefix() {
        let table_arn = "arn:aws:dynamodb:animus:0:table/orders";
        let (_, _, body) = admin_request(
            "export-create",
            &args(&[table_arn, "my-bucket", "exports/orders"]),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(
            v["payload"],
            serde_json::json!({
                "TableArn": table_arn,
                "S3Bucket": "my-bucket",
                "S3Prefix": "exports/orders",
            })
        );
    }

    #[test]
    fn export_create_needs_table_arn_and_bucket() {
        assert!(admin_request("export-create", &args(&[])).is_err());
        assert!(
            admin_request(
                "export-create",
                &args(&["arn:aws:dynamodb:animus:0:table/orders"])
            )
            .is_err()
        );
    }

    #[test]
    fn export_describe_posts_the_real_describe_export_shape() {
        let arn = "arn:aws:dynamodb:animus:0:table/orders/export/01234";
        let (method, path, body) = admin_request("export-describe", &args(&[arn])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "DescribeExport");
        assert_eq!(v["payload"], serde_json::json!({"ExportArn": arn}));
    }

    #[test]
    fn export_describe_needs_an_export_arn() {
        assert!(admin_request("export-describe", &args(&[])).is_err());
    }

    #[test]
    fn export_list_with_no_filter_posts_an_empty_payload() {
        let (method, path, body) = admin_request("export-list", &args(&[])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "ListExports");
        assert_eq!(v["payload"], serde_json::json!({}));
    }

    #[test]
    fn export_list_with_a_table_arn_filters_by_it() {
        let table_arn = "arn:aws:dynamodb:animus:0:table/orders";
        let (_, _, body) = admin_request("export-list", &args(&[table_arn])).unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["payload"], serde_json::json!({"TableArn": table_arn}));
    }

    // --- S3 import (ADR 0068 §6, S-05 PR 2) --------------------------------

    #[test]
    fn import_create_posts_the_real_import_table_shape() {
        let (method, path, body) = admin_request(
            "import-create",
            &args(&["orders", "my-bucket", "--pk", "id:S"]),
        )
        .unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "ImportTable");
        assert_eq!(v["payload"]["S3BucketSource"]["S3Bucket"], "my-bucket");
        assert!(v["payload"]["S3BucketSource"].get("S3KeyPrefix").is_none());
        assert_eq!(v["payload"]["InputFormat"], "DYNAMODB_JSON");
        assert_eq!(v["payload"]["InputCompressionType"], "GZIP");
        assert_eq!(
            v["payload"]["TableCreationParameters"],
            serde_json::json!({
                "TableName": "orders",
                "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            })
        );
    }

    #[test]
    fn import_create_includes_an_optional_prefix_and_sort_key_and_none_compression() {
        let (_, _, body) = admin_request(
            "import-create",
            &args(&[
                "orders",
                "my-bucket",
                "exports/orders",
                "--none",
                "--pk",
                "id:S",
                "--sk",
                "ts:N",
            ]),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(
            v["payload"]["S3BucketSource"]["S3KeyPrefix"],
            "exports/orders"
        );
        assert_eq!(v["payload"]["InputCompressionType"], "NONE");
        assert_eq!(
            v["payload"]["TableCreationParameters"],
            serde_json::json!({
                "TableName": "orders",
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"},
                    {"AttributeName": "ts", "AttributeType": "N"},
                ],
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"},
                    {"AttributeName": "ts", "KeyType": "RANGE"},
                ],
            })
        );
    }

    #[test]
    fn import_create_needs_table_bucket_and_pk() {
        assert!(admin_request("import-create", &args(&[])).is_err());
        assert!(admin_request("import-create", &args(&["orders"])).is_err());
        assert!(admin_request("import-create", &args(&["orders", "my-bucket"])).is_err());
        assert!(
            admin_request(
                "import-create",
                &args(&["orders", "my-bucket", "--pk", "id"])
            )
            .is_err(),
            "a malformed --pk (no `:TYPE`) must be rejected"
        );
    }

    #[test]
    fn import_describe_posts_the_real_describe_import_shape() {
        let arn = "arn:aws:dynamodb:animus:0:table/orders/import/01234";
        let (method, path, body) = admin_request("import-describe", &args(&[arn])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "DescribeImport");
        assert_eq!(v["payload"], serde_json::json!({"ImportArn": arn}));
    }

    #[test]
    fn import_describe_needs_an_import_arn() {
        assert!(admin_request("import-describe", &args(&[])).is_err());
    }

    #[test]
    fn import_list_with_no_filter_posts_an_empty_payload() {
        let (method, path, body) = admin_request("import-list", &args(&[])).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(path, "/admin/data/dynamo");
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "ListImports");
        assert_eq!(v["payload"], serde_json::json!({}));
    }

    #[test]
    fn import_list_with_a_table_arn_filters_by_it() {
        let table_arn = "arn:aws:dynamodb:animus:0:table/orders";
        let (_, _, body) = admin_request("import-list", &args(&[table_arn])).unwrap();
        let v: serde_json::Value = serde_json::from_str(body.as_ref().unwrap()).unwrap();
        assert_eq!(v["payload"], serde_json::json!({"TableArn": table_arn}));
    }

    #[test]
    fn flag_value_finds_the_value_following_its_name_anywhere_in_args() {
        let a = args(&["--a", "1", "--b", "2"]);
        assert_eq!(flag_value(&a, "--b"), Some("2"));
        assert_eq!(flag_value(&a, "--missing"), None);
    }

    // --- `--tls-ca` (ADR 0064, S-01 commit 2) -----------------------------

    #[test]
    fn extract_tls_ca_absent_is_none_and_leaves_args_untouched() {
        let mut a = vec!["status".to_string(), "127.0.0.1:9000".to_string()];
        let before = a.clone();
        assert_eq!(extract_tls_ca(&mut a).unwrap(), None);
        assert_eq!(a, before);
    }

    #[test]
    fn extract_tls_ca_removes_the_flag_and_its_value_wherever_it_appears() {
        let mut a = vec![
            "status".to_string(),
            "--tls-ca".to_string(),
            "ca.pem".to_string(),
            "127.0.0.1:9000".to_string(),
        ];
        assert_eq!(extract_tls_ca(&mut a).unwrap(), Some("ca.pem".to_string()));
        assert_eq!(a, vec!["status".to_string(), "127.0.0.1:9000".to_string()]);
    }

    #[test]
    fn extract_tls_ca_at_the_end_with_no_value_is_an_error() {
        let mut a = vec!["status".to_string(), "--tls-ca".to_string()];
        let err = extract_tls_ca(&mut a).expect_err("no value must be rejected");
        assert!(err.contains("--tls-ca"), "{err}");
    }

    #[test]
    fn build_tls_connector_rejects_a_missing_file() {
        let err = build_tls_connector("/no/such/ca.pem")
            .err()
            .expect("a nonexistent CA file must be rejected");
        assert!(err.contains("--tls-ca"), "{err}");
    }

    #[test]
    fn build_tls_connector_rejects_a_file_with_no_certificates() {
        let dir = std::env::temp_dir().join(format!(
            "animus-cli-tls-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.pem");
        std::fs::write(&path, b"not a certificate").unwrap();
        let err = build_tls_connector(path.to_str().unwrap())
            .err()
            .expect("a file with no certificates must be rejected");
        assert!(err.contains("no certificates"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
