//! End-to-end TLS coverage (ADR 0064, S-01 commit 2): a real 3-node
//! combined cluster with TLS on every port — client, dynamo, admin,
//! console, intra, and (via `animus-env`, commit 1) the internal Raft
//! wire. Every dial in this file is a genuine `tokio-rustls` handshake
//! against a real loopback socket, never a mock of `rustls` itself —
//! mirroring `animus-env`'s own `prod::tests` TLS suite (commit 1) and
//! `support::tls_pki`'s doc for why this is a small independent copy of
//! that crate's `#[cfg(test)]`-private PKI helper.
//!
//! **Real time/sockets (the `ProdEnv` edge)** — outside the `Env` seam by
//! design, same posture as every other `animusd` integration test.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use animusd::ClusterConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod support;

/// Build a **server-only** `rustls` `ClientConfig` trusting `ca_path` —
/// the exact shape `animus-cli`'s `--tls-ca` builds (this CLI/test never
/// presents a client certificate): verifies the `client`/`dynamo`/`admin`/
/// `console` ports, every one of which is server-only TLS (ADR 0064
/// Decision 2).
fn server_only_connector(ca_path: &Path) -> tokio_rustls::TlsConnector {
    let bytes = std::fs::read(ca_path).expect("read ca.pem");
    let certs = rustls_pemfile::certs(&mut bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .expect("parse ca certs");
    let mut root_store = rustls::RootCertStore::empty();
    for cert in certs {
        root_store.add(cert).expect("add ca cert");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("default protocol versions")
        .with_root_certificates(root_store)
        .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

/// Build a **mutual** `rustls` `ClientConfig`: trusts `trust_ca_path` (to
/// verify the server) and presents `own_cert_path`/`own_key_path` as its
/// own client certificate — the shape the `intra` port (and the internal
/// Raft wire) requires.
fn mutual_connector(
    trust_ca_path: &Path,
    own_cert_path: &Path,
    own_key_path: &Path,
) -> tokio_rustls::TlsConnector {
    let ca_bytes = std::fs::read(trust_ca_path).expect("read ca.pem");
    let ca_certs = rustls_pemfile::certs(&mut ca_bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .expect("parse ca certs");
    let mut root_store = rustls::RootCertStore::empty();
    for cert in ca_certs {
        root_store.add(cert).expect("add ca cert");
    }
    let own_cert_bytes = std::fs::read(own_cert_path).expect("read own cert");
    let own_certs = rustls_pemfile::certs(&mut own_cert_bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .expect("parse own certs");
    let own_key_bytes = std::fs::read(own_key_path).expect("read own key");
    let own_key = rustls_pemfile::private_key(&mut own_key_bytes.as_slice())
        .expect("parse own key")
        .expect("own key present");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("default protocol versions")
        .with_root_certificates(root_store)
        .with_client_auth_cert(own_certs, own_key)
        .expect("build mutual tls client config");
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

/// Issue one DynamoDB JSON request over a **server-only** TLS connection
/// and return `(status, body)` — the TLS counterpart of the plain-TCP
/// `dynamo()` helper every other `animusd` dynamo test file hand-rolls
/// (per this crate's own testing convention).
async fn tls_dynamo(
    connector: &tokio_rustls::TlsConnector,
    addr: SocketAddr,
    target: &str,
    body: &str,
) -> (u16, String) {
    let tcp = TcpStream::connect(addr).await.expect("connect");
    let server_name =
        animus_env::tls::server_name_for(&addr.to_string()).expect("derive server name");
    let mut stream = connector
        .connect(server_name, tcp)
        .await
        .expect("tls handshake");
    let req = format!(
        "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
         Connection: close\r\n\
         Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    // A `Connection: close` HTTP/1.x server (this crate's hand-rolled HTTP
    // edges) closes the raw TCP socket once the response is fully written,
    // without a TLS `close_notify` alert first — `rustls` treats that as
    // an `UnexpectedEof` *error*, not a clean stream end, even though every
    // byte of the response was already delivered into `buf` before the
    // abrupt close. Ignore the `Result`: what matters is what's in `buf`.
    let _ = stream.read_to_end(&mut buf).await;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
    (status, body.to_owned())
}

/// Issue one plain HTTP GET over a **server-only** TLS connection —
/// shared by the admin and console checks below.
async fn tls_get(connector: &tokio_rustls::TlsConnector, addr: SocketAddr, path: &str) -> u16 {
    let tcp = TcpStream::connect(addr).await.expect("connect");
    let server_name =
        animus_env::tls::server_name_for(&addr.to_string()).expect("derive server name");
    let mut stream = connector
        .connect(server_name, tcp)
        .await
        .expect("tls handshake");
    let req = format!("GET {path} HTTP/1.0\r\nHost: x\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    // See `tls_dynamo`'s identical comment: ignore the `Result`, the
    // response is already fully in `buf` regardless of how the TLS
    // session's own teardown is reported.
    let _ = stream.read_to_end(&mut buf).await;
    let text = String::from_utf8_lossy(&buf).into_owned();
    text.split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

const CREATE_TABLE: &str = "DynamoDB_20120810.CreateTable";
const PUT_ITEM: &str = "DynamoDB_20120810.PutItem";
const GET_ITEM: &str = "DynamoDB_20120810.GetItem";

/// A real 3-node combined cluster, every port TLS-enabled: `CreateTable`/
/// `PutItem`/`GetItem` round-trip over server-only TLS on the dynamo port,
/// issuing the write and the read against **different** nodes than the one
/// that created the table — whichever of those isn't the tablet's leader
/// forwards over the mutual-TLS `intra` port to reach it, exactly the same
/// code path a plain-TCP cluster uses, just now with every hop encrypted
/// and peer-authenticated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_cluster_serves_dynamo_put_get_across_nodes() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config, _pki_dir) =
        support::bring_up_deadline_tls(3, dir.path(), std::time::Duration::from_secs(30)).await;
    assert!(
        config.nodes.iter().all(|n| n.tls.is_some()),
        "every node's config entry must carry a tls section"
    );
    let ca_path = config.nodes[0]
        .tls
        .as_ref()
        .expect("node 0 has tls")
        .ca_path
        .clone()
        .expect("ca_path present");
    let connector = server_only_connector(&ca_path);

    let table = "tls_items";
    let (status, body) = tls_dynamo(
        &connector,
        nodes[0].dynamo_addr(),
        CREATE_TABLE,
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable over TLS failed: {body}");

    let (status, body) = tls_dynamo(
        &connector,
        nodes[1].dynamo_addr(),
        PUT_ITEM,
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"k1"}},"v":{{"S":"hello-tls"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "PutItem over TLS failed: {body}");

    let (status, body) = tls_dynamo(
        &connector,
        nodes[2].dynamo_addr(),
        GET_ITEM,
        // `ConsistentRead: true` (the linearizable ReadIndex path, ADR
        // 0055) — this read is issued against a *different* node than the
        // one that just wrote the item, and the wire's default
        // (`ConsistentRead: false`, any replica's own applied state, no
        // read-your-writes guarantee) would make this a genuine race
        // against replication, unrelated to anything this test is actually
        // checking (that TLS carries the request/forward/reply correctly).
        &format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"k1"}}}},"ConsistentRead":true}}"#),
    )
    .await;
    assert_eq!(status, 200, "GetItem over TLS failed: {body}");
    assert!(
        body.contains("hello-tls"),
        "GetItem reply must contain the value written on a different node: {body}"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// The admin `GET /admin/metrics` and console `GET /` routes both serve
/// over server-only TLS (ADR 0064 Decision 2) — same connector as the
/// dynamo port, different destination address.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_admin_and_console_serve_over_tls() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config, _pki_dir) =
        support::bring_up_deadline_tls(1, dir.path(), std::time::Duration::from_secs(30)).await;
    let ca_path = config.nodes[0]
        .tls
        .as_ref()
        .expect("tls configured")
        .ca_path
        .clone()
        .expect("ca_path present");
    let connector = server_only_connector(&ca_path);

    let status = tls_get(&connector, nodes[0].admin_addr(), "/admin/metrics").await;
    assert_eq!(status, 200, "admin /admin/metrics over TLS must succeed");

    let status = tls_get(&connector, nodes[0].console_addr(), "/").await;
    assert_eq!(status, 200, "console GET / over TLS must succeed");

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// A plain-TCP dial into the TLS-enabled dynamo port fails cleanly (no
/// valid HTTP reply — the server's TLS acceptor cannot parse a bare HTTP
/// request as a handshake and drops the connection), and the port keeps
/// serving genuine TLS clients right afterward — mirroring `animus-env`'s
/// own `tls_listener_rejects_plain_dial_and_keeps_serving_tls_peers`
/// (commit 1) at this crate's own listeners.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_tcp_dial_to_tls_dynamo_port_is_refused_and_port_keeps_serving() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config, _pki_dir) =
        support::bring_up_deadline_tls(1, dir.path(), std::time::Duration::from_secs(30)).await;
    let addr = nodes[0].dynamo_addr();

    // A bare, non-TLS HTTP request: the server's TLS acceptor treats these
    // bytes as a (garbage) handshake and closes the connection before any
    // HTTP framing is ever reached.
    let mut plain = TcpStream::connect(addr).await.expect("connect");
    let req = "GET /metrics HTTP/1.0\r\nHost: x\r\nConnection: close\r\n\r\n";
    let _ = plain.write_all(req.as_bytes()).await;
    let mut buf = Vec::new();
    let _ = plain.read_to_end(&mut buf).await;
    let text = String::from_utf8_lossy(&buf);
    assert!(
        !text.starts_with("HTTP/"),
        "a plain-TCP dial must never get a real HTTP reply from a TLS-only \
         port: {text:?}"
    );

    // The listener must still serve a genuine TLS client right after.
    let ca_path = config.nodes[0]
        .tls
        .as_ref()
        .expect("tls configured")
        .ca_path
        .clone()
        .expect("ca_path present");
    let connector = server_only_connector(&ca_path);
    let status = tls_get(&connector, addr, "/metrics").await;
    assert_eq!(
        status, 200,
        "the dynamo port must keep serving genuine TLS clients after a \
         rejected plain-TCP dial"
    );

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// A client presenting a certificate signed by a **different** CA is
/// refused on the `intra` port (mutual TLS) — the server's own
/// `ClientConfig` trusts only the cluster's own CA, so a peer from an
/// unrelated CA never completes the handshake, mirroring `animus-env`'s
/// `tls_peer_from_different_ca_is_refused` (commit 1) one layer up, at
/// `animusd`'s own intra listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mismatched_ca_client_on_intra_port_is_refused() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config, _pki_dir) =
        support::bring_up_deadline_tls(1, dir.path(), std::time::Duration::from_secs(30)).await;
    let cluster_ca_path = config.nodes[0]
        .tls
        .as_ref()
        .expect("tls configured")
        .ca_path
        .clone()
        .expect("ca_path present");

    // An entirely independent PKI — trusts the *real* cluster CA (so the
    // handshake gets far enough to check the client's own cert), but
    // presents a leaf signed by a CA the cluster never heard of.
    let (_attacker_dir, attacker_sections) = support::tls_pki(&["127.0.0.1"]);
    let attacker = &attacker_sections[0];
    let connector = mutual_connector(&cluster_ca_path, &attacker.cert_path, &attacker.key_path);

    let tcp = TcpStream::connect(nodes[0].intra_addr())
        .await
        .expect("connect");
    let server_name = animus_env::tls::server_name_for(&nodes[0].intra_addr().to_string())
        .expect("derive server name");
    // TLS 1.3's client-side handshake future can resolve `Ok` having only
    // *sent* its own `Finished` flight, without yet having read back the
    // server's verdict on the client certificate it just presented — the
    // server only sends its rejection (a fatal alert) after verifying that
    // cert, which the client only observes on its *next* read/write, not
    // necessarily from `connect()` itself (the identical reasoning
    // `animus_env::prod::tests::tls_peer_from_different_ca_is_refused`
    // documents for the server-to-server case: no frame ever arrives,
    // rather than asserting the dial call itself errors). So: try the
    // handshake, then — whether it already failed or not — attempt to
    // write and read one frame; *that* must fail either way.
    match connector.connect(server_name, tcp).await {
        Err(_) => {} // rejected during the handshake itself — also acceptable.
        Ok(mut stream) => {
            let write_ok = stream
                .write_all(b"hello-from-an-unrelated-ca")
                .await
                .is_ok();
            let mut buf = [0u8; 1];
            let read_ok = stream.read(&mut buf).await.is_ok_and(|n| n > 0);
            assert!(
                !(write_ok && read_ok),
                "a client cert signed by an unrelated CA must never get a real \
                 response out of the intra port"
            );
        }
    }

    for node in &nodes {
        node.shutdown_graceful().await;
    }
}

/// A cluster config with TLS on only some nodes is a hard load-time error
/// (ADR 0064 Decision 3 / `ClusterConfig::validate_tls`) — no live cluster
/// needed, this is caught before any node ever binds a socket.
#[test]
fn mixed_tls_config_fails_validation_at_load() {
    let (_pki_dir, sections) = support::tls_pki(&["127.0.0.1", "127.0.0.1", "127.0.0.1"]);
    let mut config = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 17000);
    // Only the first node gets a `tls` section — the other two stay plain.
    config.nodes[0].tls = Some(sections[0].clone());
    let err = ClusterConfig::from_json(&config.to_json())
        .expect_err("a config with TLS on only one of three nodes must be rejected at load");
    assert!(err.to_string().contains("mixed TLS configuration"), "{err}");

    // The all-or-none rule has two legal shapes: all-plain (the config as
    // `generate` produced it, before this test's own mutation) round-trips
    // fine, and so does all-TLS.
    let all_plain = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 17100);
    ClusterConfig::from_json(&all_plain.to_json()).expect("all-plain config must load");

    let mut all_tls = ClusterConfig::generate(3, "127.0.0.1".parse().unwrap(), 17200);
    for (node, section) in all_tls.nodes.iter_mut().zip(sections.iter()) {
        node.tls = Some(section.clone());
    }
    ClusterConfig::from_json(&all_tls.to_json()).expect("all-tls config must load");
}
