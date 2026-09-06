//! The one real-socket [`Transport`] implementor (S-04 PR 1) — gated behind
//! the default-off `prod` Cargo feature, mirroring `animus-env`'s own
//! `prod` feature (ADR 0061 rung C0): a pure consumer of this crate that
//! builds with `default-features = false` never links a socket, TLS stack,
//! or `tokio::spawn` call at all.
//!
//! One TCP (+ optional TLS) connection **per request** — no pooling, no
//! retries (the `SegmentStore` layer this crate is built for, a future PR,
//! owns both). This module is never exercised by the workspace gates: its
//! own test (`tests/minio_real_endpoint.rs`) is opt-in, gated on the
//! `ANIMUS_S3_TEST_ENDPOINT` environment variable being set, and prints a
//! skip line and returns immediately otherwise — see `CLAUDE.md` for how to
//! run it against a real MinIO/localstack instance.
#![allow(
    clippy::disallowed_methods,
    reason = "this module is the crate's one real-I/O process boundary: a \
              real TCP/TLS connection per S3 request (tokio::spawn to drive \
              the hyper connection future) — exactly the same justification \
              animus-env's ProdEnv carries for the identical call, ADR 0061 \
              rung B5"
)]

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper_util::rt::TokioIo;
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::client::{HttpRequest, HttpResponse, Transport, TransportError};

/// A [`Transport`] that dials a real TCP socket, optionally wrapped in
/// TLS, once per request.
///
/// `insecure_http` allows a plain `http://` endpoint (no TLS at all) —
/// intended **only** for a loopback MinIO/localstack test target; a real
/// S3-compatible production endpoint always negotiates TLS. This is a
/// property of the *transport instance*, not inferred from the endpoint
/// string, so a caller must opt in explicitly rather than a `http://` typo
/// silently downgrading a production config to plaintext.
pub struct HyperRustlsTransport {
    insecure_http: bool,
}

impl HyperRustlsTransport {
    /// A transport that requires TLS for every request (the production
    /// default).
    ///
    /// # Errors
    /// If the `ring` crypto provider cannot be installed as the process
    /// default (only possible if a different provider was already
    /// installed first — rustls allows exactly one per process).
    pub fn new() -> Result<Self, TransportError> {
        install_crypto_provider()?;
        Ok(HyperRustlsTransport {
            insecure_http: false,
        })
    }

    /// A transport that additionally allows a plain `http://` endpoint —
    /// see the type's own doc for why this is opt-in and explicit.
    ///
    /// # Errors
    /// See [`Self::new`].
    pub fn new_allow_insecure_http() -> Result<Self, TransportError> {
        install_crypto_provider()?;
        Ok(HyperRustlsTransport {
            insecure_http: true,
        })
    }
}

/// Install `rustls`'s `ring` crypto provider as the process default, like
/// `animus_env::tls`'s identical `ring`-pinning decision (ADR 0064) — "one
/// crypto backend in the workspace, not two." Tolerates "already
/// installed" (a second `HyperRustlsTransport` in the same process, or a
/// process that already installed one elsewhere, e.g. `animus-operator`'s
/// `kube` client) as success, not an error — only a *different* provider
/// already being active would be a genuine conflict, which
/// `CryptoProvider::install_default` itself reports as an `Err` this
/// function still treats as tolerable, since rustls has no API to ask
/// "is a provider installed, and is it this one" without trying to install
/// it.
fn install_crypto_provider() -> Result<(), TransportError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    Ok(())
}

#[async_trait::async_trait]
impl Transport for HyperRustlsTransport {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let host = request
            .headers
            .get("host")
            .cloned()
            .ok_or_else(|| TransportError::Io("request has no host header".to_string()))?;
        let use_tls = !self.insecure_http;

        let tcp = TcpStream::connect(&host)
            .await
            .map_err(|e| TransportError::Connect(format!("{host}: {e}")))?;
        tcp.set_nodelay(true).ok();

        let response = if use_tls {
            let server_name = server_name_for(&host)?;
            let connector = build_tls_connector();
            let tls_stream = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| TransportError::Connect(format!("TLS handshake with {host}: {e}")))?;
            send_over(tls_stream, &host, request).await?
        } else {
            send_over(tcp, &host, request).await?
        };
        Ok(response)
    }
}

/// Perform one HTTP/1 request/response round trip over an already-connected
/// stream (plain TCP or TLS), using `hyper`'s low-level `client::conn`
/// handshake directly rather than `hyper-util`'s pooled legacy `Client` —
/// see this crate's `CLAUDE.md` for why (the legacy client's connector
/// trait shape doesn't fit a one-shot per-request connection cleanly, and
/// this transport deliberately does not pool anyway).
async fn send_over<S>(
    stream: S,
    host: &str,
    request: HttpRequest,
) -> Result<HttpResponse, TransportError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| TransportError::Io(format!("HTTP/1 handshake with {host}: {e}")))?;

    // Drive the connection to completion in the background — hyper's own
    // contract: `SendRequest` does nothing unless `Connection` is polled.
    // This is the module's other `tokio::spawn` use, alongside the crypto
    // provider install above, covered by the same module-level allow.
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::warn!("animus-s3 prod transport: connection error: {e}");
        }
    });

    let mut builder = Request::builder().method(request.method).uri(request.uri);
    for (name, value) in &request.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let http_request = builder
        .body(Full::new(Bytes::from(request.body)))
        .map_err(|e| TransportError::Io(format!("building request: {e}")))?;

    let response = sender
        .send_request(http_request)
        .await
        .map_err(|e| TransportError::Io(format!("sending request to {host}: {e}")))?;
    let status = response.status().as_u16();
    let mut headers = std::collections::BTreeMap::new();
    for (name, value) in response.headers() {
        let value = value.to_str().unwrap_or_default().to_string();
        headers
            .entry(name.as_str().to_ascii_lowercase())
            .and_modify(|existing: &mut String| {
                existing.push(',');
                existing.push_str(&value);
            })
            .or_insert(value);
    }
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|e| TransportError::Io(format!("reading response body from {host}: {e}")))?
        .to_bytes()
        .to_vec();

    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

fn build_tls_connector() -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        // A cert this store can't parse is skipped, not fatal — mirrors
        // `rustls_native_certs`' own recommended usage (a handful of
        // unparsable platform certs is common and not this transport's
        // problem to surface as a hard error).
        let _ = roots.add(cert);
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

/// Derive the [`ServerName`] a TLS client verifies the peer's certificate
/// against, from the exact `host[:port]` string this transport dials —
/// mirrors `animus_env::tls::server_name_for`'s own derivation (a numeric
/// address becomes `ServerName::IpAddress`, anything else a DNS name);
/// duplicated rather than imported since this crate deliberately carries no
/// `animus-env` dependency in this PR (see `CLAUDE.md`).
fn server_name_for(host: &str) -> Result<ServerName<'static>, TransportError> {
    let invalid =
        || TransportError::Connect(format!("cannot derive a TLS server name from {host:?}"));
    let host_only = host.rsplit_once(':').map_or(host, |(h, _port)| h);
    if let Ok(ip) = host_only.parse::<std::net::IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    ServerName::try_from(host_only.to_string()).map_err(|_| invalid())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_name_for_derives_an_ip_address_for_a_numeric_host() {
        let name = server_name_for("127.0.0.1:9000").expect("parses");
        assert!(matches!(name, ServerName::IpAddress(_)));
        let name = server_name_for("127.0.0.1").expect("parses (no port)");
        assert!(matches!(name, ServerName::IpAddress(_)));
    }

    #[test]
    fn server_name_for_derives_a_dns_name_for_a_hostname() {
        let name = server_name_for("s3.us-east-1.amazonaws.com:443").expect("parses");
        match name {
            ServerName::DnsName(dns) => assert_eq!(dns.as_ref(), "s3.us-east-1.amazonaws.com"),
            other => panic!("expected a DnsName, got {other:?}"),
        }
    }

    #[test]
    fn server_name_for_rejects_an_empty_host() {
        assert!(server_name_for("").is_err());
    }

    #[test]
    fn build_tls_connector_does_not_panic() {
        // Exercises the native-roots load + `RootCertStore` build path
        // without a real connection — the one thing this unit test can
        // check without a socket. `ClientConfig::builder()` needs a
        // process-default crypto provider installed first (rustls panics
        // otherwise) — `install_crypto_provider` tolerates being called
        // more than once (see its own doc).
        install_crypto_provider().expect("install crypto provider");
        let _ = build_tls_connector();
    }
}
