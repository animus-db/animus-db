//! A minimal client for `animusd`'s admin/debug HTTP-JSON interface (ADR
//! 0020), used only for the scale-down member-drain sequence
//! (`crate::controller`). Plain HTTP by default — the admin port is
//! cluster-internal only (see the `NetworkPolicy` builder) — with an
//! optional server-only TLS mode (ADR 0064 commit 3) used whenever the
//! target cluster's `spec.tls` is set: `animusd` then serves the admin port
//! over TLS (server-only, ADR 0064 Decision 2), so this client must verify
//! the cluster CA rather than dial plain TCP into a TLS listener.
//!
//! Kept deliberately tiny (a GET and a POST, both JSON) rather than pulling
//! in a general-purpose HTTP client crate: `kube`'s own dependency tree
//! already carries `hyper`/`hyper-util` for talking to the API server, so
//! reusing those crates for this second, unrelated HTTP peer (a pod's admin
//! port, never the API server) avoids adding a second HTTP stack
//! (`reqwest`) for one GET and one POST. The TLS connector below is a small
//! independent equivalent of `animus-cli`'s own `build_tls_connector`
//! (server-only, no client cert — this crate doesn't join the cluster any
//! more than the CLI does) rather than a shared one: this crate deliberately
//! does not depend on `animus-env`/`animus-cli` (see this crate's own
//! `CLAUDE.md`), and `hyper-util`'s legacy `Client` needs its own connector
//! shape (a `tower_service::Service<Uri>`) distinct from either of theirs.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls_pki_types::pem::PemObject;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tower_service::Service;

/// Either a plain TCP stream or a TLS one over TCP — the admin
/// connector's own [`Connection`] response type. `TokioIo<MaybeTlsIo>` is
/// what actually implements `hyper::rt::{Read, Write}`; this type only
/// needs `tokio::io::{AsyncRead, AsyncWrite}` (which `TokioIo`'s blanket
/// impl adapts) plus [`Connection`] itself.
enum MaybeTlsIo {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsIo::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsIo::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsIo::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsIo::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsIo::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsIo::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsIo::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsIo::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

impl Connection for MaybeTlsIo {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

/// A `hyper-util` legacy-client connector that dials plain TCP, optionally
/// upgrading to TLS (server-only — no client certificate) when built with
/// [`AdminConnector::tls`]. One instance is built fresh per call
/// ([`AdminClient::get_json`]/`post_json`) rather than cached — matching
/// this module's existing "cheap, not a hot path" posture (see this file's
/// own doc).
#[derive(Clone)]
struct AdminConnector {
    tls: Option<tokio_rustls::TlsConnector>,
}

impl Service<Uri> for AdminConnector {
    type Response = TokioIo<MaybeTlsIo>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let tls = self.tls.clone();
        Box::pin(async move {
            let host = uri
                .host()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "URI has no host"))?
                .to_string();
            let port = uri
                .port_u16()
                .unwrap_or(if uri.scheme_str() == Some("https") {
                    443
                } else {
                    80
                });
            let stream = TcpStream::connect((host.as_str(), port)).await?;
            match tls {
                None => Ok(TokioIo::new(MaybeTlsIo::Plain(stream))),
                Some(connector) => {
                    let server_name = rustls_pki_types::ServerName::try_from(host)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                    let tls_stream = connector.connect(server_name, stream).await?;
                    Ok(TokioIo::new(MaybeTlsIo::Tls(Box::new(tls_stream))))
                }
            }
        })
    }
}

/// Build a server-only (no client certificate) `rustls` `ClientConfig`
/// trusting exactly the certificates in `ca_pem`, mirroring `animus-cli`'s
/// own `build_tls_connector` (see this file's own doc for why this crate
/// doesn't just reuse that one). Assumes a process-global `CryptoProvider`
/// is already installed (`main.rs::run` installs `ring` before building any
/// `kube::Client`, which happens before any admin call this connector
/// serves).
fn build_tls_connector(ca_pem: &[u8]) -> Result<tokio_rustls::TlsConnector, AdminError> {
    let certs = rustls_pki_types::CertificateDer::pem_slice_iter(ca_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| AdminError::Tls(format!("parsing CA PEM: {e}")))?;
    if certs.is_empty() {
        return Err(AdminError::Tls(
            "no certificates found in CA PEM".to_string(),
        ));
    }
    let mut root_store = rustls::RootCertStore::empty();
    for cert in certs {
        root_store
            .add(cert)
            .map_err(|e| AdminError::Tls(format!("adding CA cert to root store: {e}")))?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .map_err(|e| AdminError::Tls(format!("selecting TLS protocol versions: {e}")))?
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

/// A tiny HTTP(S) JSON client. Stateless (a new connector — and, when TLS
/// is requested, a new `rustls::ClientConfig` — is built per call): cheap,
/// since a pod-admin-port drain sequence is a handful of requests across a
/// whole scale-down, not a hot path.
#[derive(Clone, Copy, Default)]
pub struct AdminClient;

/// Any failure talking to a pod's admin port, or a non-2xx/malformed
/// response.
#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("admin request failed: {0}")]
    Http(#[from] hyper_util::client::legacy::Error),
    #[error("admin response body: {0}")]
    Body(String),
    #[error("admin endpoint returned status {status}: {body}")]
    Status { status: u16, body: String },
    #[error("admin response JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid admin URI: {0}")]
    Uri(#[from] http::uri::InvalidUri),
    #[error("building admin request: {0}")]
    Build(#[from] http::Error),
    #[error("admin TLS: {0}")]
    Tls(String),
}

impl AdminClient {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    async fn read_body(resp: Response<Incoming>) -> Result<(u16, Vec<u8>), AdminError> {
        let status = resp.status().as_u16();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| AdminError::Body(e.to_string()))?
            .to_bytes();
        Ok((status, body.to_vec()))
    }

    async fn send<T: DeserializeOwned>(
        req: Request<Full<Bytes>>,
        ca_pem: Option<&[u8]>,
    ) -> Result<T, AdminError> {
        let tls = ca_pem.map(build_tls_connector).transpose()?;
        let client = Client::builder(TokioExecutor::new()).build(AdminConnector { tls });
        let resp = client.request(req).await?;
        let (status, body) = Self::read_body(resp).await?;
        if !(200..300).contains(&status) {
            return Err(AdminError::Status {
                status,
                body: String::from_utf8_lossy(&body).to_string(),
            });
        }
        Ok(serde_json::from_slice(&body)?)
    }

    /// `GET url`, decoding the body as JSON `T`. Any status outside
    /// `200..300` is [`AdminError::Status`]. `ca_pem` selects TLS (`Some`,
    /// the cluster CA's PEM bytes) or plain TCP (`None`) — `url`'s own
    /// `http`/`https` scheme is not itself consulted; the caller must keep
    /// the two in sync (`crate::controller::admin_base_url` does).
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        url: &str,
        ca_pem: Option<&[u8]>,
    ) -> Result<T, AdminError> {
        let uri: Uri = url.parse()?;
        let req = Request::builder()
            .method("GET")
            .uri(uri)
            .body(Full::new(Bytes::new()))?;
        Self::send(req, ca_pem).await
    }

    /// `POST url` with a JSON body, decoding the response as JSON `T`. See
    /// [`Self::get_json`] for `ca_pem`.
    pub async fn post_json<B: serde::Serialize, T: DeserializeOwned>(
        &self,
        url: &str,
        body: &B,
        ca_pem: Option<&[u8]>,
    ) -> Result<T, AdminError> {
        let uri: Uri = url.parse()?;
        let payload = serde_json::to_vec(body)?;
        let req = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload)))?;
        Self::send(req, ca_pem).await
    }
}

/// The two admin-port calls `crate::controller::drain_and_remove_node`
/// performs, factored out so that sequence can be driven by an in-memory
/// fake in tests (`crate::fakes::FakeAdminClient`, `#[cfg(test)]`) instead
/// of a real socket to a pod's admin port. Errors collapse to `String`
/// (matching what `drain_and_remove_node` already did with [`AdminError`]
/// via `.to_string()` before this seam existed) rather than staying
/// [`AdminError`] — the fake has no HTTP/URI/JSON/TLS errors of its own to
/// report, only "no response queued" and caller-injected failures, so a
/// shared string keeps both implementors' error type identical without an
/// enum neither one fully populates. `ca_pem` (ADR 0064 commit 3): `Some`
/// selects TLS (the cluster CA's PEM bytes, read out of `spec.tls`'s
/// resolved `Secret`), `None` plain TCP — see [`AdminClient::get_json`].
#[async_trait::async_trait]
pub trait AdminOps: Send + Sync {
    /// `POST url` with a JSON body, decoding the response as JSON.
    async fn post_json(
        &self,
        url: &str,
        body: &serde_json::Value,
        ca_pem: Option<&[u8]>,
    ) -> Result<serde_json::Value, String>;
    /// `GET url`, decoding the body as JSON.
    async fn get_json(&self, url: &str, ca_pem: Option<&[u8]>)
    -> Result<serde_json::Value, String>;
}

#[async_trait::async_trait]
impl AdminOps for AdminClient {
    async fn post_json(
        &self,
        url: &str,
        body: &serde_json::Value,
        ca_pem: Option<&[u8]>,
    ) -> Result<serde_json::Value, String> {
        AdminClient::post_json(self, url, body, ca_pem)
            .await
            .map_err(|e| e.to_string())
    }

    async fn get_json(
        &self,
        url: &str,
        ca_pem: Option<&[u8]>,
    ) -> Result<serde_json::Value, String> {
        AdminClient::get_json(self, url, ca_pem)
            .await
            .map_err(|e| e.to_string())
    }
}
