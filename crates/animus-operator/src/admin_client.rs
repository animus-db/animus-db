//! Two ways to reach `animusd`'s admin/debug HTTP-JSON interface (ADR
//! 0020), used for the scale-down member-drain sequence
//! (`crate::controller::drain_and_remove_node`) and, since S-07d, the
//! `spec.controlNodes` growth sequence — both call sites go through the
//! [`AdminOps`] seam and don't care which implementor they get.
//!
//! - [`AdminClient`] (`--admin-access direct`) dials the pod directly —
//!   plain HTTP by default, or server-only TLS (ADR 0064 commit 3,
//!   verifying the cluster CA) whenever `spec.tls` is set. Only reachable
//!   when the operator itself runs **in-cluster**
//!   (`deploy/operator/deployment.yaml`): a pod's headless-`Service` DNS
//!   name and its `10.244.x.x` IP are both cluster-internal-only routes.
//! - [`ProxyAdminClient`] (`--admin-access proxy`, the **default**) goes
//!   through the Kubernetes API server's pod-proxy subresource instead —
//!   `GET/POST /api/v1/namespaces/{ns}/pods/{scheme}:{pod}:{port}/proxy
//!   {path}` — which works from *outside* the cluster network too, since
//!   the only address it dials is the API server itself (already reachable
//!   — this crate already talks to it for every other operation). This is
//!   what makes the documented local-iteration shape (`cargo run -p
//!   animus-operator -- run` against a local kubeconfig,
//!   `scripts/e2e-kind.sh`'s own shape) able to reach a pod's admin port at
//!   all — see ADR 0060's dated amendment and
//!   `docs/engineering-lessons.md` for the failure this closes. **No CA
//!   plumbing needed on this path**: the API server itself dials TLS to
//!   the pod for a `https:` proxy target and does not verify the pod's
//!   serving certificate (Kubernetes' own pod-proxy behavior) — `ca_pem`
//!   is accepted, to keep an identical [`AdminOps`] signature, and ignored.
//!
//! Both implementors enforce [`ADMIN_REQUEST_TIMEOUT`] on every request —
//! an unroutable pod IP/DNS name, or a wedged API-server proxy hop, fails
//! fast instead of hanging a reconcile.
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
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::{TokioExecutor, TokioIo};
use kube::Client as KubeClient;
use rustls_pki_types::pem::PemObject;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tower_service::Service;

/// Bound on one admin-port request (connect through response body, for
/// [`AdminClient`]; the whole API-server round trip, for
/// [`ProxyAdminClient`]) — applied by both implementors so a real-network
/// hang (an unroutable pod IP/DNS name, a dead TLS peer, a wedged proxy
/// hop) fails a reconcile step in seconds, never hangs it. A pod-admin-port
/// call is a local, same-cluster round trip in every deployment shape this
/// crate supports, so a few seconds is generous, not tight.
pub const ADMIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on how much of a non-2xx admin response body [`AdminError::Status`]
/// keeps. That body ends up in a `ClusterCondition.message` (`crate::
/// controller::set_condition`), which is itself bounded by the Kubernetes
/// API server — an admin error is diagnostic text, not a place a large or
/// unbounded payload belongs.
const MAX_ERROR_BODY_BYTES: usize = 2048;

/// Lossily decode `body` as UTF-8, truncated to [`MAX_ERROR_BODY_BYTES`].
fn bounded_body(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    if text.len() <= MAX_ERROR_BODY_BYTES {
        text.into_owned()
    } else {
        let mut truncated = text[..MAX_ERROR_BODY_BYTES].to_string();
        truncated.push_str("...(truncated)");
        truncated
    }
}

/// Race `fut` against [`ADMIN_REQUEST_TIMEOUT`], collapsing an expired
/// race into [`AdminError::Timeout`].
///
/// ADR 0003 / ADR 0061 Decision 4 (rung B5): this crate has no
/// `animus-env` dependency at all (`crates/animus-operator/CLAUDE.md`) — a
/// pod's admin port and the Kubernetes API server are both real network
/// peers reached entirely outside the `Env` seam (no `SimEnv`
/// counterpart), so a real `tokio::time::timeout` is the correct tool
/// here, the same posture `drain_and_remove_node`'s own polling delay
/// already takes.
#[allow(
    clippy::disallowed_methods,
    reason = "animus-operator has no Env seam (see its own CLAUDE.md); bounding a real pod-admin-port/API-server request is outside ADR 0003's scope, like drain_and_remove_node's own polling sleep"
)]
async fn with_admin_timeout<F, T>(fut: F) -> Result<T, AdminError>
where
    F: Future<Output = Result<T, AdminError>>,
{
    match tokio::time::timeout(ADMIN_REQUEST_TIMEOUT, fut).await {
        Ok(result) => result,
        Err(_) => Err(AdminError::Timeout(ADMIN_REQUEST_TIMEOUT)),
    }
}

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
    #[error("admin request timed out after {0:?}")]
    Timeout(Duration),
    #[error("admin request via pod proxy: {0}")]
    Proxy(String),
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
        with_admin_timeout(async move {
            let client = Client::builder(TokioExecutor::new()).build(AdminConnector { tls });
            let resp = client.request(req).await?;
            let (status, body) = Self::read_body(resp).await?;
            if !(200..300).contains(&status) {
                return Err(AdminError::Status {
                    status,
                    body: bounded_body(&body),
                });
            }
            Ok(serde_json::from_slice(&body)?)
        })
        .await
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

/// Parse the `http(s)://{pod}.{svc}.{ns}.svc.cluster.local:{port}{path}`
/// URL `crate::controller::admin_base_url` builds back into the pieces the
/// pod-proxy subresource needs: `(scheme, namespace, pod name, port,
/// path-and-query)`.
///
/// This crate owns both ends of this contract — `admin_base_url` is this
/// URL's *only* producer, and every caller (`drain_and_remove_node`, and
/// S-07d's `spec.controlNodes` growth machinery) already goes through
/// [`AdminOps::get_json`]/`post_json(url, ..)` with exactly this URL
/// shape — so parsing it back apart here keeps that signature, and every
/// existing call site, unchanged: no second, structured target type needs
/// threading through the trait.
fn parse_admin_url(url: &str) -> Result<(String, String, String, u16, String), AdminError> {
    let uri: Uri = url.parse()?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| AdminError::Proxy(format!("admin URL has no scheme: {url}")))?
        .to_string();
    let host = uri
        .host()
        .ok_or_else(|| AdminError::Proxy(format!("admin URL has no host: {url}")))?;
    let port = uri
        .port_u16()
        .ok_or_else(|| AdminError::Proxy(format!("admin URL has no port: {url}")))?;
    // `admin_base_url`'s host is always `{pod}.{internal-svc}.{ns}.svc.
    // cluster.local` — six dot-separated labels, pod name first,
    // namespace third.
    let labels: Vec<&str> = host.split('.').collect();
    let (pod, ns) = match labels.as_slice() {
        [pod, _svc, ns, "svc", "cluster", "local"] => (pod.to_string(), ns.to_string()),
        _ => {
            return Err(AdminError::Proxy(format!(
                "admin URL host is not a pod FQDN this crate produces: {host}"
            )));
        }
    };
    let path = uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    Ok((scheme, ns, pod, port, path))
}

/// Build the Kubernetes API server's pod-proxy-subresource path for one
/// admin-port request: `/api/v1/namespaces/{ns}/pods/{scheme}:{pod}:{port}
/// /proxy{target_path}`. The API server dials the pod itself — TLS, for
/// `scheme == "https"`, verifying nothing about the pod's serving
/// certificate — and forwards its response verbatim, which is what makes
/// this the one dial that works whether the operator runs in-cluster or
/// out-of-cluster (see this module's own doc and ADR 0060's dated
/// amendment).
fn proxy_path(ns: &str, pod: &str, port: u16, scheme: &str, target_path: &str) -> String {
    format!("/api/v1/namespaces/{ns}/pods/{scheme}:{pod}:{port}/proxy{target_path}")
}

/// Map a [`kube::Error`] from a pod-proxy request into [`AdminError`].
/// `kube::Error::Api` carries the *pod's own* response when the proxy
/// target itself answered non-2xx (the API server forwards that status
/// code and body verbatim; `kube-client`'s `handle_api_errors` falls back
/// to wrapping the raw body as `Status.message` whenever it isn't a
/// Kubernetes `Status` object, which an `animusd` admin error body never
/// is) — that case collapses to the same [`AdminError::Status`] the direct
/// client (`AdminClient`) returns for a non-2xx response, so a caller
/// matching on `AdminError` sees one shape regardless of `--admin-access`.
/// Every other `kube::Error` (reaching the API server itself, a malformed
/// proxied response) is a transport failure specific to this path.
fn admin_error_from_kube(e: kube::Error) -> AdminError {
    match e {
        kube::Error::Api(status) => AdminError::Status {
            status: status.code,
            body: bounded_body(status.message.as_bytes()),
        },
        other => AdminError::Proxy(other.to_string()),
    }
}

/// An [`AdminOps`] implementor that reaches a pod's admin port through the
/// Kubernetes API server's pod-proxy subresource instead of dialing the
/// pod directly — the **default** access mode (`AdminAccessMode::Proxy`,
/// `--admin-access proxy`). See this module's own doc for why, and for the
/// "no CA plumbing" note. Cheap to construct (a `kube::Client` clone, not
/// a fresh connection) — held by [`RealAdminClient::Proxy`], never
/// constructed per call the way [`AdminClient::send`]'s own connector is.
#[derive(Clone)]
pub struct ProxyAdminClient {
    client: KubeClient,
}

impl ProxyAdminClient {
    #[must_use]
    pub fn new(client: KubeClient) -> Self {
        Self { client }
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: &str,
        url: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T, AdminError> {
        let (scheme, ns, pod, port, path) = parse_admin_url(url)?;
        let proxy_uri = proxy_path(&ns, &pod, port, &scheme, &path);
        let mut builder = Request::builder().method(method).uri(proxy_uri.as_str());
        if body.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let req = builder.body(body.unwrap_or_default())?;
        let client = self.client.clone();
        with_admin_timeout(async move {
            client
                .request::<T>(req)
                .await
                .map_err(admin_error_from_kube)
        })
        .await
    }

    /// `GET` a pod's admin port via the API server's pod-proxy subresource,
    /// decoding the response as JSON `T`. `_ca_pem` is accepted only to
    /// keep this signature identical to [`AdminClient::get_json`] — see
    /// this module's own "no CA plumbing" note.
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        url: &str,
        _ca_pem: Option<&[u8]>,
    ) -> Result<T, AdminError> {
        self.send("GET", url, None).await
    }

    /// `POST` a JSON body to a pod's admin port via the API server's
    /// pod-proxy subresource, decoding the response as JSON `T`. See
    /// [`Self::get_json`] for `_ca_pem`.
    pub async fn post_json<B: serde::Serialize, T: DeserializeOwned>(
        &self,
        url: &str,
        body: &B,
        _ca_pem: Option<&[u8]>,
    ) -> Result<T, AdminError> {
        let payload = serde_json::to_vec(body)?;
        self.send("POST", url, Some(payload)).await
    }
}

#[async_trait::async_trait]
impl AdminOps for ProxyAdminClient {
    async fn post_json(
        &self,
        url: &str,
        body: &serde_json::Value,
        ca_pem: Option<&[u8]>,
    ) -> Result<serde_json::Value, String> {
        ProxyAdminClient::post_json(self, url, body, ca_pem)
            .await
            .map_err(|e| e.to_string())
    }

    async fn get_json(
        &self,
        url: &str,
        ca_pem: Option<&[u8]>,
    ) -> Result<serde_json::Value, String> {
        ProxyAdminClient::get_json(self, url, ca_pem)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Which route an admin-port call takes — the `animus-operator run
/// --admin-access {proxy,direct}` flag (`crate::main`), plumbed to
/// [`RealAdminClient::new`]. See this module's own doc for the trade-off;
/// [`Default`] is `Proxy`, matching the flag's own default.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum AdminAccessMode {
    #[default]
    Proxy,
    Direct,
}

impl AdminAccessMode {
    /// Parse the `--admin-access` flag's value. `Err` names the bad value
    /// verbatim so `main.rs` can print a useful usage error.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "proxy" => Ok(Self::Proxy),
            "direct" => Ok(Self::Direct),
            other => Err(format!(
                "invalid --admin-access value {other:?} (expected `proxy` or `direct`)"
            )),
        }
    }
}

/// The production [`AdminOps`] this crate's `run()` actually wires up —
/// one of [`AdminClient`] (direct dial) or [`ProxyAdminClient`] (API-server
/// pod proxy), chosen once at startup by [`AdminAccessMode`] and held for
/// the controller's whole lifetime. An enum, not a `Box<dyn AdminOps>` or a
/// second generic parameter threaded through `Context`/`reconcile`: both
/// variants are cheap to hold and the controller only ever needs exactly
/// one concrete `A: AdminOps` type to monomorphize `Context`/`reconcile`
/// against (`crate::controller::run`), so an enum keeps that single
/// concrete type while still making the runtime choice.
#[derive(Clone)]
pub enum RealAdminClient {
    Direct(AdminClient),
    Proxy(ProxyAdminClient),
}

impl RealAdminClient {
    #[must_use]
    pub fn new(mode: AdminAccessMode, client: KubeClient) -> Self {
        match mode {
            AdminAccessMode::Direct => Self::Direct(AdminClient::new()),
            AdminAccessMode::Proxy => Self::Proxy(ProxyAdminClient::new(client)),
        }
    }
}

#[async_trait::async_trait]
impl AdminOps for RealAdminClient {
    async fn post_json(
        &self,
        url: &str,
        body: &serde_json::Value,
        ca_pem: Option<&[u8]>,
    ) -> Result<serde_json::Value, String> {
        // Fully qualified: both `AdminClient`/`ProxyAdminClient` also carry
        // an inherent `post_json`/`get_json` of the same name returning
        // `Result<T, AdminError>` (generic `T`, used by each other's own
        // callers) — plain method-call syntax would resolve to that
        // inherent method (it wins over a trait method of the same name)
        // rather than the `AdminOps` trait method this match needs.
        match self {
            Self::Direct(c) => AdminOps::post_json(c, url, body, ca_pem).await,
            Self::Proxy(c) => AdminOps::post_json(c, url, body, ca_pem).await,
        }
    }

    async fn get_json(
        &self,
        url: &str,
        ca_pem: Option<&[u8]>,
    ) -> Result<serde_json::Value, String> {
        match self {
            Self::Direct(c) => AdminOps::get_json(c, url, ca_pem).await,
            Self::Proxy(c) => AdminOps::get_json(c, url, ca_pem).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_path_builds_the_documented_pod_proxy_subresource_url() {
        assert_eq!(
            proxy_path("ns1", "demo-0", 14003, "http", "/admin/drain"),
            "/api/v1/namespaces/ns1/pods/http:demo-0:14003/proxy/admin/drain"
        );
    }

    #[test]
    fn proxy_path_carries_an_https_scheme_and_a_query_string_verbatim() {
        assert_eq!(
            proxy_path(
                "ns1",
                "demo-2",
                14003,
                "https",
                "/admin/member/drain-status?node=demo-2"
            ),
            "/api/v1/namespaces/ns1/pods/https:demo-2:14003/proxy\
             /admin/member/drain-status?node=demo-2"
        );
    }

    #[test]
    fn parse_admin_url_recovers_namespace_pod_port_and_path_from_admin_base_url() {
        let url = crate::controller::admin_base_url("demo", "ns1", 2, 14003, false);
        let (scheme, ns, pod, port, path) = parse_admin_url(&format!("{url}/admin/drain")).unwrap();
        assert_eq!(scheme, "http");
        assert_eq!(ns, "ns1");
        assert_eq!(pod, "demo-2");
        assert_eq!(port, 14003);
        assert_eq!(path, "/admin/drain");
    }

    #[test]
    fn parse_admin_url_recovers_https_scheme_over_tls() {
        let url = crate::controller::admin_base_url("demo", "ns1", 0, 14003, true);
        let (scheme, ..) = parse_admin_url(&format!("{url}/admin/config")).unwrap();
        assert_eq!(scheme, "https");
    }

    #[test]
    fn parse_admin_url_rejects_a_host_this_crate_never_produces() {
        let err = parse_admin_url("http://example.com:80/admin/drain").unwrap_err();
        assert!(matches!(err, AdminError::Proxy(_)), "{err:?}");
    }

    #[test]
    fn admin_error_from_kube_api_status_becomes_admin_error_status_with_a_bounded_body() {
        let long_message = "x".repeat(MAX_ERROR_BODY_BYTES + 500);
        let status = kube::core::Status {
            status: None,
            code: 503,
            message: long_message,
            reason: String::new(),
            metadata: None,
            details: None,
        };
        let err = admin_error_from_kube(kube::Error::Api(status.boxed()));
        match err {
            AdminError::Status { status, body } => {
                assert_eq!(status, 503);
                assert!(body.len() <= MAX_ERROR_BODY_BYTES + "...(truncated)".len());
                assert!(body.ends_with("...(truncated)"));
            }
            other => panic!("expected AdminError::Status, got {other:?}"),
        }
    }

    #[test]
    fn bounded_body_passes_short_bodies_through_unchanged() {
        assert_eq!(bounded_body(b"short error"), "short error");
    }

    #[test]
    fn bounded_body_truncates_long_bodies() {
        let long = vec![b'a'; MAX_ERROR_BODY_BYTES * 2];
        let out = bounded_body(&long);
        assert!(out.len() < long.len());
        assert!(out.ends_with("...(truncated)"));
    }

    #[test]
    fn admin_access_mode_defaults_to_proxy() {
        assert_eq!(AdminAccessMode::default(), AdminAccessMode::Proxy);
    }

    #[test]
    fn admin_access_mode_parses_both_values_and_rejects_anything_else() {
        assert_eq!(AdminAccessMode::parse("proxy"), Ok(AdminAccessMode::Proxy));
        assert_eq!(
            AdminAccessMode::parse("direct"),
            Ok(AdminAccessMode::Direct)
        );
        assert!(AdminAccessMode::parse("bogus").is_err());
    }

    #[test]
    fn admin_request_timeout_is_a_few_seconds_not_unbounded() {
        // A regression guard, not a tight bound: this only needs to stay a
        // real, short bound — see this module's own doc for why.
        assert!(ADMIN_REQUEST_TIMEOUT >= Duration::from_secs(1));
        assert!(ADMIN_REQUEST_TIMEOUT <= Duration::from_secs(60));
    }
}
