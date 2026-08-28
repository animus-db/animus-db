//! A minimal client for `animusd`'s admin/debug HTTP-JSON interface (ADR
//! 0020), used only for the scale-down member-drain sequence
//! (`crate::controller`). Plain HTTP, no TLS — the admin port is
//! cluster-internal only (see the `NetworkPolicy` builder), matching
//! `animusd` itself, which serves no TLS on any port.
//!
//! Kept deliberately tiny (a GET and a POST, both JSON) rather than pulling
//! in a general-purpose HTTP client crate: `kube`'s own dependency tree
//! already carries `hyper`/`hyper-util` for talking to the API server, so
//! reusing those crates for this second, unrelated HTTP peer (a pod's admin
//! port, never the API server) avoids adding a second HTTP stack
//! (`reqwest`) for one GET and one POST.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::de::DeserializeOwned;

/// A tiny plain-HTTP JSON client, one per reconcile call (cheap: no TLS
/// handshake state to amortize, and a pod-admin-port drain sequence is a
/// handful of requests across a whole scale-down, not a hot path).
#[derive(Clone)]
pub struct AdminClient {
    inner: Client<HttpConnector, Full<Bytes>>,
}

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
}

impl Default for AdminClient {
    fn default() -> Self {
        Self::new()
    }
}

impl AdminClient {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Client::builder(TokioExecutor::new()).build_http(),
        }
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

    async fn send<T: DeserializeOwned>(&self, req: Request<Full<Bytes>>) -> Result<T, AdminError> {
        let resp = self.inner.request(req).await?;
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
    /// `200..300` is [`AdminError::Status`].
    pub async fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T, AdminError> {
        let uri: Uri = url.parse()?;
        let req = Request::builder()
            .method("GET")
            .uri(uri)
            .body(Full::new(Bytes::new()))?;
        self.send(req).await
    }

    /// `POST url` with a JSON body, decoding the response as JSON `T`.
    pub async fn post_json<B: serde::Serialize, T: DeserializeOwned>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T, AdminError> {
        let uri: Uri = url.parse()?;
        let payload = serde_json::to_vec(body)?;
        let req = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload)))?;
        self.send(req).await
    }
}
