//! Credential providers (S-08 M1): where an [`S3Client`](crate::client::S3Client)
//! gets the [`Credentials`] it signs with. Static keys are the S-04 default;
//! this module adds the temporary-credential sources a real AWS deployment
//! uses — STS `AssumeRoleWithWebIdentity` (EKS IRSA), ECS / EKS Pod Identity
//! container credentials, and EC2 IMDSv2 — behind one [`CredentialProvider`]
//! trait, plus a [`CachingProvider`] that refreshes before expiry.
//!
//! # Purity
//!
//! Everything here is **pure**: no clock, no filesystem, no environment
//! variables, no socket. "Now" is always a `now_epoch_ms` parameter (the same
//! convention as [`crate::client::S3Client`]); HTTP goes through the
//! [`Transport`] seam; the web-identity token and the container
//! authorization token arrive as injected [`TokenSource`] closures. The
//! process-boundary half (reading `AWS_*` environment variables and token
//! files) lives in the `prod`-gated `creds_prod` module.
//!
//! # Secrets
//!
//! Provider errors never include a token, a secret or a response body that
//! could contain one — only status codes and server error codes/messages.
//!
//! # Single flight
//!
//! [`CachingProvider`] serializes refreshes behind a small async gate (no
//! tokio dependency): N concurrent callers racing an expired cache produce
//! exactly one upstream fetch; the rest wait and take the fresh value.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;

use crate::client::{HttpRequest, HttpResponse, S3Error, Transport};
use crate::sigv4::{Credentials, percent_encode};
use crate::xml;

/// Refresh this long before the stated expiry, so a request signed just
/// before the boundary never reaches S3 already expired.
pub const REFRESH_SKEW_MS: u64 = 5 * 60 * 1000;

/// A pure supplier of a secret string (a web-identity JWT, a container
/// authorization token) — a closure so this module stays free of `std::fs`.
/// Called on **every** fetch (these tokens rotate on disk). The `Err` string
/// must not contain the token.
pub type TokenSource = Arc<dyn Fn() -> Result<String, String> + Send + Sync>;

/// Supplies [`Credentials`] to sign with.
#[async_trait]
pub trait CredentialProvider: Send + Sync {
    /// Credentials valid at `now_epoch_ms`. Never reads a clock.
    ///
    /// # Errors
    /// The underlying source failed (transport, STS error, malformed body).
    async fn credentials(&self, now_epoch_ms: u64) -> Result<Credentials, S3Error>;

    /// Force a refresh because S3 rejected `stale` (an `ExpiredToken`-class
    /// error). The default just asks again; [`CachingProvider`] overrides it
    /// so concurrent rejections of the same stale credentials trigger one
    /// upstream fetch.
    ///
    /// # Errors
    /// As [`Self::credentials`].
    async fn refresh(
        &self,
        stale: &Credentials,
        now_epoch_ms: u64,
    ) -> Result<Credentials, S3Error> {
        let _ = stale;
        self.credentials(now_epoch_ms).await
    }
}

/// Fixed long-lived credentials.
pub struct StaticProvider(Credentials);

impl StaticProvider {
    #[must_use]
    pub fn new(credentials: Credentials) -> Self {
        StaticProvider(credentials)
    }
}

#[async_trait]
impl CredentialProvider for StaticProvider {
    async fn credentials(&self, _now_epoch_ms: u64) -> Result<Credentials, S3Error> {
        Ok(self.0.clone())
    }
}

// --- single-flight gate -------------------------------------------------

/// A minimal async mutex *gate* (it guards no data): `lock().await` resolves
/// when no one else holds it. Hand-rolled so the pure crate needs no async
/// runtime. Dropping the guard wakes every waiter, which re-polls.
#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
}

#[derive(Default)]
struct GateState {
    locked: bool,
    waiters: Vec<Waker>,
}

struct GateGuard<'a>(&'a Gate);

struct LockFuture<'a>(&'a Gate);

impl Gate {
    fn lock(&self) -> LockFuture<'_> {
        LockFuture(self)
    }
}

impl<'a> Future for LockFuture<'a> {
    type Output = GateGuard<'a>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut st = self.0.state.lock().expect("gate lock");
        if st.locked {
            st.waiters.push(cx.waker().clone());
            Poll::Pending
        } else {
            st.locked = true;
            Poll::Ready(GateGuard(self.0))
        }
    }
}

impl Drop for GateGuard<'_> {
    fn drop(&mut self) {
        let waiters = {
            let mut st = self.0.state.lock().expect("gate lock");
            st.locked = false;
            std::mem::take(&mut st.waiters)
        };
        for w in waiters {
            w.wake();
        }
    }
}

// --- caching ------------------------------------------------------------

/// Caches an inner provider's credentials and refreshes them
/// [`REFRESH_SKEW_MS`] before they expire — single flight. Credentials with
/// no expiry (static) are cached forever.
pub struct CachingProvider<P: CredentialProvider> {
    inner: P,
    cache: Mutex<Option<Credentials>>,
    gate: Gate,
}

fn needs_refresh(c: &Credentials, now_epoch_ms: u64) -> bool {
    c.expiry_epoch_ms()
        .is_some_and(|exp| now_epoch_ms.saturating_add(REFRESH_SKEW_MS) >= exp)
}

fn still_valid(c: &Credentials, now_epoch_ms: u64) -> bool {
    c.expiry_epoch_ms().is_none_or(|exp| now_epoch_ms < exp)
}

impl<P: CredentialProvider> CachingProvider<P> {
    #[must_use]
    pub fn new(inner: P) -> Self {
        CachingProvider {
            inner,
            cache: Mutex::new(None),
            gate: Gate::default(),
        }
    }

    fn cached(&self) -> Option<Credentials> {
        self.cache.lock().expect("cache lock").clone()
    }

    /// Fetch from the inner provider and store. Called with the gate held.
    /// On failure, falls back to still-unexpired cached credentials (the
    /// refresh window is 5 min wide for exactly this reason).
    async fn fetch(&self, now_epoch_ms: u64) -> Result<Credentials, S3Error> {
        match self.inner.credentials(now_epoch_ms).await {
            Ok(fresh) => {
                *self.cache.lock().expect("cache lock") = Some(fresh.clone());
                Ok(fresh)
            }
            Err(e) => match self.cached() {
                Some(old) if still_valid(&old, now_epoch_ms) => Ok(old),
                _ => Err(e),
            },
        }
    }
}

#[async_trait]
impl<P: CredentialProvider> CredentialProvider for CachingProvider<P> {
    async fn credentials(&self, now_epoch_ms: u64) -> Result<Credentials, S3Error> {
        if let Some(c) = self.cached().filter(|c| !needs_refresh(c, now_epoch_ms)) {
            return Ok(c);
        }
        let _guard = self.gate.lock().await;
        // Someone ahead of us may have refreshed while we waited.
        if let Some(c) = self.cached().filter(|c| !needs_refresh(c, now_epoch_ms)) {
            return Ok(c);
        }
        self.fetch(now_epoch_ms).await
    }

    async fn refresh(
        &self,
        stale: &Credentials,
        now_epoch_ms: u64,
    ) -> Result<Credentials, S3Error> {
        let _guard = self.gate.lock().await;
        // If the cache already holds something other than the rejected
        // credentials, another caller refreshed first — use theirs.
        if let Some(c) = self
            .cached()
            .filter(|c| c != stale && !needs_refresh(c, now_epoch_ms))
        {
            return Ok(c);
        }
        match self.inner.credentials(now_epoch_ms).await {
            Ok(fresh) => {
                *self.cache.lock().expect("cache lock") = Some(fresh.clone());
                Ok(fresh)
            }
            Err(e) => {
                // The rejected credentials must not be served again.
                let mut cache = self.cache.lock().expect("cache lock");
                if cache.as_ref() == Some(stale) {
                    *cache = None;
                }
                Err(e)
            }
        }
    }
}

// --- shared HTTP helpers ------------------------------------------------

/// Split `scheme://host[:port]/path?query` into `(host[:port], path+query)`.
fn split_url(url: &str) -> Result<(String, String), S3Error> {
    let rest = url.split_once("://").map(|(_, r)| r).ok_or_else(|| {
        S3Error::Credentials(format!("credential endpoint {url:?} has no scheme"))
    })?;
    let (host, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if host.is_empty() {
        return Err(S3Error::Credentials(format!(
            "credential endpoint {url:?} has no host"
        )));
    }
    Ok((host.to_string(), path.to_string()))
}

fn request(
    method: &'static str,
    host: &str,
    uri: String,
    mut headers: BTreeMap<String, String>,
    body: Vec<u8>,
) -> HttpRequest {
    headers.insert("host".to_string(), host.to_string());
    if !body.is_empty() {
        headers.insert("content-length".to_string(), body.len().to_string());
    }
    HttpRequest {
        method,
        uri,
        headers,
        body,
    }
}

/// Map a non-2xx credential-endpoint response to an error. Surfaces the
/// server's own `Code`/`Message` if it is an XML error body, else status
/// only — never the raw body (it could echo a token).
fn endpoint_error(what: &str, resp: &HttpResponse) -> S3Error {
    let body = String::from_utf8_lossy(&resp.body);
    if let Some(e) = xml::parse_error(&body) {
        return S3Error::Service {
            code: e.code,
            message: format!("{what}: {}", e.message),
            status: resp.status,
        };
    }
    S3Error::Service {
        code: resp.status.to_string(),
        message: format!("{what}: request failed"),
        status: resp.status,
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct JsonCreds {
    access_key_id: String,
    secret_access_key: String,
    token: String,
    #[serde(default)]
    expiration: Option<String>,
}

fn parse_json_creds(what: &str, body: &[u8]) -> Result<Credentials, S3Error> {
    let parsed: JsonCreds = serde_json::from_slice(body)
        .map_err(|_| S3Error::Credentials(format!("{what}: malformed credentials document")))?;
    let expiry = match parsed.expiration {
        Some(e) => Some(
            xml::parse_iso8601_epoch_ms(&e)
                .ok_or_else(|| S3Error::Credentials(format!("{what}: unparseable Expiration")))?,
        ),
        None => None,
    };
    Ok(
        Credentials::new(parsed.access_key_id, parsed.secret_access_key)
            .with_session_token(parsed.token, expiry),
    )
}

// --- STS AssumeRoleWithWebIdentity --------------------------------------

/// `AssumeRoleWithWebIdentity` (EKS IRSA): exchanges a projected
/// service-account JWT for temporary credentials. The call is **unsigned** —
/// the web-identity token is the authenticator — so no `Authorization`
/// header is ever sent.
pub struct StsWebIdentityProvider<T: Transport> {
    transport: T,
    /// `scheme://host[:port]`, e.g. `https://sts.us-east-1.amazonaws.com`.
    endpoint: String,
    role_arn: String,
    session_name: String,
    token_source: TokenSource,
}

impl<T: Transport> StsWebIdentityProvider<T> {
    #[must_use]
    pub fn new(
        transport: T,
        endpoint: impl Into<String>,
        role_arn: impl Into<String>,
        session_name: impl Into<String>,
        token_source: TokenSource,
    ) -> Self {
        StsWebIdentityProvider {
            transport,
            endpoint: endpoint.into(),
            role_arn: role_arn.into(),
            session_name: session_name.into(),
            token_source,
        }
    }
}

#[async_trait]
impl<T: Transport> CredentialProvider for StsWebIdentityProvider<T> {
    async fn credentials(&self, _now_epoch_ms: u64) -> Result<Credentials, S3Error> {
        let jwt = (self.token_source)()
            .map_err(|e| S3Error::Credentials(format!("reading web identity token: {e}")))?;
        let (host, _) = split_url(&self.endpoint)?;
        let enc = |s: &str| percent_encode(s.as_bytes());
        let body = format!(
            "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={}&RoleSessionName={}&WebIdentityToken={}",
            enc(&self.role_arn),
            enc(&self.session_name),
            enc(jwt.trim())
        )
        .into_bytes();
        let mut headers = BTreeMap::new();
        headers.insert(
            "content-type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        );
        headers.insert("accept".to_string(), "application/xml".to_string());
        let resp = self
            .transport
            .send(request("POST", &host, "/".to_string(), headers, body))
            .await?;
        if !(200..300).contains(&resp.status) {
            return Err(endpoint_error("STS AssumeRoleWithWebIdentity", &resp));
        }
        let text = String::from_utf8_lossy(&resp.body);
        let c = xml::parse_sts_credentials(&text).ok_or_else(|| {
            S3Error::Credentials("STS AssumeRoleWithWebIdentity: malformed response".to_string())
        })?;
        Ok(Credentials::new(c.access_key_id, c.secret_access_key)
            .with_session_token(c.session_token, Some(c.expiration_epoch_ms)))
    }
}

// --- ECS / EKS Pod Identity container credentials -----------------------

/// ECS task-role / EKS Pod Identity credentials: a GET on the
/// `AWS_CONTAINER_CREDENTIALS_FULL_URI` (or relative-URI-resolved) endpoint,
/// optionally carrying `AWS_CONTAINER_AUTHORIZATION_TOKEN` verbatim as the
/// `Authorization` header.
pub struct ContainerProvider<T: Transport> {
    transport: T,
    /// Full URL, e.g. `http://169.254.170.23/v1/credentials`.
    full_uri: String,
    auth_token: Option<TokenSource>,
}

impl<T: Transport> ContainerProvider<T> {
    #[must_use]
    pub fn new(transport: T, full_uri: impl Into<String>, auth_token: Option<TokenSource>) -> Self {
        ContainerProvider {
            transport,
            full_uri: full_uri.into(),
            auth_token,
        }
    }
}

#[async_trait]
impl<T: Transport> CredentialProvider for ContainerProvider<T> {
    async fn credentials(&self, _now_epoch_ms: u64) -> Result<Credentials, S3Error> {
        let (host, path) = split_url(&self.full_uri)?;
        let mut headers = BTreeMap::new();
        if let Some(src) = &self.auth_token {
            let tok = src()
                .map_err(|e| S3Error::Credentials(format!("reading container auth token: {e}")))?;
            headers.insert("authorization".to_string(), tok.trim().to_string());
        }
        let resp = self
            .transport
            .send(request("GET", &host, path, headers, Vec::new()))
            .await?;
        if !(200..300).contains(&resp.status) {
            return Err(endpoint_error("container credentials", &resp));
        }
        parse_json_creds("container credentials", &resp.body)
    }
}

// --- EC2 IMDSv2 ---------------------------------------------------------

/// EC2 instance-profile credentials via IMDSv2 (token-protected; v1
/// fallback is deliberately not implemented).
pub struct ImdsV2Provider<T: Transport> {
    transport: T,
    /// `scheme://host`, normally `http://169.254.169.254`.
    endpoint: String,
}

/// Session-token TTL requested from IMDS (seconds); used once per fetch.
const IMDS_TOKEN_TTL_SECS: &str = "21600";

impl<T: Transport> ImdsV2Provider<T> {
    #[must_use]
    pub fn new(transport: T, endpoint: impl Into<String>) -> Self {
        ImdsV2Provider {
            transport,
            endpoint: endpoint.into(),
        }
    }

    /// The default link-local IMDS endpoint.
    pub const DEFAULT_ENDPOINT: &'static str = "http://169.254.169.254";
}

#[async_trait]
impl<T: Transport> CredentialProvider for ImdsV2Provider<T> {
    async fn credentials(&self, _now_epoch_ms: u64) -> Result<Credentials, S3Error> {
        let (host, _) = split_url(&self.endpoint)?;

        let mut h = BTreeMap::new();
        h.insert(
            "x-aws-ec2-metadata-token-ttl-seconds".to_string(),
            IMDS_TOKEN_TTL_SECS.to_string(),
        );
        let resp = self
            .transport
            .send(request(
                "PUT",
                &host,
                "/latest/api/token".to_string(),
                h,
                Vec::new(),
            ))
            .await?;
        if !(200..300).contains(&resp.status) {
            return Err(endpoint_error("IMDS token", &resp));
        }
        let session = String::from_utf8_lossy(&resp.body).trim().to_string();

        let mut h = BTreeMap::new();
        h.insert("x-aws-ec2-metadata-token".to_string(), session.clone());
        let resp = self
            .transport
            .send(request(
                "GET",
                &host,
                "/latest/meta-data/iam/security-credentials/".to_string(),
                h,
                Vec::new(),
            ))
            .await?;
        if !(200..300).contains(&resp.status) {
            return Err(endpoint_error("IMDS role lookup", &resp));
        }
        let role = String::from_utf8_lossy(&resp.body)
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty())
            .map(str::to_string)
            .ok_or_else(|| S3Error::Credentials("IMDS: no instance role attached".to_string()))?;

        let mut h = BTreeMap::new();
        h.insert("x-aws-ec2-metadata-token".to_string(), session);
        let resp = self
            .transport
            .send(request(
                "GET",
                &host,
                format!(
                    "/latest/meta-data/iam/security-credentials/{}",
                    percent_encode(role.as_bytes())
                ),
                h,
                Vec::new(),
            ))
            .await?;
        if !(200..300).contains(&resp.status) {
            return Err(endpoint_error("IMDS credentials", &resp));
        }
        parse_json_creds("IMDS credentials", &resp.body)
    }
}
