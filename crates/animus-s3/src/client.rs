//! A minimal S3 client (`put`/`get`/`delete`/`head`/`list_objects_v2`) over
//! an explicit [`Transport`] seam, so the client itself stays testable
//! without sockets (S-04 PR 1). No retries here — `animus_env::
//! S3SegmentStore` owns retry policy (backoff and jitter drawn from the
//! `Env` seam).
//!
//! # Why every method takes `now_epoch_ms`
//!
//! This crate deliberately carries no `animus-env` dependency in this PR
//! (see `CLAUDE.md`) — but SigV4 signing needs a real timestamp for every
//! request. Rather than reach for a clock this crate has no seam for, every
//! [`S3Client`] method takes `now_epoch_ms: u64` as a plain parameter,
//! mirroring the same "now is passed in, never read" convention
//! `animus_dynamo::ttl`/`animus_dynamo::sigv4` already use. A future
//! `SegmentStore` wrapper (PR 2, which *does* depend on `animus-env`) reads
//! `env.wall_now()` (ADR 0051 — never `env.now()`, which carries no
//! calendar meaning) and passes the result straight through.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::creds::{CredentialProvider, StaticProvider};
use crate::sigv4::{
    Credentials, PayloadHash, RequestToSign, SigningScope, canonical_query_string,
    canonical_uri_s3, format_amz_date, sign_request,
};
use crate::xml;

/// One HTTP request, as [`Transport`] moves it — owned bytes, no
/// connection/socket type in this signature at all.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: &'static str,
    /// Path + (already percent-encoded, sorted) query string, e.g.
    /// `"/bucket/key?list-type=2&prefix=x"`. No scheme/host — those ride as
    /// the `host` header and whatever the transport dials.
    pub uri: String,
    /// Lowercased header name -> value.
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

/// One HTTP response, as [`Transport`] returns it.
#[derive(Debug, Clone, Default)]
pub struct HttpResponse {
    pub status: u16,
    /// Lowercased header name -> value.
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

/// A transport-level failure — the request never got a well-formed HTTP
/// response at all (as opposed to [`S3Error::Service`], a real S3 error
/// response).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("connect failed: {0}")]
    Connect(String),
    #[error("i/o error: {0}")]
    Io(String),
    /// The transport gave up waiting (connect or whole-request deadline).
    /// Retryable, like every transport-level failure.
    #[error("timed out: {0}")]
    Timeout(String),
}

/// The seam a real socket sits behind: a minimal async
/// `send(request) -> response`, so [`S3Client`] is testable without a real
/// connection at all (see `crate::fake`) and the `prod`-gated
/// `crate::prod::HyperRustlsTransport` is the only implementor that touches
/// a socket.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError>;
}

#[async_trait]
impl<T: Transport + ?Sized> Transport for Arc<T> {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        (**self).send(request).await
    }
}

/// An S3 operation's outcome, distinguishing the shapes a caller actually
/// needs to branch on.
#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    /// The object (or bucket, for a `list`) does not exist —
    /// `NoSuchKey`/`NoSuchBucket`/a bare HTTP 404 with no parseable body.
    #[error("object not found")]
    NotFound,
    /// `AccessDenied`/HTTP 403.
    #[error("access denied")]
    AccessDenied,
    /// The transport itself failed — no HTTP response at all.
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
    /// S3 rejected the credentials as expired/invalid (`ExpiredToken`,
    /// `InvalidToken`, `TokenRefreshRequired`) **even after** one forced
    /// credential refresh and retry.
    #[error("credentials expired or invalid (still rejected after a refresh)")]
    CredentialsExpired,
    /// A credential provider failed in a way that is not an HTTP error
    /// response (unreadable token file, malformed credentials document).
    /// Never carries a secret or token.
    #[error("credential provider error: {0}")]
    Credentials(String),
    /// The client configuration is invalid (e.g. virtual-hosted addressing
    /// against an IP endpoint).
    #[error("invalid S3 configuration: {0}")]
    InvalidConfig(String),
    /// Any other S3 error response.
    #[error("S3 error {status} {code}: {message}")]
    Service {
        code: String,
        message: String,
        status: u16,
    },
}

/// A hosted object's size, from a `HeadObject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
}

/// One `ListObjectsV2` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSummary {
    pub key: String,
    pub size: u64,
}

/// One page of a `ListObjectsV2` listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListObjectsPage {
    pub objects: Vec<ObjectSummary>,
    /// `Some` iff the listing is truncated — pass this back as the next
    /// call's `continuation` to page further.
    pub next_continuation_token: Option<String>,
}

/// How a bucket is addressed in the request URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Addressing {
    /// `https://endpoint/bucket/key` — the default; what MinIO/RustFS/
    /// localstack and an explicit `--endpoint` want.
    #[default]
    Path,
    /// `https://bucket.endpoint/key` — AWS's preferred style. Needs a DNS
    /// endpoint (not an IP) and a DNS-compatible bucket name; see
    /// [`validate_virtual_hosted`].
    VirtualHosted,
}

/// Whether `bucket` can be a virtual-hosted label: 3-63 chars of
/// `[a-z0-9-]`, starting and ending alphanumeric. Dots are rejected too — a
/// dotted bucket under HTTPS breaks the `*.endpoint` wildcard certificate.
///
/// # Errors
/// A human-readable reason.
pub fn validate_virtual_hosted(endpoint: &str, bucket: &str) -> Result<(), String> {
    let host = crate::endpoint_host(endpoint);
    let host_only = if let Some(rest) = host.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        host.rsplit_once(':').map_or(host.as_str(), |(h, _)| h)
    };
    if host_only.is_empty() {
        return Err(format!("endpoint {endpoint:?} has no host"));
    }
    if host.starts_with('[') || host_only.parse::<std::net::IpAddr>().is_ok() {
        return Err(format!(
            "virtual-hosted addressing needs a DNS endpoint, but {endpoint:?} is an IP address              (use path-style addressing for it)"
        ));
    }
    let ok_chars = bucket
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    let ends_ok = bucket
        .bytes()
        .next()
        .zip(bucket.bytes().last())
        .is_some_and(|(f, l)| f.is_ascii_alphanumeric() && l.is_ascii_alphanumeric());
    if !(3..=63).contains(&bucket.len()) || !ok_chars || !ends_ok {
        return Err(format!(
            "bucket name {bucket:?} is not DNS-compatible for virtual-hosted addressing              (3-63 chars of lowercase letters, digits and hyphens, starting and ending              alphanumeric)"
        ));
    }
    Ok(())
}

/// Bucket/endpoint/credential configuration for one [`S3Client`].
///
/// Addressing is path-style unless the client is built
/// [`S3Client::with_addressing`]`(`[`Addressing::VirtualHosted`]`)` — kept
/// off this struct on purpose so existing struct-literal callers keep
/// compiling. `credentials` are static; for temporary credentials use
/// [`S3Client::with_provider`].
#[derive(Debug, Clone)]
pub struct S3Config {
    /// `scheme://host[:port]`, no trailing slash — e.g.
    /// `"https://s3.us-east-1.amazonaws.com"` or `"http://127.0.0.1:9000"`.
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub credentials: Credentials,
}

/// The credential-free part of an [`S3Config`] — what
/// [`S3Client::with_provider`] takes alongside a [`CredentialProvider`].
#[derive(Debug, Clone)]
pub struct S3Target {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
}

/// A minimal S3 client over an explicit [`Transport`] — see the module doc.
pub struct S3Client<T: Transport> {
    transport: T,
    target: S3Target,
    addressing: Addressing,
    provider: Arc<dyn CredentialProvider>,
}

/// S3 error codes meaning "the credentials are stale": force a refresh and
/// retry once.
fn is_expired_token_code(code: &str) -> bool {
    matches!(
        code,
        "ExpiredToken" | "ExpiredTokenException" | "InvalidToken" | "TokenRefreshRequired"
    )
}

impl<T: Transport> S3Client<T> {
    /// A client with static credentials (`config.credentials`), path-style.
    #[must_use]
    pub fn new(transport: T, config: S3Config) -> Self {
        let provider = Arc::new(StaticProvider::new(config.credentials));
        S3Client {
            transport,
            target: S3Target {
                endpoint: config.endpoint,
                bucket: config.bucket,
                region: config.region,
            },
            addressing: Addressing::Path,
            provider,
        }
    }

    /// A client whose credentials come from `provider` (path-style; chain
    /// [`Self::with_addressing`] for virtual-hosted).
    #[must_use]
    pub fn with_provider(
        transport: T,
        target: S3Target,
        provider: Arc<dyn CredentialProvider>,
    ) -> Self {
        S3Client {
            transport,
            target,
            addressing: Addressing::Path,
            provider,
        }
    }

    /// Select the addressing style.
    ///
    /// # Errors
    /// [`S3Error::InvalidConfig`] when `VirtualHosted` is asked for against
    /// an IP endpoint or a non-DNS-compatible bucket name.
    pub fn with_addressing(mut self, addressing: Addressing) -> Result<Self, S3Error> {
        if addressing == Addressing::VirtualHosted {
            validate_virtual_hosted(&self.target.endpoint, &self.target.bucket)
                .map_err(S3Error::InvalidConfig)?;
        }
        self.addressing = addressing;
        Ok(self)
    }

    /// The `Host` header (and TLS server name): `bucket.host` when
    /// virtual-hosted, else the endpoint's `host[:port]`.
    fn host(&self) -> String {
        let h = crate::endpoint_host(&self.target.endpoint);
        match self.addressing {
            Addressing::Path => h,
            Addressing::VirtualHosted => format!("{}.{h}", self.target.bucket),
        }
    }

    /// Raw (unescaped) path of the bucket root.
    fn bucket_path(&self) -> String {
        match self.addressing {
            Addressing::Path => format!("/{}", self.target.bucket),
            Addressing::VirtualHosted => "/".to_string(),
        }
    }

    /// `PUT /{bucket}/{key}` with `body` — write-once at the `Transport`
    /// level is not enforced here (that's `SegmentStore`'s contract, a
    /// later layer); this is a plain overwrite-on-conflict PUT, matching
    /// real S3's own default (no conditional-write header sent).
    pub async fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        now_epoch_ms: u64,
    ) -> Result<(), S3Error> {
        let resp = self
            .execute_object("PUT", key, &[], body, now_epoch_ms)
            .await?;
        match resp.status {
            200..=299 => Ok(()),
            _ => Err(self.map_error(resp)),
        }
    }

    /// `GET /{bucket}/{key}`.
    pub async fn get_object(&self, key: &str, now_epoch_ms: u64) -> Result<Vec<u8>, S3Error> {
        let resp = self
            .execute_object("GET", key, &[], Vec::new(), now_epoch_ms)
            .await?;
        match resp.status {
            200..=299 => Ok(resp.body),
            404 => Err(S3Error::NotFound),
            _ => Err(self.map_error(resp)),
        }
    }

    /// `DELETE /{bucket}/{key}` — idempotent, like real S3: deleting an
    /// already-absent key is `Ok`, not [`S3Error::NotFound`].
    pub async fn delete_object(&self, key: &str, now_epoch_ms: u64) -> Result<(), S3Error> {
        let resp = self
            .execute_object("DELETE", key, &[], Vec::new(), now_epoch_ms)
            .await?;
        match resp.status {
            200..=299 | 404 => Ok(()),
            _ => Err(self.map_error(resp)),
        }
    }

    /// `HEAD /{bucket}/{key}` — an S3 `HEAD` error response never carries a
    /// body (the S3 spec's own rule), so this maps by status code alone
    /// rather than trying to parse an XML error body.
    pub async fn head_object(&self, key: &str, now_epoch_ms: u64) -> Result<ObjectMeta, S3Error> {
        let resp = self
            .execute_object("HEAD", key, &[], Vec::new(), now_epoch_ms)
            .await?;
        match resp.status {
            200..=299 => {
                let size = resp
                    .headers
                    .get("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                Ok(ObjectMeta { size })
            }
            404 => Err(S3Error::NotFound),
            403 => Err(S3Error::AccessDenied),
            status => Err(S3Error::Service {
                code: status.to_string(),
                message: "HEAD request failed with no body to describe why".to_string(),
                status,
            }),
        }
    }

    /// `GET /{bucket}?list-type=2&prefix=...&continuation-token=...`.
    pub async fn list_objects_v2(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        now_epoch_ms: u64,
    ) -> Result<ListObjectsPage, S3Error> {
        let mut query_pairs: Vec<(&str, &str)> = vec![("list-type", "2")];
        if !prefix.is_empty() {
            query_pairs.push(("prefix", prefix));
        }
        if let Some(token) = continuation {
            query_pairs.push(("continuation-token", token));
        }
        let path = self.bucket_path();
        let resp = self
            .execute(
                "GET",
                &path,
                &query_pairs,
                Vec::new(),
                BTreeMap::new(),
                now_epoch_ms,
            )
            .await?;
        match resp.status {
            200..=299 => {
                let body = String::from_utf8_lossy(&resp.body);
                let parsed = xml::parse_list_objects_v2(&body);
                Ok(ListObjectsPage {
                    objects: parsed
                        .contents
                        .into_iter()
                        .map(|c| ObjectSummary {
                            key: c.key,
                            size: c.size,
                        })
                        .collect(),
                    next_continuation_token: if parsed.is_truncated {
                        parsed.next_continuation_token
                    } else {
                        None
                    },
                })
            }
            _ => Err(self.map_error(resp)),
        }
    }

    /// `POST /{bucket}/{key}?uploads` — start a multipart upload and return
    /// its upload id (S-08 M2).
    pub async fn create_multipart_upload(
        &self,
        key: &str,
        now_epoch_ms: u64,
    ) -> Result<String, S3Error> {
        let resp = self
            .execute_object("POST", key, &[("uploads", "")], Vec::new(), now_epoch_ms)
            .await?;
        match resp.status {
            200..=299 => xml::parse_initiate_multipart(&String::from_utf8_lossy(&resp.body))
                .ok_or_else(|| S3Error::Service {
                    code: "MalformedResponse".to_string(),
                    message: "CreateMultipartUpload response carried no UploadId".to_string(),
                    status: resp.status,
                }),
            _ => Err(self.map_error(resp)),
        }
    }

    /// `PUT /{bucket}/{key}?partNumber=N&uploadId=ID` — upload one part and
    /// return its `ETag` (the quoted header value, verbatim) for the final
    /// [`Self::complete_multipart_upload`]. `part_number` is `1..=10000`.
    pub async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: Vec<u8>,
        now_epoch_ms: u64,
    ) -> Result<String, S3Error> {
        let pn = part_number.to_string();
        let resp = self
            .execute_object(
                "PUT",
                key,
                &[("partNumber", pn.as_str()), ("uploadId", upload_id)],
                body,
                now_epoch_ms,
            )
            .await?;
        match resp.status {
            200..=299 => resp
                .headers
                .get("etag")
                .cloned()
                .ok_or_else(|| S3Error::Service {
                    code: "MalformedResponse".to_string(),
                    message: "UploadPart response carried no ETag header".to_string(),
                    status: resp.status,
                }),
            _ => Err(self.map_error(resp)),
        }
    }

    /// `POST /{bucket}/{key}?uploadId=ID` with the ordered part list. S3 can
    /// answer `200` and then fail the assembly with an `<Error>` document in
    /// the body; that is a **failure** here, not a success.
    pub async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
        now_epoch_ms: u64,
    ) -> Result<(), S3Error> {
        let mut body = String::from("<CompleteMultipartUpload>");
        for (n, etag) in parts {
            body.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>{}</ETag></Part>",
                xml::xml_escape(etag)
            ));
        }
        body.push_str("</CompleteMultipartUpload>");
        let resp = self
            .execute_object(
                "POST",
                key,
                &[("uploadId", upload_id)],
                body.into_bytes(),
                now_epoch_ms,
            )
            .await?;
        match resp.status {
            200..=299 => {
                match xml::parse_complete_multipart(&String::from_utf8_lossy(&resp.body)) {
                    Ok(()) => Ok(()),
                    Err(e) => Err(match e.code.as_str() {
                        c if is_expired_token_code(c) => S3Error::CredentialsExpired,
                        // S3 documents an in-200 InternalError/SlowDown as
                        // retryable: surface it as the 5xx it stands for.
                        "InternalError" | "SlowDown" | "ServiceUnavailable" => S3Error::Service {
                            code: e.code,
                            message: e.message,
                            status: 500,
                        },
                        _ => S3Error::Service {
                            code: e.code,
                            message: e.message,
                            status: resp.status,
                        },
                    }),
                }
            }
            _ => Err(self.map_error(resp)),
        }
    }

    /// `DELETE /{bucket}/{key}?uploadId=ID` — abort a multipart upload.
    /// Idempotent: an already-gone upload (`404 NoSuchUpload`) is `Ok`.
    pub async fn abort_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        now_epoch_ms: u64,
    ) -> Result<(), S3Error> {
        let resp = self
            .execute_object(
                "DELETE",
                key,
                &[("uploadId", upload_id)],
                Vec::new(),
                now_epoch_ms,
            )
            .await?;
        match resp.status {
            200..=299 | 404 => Ok(()),
            _ => Err(self.map_error(resp)),
        }
    }

    /// `GET /{bucket}/{key}` with `Range: bytes=start-(start+len-1)`;
    /// expects `206`. A range running past the end of the object is
    /// truncated by S3, so the result may be shorter than `len` (the final
    /// slice); a `start` at or past the end is `416` and surfaces as
    /// [`S3Error::Service`] with `status: 416`. A `200` (server ignored the
    /// range) is refused rather than silently returning the whole object.
    pub async fn get_object_range(
        &self,
        key: &str,
        start: u64,
        len: u64,
        now_epoch_ms: u64,
    ) -> Result<Vec<u8>, S3Error> {
        if len == 0 {
            return Err(S3Error::InvalidConfig(
                "get_object_range needs len >= 1".to_string(),
            ));
        }
        let end = start.saturating_add(len - 1);
        let mut headers = BTreeMap::new();
        headers.insert("range".to_string(), format!("bytes={start}-{end}"));
        let raw_path = match self.addressing {
            Addressing::Path => format!("/{}/{}", self.target.bucket, key),
            Addressing::VirtualHosted => format!("/{key}"),
        };
        let resp = self
            .execute("GET", &raw_path, &[], Vec::new(), headers, now_epoch_ms)
            .await?;
        match resp.status {
            206 => Ok(resp.body),
            404 => Err(S3Error::NotFound),
            200..=299 => Err(S3Error::Service {
                code: "RangeIgnored".to_string(),
                message: format!("server answered {} to a ranged GET", resp.status),
                status: resp.status,
            }),
            _ => Err(self.map_error(resp)),
        }
    }

    fn map_error(&self, resp: HttpResponse) -> S3Error {
        let body = String::from_utf8_lossy(&resp.body);
        if let Some(err) = xml::parse_error(&body) {
            return match err.code.as_str() {
                "NoSuchKey" | "NoSuchBucket" => S3Error::NotFound,
                "AccessDenied" => S3Error::AccessDenied,
                c if is_expired_token_code(c) => S3Error::CredentialsExpired,
                _ => S3Error::Service {
                    code: err.code,
                    message: err.message,
                    status: resp.status,
                },
            };
        }
        match resp.status {
            404 => S3Error::NotFound,
            403 => S3Error::AccessDenied,
            status => S3Error::Service {
                code: status.to_string(),
                message: "no parseable error body".to_string(),
                status,
            },
        }
    }

    async fn execute_object(
        &self,
        method: &'static str,
        key: &str,
        extra_query: &[(&str, &str)],
        body: Vec<u8>,
        now_epoch_ms: u64,
    ) -> Result<HttpResponse, S3Error> {
        // RAW (unescaped) path — `execute` percent-encodes it exactly once,
        // for both the wire URI and the signature, from this same string.
        // Building an already-percent-encoded path here and handing it to
        // `canonical_uri_s3` a second time would double-encode it (e.g. a
        // key containing a space would sign as `%20` but re-encode to
        // `%2520` on the wire) — see `sigv4`'s module doc for the general
        // "encode exactly once, from the same raw input, for each purpose"
        // rule this crate follows throughout.
        let raw_path = match self.addressing {
            Addressing::Path => format!("/{}/{}", self.target.bucket, key),
            Addressing::VirtualHosted => format!("/{key}"),
        };
        self.execute(
            method,
            &raw_path,
            extra_query,
            body,
            BTreeMap::new(),
            now_epoch_ms,
        )
        .await
    }

    /// `path` is the RAW (unescaped) request path — see
    /// [`Self::execute_object`]'s doc for why. `query_pairs` are RAW
    /// (unescaped) key/value pairs, handed straight to
    /// [`canonical_query_string`]/[`RequestToSign::query`] as typed pairs
    /// rather than joined into a `"k=v&k=v"` string first — a value may
    /// freely contain its own literal `&`, `=`, space, or any other byte
    /// (an `S3KeyPrefix` value is customer-controlled and S3 explicitly
    /// permits `&` in an object key). Joining raw pairs with `&` and later
    /// re-splitting on it, as an earlier version of this method did, is
    /// exactly the bug issue #855 fixed: a value containing a literal `&`
    /// truncated at the first one and fabricated a spurious extra
    /// parameter, self-consistently signed (both the wire query and the
    /// canonical form were re-derived from the same corrupted string) so
    /// nothing detected it — S3 just interpreted the request differently
    /// than intended.
    async fn execute(
        &self,
        method: &'static str,
        path: &str,
        query_pairs: &[(&str, &str)],
        body: Vec<u8>,
        extra_headers: BTreeMap<String, String>,
        now_epoch_ms: u64,
    ) -> Result<HttpResponse, S3Error> {
        let mut creds = self.provider.credentials(now_epoch_ms).await?;
        let mut refreshed = false;
        loop {
            let resp = self
                .send_signed(
                    &creds,
                    method,
                    path,
                    query_pairs,
                    body.clone(),
                    extra_headers.clone(),
                    now_epoch_ms,
                )
                .await?;
            let stale = resp.status >= 400
                && xml::parse_error(&String::from_utf8_lossy(&resp.body))
                    .is_some_and(|e| is_expired_token_code(&e.code));
            if !stale {
                return Ok(resp);
            }
            if refreshed {
                return Err(S3Error::CredentialsExpired);
            }
            // S3 says the credentials are stale: one forced refresh, then
            // retry the identical request once.
            creds = self.provider.refresh(&creds, now_epoch_ms).await?;
            refreshed = true;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_signed(
        &self,
        creds: &Credentials,
        method: &'static str,
        path: &str,
        query_pairs: &[(&str, &str)],
        body: Vec<u8>,
        extra_headers: BTreeMap<String, String>,
        now_epoch_ms: u64,
    ) -> Result<HttpResponse, S3Error> {
        let host = self.host();
        // Both derived from the SAME raw pairs, independently, exactly once
        // each — never chained (never fed each other's output back in), and
        // never routed through an intermediate joined-then-split string.
        let wire_path = canonical_uri_s3(path);
        let wire_query = canonical_query_string(query_pairs);

        let payload_hash = PayloadHash::signed(&body);
        let amz_date = format_amz_date((now_epoch_ms / 1000) as i64);

        let req_to_sign = RequestToSign {
            method,
            uri: path,
            host: &host,
            query: query_pairs,
            headers: &extra_headers,
            payload_sha256_hex: &payload_hash,
            timestamp: &amz_date,
        };
        let scope = SigningScope {
            region: self.target.region.clone(),
            service: "s3".to_string(),
        };
        let signed = sign_request(creds, &scope, &req_to_sign);

        let mut headers = extra_headers;
        headers.insert("host".to_string(), host);
        headers.insert("x-amz-date".to_string(), signed.x_amz_date);
        headers.insert(
            "x-amz-content-sha256".to_string(),
            signed.x_amz_content_sha256,
        );
        if let Some(token) = signed.x_amz_security_token {
            headers.insert("x-amz-security-token".to_string(), token);
        }
        headers.insert("authorization".to_string(), signed.authorization);
        if !body.is_empty() {
            headers.insert("content-length".to_string(), body.len().to_string());
        }

        let uri = if wire_query.is_empty() {
            wire_path
        } else {
            format!("{wire_path}?{wire_query}")
        };
        let request = HttpRequest {
            method,
            uri,
            headers,
            body,
        };
        Ok(self.transport.send(request).await?)
    }
}
