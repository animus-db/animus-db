//! A minimal S3 client (`put`/`get`/`delete`/`head`/`list_objects_v2`) over
//! an explicit [`Transport`] seam, so the client itself stays testable
//! without sockets (S-04 PR 1). No retries here — the `SegmentStore` layer
//! this crate is built for (a future PR) owns retry policy, exactly like
//! `animus_env::SegmentStore`'s own doc describes for its production
//! implementors.
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

use async_trait::async_trait;

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

/// Bucket/endpoint/credential configuration for one [`S3Client`].
///
/// **Path-style addressing only** in this PR (`https://endpoint/bucket/key`)
/// — the ADR 0059 amendment's own design section covers why (MinIO/
/// localstack compatibility with an explicit endpoint); virtual-hosted-style
/// (`https://bucket.endpoint/key`) is a documented, unimplemented option for
/// a later PR, not a correctness gap for real AWS S3 (which accepts
/// path-style too, if with a deprecation notice for `us-east-1`-only
/// virtual buckets that doesn't apply to a bucket created with an explicit
/// region).
#[derive(Debug, Clone)]
pub struct S3Config {
    /// `scheme://host[:port]`, no trailing slash — e.g.
    /// `"https://s3.us-east-1.amazonaws.com"` or `"http://127.0.0.1:9000"`.
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub credentials: Credentials,
}

/// A minimal S3 client over an explicit [`Transport`] — see the module doc.
pub struct S3Client<T: Transport> {
    transport: T,
    config: S3Config,
}

impl<T: Transport> S3Client<T> {
    #[must_use]
    pub fn new(transport: T, config: S3Config) -> Self {
        S3Client { transport, config }
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
        let path = format!("/{}", self.config.bucket);
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

    fn map_error(&self, resp: HttpResponse) -> S3Error {
        let body = String::from_utf8_lossy(&resp.body);
        if let Some(err) = xml::parse_error(&body) {
            return match err.code.as_str() {
                "NoSuchKey" | "NoSuchBucket" => S3Error::NotFound,
                "AccessDenied" => S3Error::AccessDenied,
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
        let raw_path = format!("/{}/{}", self.config.bucket, key);
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
    /// key/value pairs too (their values must not themselves contain a
    /// literal `&`, the one thing raw-string query construction can't
    /// disambiguate — true of every value this client passes today:
    /// `list-type`'s literal `"2"`, and S3-generated prefixes/continuation
    /// tokens, which never contain `&`).
    async fn execute(
        &self,
        method: &'static str,
        path: &str,
        query_pairs: &[(&str, &str)],
        body: Vec<u8>,
        extra_headers: BTreeMap<String, String>,
        now_epoch_ms: u64,
    ) -> Result<HttpResponse, S3Error> {
        let host = crate::endpoint_host(&self.config.endpoint);
        let raw_query = query_pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        // Both derived from the SAME raw strings, independently, exactly
        // once each — never chained (never fed each other's output back
        // in).
        let wire_path = canonical_uri_s3(path);
        let wire_query = canonical_query_string(&raw_query);

        let payload_hash = PayloadHash::signed(&body);
        let amz_date = format_amz_date((now_epoch_ms / 1000) as i64);

        let req_to_sign = RequestToSign {
            method,
            uri: path,
            host: &host,
            query: &raw_query,
            headers: &extra_headers,
            payload_sha256_hex: &payload_hash,
            timestamp: &amz_date,
        };
        let scope = SigningScope {
            region: self.config.region.clone(),
            service: "s3".to_string(),
        };
        let signed = sign_request(&self.config.credentials, &scope, &req_to_sign);

        let mut headers = extra_headers;
        headers.insert("host".to_string(), host);
        headers.insert("x-amz-date".to_string(), signed.x_amz_date);
        headers.insert(
            "x-amz-content-sha256".to_string(),
            signed.x_amz_content_sha256,
        );
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
        let resp = self.transport.send(request).await?;
        Ok(resp)
    }
}
