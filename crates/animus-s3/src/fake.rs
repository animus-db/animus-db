//! An in-process fake S3 [`Transport`] (S-04 PR 1) — no sockets, real
//! signature verification. Gated behind `#[cfg(any(test, feature =
//! "fake"))]`: always available to this crate's own tests, and to a
//! downstream crate's tests (a future `SegmentStore` contract test) via the
//! default-off `fake` Cargo feature.
//!
//! Exercises the signer end to end: every request this double receives is
//! verified with [`crate::sigv4::verify_signature`] against its own
//! registered credentials (see [`FakeS3::with_credential`]) before any
//! PUT/GET/DELETE/HEAD/List is served — a request signed with an unknown
//! access key, a wrong secret, or a claimed `x-amz-content-sha256` that
//! doesn't match the actual body bytes is rejected with the same
//! `AccessDenied`/`XAmzContentSHA256Mismatch` shape real S3 would use.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::client::{HttpRequest, HttpResponse, Transport, TransportError};
use crate::sigv4::{self, PayloadHash};

/// An in-memory S3 bucket double. Objects live in a plain `BTreeMap` (so
/// `list_objects_v2` iterates in the same lexicographic key order real S3
/// promises) keyed by object key (never the full `/bucket/key` path).
pub struct FakeS3 {
    bucket: String,
    credentials: Mutex<std::collections::BTreeMap<String, String>>,
    objects: Mutex<std::collections::BTreeMap<String, Vec<u8>>>,
    /// Max objects returned per `ListObjectsV2` page — deliberately
    /// configurable (default 1000, matching real S3's own default) so a
    /// test can force pagination across more than one page without
    /// uploading a thousand objects.
    page_size: usize,
}

impl FakeS3 {
    /// A fresh, empty bucket double named `bucket`, with no registered
    /// credentials — every request is rejected until [`Self::
    /// with_credential`] registers at least one.
    #[must_use]
    pub fn new(bucket: impl Into<String>) -> Self {
        FakeS3 {
            bucket: bucket.into(),
            credentials: Mutex::new(std::collections::BTreeMap::new()),
            objects: Mutex::new(std::collections::BTreeMap::new()),
            page_size: 1000,
        }
    }

    /// Register a credential this double will accept.
    #[must_use]
    pub fn with_credential(
        self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.credentials
            .lock()
            .expect("fake credentials lock")
            .insert(access_key_id.into(), secret_access_key.into());
        self
    }

    /// Cap how many objects one `ListObjectsV2` page returns.
    #[must_use]
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    /// How many objects this double currently holds — a test convenience.
    #[must_use]
    pub fn object_count(&self) -> usize {
        self.objects.lock().expect("fake objects lock").len()
    }

    fn handle(&self, req: HttpRequest) -> HttpResponse {
        let (raw_path_wire, raw_query_wire) = split_uri(&req.uri);
        let path = sigv4::percent_decode(raw_path_wire);
        let query = sigv4::percent_decode(raw_query_wire);

        if let Err(resp) = self.verify(&req, &path, &query) {
            return resp;
        }

        let trimmed = path.trim_start_matches('/');
        let (bucket, key) = match trimmed.split_once('/') {
            Some((b, k)) => (b.to_string(), Some(k.to_string())),
            None => (trimmed.to_string(), None),
        };
        if bucket != self.bucket {
            return error_response(404, "NoSuchBucket", "The specified bucket does not exist");
        }

        let is_list = query.split('&').any(|pair| pair == "list-type=2");
        if is_list {
            let mut prefix = String::new();
            let mut continuation: Option<String> = None;
            for pair in query.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    match k {
                        "prefix" => prefix = v.to_string(),
                        "continuation-token" => continuation = Some(v.to_string()),
                        _ => {}
                    }
                }
            }
            return self.handle_list(&prefix, continuation.as_deref());
        }

        let Some(key) = key.filter(|k| !k.is_empty()) else {
            return error_response(400, "InvalidRequest", "missing object key");
        };
        match req.method {
            "PUT" => self.handle_put(&key, req.body),
            "GET" => self.handle_get(&key),
            "HEAD" => self.handle_head(&key),
            "DELETE" => self.handle_delete(&key),
            other => error_response(
                400,
                "InvalidRequest",
                &format!("unsupported method {other}"),
            ),
        }
    }

    /// Structural + cryptographic verification of `req`, returning the
    /// error response to send back the moment anything fails.
    fn verify(&self, req: &HttpRequest, path: &str, query: &str) -> Result<(), HttpResponse> {
        let auth_header = req.headers.get("authorization").ok_or_else(|| {
            error_response(
                400,
                "MissingAuthenticationTokenException",
                "Request is missing Authentication Token",
            )
        })?;
        let parsed = sigv4::parse_authorization(auth_header)
            .ok_or_else(|| error_response(403, "AccessDenied", "Access Denied"))?;

        let secret = {
            let creds = self.credentials.lock().expect("fake credentials lock");
            creds.get(&parsed.access_key_id).cloned()
        };
        let Some(secret) = secret else {
            return Err(error_response(
                403,
                "InvalidAccessKeyId",
                "The AWS Access Key Id you provided does not exist in our records.",
            ));
        };

        let claimed_hash = req
            .headers
            .get("x-amz-content-sha256")
            .cloned()
            .unwrap_or_default();
        if claimed_hash != "UNSIGNED-PAYLOAD" {
            let actual_hash = PayloadHash::signed(&req.body).as_str().to_string();
            if claimed_hash != actual_hash {
                return Err(error_response(
                    400,
                    "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.",
                ));
            }
        }

        let verified = sigv4::verify_signature(
            &parsed,
            &secret,
            req.method,
            path,
            query,
            &req.headers,
            &claimed_hash,
        );
        if verified {
            Ok(())
        } else {
            Err(error_response(403, "AccessDenied", "Access Denied"))
        }
    }

    fn handle_put(&self, key: &str, body: Vec<u8>) -> HttpResponse {
        self.objects
            .lock()
            .expect("fake objects lock")
            .insert(key.to_string(), body);
        HttpResponse {
            status: 200,
            headers: std::collections::BTreeMap::new(),
            body: Vec::new(),
        }
    }

    fn handle_get(&self, key: &str) -> HttpResponse {
        let objects = self.objects.lock().expect("fake objects lock");
        match objects.get(key) {
            Some(bytes) => HttpResponse {
                status: 200,
                headers: std::collections::BTreeMap::new(),
                body: bytes.clone(),
            },
            None => no_such_key_response(),
        }
    }

    fn handle_head(&self, key: &str) -> HttpResponse {
        let objects = self.objects.lock().expect("fake objects lock");
        match objects.get(key) {
            Some(bytes) => {
                let mut headers = std::collections::BTreeMap::new();
                headers.insert("content-length".to_string(), bytes.len().to_string());
                HttpResponse {
                    status: 200,
                    headers,
                    body: Vec::new(),
                }
            }
            // A real S3 `HEAD` error response never carries a body.
            None => HttpResponse {
                status: 404,
                headers: std::collections::BTreeMap::new(),
                body: Vec::new(),
            },
        }
    }

    fn handle_delete(&self, key: &str) -> HttpResponse {
        self.objects.lock().expect("fake objects lock").remove(key);
        HttpResponse {
            status: 204,
            headers: std::collections::BTreeMap::new(),
            body: Vec::new(),
        }
    }

    fn handle_list(&self, prefix: &str, continuation: Option<&str>) -> HttpResponse {
        let objects = self.objects.lock().expect("fake objects lock");
        let keys: Vec<&String> = objects.keys().filter(|k| k.starts_with(prefix)).collect();
        let start = match continuation {
            Some(token) => keys
                .iter()
                .position(|k| k.as_str() > token)
                .unwrap_or(keys.len()),
            None => 0,
        };
        let remaining = &keys[start..];
        let take = remaining.len().min(self.page_size);
        let page = &remaining[..take];
        let is_truncated = remaining.len() > take;
        let next_token = if is_truncated {
            page.last().map(|k| k.to_string())
        } else {
            None
        };

        let mut xml = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
        );
        xml.push_str(&format!("<IsTruncated>{is_truncated}</IsTruncated>\n"));
        if let Some(token) = &next_token {
            xml.push_str(&format!(
                "<NextContinuationToken>{}</NextContinuationToken>\n",
                xml_escape(token)
            ));
        }
        for key in page {
            let size = objects.get(*key).map(Vec::len).unwrap_or(0);
            xml.push_str(&format!(
                "<Contents><Key>{}</Key><Size>{size}</Size></Contents>\n",
                xml_escape(key)
            ));
        }
        xml.push_str("</ListBucketResult>");
        HttpResponse {
            status: 200,
            headers: std::collections::BTreeMap::new(),
            body: xml.into_bytes(),
        }
    }
}

#[async_trait]
impl Transport for FakeS3 {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        Ok(self.handle(request))
    }
}

fn split_uri(uri: &str) -> (&str, &str) {
    uri.split_once('?').unwrap_or((uri, ""))
}

fn no_such_key_response() -> HttpResponse {
    error_response(404, "NoSuchKey", "The specified key does not exist.")
}

fn error_response(status: u16, code: &str, message: &str) -> HttpResponse {
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message></Error>",
        xml_escape(code),
        xml_escape(message)
    );
    HttpResponse {
        status,
        headers: std::collections::BTreeMap::new(),
        body: xml.into_bytes(),
    }
}

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}
