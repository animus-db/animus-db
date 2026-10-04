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

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::client::{HttpRequest, HttpResponse, Transport, TransportError};
use crate::sigv4::{self, PayloadHash};

/// One registered credential: a secret, and — for a temporary (session)
/// credential — the session token it must be presented with and the instant
/// (epoch ms) after which S3 answers `ExpiredToken`.
#[derive(Clone)]
struct FakeCred {
    secret: String,
    token: Option<String>,
    expiry_epoch_ms: Option<u64>,
}

/// An in-memory S3 bucket double. Objects live in a plain `BTreeMap` (so
/// `list_objects_v2` iterates in the same lexicographic key order real S3
/// promises) keyed by object key (never the full `/bucket/key` path).
pub struct FakeS3 {
    bucket: String,
    credentials: Mutex<BTreeMap<String, FakeCred>>,
    request_count: Mutex<u64>,
    objects: Mutex<std::collections::BTreeMap<String, Vec<u8>>>,
    /// Max objects returned per `ListObjectsV2` page — deliberately
    /// configurable (default 1000, matching real S3's own default) so a
    /// test can force pagination across more than one page without
    /// uploading a thousand objects.
    page_size: usize,
    /// In-flight multipart uploads (S-08 M2), by upload id.
    uploads: Mutex<BTreeMap<String, Upload>>,
    next_upload: Mutex<u64>,
    /// Smallest size S3 accepts for a non-last part (real S3: 5 MiB).
    min_part_size: usize,
    /// `METHOD uri` of every authenticated object request (not lists), in
    /// order — lets a test assert which operations a caller issued.
    request_log: Mutex<Vec<String>>,
    /// `(part number, remaining failures)`: `UploadPart` of that part
    /// answers `500 InternalError` while the count is non-zero.
    part_failure: Mutex<Option<(u32, u32)>>,
    /// Make `CompleteMultipartUpload` answer `200` with an `<Error>` body.
    complete_error_in_200: Mutex<Option<String>>,
}

/// One in-flight multipart upload.
struct Upload {
    key: String,
    /// part number -> (etag, bytes)
    parts: BTreeMap<u32, (String, Vec<u8>)>,
}

/// Real S3's minimum size of a non-last multipart part.
pub const S3_MIN_PART_SIZE: usize = 5 * 1024 * 1024;

impl FakeS3 {
    /// A fresh, empty bucket double named `bucket`, with no registered
    /// credentials — every request is rejected until [`Self::
    /// with_credential`] registers at least one.
    #[must_use]
    pub fn new(bucket: impl Into<String>) -> Self {
        FakeS3 {
            bucket: bucket.into(),
            credentials: Mutex::new(BTreeMap::new()),
            request_count: Mutex::new(0),
            objects: Mutex::new(std::collections::BTreeMap::new()),
            page_size: 1000,
            uploads: Mutex::new(BTreeMap::new()),
            next_upload: Mutex::new(0),
            min_part_size: S3_MIN_PART_SIZE,
            request_log: Mutex::new(Vec::new()),
            part_failure: Mutex::new(None),
            complete_error_in_200: Mutex::new(None),
        }
    }

    /// Lower the minimum non-last part size (default 5 MiB, like S3) so a
    /// test can exercise multipart with tiny parts.
    #[must_use]
    pub fn with_min_part_size(mut self, bytes: usize) -> Self {
        self.min_part_size = bytes;
        self
    }

    /// Multipart uploads started and neither completed nor aborted.
    #[must_use]
    pub fn open_upload_count(&self) -> usize {
        self.uploads.lock().expect("fake uploads lock").len()
    }

    /// `METHOD uri` of every authenticated request so far, in order.
    #[must_use]
    pub fn request_log(&self) -> Vec<String> {
        self.request_log.lock().expect("fake log lock").clone()
    }

    /// Make the next `times` `UploadPart` calls for `part_number` answer
    /// `500 InternalError` (use a large `times` for a permanent failure).
    pub fn fail_upload_part(&self, part_number: u32, times: u32) {
        *self.part_failure.lock().expect("fake part failure lock") = Some((part_number, times));
    }

    /// Make every `CompleteMultipartUpload` answer HTTP 200 with an `<Error>`
    /// body carrying `code` (what S3 does when assembly fails late).
    pub fn set_complete_error_in_200(&self, code: Option<&str>) {
        *self.complete_error_in_200.lock().expect("fake lock") = code.map(str::to_string);
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
            .insert(
                access_key_id.into(),
                FakeCred {
                    secret: secret_access_key.into(),
                    token: None,
                    expiry_epoch_ms: None,
                },
            );
        self
    }

    /// Register a temporary (session) credential: requests signed with it
    /// must carry `session_token` as a signed `x-amz-security-token`, and a
    /// request whose `x-amz-date` is at or after `expiry_epoch_ms` is
    /// rejected `400 ExpiredToken` (the instant comes from the request's own
    /// signed timestamp — i.e. the caller-supplied `now`, never a clock).
    /// Callable on a shared (`Arc`) double, so a test can register the
    /// credentials a fake STS issues later.
    pub fn register_session_credential(
        &self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: impl Into<String>,
        expiry_epoch_ms: u64,
    ) {
        self.credentials
            .lock()
            .expect("fake credentials lock")
            .insert(
                access_key_id.into(),
                FakeCred {
                    secret: secret_access_key.into(),
                    token: Some(session_token.into()),
                    expiry_epoch_ms: Some(expiry_epoch_ms),
                },
            );
    }

    /// How many requests this double has received (including rejected ones).
    #[must_use]
    pub fn request_count(&self) -> u64 {
        *self.request_count.lock().expect("fake request count lock")
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
        *self.request_count.lock().expect("fake request count lock") += 1;
        let (raw_path_wire, raw_query_wire) = split_uri(&req.uri);
        let path = sigv4::percent_decode(raw_path_wire);
        // Recover raw pairs by splitting the still-**encoded** wire query
        // first, then decoding each side — never the other order. Decoding
        // the whole query string up front and then splitting the result on
        // `&` (an earlier version of this function did exactly that) is the
        // identical bug issue #855 fixed on the signing side: a value
        // containing its own literal `&` (e.g. a `prefix` derived from a
        // customer `S3KeyPrefix`) would already have been un-escaped back
        // to `&` by then, making it indistinguishable from a real pair
        // separator. See `sigv4::parse_wire_query_pairs`'s own doc.
        let query_pairs = sigv4::parse_wire_query_pairs(raw_query_wire);
        let query_pair_refs: Vec<(&str, &str)> = query_pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        if let Err(resp) = self.verify(&req, &path, &query_pair_refs) {
            return resp;
        }

        let trimmed = path.trim_start_matches('/');
        // Virtual-hosted addressing: the bucket is the first label of the
        // `Host` header (`bucket.endpoint-host`) and the whole path is the
        // key. Otherwise path-style: `/bucket/key`.
        let virtual_bucket = req
            .headers
            .get("host")
            .and_then(|h| h.split_once('.'))
            .map(|(label, _)| label)
            .filter(|label| *label == self.bucket);
        let (bucket, key) = match virtual_bucket {
            Some(b) => (
                b.to_string(),
                Some(trimmed.to_string()).filter(|k| !k.is_empty()),
            ),
            None => match trimmed.split_once('/') {
                Some((b, k)) => (b.to_string(), Some(k.to_string())),
                None => (trimmed.to_string(), None),
            },
        };
        if bucket != self.bucket {
            return error_response(404, "NoSuchBucket", "The specified bucket does not exist");
        }

        let is_list = query_pairs
            .iter()
            .any(|(k, v)| k == "list-type" && v == "2");
        if is_list {
            let mut prefix = String::new();
            let mut continuation: Option<String> = None;
            for (k, v) in &query_pairs {
                match k.as_str() {
                    "prefix" => prefix = v.clone(),
                    "continuation-token" => continuation = Some(v.clone()),
                    _ => {}
                }
            }
            return self.handle_list(&prefix, continuation.as_deref());
        }

        let Some(key) = key.filter(|k| !k.is_empty()) else {
            return error_response(400, "InvalidRequest", "missing object key");
        };
        self.request_log
            .lock()
            .expect("fake log lock")
            .push(format!("{} {}", req.method, req.uri));
        let q = |name: &str| {
            query_pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        let upload_id = q("uploadId");
        let part_number = q("partNumber");
        match req.method {
            "POST" if q("uploads").is_some() => self.handle_create_upload(&key),
            "POST" if upload_id.is_some() => {
                self.handle_complete_upload(&key, &upload_id.unwrap_or_default(), &req.body)
            }
            "PUT" if upload_id.is_some() && part_number.is_some() => self.handle_upload_part(
                &key,
                &upload_id.unwrap_or_default(),
                part_number.as_deref().unwrap_or(""),
                req.body,
            ),
            "DELETE" if upload_id.is_some() => {
                self.handle_abort_upload(&key, &upload_id.unwrap_or_default())
            }
            "PUT" => self.handle_put(&key, req.body),
            "GET" => self.handle_get(&key, req.headers.get("range").map(String::as_str)),
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
    fn verify(
        &self,
        req: &HttpRequest,
        path: &str,
        query_pairs: &[(&str, &str)],
    ) -> Result<(), HttpResponse> {
        let auth_header = req.headers.get("authorization").ok_or_else(|| {
            error_response(
                400,
                "MissingAuthenticationTokenException",
                "Request is missing Authentication Token",
            )
        })?;
        let parsed = sigv4::parse_authorization(auth_header)
            .ok_or_else(|| error_response(403, "AccessDenied", "Access Denied"))?;

        let cred = {
            let creds = self.credentials.lock().expect("fake credentials lock");
            creds.get(&parsed.access_key_id).cloned()
        };
        let Some(cred) = cred else {
            return Err(error_response(
                403,
                "InvalidAccessKeyId",
                "The AWS Access Key Id you provided does not exist in our records.",
            ));
        };
        if let Some(expected) = &cred.token
            && req.headers.get("x-amz-security-token") != Some(expected)
        {
            return Err(error_response(
                400,
                "InvalidToken",
                "The provided token is malformed or otherwise invalid.",
            ));
        }
        if let Some(expiry) = cred.expiry_epoch_ms {
            let now = req
                .headers
                .get("x-amz-date")
                .and_then(|d| amz_date_to_epoch_ms(d));
            if now.is_none_or(|n| n >= expiry) {
                return Err(error_response(
                    400,
                    "ExpiredToken",
                    "The provided token has expired.",
                ));
            }
        }
        let secret = cred.secret;

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
            query_pairs,
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

    fn handle_get(&self, key: &str, range: Option<&str>) -> HttpResponse {
        let objects = self.objects.lock().expect("fake objects lock");
        let Some(bytes) = objects.get(key) else {
            return no_such_key_response();
        };
        let Some(range) = range else {
            return HttpResponse {
                status: 200,
                headers: std::collections::BTreeMap::new(),
                body: bytes.clone(),
            };
        };
        // Only `bytes=a-b` (both ends) — all this crate ever sends.
        let parsed = range
            .strip_prefix("bytes=")
            .and_then(|r| r.split_once('-'))
            .and_then(|(a, b)| Some((a.parse::<usize>().ok()?, b.parse::<usize>().ok()?)));
        let Some((start, end)) = parsed.filter(|(a, b)| a <= b) else {
            return error_response(
                416,
                "InvalidRange",
                "The requested range is not satisfiable",
            );
        };
        if start >= bytes.len() {
            return error_response(
                416,
                "InvalidRange",
                "The requested range is not satisfiable",
            );
        }
        let end = end.min(bytes.len() - 1);
        let mut headers = BTreeMap::new();
        headers.insert(
            "content-range".to_string(),
            format!("bytes {start}-{end}/{}", bytes.len()),
        );
        HttpResponse {
            status: 206,
            headers,
            body: bytes[start..=end].to_vec(),
        }
    }

    fn handle_create_upload(&self, key: &str) -> HttpResponse {
        let id = {
            let mut n = self.next_upload.lock().expect("fake upload counter lock");
            *n += 1;
            format!("upload-{n}")
        };
        self.uploads.lock().expect("fake uploads lock").insert(
            id.clone(),
            Upload {
                key: key.to_string(),
                parts: BTreeMap::new(),
            },
        );
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult \
             xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key>\
             <UploadId>{id}</UploadId></InitiateMultipartUploadResult>",
            xml_escape(&self.bucket),
            xml_escape(key)
        );
        HttpResponse {
            status: 200,
            headers: BTreeMap::new(),
            body: xml.into_bytes(),
        }
    }

    fn handle_upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: &str,
        body: Vec<u8>,
    ) -> HttpResponse {
        let Some(n) = part_number
            .parse::<u32>()
            .ok()
            .filter(|n| (1..=10_000).contains(n))
        else {
            return error_response(400, "InvalidArgument", "Part number must be 1..=10000");
        };
        {
            let mut inj = self.part_failure.lock().expect("fake part failure lock");
            if let Some((pn, left)) = inj.as_mut()
                && *pn == n
                && *left > 0
            {
                *left -= 1;
                return error_response(500, "InternalError", "injected part failure");
            }
        }
        let mut uploads = self.uploads.lock().expect("fake uploads lock");
        let Some(up) = uploads.get_mut(upload_id).filter(|u| u.key == key) else {
            return no_such_upload();
        };
        let etag = opaque_etag(&body);
        up.parts.insert(n, (etag.clone(), body));
        let mut headers = BTreeMap::new();
        headers.insert("etag".to_string(), etag);
        HttpResponse {
            status: 200,
            headers,
            body: Vec::new(),
        }
    }

    fn handle_complete_upload(&self, key: &str, upload_id: &str, body: &[u8]) -> HttpResponse {
        let mut uploads = self.uploads.lock().expect("fake uploads lock");
        let Some(up) = uploads.get(upload_id).filter(|u| u.key == key) else {
            return no_such_upload();
        };
        if let Some(code) = self
            .complete_error_in_200
            .lock()
            .expect("fake lock")
            .clone()
        {
            // Real S3 answers 200 and fails inside the body; the upload
            // stays open.
            let mut r = error_response(200, &code, "injected late assembly failure");
            r.status = 200;
            return r;
        }
        let listed = crate::xml::parse_complete_request(&String::from_utf8_lossy(body));
        if listed.is_empty() {
            return error_response(400, "MalformedXML", "no parts listed");
        }
        if listed.windows(2).any(|w| w[0].0 >= w[1].0) {
            return error_response(
                400,
                "InvalidPartOrder",
                "The list of parts was not in ascending order",
            );
        }
        let mut assembled = Vec::new();
        let mut etags = String::new();
        for (i, (n, etag)) in listed.iter().enumerate() {
            let Some((have, bytes)) = up.parts.get(n) else {
                return error_response(400, "InvalidPart", "part was not uploaded");
            };
            if have != etag {
                return error_response(400, "InvalidPart", "part ETag does not match");
            }
            if i + 1 < listed.len() && bytes.len() < self.min_part_size {
                return error_response(
                    400,
                    "EntityTooSmall",
                    "Your proposed upload is smaller than the minimum allowed size",
                );
            }
            assembled.extend_from_slice(bytes);
            etags.push_str(etag);
        }
        let final_etag = format!(
            "\"{}-{}\"",
            opaque_etag(etags.as_bytes()).trim_matches('"'),
            listed.len()
        );
        uploads.remove(upload_id);
        drop(uploads);
        self.objects
            .lock()
            .expect("fake objects lock")
            .insert(key.to_string(), assembled);
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult \
             xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key>\
             <ETag>{}</ETag></CompleteMultipartUploadResult>",
            xml_escape(&self.bucket),
            xml_escape(key),
            xml_escape(&final_etag)
        );
        HttpResponse {
            status: 200,
            headers: BTreeMap::new(),
            body: xml.into_bytes(),
        }
    }

    fn handle_abort_upload(&self, key: &str, upload_id: &str) -> HttpResponse {
        let mut uploads = self.uploads.lock().expect("fake uploads lock");
        if uploads.get(upload_id).is_some_and(|u| u.key == key) {
            uploads.remove(upload_id);
            HttpResponse {
                status: 204,
                headers: BTreeMap::new(),
                body: Vec::new(),
            }
        } else {
            no_such_upload()
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

/// `YYYYMMDDTHHMMSSZ` -> epoch ms.
fn amz_date_to_epoch_ms(d: &str) -> Option<u64> {
    if d.len() != 16 || !d.is_ascii() {
        return None;
    }
    let iso = format!(
        "{}-{}-{}T{}:{}:{}Z",
        &d[0..4],
        &d[4..6],
        &d[6..8],
        &d[9..11],
        &d[11..13],
        &d[13..15]
    );
    crate::xml::parse_iso8601_epoch_ms(&iso)
}

fn split_uri(uri: &str) -> (&str, &str) {
    uri.split_once('?').unwrap_or((uri, ""))
}

fn no_such_upload() -> HttpResponse {
    error_response(
        404,
        "NoSuchUpload",
        "The specified multipart upload does not exist.",
    )
}

/// A stable opaque quoted ETag (truncated SHA-256 — S3 uses MD5, but clients
/// treat it as opaque).
fn opaque_etag(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let hex: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
    format!("\"{hex}\"")
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

// --- fake credential endpoints (STS / container / IMDS) -------------------

/// One request a [`FakeCredentialService`] saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeenRequest {
    pub method: String,
    pub uri: String,
    pub has_authorization: bool,
    pub authorization: Option<String>,
    pub body: String,
}

struct CredSvcState {
    /// Issued credentials are `AKID{n}` / `SECRET{n}` / `TOKEN{n}` for the
    /// n-th successful issuance (1-based).
    issued: u32,
    /// Absolute expiry (epoch ms) stamped on the next issuance.
    next_expiry_epoch_ms: u64,
    /// `Some((status, code, message))` makes every call fail like STS would.
    fail_with: Option<(u16, String, String)>,
    expected_web_identity_token: Option<String>,
    expected_container_auth: Option<String>,
    seen: Vec<SeenRequest>,
}

/// A fake STS `AssumeRoleWithWebIdentity` / ECS container-credentials / IMDSv2
/// endpoint in one `Transport` — dispatches on method + path. No clock: the
/// expiry it stamps is whatever the test configured
/// ([`Self::set_next_expiry_epoch_ms`]).
pub struct FakeCredentialService {
    state: Mutex<CredSvcState>,
}

impl Default for FakeCredentialService {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeCredentialService {
    #[must_use]
    pub fn new() -> Self {
        FakeCredentialService {
            state: Mutex::new(CredSvcState {
                issued: 0,
                next_expiry_epoch_ms: 0,
                fail_with: None,
                expected_web_identity_token: None,
                expected_container_auth: None,
                seen: Vec::new(),
            }),
        }
    }

    fn st(&self) -> std::sync::MutexGuard<'_, CredSvcState> {
        self.state.lock().expect("fake credential service lock")
    }

    pub fn set_next_expiry_epoch_ms(&self, expiry: u64) {
        self.st().next_expiry_epoch_ms = expiry;
    }

    /// Fail every subsequent call like the real service (`None` clears).
    pub fn set_failure(&self, failure: Option<(u16, &str, &str)>) {
        self.st().fail_with = failure.map(|(s, c, m)| (s, c.to_string(), m.to_string()));
    }

    /// Require this exact web-identity JWT on STS calls
    /// (else `400 InvalidIdentityToken`).
    pub fn expect_web_identity_token(&self, token: &str) {
        self.st().expected_web_identity_token = Some(token.to_string());
    }

    /// Require this exact `Authorization` value on container-credential
    /// calls (else `403`).
    pub fn expect_container_auth(&self, token: &str) {
        self.st().expected_container_auth = Some(token.to_string());
    }

    /// Number of successful credential issuances.
    #[must_use]
    pub fn issued(&self) -> u32 {
        self.st().issued
    }

    /// Every request seen, in order.
    #[must_use]
    pub fn seen(&self) -> Vec<SeenRequest> {
        self.st().seen.clone()
    }

    /// The (access key, secret, token) the n-th issuance (1-based) carries —
    /// for registering with [`FakeS3::register_session_credential`].
    #[must_use]
    pub fn nth_credential(n: u32) -> (String, String, String) {
        (
            format!("AKID{n}"),
            format!("SECRET{n}"),
            format!("TOKEN{n}"),
        )
    }

    fn issue(&self, st: &mut CredSvcState) -> (String, String, String, u64) {
        st.issued += 1;
        let (a, s, t) = Self::nth_credential(st.issued);
        (a, s, t, st.next_expiry_epoch_ms)
    }

    fn iso(epoch_ms: u64) -> String {
        let ymd = crate::sigv4::format_amz_date((epoch_ms / 1000) as i64);
        format!(
            "{}-{}-{}T{}:{}:{}Z",
            &ymd[0..4],
            &ymd[4..6],
            &ymd[6..8],
            &ymd[9..11],
            &ymd[11..13],
            &ymd[13..15]
        )
    }

    fn handle(&self, req: HttpRequest) -> HttpResponse {
        let mut st = self.st();
        let auth = req.headers.get("authorization").cloned();
        st.seen.push(SeenRequest {
            method: req.method.to_string(),
            uri: req.uri.clone(),
            has_authorization: auth.is_some(),
            authorization: auth.clone(),
            body: String::from_utf8_lossy(&req.body).into_owned(),
        });
        let path = req.uri.split('?').next().unwrap_or("").to_string();

        // IMDSv2 token handshake is not subject to injected failures only
        // when the failure is for the credentials step; keep it simple:
        // failures apply to every endpoint.
        if let Some((status, code, msg)) = st.fail_with.clone() {
            return sts_error(status, &code, &msg);
        }

        match (req.method, path.as_str()) {
            ("POST", "/") => {
                let body = String::from_utf8_lossy(&req.body).into_owned();
                let form: BTreeMap<String, String> = body
                    .split('&')
                    .filter_map(|kv| kv.split_once('='))
                    .map(|(k, v)| {
                        (
                            crate::sigv4::percent_decode(k),
                            crate::sigv4::percent_decode(v),
                        )
                    })
                    .collect();
                if form.get("Action").map(String::as_str) != Some("AssumeRoleWithWebIdentity")
                    || form.get("Version").map(String::as_str) != Some("2011-06-15")
                {
                    return sts_error(400, "InvalidAction", "bad Action/Version");
                }
                if let Some(expected) = &st.expected_web_identity_token
                    && form.get("WebIdentityToken") != Some(expected)
                {
                    return sts_error(
                        400,
                        "InvalidIdentityToken",
                        "Couldn't retrieve verification key from your identity provider.",
                    );
                }
                if auth.is_some() {
                    return sts_error(400, "InvalidClientTokenId", "unexpected Authorization");
                }
                let (a, s, t, exp) = self.issue(&mut st);
                let xml = format!(
                    "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
<AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>{a}</AccessKeyId>\
<SecretAccessKey>{s}</SecretAccessKey><SessionToken>{t}</SessionToken>\
<Expiration>{}</Expiration></Credentials></AssumeRoleWithWebIdentityResult>\
</AssumeRoleWithWebIdentityResponse>",
                    Self::iso(exp)
                );
                ok_body(xml.into_bytes())
            }
            ("PUT", "/latest/api/token") => {
                if !req
                    .headers
                    .contains_key("x-aws-ec2-metadata-token-ttl-seconds")
                {
                    return sts_error(400, "BadRequest", "missing ttl header");
                }
                ok_body(b"imds-session-token".to_vec())
            }
            ("GET", "/latest/meta-data/iam/security-credentials/") => {
                if req
                    .headers
                    .get("x-aws-ec2-metadata-token")
                    .map(String::as_str)
                    != Some("imds-session-token")
                {
                    return sts_error(401, "Unauthorized", "missing IMDS token");
                }
                ok_body(b"test-role\n".to_vec())
            }
            ("GET", "/latest/meta-data/iam/security-credentials/test-role") => {
                if req
                    .headers
                    .get("x-aws-ec2-metadata-token")
                    .map(String::as_str)
                    != Some("imds-session-token")
                {
                    return sts_error(401, "Unauthorized", "missing IMDS token");
                }
                ok_body(self.json_creds(&mut st).into_bytes())
            }
            ("GET", "/v1/credentials") => {
                if let Some(expected) = &st.expected_container_auth
                    && auth.as_ref() != Some(expected)
                {
                    return sts_error(403, "AccessDenied", "bad container auth token");
                }
                ok_body(self.json_creds(&mut st).into_bytes())
            }
            _ => sts_error(404, "NotFound", "unknown fake credential endpoint"),
        }
    }

    fn json_creds(&self, st: &mut CredSvcState) -> String {
        let (a, s, t, exp) = self.issue(st);
        format!(
            "{{\"AccessKeyId\":\"{a}\",\"SecretAccessKey\":\"{s}\",\"Token\":\"{t}\",\"Expiration\":\"{}\"}}",
            Self::iso(exp)
        )
    }
}

fn ok_body(body: Vec<u8>) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: BTreeMap::new(),
        body,
    }
}

fn sts_error(status: u16, code: &str, message: &str) -> HttpResponse {
    let xml = format!(
        "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><Error><Type>Sender</Type>\
<Code>{}</Code><Message>{}</Message></Error><RequestId>fake</RequestId></ErrorResponse>",
        xml_escape(code),
        xml_escape(message)
    );
    HttpResponse {
        status,
        headers: BTreeMap::new(),
        body: xml.into_bytes(),
    }
}

#[async_trait]
impl Transport for FakeCredentialService {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        Ok(self.handle(request))
    }
}

// --- scripted fault injection ---------------------------------------------

/// What a [`FaultyTransport`] does to one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// Forward to the inner transport untouched.
    Pass,
    /// Answer `status` with an S3 `<Error><Code>code</Code>` body **without**
    /// forwarding (the request is not applied).
    Status { status: u16, code: String },
    /// Fail with a connection-level transport error; not applied.
    TransportError,
    /// Fail with [`TransportError::Timeout`]; not applied.
    Timeout,
    /// The ack-lost case: forward the request (it IS applied to the inner
    /// transport), discard the response, and fail with a transport error.
    ApplyThenError,
}

impl Fault {
    /// Shorthand for [`Fault::Status`].
    #[must_use]
    pub fn status(status: u16, code: &str) -> Self {
        Fault::Status {
            status,
            code: code.to_string(),
        }
    }
}

/// A scripted fault plan: called once per request with the 0-based request
/// index and the request itself (method / uri incl. query are available to
/// key on), returns the [`Fault`] to inject.
pub type FaultPlan = Box<dyn FnMut(u64, &HttpRequest) -> Fault + Send>;

/// A [`Transport`] wrapper injecting deterministic, scripted faults in front
/// of any inner transport (typically [`FakeS3`]). Pure function of the plan
/// and the request sequence: no clock, no randomness of its own — a seeded
/// test builds its plan from its seed.
pub struct FaultyTransport<T> {
    inner: T,
    plan: Mutex<FaultPlan>,
    seen: Mutex<u64>,
    injected: Mutex<u64>,
}

impl<T: Transport> FaultyTransport<T> {
    /// Wrap `inner`, consulting `plan` for every request.
    #[must_use]
    pub fn new(inner: T, plan: FaultPlan) -> Self {
        FaultyTransport {
            inner,
            plan: Mutex::new(plan),
            seen: Mutex::new(0),
            injected: Mutex::new(0),
        }
    }

    /// Wrap `inner` with a plan that never injects anything (swap it later
    /// with [`Self::set_plan`]).
    #[must_use]
    pub fn passthrough(inner: T) -> Self {
        Self::new(inner, Box::new(|_, _| Fault::Pass))
    }

    /// Replace the plan. The request index keeps counting.
    pub fn set_plan(&self, plan: FaultPlan) {
        *self.plan.lock().expect("faulty plan lock") = plan;
    }

    /// The wrapped transport.
    #[must_use]
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Requests received so far (the next request's plan index).
    #[must_use]
    pub fn requests_seen(&self) -> u64 {
        *self.seen.lock().expect("faulty seen lock")
    }

    /// Requests that got any fault other than [`Fault::Pass`].
    #[must_use]
    pub fn faults_injected(&self) -> u64 {
        *self.injected.lock().expect("faulty injected lock")
    }
}

#[async_trait]
impl<T: Transport> Transport for FaultyTransport<T> {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let index = {
            let mut seen = self.seen.lock().expect("faulty seen lock");
            let i = *seen;
            *seen += 1;
            i
        };
        let fault = (self.plan.lock().expect("faulty plan lock"))(index, &request);
        if fault != Fault::Pass {
            *self.injected.lock().expect("faulty injected lock") += 1;
        }
        match fault {
            Fault::Pass => self.inner.send(request).await,
            Fault::Status { status, code } => Ok(HttpResponse {
                status,
                headers: BTreeMap::new(),
                body: format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code>\
                     <Message>injected fault</Message></Error>"
                )
                .into_bytes(),
            }),
            Fault::TransportError => Err(TransportError::Io("injected connection reset".into())),
            Fault::Timeout => Err(TransportError::Timeout("injected timeout".into())),
            Fault::ApplyThenError => {
                let _ = self.inner.send(request).await;
                Err(TransportError::Io(
                    "injected connection reset after the request was applied".into(),
                ))
            }
        }
    }
}
