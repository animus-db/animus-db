//! Pure AWS Signature Version 4 **signing** for the S3 client (S-04 PR 1,
//! `docs/roadmap.md` §2, `docs/adr/0059-backup-restore.md`'s S-04
//! amendment). No I/O, no clock, no `Env` — "now" is the caller's
//! `timestamp` field, mirroring the same convention `animus_dynamo::ttl`
//! and `animus_dynamo::sigv4` already use ("now passed in as a parameter,
//! never read from a clock directly") — the reason this crate carries no
//! `animus-env` dependency in this PR at all: a future `SegmentStore`
//! wrapper (PR 2) reads `env.wall_now()` and passes the result straight
//! through to [`sign_request`].
//!
//! # Sharing the signing-key chain with `animus_dynamo::sigv4` (ADR 0057)
//!
//! The roadmap's own S-04 plan asks whether that verifier's `kSecret ->
//! kDate -> kRegion -> kService -> kSigning` HMAC chain can be reused here.
//! **Decision: copy the ~20-line chain, don't depend on the crate.**
//! `animus-dynamo` is a DynamoDB-specific wire adapter — its own
//! `CLAUDE.md` describes its charter as "decode/encode [DynamoDB JSON] +
//! client-edge auth verification," and its `sigv4` module exists
//! specifically to verify DynamoDB client requests. A generic S3 client has
//! no legitimate reason to depend on that crate (it would drag in the
//! DynamoDB item model, wire JSON, and table-schema types for the sake of
//! twenty lines of HMAC calls), and the dependency would point the wrong
//! way for where this crate is headed: `docs/adr/0059-backup-restore.md`'s
//! S-04 plan has `animus-cp-data`/`animusd` depend on `animus-s3` directly
//! for the backup/segment-store backend, never through `animus-dynamo`.
//! [`compute_signing_key`] is that copied chain, with a comment pointing
//! back here; `tests/sigv4_chain_matches_dynamo.rs` is the dev-dependency-
//! only proof that the two chains agree byte-for-byte on the same inputs
//! (the ONLY place this crate names `animus-dynamo` at all).
//!
//! # What's S3-specific here, vs. the generic SigV4 algorithm
//!
//! - **No dot-segment URI normalization** ([`canonical_uri_s3`]): AWS's
//!   general-service canonicalization rule resolves `.`/`..` path segments
//!   before signing (`animus_dynamo::sigv4::canonical_uri` does exactly
//!   that, verified against the vendored `normalize-path` test vectors) —
//!   but S3 object keys may legitimately contain a literal `..` or `.`
//!   *segment* as part of the key name, so the S3 canonicalization rule is
//!   "percent-encode each `/`-separated segment, once, and change nothing
//!   else." Using the generic (dot-resolving) rule against an S3 endpoint
//!   would silently mis-sign any request whose key contains such a
//!   segment.
//! - **`x-amz-content-sha256` is always a signed header** (real S3
//!   requires it; DynamoDB's own wire never uses it), and [`PayloadHash`]
//!   supports the S3-only `UNSIGNED-PAYLOAD` literal alongside a real
//!   payload hash.
//! - [`sign_request`] always emits `SignedHeaders` in **sorted** order
//!   (`BTreeMap` iteration) — the real-SDK convention `animus_dynamo::
//!   sigv4::sign` documents but does not enforce (it trusts the caller to
//!   pre-sort, since it exists to test a verifier that must tolerate any
//!   order a real client claims).

use std::collections::BTreeMap;
use std::fmt;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// Static AWS-style credentials: an access key id (not secret — it already
/// travels in plaintext on every signed request) and a secret access key.
/// **`Debug` never renders the secret** — mirrors `animus_control::
/// meta::SecretKey`'s redaction discipline exactly; never add a `Display`
/// impl or a raw accessor with a name that invites logging it.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    /// The access key id. Not secret.
    pub access_key_id: String,
    secret_access_key: String,
}

impl Credentials {
    /// Wrap a raw access-key-id/secret pair.
    #[must_use]
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Credentials {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        }
    }

    /// The raw secret bytes — the one legitimate reason to look inside this
    /// type: feeding the HMAC chain. Never log, print, or format this value.
    #[must_use]
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"REDACTED")
            .finish()
    }
}

/// The `region`/`service` pair a request is signed under (the SigV4
/// "credential scope", minus the date and `aws4_request` terminal, which
/// [`sign_request`] derives itself). `service` is `"s3"` for every call this
/// crate's [`crate::client::S3Client`] makes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigningScope {
    pub region: String,
    pub service: String,
}

/// The SigV4 payload hash: either the real hex-SHA-256 of the request body,
/// or S3's `UNSIGNED-PAYLOAD` literal (a real S3 request may skip hashing a
/// large body up front — [`Self::signed`] is what this crate's own
/// [`crate::client::S3Client`] uses for every request today; `Unsigned` is
/// exercised by this module's own test suite and available to a future
/// streaming-upload caller).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadHash {
    /// The lowercase hex SHA-256 digest of the actual body bytes.
    Signed(String),
    /// S3's `UNSIGNED-PAYLOAD` sentinel.
    Unsigned,
}

impl PayloadHash {
    /// The real hex-SHA-256 digest of `bytes`.
    #[must_use]
    pub fn signed(bytes: &[u8]) -> Self {
        PayloadHash::Signed(hex_encode_lower(&Sha256::digest(bytes)))
    }

    /// The literal string this hash contributes to the canonical request
    /// and the `x-amz-content-sha256` header.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            PayloadHash::Signed(hex) => hex,
            PayloadHash::Unsigned => "UNSIGNED-PAYLOAD",
        }
    }
}

/// One HTTP request's SigV4-relevant surface, enough to sign it.
///
/// `query` is the **raw, undecoded** query string with no leading `?`
/// (empty if none) — mirroring `animus_dynamo::sigv4::SigV4Request::query`'s
/// exact contract: [`canonical_request`] percent-encodes and sorts it
/// itself. `headers` carries any *extra* headers the caller wants signed
/// beyond `host`/`x-amz-date`/`x-amz-content-sha256`, which [`sign_request`]
/// adds automatically — do not pass those three in here.
#[derive(Debug, Clone, Copy)]
pub struct RequestToSign<'a> {
    /// The HTTP method, e.g. `"PUT"`.
    pub method: &'a str,
    /// The request path (e.g. `"/bucket/key"`), not URI-decoded.
    pub uri: &'a str,
    /// The `Host` header value (and the `ServerName` a TLS transport would
    /// verify against).
    pub host: &'a str,
    /// The raw query string, no leading `?`.
    pub query: &'a str,
    /// Extra headers to sign, beyond the three [`sign_request`] adds itself.
    pub headers: &'a BTreeMap<String, String>,
    pub payload_sha256_hex: &'a PayloadHash,
    /// `X-Amz-Date`, `YYYYMMDDTHHMMSSZ` — see [`format_amz_date`].
    pub timestamp: &'a str,
}

/// The three headers [`sign_request`] computes: `Authorization` plus the
/// two headers its own `SignedHeaders` claims to have signed (a caller must
/// send all three, verbatim, on the wire request).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedHeaders {
    pub authorization: String,
    pub x_amz_date: String,
    pub x_amz_content_sha256: String,
}

/// Sign `req` under `scope`, using `creds`. Adds `host`/`x-amz-date`/
/// `x-amz-content-sha256` to the signed-header set automatically (real S3
/// always signs all three); `SignedHeaders` is emitted in sorted order.
#[must_use]
pub fn sign_request(
    creds: &Credentials,
    scope: &SigningScope,
    req: &RequestToSign,
) -> SignedHeaders {
    let mut headers = req.headers.clone();
    headers.insert("host".to_string(), req.host.to_string());
    headers.insert("x-amz-date".to_string(), req.timestamp.to_string());
    let payload_hash = req.payload_sha256_hex.as_str().to_string();
    headers.insert("x-amz-content-sha256".to_string(), payload_hash.clone());

    // `BTreeMap::keys()` yields sorted order already — exactly the
    // alphabetical `SignedHeaders` convention real AWS SDKs emit.
    let signed_headers: Vec<String> = headers.keys().cloned().collect();
    let signed_header_refs: Vec<&str> = signed_headers.iter().map(String::as_str).collect();

    let creq = canonical_request(
        req.method,
        req.uri,
        req.query,
        &headers,
        &signed_header_refs,
        &payload_hash,
    );
    let date = &req.timestamp[..8.min(req.timestamp.len())];
    let credential_scope = format!("{date}/{}/{}/aws4_request", scope.region, scope.service);
    let sts = string_to_sign(req.timestamp, &credential_scope, &creq);
    let sig = compute_signature(
        creds.secret_access_key(),
        date,
        &scope.region,
        &scope.service,
        &sts,
    );

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={}, Signature={sig}",
        creds.access_key_id,
        signed_headers.join(";")
    );

    SignedHeaders {
        authorization,
        x_amz_date: req.timestamp.to_string(),
        x_amz_content_sha256: payload_hash,
    }
}

/// The SigV4 canonical request string, given an already-resolved
/// `payload_hash` (unlike `animus_dynamo::sigv4::canonical_request`, which
/// always hashes `req.body` itself — this crate's caller may want
/// `UNSIGNED-PAYLOAD`, so the hash is a parameter here, not derived).
#[must_use]
pub fn canonical_request(
    method: &str,
    uri: &str,
    query: &str,
    headers: &BTreeMap<String, String>,
    signed_headers: &[&str],
    payload_hash: &str,
) -> String {
    let canon_uri = canonical_uri_s3(uri);
    let canon_query = canonical_query_string(query);
    let mut headers_block = String::new();
    for name in signed_headers {
        let raw = headers.get(*name).map(String::as_str).unwrap_or("");
        headers_block.push_str(name);
        headers_block.push(':');
        headers_block.push_str(&trim_collapse_whitespace(raw));
        headers_block.push('\n');
    }
    let signed_headers_line = signed_headers.join(";");
    format!(
        "{method}\n{canon_uri}\n{canon_query}\n{headers_block}\n{signed_headers_line}\n{payload_hash}"
    )
}

/// The SigV4 string-to-sign for a canonical request.
#[must_use]
pub fn string_to_sign(amz_date: &str, credential_scope: &str, canonical_request: &str) -> String {
    let hash = hex_encode_lower(&Sha256::digest(canonical_request.as_bytes()));
    format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hash}")
}

/// The SigV4 signing-key chain (`kSecret -> kDate -> kRegion -> kService ->
/// kSigning`), applied to `string_to_sign` and hex-encoded.
///
/// **Copied from `animus_dynamo::sigv4`'s private `signature` fn (ADR
/// 0057), not imported** — see this module's own doc for why the
/// dependency direction doesn't make sense here. `tests/
/// sigv4_chain_matches_dynamo.rs` proves the two chains agree.
#[must_use]
pub fn compute_signature(
    secret_access_key: &str,
    date: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    let k_secret = format!("AWS4{secret_access_key}");
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    hex_encode_lower(&hmac_sha256(&k_signing, string_to_sign.as_bytes()))
}

// --- verification support (for `crate::fake`) ---------------------------

/// A structurally-parsed `Authorization` header value — enough to look up a
/// candidate secret and recompute the signature. Used only by
/// [`crate::fake`]'s in-memory S3 double, which must verify every inbound
/// request's signature end-to-end (never used by [`sign_request`] itself).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedAuthorization {
    pub access_key_id: String,
    pub date: String,
    pub region: String,
    pub service: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

/// Parse an `Authorization: AWS4-HMAC-SHA256 Credential=.../SignedHeaders=
/// .../Signature=...` header value. `None` on any structural problem.
#[must_use]
pub fn parse_authorization(value: &str) -> Option<ParsedAuthorization> {
    let value = value.trim();
    let rest = value.strip_prefix("AWS4-HMAC-SHA256")?.trim_start();
    if rest.is_empty() {
        return None;
    }

    let mut credential = None;
    let mut signed_headers_raw = None;
    let mut signature = None;
    for part in rest.split(',') {
        let part = part.trim();
        let (key, val) = part.split_once('=')?;
        match key.trim() {
            "Credential" => credential = Some(val.trim()),
            "SignedHeaders" => signed_headers_raw = Some(val.trim()),
            "Signature" => signature = Some(val.trim().to_string()),
            _ => {}
        }
    }

    let credential = credential?;
    let signed_headers_raw = signed_headers_raw?;
    let signature = signature?;
    if signature.is_empty() {
        return None;
    }

    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() != 5 {
        return None;
    }
    let signed_headers: Vec<String> = signed_headers_raw
        .split(';')
        .map(|h| h.trim().to_ascii_lowercase())
        .collect();
    if signed_headers.is_empty() || signed_headers.iter().any(String::is_empty) {
        return None;
    }

    Some(ParsedAuthorization {
        access_key_id: parts[0].to_string(),
        date: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
        signed_headers,
        signature,
    })
}

/// Recompute the signature `parsed` claims and compare it against
/// `secret_access_key`. `headers` must contain every header
/// `parsed.signed_headers` names (including `x-amz-date`, needed to derive
/// the string-to-sign's own timestamp line) — a name absent from `headers`
/// contributes an empty canonical value, exactly like [`canonical_request`]
/// does for [`sign_request`] itself.
#[must_use]
pub fn verify_signature(
    parsed: &ParsedAuthorization,
    secret_access_key: &str,
    method: &str,
    uri: &str,
    query: &str,
    headers: &BTreeMap<String, String>,
    payload_hash: &str,
) -> bool {
    let Some(amz_date) = headers.get("x-amz-date") else {
        return false;
    };
    let signed_header_refs: Vec<&str> = parsed.signed_headers.iter().map(String::as_str).collect();
    let creq = canonical_request(
        method,
        uri,
        query,
        headers,
        &signed_header_refs,
        payload_hash,
    );
    let credential_scope = format!(
        "{}/{}/{}/aws4_request",
        parsed.date, parsed.region, parsed.service
    );
    let sts = string_to_sign(amz_date, &credential_scope, &creq);
    let expected = compute_signature(
        secret_access_key,
        &parsed.date,
        &parsed.region,
        &parsed.service,
        &sts,
    );
    constant_time_eq(expected.as_bytes(), parsed.signature.as_bytes())
}

/// Constant-*shape* comparison (walks the full length of the longer input
/// regardless of where the inputs diverge) — mirrors `animus_dynamo::
/// sigv4::constant_time_eq` exactly (a tiny, self-contained helper, not
/// worth sharing across the crate boundary for its own sake).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = u32::from(a.len() != b.len());
    let n = a.len().max(b.len());
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= u32::from(x ^ y);
    }
    diff == 0
}

// --- amz-date formatting (for `crate::client`) ---------------------------

/// Format Unix epoch seconds as `YYYYMMDD'T'HHMMSS'Z'` — the `X-Amz-Date`
/// shape [`sign_request`] expects as `RequestToSign::timestamp`.
#[must_use]
pub fn format_amz_date(epoch_secs: i64) -> String {
    let days = epoch_secs.div_euclid(86400);
    let secs_of_day = epoch_secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day % 3600) / 60;
    let ss = secs_of_day % 60;
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

/// Days-since-epoch to proleptic-Gregorian civil date. Howard Hinnant's
/// `civil_from_days` algorithm (public domain,
/// <https://howardhinnant.github.io/date_algorithms.html>) — copied here
/// (rather than depending on `animus-dynamo`, which carries the identical
/// algorithm for the same reason: turning an epoch instant into a calendar
/// date needs no dependency) exactly as `animus_dynamo::sigv4::
/// civil_from_days` does, for the same "small, dependency-free, public
/// domain math" reason documented there.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// --- internal: canonicalization -----------------------------------------

/// The S3 canonical URI: **no dot-segment resolution** (unlike
/// `animus_dynamo::sigv4::canonical_uri`'s generic-service rule) — each
/// `/`-separated segment is percent-encoded once and slashes (including
/// consecutive ones) are preserved exactly, since an S3 key may legitimately
/// contain a literal `.`/`..`/empty segment as part of its name.
pub(crate) fn canonical_uri_s3(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    path.split('/')
        .map(|seg| percent_encode(seg.as_bytes()))
        .collect::<Vec<_>>()
        .join("/")
}

/// The SigV4 canonical query string: parses a **raw, undecoded** `&`-joined
/// `key=value` query string, percent-encodes each side once, and sorts by
/// the encoded key then the encoded value. Copied from `animus_dynamo::
/// sigv4::canonical_query_string` (same algorithm, same "raw in, canonical
/// out" contract) — see this module's own doc for why this crate doesn't
/// depend on that one instead.
pub(crate) fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .map(|part| {
            let (k, v) = part.split_once('=').unwrap_or((part, ""));
            (percent_encode(k.as_bytes()), percent_encode(v.as_bytes()))
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Trim leading/trailing whitespace and collapse any run of internal spaces
/// or tabs to a single space — the SigV4 signed-header-value
/// canonicalization rule. Copied from `animus_dynamo::sigv4::
/// trim_collapse_whitespace` (same tiny, dependency-free helper).
fn trim_collapse_whitespace(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for ch in s.trim().chars() {
        if ch == ' ' || ch == '\t' {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out
}

/// AWS's URI-encoding rule: unreserved characters (`A-Za-z0-9-._~`) pass
/// through; everything else becomes `%XX` with uppercase hex digits.
/// `pub(crate)` (not private): `crate::client` reuses this to percent-encode
/// an S3 object key's own `/`-separated segments for the wire URI, so the
/// wire path and the signed canonical URI agree by construction.
pub(crate) fn percent_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_digit_upper(b >> 4));
            out.push(hex_digit_upper(b & 0x0f));
        }
    }
    out
}

/// The inverse of [`percent_encode`] — decode `%XX` escapes back to raw
/// bytes (interpreted as UTF-8, lossily). Used only by [`crate::fake`],
/// which receives an already-canonically-encoded wire URI/query and must
/// recover the raw form [`canonical_request`]'s own encoding step expects,
/// exactly once — see `crate::client`'s module doc for why signing/wire
/// building both start from a raw string and encode it independently,
/// rather than ever re-encoding an already-encoded one.
#[cfg(any(test, feature = "fake"))]
pub(crate) fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(any(test, feature = "fake"))]
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn hex_digit_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

fn hex_encode_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(hex_digit_lower(b >> 4));
        out.push(hex_digit_lower(b & 0x0f));
    }
    out
}

fn hex_digit_lower(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'a' + (nibble - 10)) as char,
    }
}

type HmacSha256 = Hmac<Sha256>;

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts a key of any length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn format_amz_date_matches_known_instant() {
        // 2015-08-30T12:36:00Z.
        assert_eq!(format_amz_date(1_440_938_160), "20150830T123600Z");
    }

    #[test]
    fn canonical_uri_s3_does_not_resolve_dot_segments() {
        // A literal `..` path segment is an S3 key character, not a
        // dot-segment to resolve away (unlike the generic-service rule).
        assert_eq!(canonical_uri_s3("/a/../b"), "/a/../b");
        assert_eq!(canonical_uri_s3("/a//b"), "/a//b");
        assert_eq!(canonical_uri_s3(""), "/");
        assert_eq!(canonical_uri_s3("/test.txt"), "/test.txt");
    }

    #[test]
    fn sign_request_over_the_s3_documentation_examples_request_shape() {
        // The request shape from AWS's "GET Object" SigV4 worked example
        // (docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html):
        // `GET /test.txt` against `examplebucket.s3.amazonaws.com`, a
        // `Range` header, and the well-known `AKIAIOSFODNN7EXAMPLE`/
        // `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY` example credential
        // pair. **Deliberately does not assert the literal `Signature=...`
        // value** the AWS documentation states for this example — this
        // sandbox has no network access to re-derive the exact
        // byte-for-byte canonical request AWS's own worked example uses
        // from an authoritative source, and pinning a memorized constant
        // this crate could not independently verify would be a worse
        // regression oracle than none at all (a mismatch would say nothing
        // about whether the *code* is right, only whether the *test's own
        // memorized constant* is). What this test does verify, all
        // self-checkable without an external oracle: the `SignedHeaders`
        // set/order this shape produces, and that the signature
        // [`sign_request`] emits round-trips through [`verify_signature`]
        // (proving the signer and the verifier agree on this exact
        // canonicalization, independent of whether it matches AWS's own
        // published value). `tests/sigv4_known_answers.rs` and
        // `tests/sigv4_chain_matches_dynamo.rs` carry this crate's actual
        // externally-verified known-answer coverage (the vendored
        // `aws-sig-v4-test-suite` vectors, and cross-checking against
        // `animus_dynamo::sigv4`'s independently-implemented chain).
        let creds = Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        );
        let scope = SigningScope {
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
        };
        let payload_hash = PayloadHash::Signed(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
        );
        let extra = headers(&[("range", "bytes=0-9")]);
        let req = RequestToSign {
            method: "GET",
            uri: "/test.txt",
            host: "examplebucket.s3.amazonaws.com",
            query: "",
            headers: &extra,
            payload_sha256_hex: &payload_hash,
            timestamp: "20130524T000000Z",
        };
        let signed = sign_request(&creds, &scope, &req);
        assert!(signed.authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature="
        ));
        assert_eq!(signed.x_amz_date, "20130524T000000Z");
        assert_eq!(
            signed.x_amz_content_sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        // The signer and the verifier must agree on this exact
        // canonicalization, whatever the resulting signature is.
        let mut wire_headers = extra.clone();
        wire_headers.insert("host".to_string(), req.host.to_string());
        wire_headers.insert("x-amz-date".to_string(), signed.x_amz_date.clone());
        wire_headers.insert(
            "x-amz-content-sha256".to_string(),
            signed.x_amz_content_sha256.clone(),
        );
        let parsed = parse_authorization(&signed.authorization).expect("parses");
        assert!(verify_signature(
            &parsed,
            creds.secret_access_key(),
            req.method,
            req.uri,
            req.query,
            &wire_headers,
            payload_hash.as_str(),
        ));
    }

    #[test]
    fn sign_request_supports_unsigned_payload() {
        let creds = Credentials::new("AKID", "secret");
        let scope = SigningScope {
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
        };
        let empty = BTreeMap::new();
        let req = RequestToSign {
            method: "PUT",
            uri: "/bucket/key",
            host: "s3.amazonaws.com",
            query: "",
            headers: &empty,
            payload_sha256_hex: &PayloadHash::Unsigned,
            timestamp: "20150830T123600Z",
        };
        let signed = sign_request(&creds, &scope, &req);
        assert_eq!(signed.x_amz_content_sha256, "UNSIGNED-PAYLOAD");
        assert!(
            signed
                .authorization
                .contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date")
        );
    }

    #[test]
    fn verify_signature_round_trips_with_sign_request() {
        let creds = Credentials::new("AKID", "topsecret");
        let scope = SigningScope {
            region: "us-west-2".to_string(),
            service: "s3".to_string(),
        };
        let payload_hash = PayloadHash::signed(b"hello world");
        let empty = BTreeMap::new();
        let req = RequestToSign {
            method: "PUT",
            uri: "/my-bucket/my/key.txt",
            host: "127.0.0.1:9000",
            query: "",
            headers: &empty,
            payload_sha256_hex: &payload_hash,
            timestamp: "20240101T000000Z",
        };
        let signed = sign_request(&creds, &scope, &req);

        let mut wire_headers = BTreeMap::new();
        wire_headers.insert("host".to_string(), req.host.to_string());
        wire_headers.insert("x-amz-date".to_string(), signed.x_amz_date.clone());
        wire_headers.insert(
            "x-amz-content-sha256".to_string(),
            signed.x_amz_content_sha256.clone(),
        );

        let parsed = parse_authorization(&signed.authorization).expect("parses");
        assert!(verify_signature(
            &parsed,
            creds.secret_access_key(),
            req.method,
            req.uri,
            req.query,
            &wire_headers,
            payload_hash.as_str(),
        ));
        assert!(!verify_signature(
            &parsed,
            "wrong-secret",
            req.method,
            req.uri,
            req.query,
            &wire_headers,
            payload_hash.as_str(),
        ));
    }

    #[test]
    fn parse_authorization_rejects_malformed_values() {
        for bad in [
            "",
            "Bearer sometoken",
            "AWS4-HMAC-SHA256",
            "AWS4-HMAC-SHA256 Credential=AKID/20150830/us-east-1/s3/aws4_request",
            "AWS4-HMAC-SHA256 Credential=AKID, SignedHeaders=host, Signature=abc",
        ] {
            assert!(
                parse_authorization(bad).is_none(),
                "expected None for {bad:?}"
            );
        }
    }

    #[test]
    fn percent_decode_inverts_percent_encode() {
        for raw in ["my key.txt", "a/b?c&d=e", "snowman \u{2603}", ""] {
            let encoded = percent_encode(raw.as_bytes());
            assert_eq!(percent_decode(&encoded), raw, "round trip for {raw:?}");
        }
        // Lowercase hex escapes decode too (uppercase is only this crate's
        // own *encoding* convention, not a decoding requirement).
        assert_eq!(percent_decode("%2f"), "/");
        assert_eq!(percent_decode("%2F"), "/");
    }

    #[test]
    fn credentials_debug_never_prints_the_secret() {
        let creds = Credentials::new("AKID", "super-secret-value");
        let rendered = format!("{creds:?}");
        assert!(!rendered.contains("super-secret-value"));
        assert!(rendered.contains("REDACTED"));
        assert!(rendered.contains("AKID"));
    }
}
