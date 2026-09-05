//! AWS Signature Version 4 verification and signing (ADR 0057): a pure
//! canonical-request / string-to-sign / HMAC-SHA256 signing-key chain, plus
//! constant-shape signature comparison — bytes and strings in, verdict out.
//!
//! This is a **deliberate, narrow widening of the crate's charter** (ADR
//! 0057): today `animus-dynamo` is decode/encode only; this module adds
//! client-edge auth *verification*. It stays inside the crate's purity rules
//! (ADR 0003) — no `Env`, no I/O, no clock, `BTreeMap` only (never
//! `HashMap`, lint-enforced): "now" is passed in as `now_epoch_ms`, mirroring
//! how [`crate::ttl`] takes `now_epoch_secs` rather than reading a clock
//! itself. The caller (`animusd`) reads `env.wall_now()` (ADR 0051 — never
//! `SystemTime::now()`) and passes the result in; this module never decides
//! *when* to check, only *whether* a given timestamp is within tolerance of a
//! given "now".
//!
//! # Verification semantics (binding — see ADR 0057)
//!
//! - `Authorization: AWS4-HMAC-SHA256 Credential=<akid>/<date>/<region>/
//!   <service>/aws4_request, SignedHeaders=<h1;h2;...>, Signature=<hex>` is
//!   parsed whitespace-tolerantly. There is **no `Date` header fallback** —
//!   `X-Amz-Date` is required, and `SignedHeaders` must name both `host` and
//!   `x-amz-date`.
//! - The credential scope's region and service are taken from the client
//!   **verbatim** (never pinned — this codebase has no region concept), but
//!   the scope's terminal component must be `aws4_request` and its date must
//!   equal the date portion of `X-Amz-Date`.
//! - The payload hash is always the hex SHA-256 of the actual body bytes (no
//!   `UNSIGNED-PAYLOAD`, no chunked signing — out of scope per the ADR).
//! - Clock skew tolerance is **±5 minutes** (`SKEW_SECS`), AWS's own window.
//!
//! ## Check order (a deliberate design decision, documented here because the
//! ADR leaves the exact interleaving to the implementation)
//!
//! 1. **Structural validity of `Authorization`** — missing, unparseable, or
//!    missing a `host`/`x-amz-date` member of `SignedHeaders`, or a signed
//!    header the request doesn't actually carry → [`SigV4Error::MissingAuthenticationToken`].
//!    This never touches the credential store or does any crypto — it is a
//!    pure shape check on what the client sent.
//! 2. **Access key lookup** — unknown `Credential` access key id →
//!    [`SigV4Error::UnrecognizedClient`], before any signature work (so a
//!    scan for valid keys never pays HMAC cost).
//! 3. **`X-Amz-Date` value format** — must match `YYYYMMDD'T'HHMMSS'Z'`
//!    exactly; a malformed value is **not** treated as "missing" (the header
//!    is present, just unparseable) and instead falls into the same bucket
//!    as a bad signature: [`SigV4Error::SignatureMismatch`]. A forged/garbled
//!    timestamp is exactly as unverifiable as a forged signature.
//! 4. **Clock skew** — the parsed timestamp compared to `now_epoch_ms` ± 300s
//!    → [`SigV4Error::SignatureExpired`] (a distinct `InvalidSignatureException`
//!    message shape, still the same wire error code as a mismatch).
//! 5. **Credential-scope validation + recompute-and-compare** — the scope's
//!    terminal component and date are checked, then the canonical request /
//!    string-to-sign / signature are recomputed and compared. Scope
//!    validation is folded into this last bucket rather than given its own
//!    step: a scope with the wrong terminal or a mismatched date can never
//!    produce a signature that verifies anyway (the signing key is derived
//!    from exactly those fields), so rejecting it before doing the HMAC work
//!    changes nothing observable — it is purely a fast-fail.
//!
//! # Purity and the vendored test-vector suite
//!
//! There is no simulation story for this module (ADR 0003 targets
//! *nondeterminism*, and a pure function has none): correctness is instead
//! established against AWS's own published `aws-sig-v4-test-suite`, vendored
//! under `tests/sigv4_vectors/` and exercised in
//! `tests/sigv4_vectors_test.rs`. `canonical_request`/`string_to_sign`/`sign`
//! are exported specifically so that suite can assert each of the vectors'
//! `.creq`/`.sts`/`.authz` stages independently, not just the final verdict.

use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// Clock-skew tolerance either side of "now" — AWS's own ±5 minute window.
const SKEW_SECS: i64 = 300;

/// One HTTP request's SigV4-relevant surface: enough to reconstruct its
/// canonical form. `headers` are **lowercased header name → value**, with a
/// repeated header's values already comma-joined in the order received (the
/// caller's job — an HTTP layer naturally sees repeated/folded header lines
/// before this module does); `query` excludes the leading `?`.
#[derive(Debug, Clone, Copy)]
pub struct SigV4Request<'a> {
    /// The HTTP method, e.g. `"POST"`.
    pub method: &'a str,
    /// The request path, e.g. `"/"`. Not URI-decoded — see [`canonical_request`]
    /// for the normalization this module applies.
    pub path: &'a str,
    /// The raw query string with no leading `?` (empty string if none).
    pub query: &'a str,
    /// Lowercased header name → value, values as received (comma-joined if
    /// the header repeated).
    pub headers: &'a BTreeMap<String, String>,
    /// The exact request body bytes; the payload hash is always computed
    /// over these, regardless of content type.
    pub body: &'a [u8],
}

/// A SigV4 verification failure, carrying enough to render the AWS-faithful
/// wire error (ADR 0057's error-mapping table): `error_code()` is the short
/// `__type` name, `type_name()` prefixes it with the auth-layer namespace
/// (`com.amazon.coral.service#…`, distinct from the DynamoDB service
/// namespace `WireError::to_json` uses), and `message()` is AWS's own
/// message text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SigV4Error {
    /// The `Authorization` header is absent, unparseable, or its
    /// `SignedHeaders` doesn't name both `host` and `x-amz-date` (or names a
    /// header the request doesn't actually carry).
    MissingAuthenticationToken,
    /// The `Credential`'s access key id has no entry in the credential
    /// store.
    UnrecognizedClient,
    /// The recomputed signature does not match the one supplied — includes a
    /// malformed `X-Amz-Date` value and an invalid credential scope (wrong
    /// terminal component, or a scope date that doesn't match
    /// `X-Amz-Date`'s date), both of which can never produce a matching
    /// signature anyway.
    SignatureMismatch,
    /// `X-Amz-Date` is outside the ±5 minute skew window of "now".
    SignatureExpired {
        /// The request's own `X-Amz-Date` value, verbatim.
        amz_date: String,
        /// The window edge the request fell outside of (`now ∓ 5min`,
        /// formatted the same way as `X-Amz-Date`).
        bound: String,
        /// "Now" (the value `now_epoch_ms` was derived from), formatted the
        /// same way as `X-Amz-Date`.
        now: String,
        /// `true` when the request's date is *earlier* than the window
        /// (too old); `false` when it's *later* (too far in the future).
        earlier: bool,
    },
}

impl SigV4Error {
    /// The short `__type` name AWS uses for this failure (ADR 0057's
    /// error-mapping table).
    #[must_use]
    pub fn error_code(&self) -> &'static str {
        match self {
            SigV4Error::MissingAuthenticationToken => "MissingAuthenticationTokenException",
            SigV4Error::UnrecognizedClient => "UnrecognizedClientException",
            SigV4Error::SignatureMismatch | SigV4Error::SignatureExpired { .. } => {
                "InvalidSignatureException"
            }
        }
    }

    /// The full `__type` value, namespaced under the auth layer (distinct
    /// from `com.amazonaws.dynamodb.v20120810#…`, the DynamoDB service
    /// namespace).
    #[must_use]
    pub fn type_name(&self) -> String {
        format!("com.amazon.coral.service#{}", self.error_code())
    }

    /// AWS's own message text for this failure.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            SigV4Error::MissingAuthenticationToken => {
                "Request is missing Authentication Token".to_string()
            }
            SigV4Error::UnrecognizedClient => {
                "The security token included in the request is invalid.".to_string()
            }
            SigV4Error::SignatureMismatch => "The request signature we calculated does not \
                match the signature you provided. Check your AWS Secret Access Key and signing \
                method. Consult the service documentation for details."
                .to_string(),
            SigV4Error::SignatureExpired {
                amz_date,
                bound,
                now,
                earlier,
            } => {
                if *earlier {
                    format!(
                        "Signature expired: {amz_date} is now earlier than {bound} ({now} - 5 min.)"
                    )
                } else {
                    format!(
                        "Signature expired: {amz_date} is now later than {bound} ({now} + 5 min.)"
                    )
                }
            }
        }
    }
}

impl std::fmt::Display for SigV4Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for SigV4Error {}

/// The `Authorization` header's own `Credential` scope, parsed but not yet
/// checked against any credential store or clock (ADR 0066 §3) — the
/// merged-catalog gate needs the access key id up front, to look up
/// candidate secrets one at a time, before it can call [`verify`] at all;
/// `region` rides along for the same reason `animusd`'s `AccessDeniedException`
/// message synthesizes a table ARN from the caller's own credential scope
/// (ADR 0066 §5) rather than inventing a region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCredential {
    /// The `Credential` scope's access key id.
    pub access_key_id: String,
    /// The `Credential` scope's region component, taken verbatim (never
    /// pinned — this codebase has no region concept, ADR 0057).
    pub region: String,
}

/// Parse `req`'s `Authorization` header far enough to recover its
/// `Credential` scope — exactly [`verify`]'s own step 1 structural check,
/// stopping short of any credential-store lookup or crypto. Never touches
/// `credentials` or a clock.
///
/// # Errors
/// [`SigV4Error::MissingAuthenticationToken`] on every structural problem
/// `verify`'s own step 1 already rejects (absent/unparseable `Authorization`,
/// or a `SignedHeaders` member the request doesn't actually carry).
pub fn parse_credential(req: &SigV4Request) -> Result<ParsedCredential, SigV4Error> {
    let auth_header = req
        .headers
        .get("authorization")
        .ok_or(SigV4Error::MissingAuthenticationToken)?;
    let parsed = parse_authorization(auth_header)?;
    for name in &parsed.signed_headers {
        if req.headers.get(name).is_none() {
            return Err(SigV4Error::MissingAuthenticationToken);
        }
    }
    Ok(ParsedCredential {
        access_key_id: parsed.access_key_id,
        region: parsed.region,
    })
}

/// Verify `req`'s `Authorization` header against `credentials`
/// (access-key-id → secret-access-key) as of `now_epoch_ms`. See the module
/// doc for the check order and each failure's classification.
pub fn verify(
    req: &SigV4Request,
    credentials: &BTreeMap<String, String>,
    now_epoch_ms: u64,
) -> Result<(), SigV4Error> {
    // Step 1: structural validity of `Authorization`.
    let auth_header = req
        .headers
        .get("authorization")
        .ok_or(SigV4Error::MissingAuthenticationToken)?;
    let parsed = parse_authorization(auth_header)?;
    for name in &parsed.signed_headers {
        if req.headers.get(name).is_none() {
            return Err(SigV4Error::MissingAuthenticationToken);
        }
    }

    // Step 2: unknown access key, before any crypto.
    let secret = credentials
        .get(&parsed.access_key_id)
        .ok_or(SigV4Error::UnrecognizedClient)?;

    // Step 3: `X-Amz-Date` value must parse under the strict AWS format.
    // Presence was already confirmed by the signed-headers loop above (it's
    // a required SignedHeaders member).
    let amz_date_value = req
        .headers
        .get("x-amz-date")
        .expect("x-amz-date presence checked above");
    let req_ts = parse_amz_date(amz_date_value).ok_or(SigV4Error::SignatureMismatch)?;

    // Step 4: clock skew, ±5 minutes.
    let now_secs = (now_epoch_ms / 1000) as i64;
    if req_ts < now_secs - SKEW_SECS {
        return Err(SigV4Error::SignatureExpired {
            amz_date: amz_date_value.clone(),
            bound: format_amz_date(now_secs - SKEW_SECS),
            now: format_amz_date(now_secs),
            earlier: true,
        });
    }
    if req_ts > now_secs + SKEW_SECS {
        return Err(SigV4Error::SignatureExpired {
            amz_date: amz_date_value.clone(),
            bound: format_amz_date(now_secs + SKEW_SECS),
            now: format_amz_date(now_secs),
            earlier: false,
        });
    }

    // Step 5: credential-scope validation, then recompute-and-compare.
    if parsed.terminal != "aws4_request" || parsed.date != amz_date_value[..8] {
        return Err(SigV4Error::SignatureMismatch);
    }

    let signed_header_refs: Vec<&str> = parsed.signed_headers.iter().map(String::as_str).collect();
    let creq = canonical_request(req, &signed_header_refs);
    let scope = format!(
        "{}/{}/{}/aws4_request",
        parsed.date, parsed.region, parsed.service
    );
    let sts = string_to_sign(amz_date_value, &scope, &creq);
    let computed = signature(secret, &parsed.date, &parsed.region, &parsed.service, &sts);

    if constant_time_eq(computed.as_bytes(), parsed.signature.as_bytes()) {
        Ok(())
    } else {
        Err(SigV4Error::SignatureMismatch)
    }
}

/// Build the `Authorization` header value for `req`, signed with
/// `access_key_id`/`secret_access_key` under credential scope
/// `<date>/<region>/<service>/aws4_request` (`date` is `amz_date`'s first 8
/// characters). `signed_headers` is used verbatim, in the given order, for
/// both the canonical request's `SignedHeaders` line and the emitted field —
/// callers should pass them pre-sorted alphabetically (real SDK behavior;
/// this function does not re-sort, matching how [`verify`] reconstructs from
/// whatever order a client claims rather than second-guessing it).
///
/// This is the signer half of the module: used by the vendored
/// test-vector suite to assert the `.authz` stage, and (ADR 0057) by
/// `animusd`'s hand-rolled end-to-end test signer.
///
/// # Panics
///
/// Panics if `amz_date` is shorter than 8 characters — this is a builder for
/// trusted internal callers (tests, a test signer), not a parser of
/// untrusted input; see [`verify`] for the fallible, adversarial-input path.
#[must_use]
pub fn sign(
    req: &SigV4Request,
    access_key_id: &str,
    secret_access_key: &str,
    amz_date: &str,
    region: &str,
    service: &str,
    signed_headers: &[&str],
) -> String {
    let date = &amz_date[..8];
    let creq = canonical_request(req, signed_headers);
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let sts = string_to_sign(amz_date, &scope, &creq);
    let sig = signature(secret_access_key, date, region, service, &sts);
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, SignedHeaders={}, Signature={sig}",
        signed_headers.join(";")
    )
}

/// The SigV4 canonical request string for `req`, using exactly the header
/// names in `signed_headers` (in the given order) — the `.creq` stage of the
/// vendored test vectors.
#[must_use]
pub fn canonical_request(req: &SigV4Request, signed_headers: &[&str]) -> String {
    let uri = canonical_uri(req.path);
    let query = canonical_query_string(req.query);
    let headers_block = canonical_headers_block(req, signed_headers);
    let signed_headers_line = signed_headers.join(";");
    let payload_hash = hex_encode_lower(&Sha256::digest(req.body));
    format!(
        "{}\n{uri}\n{query}\n{headers_block}\n{signed_headers_line}\n{payload_hash}",
        req.method
    )
}

/// The SigV4 string-to-sign for a canonical request — the `.sts` stage.
#[must_use]
pub fn string_to_sign(amz_date: &str, credential_scope: &str, canonical_request: &str) -> String {
    let hash = hex_encode_lower(&Sha256::digest(canonical_request.as_bytes()));
    format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hash}")
}

// --- internal: parsing -------------------------------------------------

/// A structurally-valid `Authorization` header, parsed but not yet
/// cryptographically checked. Credential-scope fields (`region`/`service`)
/// are kept **verbatim** — ADR 0057 never pins them.
struct ParsedAuthorization {
    access_key_id: String,
    date: String,
    region: String,
    service: String,
    terminal: String,
    signed_headers: Vec<String>,
    signature: String,
}

/// Parse an `Authorization` header value, whitespace-tolerantly (AWS SDKs
/// vary slightly in the spacing after `,`). Any structural problem —
/// wrong algorithm token, a missing field, a `Credential` that doesn't split
/// into exactly 5 `/`-separated parts, an empty `SignedHeaders` entry, or
/// `SignedHeaders` missing `host`/`x-amz-date` — is
/// [`SigV4Error::MissingAuthenticationToken`]. The credential scope's
/// `terminal` component is captured but **not** validated as `aws4_request`
/// here; that's [`verify`]'s job (bucketed as a signature-compare failure,
/// not a structural one — see the module doc's check-order rationale).
fn parse_authorization(value: &str) -> Result<ParsedAuthorization, SigV4Error> {
    let value = value.trim();
    let rest = value
        .strip_prefix("AWS4-HMAC-SHA256")
        .ok_or(SigV4Error::MissingAuthenticationToken)?;
    let rest = rest.trim_start();
    if rest.is_empty() {
        return Err(SigV4Error::MissingAuthenticationToken);
    }

    let mut credential: Option<&str> = None;
    let mut signed_headers_raw: Option<&str> = None;
    let mut signature_raw: Option<&str> = None;
    for part in rest.split(',') {
        let part = part.trim();
        let (key, val) = part
            .split_once('=')
            .ok_or(SigV4Error::MissingAuthenticationToken)?;
        match key.trim() {
            "Credential" => credential = Some(val.trim()),
            "SignedHeaders" => signed_headers_raw = Some(val.trim()),
            "Signature" => signature_raw = Some(val.trim()),
            _ => {}
        }
    }

    let credential = credential.ok_or(SigV4Error::MissingAuthenticationToken)?;
    let signed_headers_raw = signed_headers_raw.ok_or(SigV4Error::MissingAuthenticationToken)?;
    let signature = signature_raw
        .ok_or(SigV4Error::MissingAuthenticationToken)?
        .to_string();
    if signature.is_empty() {
        return Err(SigV4Error::MissingAuthenticationToken);
    }

    let cred_parts: Vec<&str> = credential.split('/').collect();
    if cred_parts.len() != 5 {
        return Err(SigV4Error::MissingAuthenticationToken);
    }
    let [access_key_id, date, region, service, terminal] = [
        cred_parts[0],
        cred_parts[1],
        cred_parts[2],
        cred_parts[3],
        cred_parts[4],
    ];
    if access_key_id.is_empty() || date.is_empty() || region.is_empty() || service.is_empty() {
        return Err(SigV4Error::MissingAuthenticationToken);
    }

    let signed_headers: Vec<String> = signed_headers_raw
        .split(';')
        .map(|h| h.trim().to_ascii_lowercase())
        .collect();
    if signed_headers.is_empty() || signed_headers.iter().any(String::is_empty) {
        return Err(SigV4Error::MissingAuthenticationToken);
    }
    if !signed_headers.iter().any(|h| h == "host")
        || !signed_headers.iter().any(|h| h == "x-amz-date")
    {
        return Err(SigV4Error::MissingAuthenticationToken);
    }

    Ok(ParsedAuthorization {
        access_key_id: access_key_id.to_string(),
        date: date.to_string(),
        region: region.to_string(),
        service: service.to_string(),
        terminal: terminal.to_string(),
        signed_headers,
        signature,
    })
}

/// Parse a strict `YYYYMMDD'T'HHMMSS'Z'` timestamp into Unix epoch seconds,
/// or `None` if it doesn't match that exact 16-byte shape (this is the "not
/// missing, but unparseable" case the module doc's step 3 maps to
/// [`SigV4Error::SignatureMismatch`]).
fn parse_amz_date(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() != 16 || b[8] != b'T' || b[15] != b'Z' {
        return None;
    }
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let slice = b.get(range)?;
        if !slice.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(slice).ok()?.parse::<i64>().ok()
    };
    let year = digits(0..4)?;
    let month = digits(4..6)?;
    let day = digits(6..8)?;
    let hour = digits(9..11)?;
    let minute = digits(11..13)?;
    let second = digits(13..15)?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return None;
    }
    let days = days_from_civil(year, month as u32, day as u32);
    Some(days * 86400 + hour * 3600 + minute * 60 + second)
}

/// Format Unix epoch seconds as `YYYYMMDD'T'HHMMSS'Z'` (the inverse of
/// [`parse_amz_date`]) — used to render the window bound in
/// [`SigV4Error::SignatureExpired`]'s message.
fn format_amz_date(epoch_secs: i64) -> String {
    let days = epoch_secs.div_euclid(86400);
    let secs_of_day = epoch_secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day % 3600) / 60;
    let ss = secs_of_day % 60;
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

/// Days since the Unix epoch for a proleptic-Gregorian civil date. Howard
/// Hinnant's `days_from_civil` algorithm (public domain,
/// <https://howardhinnant.github.io/date_algorithms.html>) — a small,
/// dependency-free replacement for a calendar library, sufficient for the
/// one thing this module needs: turning `X-Amz-Date` into a comparable
/// instant.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (i64::from(m) + 9) % 12; // [0, 11]: Mar=0 .. Feb=11
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`].
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

/// The SigV4 canonical URI for `path`: dot-segments (`.`/`..`) resolved and
/// consecutive slashes collapsed (RFC 3986-style normalization), each
/// remaining segment percent-encoded, joined back with single `/`s. This is
/// the **non-S3** rule (the ADR is explicit: this codebase encodes the path
/// once, it does not double-encode already-percent-looking input) — verified
/// against the `aws-sig-v4-test-suite`'s `normalize-path` vectors, including
/// the `get-slash`/`get-slashes`/`get-relative*` edge cases where the
/// resolved path is empty (canonical URI is then bare `/`, never `//`).
fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    let ends_with_slash = path.ends_with('/');
    let mut stack: Vec<String> = Vec::new();
    for seg in path.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                stack.pop();
            }
            _ => stack.push(percent_encode(seg.as_bytes())),
        }
    }
    let mut out = String::from("/");
    out.push_str(&stack.join("/"));
    if ends_with_slash && !stack.is_empty() {
        out.push('/');
    }
    out
}

/// The SigV4 canonical query string: `&`-separated `key=value` pairs, each
/// side percent-encoded, sorted by the encoded key then the encoded value.
/// Empty for `query == ""`.
fn canonical_query_string(query: &str) -> String {
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

/// The canonical-headers block for exactly `signed_headers`, in the given
/// order: each line is `name:value\n` with `value` trimmed and
/// sequential-whitespace-collapsed (per the SigV4 spec); a name absent from
/// `req.headers` contributes an empty value rather than panicking — callers
/// that care (i.e. [`super::verify`]) reject that case earlier as
/// structurally invalid.
fn canonical_headers_block(req: &SigV4Request, signed_headers: &[&str]) -> String {
    let mut block = String::new();
    for name in signed_headers {
        let raw = req.headers.get(*name).map(String::as_str).unwrap_or("");
        block.push_str(name);
        block.push(':');
        block.push_str(&trim_collapse_whitespace(raw));
        block.push('\n');
    }
    block
}

/// Trim leading/trailing whitespace and collapse any run of internal spaces
/// or tabs to a single space — the SigV4 signed-header-value canonicalization
/// rule (verified against the vendored `get-header-value-trim` vector, which
/// collapses runs even inside a quoted value).
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
/// through; everything else — including `/`, since callers always invoke
/// this on an already-split path segment or a query key/value — becomes
/// `%XX` with uppercase hex digits (`%20` for space, matching the
/// `get-space` vector).
fn percent_encode(bytes: &[u8]) -> String {
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

fn hex_digit_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

// --- internal: hex + HMAC chain ------------------------------------------

/// Lowercase hex encoding (hand-rolled — the ADR is explicit that this is
/// not worth a third dependency): used for the payload hash, the
/// string-to-sign's canonical-request hash, and the final signature, all of
/// which AWS renders lowercase.
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

/// The SigV4 signing-key chain (`kSecret` → `kDate` → `kRegion` → `kService`
/// → `kSigning`), applied to `string_to_sign` and hex-encoded — the raw
/// `Signature` value (without the surrounding `Authorization` header
/// scaffolding; see [`sign`] for that).
fn signature(
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

/// Constant-*shape* comparison: always walks the full length of the longer
/// input (no early return on a length or byte mismatch), so the amount of
/// work done does not depend on where — or whether — the inputs diverge.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn creds(akid: &str, secret: &str) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert(akid.to_string(), secret.to_string());
        m
    }

    const AKID: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const DATE: &str = "20150830T123600Z";
    // now_epoch_ms exactly at 20150830T123600Z.
    const NOW_MS: u64 = 1_440_938_160_000;

    fn signed_request<'a>(hmap: &'a BTreeMap<String, String>, body: &'a [u8]) -> SigV4Request<'a> {
        SigV4Request {
            method: "GET",
            path: "/",
            query: "",
            headers: hmap,
            body,
        }
    }

    fn base_headers() -> BTreeMap<String, String> {
        headers(&[("host", "example.amazonaws.com"), ("x-amz-date", DATE)])
    }

    fn valid_headers(authz: String) -> BTreeMap<String, String> {
        let mut m = base_headers();
        m.insert("authorization".to_string(), authz);
        m
    }

    /// Sign the vanilla `GET /` request using exactly the headers `verify`
    /// will see (`base_headers()`) — signing against a different header set
    /// than the one later verified would produce an authz that can never
    /// match, which is not what these tests are exercising.
    fn sign_vanilla() -> String {
        let hmap = base_headers();
        let req = SigV4Request {
            method: "GET",
            path: "/",
            query: "",
            headers: &hmap,
            body: b"",
        };
        sign(
            &req,
            AKID,
            SECRET,
            DATE,
            "us-east-1",
            "service",
            &["host", "x-amz-date"],
        )
    }

    #[test]
    fn parse_amz_date_round_trips_through_format_amz_date() {
        let ts = parse_amz_date(DATE).unwrap();
        assert_eq!(format_amz_date(ts), DATE);
    }

    #[test]
    fn parse_amz_date_rejects_bad_shapes() {
        for bad in [
            "",
            "20150830T123600",  // missing Z
            "20150830 123600Z", // missing T
            "2015-08-30T12:36:00Z",
            "20150830T1236000Z", // too long
            "20150830T12360AZ",  // non-digit
            "20151330T123600Z",  // month 13
            "20150832T123600Z",  // day 32
        ] {
            assert!(parse_amz_date(bad).is_none(), "expected None for {bad:?}");
        }
    }

    #[test]
    fn constant_time_eq_matches_naive_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn verify_accepts_a_correctly_signed_request() {
        let authz = sign_vanilla();
        let hmap = valid_headers(authz);
        let req = signed_request(&hmap, b"");
        assert_eq!(verify(&req, &creds(AKID, SECRET), NOW_MS), Ok(()));
    }

    #[test]
    fn verify_rejects_a_tampered_body() {
        let authz = sign_vanilla();
        let hmap = valid_headers(authz);
        let req = signed_request(&hmap, b"tampered");
        assert_eq!(
            verify(&req, &creds(AKID, SECRET), NOW_MS),
            Err(SigV4Error::SignatureMismatch)
        );
    }

    #[test]
    fn verify_rejects_the_wrong_secret() {
        let authz = sign_vanilla();
        let hmap = valid_headers(authz);
        let req = signed_request(&hmap, b"");
        assert_eq!(
            verify(&req, &creds(AKID, "not-the-secret"), NOW_MS),
            Err(SigV4Error::SignatureMismatch)
        );
    }

    #[test]
    fn verify_rejects_an_unknown_access_key() {
        let authz = sign_vanilla();
        let hmap = valid_headers(authz);
        let req = signed_request(&hmap, b"");
        assert_eq!(
            verify(&req, &creds("SOMEONE-ELSE", SECRET), NOW_MS),
            Err(SigV4Error::UnrecognizedClient)
        );
    }

    #[test]
    fn verify_rejects_an_absent_authorization_header() {
        let hmap = headers(&[("host", "example.amazonaws.com"), ("x-amz-date", DATE)]);
        let req = signed_request(&hmap, b"");
        assert_eq!(
            verify(&req, &creds(AKID, SECRET), NOW_MS),
            Err(SigV4Error::MissingAuthenticationToken)
        );
    }

    #[test]
    fn verify_rejects_a_malformed_authorization_header() {
        for bad in [
            "",
            "Bearer sometoken",
            "AWS4-HMAC-SHA256",
            "AWS4-HMAC-SHA256 Credential=AKID/20150830/us-east-1/service/aws4_request",
            "AWS4-HMAC-SHA256 Credential=AKID, SignedHeaders=host, Signature=abc",
            "AWS4-HMAC-SHA256 Credential=AKID/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=my-header, Signature=abc",
        ] {
            let hmap = headers(&[
                ("host", "example.amazonaws.com"),
                ("x-amz-date", DATE),
                ("authorization", bad),
            ]);
            let req = signed_request(&hmap, b"");
            assert_eq!(
                verify(&req, &creds(AKID, SECRET), NOW_MS),
                Err(SigV4Error::MissingAuthenticationToken),
                "expected MissingAuthenticationToken for {bad:?}"
            );
        }
    }

    #[test]
    fn verify_rejects_signed_headers_missing_host() {
        let empty = BTreeMap::new();
        let req_for_sign = SigV4Request {
            method: "GET",
            path: "/",
            query: "",
            headers: &empty,
            body: b"",
        };
        let authz = sign(
            &req_for_sign,
            AKID,
            SECRET,
            DATE,
            "us-east-1",
            "service",
            &["x-amz-date"], // no "host"
        );
        let hmap = headers(&[
            ("host", "example.amazonaws.com"),
            ("x-amz-date", DATE),
            ("authorization", &authz),
        ]);
        let req = signed_request(&hmap, b"");
        assert_eq!(
            verify(&req, &creds(AKID, SECRET), NOW_MS),
            Err(SigV4Error::MissingAuthenticationToken)
        );
    }

    #[test]
    fn verify_rejects_a_scope_date_mismatched_with_x_amz_date() {
        let authz = format!(
            "AWS4-HMAC-SHA256 Credential={AKID}/20150101/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=deadbeef"
        );
        let hmap = valid_headers(authz);
        let req = signed_request(&hmap, b"");
        assert_eq!(
            verify(&req, &creds(AKID, SECRET), NOW_MS),
            Err(SigV4Error::SignatureMismatch)
        );
    }

    #[test]
    fn verify_rejects_a_non_aws4_request_scope_terminal() {
        let authz = format!(
            "AWS4-HMAC-SHA256 Credential={AKID}/20150830/us-east-1/service/not_aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=deadbeef"
        );
        let hmap = valid_headers(authz);
        let req = signed_request(&hmap, b"");
        assert_eq!(
            verify(&req, &creds(AKID, SECRET), NOW_MS),
            Err(SigV4Error::SignatureMismatch)
        );
    }

    #[test]
    fn verify_rejects_a_malformed_x_amz_date_value() {
        let authz = format!(
            "AWS4-HMAC-SHA256 Credential={AKID}/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=deadbeef"
        );
        let hmap = headers(&[
            ("host", "example.amazonaws.com"),
            ("x-amz-date", "not-a-date"),
            ("authorization", &authz),
        ]);
        let req = signed_request(&hmap, b"");
        assert_eq!(
            verify(&req, &creds(AKID, SECRET), NOW_MS),
            Err(SigV4Error::SignatureMismatch)
        );
    }

    #[test]
    fn verify_rejects_a_request_too_far_in_the_past() {
        let authz = sign_vanilla();
        let hmap = valid_headers(authz);
        let req = signed_request(&hmap, b"");
        // now is 10 minutes after the request's X-Amz-Date: outside the
        // +/-5 minute window, on the "too old" side.
        let now_ms = NOW_MS + 10 * 60 * 1000;
        match verify(&req, &creds(AKID, SECRET), now_ms) {
            Err(SigV4Error::SignatureExpired { earlier, .. }) => assert!(earlier),
            other => panic!("expected SignatureExpired{{earlier: true}}, got {other:?}"),
        }
    }

    #[test]
    fn verify_rejects_a_request_too_far_in_the_future() {
        let authz = sign_vanilla();
        let hmap = valid_headers(authz);
        let req = signed_request(&hmap, b"");
        // now is 10 minutes before the request's X-Amz-Date.
        let now_ms = NOW_MS - 10 * 60 * 1000;
        match verify(&req, &creds(AKID, SECRET), now_ms) {
            Err(SigV4Error::SignatureExpired { earlier, .. }) => assert!(!earlier),
            other => panic!("expected SignatureExpired{{earlier: false}}, got {other:?}"),
        }
    }

    #[test]
    fn verify_accepts_a_request_exactly_at_the_skew_boundary() {
        let authz = sign_vanilla();
        let hmap = valid_headers(authz);
        let req = signed_request(&hmap, b"");
        // Exactly 5 minutes late/early is still within the inclusive window.
        assert!(verify(&req, &creds(AKID, SECRET), NOW_MS + 300_000).is_ok());
        assert!(verify(&req, &creds(AKID, SECRET), NOW_MS - 300_000).is_ok());
    }

    #[test]
    fn error_wire_shapes_match_the_adr_table() {
        assert_eq!(
            SigV4Error::MissingAuthenticationToken.error_code(),
            "MissingAuthenticationTokenException"
        );
        assert_eq!(
            SigV4Error::MissingAuthenticationToken.type_name(),
            "com.amazon.coral.service#MissingAuthenticationTokenException"
        );
        assert_eq!(
            SigV4Error::MissingAuthenticationToken.message(),
            "Request is missing Authentication Token"
        );

        assert_eq!(
            SigV4Error::UnrecognizedClient.error_code(),
            "UnrecognizedClientException"
        );
        assert_eq!(
            SigV4Error::UnrecognizedClient.type_name(),
            "com.amazon.coral.service#UnrecognizedClientException"
        );
        assert_eq!(
            SigV4Error::UnrecognizedClient.message(),
            "The security token included in the request is invalid."
        );

        assert_eq!(
            SigV4Error::SignatureMismatch.error_code(),
            "InvalidSignatureException"
        );
        assert_eq!(
            SigV4Error::SignatureMismatch.type_name(),
            "com.amazon.coral.service#InvalidSignatureException"
        );
        assert_eq!(
            SigV4Error::SignatureMismatch.message(),
            "The request signature we calculated does not match the signature you provided. \
             Check your AWS Secret Access Key and signing method. Consult the service \
             documentation for details."
        );

        let expired = SigV4Error::SignatureExpired {
            amz_date: "20150830T000000Z".to_string(),
            bound: "20150830T123100Z".to_string(),
            now: "20150830T123600Z".to_string(),
            earlier: true,
        };
        assert_eq!(expired.error_code(), "InvalidSignatureException");
        assert_eq!(
            expired.message(),
            "Signature expired: 20150830T000000Z is now earlier than 20150830T123100Z \
             (20150830T123600Z - 5 min.)"
        );
    }
}
