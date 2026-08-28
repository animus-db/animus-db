//! The pure SigV4 authentication gate (ADR 0061 rung C4b, ADR 0057) — the
//! build-request/verify sequence that used to be entangled inside
//! `animusd::dynamo::handle_conn`. `animus_dynamo::sigv4::verify` was
//! already pure (and `animus-dynamo` has no `tokio` dependency at all); the
//! only thing keeping the *call* out of `animus-node` was `handle_conn`
//! itself reading `ctx.env.wall_now()` inline. [`sigv4_gate`] takes that
//! value as a plain parameter instead of ever reading a clock — the same
//! discipline `animus_dynamo::ttl`/`sigv4::verify` already follow, and the
//! one this crate's own `disallowed_methods` lint would enforce even if the
//! discipline slipped (there is no `env`/`Instant`/`SystemTime` to reach for
//! here regardless — this crate has no `tokio`/`prod` surface at all).
//!
//! `animusd::dynamo::handle_conn` becomes a thin wrapper: build nothing
//! itself, just call `env.wall_now()` and hand the result plus the parsed
//! [`crate::http::HttpRequest`] and the credential store straight into
//! [`sigv4_gate`], then render a failure via its own (unmoved)
//! `sigv4_error_body` — the AWS-namespace JSON rendering stays in `animusd`
//! since it's a wire-response-shaping concern, not an auth decision.

use std::collections::BTreeMap;

use animus_dynamo::sigv4::{self, SigV4Error, SigV4Request};

use crate::http::HttpRequest;

/// Verify `request`'s `Authorization` header against `credentials`
/// (access-key-id → secret-access-key) as of `now_epoch_ms` — the exact
/// build-`SigV4Request`-then-`verify` sequence `animusd::dynamo::handle_conn`
/// used to run inline, now callable with no socket and no clock in reach.
/// Returns the structured [`SigV4Error`] (not a rendered string) so a
/// caller can still map it to whatever wire error shape it wants — see
/// `animusd::dynamo::sigv4_error_body`, which stays in `animusd` since it
/// renders the DynamoDB-wire-specific `com.amazon.coral.service#…` body.
///
/// # Errors
/// See [`animus_dynamo::sigv4::verify`]'s own doc for the full check order
/// (structural validity → unknown access key → clock skew →
/// credential-scope + signature compare).
pub fn sigv4_gate(
    request: &HttpRequest,
    credentials: &BTreeMap<String, String>,
    now_epoch_ms: u64,
) -> Result<(), SigV4Error> {
    let sigv4_req = SigV4Request {
        method: &request.method,
        path: &request.path,
        query: &request.query,
        headers: &request.headers,
        body: &request.body,
    };
    sigv4::verify(&sigv4_req, credentials, now_epoch_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_dynamo::sigv4::sign;

    /// Build a minimally valid, correctly-signed `HttpRequest` for `akid`/
    /// `secret`, signed at [`AMZ_DATE`] — using `animus_dynamo::sigv4::sign`
    /// (the module's own test signer, ADR 0057) rather than hand-rolling the
    /// canonical-request/string-to-sign/HMAC chain here, so this module's
    /// tests need no real socket and no vendored fixture to prove the gate
    /// wires `verify` correctly end to end.
    fn signed_request(akid: &str, secret: &str) -> HttpRequest {
        let mut headers = BTreeMap::new();
        headers.insert("host".to_string(), "animus.local".to_string());
        headers.insert("x-amz-date".to_string(), AMZ_DATE.to_string());
        let body: &[u8] = b"{}";
        let signed_headers = ["host", "x-amz-date"];
        let req = SigV4Request {
            method: "POST",
            path: "/",
            query: "",
            headers: &headers,
            body,
        };
        let authorization = sign(
            &req,
            akid,
            secret,
            AMZ_DATE,
            "us-east-1",
            "dynamodb",
            &signed_headers,
        );
        headers.insert("authorization".to_string(), authorization);

        HttpRequest {
            method: "POST".to_string(),
            path: "/".to_string(),
            query: String::new(),
            target: String::new(),
            headers,
            body: body.to_vec(),
            keep_alive: true,
        }
    }

    const AKID: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    /// 2026-01-01T00:00:00Z, both as the signing time and (by default) "now"
    /// below — an arbitrary fixed instant, confirmed to correspond to
    /// [`SIGNED_AT`] via `date -u -d @1767225600`.
    const AMZ_DATE: &str = "20260101T000000Z";
    const SIGNED_AT: u64 = 1_767_225_600_000;

    fn credentials() -> BTreeMap<String, String> {
        BTreeMap::from([(AKID.to_string(), SECRET.to_string())])
    }

    #[test]
    fn a_correctly_signed_request_is_accepted() {
        let req = signed_request(AKID, SECRET);
        assert_eq!(sigv4_gate(&req, &credentials(), SIGNED_AT), Ok(()));
    }

    #[test]
    fn a_bad_signature_is_rejected() {
        let mut req = signed_request(AKID, SECRET);
        // Tamper with the body after signing — the signature no longer
        // covers this exact payload.
        req.body = b"{\"tampered\":true}".to_vec();
        let err = sigv4_gate(&req, &credentials(), SIGNED_AT).unwrap_err();
        assert_eq!(err, SigV4Error::SignatureMismatch);
    }

    #[test]
    fn an_unknown_access_key_is_rejected() {
        let req = signed_request("AKIDUNKNOWN", SECRET);
        let err = sigv4_gate(&req, &credentials(), SIGNED_AT).unwrap_err();
        assert_eq!(err, SigV4Error::UnrecognizedClient);
    }

    #[test]
    fn missing_authorization_header_is_rejected() {
        let mut req = signed_request(AKID, SECRET);
        req.headers.remove("authorization");
        let err = sigv4_gate(&req, &credentials(), SIGNED_AT).unwrap_err();
        assert_eq!(err, SigV4Error::MissingAuthenticationToken);
    }

    #[test]
    fn skew_just_outside_five_minutes_in_the_future_is_rejected() {
        let req = signed_request(AKID, SECRET);
        // "now" is more than 5 minutes before the signed time.
        let now = SIGNED_AT - (5 * 60 + 1) * 1000;
        let err = sigv4_gate(&req, &credentials(), now).unwrap_err();
        assert!(matches!(
            err,
            SigV4Error::SignatureExpired { earlier: false, .. }
        ));
    }

    #[test]
    fn skew_just_outside_five_minutes_in_the_past_is_rejected() {
        let req = signed_request(AKID, SECRET);
        // "now" is more than 5 minutes after the signed time.
        let now = SIGNED_AT + (5 * 60 + 1) * 1000;
        let err = sigv4_gate(&req, &credentials(), now).unwrap_err();
        assert!(matches!(
            err,
            SigV4Error::SignatureExpired { earlier: true, .. }
        ));
    }

    #[test]
    fn skew_exactly_at_the_five_minute_boundary_is_accepted() {
        let req = signed_request(AKID, SECRET);
        let now = SIGNED_AT + 5 * 60 * 1000;
        assert_eq!(sigv4_gate(&req, &credentials(), now), Ok(()));
    }
}
