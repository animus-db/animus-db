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

use animus_control::{Metadata, Policy};
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

/// Which of a caller's candidate secrets actually verified (ADR 0066 §3) —
/// carried back so the caller (`animusd::dynamo::handle_conn`) can bump
/// `Metric::AuthRotatedSecretUsed` without re-deriving which candidate
/// matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialSource {
    /// The static `--dynamo-auth`/`dynamo_auth` bootstrap map (ADR 0066
    /// §4) — unrestricted, the pre-S-02 behaviour.
    Bootstrap,
    /// The replicated credential catalog's **current** secret for this id.
    CatalogCurrent,
    /// The replicated credential catalog's **previous** secret, still
    /// valid inside a `RotateCredential` grace window (ADR 0066 §3 step 4).
    CatalogPrevious,
}

/// The outcome of a successful [`merged_sigv4_gate`] call — enough for the
/// caller to authorize a request's operation and record which credential
/// source matched, and nothing else: **no secret**, ever.
#[derive(Debug, Clone)]
pub struct AuthOutcome {
    /// The `Credential` scope's access key id, verbatim.
    pub access_key_id: String,
    /// The `Credential` scope's region, verbatim (ADR 0057 — never pinned;
    /// carried through for `AccessDeniedException`'s synthesized table ARN,
    /// ADR 0066 §5).
    pub region: String,
    /// Which credential matched.
    pub source: CredentialSource,
    /// This credential's authorization scope — `None` only for
    /// [`CredentialSource::Bootstrap`] (unrestricted); `Some` for a catalog
    /// match, carrying the row's own [`Policy`] (ADR 0066 §1/§5).
    pub policy: Option<Policy>,
}

/// Verify `request`'s `Authorization` header against the **merged**
/// credential source ADR 0066 §3/§4 defines: the replicated credential
/// catalog first (`catalog.credential(id)` — every candidate secret in
/// `Metadata::verify_secret_candidates`, current then previous-while-in-
/// grace), falling back to the static bootstrap map only when `id` has **no
/// row in the catalog at all** (a disabled or revoked-but-still-present row
/// shadows the static entry too, per ADR 0066 §4's "the catalog always wins
/// on a shared id" — it is never treated as absent for fallback purposes,
/// only for the wire's own unrecognized-vs-disabled distinction).
///
/// `now_epoch_ms` is `env.wall_now()`'s own millisecond reading (ADR 0051);
/// the catalog's own `now_secs` convention ([`Metadata::
/// verify_secret_candidates`]'s doc) is derived from it internally, so the
/// caller supplies exactly one clock read regardless of which path is
/// taken.
///
/// # Errors
/// See [`animus_dynamo::sigv4::verify`]'s own doc for the full check order.
/// A catalog row present but disabled, or with no secret matching, never
/// falls through to the static map — see the doc above.
pub fn merged_sigv4_gate(
    request: &HttpRequest,
    catalog: &Metadata,
    static_credentials: Option<&BTreeMap<String, String>>,
    now_epoch_ms: u64,
) -> Result<AuthOutcome, SigV4Error> {
    let sigv4_req = SigV4Request {
        method: &request.method,
        path: &request.path,
        query: &request.query,
        headers: &request.headers,
        body: &request.body,
    };
    let credential = sigv4::parse_credential(&sigv4_req)?;
    let now_secs = now_epoch_ms / 1000;

    if let Some(row) = catalog.credential(&credential.access_key_id) {
        // A row exists — the catalog always wins on this id (ADR 0066 §4),
        // so a disabled row is never treated as absent for fallback
        // purposes, only for the wire's own unrecognized-vs-disabled
        // distinction (ADR 0066 §3 step 2).
        if !row.enabled {
            return Err(SigV4Error::UnrecognizedClient);
        }
        let mut last_err = SigV4Error::SignatureMismatch;
        for (idx, secret) in catalog
            .verify_secret_candidates(&credential.access_key_id, now_secs)
            .enumerate()
        {
            let candidate = BTreeMap::from([(
                credential.access_key_id.clone(),
                secret.as_str().to_string(),
            )]);
            match sigv4::verify(&sigv4_req, &candidate, now_epoch_ms) {
                Ok(()) => {
                    return Ok(AuthOutcome {
                        access_key_id: credential.access_key_id.clone(),
                        region: credential.region.clone(),
                        source: if idx == 0 {
                            CredentialSource::CatalogCurrent
                        } else {
                            CredentialSource::CatalogPrevious
                        },
                        policy: Some(row.policy.clone()),
                    });
                }
                Err(e) => last_err = e,
            }
        }
        return Err(last_err);
    }

    let empty = BTreeMap::new();
    let statics = static_credentials.unwrap_or(&empty);
    sigv4::verify(&sigv4_req, statics, now_epoch_ms)?;
    Ok(AuthOutcome {
        access_key_id: credential.access_key_id,
        region: credential.region,
        source: CredentialSource::Bootstrap,
        policy: None,
    })
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

    // --- `merged_sigv4_gate` (ADR 0066 §3/§4) ------------------------------

    fn row(
        secret: &str,
        previous: Option<(&str, u64)>,
        enabled: bool,
    ) -> animus_control::CredentialRow {
        animus_control::CredentialRow {
            secret: animus_control::SecretKey::new(secret),
            previous: previous.map(|(s, valid_until)| animus_control::PreviousSecret {
                secret: animus_control::SecretKey::new(s),
                valid_until,
            }),
            policy: Policy::allow_all(),
            enabled,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn now_secs() -> u64 {
        SIGNED_AT / 1000
    }

    #[test]
    fn merged_gate_matches_the_catalogs_current_secret() {
        let mut meta = Metadata::default();
        meta.credentials
            .insert(AKID.to_string(), row(SECRET, None, true));
        let req = signed_request(AKID, SECRET);
        let outcome = merged_sigv4_gate(&req, &meta, None, SIGNED_AT).expect("verifies");
        assert_eq!(outcome.source, CredentialSource::CatalogCurrent);
        assert!(outcome.policy.is_some());
    }

    #[test]
    fn merged_gate_matches_the_previous_secret_inside_the_grace_window() {
        let mut meta = Metadata::default();
        meta.credentials.insert(
            AKID.to_string(),
            row("new-secret", Some((SECRET, now_secs() + 60)), true),
        );
        let req = signed_request(AKID, SECRET);
        let outcome = merged_sigv4_gate(&req, &meta, None, SIGNED_AT).expect("verifies");
        assert_eq!(outcome.source, CredentialSource::CatalogPrevious);
    }

    #[test]
    fn merged_gate_rejects_the_previous_secret_once_its_grace_window_closes() {
        let mut meta = Metadata::default();
        meta.credentials.insert(
            AKID.to_string(),
            row("new-secret", Some((SECRET, now_secs())), true),
        );
        let req = signed_request(AKID, SECRET);
        let err = merged_sigv4_gate(&req, &meta, None, SIGNED_AT).unwrap_err();
        assert_eq!(err, SigV4Error::SignatureMismatch);
    }

    #[test]
    fn merged_gate_treats_a_disabled_row_as_unrecognized_never_falling_to_bootstrap() {
        let mut meta = Metadata::default();
        meta.credentials
            .insert(AKID.to_string(), row(SECRET, None, false));
        let req = signed_request(AKID, SECRET);
        let err = merged_sigv4_gate(&req, &meta, Some(&credentials()), SIGNED_AT).unwrap_err();
        assert_eq!(err, SigV4Error::UnrecognizedClient);
    }

    #[test]
    fn merged_gate_falls_through_to_the_static_bootstrap_map_when_absent_from_the_catalog() {
        let meta = Metadata::default();
        let req = signed_request(AKID, SECRET);
        let outcome =
            merged_sigv4_gate(&req, &meta, Some(&credentials()), SIGNED_AT).expect("verifies");
        assert_eq!(outcome.source, CredentialSource::Bootstrap);
        assert!(outcome.policy.is_none());
    }

    #[test]
    fn merged_gate_unknown_key_with_no_catalog_and_no_bootstrap_match_is_unrecognized() {
        let meta = Metadata::default();
        let req = signed_request("AKIDNOWHERE", SECRET);
        let err = merged_sigv4_gate(&req, &meta, Some(&credentials()), SIGNED_AT).unwrap_err();
        assert_eq!(err, SigV4Error::UnrecognizedClient);
    }
}
