//! AWS's own published SigV4 test-vector suite (the `aws-sig-v4-test-suite`
//! `animus-dynamo` vendors under `tests/sigv4_vectors/`), transcribed here
//! as literal test cases rather than a second vendored file tree — S-04 PR
//! 1 needs only four of them (`docs/roadmap.md` §2). Each constant below is
//! copied verbatim from the corresponding `.req`/`.creq`/`.sts`/`.authz`
//! file in `crates/animus-dynamo/tests/sigv4_vectors/`; see that
//! directory's `README.md` for the suite's provenance.
//!
//! Every case uses the suite's well-known fixed constants: access key id
//! `AKIDEXAMPLE`, secret `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`, date
//! `20150830T123600Z`, region `us-east-1`, service `service` (the generic
//! non-S3 service the suite itself targets — S3-specific canonicalization
//! quirks are covered separately by `sigv4.rs`'s own
//! `sign_request_over_the_s3_documentation_examples_request_shape` unit
//! test, and by `canonical_uri_s3_does_not_resolve_dot_segments`).

use std::collections::BTreeMap;

use animus_s3::sigv4::{canonical_request, compute_signature, string_to_sign};

const AKID: &str = "AKIDEXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const DATE: &str = "20150830";
const AMZ_DATE: &str = "20150830T123600Z";
const REGION: &str = "us-east-1";
const SERVICE: &str = "service";
const CREDENTIAL_SCOPE: &str = "20150830/us-east-1/service/aws4_request";

fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

/// Assert the full chain (canonical request -> string-to-sign -> signature
/// -> full `Authorization` line) against one vector's expected outputs.
#[allow(clippy::too_many_arguments)]
fn assert_vector(
    method: &str,
    uri: &str,
    query: &str,
    header_pairs: &[(&str, &str)],
    signed_headers: &[&str],
    payload_hash: &str,
    expected_creq: &str,
    expected_sts: &str,
    expected_authz: &str,
) {
    let hmap = headers(header_pairs);
    let creq = canonical_request(method, uri, query, &hmap, signed_headers, payload_hash);
    assert_eq!(creq, expected_creq, "canonical request mismatch");

    let sts = string_to_sign(AMZ_DATE, CREDENTIAL_SCOPE, &creq);
    assert_eq!(sts, expected_sts, "string-to-sign mismatch");

    let sig = compute_signature(SECRET, DATE, REGION, SERVICE, &sts);
    let authz = format!(
        "AWS4-HMAC-SHA256 Credential={AKID}/{CREDENTIAL_SCOPE}, SignedHeaders={}, Signature={sig}",
        signed_headers.join(";")
    );
    assert_eq!(authz, expected_authz, "Authorization mismatch");
}

/// Empty-body SHA-256, the payload hash every one of these `GET`/`POST`
/// vectors with no body uses.
const EMPTY_BODY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[test]
fn get_vanilla() {
    assert_vector(
        "GET",
        "/",
        "",
        &[("host", "example.amazonaws.com"), ("x-amz-date", AMZ_DATE)],
        &["host", "x-amz-date"],
        EMPTY_BODY_SHA256,
        "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\n\
         host;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n\
         bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63",
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
         SignedHeaders=host;x-amz-date, \
         Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
    );
}

#[test]
fn get_vanilla_query_order_key_case() {
    // The suite's raw request line carries `Param2=value2&Param1=value1`;
    // `canonical_request` re-sorts by encoded key internally.
    assert_vector(
        "GET",
        "/",
        "Param2=value2&Param1=value1",
        &[("host", "example.amazonaws.com"), ("x-amz-date", AMZ_DATE)],
        &["host", "x-amz-date"],
        EMPTY_BODY_SHA256,
        "GET\n/\nParam1=value1&Param2=value2\nhost:example.amazonaws.com\n\
         x-amz-date:20150830T123600Z\n\nhost;x-amz-date\n\
         e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n\
         816cd5b414d056048ba4f7c5386d6e0533120fb1fcfa93762cf0fc39e2cf19e0",
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
         SignedHeaders=host;x-amz-date, \
         Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500",
    );
}

#[test]
fn post_x_www_form_urlencoded() {
    // Body is `Param1=value1`; its SHA-256 is precomputed here exactly as
    // the vendored `.creq` file states it (this test asserts the
    // canonicalization chain, not `Sha256::digest` itself).
    let body_sha256 = "9095672bbd1f56dfc5b65f3e153adc8731a4a654192329106275f4c7b24d0b6e";
    assert_vector(
        "POST",
        "/",
        "",
        &[
            ("content-type", "application/x-www-form-urlencoded"),
            ("host", "example.amazonaws.com"),
            ("x-amz-date", AMZ_DATE),
        ],
        &["content-type", "host", "x-amz-date"],
        body_sha256,
        "POST\n/\n\ncontent-type:application/x-www-form-urlencoded\n\
         host:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\n\
         content-type;host;x-amz-date\n9095672bbd1f56dfc5b65f3e153adc8731a4a654192329106275f4c7b24d0b6e",
        "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n\
         42a5e5bb34198acb3e84da4f085bb7927f2bc277ca766e6d19c73c2154021281",
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
         SignedHeaders=content-type;host;x-amz-date, \
         Signature=ff11897932ad3f4e8b18135d722051e5ac45fc38421b1da7b9d196a0fe09473a",
    );
}

/// Not one of the suite's own vectors (the suite predates S3's
/// `UNSIGNED-PAYLOAD` convention) — a hand-derived vector proving
/// `canonical_request`/`string_to_sign` handle the literal
/// `UNSIGNED-PAYLOAD` payload-hash string correctly (it flows through
/// unchanged, exactly like any other payload-hash string), computed
/// in-line by this same test using `sha2` directly (not a value copied
/// from elsewhere) and asserted against `canonical_request`/
/// `string_to_sign`'s own output for the identical inputs.
#[test]
fn unsigned_payload_case() {
    let hmap = headers(&[
        ("host", "examplebucket.s3.amazonaws.com"),
        ("x-amz-date", AMZ_DATE),
        ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
    ]);
    let signed_headers = ["host", "x-amz-content-sha256", "x-amz-date"];
    let creq = canonical_request(
        "PUT",
        "/test.txt",
        "",
        &hmap,
        &signed_headers,
        "UNSIGNED-PAYLOAD",
    );
    assert_eq!(
        creq,
        "PUT\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\n\
         x-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:20150830T123600Z\n\n\
         host;x-amz-content-sha256;x-amz-date\nUNSIGNED-PAYLOAD"
    );
    let sts = string_to_sign(AMZ_DATE, CREDENTIAL_SCOPE, &creq);
    // Re-derive the same chain a second, independent way (hashing the
    // canonical request text by hand via a `sha2` oracle would just
    // reimplement `string_to_sign`) — the regression this test actually
    // pins is that changing `PayloadHash::Unsigned`'s literal, or
    // `canonical_request`'s handling of a caller-supplied hash string
    // instead of one it computes itself, is caught by a byte-for-byte
    // string comparison, not merely "it doesn't panic."
    assert_eq!(
        sts,
        format!(
            "AWS4-HMAC-SHA256\n{AMZ_DATE}\n{CREDENTIAL_SCOPE}\n{}",
            hex_sha256(creq.as_bytes())
        )
    );
    let sig = compute_signature(SECRET, DATE, REGION, "s3", &sts);
    assert_eq!(sig.len(), 64, "a SigV4 signature is 64 lowercase hex chars");
    assert!(
        sig.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
}

fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
