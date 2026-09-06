//! The signing-key-chain equivalence proof between this crate's copied HMAC
//! chain (`animus_s3::sigv4::compute_signature`) and the verifier this crate
//! deliberately does not depend on in production code
//! (`animus_dynamo::sigv4`, ADR 0057) — see `crates/animus-s3/src/sigv4.rs`'s
//! module doc for why the chain was copied rather than imported.
//!
//! `animus-dynamo` is a **dev-dependency only** here (see `Cargo.toml`) —
//! this file is the one place in the crate that names it at all.

use std::collections::BTreeMap;

use animus_dynamo::sigv4 as dynamo_sigv4;
use animus_s3::sigv4::{self as s3_sigv4, PayloadHash};

fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

/// The S3 documentation's own worked-example **request shape**
/// (<https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>)
/// signed through both crates' chains, over the identical
/// method/path/host/headers/body, and asserted to produce a
/// byte-identical `Authorization` value. **Deliberately does not assert
/// the literal signature AWS's own documentation states for this
/// example** — see `sigv4.rs`'s
/// `sign_request_over_the_s3_documentation_examples_request_shape` unit
/// test for why (no network access in this sandbox to independently
/// re-derive AWS's exact byte-for-byte canonical request from an
/// authoritative source). What this test proves instead — the actual
/// point of a signing-key-chain equivalence proof — is that
/// `animus_dynamo::sigv4`'s independently-implemented HMAC chain and this
/// crate's copied one agree on *some* deterministic signature for the
/// identical inputs, which is exactly the property "copy the chain rather
/// than depend on it" needs to hold.
#[test]
fn animus_s3_and_animus_dynamo_sign_the_s3_doc_example_identically() {
    const AKID: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const AMZ_DATE: &str = "20130524T000000Z";
    const REGION: &str = "us-east-1";
    const SERVICE: &str = "s3";

    // `animus_dynamo::sigv4::sign` always hashes `req.body` itself; an
    // empty body's SHA-256 is exactly the payload hash the S3 doc example
    // states in its own `x-amz-content-sha256` header, so both chains sign
    // against the identical payload hash without animus-s3 needing to fake
    // one in.
    let dynamo_headers = headers(&[
        ("host", "examplebucket.s3.amazonaws.com"),
        ("x-amz-date", AMZ_DATE),
        (
            "x-amz-content-sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
        ("range", "bytes=0-9"),
    ]);
    let dynamo_req = dynamo_sigv4::SigV4Request {
        method: "GET",
        path: "/test.txt",
        query: "",
        headers: &dynamo_headers,
        body: b"",
    };
    let dynamo_authz = dynamo_sigv4::sign(
        &dynamo_req,
        AKID,
        SECRET,
        AMZ_DATE,
        REGION,
        SERVICE,
        &["host", "range", "x-amz-content-sha256", "x-amz-date"],
    );

    let creds = s3_sigv4::Credentials::new(AKID, SECRET);
    let scope = s3_sigv4::SigningScope {
        region: REGION.to_string(),
        service: SERVICE.to_string(),
    };
    let payload_hash = PayloadHash::signed(b"");
    let extra = headers(&[("range", "bytes=0-9")]);
    let s3_req = s3_sigv4::RequestToSign {
        method: "GET",
        uri: "/test.txt",
        host: "examplebucket.s3.amazonaws.com",
        query: "",
        headers: &extra,
        payload_sha256_hex: &payload_hash,
        timestamp: AMZ_DATE,
    };
    let s3_signed = s3_sigv4::sign_request(&creds, &scope, &s3_req);

    assert_eq!(
        dynamo_authz, s3_signed.authorization,
        "animus_dynamo::sigv4::sign and animus_s3::sigv4::sign_request must \
         produce byte-identical Authorization headers for the same request"
    );
}

/// The chain equivalence holds independent of the request shape too — a
/// second, unrelated request/secret pair, comparing the raw
/// `compute_signature`/dynamo-internal chain outputs is not possible
/// directly (dynamo's `signature` fn is private), so this drives both
/// through their own public `sign`/`sign_request` entry points instead,
/// over a plain `POST /` request with a real body.
#[test]
fn animus_s3_and_animus_dynamo_agree_on_a_second_arbitrary_request() {
    const AKID: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const AMZ_DATE: &str = "20150830T123600Z";
    const REGION: &str = "us-west-2";
    const SERVICE: &str = "s3";
    let body = b"hello world";
    let payload_hash = PayloadHash::signed(body);

    let dynamo_headers = headers(&[
        ("host", "my-bucket.s3.us-west-2.amazonaws.com"),
        ("x-amz-date", AMZ_DATE),
        ("x-amz-content-sha256", payload_hash.as_str()),
    ]);
    let dynamo_req = dynamo_sigv4::SigV4Request {
        method: "PUT",
        path: "/my/key",
        query: "",
        headers: &dynamo_headers,
        body,
    };
    let dynamo_authz = dynamo_sigv4::sign(
        &dynamo_req,
        AKID,
        SECRET,
        AMZ_DATE,
        REGION,
        SERVICE,
        &["host", "x-amz-content-sha256", "x-amz-date"],
    );

    let creds = s3_sigv4::Credentials::new(AKID, SECRET);
    let scope = s3_sigv4::SigningScope {
        region: REGION.to_string(),
        service: SERVICE.to_string(),
    };
    let empty = BTreeMap::new();
    let s3_req = s3_sigv4::RequestToSign {
        method: "PUT",
        uri: "/my/key",
        host: "my-bucket.s3.us-west-2.amazonaws.com",
        query: "",
        headers: &empty,
        payload_sha256_hex: &payload_hash,
        timestamp: AMZ_DATE,
    };
    let s3_signed = s3_sigv4::sign_request(&creds, &scope, &s3_req);

    assert_eq!(dynamo_authz, s3_signed.authorization);
}
