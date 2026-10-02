//! End-to-end regression for issue #855: `S3Client::execute` used to build
//! the query string by joining raw key/value pairs with `&` and then
//! recovering them by splitting on `&` again — a value containing a
//! literal `&` (S3 explicitly allows `&` in an object key, and ADR 0068's
//! `ImportTable` `S3KeyPrefix` is an unrestricted customer string that
//! flows straight into a `list_objects_v2` prefix) silently corrupted the
//! request: truncated at the embedded `&`, with the remainder reappearing
//! as a spurious extra parameter, self-consistently signed either way.
//!
//! Drives `S3Client::list_objects_v2` over a small recording [`Transport`]
//! (not `fake::FakeS3` — this test wants to inspect the exact wire request
//! `S3Client` produced, not have it re-interpreted by a second double) and
//! asserts the wire query is exactly the canonical SigV4 encoding, and that
//! the signature on the request verifies against the same raw pairs the
//! client built it from (proving the signed canonical form and the wire
//! form agree byte-for-byte, per SigV4's requirement).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use animus_s3::client::{HttpRequest, HttpResponse, S3Client, S3Config, Transport, TransportError};
use animus_s3::sigv4::{self, Credentials};

const NOW_MS: u64 = 1_757_073_600_000; // 2025-09-05T12:00:00Z, arbitrary.
const EMPTY_LIST_BODY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
     <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
     <IsTruncated>false</IsTruncated>\n\
     </ListBucketResult>";

/// A [`Transport`] that records the last request it was asked to send and
/// answers with a fixed, always-successful response — no interpretation of
/// the request at all, so this test observes exactly what `S3Client` built.
/// The `Arc<Mutex<..>>` is cloned before the transport is moved into
/// `S3Client::new` (which takes ownership), so the test keeps its own
/// handle to read the recorded request back afterward.
#[derive(Clone, Default)]
struct RecordingTransport {
    last: Arc<Mutex<Option<HttpRequest>>>,
}

#[async_trait]
impl Transport for RecordingTransport {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        *self.last.lock().expect("recording transport lock") = Some(request);
        Ok(HttpResponse {
            status: 200,
            headers: std::collections::BTreeMap::new(),
            body: EMPTY_LIST_BODY.as_bytes().to_vec(),
        })
    }
}

#[tokio::test]
async fn list_objects_v2_wire_query_is_the_canonical_sigv4_encoding_of_the_raw_prefix() {
    let transport = RecordingTransport::default();
    let recorder = transport.last.clone();
    let creds = Credentials::new("AKID", "secret");
    let config = S3Config {
        endpoint: "https://127.0.0.1:9000".to_string(),
        bucket: "test-bucket".to_string(),
        region: "us-east-1".to_string(),
        credentials: creds,
    };
    let client = S3Client::new(transport, config);

    let prefix = "teamA&teamB/exports/AWSDynamoDB";
    client
        .list_objects_v2(prefix, None, NOW_MS)
        .await
        .expect("list_objects_v2 against the recording transport");

    let recorded = recorder
        .lock()
        .expect("recording transport lock")
        .clone()
        .expect("a request was sent");

    // `/` is a reserved character under SigV4 *query* encoding (unlike the
    // S3 canonical-URI *path* rule, which preserves it) — so it comes out
    // as `%2F`, and `&` as `%26`. Alphabetical key sort keeps `list-type`
    // before `prefix`.
    assert_eq!(
        recorded.uri,
        "/test-bucket?list-type=2&prefix=teamA%26teamB%2Fexports%2FAWSDynamoDB"
    );

    // The signature on the wire request must verify against the SAME raw
    // pairs `S3Client` built the request from — proving the canonical form
    // that was signed is byte-identical to the one sent on the wire (the
    // property the whole "encode exactly once, from the same raw pairs"
    // fix depends on).
    let auth_header = recorded
        .headers
        .get("authorization")
        .expect("Authorization header present");
    let parsed = sigv4::parse_authorization(auth_header).expect("parses");
    let raw_pairs: &[(&str, &str)] = &[("list-type", "2"), ("prefix", prefix)];
    let payload_hash = recorded
        .headers
        .get("x-amz-content-sha256")
        .cloned()
        .unwrap_or_default();
    assert!(
        sigv4::verify_signature(
            &parsed,
            "secret",
            "GET",
            "/test-bucket",
            raw_pairs,
            &recorded.headers,
            &payload_hash,
        ),
        "the signature must verify against the exact raw pairs the client \
         built the request from"
    );

    // And, as a sanity check that this isn't vacuously true: a WRONG raw
    // prefix (the one a join-then-split bug would have silently
    // substituted) must NOT verify.
    let wrong_pairs: &[(&str, &str)] = &[("list-type", "2"), ("prefix", "teamA")];
    assert!(
        !sigv4::verify_signature(
            &parsed,
            "secret",
            "GET",
            "/test-bucket",
            wrong_pairs,
            &recorded.headers,
            &payload_hash,
        ),
        "the truncated prefix a join-then-split bug would have produced \
         must not verify against the real signature"
    );
}
