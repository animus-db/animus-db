//! An opt-in **real** round trip against a real S3-compatible endpoint
//! (MinIO/localstack/real AWS S3) — `#[cfg(feature = "prod")]`, so it only
//! builds under `cargo test -p animus-s3 --features prod` (or
//! `--all-features`). Deliberately **not** `#[ignore]`d: the test always
//! runs, but does nothing and prints a skip line when
//! `ANIMUS_S3_TEST_ENDPOINT` is unset, so the workspace gates stay green
//! with no infrastructure. See `CLAUDE.md`'s Testing section for exactly
//! how to run this against a local MinIO.
#![cfg(feature = "prod")]

use animus_s3::client::{S3Client, S3Config};
use animus_s3::prod::HyperRustlsTransport;
use animus_s3::sigv4::Credentials;

#[tokio::test]
#[allow(
    clippy::disallowed_methods,
    reason = "opt-in real-endpoint test, gated on ANIMUS_S3_TEST_ENDPOINT and \
              skipped otherwise — SystemTime::now() here only picks a unique \
              test-object key/timestamp for a real MinIO round trip, never a \
              deadline/election/backoff this repo's determinism rule protects"
)]
async fn real_endpoint_put_get_list_delete_round_trip() {
    let Ok(endpoint) = std::env::var("ANIMUS_S3_TEST_ENDPOINT") else {
        eprintln!(
            "skipping real_endpoint_put_get_list_delete_round_trip: \
             ANIMUS_S3_TEST_ENDPOINT is not set (see crates/animus-s3/CLAUDE.md)"
        );
        return;
    };
    let bucket = std::env::var("ANIMUS_S3_TEST_BUCKET")
        .expect("ANIMUS_S3_TEST_BUCKET must be set alongside ANIMUS_S3_TEST_ENDPOINT");
    let access_key_id = std::env::var("ANIMUS_S3_TEST_ACCESS_KEY_ID")
        .expect("ANIMUS_S3_TEST_ACCESS_KEY_ID must be set alongside ANIMUS_S3_TEST_ENDPOINT");
    let secret_access_key = std::env::var("ANIMUS_S3_TEST_SECRET_ACCESS_KEY")
        .expect("ANIMUS_S3_TEST_SECRET_ACCESS_KEY must be set alongside ANIMUS_S3_TEST_ENDPOINT");
    // Deliberately never logged/formatted beyond this point — see
    // `Credentials`'s own redacting `Debug`.
    let credentials = Credentials::new(access_key_id, secret_access_key);

    let transport = if endpoint.starts_with("http://") {
        HyperRustlsTransport::new_allow_insecure_http()
    } else {
        HyperRustlsTransport::new()
    }
    .expect("build transport");

    let client = S3Client::new(
        transport,
        S3Config {
            endpoint,
            bucket,
            region: std::env::var("ANIMUS_S3_TEST_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string()),
            credentials,
        },
    );

    let now_epoch_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as u64;
    let key = format!("animus-s3-minio-test/{now_epoch_ms}/{}", std::process::id());

    client
        .put_object(
            &key,
            b"animus-s3 real-endpoint round trip".to_vec(),
            now_epoch_ms,
        )
        .await
        .expect("put_object against the real endpoint");

    let got = client
        .get_object(&key, now_epoch_ms)
        .await
        .expect("get_object against the real endpoint");
    assert_eq!(got, b"animus-s3 real-endpoint round trip");

    let meta = client
        .head_object(&key, now_epoch_ms)
        .await
        .expect("head_object against the real endpoint");
    assert_eq!(meta.size, got.len() as u64);

    let page = client
        .list_objects_v2("animus-s3-minio-test/", None, now_epoch_ms)
        .await
        .expect("list_objects_v2 against the real endpoint");
    assert!(
        page.objects.iter().any(|o| o.key == key),
        "expected {key:?} in the listing, got {:?}",
        page.objects
    );

    client
        .delete_object(&key, now_epoch_ms)
        .await
        .expect("delete_object against the real endpoint");

    match client.get_object(&key, now_epoch_ms).await {
        Err(animus_s3::client::S3Error::NotFound) => {}
        other => panic!("expected NotFound after delete, got {other:?}"),
    }
}
