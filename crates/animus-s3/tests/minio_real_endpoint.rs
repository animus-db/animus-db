//! An opt-in **real** round trip against a real S3-compatible endpoint
//! (MinIO, RustFS, localstack, real AWS S3 ...; CI runs RustFS) — `#[cfg(feature = "prod")]`, so it only
//! builds under `cargo test -p animus-s3 --features prod` (or
//! `--all-features`). Deliberately **not** `#[ignore]`d: the test always
//! runs, but does nothing and prints a skip line when
//! `ANIMUS_S3_TEST_ENDPOINT` is unset, so the workspace gates stay green
//! with no infrastructure. Setting `ANIMUS_S3_REQUIRE_ENDPOINT=1` (as CI's
//! `s3-real-endpoint` job does) turns that skip into a panic. See `CLAUDE.md`'s Testing section for exactly
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
        assert!(
            std::env::var("ANIMUS_S3_REQUIRE_ENDPOINT").as_deref() != Ok("1"),
            "ANIMUS_S3_REQUIRE_ENDPOINT=1 but ANIMUS_S3_TEST_ENDPOINT is not set: \
             refusing to skip the real-endpoint test vacuously"
        );
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

    // S-08 M2: multipart upload (2 x 5 MiB + a 1 MiB tail) and ranged GETs.
    const MIB: usize = 1024 * 1024;
    let mp_key = format!("{key}.multipart");
    let payload: Vec<u8> = (0..11 * MIB).map(|i| (i * 31 % 251) as u8).collect();
    let upload_id = client
        .create_multipart_upload(&mp_key, now_epoch_ms)
        .await
        .expect("create_multipart_upload");
    let mut parts = Vec::new();
    for (i, chunk) in payload.chunks(5 * MIB).enumerate() {
        let n = u32::try_from(i + 1).expect("part number");
        let etag = client
            .upload_part(&mp_key, &upload_id, n, chunk.to_vec(), now_epoch_ms)
            .await
            .expect("upload_part");
        parts.push((n, etag));
    }
    client
        .complete_multipart_upload(&mp_key, &upload_id, &parts, now_epoch_ms)
        .await
        .expect("complete_multipart_upload");
    let meta = client
        .head_object(&mp_key, now_epoch_ms)
        .await
        .expect("head multipart object");
    assert_eq!(meta.size, payload.len() as u64);
    assert!(
        client
            .get_object(&mp_key, now_epoch_ms)
            .await
            .expect("get multipart object")
            == payload,
        "multipart object content mismatch"
    );
    // Ranged GET: a mid-object slice spanning a part boundary, and a short tail.
    let mid = client
        .get_object_range(&mp_key, 5 * MIB as u64 - 7, 14, now_epoch_ms)
        .await
        .expect("ranged get across a part boundary");
    assert_eq!(mid, payload[5 * MIB - 7..5 * MIB + 7]);
    let tail = client
        .get_object_range(&mp_key, 10 * MIB as u64, 4 * MIB as u64, now_epoch_ms)
        .await
        .expect("ranged get with a short final range");
    assert_eq!(tail, payload[10 * MIB..]);
    match client
        .get_object_range(&mp_key, payload.len() as u64, 1, now_epoch_ms)
        .await
    {
        Err(animus_s3::client::S3Error::Service { status: 416, .. }) => {}
        other => panic!("expected 416 past the end, got {other:?}"),
    }
    // An aborted upload leaves nothing behind.
    let abort_id = client
        .create_multipart_upload(&mp_key, now_epoch_ms)
        .await
        .expect("create second upload");
    client
        .upload_part(&mp_key, &abort_id, 1, vec![1; 5 * MIB], now_epoch_ms)
        .await
        .expect("upload part to be aborted");
    client
        .abort_multipart_upload(&mp_key, &abort_id, now_epoch_ms)
        .await
        .expect("abort_multipart_upload");
    client
        .delete_object(&mp_key, now_epoch_ms)
        .await
        .expect("delete multipart object");
}
