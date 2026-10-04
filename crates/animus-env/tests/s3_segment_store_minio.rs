//! Opt-in real-endpoint round trip for [`animus_env::S3SegmentStore`] (S-04
//! PR 2) — mirrors `animus-s3`'s own `tests/minio_real_endpoint.rs` exactly,
//! but drives the higher `SegmentStore` layer's
//! `assert_segment_store_contract` instead of raw client calls, so it proves
//! the store's own write-once/list-pagination logic against a real S3-
//! compatible endpoint (MinIO, RustFS, ...; CI runs RustFS), not just the underlying signer/client.
//!
//! **Unset `ANIMUS_S3_TEST_ENDPOINT` ⇒ this test prints a skip line and
//! returns immediately** unless `ANIMUS_S3_REQUIRE_ENDPOINT=1`, which
//! makes it panic instead (CI's `s3-real-endpoint` job) (never `#[ignore]`d — `cargo test -p animus-env
//! --all-features` always runs it, it just does nothing without this
//! variable), so the workspace gates stay green with no MinIO/localstack
//! infrastructure.
//!
//! To actually run it: start a local MinIO (`docker run -p 9000:9000
//! minio/minio server /data`), create a bucket, then:
//! ```text
//! ANIMUS_S3_TEST_ENDPOINT=http://127.0.0.1:9000 \
//! ANIMUS_S3_TEST_BUCKET=test-bucket \
//! ANIMUS_S3_TEST_ACCESS_KEY_ID=minioadmin \
//! ANIMUS_S3_TEST_SECRET_ACCESS_KEY=minioadmin \
//! cargo test -p animus-env --features prod --test s3_segment_store_minio -- --nocapture
//! ```
#![cfg(feature = "prod")]

use animus_env::{MultipartConfig, S3SegmentStore, SegmentStore as _};
use animus_s3::client::S3Config;
use animus_s3::prod::HyperRustlsTransport;
use animus_s3::sigv4::Credentials;

#[tokio::test]
async fn s3_segment_store_contract_against_a_real_endpoint() {
    let Ok(endpoint) = std::env::var("ANIMUS_S3_TEST_ENDPOINT") else {
        assert!(
            std::env::var("ANIMUS_S3_REQUIRE_ENDPOINT").as_deref() != Ok("1"),
            "ANIMUS_S3_REQUIRE_ENDPOINT=1 but ANIMUS_S3_TEST_ENDPOINT is not set: \
             refusing to skip the real-endpoint test vacuously"
        );
        println!(
            "ANIMUS_S3_TEST_ENDPOINT not set — skipping S3SegmentStore real-endpoint round trip"
        );
        return;
    };
    let bucket = std::env::var("ANIMUS_S3_TEST_BUCKET")
        .expect("ANIMUS_S3_TEST_BUCKET must be set alongside ANIMUS_S3_TEST_ENDPOINT");
    let access_key_id = std::env::var("ANIMUS_S3_TEST_ACCESS_KEY_ID")
        .expect("ANIMUS_S3_TEST_ACCESS_KEY_ID must be set alongside ANIMUS_S3_TEST_ENDPOINT");
    // Never printed, never included in a panic/assert message — read once
    // into a `Credentials` (whose own `Debug` already redacts it) and
    // nowhere else, mirroring `animus-s3`'s own `minio_real_endpoint.rs`.
    let secret_access_key = std::env::var("ANIMUS_S3_TEST_SECRET_ACCESS_KEY")
        .expect("ANIMUS_S3_TEST_SECRET_ACCESS_KEY must be set alongside ANIMUS_S3_TEST_ENDPOINT");

    let transport = if endpoint.starts_with("http://") {
        HyperRustlsTransport::new_allow_insecure_http()
    } else {
        HyperRustlsTransport::new()
    }
    .expect("install rustls ring crypto provider");

    let config = S3Config {
        endpoint,
        bucket,
        region: "us-east-1".to_string(),
        credentials: Credentials::new(access_key_id, secret_access_key),
    };
    let store = S3SegmentStore::new(
        transport,
        config,
        Some("animus-env-contract-test".to_string()),
    )
    // S-08 M2: a 6 MiB threshold with 5 MiB parts makes the 11 MiB object
    // below a 3-part multipart upload (5 + 5 + 1 MiB) on the real endpoint.
    .with_multipart(MultipartConfig::new(6 * MIB, 5 * MIB).expect("valid multipart config"));
    animus_env::test_support::assert_segment_store_contract(&store).await;

    let id = "multipart/object-11mib";
    let payload: Vec<u8> = (0..(11 * MIB) as usize)
        .map(|i| (i * 17 % 251) as u8)
        .collect();
    store.put(id, &payload).await.expect("multipart put");
    assert!(
        store.get(id).await.expect("get").as_deref() == Some(payload.as_slice()),
        "multipart object content mismatch"
    );
    // Idempotent re-put (HEAD + ranged compare), then a same-size differing
    // re-put must be a write-once violation.
    store.put(id, &payload).await.expect("idempotent re-put");
    let mut other = payload.clone();
    *other.last_mut().expect("non-empty") ^= 0xff;
    let err = store
        .put(id, &other)
        .await
        .expect_err("write-once violation");
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    store.delete(id).await.expect("cleanup");
}

const MIB: u64 = 1024 * 1024;
