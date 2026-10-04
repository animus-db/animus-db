//! S-08 M2: multipart upload and ranged GET against `FakeS3` — path-style,
//! virtual-hosted and session-token variants. Deterministic: explicit `now`.

use std::sync::Arc;

use animus_s3::client::{Addressing, S3Client, S3Config, S3Error, S3Target};
use animus_s3::creds::StaticProvider;
use animus_s3::fake::FakeS3;
use animus_s3::sigv4::Credentials;

const T0: u64 = 1_700_000_000_000;
const BUCKET: &str = "test-bucket";
const PART: usize = 16;

fn fake() -> FakeS3 {
    FakeS3::new(BUCKET)
        .with_credential("AKID", "secret")
        .with_min_part_size(PART)
}

fn client(fake: FakeS3) -> S3Client<FakeS3> {
    S3Client::new(
        fake,
        S3Config {
            endpoint: "https://127.0.0.1:9000".to_string(),
            bucket: BUCKET.to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKID", "secret"),
        },
    )
}

fn target() -> S3Target {
    S3Target {
        endpoint: "https://s3.example.com".to_string(),
        bucket: BUCKET.to_string(),
        region: "us-east-1".to_string(),
    }
}

fn data(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i * 7 % 251) as u8).collect()
}

/// Upload `payload` in `PART`-sized parts and complete it.
async fn multipart_put<T: animus_s3::client::Transport>(
    c: &S3Client<T>,
    key: &str,
    payload: &[u8],
) -> Result<(), S3Error> {
    let id = c.create_multipart_upload(key, T0).await?;
    let mut parts = Vec::new();
    for (i, chunk) in payload.chunks(PART).enumerate() {
        let n = u32::try_from(i + 1).expect("part number");
        let etag = c.upload_part(key, &id, n, chunk.to_vec(), T0).await?;
        parts.push((n, etag));
    }
    c.complete_multipart_upload(key, &id, &parts, T0).await
}

#[tokio::test]
async fn multipart_round_trip_path_style() {
    let c = client(fake());
    let payload = data(PART * 3 + 5);
    multipart_put(&c, "dir/big.bin", &payload)
        .await
        .expect("mp");
    assert_eq!(c.get_object("dir/big.bin", T0).await.expect("get"), payload);
    assert_eq!(
        c.head_object("dir/big.bin", T0).await.expect("head").size,
        payload.len() as u64
    );
}

#[tokio::test]
async fn multipart_round_trip_virtual_hosted() {
    let c = S3Client::with_provider(
        fake(),
        target(),
        Arc::new(StaticProvider::new(Credentials::new("AKID", "secret"))),
    )
    .with_addressing(Addressing::VirtualHosted)
    .expect("vhost");
    let payload = data(PART * 2 + 1);
    multipart_put(&c, "a/b c.bin", &payload).await.expect("mp");
    assert_eq!(c.get_object("a/b c.bin", T0).await.expect("get"), payload);
    let r = c
        .get_object_range("a/b c.bin", 3, 10, T0)
        .await
        .expect("range");
    assert_eq!(r, payload[3..13]);
}

#[tokio::test]
async fn multipart_with_session_token_credentials() {
    let f = fake();
    f.register_session_credential("ASIA1", "sec1", "tok1", T0 + 3_600_000);
    let creds = Credentials::new("ASIA1", "sec1").with_session_token("tok1", None);
    let c = S3Client::with_provider(f, target(), Arc::new(StaticProvider::new(creds)));
    let payload = data(PART * 2 + 3);
    multipart_put(&c, "k", &payload).await.expect("mp");
    assert_eq!(c.get_object("k", T0).await.expect("get"), payload);
    assert_eq!(
        c.get_object_range("k", 0, 4, T0).await.expect("range"),
        payload[..4]
    );
}

#[tokio::test]
async fn abort_removes_the_open_upload() {
    let c = client(fake());
    let id = c.create_multipart_upload("k", T0).await.expect("create");
    c.upload_part("k", &id, 1, data(PART), T0)
        .await
        .expect("part");
    c.abort_multipart_upload("k", &id, T0).await.expect("abort");
    // Idempotent.
    c.abort_multipart_upload("k", &id, T0)
        .await
        .expect("abort again");
    assert!(matches!(
        c.get_object("k", T0).await,
        Err(S3Error::NotFound)
    ));
}

#[tokio::test]
async fn open_upload_count_tracks_lifecycle() {
    let f = Arc::new(fake());
    let c = S3Client::new(
        f.clone(),
        S3Config {
            endpoint: "https://127.0.0.1:9000".to_string(),
            bucket: BUCKET.to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKID", "secret"),
        },
    );
    let id = c.create_multipart_upload("k", T0).await.expect("create");
    assert_eq!(f.open_upload_count(), 1);
    c.abort_multipart_upload("k", &id, T0).await.expect("abort");
    assert_eq!(f.open_upload_count(), 0);
    multipart_put(&c, "k2", &data(PART + 2)).await.expect("mp");
    assert_eq!(f.open_upload_count(), 0, "complete closes the upload");
}

#[tokio::test]
async fn complete_rejects_out_of_order_missing_and_bad_etag() {
    let c = client(fake());
    let id = c.create_multipart_upload("k", T0).await.expect("create");
    let e1 = c
        .upload_part("k", &id, 1, data(PART), T0)
        .await
        .expect("p1");
    let e2 = c
        .upload_part("k", &id, 2, data(PART + 1), T0)
        .await
        .expect("p2");

    let code = |r: Result<(), S3Error>| match r {
        Err(S3Error::Service { code, .. }) => code,
        other => panic!("expected Service error, got {other:?}"),
    };
    // Descending order.
    assert_eq!(
        code(
            c.complete_multipart_upload("k", &id, &[(2, e2.clone()), (1, e1.clone())], T0)
                .await
        ),
        "InvalidPartOrder"
    );
    // Missing part 3.
    assert_eq!(
        code(
            c.complete_multipart_upload("k", &id, &[(1, e1.clone()), (3, e2.clone())], T0)
                .await
        ),
        "InvalidPart"
    );
    // Wrong ETag for part 2.
    assert_eq!(
        code(
            c.complete_multipart_upload("k", &id, &[(1, e1.clone()), (2, e1.clone())], T0)
                .await
        ),
        "InvalidPart"
    );
    // The upload is still open and can still complete correctly.
    c.complete_multipart_upload("k", &id, &[(1, e1), (2, e2)], T0)
        .await
        .expect("good complete");
}

#[tokio::test]
async fn complete_enforces_minimum_non_last_part_size() {
    let c = client(fake());
    let id = c.create_multipart_upload("k", T0).await.expect("create");
    let e1 = c
        .upload_part("k", &id, 1, data(PART - 1), T0)
        .await
        .expect("p1");
    let e2 = c.upload_part("k", &id, 2, data(3), T0).await.expect("p2");
    match c
        .complete_multipart_upload("k", &id, &[(1, e1), (2, e2)], T0)
        .await
    {
        Err(S3Error::Service { code, .. }) => assert_eq!(code, "EntityTooSmall"),
        other => panic!("expected EntityTooSmall, got {other:?}"),
    }
}

#[tokio::test]
async fn error_document_inside_a_200_is_a_failure() {
    let f = Arc::new(fake());
    let c = S3Client::new(
        f.clone(),
        S3Config {
            endpoint: "https://127.0.0.1:9000".to_string(),
            bucket: BUCKET.to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKID", "secret"),
        },
    );
    let id = c.create_multipart_upload("k", T0).await.expect("create");
    let e1 = c
        .upload_part("k", &id, 1, data(PART), T0)
        .await
        .expect("p1");
    f.set_complete_error_in_200(Some("InvalidPart"));
    match c
        .complete_multipart_upload("k", &id, &[(1, e1.clone())], T0)
        .await
    {
        Err(S3Error::Service { code, status, .. }) => {
            assert_eq!(code, "InvalidPart");
            assert_eq!(status, 200);
        }
        other => panic!("a 200 carrying <Error> must fail, got {other:?}"),
    }
    // InternalError inside a 200 surfaces as a retryable 5xx.
    f.set_complete_error_in_200(Some("InternalError"));
    match c.complete_multipart_upload("k", &id, &[(1, e1)], T0).await {
        Err(S3Error::Service { status, .. }) => assert_eq!(status, 500),
        other => panic!("expected 5xx-shaped error, got {other:?}"),
    }
    assert!(matches!(
        c.get_object("k", T0).await,
        Err(S3Error::NotFound)
    ));
}

#[tokio::test]
async fn ranged_get_206_416_and_short_tail() {
    let c = client(fake());
    let payload = data(100);
    c.put_object("k", payload.clone(), T0).await.expect("put");
    assert_eq!(
        c.get_object_range("k", 10, 20, T0).await.expect("mid"),
        payload[10..30]
    );
    // Short final range: asks for 50 from offset 80, only 20 exist.
    assert_eq!(
        c.get_object_range("k", 80, 50, T0).await.expect("tail"),
        payload[80..]
    );
    // Exactly the last byte.
    assert_eq!(
        c.get_object_range("k", 99, 1, T0).await.expect("last"),
        payload[99..]
    );
    // Unsatisfiable.
    match c.get_object_range("k", 100, 10, T0).await {
        Err(S3Error::Service { status, code, .. }) => {
            assert_eq!(status, 416);
            assert_eq!(code, "InvalidRange");
        }
        other => panic!("expected 416, got {other:?}"),
    }
    assert!(matches!(
        c.get_object_range("absent", 0, 4, T0).await,
        Err(S3Error::NotFound)
    ));
}

#[tokio::test]
async fn upload_part_to_unknown_upload_is_no_such_upload() {
    let c = client(fake());
    match c.upload_part("k", "nope", 1, data(4), T0).await {
        Err(S3Error::Service { code, status, .. }) => {
            assert_eq!(code, "NoSuchUpload");
            assert_eq!(status, 404);
        }
        other => panic!("expected NoSuchUpload, got {other:?}"),
    }
}
