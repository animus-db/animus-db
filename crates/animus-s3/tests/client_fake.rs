//! `client::S3Client` round trips over `fake::FakeS3` — the client's own
//! HTTP-shape logic and the signer, exercised end to end with no sockets.

use animus_s3::client::{S3Client, S3Config, S3Error};
use animus_s3::fake::FakeS3;
use animus_s3::sigv4::Credentials;

const NOW_MS: u64 = 1_757_073_600_000; // 2025-09-05T12:00:00Z, arbitrary.

fn client(fake: FakeS3) -> S3Client<FakeS3> {
    S3Client::new(
        fake,
        S3Config {
            endpoint: "https://127.0.0.1:9000".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKID", "secret"),
        },
    )
}

fn fake_bucket() -> FakeS3 {
    FakeS3::new("test-bucket").with_credential("AKID", "secret")
}

#[tokio::test]
async fn put_get_head_delete_round_trip() {
    let c = client(fake_bucket());

    c.put_object("dir/object.bin", b"hello world".to_vec(), NOW_MS)
        .await
        .expect("put");

    let got = c.get_object("dir/object.bin", NOW_MS).await.expect("get");
    assert_eq!(got, b"hello world");

    let meta = c.head_object("dir/object.bin", NOW_MS).await.expect("head");
    assert_eq!(meta.size, 11);

    c.delete_object("dir/object.bin", NOW_MS)
        .await
        .expect("delete");

    match c.get_object("dir/object.bin", NOW_MS).await {
        Err(S3Error::NotFound) => {}
        other => panic!("expected NotFound after delete, got {other:?}"),
    }
}

#[tokio::test]
async fn delete_of_an_absent_key_is_not_an_error() {
    let c = client(fake_bucket());
    c.delete_object("never-existed", NOW_MS)
        .await
        .expect("delete of an absent key is idempotent, not an error");
}

#[tokio::test]
async fn get_of_a_missing_key_is_not_found() {
    let c = client(fake_bucket());
    match c.get_object("missing", NOW_MS).await {
        Err(S3Error::NotFound) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn head_of_a_missing_key_is_not_found_with_no_body() {
    let c = client(fake_bucket());
    match c.head_object("missing", NOW_MS).await {
        Err(S3Error::NotFound) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn a_key_containing_special_characters_round_trips() {
    // Regression: an earlier draft double-percent-encoded a key at the
    // client's own call site (see `client.rs`'s module doc) — this exact
    // case (a space and a `+`) would have silently broken.
    let c = client(fake_bucket());
    let key = "some dir/a+b (c).txt";
    c.put_object(key, b"payload".to_vec(), NOW_MS)
        .await
        .expect("put");
    let got = c.get_object(key, NOW_MS).await.expect("get");
    assert_eq!(got, b"payload");
}

#[tokio::test]
async fn wrong_secret_is_rejected() {
    let fake = fake_bucket();
    let c = S3Client::new(
        fake,
        S3Config {
            endpoint: "https://127.0.0.1:9000".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKID", "wrong-secret"),
        },
    );
    match c.put_object("k", b"v".to_vec(), NOW_MS).await {
        Err(S3Error::AccessDenied) => {}
        other => panic!("expected AccessDenied, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_access_key_is_rejected() {
    let fake = fake_bucket();
    let c = S3Client::new(
        fake,
        S3Config {
            endpoint: "https://127.0.0.1:9000".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("SOMEONE-ELSE", "secret"),
        },
    );
    match c.get_object("k", NOW_MS).await {
        Err(S3Error::Service { status, .. }) => assert_eq!(status, 403),
        other => panic!("expected a 403 Service error, got {other:?}"),
    }
}

#[tokio::test]
async fn list_objects_v2_paginates_across_more_than_one_page() {
    let fake = fake_bucket().with_page_size(2);
    let c = client(fake);

    for i in 0..5u32 {
        c.put_object(&format!("prefix/item-{i:02}"), vec![i as u8], NOW_MS)
            .await
            .expect("put");
    }

    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = c
            .list_objects_v2("prefix/", token.as_deref(), NOW_MS)
            .await
            .expect("list");
        seen.extend(page.objects.into_iter().map(|o| o.key));
        match page.next_continuation_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }

    assert_eq!(
        seen,
        vec![
            "prefix/item-00",
            "prefix/item-01",
            "prefix/item-02",
            "prefix/item-03",
            "prefix/item-04",
        ]
    );
}

#[tokio::test]
async fn list_objects_v2_only_matches_the_given_prefix() {
    let c = client(fake_bucket());
    c.put_object("a/1", b"x".to_vec(), NOW_MS).await.unwrap();
    c.put_object("b/1", b"x".to_vec(), NOW_MS).await.unwrap();

    let page = c.list_objects_v2("a/", None, NOW_MS).await.expect("list");
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].key, "a/1");
    assert_eq!(page.next_continuation_token, None);
}
