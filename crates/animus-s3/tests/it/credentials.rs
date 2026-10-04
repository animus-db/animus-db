//! S-08 M1: credential providers, session-token signing end to end against
//! `FakeS3`, refresh/single-flight, ExpiredToken retry, and virtual-hosted
//! addressing. Everything is deterministic: explicit `now`, fake endpoints.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use animus_s3::client::{Addressing, S3Client, S3Config, S3Error, S3Target};
use animus_s3::creds::{
    CachingProvider, ContainerProvider, CredentialProvider, ImdsV2Provider, StaticProvider,
    StsWebIdentityProvider, TokenSource,
};
use animus_s3::fake::{FakeCredentialService, FakeS3};
use animus_s3::sigv4::Credentials;
use async_trait::async_trait;

const T0: u64 = 1_700_000_000_000;
const MIN: u64 = 60_000;
const BUCKET: &str = "test-bucket";

fn target() -> S3Target {
    S3Target {
        endpoint: "https://s3.example.com".to_string(),
        bucket: BUCKET.to_string(),
        region: "us-east-1".to_string(),
    }
}

fn token(t: &str) -> TokenSource {
    let t = t.to_string();
    Arc::new(move || Ok(t.clone()))
}

fn sts(svc: &Arc<FakeCredentialService>) -> StsWebIdentityProvider<Arc<FakeCredentialService>> {
    StsWebIdentityProvider::new(
        svc.clone(),
        "https://sts.us-east-1.amazonaws.com",
        "arn:aws:iam::123456789012:role/animus",
        "animusd",
        token("jwt.header.payload"),
    )
}

#[tokio::test]
async fn static_session_credentials_round_trip_and_wrong_token_is_rejected() {
    let fake = FakeS3::new(BUCKET);
    fake.register_session_credential("ASIA1", "sec1", "tok1", T0 + 60 * MIN);
    let good = Credentials::new("ASIA1", "sec1").with_session_token("tok1", None);
    let client = S3Client::with_provider(fake, target(), Arc::new(StaticProvider::new(good)));
    client
        .put_object("k", b"v".to_vec(), T0)
        .await
        .expect("put");
    assert_eq!(client.get_object("k", T0).await.expect("get"), b"v");

    let fake = FakeS3::new(BUCKET);
    fake.register_session_credential("ASIA1", "sec1", "tok1", T0 + 60 * MIN);
    let bad = Credentials::new("ASIA1", "sec1").with_session_token("other", None);
    let client = S3Client::with_provider(fake, target(), Arc::new(StaticProvider::new(bad)));
    // Wrong token: S3 answers InvalidToken, a refresh of a static provider
    // returns the same thing, so the client surfaces CredentialsExpired.
    let err = client.put_object("k", b"v".to_vec(), T0).await.unwrap_err();
    assert!(matches!(err, S3Error::CredentialsExpired), "{err:?}");
}

#[tokio::test]
async fn sts_provider_is_unsigned_and_parses_credentials() {
    let svc = Arc::new(FakeCredentialService::new());
    svc.set_next_expiry_epoch_ms(T0 + 60 * MIN);
    svc.expect_web_identity_token("jwt.header.payload");
    let p = sts(&svc);
    let c = p.credentials(T0).await.expect("creds");
    assert_eq!(c.access_key_id, "AKID1");
    assert_eq!(c.secret_access_key(), "SECRET1");
    assert_eq!(c.session_token(), Some("TOKEN1"));
    assert_eq!(c.expiry_epoch_ms(), Some(T0 + 60 * MIN));
    let seen = svc.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].method, "POST");
    assert!(!seen[0].has_authorization, "STS call must be unsigned");
    assert!(seen[0].body.contains("Action=AssumeRoleWithWebIdentity"));
    assert!(seen[0].body.contains("Version=2011-06-15"));
    assert!(seen[0].body.contains("RoleSessionName=animusd"));
    assert!(
        seen[0]
            .body
            .contains("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fanimus"),
        "{}",
        seen[0].body
    );
}

#[tokio::test]
async fn sts_error_response_is_surfaced_without_leaking_the_token() {
    let svc = Arc::new(FakeCredentialService::new());
    svc.set_failure(Some((400, "InvalidIdentityToken", "Token is expired")));
    let err = sts(&svc).credentials(T0).await.unwrap_err();
    match &err {
        S3Error::Service {
            code,
            message,
            status,
        } => {
            assert_eq!(code, "InvalidIdentityToken");
            assert_eq!(*status, 400);
            assert!(message.contains("Token is expired"), "{message}");
        }
        other => panic!("expected Service, got {other:?}"),
    }
    assert!(!format!("{err}").contains("jwt.header.payload"));
}

#[tokio::test]
async fn unreadable_web_identity_token_is_a_credentials_error() {
    let svc = Arc::new(FakeCredentialService::new());
    let failing: TokenSource = Arc::new(|| Err("/var/run/token: not found".to_string()));
    let p =
        StsWebIdentityProvider::new(svc.clone(), "https://sts.example.com", "arn", "s", failing);
    let err = p.credentials(T0).await.unwrap_err();
    assert!(matches!(err, S3Error::Credentials(_)), "{err:?}");
    assert!(svc.seen().is_empty());
}

#[tokio::test]
async fn caching_refreshes_five_minutes_before_expiry() {
    let svc = Arc::new(FakeCredentialService::new());
    svc.set_next_expiry_epoch_ms(T0 + 10 * MIN);
    let p = CachingProvider::new(sts(&svc));

    let c1 = p.credentials(T0).await.expect("first");
    assert_eq!(c1.access_key_id, "AKID1");
    // 4 minutes in: still more than 5 minutes from expiry -> cached.
    let c = p.credentials(T0 + 4 * MIN).await.expect("cached");
    assert_eq!(c.access_key_id, "AKID1");
    assert_eq!(svc.issued(), 1);

    // 5 minutes before expiry exactly: refresh.
    svc.set_next_expiry_epoch_ms(T0 + 70 * MIN);
    let c2 = p.credentials(T0 + 5 * MIN).await.expect("refreshed");
    assert_eq!(c2.access_key_id, "AKID2");
    assert_eq!(svc.issued(), 2);
    let c = p.credentials(T0 + 6 * MIN).await.expect("cached again");
    assert_eq!(c.access_key_id, "AKID2");
    assert_eq!(svc.issued(), 2);
}

#[tokio::test]
async fn caching_keeps_serving_unexpired_credentials_when_refresh_fails() {
    let svc = Arc::new(FakeCredentialService::new());
    svc.set_next_expiry_epoch_ms(T0 + 10 * MIN);
    let p = CachingProvider::new(sts(&svc));
    p.credentials(T0).await.expect("first");
    svc.set_failure(Some((503, "ServiceUnavailable", "down")));
    // In the refresh window but not yet expired: old creds still served.
    let c = p.credentials(T0 + 7 * MIN).await.expect("fallback");
    assert_eq!(c.access_key_id, "AKID1");
    // Past expiry: the failure surfaces.
    let err = p.credentials(T0 + 11 * MIN).await.unwrap_err();
    assert!(
        matches!(err, S3Error::Service { status: 503, .. }),
        "{err:?}"
    );
}

/// An inner provider that yields mid-fetch so concurrent callers genuinely
/// interleave on a single-threaded runtime.
struct SlowProvider(AtomicU32);

#[async_trait]
impl CredentialProvider for SlowProvider {
    async fn credentials(&self, now: u64) -> Result<Credentials, S3Error> {
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        Ok(Credentials::new(format!("AK{n}"), "s").with_session_token("t", Some(now + 60 * MIN)))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_callers_trigger_exactly_one_fetch() {
    let p = CachingProvider::new(SlowProvider(AtomicU32::new(0)));
    let (a, b, c, d, e, f) = tokio::join!(
        p.credentials(T0),
        p.credentials(T0),
        p.credentials(T0),
        p.credentials(T0),
        p.credentials(T0),
        p.credentials(T0),
    );
    for r in [a, b, c, d, e, f] {
        assert_eq!(r.expect("creds").access_key_id, "AK1");
    }
    // Concurrent forced refreshes of the same stale credentials: one fetch.
    let stale = p.credentials(T0).await.unwrap();
    let (a, b, c, d) = tokio::join!(
        p.refresh(&stale, T0),
        p.refresh(&stale, T0),
        p.refresh(&stale, T0),
        p.refresh(&stale, T0),
    );
    for r in [a, b, c, d] {
        assert_eq!(r.expect("creds").access_key_id, "AK2");
    }
}

#[tokio::test]
async fn expired_token_forces_one_refresh_and_the_retry_succeeds() {
    let svc = Arc::new(FakeCredentialService::new());
    svc.set_next_expiry_epoch_ms(T0 + 60 * MIN);
    let s3 = Arc::new(FakeS3::new(BUCKET));
    // Server-side the first credential is already dead at the request time.
    s3.register_session_credential("AKID1", "SECRET1", "TOKEN1", T0 + MIN / 2);
    s3.register_session_credential("AKID2", "SECRET2", "TOKEN2", T0 + 120 * MIN);
    let client = S3Client::with_provider(
        s3.clone(),
        target(),
        Arc::new(CachingProvider::new(sts(&svc))),
    );
    let now = T0 + MIN;
    client
        .put_object("a/b", b"hello".to_vec(), now)
        .await
        .expect("put");
    assert_eq!(svc.issued(), 2, "exactly one forced refresh");
    // 1st attempt rejected, 2nd accepted.
    assert_eq!(s3.request_count(), 2);
    // Subsequent calls reuse the refreshed credentials.
    assert_eq!(client.get_object("a/b", now).await.expect("get"), b"hello");
    assert_eq!(svc.issued(), 2);
}

#[tokio::test]
async fn still_expired_after_refresh_surfaces_credentials_expired() {
    let svc = Arc::new(FakeCredentialService::new());
    svc.set_next_expiry_epoch_ms(T0 + 60 * MIN);
    let s3 = Arc::new(FakeS3::new(BUCKET));
    s3.register_session_credential("AKID1", "SECRET1", "TOKEN1", T0);
    s3.register_session_credential("AKID2", "SECRET2", "TOKEN2", T0);
    let client = S3Client::with_provider(
        s3.clone(),
        target(),
        Arc::new(CachingProvider::new(sts(&svc))),
    );
    let err = client
        .put_object("k", b"v".to_vec(), T0 + MIN)
        .await
        .unwrap_err();
    assert!(matches!(err, S3Error::CredentialsExpired), "{err:?}");
    assert_eq!(svc.issued(), 2);
    assert_eq!(s3.request_count(), 2, "retried exactly once");
}

#[tokio::test]
async fn provider_failure_on_refresh_is_surfaced() {
    let svc = Arc::new(FakeCredentialService::new());
    svc.set_next_expiry_epoch_ms(T0 + 60 * MIN);
    let s3 = Arc::new(FakeS3::new(BUCKET));
    s3.register_session_credential("AKID1", "SECRET1", "TOKEN1", T0);
    let client = S3Client::with_provider(
        s3.clone(),
        target(),
        Arc::new(CachingProvider::new(sts(&svc))),
    );
    // Prime the cache, then make STS fail: the refresh error surfaces.
    client
        .put_object("warm", b"w".to_vec(), T0 - MIN)
        .await
        .expect("warm");
    svc.set_failure(Some((400, "InvalidIdentityToken", "revoked")));
    let err = client
        .put_object("k", b"v".to_vec(), T0 + MIN)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, S3Error::Service { code, .. } if code == "InvalidIdentityToken"),
        "{err:?}"
    );
}

#[tokio::test]
async fn container_provider_sends_auth_token_and_parses_json() {
    let svc = Arc::new(FakeCredentialService::new());
    svc.set_next_expiry_epoch_ms(T0 + 30 * MIN);
    svc.expect_container_auth("pod-identity-token");
    let p = ContainerProvider::new(
        svc.clone(),
        "http://169.254.170.23/v1/credentials",
        Some(token("pod-identity-token")),
    );
    let c = p.credentials(T0).await.expect("creds");
    assert_eq!(c.session_token(), Some("TOKEN1"));
    assert_eq!(c.expiry_epoch_ms(), Some(T0 + 30 * MIN));
    assert_eq!(
        svc.seen()[0].authorization.as_deref(),
        Some("pod-identity-token")
    );

    // Wrong/absent auth token: 403 surfaced.
    let p = ContainerProvider::new(svc.clone(), "http://169.254.170.23/v1/credentials", None);
    let err = p.credentials(T0).await.unwrap_err();
    assert!(
        matches!(err, S3Error::Service { status: 403, .. }),
        "{err:?}"
    );
}

#[tokio::test]
async fn imds_v2_provider_does_token_role_credentials_handshake() {
    let svc = Arc::new(FakeCredentialService::new());
    svc.set_next_expiry_epoch_ms(T0 + 30 * MIN);
    let p = ImdsV2Provider::new(
        svc.clone(),
        ImdsV2Provider::<Arc<FakeCredentialService>>::DEFAULT_ENDPOINT,
    );
    let c = p.credentials(T0).await.expect("creds");
    assert_eq!(c.access_key_id, "AKID1");
    let seen = svc.seen();
    let methods: Vec<_> = seen
        .iter()
        .map(|r| (r.method.as_str(), r.uri.as_str()))
        .collect();
    assert_eq!(
        methods,
        vec![
            ("PUT", "/latest/api/token"),
            ("GET", "/latest/meta-data/iam/security-credentials/"),
            (
                "GET",
                "/latest/meta-data/iam/security-credentials/test-role"
            ),
        ]
    );
}

// --- virtual-hosted addressing -------------------------------------------

#[tokio::test]
async fn virtual_hosted_round_trip_against_fake_s3() {
    let fake = FakeS3::new(BUCKET)
        .with_credential("AKID", "secret")
        .with_page_size(2);
    let client = S3Client::new(
        fake,
        S3Config {
            endpoint: "https://s3.example.com".to_string(),
            bucket: BUCKET.to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKID", "secret"),
        },
    )
    .with_addressing(Addressing::VirtualHosted)
    .expect("valid");
    for k in ["p/a b", "p/b", "p/c", "q"] {
        client
            .put_object(k, k.as_bytes().to_vec(), T0)
            .await
            .expect("put");
    }
    assert_eq!(client.get_object("p/a b", T0).await.expect("get"), b"p/a b");
    assert_eq!(client.head_object("q", T0).await.expect("head").size, 1);
    let mut keys = Vec::new();
    let mut tok: Option<String> = None;
    loop {
        let page = client
            .list_objects_v2("p/", tok.as_deref(), T0)
            .await
            .expect("list");
        keys.extend(page.objects.into_iter().map(|o| o.key));
        match page.next_continuation_token {
            Some(t) => tok = Some(t),
            None => break,
        }
    }
    assert_eq!(keys, vec!["p/a b", "p/b", "p/c"]);
    client.delete_object("q", T0).await.expect("delete");
    assert!(matches!(
        client.get_object("q", T0).await.unwrap_err(),
        S3Error::NotFound
    ));
}

#[test]
fn virtual_hosted_rejects_ip_endpoints_and_non_dns_buckets() {
    let mk = |endpoint: &str, bucket: &str| {
        S3Client::new(
            FakeS3::new(bucket),
            S3Config {
                endpoint: endpoint.to_string(),
                bucket: bucket.to_string(),
                region: "us-east-1".to_string(),
                credentials: Credentials::new("a", "b"),
            },
        )
        .with_addressing(Addressing::VirtualHosted)
        .map(|_| ())
    };
    assert!(mk("https://s3.example.com", "good-bucket-1").is_ok());
    assert!(mk("http://localhost:9000", "good-bucket").is_ok());
    for ep in [
        "http://127.0.0.1:9000",
        "https://10.0.0.5",
        "http://[::1]:9000",
    ] {
        let e = mk(ep, "good-bucket").unwrap_err();
        assert!(
            matches!(&e, S3Error::InvalidConfig(m) if m.contains("IP")),
            "{ep}: {e:?}"
        );
    }
    for b in ["Upper", "ab", "has.dot", "-lead", "trail-", "under_score"] {
        let e = mk("https://s3.example.com", b).unwrap_err();
        assert!(
            matches!(&e, S3Error::InvalidConfig(m) if m.contains("DNS-compatible")),
            "{b}: {e:?}"
        );
    }
}

#[tokio::test]
async fn path_style_remains_the_default() {
    let fake = FakeS3::new(BUCKET).with_credential("AKID", "secret");
    let client = S3Client::new(
        fake,
        S3Config {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: BUCKET.to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKID", "secret"),
        },
    );
    client
        .put_object("k", b"v".to_vec(), T0)
        .await
        .expect("put");
}
