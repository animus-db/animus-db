//! [`S3SegmentStore`]: an S3-backed [`SegmentStore`](crate::SegmentStore) —
//! S-04 PR 2 (`docs/roadmap.md` §2; design amendment:
//! `docs/adr/0059-backup-restore.md`'s 2026-09-06 "S-04: S3 `SegmentStore`
//! backend" section). Wraps `animus_s3::client::S3Client<T>`, generic over
//! `T: animus_s3::client::Transport`, so this store is testable against
//! `animus_s3::fake::FakeS3` (no sockets, see this module's own tests) and
//! driven for real by `animus_s3::prod::HyperRustlsTransport`
//! (`animusd` constructs that concrete instantiation, not this crate).
//!
//! # Object layout
//!
//! One S3 bucket (`animus_s3::client::S3Config::bucket`), optionally scoped
//! by a key prefix: a store `id` (this trait's own opaque namespace
//! convention — `{table}/{label}/{tablet}/{epoch}/{attempt-suffix}` for a
//! stream segment, `backup/{backup_id}/...` for a backup object) becomes the
//! S3 object key `[prefix/]{id}` **verbatim** — a 1:1 mapping, no escaping.
//! This is safe because every character an `id` can legally contain (see
//! `animus_cp_data::segment`'s and `animus_cp_data::backup`'s own id-minting
//! code: table/index names, tablet ids, hex-formatted epochs/attempt
//! suffixes, and the ASCII `/`/`.`/`-`/`:` separators those use) is already a
//! literal-safe S3 object key character — S3 keys allow any UTF-8 byte
//! sequence, and `animus_s3::client::S3Client` percent-encodes the key
//! exactly once, at the wire/signing boundary (see that crate's own "Encode
//! exactly once" doc), so this layer never needs to escape anything itself.
//!
//! # Consistency
//!
//! Real S3 (and every credible S3-compatible target — MinIO, localstack) has
//! been strongly (read-after-write) consistent for both new-object and
//! overwrite `PUT`s since 2020, which is what this store's own
//! [`SegmentStore`] contract (write-once, read-after-put) assumes — see the
//! ADR amendment's "Consistency assumptions" section for the full argument.
//!
//! # Retry
//!
//! A small, bounded retry ([`MAX_RETRY_ATTEMPTS`]) on a transport failure or
//! a `5xx` service error — never on a `4xx` (a client-side mistake retrying
//! won't fix, e.g. `AccessDenied`) or [`animus_s3::client::S3Error::NotFound`]
//! (a defined outcome, not a failure). This store is **not** `Env`-generic
//! (it mirrors [`crate::FsSegmentStore`]'s own shape: a concrete,
//! `prod`-feature-gated type with no `E: Env` parameter), so the backoff
//! sleep is a plain `tokio::time::sleep` rather than `env.sleep()` — see the
//! module-level `#[allow(...)]` below.
//!
//! # Credentials
//!
//! Never read, generated, or logged by this module — `animus_s3::client::
//! S3Config::credentials` is handed in by the caller (`animusd`, resolved
//! from a config file or environment variables) already-built; this store
//! never touches the filesystem or environment for them.

#![allow(
    clippy::disallowed_methods,
    reason = "this module is S-04 PR 2's one real-time site: SystemTime::now() \
              supplies the SigV4 request timestamp every S3 call needs \
              (animus_s3::client::S3Client's own `now_epoch_ms` parameter), \
              and tokio::time::sleep paces this store's own small bounded \
              retry — the identical real-I/O-boundary justification \
              animus-env's own prod.rs module carries for the same calls \
              (ADR 0061 rung B5). This store deliberately is not `Env`- \
              generic (mirrors FsSegmentStore's own shape), so there is no \
              env.now()/env.sleep() to route through instead."
)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use animus_s3::client::{ListObjectsPage, S3Client, S3Config, S3Error, Transport};

/// Bounded retry budget for a transport error or `5xx` — small and fixed,
/// not configurable: this is a store-level reliability seatbelt, not a
/// substitute for a caller's own retry policy on genuine unavailability.
const MAX_RETRY_ATTEMPTS: u32 = 3;

/// Linear backoff step between retry attempts (`attempt * RETRY_BASE_DELAY`).
const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

/// Safety cap on `list`'s own pagination loop — real S3 usage never needs
/// more than a handful of 1000-key pages for one prefix; this exists so a
/// misbehaving/malicious endpoint returning an unbounded `IsTruncated: true`
/// chain can't wedge a sweep forever.
const LIST_PAGE_CAP: usize = 10_000;

/// An S3-backed [`SegmentStore`](crate::SegmentStore) — see the module doc.
pub struct S3SegmentStore<T: Transport> {
    client: Arc<S3Client<T>>,
    /// Key prefix every id is joined under (`{prefix}/{id}`), or `None` for
    /// no prefix — mirrors `s3://bucket[/prefix]`'s own optional-prefix
    /// shape. Never empty (`new` normalizes `Some("")` to `None`).
    prefix: Option<String>,
    /// Supplies `now_epoch_ms` for every S3 call. Defaults to the real wall
    /// clock (`new`); overridable via [`Self::with_clock`] for a test that
    /// wants a deterministic timestamp (this store's own contract test
    /// doesn't need one — `animus_s3::fake::FakeS3` doesn't check clock
    /// skew — but the hook costs nothing and keeps a future caller that does
    /// care from needing a second constructor).
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
}

// Manual `Clone`, not `#[derive(Clone)]`: every field is already cheap to
// clone (an `Arc` or an `Option<String>`) regardless of whether `T` itself
// is `Clone` — `#[derive(Clone)]` on a generic struct adds a `T: Clone`
// bound unconditionally, which would wrongly require `HyperRustlsTransport`/
// `FakeS3` to implement `Clone` just to clone this handle.
impl<T: Transport> Clone for S3SegmentStore<T> {
    fn clone(&self) -> Self {
        S3SegmentStore {
            client: self.client.clone(),
            prefix: self.prefix.clone(),
            clock: self.clock.clone(),
        }
    }
}

fn real_now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Whether a failed S3 call is worth retrying: a transport failure (the
/// request never got a well-formed response at all) or a `5xx` service
/// error — never [`S3Error::NotFound`]/[`S3Error::AccessDenied`], and never a
/// non-5xx [`S3Error::Service`] (a client-side mistake, e.g. a malformed
/// request — retrying changes nothing).
fn is_retryable(err: &S3Error) -> bool {
    match err {
        S3Error::Transport(_) => true,
        S3Error::Service { status, .. } => *status >= 500,
        S3Error::NotFound | S3Error::AccessDenied => false,
    }
}

/// Map an [`S3Error`] onto the [`std::io::Error`] kind
/// [`SegmentStore`](crate::SegmentStore)'s trait signature needs — callers
/// that care about "was this a 404" match on [`std::io::ErrorKind::NotFound`]
/// (though every trait method already turns a `NotFound` into a defined
/// `None`/no-op before this is ever reached; this exists for the residual
/// `Service`/`Transport` cases that must still surface as an `Err`).
fn map_io_error(err: S3Error) -> std::io::Error {
    match err {
        S3Error::NotFound => std::io::Error::new(std::io::ErrorKind::NotFound, err.to_string()),
        S3Error::AccessDenied => {
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, err.to_string())
        }
        other => std::io::Error::other(other.to_string()),
    }
}

/// [`SegmentStore::put`](crate::SegmentStore::put)'s write-once violation —
/// mirrors [`crate::FsSegmentStore`]'s identical error, see that store's own
/// `write_once_violation` for the full rationale.
fn write_once_violation(id: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!(
            "S3 segment store write-once violation: {id:?} already holds different content \
             (every attempt must write its own unique id — see \
             animus_cp_data::segment::segment_object_id)"
        ),
    )
}

impl<T: Transport> S3SegmentStore<T> {
    /// Build a store over `transport`/`config`, joining every id under
    /// `prefix` (if given — an empty string is treated as "no prefix").
    #[must_use]
    pub fn new(transport: T, config: S3Config, prefix: Option<String>) -> Self {
        S3SegmentStore {
            client: Arc::new(S3Client::new(transport, config)),
            prefix: prefix.filter(|p| !p.is_empty()),
            clock: Arc::new(real_now_epoch_ms),
        }
    }

    /// Override the clock this store reads `now_epoch_ms` from — a test-only
    /// hook (production always takes [`Self::new`]'s real-wall-clock
    /// default); `#[doc(hidden)]` since it is not part of this store's real
    /// API surface.
    #[must_use]
    #[doc(hidden)]
    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    /// `id` -> S3 object key: `{prefix}/{id}` (or bare `id` with no
    /// configured prefix) — see the module doc's "Object layout" section.
    fn object_key(&self, id: &str) -> String {
        match &self.prefix {
            Some(p) => format!("{p}/{id}"),
            None => id.to_string(),
        }
    }

    /// The inverse of [`Self::object_key`] — recovers the caller-facing id
    /// from a key `list_objects_v2` returned (which already carries this
    /// store's own prefix, since every `list` query is itself prefixed via
    /// [`Self::object_key`]).
    fn strip_prefix(&self, key: &str) -> String {
        match &self.prefix {
            Some(p) => key
                .strip_prefix(&format!("{p}/"))
                .unwrap_or(key)
                .to_string(),
            None => key.to_string(),
        }
    }

    async fn retry_get(&self, key: &str) -> Result<Vec<u8>, S3Error> {
        let mut attempt = 0u32;
        loop {
            let now = self.now_ms();
            match self.client.get_object(key, now).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) if attempt < MAX_RETRY_ATTEMPTS && is_retryable(&e) => {
                    attempt += 1;
                    tokio::time::sleep(RETRY_BASE_DELAY * attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn retry_put(&self, key: &str, bytes: &[u8]) -> Result<(), S3Error> {
        let mut attempt = 0u32;
        loop {
            let now = self.now_ms();
            match self.client.put_object(key, bytes.to_vec(), now).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < MAX_RETRY_ATTEMPTS && is_retryable(&e) => {
                    attempt += 1;
                    tokio::time::sleep(RETRY_BASE_DELAY * attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn retry_delete(&self, key: &str) -> Result<(), S3Error> {
        let mut attempt = 0u32;
        loop {
            let now = self.now_ms();
            match self.client.delete_object(key, now).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < MAX_RETRY_ATTEMPTS && is_retryable(&e) => {
                    attempt += 1;
                    tokio::time::sleep(RETRY_BASE_DELAY * attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn retry_list_page(
        &self,
        prefix: &str,
        continuation: Option<&str>,
    ) -> Result<ListObjectsPage, S3Error> {
        let mut attempt = 0u32;
        loop {
            let now = self.now_ms();
            match self.client.list_objects_v2(prefix, continuation, now).await {
                Ok(page) => return Ok(page),
                Err(e) if attempt < MAX_RETRY_ATTEMPTS && is_retryable(&e) => {
                    attempt += 1;
                    tokio::time::sleep(RETRY_BASE_DELAY * attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[async_trait::async_trait]
impl<T: Transport> crate::SegmentStore for S3SegmentStore<T> {
    async fn put(&self, id: &str, bytes: &[u8]) -> std::io::Result<()> {
        if id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "segment id must be non-empty",
            ));
        }
        let key = self.object_key(id);
        // Write-once (SegmentStore::put's own contract, mirroring
        // FsSegmentStore::put exactly): fetch whatever is already there
        // first. An identical-content re-put is a safe no-op that skips the
        // network PUT entirely; a differing-content re-put is a hard error.
        // Real S3 has no built-in "PUT only if absent," so this GET-then-PUT
        // is how that contract is enforced at this layer — every real
        // caller writes each attempt at its own unique id (see the module
        // doc), so this should never actually observe a differing-content
        // collision in practice.
        match self.retry_get(&key).await {
            Ok(existing) if existing == bytes => return Ok(()),
            Ok(_) => return Err(write_once_violation(id)),
            Err(S3Error::NotFound) => {}
            Err(e) => return Err(map_io_error(e)),
        }
        self.retry_put(&key, bytes).await.map_err(map_io_error)
    }

    async fn get(&self, id: &str) -> std::io::Result<Option<Vec<u8>>> {
        let key = self.object_key(id);
        match self.retry_get(&key).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(S3Error::NotFound) => Ok(None),
            Err(e) => Err(map_io_error(e)),
        }
    }

    async fn delete(&self, id: &str) -> std::io::Result<()> {
        let key = self.object_key(id);
        // `S3Client::delete_object` already treats a 404 as `Ok` (real S3's
        // own idempotent-delete behavior), so no extra handling is needed
        // here for "deleting an absent id."
        self.retry_delete(&key).await.map_err(map_io_error)
    }

    async fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        let full_prefix = self.object_key(prefix);
        let mut out = Vec::new();
        let mut continuation: Option<String> = None;
        for _ in 0..LIST_PAGE_CAP {
            let page = self
                .retry_list_page(&full_prefix, continuation.as_deref())
                .await
                .map_err(map_io_error)?;
            out.extend(page.objects.into_iter().map(|o| self.strip_prefix(&o.key)));
            match page.next_continuation_token {
                Some(token) => continuation = Some(token),
                None => break,
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use animus_s3::client::S3Config;
    use animus_s3::fake::FakeS3;
    use animus_s3::sigv4::Credentials;

    use super::S3SegmentStore;

    fn test_store(prefix: Option<&str>) -> S3SegmentStore<FakeS3> {
        let fake = FakeS3::new("test-bucket").with_credential("AKIDTEST", "secret");
        let config = S3Config {
            endpoint: "http://fake.example:9000".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKIDTEST", "secret"),
        };
        S3SegmentStore::new(fake, config, prefix.map(str::to_string))
    }

    /// The load-bearing test: the shared `SegmentStore` contract holds
    /// against a real signature-verifying fake transport, with no network
    /// at all — see `crate::test_support::assert_segment_store_contract`
    /// for exactly what this pins (put/get round trip, write-once semantics,
    /// delete idempotence, resurrection after delete, prefix-filtered
    /// `list`).
    #[tokio::test]
    async fn contract_holds_against_the_fake_transport() {
        let store = test_store(None);
        crate::test_support::assert_segment_store_contract(&store).await;
    }

    /// The identical contract holds again with a configured bucket-level
    /// prefix — proving `object_key`/`strip_prefix` compose correctly with
    /// the id-level `"contract-test/"` scoping the contract test itself
    /// uses (a prefix-of-a-prefix), not just the no-prefix case above.
    #[tokio::test]
    async fn contract_holds_with_a_configured_prefix() {
        let store = test_store(Some("animus/segments"));
        crate::test_support::assert_segment_store_contract(&store).await;
    }

    /// A minimal, deterministic-enough `Rng` for a test that just needs
    /// distinct per-object salts — not a `SimEnv`-driven corpus, so real
    /// seed-reproducibility doesn't matter here; a plain counter-seeded
    /// splitmix64 avoids pulling in `OsRng` (a `disallowed_types` hit) for
    /// what is purely local test scaffolding.
    struct CounterRng(std::sync::atomic::AtomicU64);

    impl CounterRng {
        fn new() -> Self {
            CounterRng(std::sync::atomic::AtomicU64::new(0x9E37_79B9))
        }
    }

    impl crate::Rng for CounterRng {
        fn next_u64(&self) -> u64 {
            let x = self
                .0
                .fetch_add(0x9E37_79B9_7F4A_7C15, std::sync::atomic::Ordering::Relaxed);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn fill_bytes(&self, dst: &mut [u8]) {
            let mut i = 0;
            while i < dst.len() {
                let v = self.next_u64().to_le_bytes();
                let n = (dst.len() - i).min(8);
                dst[i..i + n].copy_from_slice(&v[..n]);
                i += n;
            }
        }
    }

    /// `EncryptedSegmentStore<S3SegmentStore<FakeS3>, CounterRng>` satisfies
    /// the identical shared contract (ADR 0069, S-03 PR 2) — the S3 sibling
    /// of `prod.rs`'s own `encrypted_fs_segment_store_satisfies_the_contract`.
    #[tokio::test]
    async fn contract_holds_through_the_encrypted_wrapper() {
        let raw = test_store(None);
        let key = crate::EncryptionKey::from_bytes([0x24; 32]);
        let store = crate::EncryptedSegmentStore::open(raw, CounterRng::new(), key)
            .await
            .expect("open a fresh store with a key");
        crate::test_support::assert_segment_store_contract(&store).await;
    }

    /// The wrong key against an already-encrypted S3 store is refused at
    /// open time, and the raw bytes actually sitting in the (fake) bucket
    /// are not the plaintext value.
    #[tokio::test]
    async fn wrong_key_is_refused_and_ciphertext_never_hits_the_bucket() {
        use crate::SegmentStore as _;

        let raw = test_store(None);
        let raw_for_asserts = raw.clone();
        let key_a = crate::EncryptionKey::from_bytes([0x11; 32]);
        let key_b = crate::EncryptionKey::from_bytes([0x22; 32]);

        let store = crate::EncryptedSegmentStore::open(raw, CounterRng::new(), key_a)
            .await
            .expect("open with key A");
        store
            .put("t/label/1/0", b"a secret value")
            .await
            .expect("put");
        let raw_bytes = raw_for_asserts
            .get("t/label/1/0")
            .await
            .expect("get raw")
            .expect("present");
        assert!(
            !raw_bytes
                .windows(b"a secret value".len())
                .any(|w| w == b"a secret value"),
            "the plaintext value must never appear verbatim in the stored object"
        );

        let err = crate::EncryptedSegmentStore::open(raw_for_asserts, CounterRng::new(), key_b)
            .await
            .map(|_| ())
            .expect_err("the wrong key must be refused");
        assert!(err.to_string().contains("does not match the key"));
    }

    /// `list` must paginate across more than one page, exactly like
    /// `animus-s3`'s own `client_fake.rs` proves for the raw client —
    /// proving this store's own `list` loop actually follows
    /// `next_continuation_token` rather than silently truncating at the
    /// first page.
    #[tokio::test]
    async fn list_paginates_across_more_than_one_page() {
        let fake = FakeS3::new("test-bucket")
            .with_credential("AKIDTEST", "secret")
            .with_page_size(2);
        let config = S3Config {
            endpoint: "http://fake.example:9000".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKIDTEST", "secret"),
        };
        let store = S3SegmentStore::new(fake, config, None);
        use crate::SegmentStore as _;
        for i in 0..5 {
            store
                .put(&format!("page-test/{i}"), format!("v{i}").as_bytes())
                .await
                .expect("put");
        }
        let mut listed = store.list("page-test/").await.expect("list");
        listed.sort();
        assert_eq!(
            listed,
            vec![
                "page-test/0".to_string(),
                "page-test/1".to_string(),
                "page-test/2".to_string(),
                "page-test/3".to_string(),
                "page-test/4".to_string(),
            ]
        );
    }
}
