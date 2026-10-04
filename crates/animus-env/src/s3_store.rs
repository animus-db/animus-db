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
//! # Retry (S-08 M3)
//!
//! One retry helper ([`S3SegmentStore::retry_op`]) wraps every S3 request.
//! The store is generic over the env's time and randomness
//! (`E: Clock + Rng`): the SigV4 timestamp and credential-provider `now`
//! come from `env.wall_now()`, the backoff sleeps from `env.sleep()`, and the
//! jitter from `env.gen_below()` — so under `SimEnv` the whole retry
//! schedule is a pure function of the seed (`animus-test`'s
//! `s3_fault_corpus`). Policy ([`RetryPolicy`], default 5 retries): delay
//! before retry `n` (0-based) is drawn uniformly from
//! `[0, min(cap, base * 2^n)]` ("full jitter", default base 100 ms, cap
//! 5 s). Retried: a transport failure (incl. timeouts), `5xx`, `429`,
//! `RequestTimeout` (`408`) and `SlowDown`. Never retried: any other `4xx`
//! (a client-side mistake retrying won't fix, e.g. `AccessDenied`) and
//! [`animus_s3::client::S3Error::NotFound`] (a defined outcome).
//!
//! Retrying a `PUT` after a lost ack is safe: the key is write-once and the
//! bytes identical, so a replay is an idempotent overwrite.
//!
//! # Multipart (S-08 M2)
//!
//! A `put` larger than [`MultipartConfig::threshold`] is sent as a multipart
//! upload (`CreateMultipartUpload`, one `UploadPart` per
//! [`MultipartConfig::part_size`] slice, `CompleteMultipartUpload`); every
//! request goes through the same bounded retry helper as a plain `PUT`. If
//! any part (or the complete) finally fails, the upload is aborted (best
//! effort) and the error returned. Multipart is **transport only**: the
//! object id, key and bytes are identical to a single `PUT`, so no durable
//! format is involved (ADR 0073). A `Complete` whose ack was lost (retry sees
//! `NoSuchUpload`) is resolved by checking the object holds exactly our bytes.
//! A `Create` whose ack was lost orphans an upload id nobody knows (only the
//! lifecycle rule below reaps it). An abort that itself fails (or a process
//! killed mid-upload) leaves an incomplete upload that bills for storage —
//! operators should add a bucket lifecycle rule
//! `AbortIncompleteMultipartUpload` (e.g. after 1 day).
//!
//! The write-once check no longer downloads the whole existing object: it
//! `HEAD`s it (size mismatch is a violation) and compares in
//! `part_size`-bounded ranged `GET`s.
//!
//! # Credentials
//!
//! Never read, generated, or logged by this module — `animus_s3::client::
//! S3Config::credentials` is handed in by the caller (`animusd`, resolved
//! from a config file or environment variables) already-built; this store
//! never touches the filesystem or environment for them.

use std::sync::Arc;
use std::time::Duration;

use animus_s3::client::{S3Client, S3Config, S3Error, Transport};

use crate::{Clock, Rng};

/// Retry/backoff policy for every S3 request (S-08 M3). Set with
/// [`S3SegmentStore::with_retry_policy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Retries after the first attempt (so up to `max_retries + 1` requests).
    pub max_retries: u32,
    /// Backoff ceiling before the first retry; doubles per retry.
    pub base_delay: Duration,
    /// Upper bound on the backoff ceiling.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_retries: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    /// The full-jitter ceiling for 0-based retry `attempt`:
    /// `min(max_delay, base_delay * 2^attempt)`. The actual sleep is drawn
    /// uniformly from `[0, ceiling]`.
    #[must_use]
    pub fn backoff_ceiling(&self, attempt: u32) -> Duration {
        let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
        self.base_delay
            .checked_mul(factor)
            .map_or(self.max_delay, |d| d.min(self.max_delay))
    }
}

/// S3's hard minimum for every non-last part of a multipart upload.
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// S3's hard maximum number of parts in one multipart upload.
pub const MAX_PARTS: u64 = 10_000;

/// When and how [`S3SegmentStore`] splits a `put` into a multipart upload
/// (S-08 M2). Set with [`S3SegmentStore::with_multipart`]; the default
/// (64 MiB threshold, 16 MiB parts) is what production uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartConfig {
    /// Objects strictly larger than this many bytes are uploaded multipart.
    pub threshold: u64,
    /// Size of each part (the last may be smaller). Grown automatically so
    /// an object never needs more than [`MAX_PARTS`] parts.
    pub part_size: u64,
}

impl Default for MultipartConfig {
    fn default() -> Self {
        MultipartConfig {
            threshold: 64 * 1024 * 1024,
            part_size: 16 * 1024 * 1024,
        }
    }
}

impl MultipartConfig {
    /// A validated config: `part_size` must be at least S3's 5 MiB minimum.
    ///
    /// # Errors
    /// A human-readable reason.
    pub fn new(threshold: u64, part_size: u64) -> Result<Self, String> {
        if part_size < MIN_PART_SIZE {
            return Err(format!(
                "multipart part size {part_size} is below S3's {MIN_PART_SIZE}-byte minimum"
            ));
        }
        Ok(MultipartConfig {
            threshold,
            part_size,
        })
    }

    /// Skips the 5 MiB minimum — for tests against a `FakeS3` whose own
    /// minimum was lowered. Never use against a real endpoint.
    #[must_use]
    #[doc(hidden)]
    pub fn new_unchecked(threshold: u64, part_size: u64) -> Self {
        MultipartConfig {
            threshold,
            part_size: part_size.max(1),
        }
    }

    /// The slice size to use for an object of `len` bytes: `part_size`,
    /// grown so `len` fits in [`MAX_PARTS`] parts.
    fn effective_part_size(&self, len: u64) -> u64 {
        self.part_size.max(len.div_ceil(MAX_PARTS)).max(1)
    }
}

/// Safety cap on `list`'s own pagination loop — real S3 usage never needs
/// more than a handful of 1000-key pages for one prefix; this exists so a
/// misbehaving/malicious endpoint returning an unbounded `IsTruncated: true`
/// chain can't wedge a sweep forever.
const LIST_PAGE_CAP: usize = 10_000;

/// An S3-backed [`SegmentStore`](crate::SegmentStore) — see the module doc.
pub struct S3SegmentStore<T: Transport, E: Clock + Rng> {
    client: Arc<S3Client<T>>,
    /// Key prefix every id is joined under (`{prefix}/{id}`), or `None` for
    /// no prefix — mirrors `s3://bucket[/prefix]`'s own optional-prefix
    /// shape. Never empty (`new` normalizes `Some("")` to `None`).
    prefix: Option<String>,
    /// Supplies `now_epoch_ms` (`wall_now`), backoff sleeps and jitter.
    env: E,
    multipart: MultipartConfig,
    retry: RetryPolicy,
}

// Manual `Clone`, not `#[derive(Clone)]`: every field is cheap to clone
// regardless of whether `T` itself is `Clone` — `#[derive(Clone)]` on a
// generic struct adds a `T: Clone` bound unconditionally, which would
// wrongly require `HyperRustlsTransport`/`FakeS3` to implement `Clone` just
// to clone this handle.
impl<T: Transport, E: Clock + Rng + Clone> Clone for S3SegmentStore<T, E> {
    fn clone(&self) -> Self {
        S3SegmentStore {
            client: self.client.clone(),
            prefix: self.prefix.clone(),
            env: self.env.clone(),
            multipart: self.multipart,
            retry: self.retry,
        }
    }
}

/// Whether a failed S3 call is worth retrying: a transport failure (the
/// request never got a well-formed response at all, incl. a timeout), a
/// `5xx`, `429`, `408` or `SlowDown`/`RequestTimeout` service error — never
/// [`S3Error::NotFound`]/[`S3Error::AccessDenied`], and never any other
/// [`S3Error::Service`] (a client-side mistake, e.g. a malformed request —
/// retrying changes nothing).
fn is_retryable(err: &S3Error) -> bool {
    match err {
        S3Error::Transport(_) => true,
        S3Error::Service { status, code, .. } => {
            *status >= 500
                || *status == 429
                || *status == 408
                || matches!(code.as_str(), "SlowDown" | "RequestTimeout")
        }
        S3Error::NotFound
        | S3Error::AccessDenied
        | S3Error::CredentialsExpired
        | S3Error::Credentials(_)
        | S3Error::InvalidConfig(_) => false,
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
        S3Error::AccessDenied | S3Error::CredentialsExpired => {
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

impl<T: Transport, E: Clock + Rng> S3SegmentStore<T, E> {
    /// Build a store over `transport`/`config`, joining every id under
    /// `prefix` (if given — an empty string is treated as "no prefix").
    /// `env` supplies the wall clock, backoff sleeps and jitter.
    #[must_use]
    pub fn new(transport: T, config: S3Config, prefix: Option<String>, env: E) -> Self {
        Self::from_client(S3Client::new(transport, config), prefix, env)
    }

    /// Build a store over an already-constructed [`S3Client`] — the entry
    /// point for a client with a non-static credential provider or
    /// virtual-hosted addressing (S-08 M1; see `S3Client::with_provider`/
    /// `with_addressing`).
    #[must_use]
    pub fn from_client(client: S3Client<T>, prefix: Option<String>, env: E) -> Self {
        S3SegmentStore {
            client: Arc::new(client),
            prefix: prefix.filter(|p| !p.is_empty()),
            env,
            multipart: MultipartConfig::default(),
            retry: RetryPolicy::default(),
        }
    }

    /// Override the multipart threshold/part size (default: 64 MiB / 16 MiB).
    #[must_use]
    pub fn with_multipart(mut self, multipart: MultipartConfig) -> Self {
        self.multipart = multipart;
        self
    }

    /// Override the retry/backoff policy (default: 5 retries, 100 ms base,
    /// 5 s cap, full jitter).
    #[must_use]
    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    fn now_ms(&self) -> u64 {
        self.env.wall_now().0
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

    /// The one bounded-retry loop every S3 request goes through: `op` is
    /// called with a fresh `now_epoch_ms` per attempt (so SigV4 timestamps
    /// and credential expiry track the env's wall clock across backoffs).
    async fn retry_op<R, F, Fut>(&self, op: F) -> Result<R, S3Error>
    where
        F: Fn(u64) -> Fut,
        Fut: std::future::Future<Output = Result<R, S3Error>>,
    {
        let mut attempt = 0u32;
        loop {
            match op(self.now_ms()).await {
                Ok(v) => return Ok(v),
                Err(e) if attempt < self.retry.max_retries && is_retryable(&e) => {
                    let ceiling = self.retry.backoff_ceiling(attempt);
                    let ceiling_ns = u64::try_from(ceiling.as_nanos()).unwrap_or(u64::MAX - 1);
                    // Uniform in [0, ceiling] inclusive.
                    let jittered = self.env.gen_below(ceiling_ns + 1);
                    self.env.sleep(Duration::from_nanos(jittered)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Upload `bytes` as a multipart upload; abort it (best effort) if any
    /// step finally fails.
    async fn put_multipart(&self, key: &str, bytes: &[u8]) -> Result<(), S3Error> {
        let part_size = self.multipart.effective_part_size(bytes.len() as u64) as usize;
        let upload_id = self
            .retry_op(|now| self.client.create_multipart_upload(key, now))
            .await?;
        let result: Result<(), S3Error> = async {
            let mut parts = Vec::new();
            for (i, chunk) in bytes.chunks(part_size).enumerate() {
                let n = u32::try_from(i + 1).unwrap_or(u32::MAX);
                let etag = self
                    .retry_op(|now| {
                        self.client
                            .upload_part(key, &upload_id, n, chunk.to_vec(), now)
                    })
                    .await?;
                parts.push((n, etag));
            }
            let completed = self
                .retry_op(|now| {
                    self.client
                        .complete_multipart_upload(key, &upload_id, &parts, now)
                })
                .await;
            match completed {
                // A lost `Complete` ack: the first attempt assembled the
                // object, so the retry finds the upload gone. If the object
                // now holds exactly our bytes, the put succeeded.
                Err(S3Error::Service { ref code, .. }) if code == "NoSuchUpload" => {
                    match self.existing_equals(key, bytes).await {
                        Ok(Some(true)) => Ok(()),
                        _ => completed,
                    }
                }
                other => other,
            }
        }
        .await;
        if result.is_err() {
            // Best effort: a failed abort leaves an incomplete upload that
            // a bucket lifecycle rule (AbortIncompleteMultipartUpload) reaps.
            let _ = self
                .retry_op(|now| self.client.abort_multipart_upload(key, &upload_id, now))
                .await;
        }
        result
    }

    /// Whether the object at `key` already holds exactly `bytes`:
    /// `Ok(None)` when absent, `Ok(Some(equal))` otherwise. `HEAD` first
    /// (size mismatch short-circuits), then bounded-memory ranged compares.
    async fn existing_equals(&self, key: &str, bytes: &[u8]) -> Result<Option<bool>, S3Error> {
        let meta = match self.retry_op(|now| self.client.head_object(key, now)).await {
            Ok(m) => m,
            Err(S3Error::NotFound) => return Ok(None),
            Err(e) => return Err(e),
        };
        if meta.size != bytes.len() as u64 {
            return Ok(Some(false));
        }
        let slice = self.multipart.effective_part_size(bytes.len() as u64) as usize;
        let mut offset = 0usize;
        while offset < bytes.len() {
            let want = slice.min(bytes.len() - offset);
            let got = match self
                .retry_op(|now| {
                    self.client
                        .get_object_range(key, offset as u64, want as u64, now)
                })
                .await
            {
                Ok(g) => g,
                Err(S3Error::NotFound) => return Ok(None),
                Err(e) => return Err(e),
            };
            if got != bytes[offset..offset + want] {
                return Ok(Some(false));
            }
            offset += want;
        }
        Ok(Some(true))
    }
}

#[async_trait::async_trait]
impl<T: Transport, E: Clock + Rng + Clone + 'static> crate::SegmentStore for S3SegmentStore<T, E> {
    async fn put(&self, id: &str, bytes: &[u8]) -> std::io::Result<()> {
        if id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "segment id must be non-empty",
            ));
        }
        let key = self.object_key(id);
        // Write-once (SegmentStore::put's own contract, mirroring
        // FsSegmentStore::put exactly): look at whatever is already there
        // first. An identical-content re-put is a safe no-op that skips the
        // network PUT entirely; a differing-content re-put is a hard error.
        // Real S3 has no built-in "PUT only if absent," so this check-then-
        // PUT is how that contract is enforced at this layer — every real
        // caller writes each attempt at its own unique id (see the module
        // doc), so this should never actually observe a differing-content
        // collision in practice. The check is a HEAD plus ranged compares,
        // never a whole-object download (S-08 M2).
        match self.existing_equals(&key, bytes).await {
            Ok(Some(true)) => return Ok(()),
            Ok(Some(false)) => return Err(write_once_violation(id)),
            Ok(None) => {}
            Err(e) => return Err(map_io_error(e)),
        }
        if bytes.len() as u64 > self.multipart.threshold {
            self.put_multipart(&key, bytes).await.map_err(map_io_error)
        } else {
            self.retry_op(|now| self.client.put_object(&key, bytes.to_vec(), now))
                .await
                .map_err(map_io_error)
        }
    }

    async fn get(&self, id: &str) -> std::io::Result<Option<Vec<u8>>> {
        let key = self.object_key(id);
        match self.retry_op(|now| self.client.get_object(&key, now)).await {
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
        self.retry_op(|now| self.client.delete_object(&key, now))
            .await
            .map_err(map_io_error)
    }

    async fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        let full_prefix = self.object_key(prefix);
        let mut out = Vec::new();
        let mut continuation: Option<String> = None;
        for _ in 0..LIST_PAGE_CAP {
            let page = self
                .retry_op(|now| {
                    self.client
                        .list_objects_v2(&full_prefix, continuation.as_deref(), now)
                })
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

    /// Issue exactly one `ListObjectsV2` request and answer from its first
    /// page alone — never the `list`'s own full-pagination loop above
    /// (issue #861: a full drain just to answer a yes/no question is a
    /// slow, billable enumeration of an entire populated bucket, paid on
    /// every node's startup by `verify_or_init_segment_store_marker`'s own
    /// "does anything else exist here" check). Whether that one page is
    /// truncated is irrelevant — a single returned object already answers
    /// "not empty," and `IsTruncated: true` with zero objects cannot
    /// happen (a page only truncates because it *had* something to cut
    /// off).
    async fn is_empty(&self, prefix: &str) -> std::io::Result<bool> {
        let full_prefix = self.object_key(prefix);
        let page = self
            .retry_op(|now| self.client.list_objects_v2(&full_prefix, None, now))
            .await
            .map_err(map_io_error)?;
        Ok(page.objects.is_empty())
    }
}

// `#[tokio::test]` needs the `prod` feature (tokio); the seed-driven retry
// tests live in `animus-test`'s `s3_fault_corpus` over `SimEnv`.
#[cfg(all(test, feature = "prod"))]
mod tests {
    use animus_s3::client::S3Config;
    use animus_s3::fake::FakeS3;
    use animus_s3::sigv4::Credentials;

    use super::S3SegmentStore;

    /// A minimal `Clock + Rng` env for these unit tests: fixed wall clock,
    /// `sleep` returns immediately (backoff pacing is exercised under
    /// `SimEnv` in `animus-test`), counter-seeded randomness.
    #[derive(Clone)]
    struct TestEnv(std::sync::Arc<CounterRng>);

    impl TestEnv {
        fn new() -> Self {
            TestEnv(std::sync::Arc::new(CounterRng::new()))
        }
    }

    #[async_trait::async_trait]
    impl crate::Clock for TestEnv {
        fn now(&self) -> crate::Nanos {
            crate::Nanos(0)
        }
        fn wall_now(&self) -> crate::UnixMillis {
            crate::UnixMillis(1_700_000_000_000)
        }
        async fn sleep(&self, _dur: std::time::Duration) {}
    }

    impl crate::Rng for TestEnv {
        fn next_u64(&self) -> u64 {
            self.0.next_u64()
        }
        fn fill_bytes(&self, dst: &mut [u8]) {
            self.0.fill_bytes(dst);
        }
    }

    fn test_store(prefix: Option<&str>) -> S3SegmentStore<FakeS3, TestEnv> {
        let fake = FakeS3::new("test-bucket").with_credential("AKIDTEST", "secret");
        let config = S3Config {
            endpoint: "http://fake.example:9000".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKIDTEST", "secret"),
        };
        S3SegmentStore::new(fake, config, prefix.map(str::to_string), TestEnv::new())
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

    /// A [`Transport`] wrapper counting how many `ListObjectsV2` requests
    /// pass through it — the regression harness for issue #861: proves
    /// [`S3SegmentStore::is_empty`] answers from exactly one page,
    /// regardless of how many pages the underlying listing actually has.
    struct CountingTransport<T> {
        inner: T,
        list_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl<T: animus_s3::client::Transport> animus_s3::client::Transport for CountingTransport<T> {
        async fn send(
            &self,
            request: animus_s3::client::HttpRequest,
        ) -> Result<animus_s3::client::HttpResponse, animus_s3::client::TransportError> {
            let (_, query) = request.uri.split_once('?').unwrap_or((&request.uri, ""));
            if query.split('&').any(|pair| pair == "list-type=2") {
                self.list_calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.send(request).await
        }
    }

    /// [`S3SegmentStore::is_empty`] against a populated, multi-page
    /// listing must issue exactly **one** `ListObjectsV2` request and
    /// report non-empty — the direct regression for issue #861, where the
    /// old `list("").is_empty()` check drained every page first.
    #[tokio::test]
    async fn is_empty_probes_one_page_against_a_multi_page_listing() {
        use crate::SegmentStore as _;

        let fake = FakeS3::new("test-bucket")
            .with_credential("AKIDTEST", "secret")
            .with_page_size(2);
        let config = S3Config {
            endpoint: "http://fake.example:9000".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKIDTEST", "secret"),
        };
        let list_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting = CountingTransport {
            inner: fake,
            list_calls: list_calls.clone(),
        };
        let store = S3SegmentStore::new(counting, config, None, TestEnv::new());
        for i in 0..5 {
            store
                .put(&format!("page-test/{i}"), format!("v{i}").as_bytes())
                .await
                .expect("put");
        }
        list_calls.store(0, std::sync::atomic::Ordering::SeqCst);

        let empty = store.is_empty("page-test/").await.expect("is_empty");

        assert!(!empty, "a populated prefix must report non-empty");
        assert_eq!(
            list_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "is_empty must issue exactly one ListObjectsV2 request, never paginate"
        );
    }

    /// The empty-store counterpart: no objects at all under `prefix`
    /// still resolves from one request and reports empty.
    #[tokio::test]
    async fn is_empty_probes_one_page_against_an_empty_store() {
        use crate::SegmentStore as _;

        let fake = FakeS3::new("test-bucket").with_credential("AKIDTEST", "secret");
        let config = S3Config {
            endpoint: "http://fake.example:9000".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKIDTEST", "secret"),
        };
        let list_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting = CountingTransport {
            inner: fake,
            list_calls: list_calls.clone(),
        };
        let store = S3SegmentStore::new(counting, config, None, TestEnv::new());

        let empty = store.is_empty("").await.expect("is_empty");

        assert!(empty, "a fresh bucket must report empty");
        assert_eq!(
            list_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "is_empty must issue exactly one ListObjectsV2 request even against an empty store"
        );
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
        let store = S3SegmentStore::new(fake, config, None, TestEnv::new());
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

    // --- S-08 M2: multipart + ranged write-once -------------------------

    use super::MultipartConfig;
    use crate::SegmentStore as _;
    use std::sync::Arc;

    const PART: usize = 16;

    fn mp_store(fake: Arc<FakeS3>) -> S3SegmentStore<Arc<FakeS3>, TestEnv> {
        let config = S3Config {
            endpoint: "http://fake.example:9000".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            credentials: Credentials::new("AKIDTEST", "secret"),
        };
        S3SegmentStore::new(fake, config, None, TestEnv::new())
            .with_multipart(MultipartConfig::new_unchecked(40, PART as u64))
    }

    fn mp_fake() -> Arc<FakeS3> {
        Arc::new(
            FakeS3::new("test-bucket")
                .with_credential("AKIDTEST", "secret")
                .with_min_part_size(PART),
        )
    }

    fn payload(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 13 % 247) as u8).collect()
    }

    fn count(fake: &FakeS3, needle: &str) -> usize {
        fake.request_log()
            .iter()
            .filter(|l| l.contains(needle))
            .count()
    }

    #[test]
    fn multipart_config_validates_part_size_and_caps_part_count() {
        assert!(MultipartConfig::new(64, 5 * 1024 * 1024 - 1).is_err());
        let ok = MultipartConfig::new(64, 5 * 1024 * 1024).expect("5 MiB is the minimum");
        // 10_000 parts of 5 MiB cover 50_000 MiB; beyond that parts grow.
        let big = 10_000u64 * 5 * 1024 * 1024 * 2;
        assert!(ok.effective_part_size(big) * 10_000 >= big);
        assert_eq!(ok.effective_part_size(1024), 5 * 1024 * 1024);
        let d = MultipartConfig::default();
        assert_eq!((d.threshold, d.part_size), (64 << 20, 16 << 20));
    }

    #[tokio::test]
    async fn put_above_threshold_goes_multipart_and_below_does_not() {
        let fake = mp_fake();
        let store = mp_store(fake.clone());
        let small = payload(40); // == threshold: single PUT
        store.put("s/small", &small).await.expect("small");
        assert_eq!(count(&fake, "uploads"), 0);

        let big = payload(PART * 3 + 5);
        store.put("s/big", &big).await.expect("big");
        assert_eq!(count(&fake, "POST /test-bucket/s/big?uploads"), 1);
        assert_eq!(
            count(&fake, "partNumber"),
            4,
            "3 full parts + a 5-byte tail"
        );
        assert_eq!(count(&fake, "POST /test-bucket/s/big?uploadId"), 1);
        assert_eq!(fake.open_upload_count(), 0);
        assert_eq!(store.get("s/big").await.expect("get"), Some(big));
        assert_eq!(store.get("s/small").await.expect("get"), Some(small));
    }

    #[tokio::test]
    async fn failed_part_aborts_the_upload_and_returns_the_error() {
        let fake = mp_fake();
        let store = mp_store(fake.clone());
        fake.fail_upload_part(2, 1_000);
        let err = store
            .put("s/big", &payload(PART * 3))
            .await
            .expect_err("a permanently failing part must fail the put");
        assert!(err.to_string().contains("InternalError"), "{err}");
        assert_eq!(fake.open_upload_count(), 0, "upload must be aborted");
        assert_eq!(store.get("s/big").await.expect("get"), None);
        assert_eq!(count(&fake, "DELETE /test-bucket/s/big?uploadId"), 1);
    }

    #[tokio::test]
    async fn transient_part_failure_is_retried_within_budget() {
        let fake = mp_fake();
        let store = mp_store(fake.clone());
        fake.fail_upload_part(2, 2);
        let big = payload(PART * 3);
        store.put("s/big", &big).await.expect("retried");
        assert_eq!(store.get("s/big").await.expect("get"), Some(big));
        assert_eq!(fake.open_upload_count(), 0);
    }

    #[tokio::test]
    async fn error_in_200_on_complete_aborts_and_fails() {
        let fake = mp_fake();
        let store = mp_store(fake.clone());
        fake.set_complete_error_in_200(Some("InvalidPart"));
        store
            .put("s/big", &payload(PART * 3))
            .await
            .expect_err("a 200 carrying <Error> must fail the put");
        assert_eq!(fake.open_upload_count(), 0);
        assert_eq!(store.get("s/big").await.expect("get"), None);
    }

    #[tokio::test]
    async fn multipart_sized_write_once_uses_head_and_ranged_compare() {
        let fake = mp_fake();
        let store = mp_store(fake.clone());
        let big = payload(PART * 3 + 5);
        store.put("s/big", &big).await.expect("first");

        let uploads_before = count(&fake, "uploads");
        let puts_before = count(&fake, "PUT ");
        // Identical re-put: a no-op, compared via HEAD + ranged GETs.
        store.put("s/big", &big).await.expect("idempotent re-put");
        assert_eq!(count(&fake, "uploads"), uploads_before, "no new upload");
        assert_eq!(count(&fake, "PUT "), puts_before, "no new part/PUT");
        assert!(count(&fake, "HEAD ") >= 1);

        // Same size, different content: violation, found via ranged GET.
        let mut other = big.clone();
        *other.last_mut().expect("non-empty") ^= 0xff;
        let err = store.put("s/big", &other).await.expect_err("violation");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        // Different size: violation, found via HEAD alone (no ranged GET).
        let gets = count(&fake, "GET ");
        let err = store
            .put("s/big", &payload(PART * 3))
            .await
            .expect_err("size mismatch");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(count(&fake, "GET "), gets, "size mismatch needs no GET");
        assert_eq!(store.get("s/big").await.expect("get"), Some(big));
    }

    #[tokio::test]
    async fn contract_holds_with_multipart_forced_on() {
        let fake = mp_fake();
        let store = mp_store(fake).with_multipart(MultipartConfig::new_unchecked(0, PART as u64));
        crate::test_support::assert_segment_store_contract(&store).await;
    }
}
