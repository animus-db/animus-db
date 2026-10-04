//! S3 store retry / fault-injection corpus (S-08 M3).
//!
//! `S3SegmentStore<T, E: Clock + Rng>` takes its wall clock, backoff sleeps
//! and jitter from the env, so under `SimEnv` the whole retry schedule is a
//! pure function of the seed. This corpus drives it over
//! `FaultyTransport<Arc<FakeS3>>` (scripted 5xx / 429 / transport-error /
//! timeout / ack-lost faults) and, for credential cells, a real
//! `CachingProvider<StsWebIdentityProvider>` over `FakeCredentialService`.
//!
//! Invariants checked in every cell: every put that returned `Ok` is
//! readable with identical bytes; a put that returned `Err` leaves nothing
//! partial (the object is absent or, if an ack-lost write landed, exactly the
//! intended bytes); no operation hangs (the scenario settles within a bounded
//! number of simulator steps and within a bounded amount of virtual time).
//!
//! Corpus shape: `animus_test::corpus::for_each_seed` (variant 0 is the
//! canonical name-derived seed), depth knob **`ANIMUS_S3_FAULT_SEEDS`**
//! (default 1). `ANIMUS_SEED=<seed>` replays one seed per cell. Each seed
//! randomizes the fault schedule (burst lengths, fault flavours, which part
//! fails, credential lifetimes).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use animus_env::{Clock, EnvExt, MultipartConfig, RetryPolicy, S3SegmentStore, SegmentStore, nid};
use animus_s3::client::{HttpRequest, S3Client, S3Config, S3Target};
use animus_s3::creds::{CachingProvider, StsWebIdentityProvider, TokenSource};
use animus_s3::fake::{FakeCredentialService, FakeS3, Fault, FaultyTransport};
use animus_s3::sigv4::Credentials;
use animus_sim::{SimEnv, Simulator};
use animus_test::corpus;

const BUCKET: &str = "fault-bucket";
const PART: usize = 16;
const MAX_STEPS: usize = 200_000;

type Faulty = Arc<FaultyTransport<Arc<FakeS3>>>;
type Store = S3SegmentStore<Faulty, SimEnv>;

fn depth() -> usize {
    corpus::seeds_from_env("ANIMUS_S3_FAULT_SEEDS")
}

/// `ANIMUS_SEED` replays exactly one seed per cell; otherwise the house
/// `for_each_seed` expansion.
fn for_each_seed(name: &str, body: impl FnMut(u64)) {
    let mut body = body;
    if let Some(seed) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        body(seed);
    } else {
        corpus::for_each_seed(name, depth(), body);
    }
}

/// Tiny splitmix64 for schedule decisions (pure function of the seed; the
/// store's own jitter comes from the sim env).
struct Prng(u64);

impl Prng {
    fn new(seed: u64) -> Self {
        Prng(seed ^ 0x5DEE_CE66_D1CE_4E5B)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    /// Uniform in `lo..=hi`.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }
    fn pick<'a, T>(&mut self, v: &'a [T]) -> &'a T {
        &v[self.below(v.len() as u64) as usize]
    }
}

/// Run `body` as a sim task and require the whole scenario to settle.
fn drive<F>(sim: &Simulator, env: &SimEnv, seed: u64, body: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    env.spawn_task(body);
    let mut driver = sim.clone();
    assert!(
        driver.run_until_quiescent(MAX_STEPS),
        "seed {seed}: scenario did not settle within {MAX_STEPS} steps (hang)"
    );
}

fn payload(seed: u64, n: usize) -> Vec<u8> {
    let mut p = Prng::new(seed);
    (0..n).map(|_| p.next() as u8).collect()
}

fn config() -> S3Config {
    S3Config {
        endpoint: "http://fake.example:9000".to_string(),
        bucket: BUCKET.to_string(),
        region: "us-east-1".to_string(),
        credentials: Credentials::new("AKIDTEST", "secret"),
    }
}

fn new_fake() -> Arc<FakeS3> {
    Arc::new(
        FakeS3::new(BUCKET)
            .with_credential("AKIDTEST", "secret")
            .with_min_part_size(PART),
    )
}

fn mp() -> MultipartConfig {
    MultipartConfig::new_unchecked(40, PART as u64)
}

/// A store over a fault-injecting transport with an initially empty plan.
fn faulty_store(env: &SimEnv, fake: &Arc<FakeS3>) -> (Store, Faulty) {
    let faulty = Arc::new(FaultyTransport::passthrough(fake.clone()));
    let store =
        S3SegmentStore::new(faulty.clone(), config(), None, env.clone()).with_multipart(mp());
    (store, faulty)
}

/// A fault-free store over the same bucket, to inspect the true state.
fn clean_store(env: &SimEnv, fake: &Arc<FakeS3>) -> S3SegmentStore<Arc<FakeS3>, SimEnv> {
    S3SegmentStore::new(fake.clone(), config(), None, env.clone()).with_multipart(mp())
}

/// Fail the first `n` requests matching `pred` with `fault`, pass the rest.
fn fail_first(
    n: u64,
    pred: impl Fn(&HttpRequest) -> bool + Send + 'static,
    fault: impl Fn(u64) -> Fault + Send + 'static,
) -> animus_s3::fake::FaultPlan {
    let mut matched = 0u64;
    Box::new(move |_, req| {
        if pred(req) {
            let i = matched;
            matched += 1;
            if i < n {
                return fault(i);
            }
        }
        Fault::Pass
    })
}

fn is_plain_put(r: &HttpRequest) -> bool {
    r.method == "PUT" && !r.uri.contains("partNumber=")
}

fn is_part(r: &HttpRequest, n: u32) -> bool {
    r.method == "PUT" && r.uri.contains(&format!("partNumber={n}&"))
}

fn is_complete(r: &HttpRequest) -> bool {
    r.method == "POST" && r.uri.contains("uploadId=")
}

fn count(fake: &FakeS3, needle: &str) -> usize {
    fake.request_log()
        .iter()
        .filter(|l| l.contains(needle))
        .count()
}

/// Upper bound on total backoff sleep for `retries` retries (full jitter
/// draws at most the ceiling each time).
fn backoff_bound(policy: &RetryPolicy, retries: u32) -> Duration {
    (0..retries).map(|n| policy.backoff_ceiling(n)).sum()
}

fn elapsed(env: &SimEnv, since: animus_env::Nanos) -> Duration {
    Duration::from_nanos(env.now().0 - since.0)
}

/// Invariant: every acked put reads back identically.
async fn assert_acked_readable(
    clean: &S3SegmentStore<Arc<FakeS3>, SimEnv>,
    acked: &BTreeMap<String, Vec<u8>>,
    seed: u64,
) {
    for (id, bytes) in acked {
        assert_eq!(
            clean.get(id).await.expect("clean get").as_deref(),
            Some(bytes.as_slice()),
            "seed {seed}: acked put {id} must be readable with identical bytes"
        );
    }
}

/// Invariant: a failed put left nothing partial — absent, or exactly the
/// intended bytes (a lost-ack write that landed).
async fn assert_not_partial(
    clean: &S3SegmentStore<Arc<FakeS3>, SimEnv>,
    id: &str,
    bytes: &[u8],
    seed: u64,
) {
    if let Some(got) = clean.get(id).await.expect("clean get") {
        assert_eq!(
            got, bytes,
            "seed {seed}: a failed put {id} must not leave partial bytes"
        );
    }
}

const FIVE_XX: &[(u16, &str)] = &[
    (500, "InternalError"),
    (503, "ServiceUnavailable"),
    (502, "BadGateway"),
    (504, "GatewayTimeout"),
];

/// A 5xx burst shorter than the retry budget: the put succeeds, spending
/// only backoff-bounded virtual time.
#[test]
fn five_xx_burst_within_budget_succeeds() {
    for_each_seed("s3_fault_5xx_within_budget", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = new_fake();
        let (store, faulty) = faulty_store(&env, &fake);
        let mut p = Prng::new(seed);
        let burst = p.range(1, 5);
        let &(status, code) = p.pick(FIVE_XX);
        faulty.set_plan(fail_first(burst, is_plain_put, move |_| {
            Fault::status(status, code)
        }));
        let bytes = payload(seed, 24);
        let (env2, fake2) = (env.clone(), fake.clone());
        drive(&sim, &env, seed, async move {
            let t0 = env2.now();
            store.put("t/a/1/0", &bytes).await.unwrap_or_else(|e| {
                panic!("seed {seed}: burst {burst} of {status} {code} must be absorbed: {e}")
            });
            let spent = elapsed(&env2, t0);
            let bound = backoff_bound(&RetryPolicy::default(), burst as u32);
            assert!(
                spent <= bound,
                "seed {seed}: spent {spent:?} exceeds the backoff bound {bound:?} for {burst} retries"
            );
            assert_eq!(faulty.faults_injected(), burst, "seed {seed}");
            assert_eq!(count(&fake2, "PUT /fault-bucket/t/a/1/0"), 1, "seed {seed}");
            let clean = clean_store(&env2, &fake2);
            assert_eq!(
                clean.get("t/a/1/0").await.unwrap(),
                Some(bytes),
                "seed {seed}"
            );
        });
    });
}

/// A burst longer than the budget: the put fails with the service error,
/// nothing partial is visible, the time spent is bounded, and the very next
/// put (burst drained) succeeds.
#[test]
fn five_xx_burst_past_budget_fails_cleanly() {
    for_each_seed("s3_fault_5xx_past_budget", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = new_fake();
        let (store, faulty) = faulty_store(&env, &fake);
        let mut p = Prng::new(seed);
        let policy = RetryPolicy::default();
        let attempts = u64::from(policy.max_retries) + 1;
        let burst = p.range(attempts, attempts + 3);
        let &(status, code) = p.pick(FIVE_XX);
        faulty.set_plan(fail_first(burst, is_plain_put, move |_| {
            Fault::status(status, code)
        }));
        let bytes = payload(seed, 24);
        let (env2, fake2) = (env.clone(), fake.clone());
        drive(&sim, &env, seed, async move {
            let t0 = env2.now();
            let err = store
                .put("t/a/1/0", &bytes)
                .await
                .expect_err("a burst past the retry budget must fail the put");
            assert!(
                err.to_string().contains(code),
                "seed {seed}: error must carry the service code: {err}"
            );
            let spent = elapsed(&env2, t0);
            assert!(
                spent <= backoff_bound(&policy, policy.max_retries),
                "seed {seed}: {spent:?}"
            );
            assert_eq!(
                count(&fake2, "PUT /fault-bucket/t/a/1/0"),
                0,
                "seed {seed}: injected 5xx never reach the bucket"
            );
            let clean = clean_store(&env2, &fake2);
            assert_not_partial(&clean, "t/a/1/0", &bytes, seed).await;
            assert_eq!(clean.get("t/a/1/0").await.unwrap(), None, "seed {seed}");
            // The remainder of the burst is shorter than the budget.
            store
                .put("t/a/1/0", &bytes)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: put after the burst drained: {e}"));
            assert_eq!(
                clean.get("t/a/1/0").await.unwrap(),
                Some(bytes),
                "seed {seed}"
            );
        });
    });
}

/// 429 / SlowDown / RequestTimeout are retried like a 5xx.
#[test]
fn throttling_responses_are_retried() {
    for_each_seed("s3_fault_throttling", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = new_fake();
        let (store, faulty) = faulty_store(&env, &fake);
        let mut p = Prng::new(seed);
        let burst = p.range(1, 5);
        let flavours: &[(u16, &str)] = &[
            (429, "TooManyRequests"),
            (503, "SlowDown"),
            (408, "RequestTimeout"),
            (400, "RequestTimeout"),
            (400, "SlowDown"),
        ];
        let picks: Vec<(u16, &str)> = (0..burst).map(|_| *p.pick(flavours)).collect();
        faulty.set_plan(fail_first(burst, is_plain_put, move |i| {
            let (s, c) = picks[i as usize];
            Fault::status(s, c)
        }));
        let bytes = payload(seed, 24);
        let (env2, fake2) = (env.clone(), fake.clone());
        drive(&sim, &env, seed, async move {
            store
                .put("t/a/1/0", &bytes)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: throttling must be retried: {e}"));
            let clean = clean_store(&env2, &fake2);
            assert_eq!(
                clean.get("t/a/1/0").await.unwrap(),
                Some(bytes),
                "seed {seed}"
            );
        });
    });
}

/// A non-retryable 4xx is returned after exactly one attempt.
#[test]
fn client_errors_are_never_retried() {
    for_each_seed("s3_fault_client_errors", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = new_fake();
        let (store, faulty) = faulty_store(&env, &fake);
        let mut p = Prng::new(seed);
        let &(status, code) = p.pick(&[
            (403u16, "AccessDenied"),
            (400, "InvalidRequest"),
            (400, "MalformedXML"),
            (409, "OperationAborted"),
        ]);
        faulty.set_plan(fail_first(
            1_000,
            |_| true,
            move |_| Fault::status(status, code),
        ));
        let (env2, fake2) = (env.clone(), fake.clone());
        drive(&sim, &env, seed, async move {
            let t0 = env2.now();
            store
                .put("t/a/1/0", b"abc")
                .await
                .expect_err("a client error must fail the put");
            assert_eq!(
                elapsed(&env2, t0),
                Duration::ZERO,
                "seed {seed}: a client error must not back off"
            );
            // Existence HEAD hit the fault first: exactly one request total.
            assert_eq!(faulty.requests_seen(), 1, "seed {seed}");
            assert_eq!(fake2.object_count(), 0, "seed {seed}");
        });
    });
}

/// A storm of transport errors, timeouts and lost acks across a mixed
/// workload of single-PUT and multipart objects, plus gets and deletes.
/// Individual ops may exhaust the budget; the invariants must hold anyway.
#[test]
fn transport_error_and_timeout_storm_keeps_invariants() {
    for_each_seed("s3_fault_transport_storm", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = new_fake();
        let (store, faulty) = faulty_store(&env, &fake);
        let mut p = Prng::new(seed);
        // Per-request fault probability (percent) and flavour mix.
        let pct = p.range(20, 60);
        let mut plan_rng = Prng::new(seed ^ 0xFA17);
        faulty.set_plan(Box::new(move |_, req| {
            if plan_rng.below(100) >= pct {
                return Fault::Pass;
            }
            match plan_rng.below(4) {
                0 => Fault::TransportError,
                1 => Fault::Timeout,
                2 => Fault::status(503, "SlowDown"),
                // Ack-lost on the mutating requests, plain error otherwise.
                _ if req.method == "PUT" || req.method == "POST" => Fault::ApplyThenError,
                _ => Fault::TransportError,
            }
        }));
        let (env2, fake2) = (env.clone(), fake.clone());
        drive(&sim, &env, seed, async move {
            let t0 = env2.now();
            let clean = clean_store(&env2, &fake2);
            let mut acked: BTreeMap<String, Vec<u8>> = BTreeMap::new();
            let mut failed: Vec<(String, Vec<u8>)> = Vec::new();
            let mut ops = 0u32;
            for i in 0..10u64 {
                let size = if p.below(2) == 0 { 24 } else { PART * 3 + 5 };
                let id = format!("t/storm/{i}");
                let bytes = payload(seed ^ i, size);
                ops += 1;
                match store.put(&id, &bytes).await {
                    Ok(()) => {
                        acked.insert(id, bytes);
                    }
                    Err(_) => failed.push((id, bytes)),
                }
                // Reads under the storm: either the exact bytes or an error.
                if let Some(id) = acked.keys().next().cloned() {
                    ops += 1;
                    if let Ok(got) = store.get(&id).await {
                        assert_eq!(
                            got.as_deref(),
                            acked.get(&id).map(Vec::as_slice),
                            "seed {seed}"
                        );
                    }
                }
            }
            assert_acked_readable(&clean, &acked, seed).await;
            for (id, bytes) in &failed {
                assert_not_partial(&clean, id, bytes, seed).await;
            }
            // Each op spends at most (retries+1) requests' worth of backoff.
            let policy = RetryPolicy::default();
            let bound = backoff_bound(&policy, policy.max_retries) * (ops * 12);
            assert!(
                elapsed(&env2, t0) <= bound,
                "seed {seed}: unbounded virtual time {:?}",
                elapsed(&env2, t0)
            );
        });
    });
}

/// A lost ack on a single PUT: the retry replays an identical write-once
/// PUT and succeeds; re-putting the same bytes is a no-op, different bytes
/// are a write-once violation.
#[test]
fn ack_lost_put_is_idempotent() {
    for_each_seed("s3_fault_ack_lost_put", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = new_fake();
        let (store, faulty) = faulty_store(&env, &fake);
        let mut p = Prng::new(seed);
        let lost = p.range(1, 3);
        faulty.set_plan(fail_first(lost, is_plain_put, |_| Fault::ApplyThenError));
        let bytes = payload(seed, 24);
        let (env2, fake2) = (env.clone(), fake.clone());
        drive(&sim, &env, seed, async move {
            store
                .put("t/a/1/0", &bytes)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: ack-lost put must retry to Ok: {e}"));
            assert_eq!(
                count(&fake2, "PUT /fault-bucket/t/a/1/0"),
                lost as usize + 1,
                "seed {seed}: every replay reached the bucket"
            );
            assert_eq!(fake2.object_count(), 1, "seed {seed}");
            let clean = clean_store(&env2, &fake2);
            assert_eq!(
                clean.get("t/a/1/0").await.unwrap(),
                Some(bytes.clone()),
                "seed {seed}"
            );
            store
                .put("t/a/1/0", &bytes)
                .await
                .expect("same bytes: no-op");
            let err = store
                .put("t/a/1/0", b"different")
                .await
                .expect_err("different bytes violate write-once");
            assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists, "seed {seed}");
        });
    });
}

/// Multipart: part N fails past the budget — the put errors, the upload is
/// aborted (no leak) and no object appears.
#[test]
fn multipart_part_failure_past_budget_aborts() {
    for_each_seed("s3_fault_multipart_part_past_budget", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = new_fake();
        let (store, faulty) = faulty_store(&env, &fake);
        let mut p = Prng::new(seed);
        let parts = p.range(3, 6) as usize;
        let n = p.range(1, parts as u64) as u32;
        let flavour = p.below(3);
        faulty.set_plan(Box::new(move |_, req| {
            if is_part(req, n) {
                match flavour {
                    0 => Fault::status(500, "InternalError"),
                    1 => Fault::TransportError,
                    _ => Fault::Timeout,
                }
            } else {
                Fault::Pass
            }
        }));
        let bytes = payload(seed, PART * parts);
        let (env2, fake2) = (env.clone(), fake.clone());
        drive(&sim, &env, seed, async move {
            store
                .put("t/big/1/0", &bytes)
                .await
                .expect_err("a permanently failing part must fail the put");
            assert_eq!(
                fake2.open_upload_count(),
                0,
                "seed {seed}: the failed upload must be aborted (part {n} of {parts})"
            );
            let clean = clean_store(&env2, &fake2);
            assert_eq!(clean.get("t/big/1/0").await.unwrap(), None, "seed {seed}");
            assert_eq!(
                count(&fake2, "DELETE /fault-bucket/t/big/1/0?uploadId"),
                1,
                "seed {seed}"
            );
        });
    });
}

/// Multipart: transient part failures within budget (including lost part
/// acks) still complete the object and leak nothing.
#[test]
fn multipart_transient_part_faults_complete() {
    for_each_seed("s3_fault_multipart_transient", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = new_fake();
        let (store, faulty) = faulty_store(&env, &fake);
        let mut p = Prng::new(seed);
        let parts = p.range(3, 6) as usize;
        let n = p.range(1, parts as u64) as u32;
        let k = p.range(1, 5);
        let flavour = p.below(3);
        faulty.set_plan(fail_first(
            k,
            move |r| is_part(r, n),
            move |_| match flavour {
                0 => Fault::status(503, "SlowDown"),
                1 => Fault::ApplyThenError,
                _ => Fault::Timeout,
            },
        ));
        let bytes = payload(seed, PART * parts + 3);
        let (env2, fake2) = (env.clone(), fake.clone());
        drive(&sim, &env, seed, async move {
            store.put("t/big/1/0", &bytes).await.unwrap_or_else(|e| {
                panic!("seed {seed}: {k} faults on part {n} must be absorbed: {e}")
            });
            assert_eq!(fake2.open_upload_count(), 0, "seed {seed}");
            let clean = clean_store(&env2, &fake2);
            assert_eq!(
                clean.get("t/big/1/0").await.unwrap(),
                Some(bytes),
                "seed {seed}"
            );
        });
    });
}

/// Multipart: the `Complete` ack is lost one or more times. The retry finds
/// the upload gone (`NoSuchUpload`) but the object assembled — the put must
/// resolve to success, the object equals the bytes, and nothing leaks.
#[test]
fn multipart_ack_lost_complete_resolves() {
    for_each_seed("s3_fault_multipart_ack_lost_complete", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = new_fake();
        let (store, faulty) = faulty_store(&env, &fake);
        let mut p = Prng::new(seed);
        let parts = p.range(3, 6) as usize;
        let lost = p.range(1, 3);
        faulty.set_plan(fail_first(lost, is_complete, |_| Fault::ApplyThenError));
        let bytes = payload(seed, PART * parts + 7);
        let (env2, fake2) = (env.clone(), fake.clone());
        drive(&sim, &env, seed, async move {
            store.put("t/big/1/0", &bytes).await.unwrap_or_else(|e| {
                panic!("seed {seed}: ack-lost Complete must resolve via the object check: {e}")
            });
            assert_eq!(fake2.open_upload_count(), 0, "seed {seed}");
            let clean = clean_store(&env2, &fake2);
            assert_eq!(
                clean.get("t/big/1/0").await.unwrap(),
                Some(bytes),
                "seed {seed}"
            );
        });
    });
}

fn sts_store(
    env: &SimEnv,
    svc: &Arc<FakeCredentialService>,
    faulty: Faulty,
) -> (
    Store,
    Arc<CachingProvider<StsWebIdentityProvider<Arc<FakeCredentialService>>>>,
) {
    let token: TokenSource = Arc::new(|| Ok("jwt.header.payload".to_string()));
    let provider = Arc::new(CachingProvider::new(StsWebIdentityProvider::new(
        svc.clone(),
        "https://sts.us-east-1.amazonaws.com",
        "arn:aws:iam::123456789012:role/animus",
        "animusd",
        token,
    )));
    let target = S3Target {
        endpoint: "https://s3.example.com".to_string(),
        bucket: BUCKET.to_string(),
        region: "us-east-1".to_string(),
    };
    let client = S3Client::with_provider(faulty, target, provider.clone());
    (
        S3SegmentStore::from_client(client, None, env.clone()).with_multipart(mp()),
        provider,
    )
}

/// Pre-register the credential the provider will issue next, with a server
/// side expiry of `server_life` from now while the provider stamps
/// `provider_life` — a shorter server life makes S3 answer `ExpiredToken`
/// before the client's own 5-minute refresh window opens.
fn arm_next_credential(
    env: &SimEnv,
    fake: &FakeS3,
    svc: &FakeCredentialService,
    provider_life: Duration,
    server_life: Duration,
) {
    let now = env.wall_now().0;
    svc.set_next_expiry_epoch_ms(now + provider_life.as_millis() as u64);
    let (ak, sk, tok) = FakeCredentialService::nth_credential(svc.issued() + 1);
    fake.register_session_credential(ak, sk, tok, now + server_life.as_millis() as u64);
}

/// Session credentials expire repeatedly while the store keeps working (a
/// refreshing provider, optionally with server-side expiry ahead of the
/// client's own idea of it so the `ExpiredToken` retry path is exercised):
/// every op succeeds and the number of STS issuances stays bounded.
#[test]
fn credentials_expiring_mid_run_refresh_and_succeed() {
    for_each_seed("s3_fault_creds_expiring", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = Arc::new(FakeS3::new(BUCKET).with_min_part_size(PART));
        let svc = Arc::new(FakeCredentialService::new());
        let faulty = Arc::new(FaultyTransport::passthrough(fake.clone()));
        let (store, _provider) = sts_store(&env, &svc, faulty.clone());
        let mut p = Prng::new(seed);
        let min = Duration::from_secs(60);
        let provider_life = min * 10;
        // 0: server and client agree; 1: server expires 6 minutes earlier.
        let server_life = if p.below(2) == 0 {
            provider_life
        } else {
            provider_life - min * 6
        };
        // Sprinkle mild 5xx noise on top.
        let mut noise = Prng::new(seed ^ 0xC4ED);
        faulty.set_plan(Box::new(move |_, _| {
            if noise.below(100) < 10 {
                Fault::status(503, "SlowDown")
            } else {
                Fault::Pass
            }
        }));
        let (env2, fake2, svc2) = (env.clone(), fake.clone(), svc.clone());
        drive(&sim, &env, seed, async move {
            let t0 = env2.now();
            let wall0 = env2.wall_now().0;
            let mut acked = BTreeMap::new();
            for i in 0..14u64 {
                arm_next_credential(&env2, &fake2, &svc2, provider_life, server_life);
                let id = format!("t/creds/{i}");
                let size = if p.below(3) == 0 { PART * 3 + 1 } else { 24 };
                let bytes = payload(seed ^ i, size);
                store.put(&id, &bytes).await.unwrap_or_else(|e| {
                    panic!("seed {seed}: op {i} must succeed across expiry: {e}")
                });
                acked.insert(id, bytes);
                env2.sleep(Duration::from_secs(p.range(20, 150))).await;
            }
            for (id, bytes) in &acked {
                arm_next_credential(&env2, &fake2, &svc2, provider_life, server_life);
                assert_eq!(
                    store.get(id).await.expect("get").as_deref(),
                    Some(bytes.as_slice()),
                    "seed {seed}: {id}"
                );
            }
            let run_ms = (env2.wall_now().0 - wall0).max(1);
            let issued = u64::from(svc2.issued());
            // A credential lasts at least 4 minutes before it is replaced.
            let bound = run_ms / (4 * 60_000) + 2;
            assert!(
                issued >= 2,
                "seed {seed}: credentials must have rotated, issued {issued}"
            );
            assert!(
                issued <= bound,
                "seed {seed}: {issued} issuances in {run_ms} ms exceeds bound {bound}"
            );
            assert!(
                elapsed(&env2, t0) < Duration::from_secs(3 * 3600),
                "seed {seed}"
            );
        });
    });
}

/// The credential source fails when a refresh is due: the op surfaces an
/// error (no panic, no hang, bounded virtual time) and the store recovers
/// once the source does.
#[test]
fn provider_refresh_failure_surfaces_and_recovers() {
    for_each_seed("s3_fault_creds_refresh_failure", |seed| {
        let sim = Simulator::new(seed);
        let env = sim.env(nid(0));
        let fake = Arc::new(FakeS3::new(BUCKET).with_min_part_size(PART));
        let svc = Arc::new(FakeCredentialService::new());
        let faulty = Arc::new(FaultyTransport::passthrough(fake.clone()));
        let (store, _provider) = sts_store(&env, &svc, faulty);
        let mut p = Prng::new(seed);
        // Retryable (STS 503) or terminal (STS 400) failure flavour.
        let (status, code) = *p.pick(&[
            (503u16, "ServiceUnavailable"),
            (400, "InvalidIdentityToken"),
        ]);
        let (env2, fake2, svc2) = (env.clone(), fake.clone(), svc.clone());
        drive(&sim, &env, seed, async move {
            let life = Duration::from_secs(10 * 60);
            arm_next_credential(&env2, &fake2, &svc2, life, life);
            store.put("t/c/1", b"before").await.expect("first put");
            // Past the credential's expiry, with the credential service down.
            env2.sleep(life + Duration::from_secs(60)).await;
            svc2.set_failure(Some((status, code, "down")));
            let t0 = env2.now();
            let err = store
                .put("t/c/2", b"during")
                .await
                .expect_err("a failed refresh must surface as an error");
            let policy = RetryPolicy::default();
            assert!(
                elapsed(&env2, t0) <= backoff_bound(&policy, policy.max_retries),
                "seed {seed}: {err}"
            );
            let clean = S3SegmentStore::new(fake2.clone(), config(), None, env2.clone());
            drop(clean);
            // Recovery.
            svc2.set_failure(None);
            arm_next_credential(&env2, &fake2, &svc2, life, life);
            store
                .put("t/c/2", b"after")
                .await
                .expect("put after recovery");
            assert_eq!(
                store.get("t/c/1").await.unwrap(),
                Some(b"before".to_vec()),
                "seed {seed}"
            );
            assert_eq!(
                store.get("t/c/2").await.unwrap(),
                Some(b"after".to_vec()),
                "seed {seed}"
            );
        });
    });
}

/// The retry schedule (jittered backoff sleeps, SigV4 timestamps) is a pure
/// function of the seed: two runs of the same faulty scenario spend exactly
/// the same virtual time and issue exactly the same request sequence.
#[test]
fn same_seed_replays_identically() {
    for_each_seed("s3_fault_replay_determinism", |seed| {
        let run = || {
            let sim = Simulator::new(seed);
            let env = sim.env(nid(0));
            let fake = new_fake();
            let (store, faulty) = faulty_store(&env, &fake);
            faulty.set_plan(fail_first(4, is_plain_put, |_| {
                Fault::status(503, "SlowDown")
            }));
            let out = Arc::new(std::sync::Mutex::new(None));
            let (env2, fake2, out2) = (env.clone(), fake.clone(), out.clone());
            drive(&sim, &env, seed, async move {
                let t0 = env2.now();
                store.put("t/a/1/0", b"replay").await.expect("put");
                *out2.lock().unwrap() = Some((elapsed(&env2, t0), fake2.request_log()));
            });
            let taken = out.lock().unwrap().take();
            taken.expect("scenario ran")
        };
        let (a, b) = (run(), run());
        assert_eq!(a, b, "seed {seed}: replay diverged");
        assert!(
            a.0 > Duration::ZERO,
            "seed {seed}: backoff must consume virtual time"
        );
    });
}
