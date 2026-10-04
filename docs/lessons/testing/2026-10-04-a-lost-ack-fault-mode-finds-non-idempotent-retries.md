# A lost-ack fault mode finds retries that are not idempotent; a retry loop behind a non-Env type cannot be seed-tested

**Context**: S-08 M3, moving `S3SegmentStore`'s retry/backoff behind the `Env`
seam and adding `s3_fault_corpus`.

**Lesson 1 — inject "applied, then the response is lost", not just "failed".**
`FaultyTransport`'s `ApplyThenError` forwards the request to the inner fake and
then replaces the response with a transport error. A plain error/5xx fault
never applies the request, so every retry looks idempotent. The first corpus
run with ack-lost faults on `CompleteMultipartUpload` failed immediately: the
retry found the upload gone (`NoSuchUpload`) although the object had already
assembled, so `put` returned `Err` for data that was durably written. Fix: on
`NoSuchUpload` from a retried Complete, HEAD+compare the object and treat an
exact match as success. Any retried mutating call needs a lost-ack cell.
Residual (documented, not fixable client-side): a lost `Create` ack orphans an
upload id nobody knows; only a bucket lifecycle rule reaps it.

**Lesson 2 — a retry loop that sleeps via a concrete runtime and reads the real
clock cannot have a seed-reproducible test.** The old store carried a
module-level `#![allow(clippy::disallowed_methods)]` for `tokio::time::sleep`
and `SystemTime::now` because it was "deliberately not `Env`-generic". That
allow was the symptom: backoff, jitter and credential-expiry timing were
untestable. Making the type generic over `E: Clock + Rng` (a feature-gated
`s3` module that needs no tokio) let `SimEnv` drive the exact production retry
code and assert virtual-time bounds (`sum of RetryPolicy::backoff_ceiling`).
If a module needs a lint allow for time/sleep/randomness, ask whether the type
should take the env instead of adding the allow.

**Lesson 3 — assert time with an upper bound derived from the policy.** With
full jitter the sleep is uniform in `[0, ceiling]`, so tests assert
`elapsed <= sum(ceilings)` (and `> 0` only in a fixed-seed replay check), never
an exact duration.
