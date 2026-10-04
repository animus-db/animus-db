# Extending a widely struct-literal'd config: add a builder, not a field

**Found while:** S-08 M1 (non-static S3 credentials + virtual-hosted addressing).

`animus_s3::client::S3Config { endpoint, bucket, region, credentials }` is built
as a struct literal in ~15 places across four crates (tests, benches, a
real-endpoint test another agent was editing concurrently). Adding an
`addressing` field (or turning `credentials` into a provider) would have been a
compiler-enumerated fan-out and would have broken files nobody on the change
owned. Instead the new knobs live on the client: `S3Client::with_provider(..)`
and `S3Client::with_addressing(..)` (the latter validating and returning
`Result`), and `S3Config` stayed byte-for-byte compatible. Prefer this whenever a
public all-pub-fields struct is literal-constructed outside its crate.

Two smaller notes from the same change:

- The pure `animus-s3` crate must not depend on tokio, so single-flight
  credential refresh uses a ~40-line hand-rolled async gate (a `Mutex<bool +
  Vec<Waker>>` that wakes all waiters on drop). It guards no data — the cache is
  a separate `std::sync::Mutex` — which keeps it free of `unsafe`. Test it with a
  provider that `yield_now()`s mid-fetch on a current-thread runtime, otherwise a
  synchronous fake never actually interleaves and the test proves nothing.
- A forced refresh must be keyed on the *rejected* credentials
  (`refresh(&stale, now)`): N callers all rejected with `ExpiredToken` would
  otherwise each trigger an upstream STS call; with the key, the first refreshes
  and the rest see a cache entry that is no longer `stale` and reuse it.
