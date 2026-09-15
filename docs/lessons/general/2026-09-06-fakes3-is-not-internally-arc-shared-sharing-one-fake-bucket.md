# `FakeS3` is not internally `Arc`-shared — sharing one fake bucket across several independently-constructed store handles needs an external `Arc` plus a thin `Transport`-wrapping newtype

`animus_s3::fake::FakeS3` holds its object state behind plain (non-`Arc`)
interior mutability, so two separately-constructed `S3SegmentStore`
instances built from two separate `FakeS3::new()` calls see two disjoint
buckets — fine for a single-store test, wrong for S-05 PR 1's
`dynamo_export.rs`, which needed every node in a 3-node cluster (each
building its own `SegmentStoreHandle::S3`-shaped store via its own
`ExportStoreFactory` call) plus the test's own verification reads to all
observe the *same* bucket. The fix: wrap one `Arc<FakeS3>` the test owns in
a local newtype (`SharedFakeS3(Arc<FakeS3>)`) implementing
`animus_s3::client::Transport` by delegating `send()` to the inner
`Arc`'s own `send()`, then hand a cheap clone of that newtype to every
`S3SegmentStore` constructed anywhere in the test (one per node's factory
call, plus a standalone one for the test's own `get_object` verification
helper). **General form**: a fake/in-memory backend used to simulate a
shared remote resource across multiple independently-constructed client
handles needs either the fake itself to be internally `Arc`-shared, or the
test to hold the one real `Arc` and thread clones of a thin wrapper into
every handle — check which shape a fake actually has before assuming
"construct one per caller" gives you the shared-state semantics the real
remote service would.
