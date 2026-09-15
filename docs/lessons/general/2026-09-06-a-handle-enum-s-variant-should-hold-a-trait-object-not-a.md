# A handle enum's variant should hold a trait object, not a concrete generic type, when only production needs the concrete type (S-04 PR 2, `animusd`)

`SegmentStoreHandle`/`BackupStoreHandle` (`animusd::lib.rs`) each gained a
third variant for the new S3-backed store (ADR 0059's 2026-09-06
amendment). The store itself, `animus_env::S3SegmentStore<T: animus_s3::
client::Transport>`, is generic over its transport specifically so it can
be tested against `animus_s3::fake::FakeS3` (no sockets) while production
uses `animus_s3::prod::HyperRustlsTransport` — the identical shape
`animus_s3::client::S3Client<T>` itself already uses (S-04 PR 1). The
naive way to add the variant would be `S3(animus_env::S3SegmentStore
<animus_s3::prod::HyperRustlsTransport>)` — concretely typed, mirroring
how `Cluster`/`Fs` are concretely typed today. That would have made an
in-crate test wanting to exercise the `S3` variant over `FakeS3` impossible
without also making the *enum itself* generic over a transport type
parameter — a much bigger, more invasive change purely to serve a test.

**The fix**: `S3(Arc<dyn animus_env::SegmentStore>)` — a trait object.
`SegmentStore` is already `#[async_trait]` (boxes its futures), so it's
dyn-compatible for free; `Arc` makes the variant cheaply `Clone` regardless
of which concrete transport backs it. Production still constructs the
concrete `S3SegmentStore<HyperRustlsTransport>` and stores it behind the
same `Arc<dyn ..>` coercion; a test builds the identical variant over
`S3SegmentStore<FakeS3>` with zero changes to either enum's shape, and
every other match arm in both `impl` blocks treats `S3` exactly like `Fs`
(no per-node replica concept) without ever needing to know or care which
concrete transport is underneath. **General form**: when a handle enum's
variant wraps a value that is generic purely so it can be swapped for a
test double, and the enum's own consumers never need to be generic over
that same parameter, a trait object at the variant boundary buys the
testability without forcing genericity onto everything that touches the
enum — reach for this before genericizing an enum (or a struct built
around one) that has no other reason to carry a type parameter.
