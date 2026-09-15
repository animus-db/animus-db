# An unpinned `:latest` on a third-party registry is an unversioned dependency on someone else's publishing decisions (2026-09-13, #863)

`scripts/e2e-kind.sh` pulled `minio/minio:latest` and `minio/mc:latest` to
stand up the S3 target for the `E2E_S3` leg. Both repositories stopped
resolving on Docker Hub, and `e2e-kind-s3` went red on every branch at
once — before a single assertion ran, with nothing in the repo to fall
back to and no way to reproduce the previously-green state. The floating
tag is what turned an upstream change into an instant, unrecoverable
break: a pinned digest or release tag would have kept pulling the image
already cached in the registry's history, and the failure would have been
a deliberate bump instead of an outage. Pin third-party images used by CI,
and treat a bump as a change worth reviewing.

Three things about diagnosing this generalize beyond the image:

**A registry's "pull access denied ... may require authorization:
insufficient_scope" is ambiguous, and the ambiguity matters.** It reads
like a credentials or rate-limit problem — the two explanations that
suggest waiting or adding a secret — but it is also what a registry says
when the repository is simply gone. Querying the registry API directly
(`https://hub.docker.com/v2/repositories/<repo>/`) separates them:
`object not found` is not `429 toomanyrequests`. That query is only
evidence with a **control** alongside it; a second, known-good repository
fetched in the same breath is what rules out the network path and the
proxy, and turns "probably not our bug" into a fact. Kubelet retrying
four times inside the failed job is not that evidence — it only shows the
failure is not transient.

**When one image from a vendor disappears, check every image from that
vendor.** The failing pod named only `minio/minio`, so that is the fix
that suggests itself; `minio/mc`, used later in the same leg to create the
bucket, had gone too and would have failed the next step after a
one-line image swap "fixed" the first one.

**Swapping an S3-compatible test double is not an image swap.** Moving to
`rustfs/rustfs` changed five things that all have to agree, none of them
the image reference: the credential env var names
(`MINIO_ROOT_USER`/`_PASSWORD` → `RUSTFS_ACCESS_KEY`/`_SECRET_KEY`), the
data-directory contract (an `args: ["server", "/data"]` argument →
a `RUSTFS_VOLUMES` env var), the health endpoint the readiness probe hits
(`/minio/health/ready` → `/health`), the container's user — RustFS runs as
non-root UID 10001, so an `emptyDir` created root-owned needs `fsGroup`
or the pod never goes ready — and, in the replacement bucket-creation
pod, the S3 client's addressing style: `aws-cli` defaults to virtual-host
addressing, which resolves the bucket as a DNS name
(`<bucket>.rustfs.<ns>.svc`) and fails, so `addressing_style = path` is
load-bearing rather than cosmetic. A migration like this is worth reading
as a contract change, not a find-and-replace.
