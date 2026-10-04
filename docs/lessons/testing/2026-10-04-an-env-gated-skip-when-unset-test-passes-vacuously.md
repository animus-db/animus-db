# An env-gated "skip when unset" real-endpoint test passes vacuously, so the CI job that exists to run it must make the skip fail

**A test that prints a skip line and returns when its endpoint env var is
unset is green whether or not it ran** — a CI job that merely invokes it
(wrong env name, a failed server start that was not checked, a typo in a
secret) goes green having proved nothing. The job that exists to run such a
test must set a "require" switch (`ANIMUS_S3_REQUIRE_ENDPOINT=1`) that turns
the missing-endpoint skip into a panic, while the local/default workspace run
keeps skipping. Same shape applies to any opt-in infrastructure test.

**Also verify which server a doc claims vs what the workflow actually
runs.** The "MinIO" real-endpoint leg's prose and test file names said MinIO,
but the only S3 server anything in CI actually ran was RustFS (MinIO images
stopped resolving, issue #863; `scripts/e2e-kind.sh` pins
`rustfs/rustfs:1.0.0-rc.6`), and the cargo tests were not run in CI at all.
Read the workflow, not the comments. (S-08 M4, `s3-real-endpoint` job.)
