# A CI container pull is infrastructure: pull it explicitly, with retries

**What happened.** `s3-real-endpoint` went red on three unrelated PRs
(#1266, #1267, #1268) on 2026-10-10. Each time the `Create bucket` step
exited 125, so `docker run` itself failed before the AWS CLI ran. Moving
the images off Docker Hub on 2026-10-09 had not fixed it, because
unauthenticated ECR Public pulls from shared runner IPs also fail
intermittently. A re-run of the same commit passed.

**Rule.** When a job pulls a third-party image, the pull is part of the
runner's infrastructure, like checkout or toolchain install. It is not the
behaviour under test. Do the pull as its own `docker pull` with a few
bounded retries before `docker run`. The log then names the failed pull,
not an exit code from `docker run`. The "flakiness is a bug, never retry"
rule (root `CLAUDE.md`) still applies in full to the test step: a retry
belongs only on the fetch of an artifact, never on a test.
