# A workflow can list a gitignored file as a trigger path for years without anyone noticing the file never exists in CI (`Cargo.lock`, S-07a, 2026-09-02)

`.github/workflows/image.yml` listed `Cargo.lock` in both its `push` and
`pull_request` `paths:` filters since the image workflow was written
(ADR 0060 Part 2), while `.gitignore:3` gitignored that same file the
entire time. Neither side was wrong in isolation — a `paths:` filter on a
file that happens not to exist just never fires on that file — but the
combination meant the trigger path was silent dead weight and, more
importantly, that every image build (CI and any real deployment) resolved
its own dependency versions fresh from the registry at build time with no
committed, reviewed lockfile pinning them: a supply-chain gap that sat
undetected because nothing about it *fails* — a missing lock doesn't error,
it just silently makes every build a little less reproducible than it
looks. Caught only by an explicit audit (`docs/roadmap.md`'s S-07a) cross-
referencing `.gitignore` against every workflow's `paths:` list, not by any
test or gate. **The general lesson**: a `paths:` filter (or any config that
*names* a file) is not evidence the file is tracked — grep `.gitignore`
for anything a workflow, Dockerfile `COPY`, or path filter names, especially
for lockfiles (`Cargo.lock`, `package-lock.json`, `poetry.lock`, ...) where
"gitignored" is a very easy default to reach for early in a project and a
very easy thing to forget to revisit once the project ships reproducible
builds as a goal.

**A second, smaller lesson from the same change**: `cargo deny check` does
not accept `--locked` (confirmed via `cargo deny check --help`, which lists
no such flag) — it always resolves against whatever `Cargo.lock` is present
on disk (or fails outright if none exists and none can be generated), so
committing the lock is what makes its output reproducible, not a flag on
its own invocation. Don't assume every `cargo <subcommand>`-shaped CLI
tool shares `cargo build`/`cargo test`'s flag surface; check `--help` on
the actual tool before adding a flag to a wrapped invocation.

**A third lesson, on multi-image Dockerfiles**: adding a second published
image (`animus-operator`, alongside the existing `animusd` image) did not
need a second Dockerfile — the existing root `Dockerfile` was already a
multi-stage build of the whole workspace with one shared `builder` stage,
so the natural extension was one more `FROM ... AS <name>` runtime stage
drawing from the same builder (`COPY --from=builder`) and a second
`docker build --target <name>` invocation, matrixed in CI
(`docker/build-push-action`'s `target:` input) with a distinct GHA cache
`scope` per target so the two parallel matrix jobs don't race writing the
same cache key. A `crates/<crate>/Dockerfile` per binary is the right call
only when the binaries genuinely don't share a build (different toolchain,
different base image, unrelated dependency graphs) — here they're two
binaries from the same `cargo build` invocation, so one Dockerfile with one
shared compile stage is strictly less duplication than two.

**A near-miss caught only by re-reading the finished file, not by any
gate**: the first draft appended the new `runtime-operator` stage *after*
the pre-existing `runtime` stage, textually at the bottom of the file where
a new addition naturally goes. That silently changes `docker build .`'s
behavior with no `--target` flag — Docker builds whichever stage is
declared **last** in the file, not the first, and not the one historically
treated as "the" image. Every caller that matrices explicitly
(`.github/workflows/image.yml`'s new `target:` input) was fine either way,
but `scripts/e2e-kind.sh` and the Dockerfile's own header-comment example
both call `docker build -t animusd:e2e .` with no `--target`, and would
have silently started building and tagging the *operator* binary as
`animusd:e2e` — a wrong-binary bug with no error message anywhere, only a
container that fails at `docker run` in a completely unrelated way (or,
worse, "succeeds" by running the wrong thing). Nothing in `cargo fmt`/
`clippy`/`cargo test` can catch this class of bug — it lives entirely in
Dockerfile stage order, which no gate parses. **The general lesson**: when
adding a new stage to a multi-stage Dockerfile that already has an implicit
default consumer (anything that builds it with no `--target`), the new
stage goes *before* the existing default in file order, never after —
check `grep -n '^FROM' Dockerfile` after any such edit and confirm the last
line is still the stage every no-`--target` caller expects.
