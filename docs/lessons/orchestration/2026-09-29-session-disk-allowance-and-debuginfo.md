# Session disk allowance: build the workspace without debuginfo

**Context.** A web session has a fixed total disk allowance (~38 GB). A full-debuginfo
`cargo test --workspace` build (dev/test profiles default to debuginfo) exhausted it and
forced `target/` to be wiped mid-task.

**Lesson.** For gate runs in a size-limited session export
`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0` on every cargo
command (same values every time, so the cache is shared and not rebuilt), never build
`--release`, run gates one at a time, and check `df -h /` between them. Debuginfo is the bulk
of `target/`; the gates themselves do not need it (replay backtraces still show symbol names).
Do not delete anything else to reclaim space; `rm -rf target` is the last resort.
