# When you fix a shared-pattern bug in one crate, grep sibling crates for the verbatim copy.

**The `wal_lock` starvation fixed in `animus-control` (unfair `futures::lock::Mutex`
vs. a back-to-back persist loop) was a copy-pasted design: `animus-cp-data` had the
same `persist_wal`/`apply_and_compact`/`wal_lock` trio and the same bug**, found only
because the control-plane lesson said to check. After fixing a concurrency or
liveness pattern, grep the other crates for the same type, function names and
comments before calling it done, and port the regression test too.

Also: a flag-gated path can still take the default path's lock *before* the flag
branch. `persist_wal` locked `wal_lock` and only then branched on `shared`, so the
`SharedWal` path (opt-in, separate fsync machinery) starved the apply task exactly
as the per-group path did. When reviewing "this path is different" claims, read
what runs before the branch, and test both paths (the new test runs each over the
same seeds). Test: `animus-cp-data/tests/apply_not_starved_by_wal_lock.rs`.
