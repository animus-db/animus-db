# When a driver stops doing something synchronously, audit every other writer of the state it used to own exclusively — "safe because nothing else can observe it mid-flight" is a precondition, not a property (issue #279).

**When a driver stops doing something synchronously, audit every other
writer of the state it used to own exclusively — "safe because nothing
else can observe it mid-flight" is a precondition, not a property (issue
#279).** `apply_and_compact` discarding the consensus loop's un-persisted
`RaftCore::pending` under `wal_lock` was correct and documented for as long
as the loop drained inline: the loop could not be mid-anything, because it
was blocked. The moment persistence moved off the loop, that same discard
became a silent theft. Nothing about the compaction code changed or looked
wrong in review — the invariant it rested on was in the *other* task's
control flow. When making a synchronous step concurrent, enumerate the
state it touched and find every other writer; each one is a place where an
unstated "…while the loop is blocked" may be doing load-bearing work.
