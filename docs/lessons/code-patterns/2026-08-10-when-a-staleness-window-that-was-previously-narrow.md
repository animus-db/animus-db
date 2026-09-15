# When a staleness window that was previously "narrow/theoretical" becomes "routine" (a new node shape now hits it constantly instead of almost never), re-audit every cache-tolerant read that feeds a *non-retried, permanent* decision — not just the reads already flagged as commit-wait polls.

**When a staleness window that was previously "narrow/theoretical" becomes
"routine" (a new node shape now hits it constantly instead of almost
never), re-audit every cache-tolerant read that feeds a *non-retried,
permanent* decision — not just the reads already flagged as commit-wait
polls.** Building ADR 0035 PR4's data-only node (`ControlHandle::Remote`,
no local control Raft at all — reads are a genuinely poll-interval-stale
mirror as a matter of course, not a rare crossover), `provision_tablet`'s
"no tablet yet" branch picks a table's *initial, permanent* replica set
from a cache-tolerant `metadata_cached()` scan of `Active` members —
`CreateTablet` only ever succeeds once per table (idempotent,
first-committer wins), so a stale read there doesn't cause a transient
hiccup a later retry heals, it silently and *permanently*
under-replicates the tablet (nothing re-checks and grows an
already-recorded RF policy afterward). This was already a latent hazard
for a normal (`Local`) node — real control-Raft replication lag is
sub-millisecond — so it had never actually fired; a data-only node's
*routinely* stale mirror widened the window enough that a fresh
integration test (two data nodes, one write immediately after both were
confirmed `Active`) flaked on almost every run (replication factor pinned
at 1 instead of 2), not once in a blue moon. Fixed by switching that one
read from `metadata_cached()` to `metadata_fresh().await` — the same
principle already applied to the schema commit-wait polls (see the entry
above), just not yet audited onto this call site, because nothing before
ADR 0035 made its staleness window wide enough to notice. General check
when a change makes an existing staleness assumption materially truer:
grep every `metadata_cached()`/`effective_metadata()` call site the new
node shape can reach, and ask "if this read is one poll interval behind,
does anything downstream treat the result as permanent?" — not just "is
this a commit-wait poll." (`animusd::ClientCtx::provision_tablet`;
`tests/data_only.rs`.)
