# A strict-open upgrade/restart harness exposes recovery bugs that a destroy-and-reopen fallback in older corpora masked.

**A strict-open upgrade/restart harness exposes recovery bugs that a
destroy-and-reopen fallback in older corpora masked.** The raftkv corpus
reopens an engine that fails to open by wiping it and letting Raft repair the
replica, which is a reasonable liveness choice for a replication test but
turns "recovery is broken" into "recovery looked fine". The ADR 0073 tier-1
`upgrade_restart_corpus` opens every engine with `.expect`, so a failed open
is a failed cell. Within days it found three real bugs the older corpora had
never flagged: CWL1/SWL1 treating a CRC failure anywhere as a torn tail, a
`wal_lock` starvation of the ADR 0038 apply task (#1133), and an `LsmEngine`
WAL torn-header open failure. Why it matters: a fallback hides exactly the
failures an upgrade harness exists to find (a bad transcode looks like a clean
wipe). Rules: never copy a destroy-and-reopen fallback into a recovery or
upgrade harness; give it a wall-clock watchdog too, since a zero-virtual-time
livelock never trips an in-sim budget; and keep negative controls
(a corrupted restart must fail) so the strictness stays proven.
