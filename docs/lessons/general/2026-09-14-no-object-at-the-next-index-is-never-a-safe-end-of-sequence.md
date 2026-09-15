# "No object at the next index" is never a safe end-of-sequence signal for a deletable, externally-reclaimed object sequence (issue #856)

`animusd::backup_restore::restore_tick`'s chunk sweep (and its
self-contained mirror in `crates/animus-test/tests/backup_fault_corpus.rs`)
used `Ok(None)` from a chunked object store read as its *sole* signal that
a reporting tablet's own chunk sequence was exhausted — the same "keep
reading until the store says no" idiom several other per-tablet sweeps in
this codebase use safely, because in those cases nothing else in the
system ever deletes an object out from under an in-progress reader. Backup
chunks are different: `DeleteBackup` can mark a backup `Expired` while a
restore is still reading it, and the two-phase janitor then reclaims its
objects on its own schedule, in **whatever order its own unsorted `list`
happens to return them** (`FsSegmentStore::list` is a plain recursive
directory walk; chunk ids aren't zero-padded, so even a sorted listing
isn't numeric order). A reader using "no object" as "done" cannot tell a
genuine end from a hole punched by an out-of-order reclaim partway
through — and reading a hole *before* the real end is strictly worse than
reading one *at* the end, because the reader has no way to know it wasn't
supposed to stop there: it silently returns everything read so far as a
complete, correct result.

**The fix is a positive, recorded terminal signal, not a smarter absence
check.** `BackupTabletProgress` gained a `chunk_count: u64` field — the
capture driver's own `CaptureCursor::next_chunk` at the moment its capture
completed, i.e. "valid chunk indices are exactly `0..chunk_count`" — and
`restore_tick`'s sweep bounds itself to that recorded count instead of
looping until `Ok(None)`. A miss *inside* the recorded range is now
unambiguously a hole (hard `FailRestore`); a store answering `None` past
the range never happens, because the sweep never asks past it. This
generalizes past backups: **any per-item sweep over a sequence whose
individual items can be deleted by a party other than the sweeper needs an
expected-count (or explicit terminal marker) recorded by the producer at
write time, checked by the consumer, rather than inferring completion from
the consumer's own read failing to find the next item.** "The store said
no" answers "does this specific id exist," never "have I read
everything I was supposed to" — those are different questions, and only
the second is what "done" is supposed to mean. Before shipping a new
sweep-to-exhaustion loop over externally-mutable objects, ask explicitly
whether anything else in the system can delete one of those objects
out of band, and if so, give the sweep something better than absence to
stop on.

Regression: `crates/animus-test/tests/backup_fault_corpus.rs::
delete_backup_mid_restore_fails_restore` — deletes a middle chunk of a
multi-chunk backup directly from the store (standing in for the
`DeleteBackup`+janitor-out-of-order-reclaim race) and asserts the restore
hard-fails; proven red against the old "`Ok(None)` is the sole
end-of-sequence signal" algorithm (the restore silently completed missing
every row from the deleted chunk onward) before the fix, green after.
