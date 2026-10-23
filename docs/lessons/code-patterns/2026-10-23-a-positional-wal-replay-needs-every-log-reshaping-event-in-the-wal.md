# A positional WAL replay needs every log-reshaping event in the WAL, in order

**Context (issue #1368, soak A/C).** After a `kill -9` + restart one replica's
apply task panicked in `assert_ts_monotonic`: two consecutive same-term
`TxnResolve` entries with the identical HLC ts. The other replicas were fine.
The surviving WAL showed the same entry (index 18326) appended twice back to
back.

**Mechanism.** `PersistedState::replay` rebuilds the log by *position*
(`push` / `truncate(keep)`), and `RaftCore::term_at`, `apply`'s index labels
and `Truncate{keep}` are all positional too. `handle_install_snapshot` moves
the base and clears the in-memory log but writes **nothing** to the WAL; the
base only reaches the WAL with the apply task's later rewrite (it must install
the engine image first). In that window the consensus loop persists the
leader's next `AppendEntries` (indexes past the new base) onto the OLD file. A
crash there replays `[old tail] ++ [new entries]`: a gap before the new
entries, so every position holds an entry one index too high. The leader's
resend then matched on the wrong entry (`term_at` out of range = 0, a "conflict"),
`Truncate{keep == len}` removed nothing, and the entry was appended a second
time. The apply task labelled entries by position, so it applied the same
entry (same ts) twice in a row and the monotonicity assert fired.

**Rule.**
- Any in-memory reshaping of the log (prefix drop, clear, rebase) must reach
  the WAL **in log order** with the records that follow it, or the following
  records must be withheld until it does. "A rewrite will make it durable
  soon" is not enough when appends keep flowing in between.
- A positional replay should be paired with a cheap recovery invariant
  (`entry.index == base + 1 + position`, `PersistedState::check_log_contiguous`)
  checked before the core is built; a violation is a loud halt, never a
  silent shift.
- Making the WAL run ahead of a second durable store (here the engine) needs
  the recovering side to cope with "WAL ahead of engine". The data plane does
  (`state_machine_behind`, #554); the control plane does not, so the fix is
  opt-in (`RaftCore::set_wal_records_install`) rather than global. The
  corpus caught this (`chunked_snapshot_receiver_stop_restart_3`): run the
  whole `animus-control` suite, not just the new test, after changing what a
  shared core persists.

**Debugging tip that found it.** The panic's `prev_entry=(18325, ..)` carried
the *same* ts as `this_entry`, yet the decoded WAL showed 18325 with a smaller
ts: the apply labels are positional, so the labels disagreed with the entries'
own indexes. Decode the WAL with the crate's own record format and compare
`entry.index` against position before theorising about the leader.
