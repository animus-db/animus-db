# A guard's escape hatch keyed on a flag must check *why* the flag is set, not just that it's set

**What happened.** Live on `--cluster-control 3 --cluster-data 5
--auto-split-bytes 1000000` under a heavy `BatchWriteItem` bulk seed, a
follower's apply task hard-panicked with `assert_ts_monotonic`'s "HLC ts …
did not strictly exceed the last applied …" — the same panic class the
2026-09-27 stale-snapshot-rewind fix (see this directory's matching entry)
had already closed once, by making `RaftCore::handle_install_snapshot`'s
"already at least this far along" short-circuit compare against
`last_applied` instead of the laggier `snapshot_index`. That fix was
correct and stayed correct. The new panic came from the *other* clause on
the same guard: `last_index <= self.last_applied && !self.
state_machine_behind`. `state_machine_behind` was added for one specific
reason (issue #554: a follower whose engine was wiped and reopened fresh
behind an intact log can't trust its own watermarks, so it must accept even
a same-index offer that would otherwise look redundant) — but the flag
itself carries no record of *why* it's true. It is computed live, every
consensus-loop iteration, as `engine_applied < snapshot_index`, and that
condition is *also* true, completely legitimately, in the ordinary window
after every genuine `InstallSnapshot` completes: `last_applied`/
`snapshot_index` advance synchronously inside `handle_install_snapshot`
itself, while the separate async apply task drains the install into the
engine over the following ticks. An unrelated, already-obsolete transfer's
final chunk landing in that second window — `last_index` strictly *below*
`last_applied`, a shape the wipe case never produces — sailed through the
`!state_machine_behind` override exactly as if it were a genuine #554
offer, reinstalling and rewinding the log.

**The general shape.** A boolean guarding an escape hatch ("skip the normal
check because of condition X") is only as narrow as the set of situations
that actually set it. If the flag's own definition is "recomputed live from
some general fact" rather than "latched exactly at the one call site X
describes," any other situation that happens to make that same general fact
true inherits the escape hatch for free — whether or not it should. Here,
`engine_applied < snapshot_index` is a true statement about *both* "engine
was wiped, log has an old but valid `snapshot_index` it hasn't caught up
to" and "an install just landed and the driver hasn't drained it into the
engine yet," and the override's own comment only ever discussed the first.
The fix wasn't to distinguish the two situations (no separate marker exists
to tell them apart, and adding one would be strictly more invasive than
needed) — it was to notice that the two situations don't actually need the
*same amount* of escape hatch: the wipe case only ever offers `last_index
>= last_applied` (typically exactly equal), so narrowing the override from
"any `last_index <= last_applied`" to "only `last_index == last_applied`"
keeps the #554 case working while closing the case it was never meant to
cover.

**What to do.**

- **When auditing a guard with a documented exception, ask what ELSE could
  set the exception's condition true**, not just whether the documented
  case is handled correctly. A flag's doc comment describing its intended
  trigger is not a proof that nothing else can trigger it — especially for
  a flag recomputed live from a general expression rather than set at one
  call site.
- **The narrowest correct fix is often "restrict which values of the
  primary comparison the exception is allowed to override," not "add a new
  flag to disambiguate."** Here, the override only ever legitimately needs
  to rescue the `==` boundary case; the general `<=` was accidentally doing
  double duty. Before reaching for a new marker/flag to distinguish two
  situations, check whether the existing comparison already has a
  sub-case that's the only one the exception actually needs.
- **A guard's own regression test should include a case that forces the
  exception's condition true for the WRONG reason**, not only the
  documented reason — `crates/animus-control/tests/
  stale_snapshot_no_rewind.rs`'s
  `stale_install_snapshot_below_last_applied_is_rejected_even_when_state_machine_behind`
  sets `state_machine_behind` directly via `RaftCore::
  set_state_machine_behind` (standing in for the post-install-transient
  cause) rather than reproducing a wipe, specifically so the test can't be
  satisfied by accident by whatever already makes the documented case
  pass.
