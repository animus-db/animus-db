# An open cross-plane fault-finding (issue #495, the shared WAL-corruption gap) does not automatically reproduce in every plane that shares the vulnerable codec — confirm per-plane before assuming (`animus-control`'s `control_corpus.rs`, PR②)

Issue #495 is a confirmed, reproducible hard panic in `animus-cp-data`:
composing `DiskConfig::torn_tail_on_crash` with `corrupt_on_crash` lets a
corrupted-but-still-JSON-valid WAL record (`animus-control::persist::
WalRecord`, no per-record checksum — the codec `animus-control` and
`animus-cp-data` **share**) decode successfully with a wrong value, which
that plane's `assert_ts_monotonic` (an HLC-timestamp monotonicity invariant)
then trips on once a later entry applies past it. Building `control_corpus.rs`'s
own `#[ignore]`d regression probe for the identical composition, the natural
assumption was "the codec is shared, so the panic should reproduce here
too" — it did not, across a deliberate 80-combination sweep (many seeds ×
`PlainChurn`/`AllocatorRace` workloads × `LeaderKill`/`FollowerKill` ×
with/without an `FsyncLie`-accumulated un-synced buffer before the crash,
done during development, not committed as code). The underlying codec gap
is real in both places (the corruption fires identically — confirmed by
inspecting `DiskCorrupt`/`DiskTear` trace events), but `animus-control`'s
`Metadata::apply`/recovery path has no invariant as strict as
`assert_ts_monotonic` for a wrong-but-decodable *numeric* field to trip —
this plane's commands carry no HLC timestamp at all, and its epoch/CAS
checks *reject* a mismatch rather than *asserting* on one, so a corrupted
epoch or tablet id just fails a CAS instead of panicking. **General lesson:
a fault-finding confirmed in one plane over a codec/primitive that plane
shares with another does not transfer by assumption — the reproducing
mechanism is downstream of the shared corruption (some specific invariant
the corrupted-but-valid value eventually trips), and a sibling plane may
share the corruption but not the invariant. Confirm (or rule out) the
composition explicitly in each plane it could plausibly reach, and record a
negative result as carefully as a positive one — it is what tells a future
reader whether the standing regression probe is still watching for
something that could happen, or has already been checked and cleared for
that plane's current invariant set.**

**Update**: issue #495's underlying codec gap (`animus-control::persist::
WalRecord` having no per-record checksum) is now fixed — every WAL line
carries a CRC32 checksum, and a corrupted-but-parseable record is dropped
at decode time (along with everything physically after it in the file)
instead of decoding into a wrong value. The methodology lesson above is
unaffected by the fix (it's about how fault-findings do or don't transfer
across planes, not about this specific bug's status); `control_corpus.rs`'s
`control_corrupt_on_crash_may_hard_panic_issue_495` stays in place as a
standing regression probe, now expected to stay clean.
