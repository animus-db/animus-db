# A one-shot file header's "written" flag must flip exactly when the write that carries it returns `Ok` — not before, not only after `sync`

**What it's for.** ADR 0073 Phase 0's LSM WAL file header (`LWL1` + `u8`
version, `crates/animus-storage/src/lsm/wal.rs`) is written once per segment
file, prepended to that segment's first-ever batch in the same `Disk::append`
call as the batch's own records, so it becomes durable in exactly the `sync`
that covers them. `GroupCommit::Inner::active_seg_needs_header` tracks, for
the currently-active segment, whether the *next* batch must carry it.

**The timing that matters.** That flag flips to `false` the instant the
`append` carrying the header returns `Ok` — deliberately neither earlier nor
later:

- **Not before the `append` resolves** (e.g. alongside a speculative,
  pre-outcome byte-count bump the same code path also does, which *is* safe
  to do speculatively because it only feeds a rotation-size heuristic). A
  failed `append` leaves the on-disk file exactly as it was — no partial
  write — so flipping the flag before knowing the call succeeded would leave
  a segment with no header at all the moment that specific call happened to
  fail. Unlike the harmless byte-count heuristic, a missing header is a real,
  later-fatal format error on reopen (`decode_wal` sees a foreign/absent
  magic and refuses to guess).
- **Not after `sync` succeeds.** A failed `sync` does not roll back an
  already-`append`ed buffer — the bytes stay written, to be flushed
  whenever some later `sync` on that file eventually succeeds. If the flag
  waited for `sync`, a retried batch on the same segment would prepend a
  **second** header ahead of a buffer that already starts with one, the
  moment that first sync-failed append is ever followed by a later
  successful sync — corrupting the file with no crash involved at all.

**The general shape.** A one-shot "have I already written this exactly-once
prefix" flag has exactly one correct flip point: the first I/O call whose
own success durably (in the "won't be silently undone" sense, not
necessarily fsynced) commits that prefix to the resource. Any earlier flip
risks a false-positive skip after a failed attempt (the prefix never lands
at all); any later flip — gated on a *subsequent*, independent operation's
success — risks a false-negative re-write after that later operation's own
failure, because the earlier operation's effect wasn't rolled back by the
later one failing. The two checks to run when adding this pattern: "does a
failure of the write I'm gating on actually leave nothing behind?" (governs
not-before) and "does a failure of whatever I'm waiting on afterward undo
the write I already made?" (governs not-after). Here `append`'s own success
is the only event that satisfies both — `sync`'s success proves durability,
which this flag doesn't need, and `sync`'s *failure* proves nothing about
whether the header is still sitting in the file (it is).

See `crates/animus-storage/src/lsm/wal.rs`'s own module docs ("File-level
format header") for the full crash-safety argument this is extracted from,
and `crates/animus-storage/CLAUDE.md`'s matching WAL-header bullet.
