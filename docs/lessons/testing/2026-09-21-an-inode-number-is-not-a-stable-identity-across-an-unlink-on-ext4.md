# An inode number is not a stable identity across an unlink — ext4 reuses a freed one eagerly, tmpfs never does, so a race test that snapshots one can be sound on a dev box and unsound on an ext4 CI runner or sandbox root (2026-09-21, issue #1003).

**An inode number is not a stable identity across an unlink — ext4
reuses a freed one eagerly, tmpfs never does, so a race test that
snapshots one can be sound on a dev box and unsound on an ext4 CI
runner or sandbox root (2026-09-21, issue #1003).** The issue #511
regression suite's red test
(`crates/animusd/tests/panic_safe_teardown.rs::
bare_tempdir_removes_its_directory_out_from_under_a_live_background_writer_on_panic`)
races a live background thread recreating `dir/wal` against a
panicking `TempDir::drop`'s `remove_dir_all`. Issue #555 (2026-09-02)
found the test's original `!path.exists()` assertion was unsound —
`remove_dir_all` can lose the unlink-vs-rmdir recreate race, leaving
the directory behind with a writer-recreated `wal` inside — and fixed
it by capturing `wal`'s inode **number** right before the panic,
asserting that whatever exists at that path afterward, if anything,
resolves to a *different* number. That fix's own doc claimed "0/200
under load, closing the gap" and "there is no fourth outcome."

**Both claims were wrong, for the identical underlying reason: an
inode number is only a stable identity for as long as nothing reuses
it.** tmpfs never recycles a freed inode number, so on tmpfs the
inode-number check is sound and #555's own load testing (which never
stated which filesystem it ran on) never found the hole. ext4 recycles
a freed inode number *eagerly* — a freed number becomes the lowest
free bit in its own flex group's inode bitmap, and the very next
`O_CREAT` in that group claims it. When the background writer wins the
unlink-vs-rmdir recreate race on ext4, the file it recreates can land
on the exact same inode number the original `wal` had. On that run the
directory persists, the "different inode" disjunct reads `false`, and
no I/O error was ever observed either (`File::create` against a
still-present parent directory just succeeds) — the fourth outcome
#555's own doc asserted didn't exist. Measured directly on this repo's
own ext4 sandbox root under CPU load: ~1/6000 iterations of a
standalone mimic of the test's own loop; a loop-mounted ext4 filled to
near-ENOSPC hit it more often, ~3/3000. Filling the disk was not the
issue's own reported mechanism, either — `unlink`/`rmdir`/`O_CREAT`
all still succeed at 100% full on ext4, measured directly — near-full
disk only shifts the race's own scheduling odds, amplifying an
already-real bug rather than causing a different one.

**#555's own "0/200, closes the gap" claim was itself a third
relocation of the same ambiguity, not a fix that removed it.** The
original bug (`!path.exists()` alone) had one blind spot: the
directory-persists-with-a-different-`wal` outcome. The naive
"or the writer observed an I/O error" fix people reach for next has a
second, narrower blind spot: the writer can win the recreate race with
no error at all. #555's inode-number fix closed both of those — but
inherited a third, unstated assumption (inode numbers don't get
reused) that happened to hold on whatever filesystem #555 was tested
against and silently didn't hold on ext4. Each fix looked, from
inside, like it had finally enumerated every outcome; each one had
simply moved the ambiguity to a case its own load testing didn't
happen to exercise. The generalizable form: **a real-thread test that
exists to demonstrate a race is only as sound as its enumeration of
every outcome that race can produce — verify that enumeration under
load both before *and* after each fix, on the actual filesystem the
test will run on, not just once, and not just the filesystem
convenient for local iteration.**

**The fix that actually holds regardless of filesystem: don't snapshot
an identity, pin one.** Opening the original `wal` file and holding
that `File` handle across the panicking drop does two things a bare
`stat`-and-compare cannot: it makes it impossible for the filesystem
to recycle that inode's number while the test still holds the handle
open (the kernel will not reuse an inode with a nonzero reference
count), and it gives a direct, `fstat`-visible observable of the
unlink itself — the handle's own `nlink`, decremented by
`unlink`/`unlinkat` the instant the last directory entry naming that
inode is removed, independent of whether anything later reuses the
freed number for a different file. Since `remove_dir_all`'s directory
listing is taken strictly after the writer has already reported itself
live, it is guaranteed to call `unlinkat` on exactly the entry the
writer was using at that point — the pinned handle's `nlink` reaching
0 is race-free and filesystem-agnostic proof of that unlink, decided
the instant the panicking drop returns, with no polling. (The one
documented exception is NFS's silly-rename, which can leave an
open-but-unlinked file's directory entry renamed rather than removed
outright — not a concern for this repo's CI or sandbox targets, all
local filesystems.)

**General lesson, past this one test**: when a fix to a race test
picks an "identity" to assert on — an inode number, a file handle, a
generation counter, a version stamp — check what guarantees that
identity stays stable across the *exact* operation being raced, on the
*exact* filesystem/runtime the test will actually run against. A
number that looks like a stable identity (an inode number in
isolation) can be recycled by the very mechanism under test; a handle
that is actually pinned (an open file descriptor, held for the
duration) is a stronger guarantee because the pinning itself changes
what the OS is allowed to do, not just what the test happens to
observe. See `crates/animusd/tests/panic_safe_teardown.rs`'s own module
doc for the full mechanism and the corrected red-test verdict, and
`docs/lessons/testing/2026-09-02-a-test-that-deliberately-races-a-real-thread-against.md`
(issue #555) for the earlier two relocations of this same ambiguity.
