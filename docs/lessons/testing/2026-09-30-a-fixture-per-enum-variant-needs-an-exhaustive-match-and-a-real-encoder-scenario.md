# A fixture per enum variant needs an exhaustive match and a scenario the real encoder runs

**Lesson (2026-09-30, ADR 0073 Phase 1 P1-C, mirror `EntityKind` fixtures):**
"One golden fixture per variant" only stays true if adding a variant breaks the
build or a test. A directory-listing check alone lets a new variant go
uncovered (nothing lists it), and an `ALL` array alone can silently miss the
new variant. Use both: an exhaustive `match` with no wildcard (the variant
must be named to compile), the `ALL` list, and a test that every listed kind
has a fixture directory holding the current version.

Generate the bytes by running the real encoder over a scripted command
sequence and asserting (in a normal, non-ignored test) that the script produces
a live row of every kind. Otherwise a fixture can be hand-shaped bytes the
encoder never writes. Two traps hit while building this: a script that applies
a command the state machine rejects silently yields no row (assert `Applied`
per command), and ids allocated by earlier commands (restore/import tablets)
raise the monotonic allocator floor, so a later split's child ids must start
above it.

Zero-length values are legitimate (presence-marker kinds write an empty
value): the fixture is a zero-byte file, and the decode test still proves the
key/kind round trip.
