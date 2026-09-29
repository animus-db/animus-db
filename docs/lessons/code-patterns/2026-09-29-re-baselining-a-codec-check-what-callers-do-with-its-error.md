# Re-baselining a codec: check what callers do with its error type before typing it

**Context.** ADR 0073 Phase 0 workstream C reset the segment codec (`segment.rs`) to
`VERSION = 1` and needed the shared named `FormatError` (`PreBaselineFormat` /
`UnsupportedFormatVersion` / `Malformed`) instead of a free-text `String` error.

**Lesson.** `SegmentError` was a `pub type` alias for `String`, so swapping the alias to
`FormatError` looked like a cross-crate API break into `animusd`, `animus-test` and other crates
we were told not to edit. A grep of every call site (`decode`, `decode_and_slice`) showed each
one only `Display`s the error (`{e}`, `%err`) or discards it, so the change compiled everywhere
without touching them. Grep for how the error is consumed (methods like `.contains`, `.as_str`)
before deciding the change is out of scope, and keep the internal cursor helpers `String`-typed,
converting once at the public boundary into `Malformed`.

**Also.** The pre-check order matters for the "named, loud Err" rule: magic/length first
(`PreBaselineFormat`), then version (`UnsupportedFormatVersion`, including `0`), and only then
body decoding, so a truncated 3-byte buffer is never reported as a version problem.
