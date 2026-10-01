# A negative control for a recovery path that tolerates torn tails must assert the exact outcome per format, because "the open fails" is false for several of them — and the probe found CWL1/SWL1 treat a mid-file CRC failure as a torn tail.

**A negative control for a recovery path that tolerates torn tails must
assert the exact outcome per format and corruption, never "it fails".**
Writing the ADR 0073 tier-0 upgrade-restart controls showed the three
shapes differ: the LSM WAL refuses a mid-file bad record outright ("a
valid record still parses later ... not a torn tail"), a truncated tail is
tolerated in every WAL (open succeeds, so only the content check has
teeth), and the `CWL1`/`SWL1` **v1** line formats (no sync markers) treat a CRC failure as a
torn tail *wherever it sits*; v2 (issue #1132) raises the named
`MidFileCorruption` once a durable marker follows the bad line, and the
control now pins both. For v1 — corrupting the first line of a control WAL
decodes cleanly to zero records and silently drops the history after it.
Probe each corruption first (print the outcome), then pin it: a named
error where the decoder promises one, and "opens fine, content differs"
where recovery tolerates it. A control asserting only `is_err()` would have
failed on the tolerated shapes and hidden the asymmetry between formats.
(`crates/animus-test/tests/upgrade_restart_tier0.rs`.) The lesson inside the
lesson: when a format bump changes a tolerance, the old control silently
becomes version-specific. Say which version it pins and add the new version's
counterpart.
